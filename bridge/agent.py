from __future__ import annotations

import hashlib
import json
import logging
import math
import os
import socket
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Optional

from redis import Redis

from .config import Settings
from .protocol import RedisKeys, encrypt_chunk, new_id, now_seconds, sign_manifest, verify_task
from .store import BridgeStore


LOGGER = logging.getLogger("redis-file-agent")
MAX_ATTEMPTS = 3
CLAIM_IDLE_MS = 60_000


@dataclass(frozen=True)
class FilePolicy:
    path: Path
    max_file_bytes: Optional[int] = None
    allowed_hours: Optional[str] = None

    def validate_time(self) -> None:
        if not self.allowed_hours:
            return
        start_text, end_text = self.allowed_hours.split("-", 1)
        now = datetime.now().time()
        start = datetime.strptime(start_text, "%H:%M").time()
        end = datetime.strptime(end_text, "%H:%M").time()
        allowed = start <= now <= end if start <= end else now >= start or now <= end
        if not allowed:
            raise ValueError("file_id is not available at the current time")


def load_file_map(path: Path) -> dict[str, FilePolicy]:
    from .protocol import require_safe_id

    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict) or not raw:
        raise ValueError("file map must be a non-empty JSON object")
    result: dict[str, FilePolicy] = {}
    for file_id, value in raw.items():
        require_safe_id(str(file_id), "file_id")
        if isinstance(value, str):
            configured = Path(value).expanduser()
            maximum = None
            allowed_hours = None
        elif isinstance(value, dict):
            configured = Path(str(value["path"])).expanduser()
            maximum = (
                int(value["max_file_bytes"])
                if value.get("max_file_bytes") is not None
                else None
            )
            allowed_hours = (
                str(value["allowed_hours"]) if value.get("allowed_hours") else None
            )
        else:
            raise ValueError("file map values must be paths or policy objects")
        if not configured.is_absolute():
            raise ValueError(f"path for {file_id} must be absolute")
        result[str(file_id)] = FilePolicy(
            configured.resolve(), maximum, allowed_hours
        )
    return result


class FileAgent:
    def __init__(
        self,
        settings: Settings,
        client: Optional[Redis] = None,
        consumer_name: Optional[str] = None,
    ):
        settings.validate()
        self.settings = settings
        self.client = client or Redis.from_url(settings.redis_url)
        self.keys = RedisKeys(settings.prefix, settings.agent_id)
        self.store = BridgeStore(self.client, self.keys, settings.data_ttl_seconds)
        self.file_map = load_file_map(settings.file_map_path)
        self.consumer_name = consumer_name or f"{socket.gethostname()}-{os.getpid()}"
        settings.snapshot_dir.mkdir(parents=True, exist_ok=True)

    def run_forever(self) -> None:
        self.store.ensure_group()
        LOGGER.info("agent %s is listening on %s", self.consumer_name, self.keys.task_stream)
        last_claim = 0.0
        while True:
            self.store.heartbeat(self.consumer_name)
            if time.monotonic() - last_claim >= 30:
                self._claim_stale()
                last_claim = time.monotonic()
            messages = self.client.xreadgroup(
                self.keys.consumer_group,
                self.consumer_name,
                {self.keys.task_stream: ">"},
                count=1,
                block=5000,
            )
            for message_id, task in self._iter_messages(messages):
                self._handle(message_id, task)

    def _claim_stale(self) -> None:
        claimed = self.client.xautoclaim(
            self.keys.task_stream,
            self.keys.consumer_group,
            self.consumer_name,
            min_idle_time=CLAIM_IDLE_MS,
            start_id="0-0",
            count=10,
        )
        for message_id, fields in (claimed[1] if len(claimed) >= 2 else []):
            self._handle(message_id, self._decode_map(fields))

    def _handle(self, message_id: Any, task: dict[str, str]) -> None:
        task_id = task.get("task_id", "unknown")
        attempt_id = ""
        attempts = 0
        try:
            hmac_key, _ = self.settings.keys_for(task["key_id"])
            verify_task(task, hmac_key, self.settings.data_ttl_seconds)
            if task["agent_id"] != self.settings.agent_id:
                raise ValueError("task targets a different agent")
            self.store.register_nonce(task["nonce"], task_id)
            if self.store.is_cancelled(task_id):
                self._ack(message_id)
                return
            status = self.store.get_status(task_id)
            if status.get("state") in {"cancelled", "expired"}:
                self._ack(message_id)
                return
            if status.get("state") in {"cleaned", "downloaded", "stored_local"}:
                self._ack(message_id)
                return

            attempts = self.client.hincrby(self.keys.status(task_id), "attempts", 1)
            attempt_id = new_id()
            self.store.set_status(
                task_id,
                "claimed",
                attempts=attempts,
                current_attempt=attempt_id,
                claimed_at=now_seconds(),
            )
            self._transfer(task, attempt_id)
            self._ack(message_id)
            LOGGER.info("task %s attempt %s completed remotely", task_id, attempt_id)
        except Exception as exc:
            LOGGER.exception("task %s attempt %s failed", task_id, attempt_id)
            if attempts == 0:
                attempts = self.client.hincrby(
                    self.keys.status(task_id), "attempts", 1
                )
            if attempt_id:
                manifest = self.store.get_manifest(task_id, attempt_id)
                self.store.cleanup_attempt(
                    task_id,
                    attempt_id,
                    int(manifest.get("chunks", "0")),
                    release_sizes=True,
                )
            if self.store.is_cancelled(task_id):
                self.store.set_status(
                    task_id,
                    "cancelled",
                    error="cancelled by requester",
                    attempts=attempts,
                )
                self._ack(message_id)
                return
            final = attempts >= MAX_ATTEMPTS
            self.store.set_status(
                task_id,
                "failed" if final else "retrying",
                error=str(exc)[:500],
                attempts=attempts,
            )
            if final:
                self.store.dead_letter(task, str(exc), attempts)
                self._ack(message_id)

    def _transfer(self, task: dict[str, str], attempt_id: str) -> None:
        task_id = task["task_id"]
        policy = self.file_map.get(task["file_id"])
        if policy is None:
            raise ValueError(f"file_id is not allowed: {task['file_id']}")
        policy.validate_time()
        path = policy.path
        if path.is_symlink() or not path.is_file():
            raise ValueError("mapped path is not a regular file")
        maximum = min(
            self.settings.max_file_bytes,
            policy.max_file_bytes or self.settings.max_file_bytes,
        )
        if path.stat().st_size > maximum:
            raise ValueError("file exceeds configured size limit")
        if not self.store.acquire_transfer(
            attempt_id,
            self.settings.max_concurrent_transfers,
            self.settings.transfer_timeout_seconds,
        ):
            raise RuntimeError("maximum concurrent transfers reached")

        snapshot: Optional[Path] = None
        try:
            snapshot, size, digest = self._snapshot(path, maximum, task_id)
            chunks = math.ceil(size / self.settings.chunk_size) if size else 0
            hmac_key, encryption_key = self.settings.keys_for(task["key_id"])
            manifest = sign_manifest(
                {
                    "task_id": task_id,
                    "attempt_id": attempt_id,
                    "agent_id": self.settings.agent_id,
                    "file_id": task["file_id"],
                    "filename": path.name,
                    "size": str(size),
                    "chunks": str(chunks),
                    "chunk_size": str(self.settings.chunk_size),
                    "sha256": digest,
                    "key_id": task["key_id"],
                    "created_at": str(now_seconds()),
                },
                hmac_key,
            )
            self.store.save_manifest(task_id, attempt_id, manifest)
            self.store.set_status(
                task_id,
                "running",
                current_attempt=attempt_id,
                total_bytes=size,
                chunks=chunks,
            )
            deadline = time.time() + self.settings.transfer_timeout_seconds
            with snapshot.open("rb") as source:
                for index in range(chunks):
                    self._wait_for_window(task_id, attempt_id, index, deadline)
                    self._check_resource_limits(task_id)
                    plaintext = source.read(self.settings.chunk_size)
                    encrypted = encrypt_chunk(
                        encryption_key,
                        task_id,
                        attempt_id,
                        index,
                        task["key_id"],
                        plaintext,
                    )
                    if not self.store.reserve_buffer(
                        len(encrypted), self.settings.max_transfer_buffer_bytes
                    ):
                        raise RuntimeError("transfer buffer limit reached")
                    try:
                        self.store.save_chunk(task_id, attempt_id, index, encrypted)
                    except Exception:
                        self.store.release_buffer(len(encrypted))
                        raise
                    self.store.touch_transfer(attempt_id)
                    self.store.set_status(
                        task_id,
                        "uploading",
                        current_attempt=attempt_id,
                        bytes_transferred=min((index + 1) * self.settings.chunk_size, size),
                        total_bytes=size,
                        chunks=chunks,
                    )
            self._wait_for_final_ack(task_id, attempt_id, chunks, deadline)
            self.store.mark_remote_complete(task_id, attempt_id)
            self.store.set_status(
                task_id,
                "completed_remote",
                current_attempt=attempt_id,
                filename=path.name,
                size=size,
                chunks=chunks,
                sha256=digest,
            )
        finally:
            self.store.release_transfer(attempt_id)
            if snapshot:
                snapshot.unlink(missing_ok=True)

    def _snapshot(self, source: Path, maximum: int, task_id: str) -> tuple[Path, int, str]:
        before = source.stat()
        fd, name = tempfile.mkstemp(
            prefix=f"{task_id}-", suffix=".snapshot", dir=self.settings.snapshot_dir
        )
        digest = hashlib.sha256()
        size = 0
        snapshot = Path(name)
        try:
            with source.open("rb") as input_file, os.fdopen(fd, "wb") as output:
                while True:
                    block = input_file.read(1024 * 1024)
                    if not block:
                        break
                    size += len(block)
                    if size > maximum:
                        raise ValueError("file exceeds configured size limit")
                    output.write(block)
                    digest.update(block)
                output.flush()
                os.fsync(output.fileno())
            after = source.stat()
            if before.st_size != after.st_size or before.st_mtime_ns != after.st_mtime_ns:
                raise RuntimeError("source file changed while snapshot was created")
            return snapshot, size, digest.hexdigest()
        except Exception:
            snapshot.unlink(missing_ok=True)
            raise

    def _wait_for_window(
        self, task_id: str, attempt_id: str, index: int, deadline: float
    ) -> None:
        while index - self.store.acknowledged_chunk(task_id, attempt_id) - 1 >= self.settings.window_size:
            self._check_wait_state(task_id, deadline, "chunk acknowledgement timeout")
            time.sleep(0.05)

    def _wait_for_final_ack(
        self, task_id: str, attempt_id: str, chunks: int, deadline: float
    ) -> None:
        expected = chunks - 1
        while self.store.acknowledged_chunk(task_id, attempt_id) < expected:
            self._check_wait_state(task_id, deadline, "final chunk acknowledgement timeout")
            time.sleep(0.05)

    def _check_wait_state(self, task_id: str, deadline: float, message: str) -> None:
        if self.store.is_cancelled(task_id):
            raise RuntimeError("task cancelled")
        if time.time() >= deadline:
            raise TimeoutError(message)

    def _check_resource_limits(self, task_id: str) -> None:
        if self.store.is_cancelled(task_id):
            raise RuntimeError("task cancelled")
        percent = self.store.redis_memory_percent()
        if percent is None:
            return
        if percent >= self.settings.redis_memory_hard_stop_percent:
            raise RuntimeError(f"Redis memory hard stop reached: {percent:.1f}%")
        if percent >= self.settings.redis_memory_warn_percent:
            LOGGER.warning("Redis memory usage is %.1f%% for task %s", percent, task_id)

    def _ack(self, message_id: Any) -> None:
        self.client.xack(self.keys.task_stream, self.keys.consumer_group, message_id)

    @classmethod
    def _iter_messages(
        cls, messages: Iterable[Any]
    ) -> Iterable[tuple[Any, dict[str, str]]]:
        for _, entries in messages:
            for message_id, fields in entries:
                yield message_id, cls._decode_map(fields)

    @staticmethod
    def _decode_map(fields: dict[Any, Any]) -> dict[str, str]:
        def text(value: Any) -> str:
            return value.decode("utf-8") if isinstance(value, bytes) else str(value)

        return {text(key): text(value) for key, value in fields.items()}


def main() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    FileAgent(Settings.from_env()).run_forever()


if __name__ == "__main__":
    main()
