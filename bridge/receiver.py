from __future__ import annotations

import hashlib
import logging
import os
import threading
import time
from pathlib import Path

from .audit import AuditStore
from .config import Settings
from .protocol import READY_STATES, RedisKeys, decrypt_chunk, now_seconds, verify_manifest
from .store import BridgeStore


LOGGER = logging.getLogger("redis-file-receiver")


class AttemptSuperseded(Exception):
    pass


class LocalReceiver:
    def __init__(self, settings: Settings, store: BridgeStore, audit: AuditStore):
        self.settings = settings
        self.store = store
        self.audit = audit
        self._lock = threading.Lock()
        self._workers: dict[str, threading.Thread] = {}

    def recover(self) -> None:
        for task in self.audit.active():
            self.start(task["task_id"])

    def start(self, task_id: str) -> None:
        with self._lock:
            worker = self._workers.get(task_id)
            if worker and worker.is_alive():
                return
            worker = threading.Thread(
                target=self._run,
                args=(task_id,),
                name=f"receiver-{task_id[:8]}",
                daemon=True,
            )
            self._workers[task_id] = worker
            worker.start()

    def _run(self, task_id: str) -> None:
        try:
            self._receive_task(task_id)
        except Exception as exc:
            LOGGER.exception("local receive failed for task %s", task_id)
            self.audit.transition(
                task_id, "failed", error_message=str(exc)[:1000]
            )
            status = self.store.get_status(task_id)
            if status.get("state") not in {"cancelled", "expired"}:
                self.store.set_status(task_id, "failed", error=str(exc)[:500])
        finally:
            with self._lock:
                self._workers.pop(task_id, None)

    def _receive_task(self, task_id: str) -> None:
        audit_row = self.audit.get(task_id)
        if not audit_row:
            return
        if (
            audit_row.get("status") == "stored_local"
            and audit_row.get("local_path")
            and Path(audit_row["local_path"]).is_file()
        ):
            remote = self.store.get_status(task_id)
            attempt_id = audit_row.get("attempt_id") or remote.get("current_attempt")
            if attempt_id:
                manifest = self.store.get_manifest(task_id, attempt_id)
                self.store.cleanup_attempt(
                    task_id, attempt_id, int(manifest.get("chunks", "0"))
                )
            self.audit.transition(task_id, "cleaned")
            self.store.set_status(
                task_id,
                "cleaned",
                current_attempt=attempt_id or "",
                local_filename=Path(audit_row["local_path"]).name,
            )
            return
        deadline = audit_row["created_at"] + self.settings.transfer_timeout_seconds
        last_attempt = ""
        while time.time() < deadline:
            status = self.store.get_status(task_id)
            state = status.get("state", "")
            self._sync_audit(task_id, status)
            if state in {"cancelled", "expired", "failed"}:
                return
            if (
                state == "queued"
                and now_seconds() - int(status.get("created_at", now_seconds()))
                > self.settings.queued_timeout_seconds
            ):
                self.store.set_status(task_id, "expired", error="queue timeout")
                self.audit.transition(
                    task_id, "expired", error_message="queue timeout"
                )
                return

            attempt_id = status.get("current_attempt", "")
            if attempt_id and attempt_id != last_attempt:
                last_attempt = attempt_id
                try:
                    completed = self._receive_attempt(task_id, attempt_id, deadline)
                except TimeoutError as exc:
                    LOGGER.warning(
                        "attempt %s for task %s timed out locally: %s",
                        attempt_id,
                        task_id,
                        exc,
                    )
                    self.audit.transition(
                        task_id, "retrying", error_message=str(exc)[:1000]
                    )
                    last_attempt = ""
                    completed = False
                if completed:
                    return
            time.sleep(self.settings.receiver_poll_seconds)
        raise TimeoutError("local receive exceeded transfer timeout")

    def _receive_attempt(
        self, task_id: str, attempt_id: str, deadline: float
    ) -> bool:
        manifest: dict[str, str] = {}
        while time.time() < deadline:
            status = self.store.get_status(task_id)
            if status.get("current_attempt") != attempt_id:
                return False
            if self.store.is_cancelled(task_id):
                return False
            manifest = self.store.get_manifest(task_id, attempt_id)
            if manifest:
                break
            time.sleep(self.settings.receiver_poll_seconds)
        if not manifest:
            raise TimeoutError("manifest was not published")

        hmac_key, encryption_key = self.settings.keys_for(manifest["key_id"])
        verify_manifest(manifest, hmac_key)
        if manifest["task_id"] != task_id or manifest["attempt_id"] != attempt_id:
            raise ValueError("manifest identity mismatch")
        if manifest["agent_id"] != self.settings.agent_id:
            raise ValueError("manifest agent mismatch")

        expected_size = int(manifest["size"])
        chunk_count = int(manifest["chunks"])
        if expected_size > self.settings.max_file_bytes:
            raise ValueError("manifest exceeds BRIDGE_MAX_FILE_BYTES")
        if chunk_count < 0:
            raise ValueError("manifest chunk count is invalid")

        filename = _safe_filename(manifest["filename"])
        final_path = self.settings.download_dir / f"{task_id}-{filename}"
        self.settings.download_dir.mkdir(parents=True, exist_ok=True)
        temporary_path = (
            self.settings.download_dir / f".{task_id}-{attempt_id}.part"
        )
        audit_row = self.audit.get(task_id)
        resume = (
            audit_row.get("attempt_id") == attempt_id
            and audit_row.get("part_path") == str(temporary_path)
            and temporary_path.is_file()
        )
        start_index = int(audit_row.get("received_chunks") or 0) if resume else 0
        written = int(audit_row.get("received_bytes") or 0) if resume else 0
        if start_index > chunk_count or written > expected_size:
            resume = False
            start_index = 0
            written = 0
        if not resume:
            old_part = audit_row.get("part_path")
            if old_part:
                Path(old_part).unlink(missing_ok=True)
            temporary_path.unlink(missing_ok=True)

        digest = hashlib.sha256()
        if resume:
            with temporary_path.open("r+b") as existing:
                existing.truncate(written)
                existing.seek(0)
                while True:
                    block = existing.read(1024 * 1024)
                    if not block:
                        break
                    digest.update(block)
            if start_index:
                self.store.acknowledge_chunk(
                    task_id, attempt_id, start_index - 1
                )
        self.audit.transition(
            task_id,
            "receiving_local",
            attempt_id=attempt_id,
            attempt_count=int(self.store.get_status(task_id).get("attempts", "0")),
            part_path=str(temporary_path),
            received_chunks=start_index,
            received_bytes=written,
        )
        self.store.set_status(
            task_id, "receiving_local", current_attempt=attempt_id
        )
        try:
            with temporary_path.open("ab") as output:
                for index in range(start_index, chunk_count):
                    try:
                        encrypted = self._wait_for_chunk(
                            task_id, attempt_id, index, deadline
                        )
                    except AttemptSuperseded:
                        return False
                    plaintext = decrypt_chunk(
                        encryption_key,
                        task_id,
                        attempt_id,
                        index,
                        manifest["key_id"],
                        encrypted,
                    )
                    output.write(plaintext)
                    output.flush()
                    os.fsync(output.fileno())
                    digest.update(plaintext)
                    written += len(plaintext)
                    self.audit.update(
                        task_id,
                        received_chunks=index + 1,
                        received_bytes=written,
                    )
                    self.store.delete_chunk(task_id, attempt_id, index)
                    self.store.release_buffer(len(encrypted))
                    self.store.acknowledge_chunk(task_id, attempt_id, index)

            while not self.store.is_remote_complete(task_id, attempt_id):
                status = self.store.get_status(task_id)
                if status.get("current_attempt") != attempt_id:
                    return False
                if time.time() >= deadline:
                    raise TimeoutError("remote completion marker timeout")
                time.sleep(self.settings.receiver_poll_seconds)

            if written != expected_size:
                raise ValueError("assembled file size does not match manifest")
            if digest.hexdigest() != manifest["sha256"]:
                raise ValueError("assembled file SHA-256 does not match manifest")
            temporary_path.replace(final_path)
            self.audit.transition(
                task_id,
                "stored_local",
                attempt_id=attempt_id,
                file_size=written,
                sha256=digest.hexdigest(),
                local_path=str(final_path),
                part_path=None,
                received_chunks=chunk_count,
                received_bytes=written,
                error_message=None,
            )
            self.store.set_status(
                task_id,
                "stored_local",
                current_attempt=attempt_id,
                local_filename=final_path.name,
                size=written,
                sha256=digest.hexdigest(),
            )
            self.store.cleanup_attempt(task_id, attempt_id, chunk_count)
            self.audit.transition(task_id, "cleaned")
            self.store.set_status(
                task_id,
                "cleaned",
                current_attempt=attempt_id,
                local_filename=final_path.name,
            )
            return True
        finally:
            if self.audit.get(task_id).get("attempt_id") != attempt_id:
                temporary_path.unlink(missing_ok=True)

    def _wait_for_chunk(
        self, task_id: str, attempt_id: str, index: int, deadline: float
    ) -> bytes:
        chunk_deadline = min(
            deadline, time.time() + self.settings.chunk_ack_timeout_seconds
        )
        while time.time() < chunk_deadline:
            if self.store.is_cancelled(task_id):
                raise RuntimeError("task cancelled")
            status = self.store.get_status(task_id)
            if status.get("current_attempt") != attempt_id:
                raise AttemptSuperseded()
            payload = self.store.get_chunk(task_id, attempt_id, index)
            if payload is not None:
                return payload
            time.sleep(self.settings.receiver_poll_seconds)
        raise TimeoutError(f"chunk {index} receive timeout")

    def _sync_audit(self, task_id: str, status: dict[str, str]) -> None:
        state = status.get("state")
        if not state:
            return
        fields = {
            "attempt_id": status.get("current_attempt"),
            "attempt_count": int(status.get("attempts", "0")),
            "error_message": status.get("error"),
        }
        self.audit.transition(task_id, state, **fields)


def _safe_filename(filename: str) -> str:
    sanitized = Path(filename).name.replace("\x00", "")
    if sanitized in {"", ".", ".."}:
        return "download.bin"
    return sanitized
