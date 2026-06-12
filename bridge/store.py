from __future__ import annotations

import time
import uuid
from typing import Any, Optional

from redis import Redis
from redis.exceptions import ResponseError

from .protocol import RedisKeys, now_seconds


class BridgeStore:
    def __init__(self, client: Redis, keys: RedisKeys, ttl: int):
        self.client = client
        self.keys = keys
        self.ttl = ttl

    def ensure_group(self) -> None:
        try:
            self.client.xgroup_create(
                self.keys.task_stream,
                self.keys.consumer_group,
                id="0",
                mkstream=True,
            )
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    def enqueue(self, task: dict[str, str]) -> str:
        task_id = task["task_id"]
        pipe = self.client.pipeline(transaction=True)
        pipe.hset(
            self.keys.status(task_id),
            mapping={
                "task_id": task_id,
                "agent_id": task["agent_id"],
                "file_id": task["file_id"],
                "key_id": task["key_id"],
                "state": "queued",
                "created_at": task["created_at"],
                "updated_at": str(now_seconds()),
                "attempts": "0",
            },
        )
        pipe.expire(self.keys.status(task_id), self.ttl)
        pipe.xadd(self.keys.task_stream, task)
        return self._text(pipe.execute()[-1])

    def set_status(self, task_id: str, state: str, **details: Any) -> None:
        mapping = {
            "state": state,
            "updated_at": str(now_seconds()),
            **{key: str(value) for key, value in details.items()},
        }
        pipe = self.client.pipeline(transaction=True)
        pipe.hset(self.keys.status(task_id), mapping=mapping)
        pipe.expire(self.keys.status(task_id), self.ttl)
        pipe.execute()

    def get_status(self, task_id: str) -> dict[str, str]:
        return self._decode_map(self.client.hgetall(self.keys.status(task_id)))

    def register_nonce(self, nonce: str, task_id: str) -> None:
        key = self.keys.nonce(nonce)
        existing = self.client.get(key)
        if existing is None:
            if not self.client.set(key, task_id, nx=True, ex=self.ttl):
                existing = self.client.get(key)
        if existing is not None and self._text(existing) != task_id:
            raise ValueError("task nonce has already been used")

    def consume_nonce(self, nonce: str, purpose: str, ttl: int) -> bool:
        return bool(
            self.client.set(
                self.keys.nonce(f"{purpose}.{nonce}"),
                "1",
                nx=True,
                ex=ttl,
            )
        )

    def save_chunk(
        self, task_id: str, attempt_id: str, index: int, payload: bytes
    ) -> None:
        self.client.set(
            self.keys.chunk(task_id, attempt_id, index), payload, ex=self.ttl
        )

    def get_chunk(
        self, task_id: str, attempt_id: str, index: int
    ) -> Optional[bytes]:
        return self.client.get(self.keys.chunk(task_id, attempt_id, index))

    def delete_chunk(self, task_id: str, attempt_id: str, index: int) -> None:
        self.client.delete(self.keys.chunk(task_id, attempt_id, index))

    def save_manifest(
        self, task_id: str, attempt_id: str, manifest: dict[str, str]
    ) -> None:
        key = self.keys.manifest(task_id, attempt_id)
        pipe = self.client.pipeline(transaction=True)
        pipe.hset(key, mapping=manifest)
        pipe.expire(key, self.ttl)
        pipe.execute()

    def get_manifest(self, task_id: str, attempt_id: str) -> dict[str, str]:
        return self._decode_map(
            self.client.hgetall(self.keys.manifest(task_id, attempt_id))
        )

    def acknowledge_chunk(self, task_id: str, attempt_id: str, index: int) -> None:
        self.client.set(
            self.keys.chunk_ack(task_id, attempt_id), index, ex=self.ttl
        )

    def acknowledged_chunk(self, task_id: str, attempt_id: str) -> int:
        value = self.client.get(self.keys.chunk_ack(task_id, attempt_id))
        return int(value) if value is not None else -1

    def mark_remote_complete(self, task_id: str, attempt_id: str) -> None:
        self.client.set(
            self.keys.remote_complete(task_id, attempt_id), "1", ex=self.ttl
        )

    def is_remote_complete(self, task_id: str, attempt_id: str) -> bool:
        return bool(self.client.exists(self.keys.remote_complete(task_id, attempt_id)))

    def request_cancel(self, task_id: str) -> None:
        self.client.set(self.keys.cancel(task_id), "1", ex=self.ttl)
        self.set_status(task_id, "cancelled", cancelled_at=now_seconds())

    def is_cancelled(self, task_id: str) -> bool:
        return bool(self.client.exists(self.keys.cancel(task_id)))

    def reserve_buffer(self, amount: int, maximum: int) -> bool:
        current = self.client.incrby(self.keys.transfer_buffer_bytes, amount)
        self.client.expire(self.keys.transfer_buffer_bytes, self.ttl)
        if current > maximum:
            self.client.decrby(self.keys.transfer_buffer_bytes, amount)
            return False
        return True

    def release_buffer(self, amount: int) -> None:
        current = self.client.decrby(self.keys.transfer_buffer_bytes, amount)
        if current < 0:
            self.client.set(self.keys.transfer_buffer_bytes, 0, ex=self.ttl)

    def buffer_bytes(self) -> int:
        return int(self.client.get(self.keys.transfer_buffer_bytes) or 0)

    def redis_memory_percent(self) -> Optional[float]:
        try:
            info = self.client.info("memory")
            used = int(info.get("used_memory", 0))
            maximum = int(info.get("maxmemory", 0))
            if maximum <= 0:
                return None
            return used * 100.0 / maximum
        except (ResponseError, NotImplementedError):
            return None

    def acquire_transfer(self, token: str, maximum: int, timeout: int) -> bool:
        lock_token = str(uuid.uuid4())
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            if self.client.set(
                self.keys.transfer_lock, lock_token, nx=True, ex=5
            ):
                try:
                    cutoff = now_seconds() - timeout
                    self.client.zremrangebyscore(
                        self.keys.active_transfers, "-inf", cutoff
                    )
                    if self.client.zcard(self.keys.active_transfers) >= maximum:
                        return False
                    self.client.zadd(
                        self.keys.active_transfers, {token: now_seconds()}
                    )
                    self.client.expire(self.keys.active_transfers, self.ttl)
                    return True
                finally:
                    if self._text(self.client.get(self.keys.transfer_lock) or "") == lock_token:
                        self.client.delete(self.keys.transfer_lock)
            time.sleep(0.05)
        return False

    def touch_transfer(self, token: str) -> None:
        self.client.zadd(self.keys.active_transfers, {token: now_seconds()})

    def release_transfer(self, token: str) -> None:
        self.client.zrem(self.keys.active_transfers, token)

    def cleanup_attempt(
        self, task_id: str, attempt_id: str, chunks: int, release_sizes: bool = False
    ) -> None:
        keys = [
            self.keys.manifest(task_id, attempt_id),
            self.keys.chunk_ack(task_id, attempt_id),
            self.keys.remote_complete(task_id, attempt_id),
        ]
        for index in range(chunks):
            chunk_key = self.keys.chunk(task_id, attempt_id, index)
            if release_sizes:
                payload = self.client.get(chunk_key)
                if payload is not None:
                    self.release_buffer(len(payload))
            keys.append(chunk_key)
        self.client.delete(*keys)

    def dead_letter(self, task: dict[str, str], error: str, attempts: int) -> None:
        self.client.xadd(
            self.keys.dead_letter_stream,
            {
                "task_id": task.get("task_id", "unknown"),
                "agent_id": task.get("agent_id", "unknown"),
                "file_id": task.get("file_id", "unknown"),
                "error": error[:1000],
                "attempts": str(attempts),
                "failed_at": str(now_seconds()),
            },
        )

    def heartbeat(self, consumer_name: str) -> None:
        self.client.set(
            self.keys.heartbeat, f"{consumer_name}:{now_seconds()}", ex=30
        )

    @staticmethod
    def _text(value: Any) -> str:
        return value.decode("utf-8") if isinstance(value, bytes) else str(value)

    @classmethod
    def _decode_map(cls, values: dict[Any, Any]) -> dict[str, str]:
        return {cls._text(key): cls._text(value) for key, value in values.items()}
