from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import time
import uuid
import base64
from dataclasses import dataclass
from typing import Any, Mapping

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM


SAFE_ID = re.compile(r"^[A-Za-z0-9_.-]{1,128}$")
SIGNED_TASK_FIELDS = (
    "task_id",
    "agent_id",
    "action",
    "file_id",
    "key_id",
    "created_at",
    "nonce",
)
SIGNED_MANIFEST_FIELDS = (
    "task_id",
    "attempt_id",
    "agent_id",
    "file_id",
    "filename",
    "size",
    "chunks",
    "chunk_size",
    "sha256",
    "key_id",
    "created_at",
)
TERMINAL_STATES = {"downloaded", "cleaned", "failed", "cancelled", "expired"}
READY_STATES = {"stored_local", "cleaned", "downloaded"}


def now_seconds() -> int:
    return int(time.time())


def new_id() -> str:
    return str(uuid.uuid4())


def require_safe_id(value: str, field: str) -> str:
    if not SAFE_ID.fullmatch(value):
        raise ValueError(f"{field} contains unsupported characters")
    return value


def canonical_payload(data: Mapping[str, Any], fields: tuple[str, ...]) -> bytes:
    selected = {field: str(data[field]) for field in fields}
    return json.dumps(
        selected, ensure_ascii=True, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def sign_fields(
    data: Mapping[str, Any], fields: tuple[str, ...], secret: bytes
) -> str:
    return hmac.new(secret, canonical_payload(data, fields), hashlib.sha256).hexdigest()


def verify_fields(
    data: Mapping[str, Any], fields: tuple[str, ...], secret: bytes, signature: str
) -> bool:
    expected = sign_fields(data, fields, secret)
    return hmac.compare_digest(expected, signature)


def build_task(
    agent_id: str, file_id: str, key_id: str, secret: bytes
) -> dict[str, str]:
    require_safe_id(agent_id, "agent_id")
    require_safe_id(file_id, "file_id")
    require_safe_id(key_id, "key_id")
    task = {
        "task_id": new_id(),
        "agent_id": agent_id,
        "action": "fetch_file",
        "file_id": file_id,
        "key_id": key_id,
        "created_at": str(now_seconds()),
        "nonce": new_id(),
    }
    task["signature"] = sign_fields(task, SIGNED_TASK_FIELDS, secret)
    return task


def verify_task(task: Mapping[str, Any], secret: bytes, max_age: int) -> None:
    missing = [field for field in (*SIGNED_TASK_FIELDS, "signature") if field not in task]
    if missing:
        raise ValueError(f"task is missing fields: {', '.join(missing)}")
    if task["action"] != "fetch_file":
        raise ValueError("unsupported task action")
    for field in ("task_id", "agent_id", "file_id", "key_id", "nonce"):
        require_safe_id(str(task[field]), field)
    if not verify_fields(task, SIGNED_TASK_FIELDS, secret, str(task["signature"])):
        raise ValueError("invalid task signature")
    age = now_seconds() - int(task["created_at"])
    if age < -60 or age > max_age:
        raise ValueError("task timestamp is outside the accepted window")


def chunk_aad(task_id: str, attempt_id: str, index: int, key_id: str) -> bytes:
    return f"{task_id}:{attempt_id}:{index}:{key_id}".encode("ascii")


def encrypt_chunk(
    key: bytes,
    task_id: str,
    attempt_id: str,
    index: int,
    key_id: str,
    plaintext: bytes,
) -> bytes:
    nonce = os.urandom(12)
    ciphertext = AESGCM(key).encrypt(
        nonce, plaintext, chunk_aad(task_id, attempt_id, index, key_id)
    )
    return nonce + ciphertext


def decrypt_chunk(
    key: bytes,
    task_id: str,
    attempt_id: str,
    index: int,
    key_id: str,
    payload: bytes,
) -> bytes:
    if len(payload) < 28:
        raise ValueError("encrypted chunk is too short")
    try:
        return AESGCM(key).decrypt(
            payload[:12],
            payload[12:],
            chunk_aad(task_id, attempt_id, index, key_id),
        )
    except InvalidTag as exc:
        raise ValueError("encrypted chunk authentication failed") from exc


def sign_manifest(manifest: dict[str, str], secret: bytes) -> dict[str, str]:
    manifest["signature"] = sign_fields(manifest, SIGNED_MANIFEST_FIELDS, secret)
    return manifest


def verify_manifest(manifest: Mapping[str, Any], secret: bytes) -> None:
    missing = [
        field for field in (*SIGNED_MANIFEST_FIELDS, "signature") if field not in manifest
    ]
    if missing:
        raise ValueError(f"manifest is missing fields: {', '.join(missing)}")
    for field in ("task_id", "attempt_id", "agent_id", "file_id", "key_id"):
        require_safe_id(str(manifest[field]), field)
    if not verify_fields(
        manifest, SIGNED_MANIFEST_FIELDS, secret, str(manifest["signature"])
    ):
        raise ValueError("invalid manifest signature")


def build_download_ticket(task_id: str, secret: bytes, ttl: int = 60) -> str:
    require_safe_id(task_id, "task_id")
    payload = {
        "task_id": task_id,
        "expires_at": now_seconds() + ttl,
        "nonce": new_id(),
    }
    encoded = base64.urlsafe_b64encode(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).rstrip(b"=")
    signature = hmac.new(secret, encoded, hashlib.sha256).hexdigest().encode("ascii")
    return (encoded + b"." + signature).decode("ascii")


def verify_download_ticket(ticket: str, secret: bytes) -> dict[str, Any]:
    try:
        encoded_text, signature = ticket.split(".", 1)
        encoded = encoded_text.encode("ascii")
    except (ValueError, UnicodeEncodeError) as exc:
        raise ValueError("invalid download ticket") from exc
    expected = hmac.new(secret, encoded, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected, signature):
        raise ValueError("invalid download ticket signature")
    padded = encoded + b"=" * (-len(encoded) % 4)
    try:
        payload = json.loads(base64.urlsafe_b64decode(padded).decode("utf-8"))
    except (ValueError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("invalid download ticket payload") from exc
    require_safe_id(str(payload.get("task_id", "")), "task_id")
    require_safe_id(str(payload.get("nonce", "")), "nonce")
    if int(payload.get("expires_at", 0)) < now_seconds():
        raise ValueError("download ticket has expired")
    return payload


@dataclass(frozen=True)
class RedisKeys:
    prefix: str
    agent_id: str

    @property
    def task_stream(self) -> str:
        return f"{self.prefix}:agent:{self.agent_id}:tasks"

    @property
    def consumer_group(self) -> str:
        return "file_agents"

    @property
    def dead_letter_stream(self) -> str:
        return f"{self.prefix}:file_transfer:dead_letter"

    def status(self, task_id: str) -> str:
        return f"{self.prefix}:task:{task_id}:status"

    def manifest(self, task_id: str, attempt_id: str) -> str:
        return f"{self.prefix}:task:{task_id}:attempt:{attempt_id}:manifest"

    def chunk(self, task_id: str, attempt_id: str, index: int) -> str:
        return f"{self.prefix}:task:{task_id}:attempt:{attempt_id}:chunk:{index}"

    def chunk_ack(self, task_id: str, attempt_id: str) -> str:
        return f"{self.prefix}:task:{task_id}:attempt:{attempt_id}:chunk_ack"

    def remote_complete(self, task_id: str, attempt_id: str) -> str:
        return f"{self.prefix}:task:{task_id}:attempt:{attempt_id}:remote_complete"

    def cancel(self, task_id: str) -> str:
        return f"{self.prefix}:task:{task_id}:cancel"

    def nonce(self, nonce: str) -> str:
        return f"{self.prefix}:nonce:{nonce}"

    @property
    def transfer_buffer_bytes(self) -> str:
        return f"{self.prefix}:transfer_buffer_bytes"

    @property
    def active_transfers(self) -> str:
        return f"{self.prefix}:active_transfers"

    @property
    def transfer_lock(self) -> str:
        return f"{self.prefix}:active_transfers:lock"

    @property
    def heartbeat(self) -> str:
        return f"{self.prefix}:agent:{self.agent_id}:heartbeat"
