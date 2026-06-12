from __future__ import annotations

import base64
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional, Tuple

from dotenv import load_dotenv


def _required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ValueError(f"missing required environment variable: {name}")
    return value


def _decode_aes(value: str) -> bytes:
    key = base64.urlsafe_b64decode(value.encode("ascii"))
    if len(key) != 32:
        raise ValueError("encryption key must decode to exactly 32 bytes")
    return key


@dataclass(frozen=True)
class Settings:
    redis_url: str
    prefix: str
    agent_id: str
    hmac_key: bytes
    encryption_key: bytes
    api_token: str
    download_dir: Path
    file_map_path: Path
    chunk_size: int
    data_ttl_seconds: int
    max_file_bytes: int
    key_id: str = "v1"
    keyring: Optional[Dict[str, Tuple[bytes, bytes]]] = None
    audit_db_path: Path = Path("./audit.sqlite3")
    snapshot_dir: Path = Path("./snapshots")
    window_size: int = 16
    max_concurrent_transfers: int = 1
    max_transfer_buffer_bytes: int = 300 * 1024 * 1024
    redis_memory_warn_percent: int = 70
    redis_memory_hard_stop_percent: int = 80
    queued_timeout_seconds: int = 600
    transfer_timeout_seconds: int = 1800
    chunk_ack_timeout_seconds: int = 300
    receiver_poll_seconds: float = 0.1

    @classmethod
    def from_env(cls) -> "Settings":
        load_dotenv()
        key_id = os.getenv("BRIDGE_KEY_ID", "v1").strip()
        hmac_key = _required("BRIDGE_HMAC_KEY").encode("utf-8")
        if len(hmac_key) < 32:
            raise ValueError("BRIDGE_HMAC_KEY must be at least 32 bytes")
        encryption_key = _decode_aes(_required("BRIDGE_ENCRYPTION_KEY"))

        keyring: Dict[str, Tuple[bytes, bytes]] = {
            key_id: (hmac_key, encryption_key)
        }
        keyring_path = os.getenv("BRIDGE_KEYRING_PATH", "").strip()
        if keyring_path:
            raw = json.loads(Path(keyring_path).expanduser().read_text("utf-8"))
            for ring_id, material in raw.items():
                ring_hmac = str(material["hmac"]).encode("utf-8")
                if len(ring_hmac) < 32:
                    raise ValueError(f"HMAC key {ring_id} must be at least 32 bytes")
                keyring[str(ring_id)] = (
                    ring_hmac,
                    _decode_aes(str(material["encryption"])),
                )
        if key_id not in keyring:
            raise ValueError("BRIDGE_KEY_ID is not present in the keyring")

        return cls(
            redis_url=_required("REDIS_URL"),
            prefix=os.getenv("BRIDGE_PREFIX", "redis_file_bridge").strip(),
            agent_id=os.getenv("BRIDGE_AGENT_ID", "custodian_01").strip(),
            hmac_key=hmac_key,
            encryption_key=encryption_key,
            api_token=os.getenv("BRIDGE_API_TOKEN", "").strip(),
            download_dir=Path(
                os.getenv("BRIDGE_DOWNLOAD_DIR", "./downloads")
            ).expanduser(),
            file_map_path=Path(
                os.getenv("BRIDGE_FILE_MAP", "./config/files.json")
            ).expanduser(),
            chunk_size=int(os.getenv("BRIDGE_CHUNK_SIZE", "1048576")),
            data_ttl_seconds=int(os.getenv("BRIDGE_DATA_TTL_SECONDS", "86400")),
            max_file_bytes=int(
                os.getenv("BRIDGE_MAX_FILE_BYTES", str(100 * 1024 * 1024))
            ),
            key_id=key_id,
            keyring=keyring,
            audit_db_path=Path(
                os.getenv("BRIDGE_AUDIT_DB", "./audit.sqlite3")
            ).expanduser(),
            snapshot_dir=Path(
                os.getenv("BRIDGE_SNAPSHOT_DIR", "./snapshots")
            ).expanduser(),
            window_size=int(os.getenv("BRIDGE_WINDOW_SIZE", "16")),
            max_concurrent_transfers=int(
                os.getenv("BRIDGE_MAX_CONCURRENT_TRANSFERS", "1")
            ),
            max_transfer_buffer_bytes=int(
                os.getenv(
                    "BRIDGE_MAX_TRANSFER_BUFFER_BYTES", str(300 * 1024 * 1024)
                )
            ),
            redis_memory_warn_percent=int(
                os.getenv("BRIDGE_REDIS_MEMORY_WARN_PERCENT", "70")
            ),
            redis_memory_hard_stop_percent=int(
                os.getenv("BRIDGE_REDIS_MEMORY_HARD_STOP_PERCENT", "80")
            ),
            queued_timeout_seconds=int(
                os.getenv("BRIDGE_QUEUED_TIMEOUT_SECONDS", "600")
            ),
            transfer_timeout_seconds=int(
                os.getenv("BRIDGE_TRANSFER_TIMEOUT_SECONDS", "1800")
            ),
            chunk_ack_timeout_seconds=int(
                os.getenv("BRIDGE_CHUNK_ACK_TIMEOUT_SECONDS", "300")
            ),
            receiver_poll_seconds=float(
                os.getenv("BRIDGE_RECEIVER_POLL_SECONDS", "0.1")
            ),
        )

    def keys_for(self, key_id: str) -> Tuple[bytes, bytes]:
        if self.keyring and key_id in self.keyring:
            return self.keyring[key_id]
        if key_id == self.key_id:
            return self.hmac_key, self.encryption_key
        raise ValueError(f"unknown key_id: {key_id}")

    def validate(self) -> None:
        if not self.prefix or not self.agent_id:
            raise ValueError("BRIDGE_PREFIX and BRIDGE_AGENT_ID cannot be empty")
        if not 64 * 1024 <= self.chunk_size <= 8 * 1024 * 1024:
            raise ValueError("BRIDGE_CHUNK_SIZE must be between 64 KiB and 8 MiB")
        if self.max_file_bytes <= 0:
            raise ValueError("BRIDGE_MAX_FILE_BYTES must be positive")
        if not 1 <= self.window_size <= 256:
            raise ValueError("BRIDGE_WINDOW_SIZE must be between 1 and 256")
        if self.max_concurrent_transfers not in (1, 2):
            raise ValueError("BRIDGE_MAX_CONCURRENT_TRANSFERS must be 1 or 2")
        if self.max_transfer_buffer_bytes < self.chunk_size * self.window_size:
            raise ValueError("transfer buffer must hold at least one window")
        if not (
            1
            <= self.redis_memory_warn_percent
            < self.redis_memory_hard_stop_percent
            <= 100
        ):
            raise ValueError("invalid Redis memory thresholds")
