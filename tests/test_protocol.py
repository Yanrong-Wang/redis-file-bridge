import base64
import hashlib

import pytest

from bridge.protocol import (
    SIGNED_MANIFEST_FIELDS,
    build_download_ticket,
    build_task,
    decrypt_chunk,
    encrypt_chunk,
    sign_manifest,
    verify_manifest,
    verify_download_ticket,
    verify_task,
)


HMAC_KEY = b"h" * 32
AES_KEY = base64.urlsafe_b64decode(
    b"MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY="
)


def test_task_is_signed_with_key_and_nonce_and_tampering_is_rejected():
    task = build_task("agent-1", "daily-report", "v1", HMAC_KEY)
    verify_task(task, HMAC_KEY, max_age=3600)
    assert task["key_id"] == "v1"
    assert task["nonce"]

    task["file_id"] = "trade-log"
    with pytest.raises(ValueError, match="signature"):
        verify_task(task, HMAC_KEY, max_age=3600)


def test_encrypted_chunk_is_bound_to_attempt_and_index():
    encrypted = encrypt_chunk(
        AES_KEY, "task-1", "attempt-1", 0, "v1", b"secret contents"
    )
    assert (
        decrypt_chunk(AES_KEY, "task-1", "attempt-1", 0, "v1", encrypted)
        == b"secret contents"
    )
    with pytest.raises(ValueError, match="authentication"):
        decrypt_chunk(AES_KEY, "task-1", "attempt-2", 0, "v1", encrypted)


def test_manifest_signature_covers_attempt_and_integrity_fields():
    data = b"report"
    manifest = sign_manifest(
        {
            "task_id": "task-1",
            "attempt_id": "attempt-1",
            "agent_id": "agent-1",
            "file_id": "report",
            "filename": "report.csv",
            "size": str(len(data)),
            "chunks": "1",
            "chunk_size": "1024",
            "sha256": hashlib.sha256(data).hexdigest(),
            "key_id": "v1",
            "created_at": "100",
        },
        HMAC_KEY,
    )
    assert set(SIGNED_MANIFEST_FIELDS).issubset(manifest)
    verify_manifest(manifest, HMAC_KEY)

    manifest["attempt_id"] = "attempt-2"
    with pytest.raises(ValueError, match="signature"):
        verify_manifest(manifest, HMAC_KEY)


def test_download_ticket_is_signed_and_contains_task():
    ticket = build_download_ticket("task-1", HMAC_KEY, ttl=60)
    payload = verify_download_ticket(ticket, HMAC_KEY)
    assert payload["task_id"] == "task-1"

    damaged = ticket[:-1] + ("0" if ticket[-1] != "0" else "1")
    with pytest.raises(ValueError, match="signature"):
        verify_download_ticket(damaged, HMAC_KEY)
