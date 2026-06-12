import base64
import hashlib
import time
from pathlib import Path

import fakeredis

from bridge.agent import FileAgent
from bridge.audit import AuditStore
from bridge.config import Settings
from bridge.protocol import RedisKeys, build_task, encrypt_chunk, sign_manifest
from bridge.receiver import LocalReceiver
from bridge.store import BridgeStore
from bridge.web import create_app


def make_settings(tmp_path: Path, file_map: Path) -> Settings:
    return Settings(
        redis_url="redis://unused",
        prefix="test_bridge",
        agent_id="agent_1",
        hmac_key=b"h" * 32,
        encryption_key=base64.urlsafe_b64decode(
            b"MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY="
        ),
        api_token="test-token",
        download_dir=tmp_path / "downloads",
        file_map_path=file_map,
        chunk_size=64 * 1024,
        data_ttl_seconds=3600,
        max_file_bytes=1024 * 1024,
        key_id="v1",
        audit_db_path=tmp_path / "audit.sqlite3",
        snapshot_dir=tmp_path / "snapshots",
        window_size=2,
        max_concurrent_transfers=1,
        max_transfer_buffer_bytes=2 * 64 * 1024 + 1024,
        receiver_poll_seconds=0.01,
        chunk_ack_timeout_seconds=5,
        transfer_timeout_seconds=10,
    )


def wait_for(predicate, timeout=3):
    deadline = time.time() + timeout
    while time.time() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(0.01)
    raise AssertionError("condition was not met before timeout")


def test_sliding_window_transfer_stores_local_file_and_cleans_redis(tmp_path):
    source = tmp_path / "source.bin"
    original = (b"redis-file-bridge-" * 9000) + b"end"
    source.write_bytes(original)
    file_map = tmp_path / "files.json"
    file_map.write_text(
        '{"allowed-file": {"path": "'
        + str(source).replace("\\", "\\\\")
        + '", "max_file_bytes": 1048576}}',
        encoding="utf-8",
    )
    settings = make_settings(tmp_path, file_map)
    client = fakeredis.FakeRedis()
    keys = RedisKeys(settings.prefix, settings.agent_id)
    store = BridgeStore(client, keys, settings.data_ttl_seconds)
    audit = AuditStore(settings.audit_db_path)
    receiver = LocalReceiver(settings, store, audit)
    agent = FileAgent(settings, client=client, consumer_name="test-consumer")
    task = build_task(
        settings.agent_id, "allowed-file", settings.key_id, settings.hmac_key
    )
    audit.create(
        task["task_id"],
        settings.prefix,
        settings.agent_id,
        task["file_id"],
        "pytest",
        int(task["created_at"]),
    )
    store.enqueue(task)
    attempt_id = "attempt-1"
    store.set_status(
        task["task_id"],
        "claimed",
        current_attempt=attempt_id,
        attempts=1,
    )
    receiver.start(task["task_id"])

    agent._transfer(task, attempt_id)

    row = wait_for(
        lambda: (
            value
            if (value := audit.get(task["task_id"])).get("status") == "cleaned"
            else None
        )
    )
    assert Path(row["local_path"]).read_bytes() == original
    assert row["attempt_id"] == attempt_id
    assert store.buffer_bytes() == 0
    assert not client.exists(keys.manifest(task["task_id"], attempt_id))


def test_attempt_namespaces_do_not_mix_chunks(tmp_path):
    file_map = tmp_path / "files.json"
    source = tmp_path / "source"
    source.write_bytes(b"x")
    file_map.write_text('{"file": "' + str(source) + '"}', encoding="utf-8")
    settings = make_settings(tmp_path, file_map)
    client = fakeredis.FakeRedis()
    keys = RedisKeys(settings.prefix, settings.agent_id)
    store = BridgeStore(client, keys, settings.data_ttl_seconds)

    store.save_chunk("task-1", "attempt-1", 0, b"old")
    store.save_chunk("task-1", "attempt-2", 0, b"new")

    assert store.get_chunk("task-1", "attempt-1", 0) == b"old"
    assert store.get_chunk("task-1", "attempt-2", 0) == b"new"


def test_transfer_buffer_limit_and_dead_letter_queue(tmp_path):
    file_map = tmp_path / "files.json"
    source = tmp_path / "source"
    source.write_bytes(b"x")
    file_map.write_text('{"file": "' + str(source) + '"}', encoding="utf-8")
    settings = make_settings(tmp_path, file_map)
    client = fakeredis.FakeRedis()
    keys = RedisKeys(settings.prefix, settings.agent_id)
    store = BridgeStore(client, keys, settings.data_ttl_seconds)

    assert store.reserve_buffer(100, 150)
    assert not store.reserve_buffer(100, 150)
    assert store.buffer_bytes() == 100
    store.release_buffer(100)
    assert store.buffer_bytes() == 0

    store.dead_letter(
        {"task_id": "task-1", "agent_id": "agent-1", "file_id": "file"},
        "sha256 mismatch",
        3,
    )
    entries = client.xrange(keys.dead_letter_stream)
    assert len(entries) == 1
    assert entries[0][1][b"attempts"] == b"3"


def test_receiver_resumes_from_durable_part_file(tmp_path):
    source = tmp_path / "source"
    source.write_bytes(b"x")
    file_map = tmp_path / "files.json"
    file_map.write_text('{"file": "' + str(source) + '"}', encoding="utf-8")
    settings = make_settings(tmp_path, file_map)
    settings.download_dir.mkdir()
    client = fakeredis.FakeRedis()
    keys = RedisKeys(settings.prefix, settings.agent_id)
    store = BridgeStore(client, keys, settings.data_ttl_seconds)
    audit = AuditStore(settings.audit_db_path)
    receiver = LocalReceiver(settings, store, audit)
    task = build_task(settings.agent_id, "file", settings.key_id, settings.hmac_key)
    attempt_id = "attempt-resume"
    first = b"a" * settings.chunk_size
    second = b"tail"
    complete = first + second
    manifest = sign_manifest(
        {
            "task_id": task["task_id"],
            "attempt_id": attempt_id,
            "agent_id": settings.agent_id,
            "file_id": "file",
            "filename": "source.bin",
            "size": str(len(complete)),
            "chunks": "2",
            "chunk_size": str(settings.chunk_size),
            "sha256": hashlib.sha256(complete).hexdigest(),
            "key_id": settings.key_id,
            "created_at": task["created_at"],
        },
        settings.hmac_key,
    )
    audit.create(
        task["task_id"],
        settings.prefix,
        settings.agent_id,
        "file",
        "pytest",
        int(task["created_at"]),
    )
    part = settings.download_dir / f".{task['task_id']}-{attempt_id}.part"
    part.write_bytes(first + b"uncommitted-data")
    audit.transition(
        task["task_id"],
        "receiving_local",
        attempt_id=attempt_id,
        part_path=str(part),
        received_chunks=1,
        received_bytes=len(first),
    )
    store.enqueue(task)
    store.set_status(
        task["task_id"],
        "uploading",
        current_attempt=attempt_id,
        attempts=1,
    )
    store.save_manifest(task["task_id"], attempt_id, manifest)
    encrypted = encrypt_chunk(
        settings.encryption_key,
        task["task_id"],
        attempt_id,
        1,
        settings.key_id,
        second,
    )
    assert store.reserve_buffer(
        len(encrypted), settings.max_transfer_buffer_bytes
    )
    store.save_chunk(task["task_id"], attempt_id, 1, encrypted)
    store.mark_remote_complete(task["task_id"], attempt_id)

    receiver.start(task["task_id"])

    row = wait_for(
        lambda: (
            value
            if (value := audit.get(task["task_id"])).get("status") == "cleaned"
            else None
        )
    )
    assert Path(row["local_path"]).read_bytes() == complete
    assert row["received_chunks"] == 2


def test_web_api_authenticates_audits_enqueues_and_cancels(tmp_path):
    source = tmp_path / "source.txt"
    source.write_text("data", encoding="utf-8")
    file_map = tmp_path / "files.json"
    file_map.write_text(
        '{"daily-report": "' + str(source).replace("\\", "\\\\") + '"}',
        encoding="utf-8",
    )
    settings = make_settings(tmp_path, file_map)
    client = fakeredis.FakeRedis()
    app = create_app(settings, client)
    web = app.test_client()

    assert web.post("/api/tasks", json={"file_id": "daily-report"}).status_code == 401
    response = web.post(
        "/api/tasks",
        json={"file_id": "daily-report"},
        headers={
            "Authorization": "Bearer test-token",
            "X-Requester": "amy",
        },
    )
    assert response.status_code == 202
    task_id = response.get_json()["task_id"]
    status = web.get(
        f"/api/tasks/{task_id}",
        headers={"Authorization": "Bearer test-token"},
    ).get_json()
    assert status["state"] == "queued"
    assert status["requester"] == "amy"

    cancelled = web.post(
        f"/api/tasks/{task_id}/cancel",
        headers={"Authorization": "Bearer test-token"},
    )
    assert cancelled.get_json()["state"] == "cancelled"


def test_web_issues_one_time_native_download_ticket(tmp_path):
    source = tmp_path / "source.txt"
    source.write_text("browser download", encoding="utf-8")
    file_map = tmp_path / "files.json"
    file_map.write_text('{"file": "' + str(source) + '"}', encoding="utf-8")
    settings = make_settings(tmp_path, file_map)
    settings.download_dir.mkdir()
    client = fakeredis.FakeRedis()
    app = create_app(settings, client)
    web = app.test_client()
    audit = app.config["AUDIT"]
    stored = settings.download_dir / "task-1-source.txt"
    stored.write_bytes(source.read_bytes())
    audit.create(
        "task-1",
        settings.prefix,
        settings.agent_id,
        "file",
        "pytest",
        int(time.time()),
    )
    audit.transition(
        "task-1",
        "cleaned",
        local_path=str(stored),
        file_size=stored.stat().st_size,
    )
    headers = {"Authorization": "Bearer test-token"}

    response = web.post("/api/tasks/task-1/download-ticket", headers=headers)
    assert response.status_code == 200
    download_url = response.get_json()["download_url"]

    first = web.get(download_url)
    assert first.status_code == 200
    assert first.data == b"browser download"
    assert "attachment" in first.headers["Content-Disposition"]

    second = web.get(download_url)
    assert second.status_code == 410
