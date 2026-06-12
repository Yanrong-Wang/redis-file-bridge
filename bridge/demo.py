from __future__ import annotations

import base64
import json
import logging
import threading
from pathlib import Path

import fakeredis
from waitress import serve

from .agent import FileAgent
from .config import Settings
from .web import create_app


def main() -> None:
    project_dir = Path(__file__).resolve().parent.parent
    file_map_path = project_dir / "config" / ".files.local-demo.json"
    file_map_path.write_text(
        json.dumps(
            {"demo-file": str((project_dir / "demo-data" / "example.txt").resolve())}
        ),
        encoding="utf-8",
    )
    settings = Settings(
        redis_url="redis://in-memory-demo",
        prefix="local_demo",
        agent_id="local_machine_b",
        hmac_key=b"local-demo-hmac-key-change-in-production",
        encryption_key=base64.urlsafe_b64decode(
            b"MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY="
        ),
        api_token="demo-browser-token",
        download_dir=project_dir / "downloads",
        file_map_path=file_map_path,
        chunk_size=64 * 1024,
        data_ttl_seconds=3600,
        max_file_bytes=1024 * 1024 * 1024,
        audit_db_path=project_dir / ".demo-audit.sqlite3",
        snapshot_dir=project_dir / ".demo-snapshots",
        window_size=4,
        max_transfer_buffer_bytes=4 * 64 * 1024,
    )
    settings.download_dir.mkdir(parents=True, exist_ok=True)
    client = fakeredis.FakeRedis()
    agent = FileAgent(settings, client=client, consumer_name="local-demo-agent")
    agent_thread = threading.Thread(
        target=agent.run_forever,
        name="local-demo-agent",
        daemon=True,
    )
    agent_thread.start()

    logging.getLogger("redis-file-demo").info(
        "Open http://127.0.0.1:8080, file ID demo-file, token demo-browser-token"
    )
    try:
        serve(create_app(settings, client), host="127.0.0.1", port=8080)
    finally:
        file_map_path.unlink(missing_ok=True)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    main()
