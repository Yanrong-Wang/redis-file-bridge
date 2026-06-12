from __future__ import annotations

import logging
import os
from functools import wraps
from pathlib import Path
from typing import Any, Callable, Optional, TypeVar

from flask import Flask, jsonify, render_template, request, send_file
from redis import Redis

from .audit import AuditStore
from .config import Settings
from .protocol import (
    READY_STATES,
    RedisKeys,
    build_download_ticket,
    build_task,
    now_seconds,
    require_safe_id,
    verify_download_ticket,
)
from .receiver import LocalReceiver
from .store import BridgeStore


LOGGER = logging.getLogger("redis-file-web")
View = TypeVar("View", bound=Callable[..., Any])


def create_app(
    settings: Optional[Settings] = None, client: Optional[Redis] = None
) -> Flask:
    settings = settings or Settings.from_env()
    settings.validate()
    redis_client = client or Redis.from_url(settings.redis_url)
    keys = RedisKeys(settings.prefix, settings.agent_id)
    store = BridgeStore(redis_client, keys, settings.data_ttl_seconds)
    audit = AuditStore(settings.audit_db_path)
    receiver = LocalReceiver(settings, store, audit)
    settings.download_dir.mkdir(parents=True, exist_ok=True)

    app = Flask(__name__)
    app.config.update(
        SETTINGS=settings,
        STORE=store,
        AUDIT=audit,
        RECEIVER=receiver,
    )

    def require_token(view: View) -> View:
        @wraps(view)
        def wrapped(*args: Any, **kwargs: Any) -> Any:
            supplied = request.headers.get("Authorization", "")
            if settings.api_token and supplied != f"Bearer {settings.api_token}":
                return jsonify({"error": "unauthorized"}), 401
            return view(*args, **kwargs)

        return wrapped  # type: ignore[return-value]

    @app.get("/")
    def index() -> str:
        return render_template("index.html", agent_id=settings.agent_id)

    @app.get("/readme")
    def readme() -> Any:
        readme_path = Path(__file__).resolve().parent.parent / "README.md"
        if not readme_path.is_file():
            return jsonify({"error": "README.md not found"}), 404
        return send_file(readme_path, mimetype="text/markdown; charset=utf-8")

    @app.get("/health")
    def health() -> Any:
        try:
            redis_client.ping()
            heartbeat = redis_client.get(keys.heartbeat)
            memory_percent = store.redis_memory_percent()
            return jsonify(
                {
                    "ok": True,
                    "redis": "connected",
                    "agent_online": heartbeat is not None,
                    "redis_memory_percent": memory_percent,
                    "transfer_buffer_bytes": store.buffer_bytes(),
                }
            )
        except Exception as exc:
            return jsonify({"ok": False, "error": str(exc)}), 503

    @app.post("/api/tasks")
    @require_token
    def create_task() -> Any:
        payload = request.get_json(silent=True) or {}
        file_id = str(payload.get("file_id", "")).strip()
        requester = (
            request.headers.get("X-Requester", "").strip()
            or request.remote_addr
            or "unknown"
        )[:200]
        try:
            require_safe_id(file_id, "file_id")
            hmac_key, _ = settings.keys_for(settings.key_id)
            task = build_task(
                settings.agent_id, file_id, settings.key_id, hmac_key
            )
            audit.create(
                task["task_id"],
                settings.prefix,
                settings.agent_id,
                file_id,
                requester,
                int(task["created_at"]),
            )
            message_id = store.enqueue(task)
            receiver.start(task["task_id"])
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400
        return jsonify(
            {
                "task_id": task["task_id"],
                "message_id": message_id,
                "state": "queued",
            }
        ), 202

    @app.get("/api/tasks")
    @require_token
    def recent_tasks() -> Any:
        try:
            limit = int(request.args.get("limit", "50"))
        except ValueError:
            return jsonify({"error": "limit must be an integer"}), 400
        return jsonify({"tasks": audit.recent(limit)})

    @app.get("/api/tasks/<task_id>")
    @require_token
    def task_status(task_id: str) -> Any:
        try:
            require_safe_id(task_id, "task_id")
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400
        local = audit.get(task_id)
        remote = store.get_status(task_id)
        if not local and not remote:
            return jsonify({"error": "task not found"}), 404
        response = dict(remote)
        response.update(
            {
                key: value
                for key, value in local.items()
                if value is not None and key != "local_path"
            }
        )
        response["download_ready"] = local.get("status") in READY_STATES
        response["state"] = local.get("status") or remote.get("state")
        return jsonify(response)

    @app.post("/api/tasks/<task_id>/cancel")
    @require_token
    def cancel(task_id: str) -> Any:
        try:
            require_safe_id(task_id, "task_id")
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400
        if not audit.get(task_id):
            return jsonify({"error": "task not found"}), 404
        store.request_cancel(task_id)
        audit.transition(task_id, "cancelled", error_message="cancelled by requester")
        return jsonify({"task_id": task_id, "state": "cancelled"})

    @app.post("/api/tasks/<task_id>/download-ticket")
    @require_token
    def create_download_ticket(task_id: str) -> Any:
        try:
            require_safe_id(task_id, "task_id")
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400
        task = audit.get(task_id)
        if not task:
            return jsonify({"error": "task not found"}), 404
        if task.get("status") not in READY_STATES:
            return jsonify({"error": "local file is not ready"}), 409
        path = Path(task.get("local_path") or "")
        if not path.is_file():
            return jsonify({"error": "stored local file is missing"}), 410
        ticket = build_download_ticket(task_id, settings.hmac_key, ttl=60)
        return jsonify(
            {
                "download_url": f"/download/{ticket}",
                "expires_in": 60,
                "filename": _original_name(path),
            }
        )

    @app.get("/download/<ticket>")
    def download(ticket: str) -> Any:
        try:
            payload = verify_download_ticket(ticket, settings.hmac_key)
            task_id = str(payload["task_id"])
            remaining = max(1, int(payload["expires_at"]) - now_seconds())
            if not store.consume_nonce(
                str(payload["nonce"]), "download", remaining
            ):
                return jsonify({"error": "download ticket has already been used"}), 410
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400
        task = audit.get(task_id)
        if not task or task.get("status") not in READY_STATES:
            return jsonify({"error": "local file is not ready"}), 409
        path = Path(task.get("local_path") or "")
        if not path.is_file():
            return jsonify({"error": "stored local file is missing"}), 410
        audit.transition(task_id, "downloaded")
        store.set_status(
            task_id,
            "downloaded",
            local_filename=path.name,
        )
        return send_file(path, as_attachment=True, download_name=_original_name(path))

    receiver.recover()
    return app


def _original_name(path: Path) -> str:
    name = path.name
    if len(name) > 37 and name[36] == "-":
        return name[37:]
    return name


def main() -> None:
    from waitress import serve

    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    settings = Settings.from_env()
    serve(
        create_app(settings),
        host=os.getenv("BRIDGE_BIND_HOST", "127.0.0.1"),
        port=int(os.getenv("BRIDGE_BIND_PORT", "8080")),
    )


if __name__ == "__main__":
    main()
