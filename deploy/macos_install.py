from __future__ import annotations

import base64
import json
import os
import secrets
import shutil
import stat
import subprocess
import sys
from pathlib import Path
from urllib.parse import urlparse
from xml.sax.saxutils import escape


PROJECT_DIR = Path(__file__).resolve().parent.parent
DEPLOY_DIR = Path.home() / "Library" / "Application Support" / "RedisFileBridge"
RUNTIME_DIR = DEPLOY_DIR / "runtime"
LOG_DIR = RUNTIME_DIR / "logs"
ENV_PATH = DEPLOY_DIR / ".env"
FILE_MAP_PATH = DEPLOY_DIR / "config" / "files.json"
LAUNCH_AGENTS = Path.home() / "Library" / "LaunchAgents"
LABELS = (
    "com.redis-file-bridge.redis",
    "com.redis-file-bridge.agent",
    "com.redis-file-bridge.web",
)


def random_urlsafe(length: int = 48) -> str:
    return secrets.token_urlsafe(length)


def write_private(path: Path, text: str) -> None:
    path.write_text(text, encoding="utf-8")
    path.chmod(stat.S_IRUSR | stat.S_IWUSR)


def read_env(path: Path) -> dict[str, str]:
    if not path.is_file():
        return {}
    values: dict[str, str] = {}
    for line in path.read_text("utf-8").splitlines():
        if line and not line.lstrip().startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            values[key] = value
    return values


def plist(label: str, arguments: list[str]) -> str:
    arguments_xml = "\n".join(
        f"      <string>{escape(argument)}</string>" for argument in arguments
    )
    stdout = escape(str(LOG_DIR / f"{label}.log"))
    stderr = escape(str(LOG_DIR / f"{label}.error.log"))
    working_directory = escape(str(DEPLOY_DIR))
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
  <dict>
    <key>Label</key>
    <string>{label}</string>
    <key>ProgramArguments</key>
    <array>
{arguments_xml}
    </array>
    <key>WorkingDirectory</key>
    <string>{working_directory}</string>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>StandardOutPath</key>
    <string>{stdout}</string>
    <key>StandardErrorPath</key>
    <string>{stderr}</string>
    <key>ProcessType</key>
    <string>Background</string>
  </dict>
</plist>
"""


def main() -> None:
    redis_server = shutil.which("redis-server")
    if not redis_server:
        raise SystemExit("redis-server not found; install Redis first")

    DEPLOY_DIR.mkdir(parents=True, exist_ok=True)
    shutil.copytree(PROJECT_DIR / "bridge", DEPLOY_DIR / "bridge", dirs_exist_ok=True)
    shutil.copytree(
        PROJECT_DIR / "demo-data", DEPLOY_DIR / "demo-data", dirs_exist_ok=True
    )
    (DEPLOY_DIR / "config").mkdir(exist_ok=True)
    shutil.copy2(PROJECT_DIR / "requirements.txt", DEPLOY_DIR / "requirements.txt")
    shutil.copy2(PROJECT_DIR / "README.md", DEPLOY_DIR / "README.md")

    python = DEPLOY_DIR / ".venv" / "bin" / "python"
    if not python.is_file():
        subprocess.run(
            [sys.executable, "-m", "venv", str(DEPLOY_DIR / ".venv")],
            check=True,
        )
    subprocess.run(
        [
            str(python),
            "-m",
            "pip",
            "install",
            "--disable-pip-version-check",
            "-r",
            str(DEPLOY_DIR / "requirements.txt"),
        ],
        check=True,
    )

    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    (DEPLOY_DIR / "downloads").mkdir(exist_ok=True)
    LAUNCH_AGENTS.mkdir(parents=True, exist_ok=True)

    existing = read_env(ENV_PATH)
    existing_redis = existing.get("REDIS_URL", "")
    redis_password = urlparse(existing_redis).password or random_urlsafe()
    api_token = existing.get("BRIDGE_API_TOKEN") or random_urlsafe()
    hmac_key = existing.get("BRIDGE_HMAC_KEY") or random_urlsafe()
    encryption_key = existing.get("BRIDGE_ENCRYPTION_KEY") or base64.urlsafe_b64encode(
        secrets.token_bytes(32)
    ).decode("ascii")
    key_id = existing.get("BRIDGE_KEY_ID", "v1")
    keyring_path = DEPLOY_DIR / "config" / "keys.json"

    write_private(
        RUNTIME_DIR / "redis.conf",
        "\n".join(
            (
                "bind 127.0.0.1 ::1",
                "protected-mode yes",
                "port 6379",
                f"requirepass {redis_password}",
                f'dir "{RUNTIME_DIR}"',
                'dbfilename "redis-file-bridge.rdb"',
                "appendonly yes",
                'appenddirname "appendonly"',
                "maxmemory 1gb",
                "maxmemory-policy noeviction",
                "save 900 1",
                "",
            )
        ),
    )
    write_private(
        ENV_PATH,
        "\n".join(
            (
                f"REDIS_URL=redis://:{redis_password}@127.0.0.1:6379/0",
                "BRIDGE_PREFIX=broker_local",
                "BRIDGE_AGENT_ID=local_machine_b",
                f"BRIDGE_KEY_ID={key_id}",
                f"BRIDGE_HMAC_KEY={hmac_key}",
                f"BRIDGE_ENCRYPTION_KEY={encryption_key}",
                f"BRIDGE_KEYRING_PATH={keyring_path}",
                f"BRIDGE_API_TOKEN={api_token}",
                "BRIDGE_BIND_HOST=127.0.0.1",
                "BRIDGE_BIND_PORT=8080",
                f"BRIDGE_DOWNLOAD_DIR={DEPLOY_DIR / 'downloads'}",
                f"BRIDGE_AUDIT_DB={DEPLOY_DIR / 'audit.sqlite3'}",
                f"BRIDGE_FILE_MAP={FILE_MAP_PATH}",
                f"BRIDGE_SNAPSHOT_DIR={DEPLOY_DIR / 'snapshots'}",
                "BRIDGE_CHUNK_SIZE=1048576",
                "BRIDGE_WINDOW_SIZE=16",
                "BRIDGE_DATA_TTL_SECONDS=86400",
                "BRIDGE_MAX_FILE_BYTES=104857600",
                "BRIDGE_MAX_CONCURRENT_TRANSFERS=1",
                "BRIDGE_MAX_TRANSFER_BUFFER_BYTES=314572800",
                "BRIDGE_REDIS_MEMORY_WARN_PERCENT=70",
                "BRIDGE_REDIS_MEMORY_HARD_STOP_PERCENT=80",
                "BRIDGE_QUEUED_TIMEOUT_SECONDS=600",
                "BRIDGE_TRANSFER_TIMEOUT_SECONDS=1800",
                "BRIDGE_CHUNK_ACK_TIMEOUT_SECONDS=300",
                "",
            )
        ),
    )
    write_private(
        keyring_path,
        json.dumps(
            {
                key_id: {
                    "hmac": hmac_key,
                    "encryption": encryption_key,
                }
            },
            indent=2,
        )
        + "\n",
    )
    write_private(
        FILE_MAP_PATH,
        json.dumps(
            {
                "demo-file": {
                    "path": str(
                        (DEPLOY_DIR / "demo-data" / "example.txt").resolve()
                    ),
                    "max_file_bytes": 104857600,
                }
            },
            indent=2,
        )
        + "\n",
    )
    write_private(
        DEPLOY_DIR / "access.txt",
        "\n".join(
            (
                "URL=http://127.0.0.1:8080",
                "FILE_ID=demo-file",
                f"API_TOKEN={api_token}",
                "",
            )
        ),
    )

    definitions = {
        LABELS[0]: [redis_server, str(RUNTIME_DIR / "redis.conf")],
        LABELS[1]: [str(python), "-m", "bridge.agent"],
        LABELS[2]: [str(python), "-m", "bridge.web"],
    }
    domain = f"gui/{os.getuid()}"
    for label, arguments in definitions.items():
        path = LAUNCH_AGENTS / f"{label}.plist"
        path.write_text(plist(label, arguments), encoding="utf-8")
        subprocess.run(
            ["launchctl", "bootout", domain, str(path)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        subprocess.run(["launchctl", "bootstrap", domain, str(path)], check=True)

    print(f"Deployed: http://127.0.0.1:8080")
    print(f"Credentials: {DEPLOY_DIR / 'access.txt'}")


if __name__ == "__main__":
    main()
