from __future__ import annotations

import os
import subprocess
from pathlib import Path


LABELS = (
    "com.redis-file-bridge.web",
    "com.redis-file-bridge.agent",
    "com.redis-file-bridge.redis",
)
LAUNCH_AGENTS = Path.home() / "Library" / "LaunchAgents"


def main() -> None:
    domain = f"gui/{os.getuid()}"
    for label in LABELS:
        path = LAUNCH_AGENTS / f"{label}.plist"
        subprocess.run(
            ["launchctl", "bootout", domain, str(path)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        path.unlink(missing_ok=True)
    print("Redis File Bridge launch agents removed.")


if __name__ == "__main__":
    main()

