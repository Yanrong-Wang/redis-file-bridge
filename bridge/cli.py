from __future__ import annotations

import argparse
import base64
import os


def main() -> None:
    parser = argparse.ArgumentParser(description="Redis file bridge utilities")
    parser.add_argument(
        "command", choices=["generate-key", "generate-secret"]
    )
    args = parser.parse_args()
    if args.command == "generate-key":
        print(base64.urlsafe_b64encode(os.urandom(32)).decode("ascii"))
    elif args.command == "generate-secret":
        print(base64.urlsafe_b64encode(os.urandom(48)).decode("ascii"))


if __name__ == "__main__":
    main()
