from __future__ import annotations

import argparse
import socket


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", required=True, type=int)
    parser.add_argument("--timeout", default=2.0, type=float)
    args = parser.parse_args()
    with socket.create_connection((args.host, args.port), timeout=args.timeout):
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
