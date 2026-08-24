#!/usr/bin/env python3
"""Spawn one zkcode service in a new process session and print its PID."""

from __future__ import annotations

import argparse
import subprocess
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--working-directory", required=True, type=Path)
    parser.add_argument("--log", required=True, type=Path)
    parser.add_argument("program", type=Path)
    args = parser.parse_args()

    args.log.parent.mkdir(parents=True, exist_ok=True)
    with args.log.open("ab", buffering=0) as log_handle:
        process = subprocess.Popen(
            [str(args.program.resolve())],
            cwd=args.working_directory.resolve(),
            stdin=subprocess.DEVNULL,
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            close_fds=True,
            start_new_session=True,
        )
    print(process.pid)


if __name__ == "__main__":
    main()
