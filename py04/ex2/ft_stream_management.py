#!/usr/bin/env python3
import sys


def ft_stream_management() -> None:
    print("=== CYBER ARCHIVES - COMMUNICATION SYSTEM ===")
    print()
    arch_id = input("Input Stream active. Enter archivist ID: ")
    status = input("Input Stream active. Enter status report: ")
    print()
    sys.stdout.write(f"[STANDARD] Archive status from {arch_id}: {status}\n")
    sys.stderr.write(
        "[ALERT] System diagnostic: "
        "Communication channels verified\n")
    sys.stdout.write("[STANDARD] Data transmission complete\n")
    print()
    sys.stdout.write("Three-channel communication test successful.\n")


if __name__ == "__main__":
    ft_stream_management()
