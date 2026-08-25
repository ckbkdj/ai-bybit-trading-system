from __future__ import annotations

import argparse
import json
import multiprocessing
import time
from pathlib import Path


def _bounded_cpu_pressure(stop: multiprocessing.Event) -> None:
    value = 1
    while not stop.is_set():
        value = (value * 1_103_515_245 + 12_345) & 0x7FFFFFFF


def run_probe(
    *, duration_seconds: float,
    cadence_seconds: float,
    max_jitter_seconds: float,
    pressure_workers: int,
) -> dict:
    if duration_seconds <= 0 or cadence_seconds <= 0 or max_jitter_seconds <= 0:
        raise ValueError("probe timing must be positive")
    if not 1 <= pressure_workers <= 4:
        raise ValueError("pressure_workers must be between 1 and 4")
    stop = multiprocessing.Event()
    workers = [
        multiprocessing.Process(target=_bounded_cpu_pressure, args=(stop,), daemon=True)
        for _ in range(pressure_workers)
    ]
    lateness = []
    started = time.monotonic()
    deadline = started
    try:
        for worker in workers:
            worker.start()
        while time.monotonic() - started < duration_seconds:
            deadline += cadence_seconds
            time.sleep(max(0.0, deadline - time.monotonic()))
            lateness.append(max(0.0, time.monotonic() - deadline))
    finally:
        stop.set()
        for worker in workers:
            worker.join(timeout=5)
        for worker in workers:
            if worker.is_alive():
                worker.terminate()
                worker.join(timeout=5)
    maximum = max(lateness, default=0.0)
    return {
        "status": "PASS" if maximum <= max_jitter_seconds else "FAIL",
        "duration_seconds": duration_seconds,
        "cadence_seconds": cadence_seconds,
        "samples": len(lateness),
        "max_jitter_seconds": maximum,
        "jitter_slo_seconds": max_jitter_seconds,
        "pressure_workers": pressure_workers,
        "background_processes_remaining": sum(worker.is_alive() for worker in workers),
        "workload": "bounded synthetic CPU pressure; no model training or backfill",
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--duration-seconds", type=float, default=2.0)
    parser.add_argument("--cadence-ms", type=float, default=50.0)
    parser.add_argument("--max-jitter-ms", type=float, default=500.0)
    parser.add_argument("--pressure-workers", type=int, default=1)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    report = run_probe(
        duration_seconds=args.duration_seconds,
        cadence_seconds=args.cadence_ms / 1000.0,
        max_jitter_seconds=args.max_jitter_ms / 1000.0,
        pressure_workers=args.pressure_workers,
    )
    text = json.dumps(report, indent=2, sort_keys=True)
    if args.output:
        args.output.write_text(text + "\n", encoding="utf-8")
    print(text)
    return 0 if report["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
