#!/usr/bin/env python3
"""Physical two-host production-paper acceptance.

Run this script from a trusted operations machine after the predictor and
executor bundles have been installed. It uses the system ``ssh`` client and
Python's standard-library TLS stack. It never enables testnet/live and never
contacts Bybit private endpoints.
"""

from __future__ import annotations

import argparse
import json
import os
import shlex
import ssl
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


PREDICTOR_SERVICES = (
    "predictor-realtime.service",
    "control-plane-api.service",
    "market-collector.service",
    "publication-worker.service",
)
EXECUTOR_SERVICES = ("executor.service",)
PLACEHOLDER_MARKERS = ("<", "change-me", "replace-me", "placeholder")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def is_placeholder(value: str) -> bool:
    normalized = value.strip().lower()
    return not normalized or any(marker in normalized for marker in PLACEHOLDER_MARKERS)


@dataclass
class Check:
    name: str
    passed: bool
    detail: Any = None


@dataclass
class Report:
    schema_version: str = "physical-two-node-acceptance.v1"
    started_at: str = field(default_factory=utc_now)
    finished_at: str | None = None
    expected_sha: str = ""
    execution_mode: str = "paper"
    mainnet_allowed: bool = False
    physical_two_node_acceptance: str = "FAIL"
    checks: list[Check] = field(default_factory=list)
    evidence: dict[str, Any] = field(default_factory=dict)

    def add(self, name: str, passed: bool, detail: Any = None) -> None:
        self.checks.append(Check(name=name, passed=bool(passed), detail=detail))

    def finish(self) -> None:
        self.finished_at = utc_now()
        self.physical_two_node_acceptance = (
            "PASS" if self.checks and all(item.passed for item in self.checks) else "FAIL"
        )

    def payload(self) -> dict[str, Any]:
        data = asdict(self)
        data["checks"] = [asdict(item) for item in self.checks]
        return data


class CommandError(RuntimeError):
    pass


def run(
    command: list[str],
    *,
    timeout: int = 60,
    env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    completed = subprocess.run(
        command,
        capture_output=True,
        text=True,
        timeout=timeout,
        env=env,
    )
    if completed.returncode != 0:
        raise CommandError(
            f"command failed ({completed.returncode}): {shlex.join(command)}\n"
            f"stdout:\n{completed.stdout}\nstderr:\n{completed.stderr}"
        )
    return completed


def ssh(host: str, remote_command: str, *, timeout: int = 90) -> str:
    completed = run(
        [
            "ssh",
            "-o",
            "BatchMode=yes",
            "-o",
            "ConnectTimeout=10",
            "-o",
            "ServerAliveInterval=10",
            "-o",
            "ServerAliveCountMax=3",
            host,
            "bash",
            "-lc",
            remote_command,
        ],
        timeout=timeout,
    )
    return completed.stdout.strip()


def quote(value: str) -> str:
    return shlex.quote(value)


def remote_release_sha(host: str) -> str:
    command = r'''
set -euo pipefail
if [ -f /opt/ai-bybit/RELEASE_MANIFEST.json ]; then
  python3 - <<'PY'
import json
from pathlib import Path
payload=json.loads(Path("/opt/ai-bybit/RELEASE_MANIFEST.json").read_text(encoding="utf-8"))
print(payload.get("code_commit", ""))
PY
elif [ -d /opt/ai-bybit/.git ]; then
  git -C /opt/ai-bybit rev-parse HEAD
else
  echo "MISSING_RELEASE_ID"
fi
'''
    return ssh(host, command)


def remote_service_states(host: str, services: Iterable[str]) -> dict[str, str]:
    names = " ".join(quote(service) for service in services)
    command = f'''
set -u
for service in {names}; do
  state="$(systemctl is-active "$service" 2>/dev/null || true)"
  printf '%s=%s\n' "$service" "${{state:-unknown}}"
done
'''
    output = ssh(host, command)
    result: dict[str, str] = {}
    for line in output.splitlines():
        if "=" in line:
            key, value = line.split("=", 1)
            result[key] = value
    return result


def remote_preflight(host: str, role: str) -> dict[str, Any]:
    if role == "predictor":
        env_file = "/etc/ai-bybit/predictor-production-paper.env"
        cwd = "/opt/ai-bybit/ai_bot3/ai_bot3"
        entry = "scripts/preflight_production_predictor.py"
        user = "ai-bybit"
    else:
        env_file = "/etc/ai-bybit/executor-production-paper.env"
        cwd = "/opt/ai-bybit/BybitContractBotV4"
        entry = "main.py --preflight"
        user = "ai-bybit-executor"
    command = f'''
set -euo pipefail
test -f {quote(env_file)}
set -a
. {quote(env_file)}
set +a
cd {quote(cwd)}
sudo -u {quote(user)} -E /opt/ai-bybit/.venv/bin/python {entry}
'''
    output = ssh(host, command, timeout=180)
    return json.loads(output.splitlines()[-1])


def remote_ntp(host: str) -> dict[str, Any]:
    command = r'''
set -euo pipefail
python3 - <<'PY'
import json, subprocess, time
def read(name):
    p=subprocess.run(["timedatectl","show","-p",name,"--value"],capture_output=True,text=True)
    return p.stdout.strip() if p.returncode == 0 else ""
print(json.dumps({
  "ntp_synchronized": read("NTPSynchronized"),
  "timezone": read("Timezone"),
  "unix_time": time.time(),
}))
PY
'''
    return json.loads(ssh(host, command))


def remote_storage(host: str, paths: Iterable[str]) -> dict[str, Any]:
    encoded = json.dumps(list(paths))
    command = f'''
set -euo pipefail
python3 - <<'PY'
import json, os, shutil, subprocess
paths=json.loads({encoded!r})
rows=[]
for path in paths:
    parent=path if os.path.exists(path) else os.path.dirname(path)
    usage=shutil.disk_usage(parent or "/")
    probe=subprocess.run(["findmnt","-T",parent or "/","-n","-o","FSTYPE,SOURCE,TARGET"],
                         capture_output=True,text=True)
    rows.append({{
      "path": path,
      "exists": os.path.exists(path),
      "free_bytes": usage.free,
      "mount": probe.stdout.strip(),
    }})
print(json.dumps(rows))
PY
'''
    return {"paths": json.loads(ssh(host, command))}


def http_json(
    url: str,
    *,
    token: str,
    consumer_id: str,
    ca_bundle: Path,
    client_cert: Path,
    client_key: Path,
    timeout: int = 20,
) -> dict[str, Any]:
    context = ssl.create_default_context(cafile=str(ca_bundle))
    context.load_cert_chain(certfile=str(client_cert), keyfile=str(client_key))
    request = urllib.request.Request(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "X-Executor-Consumer-ID": consumer_id,
            "Accept": "application/json",
        },
    )
    with urllib.request.urlopen(request, context=context, timeout=timeout) as response:
        payload = response.read().decode("utf-8")
        if response.status != 200:
            raise RuntimeError(f"{url} returned HTTP {response.status}: {payload}")
        return json.loads(payload)


def control_snapshot(args: argparse.Namespace) -> dict[str, Any]:
    base = args.control_url.rstrip("/")
    result: dict[str, Any] = {}
    for endpoint in (
        "/v1/health/live",
        "/v1/health/ready",
        "/v1/health/dependencies",
        "/v1/capabilities",
        "/v1/time",
    ):
        result[endpoint] = http_json(
            base + endpoint,
            token=args.control_token,
            consumer_id=args.consumer_id,
            ca_bundle=args.ca_bundle,
            client_cert=args.client_cert,
            client_key=args.client_key,
        )
    return result


def latest_forecast(args: argparse.Namespace) -> dict[str, Any] | None:
    url = args.control_url.rstrip("/") + "/v1/forecasts/latest"
    if args.symbol:
        url += "?symbol=" + urllib.parse.quote(args.symbol)
    try:
        return http_json(
            url,
            token=args.control_token,
            consumer_id=args.consumer_id,
            ca_bundle=args.ca_bundle,
            client_cert=args.client_cert,
            client_key=args.client_key,
        )
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return None
        raise


def forecast_marker(payload: dict[str, Any] | None) -> tuple[Any, ...]:
    if not payload:
        return ()
    time_payload = payload.get("time") if isinstance(payload.get("time"), dict) else {}
    return (
        payload.get("forecast_id"),
        payload.get("revision"),
        time_payload.get("created_at"),
        payload.get("generated_at"),
    )


def stop_service(host: str, service: str) -> None:
    ssh(host, f"sudo systemctl stop {quote(service)}", timeout=60)


def start_service(host: str, service: str) -> None:
    ssh(host, f"sudo systemctl start {quote(service)}", timeout=60)


def wait_active(host: str, service: str, timeout: int = 90) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        state = ssh(host, f"systemctl is-active {quote(service)} 2>/dev/null || true")
        if state.strip() == "active":
            return True
        time.sleep(3)
    return False


def ensure_local_inputs(args: argparse.Namespace) -> None:
    if is_placeholder(args.control_token):
        raise SystemExit("CONTROL_PLANE token is missing or a placeholder")
    for path in (args.ca_bundle, args.client_cert, args.client_key):
        if not path.is_file():
            raise SystemExit(f"required local TLS file does not exist: {path}")
    if not args.expected_sha or len(args.expected_sha) < 12:
        raise SystemExit("--expected-sha must be an exact Git SHA")
    if not args.control_url.lower().startswith("https://"):
        raise SystemExit("--control-url must use HTTPS")
    run(["ssh", "-V"], timeout=10)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate two physical production-paper hosts"
    )
    parser.add_argument("--predictor-ssh", required=True, help="user@predictor-host")
    parser.add_argument("--executor-ssh", required=True, help="user@executor-host")
    parser.add_argument("--expected-sha", required=True)
    parser.add_argument("--control-url", required=True)
    parser.add_argument("--consumer-id", required=True)
    parser.add_argument(
        "--control-token",
        default=os.environ.get("CONTROL_PLANE_API_TOKEN", ""),
        help="prefer CONTROL_PLANE_API_TOKEN environment variable",
    )
    parser.add_argument("--ca-bundle", type=Path, required=True)
    parser.add_argument("--client-cert", type=Path, required=True)
    parser.add_argument("--client-key", type=Path, required=True)
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--outage-seconds", type=int, default=240)
    parser.add_argument(
        "--exercise-outage",
        action="store_true",
        help="stop and restart executor.service while proving predictor continuity",
    )
    parser.add_argument(
        "--require-new-forecast",
        action="store_true",
        help="fail outage test unless the forecast marker changes",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("physical-two-node-acceptance.json"),
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    ensure_local_inputs(args)
    report = Report(expected_sha=args.expected_sha)

    try:
        predictor_sha = remote_release_sha(args.predictor_ssh)
        executor_sha = remote_release_sha(args.executor_ssh)
        report.evidence["release_sha"] = {
            "predictor": predictor_sha,
            "executor": executor_sha,
        }
        report.add(
            "exact_release_sha_on_both_hosts",
            predictor_sha == executor_sha == args.expected_sha,
            report.evidence["release_sha"],
        )

        predictor_preflight = remote_preflight(args.predictor_ssh, "predictor")
        executor_preflight = remote_preflight(args.executor_ssh, "executor")
        report.evidence["preflight"] = {
            "predictor": predictor_preflight,
            "executor": executor_preflight,
        }
        report.add(
            "predictor_preflight",
            predictor_preflight.get("status") == "PASS"
            and predictor_preflight.get("execution_mode") == "paper"
            and not predictor_preflight.get("mainnet_allowed", False),
            predictor_preflight,
        )
        report.add(
            "executor_preflight",
            executor_preflight.get("status") == "PASS"
            and executor_preflight.get("execution_mode") == "paper"
            and not executor_preflight.get("mainnet_enabled", False)
            and not executor_preflight.get("private_exchange_credentials_present", False),
            executor_preflight,
        )

        predictor_states = remote_service_states(args.predictor_ssh, PREDICTOR_SERVICES)
        executor_states = remote_service_states(args.executor_ssh, EXECUTOR_SERVICES)
        report.evidence["services"] = {
            "predictor": predictor_states,
            "executor": executor_states,
        }
        report.add(
            "predictor_services_active",
            all(predictor_states.get(name) == "active" for name in PREDICTOR_SERVICES),
            predictor_states,
        )
        report.add(
            "executor_service_active",
            executor_states.get("executor.service") == "active",
            executor_states,
        )

        predictor_ntp = remote_ntp(args.predictor_ssh)
        executor_ntp = remote_ntp(args.executor_ssh)
        host_skew = abs(float(predictor_ntp["unix_time"]) - float(executor_ntp["unix_time"]))
        report.evidence["clock"] = {
            "predictor": predictor_ntp,
            "executor": executor_ntp,
            "host_skew_seconds": host_skew,
        }
        report.add(
            "host_clocks_synchronized",
            predictor_ntp.get("ntp_synchronized", "").lower() == "yes"
            and executor_ntp.get("ntp_synchronized", "").lower() == "yes"
            and host_skew <= 5,
            report.evidence["clock"],
        )

        predictor_storage = remote_storage(
            args.predictor_ssh,
            (
                "/var/lib/ai-bybit/predictor-realtime",
                "/var/lib/ai-bybit/publication-worker",
                "/var/lib/ai-bybit/control-plane",
                "/var/lib/ai-bybit/market-collector",
            ),
        )
        executor_storage = remote_storage(
            args.executor_ssh,
            ("/var/lib/ai-bybit/executor",),
        )
        report.evidence["storage"] = {
            "predictor": predictor_storage,
            "executor": executor_storage,
        }
        forbidden_network_fs = ("nfs", "cifs", "smb", "fuse.sshfs")
        mounts = [
            row.get("mount", "").lower()
            for group in (predictor_storage, executor_storage)
            for row in group["paths"]
        ]
        report.add(
            "sqlite_storage_is_host_local",
            not any(any(token in mount for token in forbidden_network_fs) for mount in mounts),
            mounts,
        )
        report.add(
            "minimum_free_disk_1gib",
            all(
                int(row.get("free_bytes", 0)) >= 1024**3
                for group in (predictor_storage, executor_storage)
                for row in group["paths"]
            ),
            report.evidence["storage"],
        )

        snapshot = control_snapshot(args)
        report.evidence["control_plane"] = snapshot
        ready = snapshot["/v1/health/ready"]
        capabilities = snapshot["/v1/capabilities"]
        server_time = snapshot["/v1/time"]
        local_time = time.time()
        server_unix = float(server_time.get("unix_time", 0))
        report.add(
            "control_plane_ready_over_mtls",
            ready.get("status") == "ready"
            and ready.get("execution_mode") == "paper"
            and capabilities.get("cluster_id") == executor_preflight.get("cluster_id")
            and capabilities.get("deployment_id") == executor_preflight.get("deployment_id"),
            {"ready": ready, "capabilities": capabilities},
        )
        report.add(
            "control_plane_clock_skew_within_5_seconds",
            abs(local_time - server_unix) <= 5,
            {"local_unix": local_time, "server_unix": server_unix},
        )

        if args.exercise_outage:
            before = latest_forecast(args)
            before_marker = forecast_marker(before)
            stop_service(args.executor_ssh, "executor.service")
            try:
                stopped = remote_service_states(
                    args.executor_ssh, EXECUTOR_SERVICES
                ).get("executor.service")
                report.add(
                    "executor_was_stopped_for_outage_test",
                    stopped in {"inactive", "failed", "deactivating"},
                    {"state": stopped},
                )
                time.sleep(max(10, args.outage_seconds))
                during = control_snapshot(args)
                after = latest_forecast(args)
                after_marker = forecast_marker(after)
                report.evidence["executor_outage"] = {
                    "seconds": args.outage_seconds,
                    "before_forecast": before_marker,
                    "after_forecast": after_marker,
                    "control_ready_during_outage": during["/v1/health/ready"],
                }
                continuity = during["/v1/health/live"].get("status") == "live"
                if args.require_new_forecast:
                    continuity = continuity and bool(after_marker) and after_marker != before_marker
                report.add(
                    "predictor_continues_while_executor_offline",
                    continuity,
                    report.evidence["executor_outage"],
                )
            finally:
                start_service(args.executor_ssh, "executor.service")
            report.add(
                "executor_recovers_after_outage",
                wait_active(args.executor_ssh, "executor.service"),
                remote_service_states(args.executor_ssh, EXECUTOR_SERVICES),
            )
        else:
            report.add(
                "physical_executor_outage_test",
                False,
                "GATED: rerun with --exercise-outage on paper-only hosts",
            )

    except Exception as exc:
        report.add("acceptance_runtime", False, f"{type(exc).__name__}: {exc}")

    report.finish()
    payload = report.payload()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0 if report.physical_two_node_acceptance == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
