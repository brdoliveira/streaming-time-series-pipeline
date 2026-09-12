from __future__ import annotations

import argparse
import csv
import hashlib
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path


EXPECTED_QUERIES = {
    ("5min", "raw"),
    ("1h", "raw"),
    ("24h", "raw"),
    ("5min", "cagg"),
    ("1h", "cagg"),
    ("24h", "cagg"),
}


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest().upper()


def tree_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    for file_path in sorted(item for item in path.rglob("*") if item.is_file()):
        relative = file_path.relative_to(path).as_posix().encode("utf-8")
        digest.update(len(relative).to_bytes(4, "big"))
        digest.update(relative)
        digest.update(bytes.fromhex(sha256(file_path)))
    return digest.hexdigest().upper()


def git_value(project_root: Path, *args: str) -> str | None:
    result = subprocess.run(
        ["git", "-C", str(project_root), *args],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    return result.stdout.strip() or None


def audit_run(run_dir: Path, project_root: Path) -> dict[str, object]:
    required = {
        "metadata.json",
        "latency-by-scenario.csv",
        "throughput-by-scenario.csv",
        "query-response-times.csv",
        "docker-stats-samples.csv",
        "docker-stats-samples.csv.status.json",
        "producer.log",
        "run.log",
    }
    available = {item.name for item in run_dir.iterdir() if item.is_file()}
    missing = sorted(required - available)
    if missing:
        return {
            "run": run_dir.relative_to(project_root).as_posix(),
            "valid": False,
            "errors": [f"missing: {name}" for name in missing],
        }

    metadata = json.loads((run_dir / "metadata.json").read_text(encoding="utf-8-sig"))
    scenario = str(metadata["scenario"])
    expected_count = int(metadata["rate_per_second"]) * int(metadata["duration_seconds"])

    latency = [row for row in read_csv(run_dir / "latency-by-scenario.csv") if row["scenario"] == scenario]
    throughput = [row for row in read_csv(run_dir / "throughput-by-scenario.csv") if row["scenario"] == scenario]
    query_rows = read_csv(run_dir / "query-response-times.csv")
    resource_rows = read_csv(run_dir / "docker-stats-samples.csv")
    resource_status = json.loads(
        (run_dir / "docker-stats-samples.csv.status.json").read_text(encoding="utf-8-sig")
    )

    errors: list[str] = []
    if len(latency) != 1:
        errors.append(f"latency rows for {scenario}: {len(latency)}")
    if len(throughput) != 1:
        errors.append(f"throughput rows for {scenario}: {len(throughput)}")

    latency_count = int(latency[0]["total_events"]) if len(latency) == 1 else None
    throughput_count = int(throughput[0]["total_events"]) if len(throughput) == 1 else None
    if latency_count != expected_count:
        errors.append(f"latency count: expected {expected_count}, found {latency_count}")
    if throughput_count != expected_count:
        errors.append(f"throughput count: expected {expected_count}, found {throughput_count}")

    query_keys = {(row["window"], row["type"]) for row in query_rows if row["scenario"] == scenario}
    if query_keys != EXPECTED_QUERIES:
        errors.append(f"query set differs: {sorted(query_keys)}")

    if resource_status.get("status") != "success" or int(resource_status.get("exit_code", -1)) != 0:
        errors.append("resource collector did not finish successfully")
    if int(resource_status.get("docker_sample_count", -1)) != len(resource_rows):
        errors.append("resource status count differs from docker-stats-samples.csv")
    if int(metadata.get("resource_sample_count", -1)) != len(resource_rows):
        errors.append("metadata resource count differs from docker-stats-samples.csv")
    if metadata.get("resource_collection_status") != "success":
        errors.append("metadata reports invalid resource collection")

    return {
        "run": run_dir.relative_to(project_root).as_posix(),
        "repetition": int(run_dir.parent.name.removeprefix("rep-")),
        "scenario": scenario,
        "rate_per_second": int(metadata["rate_per_second"]),
        "duration_seconds": int(metadata["duration_seconds"]),
        "expected_event_count": expected_count,
        "observed_event_count": latency_count,
        "valid_event_count": latency_count == expected_count == throughput_count,
        "resource_sample_count": len(resource_rows),
        "query_measurement_count": len(query_rows),
        "started_at": metadata["started_at"],
        "completed_at": metadata["completed_at"],
        "tree_sha256": tree_sha256(run_dir),
        "valid": not errors,
        "errors": errors,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit the nine repeated TCC scenario runs without modifying them.")
    parser.add_argument("--root", type=Path, default=Path("scripts/experiments/results-repeated"))
    parser.add_argument(
        "--out",
        type=Path,
        default=Path("docs/experiments/data/repeated-results-audit-2026-09-12.json"),
    )
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parents[2]
    root = args.root if args.root.is_absolute() else project_root / args.root
    output = args.out if args.out.is_absolute() else project_root / args.out

    run_dirs = sorted(
        run_dir
        for repetition in root.glob("rep-*")
        for run_dir in repetition.iterdir()
        if run_dir.is_dir() and not run_dir.name.startswith("summary-")
    )
    audits = [audit_run(run_dir, project_root) for run_dir in run_dirs]
    scenario_counts = {
        scenario: sum(1 for audit in audits if audit.get("scenario") == scenario)
        for scenario in ("low", "medium", "high")
    }
    all_valid = len(audits) == 9 and scenario_counts == {"low": 3, "medium": 3, "high": 3} and all(
        bool(audit["valid"]) for audit in audits
    )

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "project_commit": git_value(project_root, "rev-parse", "HEAD"),
        "remote_url": git_value(project_root, "remote", "get-url", "origin"),
        "audit_scope": "read-only validation of raw artifacts; no historical result was modified",
        "run_count": len(audits),
        "scenario_counts": scenario_counts,
        "all_valid": all_valid,
        "runs": audits,
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"output": output.as_posix(), "all_valid": all_valid, "run_count": len(audits)}))
    return 0 if all_valid else 1


if __name__ == "__main__":
    raise SystemExit(main())
