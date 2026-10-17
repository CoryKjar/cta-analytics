"""Observe real post-cutover collector/recovery/health logs until explicit gates pass."""
import argparse
import json
import queue
import re
import subprocess
import threading
from datetime import datetime, timezone
from time import monotonic

from src.config.stations import STATIONS


def event_kind(service, message):
    if "Traceback" in message or "| ERROR |" in message or re.search(r"(?:parse_failures|download_failures|load_failures)\s*[:=]\s*[1-9]", message):
        return "error"
    if service == "cta-analytics" and "event=pipeline_completed" in message and "parse_failures=0" in message:
        values = {name: int(value) for name, value in re.findall(r"([a-z_]+)=(\d+)", message)}
        if values.get("uploaded_files") != len(STATIONS) or values.get("loaded_files", 0) + values.get("zero_arrival_files", 0) != len(STATIONS):
            return "error"
        return "collector"
    if service == "cta-monitor" and "Health check complete: issues=0" in message:
        return "monitor"
    if service == "cta-monitor" and ("HEALTH ISSUE" in message or re.search(r"issues=[1-9]", message)):
        return "error"
    if service == "cta-recovery" and "Unprocessed candidates skipped due to attempt cap: 0" in message:
        return "recovery"
    return None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--since", required=True, help="UTC instant after which events count")
    parser.add_argument("--timeout-minutes", type=int, default=30)
    args = parser.parse_args()
    since = datetime.fromisoformat(args.since.replace("Z", "+00:00"))
    if since.tzinfo is None:
        parser.error("The observation start must include a timezone.")
    events = queue.Queue()
    processes = []
    counts = {"collector": 0, "monitor": 0, "recovery": 0}
    seen = set()
    last_collector = None

    def follow(service, process):
        for line in process.stdout:
            try:
                row = json.loads(line)
                if isinstance(row, dict):
                    events.put((service, row))
            except json.JSONDecodeError:
                continue
        events.put((service, {"stream_closed": True}))

    try:
        for service in ("cta-analytics", "cta-monitor", "cta-recovery"):
            process = subprocess.Popen(
                ["railway", "logs", "--service", service, "--environment", "production", "--json"],
                stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True, encoding="utf-8", errors="replace",
            )
            processes.append(process)
            threading.Thread(target=follow, args=(service, process), daemon=True).start()
        deadline = monotonic() + args.timeout_minutes * 60
        print("Monitoring real runtime events after", since.isoformat(), flush=True)
        while monotonic() < deadline:
            try:
                service, row = events.get(timeout=min(30, max(0.1, deadline-monotonic())))
            except queue.Empty:
                print("Awaiting scheduled runtime events; observed", counts, flush=True)
                continue
            if row.get("stream_closed"):
                raise RuntimeError(f"{service} log stream closed before verification completed.")
            stamp = row.get("timestamp")
            if not stamp:
                continue
            moment = datetime.fromisoformat(stamp.replace("Z", "+00:00"))
            if moment < since:
                continue
            kind = event_kind(service, str(row.get("message", "")))
            if kind == "error":
                raise RuntimeError(f"Runtime error/health issue detected in {service} at {moment.isoformat()}; monitoring gate failed.")
            identity = (service, stamp, kind)
            if kind and identity not in seen:
                seen.add(identity)
                if kind == "collector":
                    if last_collector is not None and (moment-last_collector).total_seconds() > 480:
                        counts["collector"] = 0
                        print("Collector schedule gap resets the consecutive-run gate.", flush=True)
                    last_collector = moment
                counts[kind] += 1
                print(moment.isoformat(), kind, counts, flush=True)
            if counts["collector"] >= 3 and counts["monitor"] >= 2 and counts["recovery"] >= 1:
                print("RUNTIME MONITORING GATE PASSED:", json.dumps(counts), flush=True)
                return
        raise RuntimeError("Monitoring window expired before all runtime gates passed: " + json.dumps(counts))
    finally:
        for process in processes:
            process.terminate()
        for process in processes:
            process.wait(timeout=10)


if __name__ == "__main__":
    main()