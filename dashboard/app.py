"""Read-only Streamlit dashboard for the CTA pipeline."""
import os
import sys
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import streamlit as st
from dotenv import load_dotenv

# Ensure repository root is importable when Streamlit is launched from dashboard/.
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dashboard.manifest_status import build_manifest_status_view  # noqa: E402
from dashboard.manifest_history import fetch_manifest_history  # noqa: E402
from src.monitoring.health_check import collect_health  # noqa: E402

load_dotenv()

st.set_page_config(page_title="CTA Pipeline Monitor", page_icon="🚇", layout="wide")
st.title("CTA Pipeline Monitor")
st.caption("Read-only operational view of the existing collection and recovery pipeline.")

if not os.getenv("DATABASE_URL"):
    st.error("DATABASE_URL is not configured for this Streamlit service.")
    st.stop()

if st.button("Refresh health data"):
    st.rerun()

try:
    health = collect_health()
except Exception as exc:
    st.error(f"Unable to read pipeline health: {exc}")
    st.stop()

issues = health["issues"]
if issues:
    st.error(f"{len(issues)} health issue(s) detected")
    for name, description in issues.items():
        st.write(f"**{name.replace('_', ' ').title()}:** {description}")
else:
    st.success("No monitored issues detected")

latest = health["latest_snapshot"]
latest_text = (
    latest.astimezone(ZoneInfo("America/Chicago")).isoformat()
    if latest else "No processed snapshots"
)
age = health["age_minutes"]
age_text = f"{age:.1f} min" if age is not None else "N/A"
statuses = health["status_counts"]

c1, c2, c3, c4 = st.columns(4)
c1.metric("Latest processed snapshot (America/Chicago)", latest_text)
c2.metric("Snapshot age", age_text)
c3.metric("Manifest records updated in last 15 min", f"{health['recent_updates']:,}")
c4.metric("Row-count mismatches", f"{health['mismatch_count']:,}")
st.caption(
    "Recent updates count manifest-record updates, not confirmed collector runs; "
    "recovery or reconciliation can also update records."
)

st.subheader("Manifest snapshot files by collection hour")
st.caption(
    "Counts manifest files by source collection timestamp in America/Chicago, "
    "not collection runs, API requests, or arrival records."
)
try:
    history_frame = fetch_manifest_history()
except Exception as exc:
    st.warning(f"Unable to read collection history: {exc}")
else:
    if history_frame.empty or history_frame["Manifest files"].sum() == 0:
        st.info("No manifest snapshot files found in the last 24 hours.")
    else:
        st.bar_chart(history_frame)

st.subheader("Manifest status")
processed_count, status_frame, manifest_state = build_manifest_status_view(statuses)
st.metric("Processed manifest files", f"{processed_count:,}")
if manifest_state == "empty":
    st.info("No ingestion manifest rows found.")
elif manifest_state == "all_processed":
    st.success("All manifest files are marked processed.")
else:
    st.bar_chart(status_frame.set_index("Status"))

st.subheader("Files requiring attention")
rows = health["nonprocessed_rows"]
if rows:
    df = pd.DataFrame(
        rows,
        columns=["source_file", "status", "attempts", "last_error", "updated_at"],
    )
    st.dataframe(df, use_container_width=True, hide_index=True)
elif manifest_state == "empty":
    st.info("No manifest files to review.")
else:
    st.success("All manifest files are marked processed.")

st.subheader("Retry-cap exhaustion")
retry_rows = health["retry_rows"]
if retry_rows:
    df = pd.DataFrame(
        retry_rows,
        columns=["source_file", "status", "attempts", "last_error", "updated_at"],
    )
    st.dataframe(df, use_container_width=True, hide_index=True)
else:
    st.write("No non-processed files at the configured retry cap.")

st.caption(
    "This dashboard reads PostgreSQL only. It does not modify ingestion data, "
    "trigger collection, or run self-heal."
)
