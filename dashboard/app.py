"""Read-only Streamlit dashboard for the CTA pipeline."""
import os
import json
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
from dashboard.manifest_history import fetch_manifest_history, fetch_validation_history  # noqa: E402
from src.monitoring.health_check import collect_health  # noqa: E402

load_dotenv()


@st.cache_data(ttl=300, show_spinner=False)
def validation_history():
    return fetch_validation_history()


def plot_history(frame, columns, height=260):
    columns = [columns] if isinstance(columns, str) else columns
    chart = frame[["Hour", *columns]].copy()
    chart["Hour index"] = range(len(chart))
    ticks = sorted({round(index * (len(chart) - 1) / 4) for index in range(5)})
    labels = {str(index): chart["Hour"].iloc[index][5:] for index in ticks}
    st.vega_lite_chart(chart, {
        "height": height,
        "mark": {"type": "line", "strokeWidth": 2},
        "transform": [
            {"fold": columns, "as": ["Metric", "Value"]},
            {"filter": "isValid(datum.Value)"},
        ],
        "encoding": {
            "x": {"field": "Hour index", "type": "quantitative", "title": "Collection hour (Chicago)",
                  "scale": {"nice": False},
                  "axis": {"values": ticks, "labelExpr": "(" + json.dumps(labels) + ")[datum.value]",
                           "labelAngle": -25, "labelOverlap": True, "labelLimit": 120}},
            "y": {"field": "Value", "type": "quantitative", "title": None, "scale": {"zero": True}},
            "color": {"field": "Metric", "type": "nominal", "title": None,
                      "scale": {"scheme": "tableau10"}, "legend": {"orient": "bottom"}},
            "tooltip": [{"field": "Hour", "type": "nominal"},
                        {"field": "Metric", "type": "nominal"},
                        {"field": "Value", "type": "quantitative", "format": ".2f"}],
        },
    }, use_container_width=True)


st.set_page_config(page_title="CTA Pipeline Monitor", page_icon="🚇", layout="wide")
st.title("CTA Pipeline Monitor")
st.caption("Read-only operational view of the existing collection and recovery pipeline.")

if not os.getenv("DATABASE_URL"):
    st.error("DATABASE_URL is not configured for this Streamlit service.")
    st.stop()

if st.button("Refresh health data"):
    validation_history.clear()
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

st.subheader("Historical data checks")
try:
    quality = validation_history()
except Exception as exc:
    st.warning(f"Historical data checks unavailable ({type(exc).__name__}).")
else:
    if quality.empty:
        st.info("No historical snapshots available.")
    else:
        total_snapshots, invalid_rows, mismatches, unknown_flags = st.columns(4)
        total_snapshots.metric("Historical snapshots", f"{quality['Snapshots'].sum():,}")
        invalid_rows.metric("Invalid prediction rows", f"{quality['Invalid prediction rows'].sum():,}")
        mismatches.metric("Manifest mismatches", f"{quality['Manifest mismatches'].sum():,}")
        unknown_flags.metric("Unknown delay flags", f"{quality['Unknown delay flags'].sum():,}")
        st.caption("Source hours: America/Chicago. Nominal cadence: 12 snapshots per station per hour. Coverage excludes partial boundary hours.")
        coverage, consistency, source_fields = st.tabs(["Coverage", "Consistency", "Source fields"])
        with coverage:
            st.write("Snapshots collected versus nominal cadence")
            plot_history(quality, ["Snapshots", "Nominal snapshots"], height=280)
            st.write("Hourly coverage and prediction volume")
            coverage_percent, prediction_volume = st.columns(2)
            with coverage_percent:
                plot_history(quality, "Coverage (%)", height=240)
            with prediction_volume:
                plot_history(quality, "Predictions", height=240)
        with consistency:
            st.write("Required fields, timing and numeric-range violations")
            plot_history(quality, "Invalid prediction rows")
            st.write("File processing and manifest reconciliation")
            plot_history(quality, ["Manifest mismatches", "Unprocessed files"])
        with source_fields:
            st.write("API-reported delays and unknown flags")
            plot_history(quality, ["Reported delayed (%)", "Unknown delay (%)"])
            st.write("Missing and zero-valued source coordinates")
            plot_history(quality, ["Missing GPS (%)", "Zero GPS (%)"])
        st.caption("Predictions are forecasts, not observed arrivals. Missing coordinates and unknown flags are retained; source fidelity does not prove forecast accuracy.")
        with st.expander("Hourly data"):
            st.dataframe(quality, use_container_width=True, hide_index=True)

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
