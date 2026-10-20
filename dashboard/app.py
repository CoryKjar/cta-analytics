"""Read-only CTA collection, forecast-behavior and data-quality dashboard."""
import json
import os
import sys
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import streamlit as st
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dashboard.manifest_history import fetch_dashboard_history  # noqa: E402
from dashboard.manifest_status import build_manifest_status_view  # noqa: E402
from src.monitoring.health_check import collect_health  # noqa: E402

load_dotenv()


@st.cache_data(ttl=300, show_spinner=False)
def monitoring_data(window_hours):
    return fetch_dashboard_history(window_hours)


def line_chart(frame, columns, *, height=270, percent=False):
    columns = [columns] if isinstance(columns, str) else columns
    chart = frame[["Hour", *columns]].copy()
    chart["Hour index"] = range(len(chart))
    if chart.empty:
        st.info("No observations in this time window.")
        return
    ticks = sorted({round(index * (len(chart) - 1) / 4) for index in range(5)})
    labels = {str(index): chart["Hour"].iloc[index][5:] for index in ticks}
    st.vega_lite_chart(
        chart,
        {
            "height": height,
            "mark": {"type": "line", "strokeWidth": 2, "point": len(chart) <= 48},
            "transform": [
                {"fold": columns, "as": ["Metric", "Value"]},
                {"filter": "isValid(datum.Value)"},
            ],
            "encoding": {
                "x": {
                    "field": "Hour index", "type": "quantitative",
                    "title": "Collection hour (Chicago)", "scale": {"nice": False},
                    "axis": {
                        "values": ticks,
                        "labelExpr": "(" + json.dumps(labels) + ")[datum.value]",
                        "labelAngle": -25, "labelOverlap": True, "labelLimit": 120,
                    },
                },
                "y": {
                    "field": "Value", "type": "quantitative", "title": None,
                    "scale": {"zero": not percent},
                    **({"axis": {"labelExpr": "datum.value + '%'"}} if percent else {}),
                },
                "color": {
                    "field": "Metric", "type": "nominal", "title": None,
                    "scale": {"scheme": "tableau10"},
                    "legend": {"orient": "bottom"},
                },
                "tooltip": [
                    {"field": "Hour", "type": "nominal"},
                    {"field": "Metric", "type": "nominal"},
                    {"field": "Value", "type": "quantitative", "format": ".2f"},
                ],
            },
            "config": {
                "background": "#172428",
                "view": {"stroke": "#30464b", "strokeWidth": 1},
                "axis": {"labelColor": "#b8c8c6", "titleColor": "#a8bcba", "gridColor": "#2a3c41"},
                "legend": {"labelColor": "#cbd8d6", "titleColor": "#a8bcba"},
            },
        },
        use_container_width=True,
    )


def bar_chart(frame, category, columns, *, height=300, horizontal=True):
    columns = [columns] if isinstance(columns, str) else columns
    chart = frame[[category, *columns]].copy()
    if chart.empty:
        st.info("No observations in this time window.")
        return
    st.vega_lite_chart(
        chart,
        {
            "height": height,
            "mark": {"type": "bar", "cornerRadiusEnd": 3},
            "transform": [{"fold": columns, "as": ["Metric", "Value"]}],
            "encoding": {
                "y" if horizontal else "x": {
                    "field": category, "type": "nominal", "sort": "-x" if horizontal else None,
                    "axis": {"labelLimit": 180, "labelColor": "#596b69"},
                },
                "x" if horizontal else "y": {
                    "field": "Value", "type": "quantitative", "title": None,
                    "scale": {"zero": True},
                    "axis": {"labelColor": "#b8c8c6", "gridColor": "#2a3c41"},
                },
                "color": {"field": "Metric", "type": "nominal", "title": None,
                          "scale": {"scheme": "tableau10"}, "legend": {"orient": "bottom"}},
                "tooltip": [
                    {"field": category, "type": "nominal"},
                    {"field": "Metric", "type": "nominal"},
                    {"field": "Value", "type": "quantitative", "format": ".2f"},
                ],
            },
            "config": {
                "background": "#172428",
                "view": {"stroke": "#30464b", "strokeWidth": 1},
                "axis": {"labelColor": "#b8c8c6", "titleColor": "#a8bcba", "gridColor": "#2a3c41"},
                "legend": {"labelColor": "#cbd8d6", "titleColor": "#a8bcba"},
            },
        },
        use_container_width=True,
    )


st.set_page_config(page_title="CTA Operations", page_icon="🚇", layout="wide")
st.markdown(
    """
    <style>
    :root {
        --ink: #e4eeec;
        --muted: #9eb2b0;
        --line: #2d4146;
        --paper: #0d171a;
        --panel: #172428;
        --teal: #56d6be;
    }
    [data-testid="stAppViewContainer"] { background: var(--paper); color: var(--ink); }
    [data-testid="stHeader"] { background: var(--paper); }
    [data-testid="stMainBlockContainer"] { max-width: 1500px; padding-top: 1.2rem; }
    section[data-testid="stSidebar"] { background: #111d21; border-right: 1px solid var(--line); }
    section[data-testid="stSidebar"] * { color: #dce8e5 !important; }
    section[data-testid="stSidebar"] [data-testid="stWidgetLabel"] p { color: #a9bdb9 !important; }
    [data-testid="stSidebar"] [data-testid="stSelectbox"] div[role="combobox"] { background: #1b2a2e; }
    section[data-testid="stSidebar"] [data-testid="stButton"] button {
        border-color: #456065; color: #e5efed; background: #1a2a2e;
    }
    section[data-testid="stSidebar"] [data-testid="stButton"] button:hover { border-color: var(--teal); color: #ffffff !important; }
    [data-testid="stAppViewContainer"] h1,
    [data-testid="stAppViewContainer"] h2,
    [data-testid="stAppViewContainer"] h3 { color: var(--ink) !important; }
    [data-testid="stAppViewContainer"] h1 { font-family: Georgia, "Times New Roman", serif; font-size: 2.05rem; font-weight: 500; }
    [data-testid="stAppViewContainer"] h2 { font-size: 1.18rem; font-weight: 650; }
    [data-testid="stAppViewContainer"] [data-testid="stMarkdownContainer"] p { color: var(--muted); }
    [data-testid="stMetric"] {
        background: var(--panel); border: 1px solid var(--line); border-top: 3px solid var(--teal);
        padding: .7rem .85rem; border-radius: 4px;
    }
    [data-testid="stMetricLabel"] { color: var(--muted); }
    [data-testid="stMetricValue"] { color: var(--ink); }
    [data-testid="stAlert"] { background: #172b2c; border-color: #315054; color: var(--ink); }
    [data-testid="stTabs"] button[role="tab"] { color: var(--muted) !important; }
    [data-testid="stTabs"] button[aria-selected="true"] { color: var(--teal) !important; border-bottom-color: var(--teal) !important; }
    div[data-testid="stDataFrame"] { border: 1px solid var(--line); }
    hr { border-color: var(--line); }
    .dash-kicker { color: var(--teal); font-size: .76rem; font-weight: 700; text-transform: uppercase; }
    .dash-note { color: var(--muted); font-size: .86rem; }
    </style>
    """,
    unsafe_allow_html=True,
)

st.markdown('<div class="dash-kicker">Chicago Transit Authority · data monitor</div>', unsafe_allow_html=True)
st.title("CTA Operations")
st.markdown('<div class="dash-note">Collection health, snapshot coverage and prediction behavior · all times Chicago local</div>', unsafe_allow_html=True)

with st.sidebar:
    st.markdown("### Monitor window")
    window_label = st.selectbox("History", ["24 hours", "7 days", "30 days", "All history"], index=1)
    window_hours = {"24 hours": 24, "7 days": 168, "30 days": 720, "All history": None}[window_label]
    if st.button("Refresh", use_container_width=True):
        monitoring_data.clear()
        st.rerun()
    st.caption("Historical quality calculations use a consistent, read-only database snapshot.")

if not os.getenv("DATABASE_URL"):
    st.error("DATABASE_URL is not configured.")
    st.stop()

try:
    health = collect_health(collection_window_hours=window_hours)
    data = monitoring_data(window_hours)
except Exception as exc:
    st.error(f"Dashboard data unavailable ({type(exc).__name__}).")
    st.stop()

hourly = data["hourly"]
routes = data["routes"]
stations = data["stations"]
latest = health["latest_snapshot"]
latest_text = latest.astimezone(ZoneInfo("America/Chicago")).strftime("%b %d · %H:%M %Z") if latest else "No snapshots"
coverage_values = hourly["Coverage (%)"].dropna() if not hourly.empty else pd.Series(dtype=float)
avg_coverage = f"{coverage_values.mean():.1f}%" if not coverage_values.empty else "N/A"
integrity_clear = bool(
    hourly.empty or (
        hourly["Invalid prediction rows"].sum() == 0
        and hourly["Manifest mismatches"].sum() == 0
        and hourly["Unprocessed files"].sum() == 0
    )
)

if health["issues"]:
    st.error(f"{len(health['issues'])} monitored issue(s): " + ", ".join(health["issues"]))
else:
    st.success("Pipeline checks are clear · all configured stations have a fresh processed snapshot")

metrics = st.columns(5)
metrics[0].metric("Latest (CDT/CST)", latest.astimezone(ZoneInfo("America/Chicago")).strftime("%H:%M") if latest else "—", f"{health['age_minutes']:.1f} min ago" if health["age_minutes"] is not None else None)
metrics[1].metric("Snapshots in window", f"{hourly['Snapshots'].sum():,.0f}" if not hourly.empty else "0")
metrics[2].metric("Mean full-hour coverage", avg_coverage)
metrics[3].metric("Prediction records", f"{hourly['Predictions'].sum():,.0f}" if not hourly.empty else "0")
metrics[4].metric("Integrity checks", "Clear" if integrity_clear else "Review")

st.caption(
    f"Window: {data['window_start'].astimezone(ZoneInfo('America/Chicago')):%Y-%m-%d %H:%M}"
    f" to {data['as_of'].astimezone(ZoneInfo('America/Chicago')):%Y-%m-%d %H:%M} · "
    "Coverage expectation is 12 snapshots per configured station per full hour; partial edge hours are excluded."
)

overview, collection, forecast, quality_tab = st.tabs(["Overview", "Collection", "Forecast behavior", "Data quality"])

with overview:
    left, right = st.columns([1.45, 1])
    with left:
        st.subheader("Collection polling cadence")
        if not hourly.empty:
            line_chart(hourly, ["Snapshots", "Nominal snapshots"], height=300)
        else:
            st.info("No snapshots in this time range.")
    with right:
        st.subheader("Station polling coverage")
        if not stations.empty:
            low = stations.head(12).sort_values("Coverage (%)", ascending=True)
            bar_chart(low, "Station", "Coverage (%)", height=300)
            st.caption("Lowest polling coverage. This measures poll completion, not service hours; valid empty responses count as collected.")
    st.subheader("Prediction volume")
    if not hourly.empty:
        line_chart(hourly, "Predictions", height=240)
    if health["nonprocessed_rows"]:
        st.subheader("Files needing attention")
        attention = pd.DataFrame(health["nonprocessed_rows"], columns=["source_file", "status", "attempts", "last_error", "updated_at"])
        st.dataframe(attention[["source_file", "status", "attempts", "updated_at"]], use_container_width=True, hide_index=True)
    else:
        st.caption("No unprocessed or failed manifest files in the current health check.")

with collection:
    coverage_column, empty_column = st.columns(2)
    with coverage_column:
        st.subheader("Full-hour polling coverage")
        if not hourly.empty:
            line_chart(hourly, "Coverage (%)", height=300, percent=True)
    with empty_column:
        st.subheader("Processed files with no predictions")
        if not hourly.empty:
            line_chart(hourly, ["Empty snapshots", "Unprocessed files"], height=300)
    st.subheader("Station-by-station coverage")
    if not stations.empty:
        st.dataframe(stations, use_container_width=True, hide_index=True)

with forecast:
    st.info("These are CTA-reported forecasts and flags. No observed train-arrival outcome is stored here, so this view describes forecast behavior, not forecast accuracy.")
    wait_column, delay_column = st.columns(2)
    with wait_column:
        st.subheader("Predicted wait horizon over time")
        st.caption("Median and 90th percentile of predicted arrival minus prediction time. These are forecast lead times, not observed waits.")
        if not hourly.empty:
            line_chart(hourly, ["Wait median (min)", "Wait p90 (min)"], height=300)
    with delay_column:
        st.subheader("Reported delay rate by route")
        route_plot = routes.dropna(subset=["Reported delayed (%)"]).nlargest(10, "Predictions") if not routes.empty else routes
        if not route_plot.empty:
            bar_chart(route_plot, "Route", "Reported delayed (%)", height=300)
        else:
            st.info("No known delay flags in this window.")
    st.subheader("Route forecast horizons")
    if not routes.empty:
        route_plot = routes.nlargest(10, "Predictions")
        bar_chart(route_plot, "Route", ["Median predicted wait (min)", "P90 predicted wait (min)"], height=360)
        st.dataframe(routes, use_container_width=True, hide_index=True)

with quality_tab:
    if not hourly.empty:
        st.subheader("Internal consistency checks")
        invalid_column, manifest_column = st.columns(2)
        with invalid_column:
            st.markdown("**Invalid prediction rows**")
            st.caption("Missing required values, malformed run identity, wait arithmetic/timing violations, or out-of-range source values.")
            line_chart(hourly, "Invalid prediction rows", height=280)
        with manifest_column:
            st.markdown("**Manifest reconciliation**")
            st.caption("Processed-file row counts that do not match stored arrivals, plus files not yet processed.")
            line_chart(hourly, ["Manifest mismatches", "Unprocessed files"], height=280)

        st.subheader("Source-field availability")
        gps_column, flag_column = st.columns(2)
        with gps_column:
            st.markdown("**Missing GPS by prediction type**")
            st.caption("Schedule-based predictions may not include a live train position.")
            line_chart(hourly, [
                "Scheduled missing GPS (%)", "Live missing GPS (%)",
                "Unclassified missing GPS (%)",
            ], height=300, percent=True)
        with flag_column:
            st.markdown("**Unknown delay flags and zero coordinates**")
            line_chart(hourly, ["Unknown delay (%)", "Zero GPS (%)"], height=300, percent=True)

        st.subheader("Status and retries")
        processed_count, status_frame, manifest_state = build_manifest_status_view(health["status_counts"])
        a, b = st.columns([1, 2])
        with a:
            st.metric("Processed manifest files", f"{processed_count:,}")
            st.metric("Row-count mismatches", f"{health['mismatch_count']:,}")
        with b:
            if not status_frame.empty:
                bar_chart(status_frame, "Status", "Files", height=190)

        st.subheader("Collection logs")
        collection_period = "across all history" if window_hours is None else f"in the last {window_label}"
        file_issue_rows = health.get("file_issue_logs", [])
        if file_issue_rows:
            st.markdown("**Files with processing issues**")
            file_frame = pd.DataFrame(file_issue_rows, columns=[
                "Source file", "Status", "Failed attempts", "Last recorded error", "Updated",
            ])
            errors = file_frame["Last recorded error"].astype("string")
            safe_errors = errors.str.fullmatch(
                r"(?:parse|download|load|empty-file registration): [A-Za-z_][A-Za-z0-9_]*",
                na=False,
            )
            file_frame["Last recorded error"] = errors.where(
                safe_errors, "Stored error details unavailable in safe format",
            )
            no_error = errors.isna() | errors.eq("")
            file_frame.loc[no_error, "Last recorded error"] = "No error reason recorded"
            file_frame.loc[no_error & file_frame["Status"].eq("processed"), "Last recorded error"] = (
                "Recovered; previous error not retained"
            )
            updated = pd.to_datetime(file_frame["Updated"], utc=True, errors="coerce")
            file_frame["Updated"] = updated.dt.tz_convert("America/Chicago").dt.strftime(
                "%Y-%m-%d %H:%M:%S %Z"
            )
            st.dataframe(file_frame, use_container_width=True, hide_index=True)
        else:
            st.caption(f"No file processing issues recorded {collection_period}.")
        if not health.get("has_collection_ledger", False):
            st.info("Collection-attempt logging is not installed in this database. Apply the collection ledger migration to enable it.")
        else:
            run_rows = health.get("recent_collection_runs", [])
            if run_rows:
                run_frame = pd.DataFrame(run_rows, columns=[
                    "Run ID", "Started", "Finished", "Status", "Expected stations",
                    "Uploaded", "Failed", "Fatal error", "Pending", "In progress",
                ])
                run_frame["Run ID"] = run_frame["Run ID"].str.slice(0, 8)
                for column in ("Started", "Finished"):
                    values = pd.to_datetime(run_frame[column], utc=True, errors="coerce")
                    run_frame[column] = values.dt.tz_convert("America/Chicago").dt.strftime(
                        "%Y-%m-%d %H:%M:%S %Z"
                    )
                st.dataframe(run_frame, use_container_width=True, hide_index=True)
            else:
                st.caption(f"No collection runs recorded {collection_period}.")

            attempt_rows = health.get("collection_attempt_logs", [])
            if attempt_rows:
                st.markdown("**Collection failures and request retries**")
                attempt_frame = pd.DataFrame(attempt_rows, columns=[
                    "Run ID", "Run started", "Station", "Station ID", "Station index",
                    "Status", "Stage", "Request attempts", "HTTP status", "CTA error code",
                    "Storage error", "Storage HTTP status", "Error type", "Source file",
                    "Bucket key", "Duration (s)",
                ])
                attempt_frame["Run ID"] = attempt_frame["Run ID"].str.slice(0, 8)
                run_started = pd.to_datetime(attempt_frame["Run started"], utc=True, errors="coerce")
                attempt_frame["Run started"] = run_started.dt.tz_convert("America/Chicago").dt.strftime(
                    "%Y-%m-%d %H:%M:%S %Z"
                )
                st.dataframe(attempt_frame, use_container_width=True, hide_index=True)
            else:
                st.info(
                    f"No collection failures or request retries recorded {collection_period}."
                )

st.markdown('<div class="dash-note">Read-only monitor · Snapshot coverage and source consistency do not establish actual train-arrival accuracy.</div>', unsafe_allow_html=True)