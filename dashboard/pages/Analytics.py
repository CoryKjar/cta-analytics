"""Read-only starting point for historical CTA analysis."""
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import streamlit as st
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from dashboard.manifest_history import aggregate_daily, fetch_dashboard_history  # noqa: E402
from src.config.stations import STATIONS  # noqa: E402

BASELINE_START_UTC = datetime(2026, 11, 2, 22, 30, tzinfo=timezone.utc)
CHICAGO = ZoneInfo("America/Chicago")
WINDOWS = {"Since clean baseline": None, "24 hours": 24, "7 days": 168, "30 days": 720}

load_dotenv()
st.set_page_config(page_title="CTA Analytics", page_icon="📈", layout="wide")
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
    [data-testid="stAppViewContainer"] h1,
    [data-testid="stAppViewContainer"] h2,
    [data-testid="stAppViewContainer"] h3 { color: var(--ink) !important; }
    [data-testid="stAppViewContainer"] h1 { font-family: Georgia, "Times New Roman", serif; font-size: 2.05rem; font-weight: 500; }
    [data-testid="stAppViewContainer"] [data-testid="stMarkdownContainer"] p { color: var(--muted); }
    [data-testid="stMetric"] { background: var(--panel); border: 1px solid var(--line); border-top: 3px solid var(--teal); padding: .7rem .85rem; border-radius: 4px; }
    [data-testid="stMetricLabel"] { color: var(--muted); }
    [data-testid="stMetricValue"] { color: var(--ink); }
    </style>
    """,
    unsafe_allow_html=True,
)


@st.cache_data(ttl=300, show_spinner=False)
def load_analytics(start_iso, end_iso, station_ids):
    return fetch_dashboard_history(
        None,
        start=datetime.fromisoformat(start_iso),
        end=datetime.fromisoformat(end_iso),
        station_ids=list(station_ids) if station_ids else None,
    )


def trend_chart(frame, columns, *, height=280):
    columns = [columns] if isinstance(columns, str) else columns
    chart = frame[["Hour", *columns]].copy()
    if chart.empty:
        st.info("No observations in this time window.")
        return
    chart["Hour index"] = range(len(chart))
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
                "y": {"field": "Value", "type": "quantitative", "title": None},
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
        width="stretch",
    )


st.markdown(
    '<div style="color:#56d6be;font-size:.76rem;font-weight:700;text-transform:uppercase">Chicago Transit Authority · exploration</div>',
    unsafe_allow_html=True,
)
st.title("CTA Analytics")
st.caption("Historical patterns in CTA-reported predictions. These are not observed train arrivals or prediction-accuracy measurements.")

with st.sidebar:
    st.subheader("Analysis window")
    window_label = st.selectbox("Period", list(WINDOWS), index=0)
    station_names = st.multiselect(
        "Stations",
        sorted(name.replace("_", " ") for name in STATIONS),
        placeholder="All stations",
    )
    granularity = st.radio("Granularity", ["Hourly", "Daily"], horizontal=True)
    if st.button("Refresh", width="stretch"):
        load_analytics.clear()
        st.rerun()

if not os.getenv("DATABASE_URL"):
    st.error("DATABASE_URL is not configured.")
    st.stop()

now = datetime.now(timezone.utc)
window_hours = WINDOWS[window_label]
window_start = (
    BASELINE_START_UTC
    if window_hours is None
    else max(BASELINE_START_UTC, now - timedelta(hours=window_hours))
)
selected_station_ids = tuple(sorted(
    station_id for name, station_id in STATIONS.items()
    if name.replace("_", " ") in station_names
))

try:
    data = load_analytics(
        window_start.isoformat(), now.isoformat(), selected_station_ids or None,
    )
except Exception as exc:
    st.error(f"Analytics data unavailable ({type(exc).__name__}).")
    st.stop()

hourly = data["hourly"]
routes = data["routes"]
stations = data["stations"]
if hourly.empty:
    st.info("No snapshots are available in this analysis window yet.")
    st.stop()

view = aggregate_daily(hourly) if granularity == "Daily" else hourly
coverage = hourly["Coverage (%)"].dropna()
metrics = st.columns(4)
metrics[0].metric("Snapshots", f"{hourly['Snapshots'].sum():,.0f}")
metrics[1].metric("Predictions", f"{hourly['Predictions'].sum():,.0f}")
metrics[2].metric("Mean full-hour coverage", f"{coverage.mean():.1f}%" if not coverage.empty else "N/A")
metrics[3].metric("Invalid prediction rows", f"{hourly['Invalid prediction rows'].sum():,.0f}")

st.caption(
    f"Baseline: {BASELINE_START_UTC.astimezone(CHICAGO):%Y-%m-%d %H:%M %Z} onward · "
    f"Window ends {now.astimezone(CHICAGO):%Y-%m-%d %H:%M %Z} · "
    "coverage uses source snapshot time and accounts for 23/24/25-hour Chicago days."
)

st.subheader("Collection and prediction volume")
trend_chart(view, ["Snapshots", "Nominal snapshots"])

st.subheader("Forecast horizon")
if granularity == "Daily":
    st.info("Wait-time percentiles are shown hourly only; hourly percentiles cannot be combined into daily percentiles.")
else:
    trend_chart(hourly, ["Wait median (min)", "Wait p90 (min)"])

left, right = st.columns(2)
with left:
    st.subheader("Route patterns")
    if routes.empty:
        st.info("No route-level predictions in this window.")
    else:
        st.dataframe(
            routes[["Route", "Predictions", "Median predicted wait (min)",
                    "P90 predicted wait (min)", "Reported delayed (%)", "Unknown delay (%)"]],
            width="stretch",
            hide_index=True,
        )
with right:
    st.subheader("Station coverage")
    if stations.empty:
        st.info("No station coverage in this window.")
    else:
        st.dataframe(
            stations[["Station", "Snapshots", "Expected snapshots", "Coverage (%)",
                      "Empty snapshots", "Unprocessed files"]],
            width="stretch",
            hide_index=True,
        )