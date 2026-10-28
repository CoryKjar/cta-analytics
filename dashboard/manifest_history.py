"""Read-only hourly manifest history for the dashboard."""
import pandas as pd

from src.monitoring.health_check import connect
from src.config.stations import STATIONS


VALIDATION_COLUMNS = (
    "hour_utc", "snapshot_files", "empty_files", "unprocessed_files",
    "manifest_mismatches", "prediction_rows", "invalid_rows", "delayed_rows",
    "not_delayed_rows", "unknown_delay_rows", "missing_gps_rows", "zero_gps_rows",
    "scheduled_rows", "live_rows", "unknown_schedule_rows",
    "scheduled_missing_gps_rows", "live_missing_gps_rows",
    "unclassified_missing_gps_rows",
    "median_wait", "p90_wait",
)

VALIDATION_HISTORY_QUERY = """
    WITH scoped_files AS (
        SELECT file_id, collection_timestamp, station_id, status, row_count
        FROM ingestion_files
        WHERE collection_timestamp AT TIME ZONE 'America/Chicago' >= %s
          AND collection_timestamp AT TIME ZONE 'America/Chicago' < %s
          AND (%s::integer[] IS NULL OR station_id = ANY(%s::integer[]))
    ), prediction_counts AS (
        SELECT a.file_id, COUNT(*)::bigint AS prediction_rows,
               COUNT(*) FILTER (WHERE
                   a.run_number IS NULL OR a.run_number !~ '^[0-9]+$'
                   OR a.collection_timestamp IS NULL OR a.station_id IS NULL
                   OR a.stop_id IS NULL OR a.route_id IS NULL OR a.destination_id IS NULL
                   OR a.prediction_time IS NULL OR a.arrival_time IS NULL
                   OR a.direction IS NULL OR a.direction NOT IN ('1', '5')
                   OR a.wait_time IS NULL OR a.wait_time < 0
                   OR a.prediction_time > a.collection_timestamp
                   OR ABS(a.wait_time - EXTRACT(EPOCH FROM (a.arrival_time - a.prediction_time)) / 60) > 0.0001
                   OR a.latitude NOT BETWEEN -90 AND 90
                   OR a.longitude NOT BETWEEN -180 AND 180
                   OR a.heading NOT BETWEEN 0 AND 360
               )::bigint AS invalid_rows,
               COUNT(*) FILTER (WHERE a.is_delayed IS TRUE)::bigint AS delayed_rows,
               COUNT(*) FILTER (WHERE a.is_delayed IS FALSE)::bigint AS not_delayed_rows,
               COUNT(*) FILTER (WHERE a.is_delayed IS NULL)::bigint AS unknown_delay_rows,
               COUNT(*) FILTER (WHERE a.latitude IS NULL OR a.longitude IS NULL)::bigint AS missing_gps_rows,
               COUNT(*) FILTER (WHERE a.latitude = 0 AND a.longitude = 0)::bigint AS zero_gps_rows,
               COUNT(*) FILTER (WHERE a.is_scheduled IS TRUE)::bigint AS scheduled_rows,
               COUNT(*) FILTER (WHERE a.is_scheduled IS FALSE)::bigint AS live_rows,
               COUNT(*) FILTER (WHERE a.is_scheduled IS NULL)::bigint AS unknown_schedule_rows,
               COUNT(*) FILTER (WHERE a.is_scheduled IS TRUE
                   AND (a.latitude IS NULL OR a.longitude IS NULL))::bigint AS scheduled_missing_gps_rows,
               COUNT(*) FILTER (WHERE a.is_scheduled IS FALSE
                   AND (a.latitude IS NULL OR a.longitude IS NULL))::bigint AS live_missing_gps_rows,
               COUNT(*) FILTER (WHERE a.is_scheduled IS NULL
                   AND (a.latitude IS NULL OR a.longitude IS NULL))::bigint AS unclassified_missing_gps_rows
        FROM arrivals a JOIN scoped_files f USING (file_id) GROUP BY a.file_id
    ), hourly_forecast AS (
        SELECT date_trunc('hour', a.collection_timestamp AT TIME ZONE 'America/Chicago', 'UTC') AS hour_utc,
               percentile_cont(0.5) WITHIN GROUP (ORDER BY a.wait_time) AS median_wait,
               percentile_cont(0.9) WITHIN GROUP (ORDER BY a.wait_time) AS p90_wait
        FROM arrivals a JOIN scoped_files f USING (file_id)
        WHERE a.wait_time IS NOT NULL
        GROUP BY 1
    ), hourly_files AS (
        SELECT date_trunc('hour', f.collection_timestamp AT TIME ZONE 'America/Chicago', 'UTC') AS hour_utc,
               COUNT(*)::bigint AS snapshot_files,
               COUNT(*) FILTER (WHERE f.status = 'processed' AND f.row_count = 0)::bigint AS empty_files,
               COUNT(*) FILTER (WHERE f.status <> 'processed')::bigint AS unprocessed_files,
               COUNT(*) FILTER (WHERE f.status = 'processed'
                   AND f.row_count IS DISTINCT FROM COALESCE(p.prediction_rows, 0))::bigint AS manifest_mismatches,
               COALESCE(SUM(p.prediction_rows), 0)::bigint AS prediction_rows,
               COALESCE(SUM(p.invalid_rows), 0)::bigint AS invalid_rows,
               COALESCE(SUM(p.delayed_rows), 0)::bigint AS delayed_rows,
               COALESCE(SUM(p.not_delayed_rows), 0)::bigint AS not_delayed_rows,
               COALESCE(SUM(p.unknown_delay_rows), 0)::bigint AS unknown_delay_rows,
               COALESCE(SUM(p.missing_gps_rows), 0)::bigint AS missing_gps_rows,
               COALESCE(SUM(p.zero_gps_rows), 0)::bigint AS zero_gps_rows,
               COALESCE(SUM(p.scheduled_rows), 0)::bigint AS scheduled_rows,
               COALESCE(SUM(p.live_rows), 0)::bigint AS live_rows,
               COALESCE(SUM(p.unknown_schedule_rows), 0)::bigint AS unknown_schedule_rows,
               COALESCE(SUM(p.scheduled_missing_gps_rows), 0)::bigint AS scheduled_missing_gps_rows,
               COALESCE(SUM(p.live_missing_gps_rows), 0)::bigint AS live_missing_gps_rows,
               COALESCE(SUM(p.unclassified_missing_gps_rows), 0)::bigint AS unclassified_missing_gps_rows
        FROM scoped_files f LEFT JOIN prediction_counts p USING (file_id)
        GROUP BY 1
    )
    SELECT h.hour_utc,h.snapshot_files,h.empty_files,h.unprocessed_files,
           h.manifest_mismatches,h.prediction_rows,h.invalid_rows,h.delayed_rows,
           h.not_delayed_rows,h.unknown_delay_rows,h.missing_gps_rows,h.zero_gps_rows,
           h.scheduled_rows,h.live_rows,h.unknown_schedule_rows,
           h.scheduled_missing_gps_rows,h.live_missing_gps_rows,
           h.unclassified_missing_gps_rows,
           f.median_wait,f.p90_wait
    FROM hourly_files h LEFT JOIN hourly_forecast f USING (hour_utc)
    ORDER BY h.hour_utc
"""

ROUTE_QUALITY_QUERY = """
    SELECT r.route_name AS route, COUNT(*)::bigint AS predictions,
           percentile_cont(0.5) WITHIN GROUP (ORDER BY a.wait_time) AS median_wait,
           percentile_cont(0.9) WITHIN GROUP (ORDER BY a.wait_time) AS p90_wait,
           COUNT(*) FILTER (WHERE a.is_delayed IS TRUE)::bigint AS delayed_rows,
           COUNT(*) FILTER (WHERE a.is_delayed IS FALSE)::bigint AS not_delayed_rows,
           COUNT(*) FILTER (WHERE a.is_delayed IS NULL)::bigint AS unknown_delay_rows
    FROM arrivals a JOIN routes r USING (route_id)
    WHERE a.collection_timestamp AT TIME ZONE 'America/Chicago' >= %s
      AND a.collection_timestamp AT TIME ZONE 'America/Chicago' < %s
      AND (%s::integer[] IS NULL OR a.station_id = ANY(%s::integer[]))
    GROUP BY r.route_id,r.route_name ORDER BY predictions DESC
"""

STATION_COVERAGE_QUERY = """
    SELECT station_id,
           COUNT(*) FILTER (WHERE status = 'processed')::bigint AS processed_files,
           COUNT(*) FILTER (WHERE status <> 'processed')::bigint AS unprocessed_files,
           COUNT(*) FILTER (WHERE status = 'processed' AND row_count = 0)::bigint AS empty_files,
           MAX(collection_timestamp) AS latest_snapshot
    FROM ingestion_files
    WHERE collection_timestamp AT TIME ZONE 'America/Chicago' >= %s
      AND collection_timestamp AT TIME ZONE 'America/Chicago' < %s
      AND station_id IS NOT NULL
      AND (%s::integer[] IS NULL OR station_id = ANY(%s::integer[]))
    GROUP BY station_id
"""


def build_validation_history_frame(rows, window_start, as_of, station_count):
    """Keep missing hours visible and exclude partial hours from nominal coverage."""
    frame = pd.DataFrame(rows, columns=VALIDATION_COLUMNS)
    if frame.empty or window_start is None or as_of is None:
        return frame
    window_start = pd.Timestamp(window_start).tz_convert("UTC")
    as_of = pd.Timestamp(as_of).tz_convert("UTC")
    frame["hour_utc"] = pd.to_datetime(frame["hour_utc"], utc=True)
    frame = frame.set_index("hour_utc").sort_index()
    last_hour = (as_of - pd.Timedelta(microseconds=1)).floor("h")
    first_hour = window_start.floor("h")
    if last_hour < first_hour:
        return pd.DataFrame()
    hours = pd.date_range(first_hour, max(last_hour, frame.index.max()), freq="h")
    frame = frame.reindex(hours, fill_value=0)
    full_hours = (frame.index >= window_start) & (frame.index + pd.Timedelta(hours=1) <= as_of)
    frame["Nominal snapshots"] = pd.Series(float(station_count * 12), index=hours).where(full_hours)
    frame["Coverage (%)"] = 100 * frame["snapshot_files"] / frame["Nominal snapshots"]
    predictions = frame["prediction_rows"].replace(0, float("nan"))
    known_flags = (frame["delayed_rows"] + frame["not_delayed_rows"]).replace(0, float("nan"))
    frame["Reported delayed (%)"] = 100 * frame["delayed_rows"] / known_flags
    frame["Unknown delay (%)"] = 100 * frame["unknown_delay_rows"] / predictions
    frame["Missing GPS (%)"] = 100 * frame["missing_gps_rows"] / predictions
    frame["Zero GPS (%)"] = 100 * frame["zero_gps_rows"] / predictions
    scheduled = frame["scheduled_rows"].replace(0, float("nan"))
    live = frame["live_rows"].replace(0, float("nan"))
    unclassified = frame["unknown_schedule_rows"].replace(0, float("nan"))
    frame["Scheduled missing GPS (%)"] = 100 * frame["scheduled_missing_gps_rows"] / scheduled
    frame["Live missing GPS (%)"] = 100 * frame["live_missing_gps_rows"] / live
    frame["Unclassified missing GPS (%)"] = (
        100 * frame["unclassified_missing_gps_rows"] / unclassified
    )
    frame["Hour"] = frame.index.tz_convert("America/Chicago").strftime("%Y-%m-%d %H:%M %Z")
    frame["Wait median (min)"] = frame["median_wait"]
    frame["Wait p90 (min)"] = frame["p90_wait"]
    return frame.rename(columns={
        "snapshot_files": "Snapshots", "empty_files": "Empty snapshots",
        "prediction_rows": "Predictions", "invalid_rows": "Invalid prediction rows",
        "unprocessed_files": "Unprocessed files", "manifest_mismatches": "Manifest mismatches",
        "unknown_delay_rows": "Unknown delay flags",
    }).reset_index(drop=True)


def build_route_quality_frame(rows):
    frame = pd.DataFrame(rows, columns=[
        "Route", "Predictions", "Median predicted wait (min)", "P90 predicted wait (min)",
        "Reported delayed", "Reported not delayed", "Unknown delay flags",
    ])
    if frame.empty:
        return frame
    known = frame["Reported delayed"] + frame["Reported not delayed"]
    frame["Reported delayed (%)"] = 100 * frame["Reported delayed"] / known.replace(0, float("nan"))
    frame["Unknown delay (%)"] = 100 * frame["Unknown delay flags"] / frame["Predictions"].replace(0, float("nan"))
    return frame


def build_station_coverage_frame(rows, window_start, as_of, station_ids=None):
    found = {int(row[0]): row[1:] for row in rows}
    expected = max(0.0, (pd.Timestamp(as_of) - pd.Timestamp(window_start)).total_seconds() / 300)
    result = []
    for name, station_id in STATIONS.items():
        if station_ids and station_id not in station_ids:
            continue
        processed, unprocessed, empty, latest = found.get(station_id, (0, 0, 0, None))
        result.append({
            "Station": name.replace("_", " "), "Station ID": station_id,
            "Snapshots": processed, "Expected snapshots": expected,
            "Coverage (%)": 100 * processed / expected if expected else None,
            "Unprocessed files": unprocessed, "Empty snapshots": empty,
            "Latest snapshot": latest,
        })
    frame = pd.DataFrame(result)
    return frame.sort_values(["Coverage (%)", "Station"], na_position="first").reset_index(drop=True)


def aggregate_daily(hourly):
    """Sum hourly counts into Chicago calendar days and recompute rates from the sums."""
    if hourly.empty:
        return hourly
    frame = hourly.copy()
    frame["Day"] = frame["Hour"].str.slice(0, 10)
    frame["full_hour_snapshots"] = frame["Snapshots"].where(frame["Nominal snapshots"].notna())
    counts = [
        "Snapshots", "Empty snapshots", "Unprocessed files", "Manifest mismatches",
        "Predictions", "Invalid prediction rows", "delayed_rows", "not_delayed_rows",
        "Unknown delay flags", "missing_gps_rows", "zero_gps_rows", "scheduled_rows",
        "live_rows", "unknown_schedule_rows", "scheduled_missing_gps_rows",
        "live_missing_gps_rows", "unclassified_missing_gps_rows",
    ]
    daily = frame.groupby("Day", sort=True).agg(
        **{column: (column, "sum") for column in counts},
        full_hour_snapshots=("full_hour_snapshots", "sum"),
        **{"Nominal snapshots": ("Nominal snapshots", lambda values: values.sum(min_count=1))},
        partial_hours=("Nominal snapshots", lambda values: values.isna().sum()),
        hours=("Hour", "size"),
    ).reset_index()
    nan = float("nan")
    # Days cut by the window edge would compare all snapshots against a partial nominal.
    partial_day = (daily["partial_hours"] > 0) | (daily["hours"] < 23)
    daily.loc[partial_day, "Nominal snapshots"] = nan
    daily["Coverage (%)"] = 100 * daily["full_hour_snapshots"] / daily["Nominal snapshots"]
    predictions = daily["Predictions"].replace(0, nan)
    known_flags = (daily["delayed_rows"] + daily["not_delayed_rows"]).replace(0, nan)
    daily["Reported delayed (%)"] = 100 * daily["delayed_rows"] / known_flags
    daily["Unknown delay (%)"] = 100 * daily["Unknown delay flags"] / predictions
    daily["Missing GPS (%)"] = 100 * daily["missing_gps_rows"] / predictions
    daily["Zero GPS (%)"] = 100 * daily["zero_gps_rows"] / predictions
    daily["Scheduled missing GPS (%)"] = 100 * daily["scheduled_missing_gps_rows"] / daily["scheduled_rows"].replace(0, nan)
    daily["Live missing GPS (%)"] = 100 * daily["live_missing_gps_rows"] / daily["live_rows"].replace(0, nan)
    daily["Unclassified missing GPS (%)"] = (
        100 * daily["unclassified_missing_gps_rows"] / daily["unknown_schedule_rows"].replace(0, nan)
    )
    # Percentiles cannot be combined from hourly percentiles.
    daily["Wait median (min)"] = nan
    daily["Wait p90 (min)"] = nan
    daily["Hour"] = daily["Day"]
    return daily


def fetch_dashboard_history(window_hours=168, *, start=None, end=None, station_ids=None):
    """Return consistent, read-only hourly, route and station history for one window.

    ``start``/``end`` (aware datetimes) override ``window_hours`` for a custom range.
    """
    if start is None and window_hours not in (6, 24, 168, 720, None):
        raise ValueError("Window must be 6, 24, 168, 720 hours or None for all history.")
    station_ids = sorted(station_ids) if station_ids else None
    with connect() as conn:
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        conn.execute("SET LOCAL statement_timeout = '120s'")
        first_snapshot, as_of = conn.execute(
            "SELECT MIN(collection_timestamp AT TIME ZONE 'America/Chicago'),NOW() FROM ingestion_files"
        ).fetchone()
        station_count = len(station_ids) if station_ids else len(STATIONS)
        if first_snapshot is None:
            empty = build_validation_history_frame([], None, as_of, station_count)
            return {"hourly": empty, "routes": build_route_quality_frame([]),
                    "stations": build_station_coverage_frame([], as_of, as_of, station_ids),
                    "window_start": as_of, "as_of": as_of}
        earliest = pd.Timestamp(first_snapshot).tz_convert("UTC")
        if start is not None:
            window_start = max(earliest, pd.Timestamp(start).tz_convert("UTC"))
            if end is not None:
                as_of = min(pd.Timestamp(as_of), pd.Timestamp(end).tz_convert("UTC")).to_pydatetime()
        else:
            window_start = max(earliest, pd.Timestamp(as_of) - pd.Timedelta(hours=window_hours)) if window_hours else earliest
        if window_start >= pd.Timestamp(as_of):
            window_start = pd.Timestamp(as_of)
        bounds = (window_start.to_pydatetime(), as_of, station_ids, station_ids)
        hourly_rows = conn.execute(VALIDATION_HISTORY_QUERY, bounds).fetchall()
        route_rows = conn.execute(ROUTE_QUALITY_QUERY, bounds).fetchall()
        station_rows = conn.execute(STATION_COVERAGE_QUERY, bounds).fetchall()
    return {
        "hourly": build_validation_history_frame(hourly_rows, window_start, as_of, station_count),
        "routes": build_route_quality_frame(route_rows),
        "stations": build_station_coverage_frame(station_rows, window_start, as_of, station_ids),
        "window_start": window_start,
        "as_of": as_of,
    }