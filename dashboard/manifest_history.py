"""Read-only hourly manifest history for the dashboard."""
import pandas as pd

from src.monitoring.health_check import connect
from src.config.stations import STATIONS


MANIFEST_HISTORY_QUERY = """
    WITH bounds AS (
        SELECT NOW() AS window_end,
               NOW() - INTERVAL '24 hours' AS window_start
    ), hourly_counts AS (
        SELECT date_trunc(
                   'hour',
                   f.collection_timestamp AT TIME ZONE 'America/Chicago',
                   'UTC'
               ) AS collection_hour_utc,
               COUNT(*)::bigint AS manifest_files
        FROM ingestion_files AS f
        CROSS JOIN bounds
        WHERE f.collection_timestamp AT TIME ZONE 'America/Chicago'
                  >= bounds.window_start
          AND f.collection_timestamp AT TIME ZONE 'America/Chicago'
                  < bounds.window_end
        GROUP BY 1
    ), hour_bins AS (
        SELECT hour_utc AT TIME ZONE 'UTC' AS collection_hour_utc
        FROM bounds
        CROSS JOIN LATERAL generate_series(
            date_trunc('hour', bounds.window_start AT TIME ZONE 'UTC'),
            date_trunc(
                'hour',
                (bounds.window_end - INTERVAL '1 microsecond') AT TIME ZONE 'UTC'
            ),
            INTERVAL '1 hour'
        ) AS hours(hour_utc)
    )
    SELECT hour_bins.collection_hour_utc,
           COALESCE(hourly_counts.manifest_files, 0)::bigint AS manifest_files
    FROM hour_bins
    LEFT JOIN hourly_counts USING (collection_hour_utc)
    ORDER BY hour_bins.collection_hour_utc
"""


def build_manifest_history_frame(rows):
    """Convert UTC hourly aggregates into a Chicago-local chart frame."""
    frame = pd.DataFrame(
        rows,
        columns=["collection_hour_utc", "manifest_files"],
    )
    if frame.empty:
        return pd.DataFrame(
            {"Manifest files": pd.Series(dtype="int64")},
            index=pd.DatetimeIndex(
                [], name="Collection hour", tz="America/Chicago"
            ),
        )

    frame["collection_hour_utc"] = pd.to_datetime(
        frame["collection_hour_utc"], utc=True
    )
    frame = frame.groupby("collection_hour_utc")["manifest_files"].sum()
    complete_hours = pd.date_range(
        start=frame.index.min(), end=frame.index.max(), freq="h"
    )
    frame = frame.reindex(complete_hours, fill_value=0)
    frame.index = frame.index.tz_convert("America/Chicago")
    frame.index.name = "Collection hour"
    frame.name = "Manifest files"
    return frame.to_frame()


def fetch_manifest_history():
    """Read the last 24 hours of manifest files, including empty hour buckets."""
    with connect() as conn:
        rows = conn.execute(MANIFEST_HISTORY_QUERY).fetchall()
    return build_manifest_history_frame(rows)


VALIDATION_COLUMNS = (
    "hour_utc", "snapshot_files", "empty_files", "unprocessed_files",
    "manifest_mismatches", "prediction_rows", "invalid_rows", "delayed_rows",
    "not_delayed_rows", "unknown_delay_rows", "missing_gps_rows", "zero_gps_rows",
    "median_wait", "p90_wait",
)

VALIDATION_HISTORY_QUERY = """
    WITH scoped_files AS (
        SELECT file_id, collection_timestamp, station_id, status, row_count
        FROM ingestion_files
        WHERE collection_timestamp AT TIME ZONE 'America/Chicago' >= %s
          AND collection_timestamp AT TIME ZONE 'America/Chicago' < %s
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
               COUNT(*) FILTER (WHERE a.latitude = 0 AND a.longitude = 0)::bigint AS zero_gps_rows
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
               COALESCE(SUM(p.zero_gps_rows), 0)::bigint AS zero_gps_rows
        FROM scoped_files f LEFT JOIN prediction_counts p USING (file_id)
        GROUP BY 1
    )
    SELECT h.hour_utc,h.snapshot_files,h.empty_files,h.unprocessed_files,
           h.manifest_mismatches,h.prediction_rows,h.invalid_rows,h.delayed_rows,
           h.not_delayed_rows,h.unknown_delay_rows,h.missing_gps_rows,h.zero_gps_rows,
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


def build_station_coverage_frame(rows, window_start, as_of):
    found = {int(row[0]): row[1:] for row in rows}
    expected = max(0.0, (pd.Timestamp(as_of) - pd.Timestamp(window_start)).total_seconds() / 300)
    result = []
    for name, station_id in STATIONS.items():
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


def fetch_dashboard_history(window_hours=168):
    """Return consistent, read-only hourly, route and station history for one window."""
    if window_hours not in (24, 168, 720, None):
        raise ValueError("Window must be 24, 168, 720 hours or None for all history.")
    with connect() as conn:
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        conn.execute("SET LOCAL statement_timeout = '120s'")
        first_snapshot, as_of = conn.execute(
            "SELECT MIN(collection_timestamp AT TIME ZONE 'America/Chicago'),NOW() FROM ingestion_files"
        ).fetchone()
        if first_snapshot is None:
            empty = build_validation_history_frame([], None, as_of, len(STATIONS))
            return {"hourly": empty, "routes": build_route_quality_frame([]),
                    "stations": build_station_coverage_frame([], as_of, as_of),
                    "window_start": as_of, "as_of": as_of}
        earliest = pd.Timestamp(first_snapshot).tz_convert("UTC")
        window_start = max(earliest, pd.Timestamp(as_of) - pd.Timedelta(hours=window_hours)) if window_hours else earliest
        bounds = (window_start.to_pydatetime(), as_of)
        hourly_rows = conn.execute(VALIDATION_HISTORY_QUERY, bounds).fetchall()
        route_rows = conn.execute(ROUTE_QUALITY_QUERY, bounds).fetchall()
        station_rows = conn.execute(STATION_COVERAGE_QUERY, bounds).fetchall()
    return {
        "hourly": build_validation_history_frame(hourly_rows, window_start, as_of, len(STATIONS)),
        "routes": build_route_quality_frame(route_rows),
        "stations": build_station_coverage_frame(station_rows, window_start, as_of),
        "window_start": window_start,
        "as_of": as_of,
    }


def fetch_validation_history():
    """Compatibility wrapper for callers that need the full hourly quality history."""
    return fetch_dashboard_history(None)["hourly"]