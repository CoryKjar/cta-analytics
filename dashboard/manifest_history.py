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
)

VALIDATION_HISTORY_QUERY = """
    WITH prediction_counts AS (
        SELECT file_id, COUNT(*)::bigint AS prediction_rows,
               COUNT(*) FILTER (WHERE
                   run_number IS NULL OR run_number !~ '^[0-9]+$'
                   OR collection_timestamp IS NULL OR station_id IS NULL
                   OR stop_id IS NULL OR route_id IS NULL OR destination_id IS NULL
                   OR prediction_time IS NULL OR arrival_time IS NULL
                   OR direction IS NULL OR direction NOT IN ('1', '5')
                   OR wait_time IS NULL OR wait_time < 0
                   OR prediction_time > collection_timestamp
                   OR ABS(wait_time - EXTRACT(EPOCH FROM (arrival_time - prediction_time)) / 60) > 0.0001
                   OR latitude NOT BETWEEN -90 AND 90
                   OR longitude NOT BETWEEN -180 AND 180
                   OR heading NOT BETWEEN 0 AND 360
               )::bigint AS invalid_rows,
               COUNT(*) FILTER (WHERE is_delayed IS TRUE)::bigint AS delayed_rows,
               COUNT(*) FILTER (WHERE is_delayed IS FALSE)::bigint AS not_delayed_rows,
               COUNT(*) FILTER (WHERE is_delayed IS NULL)::bigint AS unknown_delay_rows,
               COUNT(*) FILTER (WHERE latitude IS NULL OR longitude IS NULL)::bigint AS missing_gps_rows,
               COUNT(*) FILTER (WHERE latitude = 0 AND longitude = 0)::bigint AS zero_gps_rows
        FROM arrivals GROUP BY file_id
    )
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
    FROM ingestion_files f LEFT JOIN prediction_counts p USING (file_id)
    GROUP BY 1 ORDER BY 1
"""


def build_validation_history_frame(rows, first_snapshot, as_of, station_count):
    """Keep missing hours visible and exclude partial hours from nominal coverage."""
    frame = pd.DataFrame(rows, columns=VALIDATION_COLUMNS)
    if frame.empty or first_snapshot is None:
        return frame
    first_snapshot = pd.Timestamp(first_snapshot).tz_convert("UTC")
    as_of = pd.Timestamp(as_of).tz_convert("UTC")
    frame["hour_utc"] = pd.to_datetime(frame["hour_utc"], utc=True)
    frame = frame.set_index("hour_utc").sort_index()
    hours = pd.date_range(first_snapshot.floor("h"), max(as_of.floor("h"), frame.index.max()), freq="h")
    frame = frame.reindex(hours, fill_value=0)
    full_hours = (frame.index >= first_snapshot) & (frame.index + pd.Timedelta(hours=1) <= as_of)
    frame["Nominal snapshots"] = pd.Series(float(station_count * 12), index=hours).where(full_hours)
    frame["Coverage (%)"] = 100 * frame["snapshot_files"] / frame["Nominal snapshots"]
    predictions = frame["prediction_rows"].replace(0, float("nan"))
    known_flags = (frame["delayed_rows"] + frame["not_delayed_rows"]).replace(0, float("nan"))
    frame["Reported delayed (%)"] = 100 * frame["delayed_rows"] / known_flags
    frame["Unknown delay (%)"] = 100 * frame["unknown_delay_rows"] / predictions
    frame["Missing GPS (%)"] = 100 * frame["missing_gps_rows"] / predictions
    frame["Zero GPS (%)"] = 100 * frame["zero_gps_rows"] / predictions
    frame["Hour"] = frame.index.tz_convert("America/Chicago").strftime("%Y-%m-%d %H:%M %Z")
    return frame.rename(columns={
        "snapshot_files": "Snapshots", "empty_files": "Empty snapshots",
        "prediction_rows": "Predictions", "invalid_rows": "Invalid prediction rows",
        "unprocessed_files": "Unprocessed files", "manifest_mismatches": "Manifest mismatches",
        "unknown_delay_rows": "Unknown delay flags",
    }).reset_index(drop=True)


def fetch_validation_history():
    """Aggregate historical quality metrics in one read-only database snapshot."""
    with connect() as conn:
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        conn.execute("SET LOCAL statement_timeout = '120s'")
        first_snapshot, as_of = conn.execute(
            "SELECT MIN(collection_timestamp AT TIME ZONE 'America/Chicago'), NOW() FROM ingestion_files"
        ).fetchone()
        rows = conn.execute(VALIDATION_HISTORY_QUERY).fetchall()
    return build_validation_history_frame(rows, first_snapshot, as_of, len(STATIONS))