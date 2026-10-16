"""Read-only hourly manifest history for the dashboard."""
import pandas as pd

from src.monitoring.health_check import connect


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