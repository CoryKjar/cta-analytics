"""Presentation helpers for manifest status data."""
import pandas as pd


def build_manifest_status_view(status_counts):
    """Return the processed count, non-processed rows, and manifest state."""
    processed_count = status_counts.get("processed", 0)
    rows = [
        {
            "Status": "(unset)" if status is None else str(status),
            "Files": count,
        }
        for status, count in status_counts.items()
        if status != "processed"
    ]
    rows.sort(key=lambda row: row["Status"].casefold())
    status_frame = pd.DataFrame(rows, columns=["Status", "Files"])

    if not status_counts:
        manifest_state = "empty"
    elif not rows:
        manifest_state = "all_processed"
    else:
        manifest_state = "has_nonprocessed"

    return processed_count, status_frame, manifest_state