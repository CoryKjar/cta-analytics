"""Regression tests for ledger mid-run failure handling (Critical Defect Fix)."""
import pytest
from datetime import datetime, timezone
from unittest.mock import Mock, patch, MagicMock
import os

from src.storage.collection_ledger import (
    CollectionAttemptLedger,
    CollectionSkipped,
)


class TestLedgerMidRunFailureFix:
    """Tests verifying that ledger write failures now raise instead of silently failing."""

    @pytest.fixture
    def mock_logger(self):
        return Mock()

    @pytest.fixture
    def mock_db_env(self, monkeypatch):
        """Set DATABASE_URL environment variable."""
        monkeypatch.setenv("DATABASE_URL", "postgresql://test@localhost/cta_test")
        yield

    def test_execute_disabled_ledger_raises(self, mock_logger):
        """_execute() raises when ledger is disabled (connection unavailable)."""
        ledger = Mock(spec=CollectionAttemptLedger)
        ledger.enabled = False
        ledger.run_id = "test-run"
        
        # Call the real _execute method on a disabled ledger
        with pytest.raises(RuntimeError, match="Ledger is disabled"):
            CollectionAttemptLedger._execute(ledger, "test_stage", "SELECT 1", [])

    def test_execute_database_error_raises(self, mock_logger, mock_db_env):
        """_execute() raises RuntimeError when database error occurs."""
        with patch('src.storage.collection_ledger.psycopg.connect') as mock_connect:
            mock_conn = MagicMock()
            mock_cursor = MagicMock()
            mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
            
            # Setup for init: lock + interval + retry_details checks succeed
            mock_conn.execute.side_effect = [
                Mock(fetchone=Mock(return_value=[True])),   # advisory lock
                Mock(fetchone=Mock(return_value=[False])),  # interval check
                Mock(fetchone=Mock(return_value=[0])),      # retry_details_available (no columns)
            ]
            mock_connect.return_value = mock_conn
            
            stations = {"40540": "Monroe"}
            ledger = CollectionAttemptLedger(stations, logger=mock_logger)
            assert ledger.enabled, "Ledger should be enabled after successful init"
            
            # Now make cursor.execute raise
            mock_cursor.execute.side_effect = OSError("Connection closed by peer")
            
            # Any ledger write should now raise RuntimeError (not return False)
            with pytest.raises(RuntimeError, match="Ledger write failed"):
                ledger.begin_station("40540", started_at=datetime.now(timezone.utc))

    def test_begin_station_raises_not_returns_false(self, mock_logger, mock_db_env):
        """begin_station() raises on error, not silent fail."""
        with patch('src.storage.collection_ledger.psycopg.connect') as mock_connect:
            mock_conn = MagicMock()
            mock_cursor = MagicMock()
            mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
            mock_conn.execute.side_effect = [
                Mock(fetchone=Mock(return_value=[True])),
                Mock(fetchone=Mock(return_value=[False])),
                Mock(fetchone=Mock(return_value=[0])),
            ]
            mock_connect.return_value = mock_conn
            
            ledger = CollectionAttemptLedger({"40540": "Monroe"}, logger=mock_logger)
            mock_cursor.execute.side_effect = Exception("DB error")
            
            # Should raise, not return False
            with pytest.raises(RuntimeError, match="station_start"):
                ledger.begin_station("40540", started_at=datetime.now(timezone.utc))

    def test_set_stage_raises_not_returns_false(self, mock_logger, mock_db_env):
        """set_stage() raises on error, not silent fail."""
        with patch('src.storage.collection_ledger.psycopg.connect') as mock_connect:
            mock_conn = MagicMock()
            mock_cursor = MagicMock()
            mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
            mock_conn.execute.side_effect = [
                Mock(fetchone=Mock(return_value=[True])),
                Mock(fetchone=Mock(return_value=[False])),
                Mock(fetchone=Mock(return_value=[0])),
            ]
            mock_connect.return_value = mock_conn
            
            ledger = CollectionAttemptLedger({"40540": "Monroe"}, logger=mock_logger)
            mock_cursor.execute.side_effect = TimeoutError("Query timeout")
            
            with pytest.raises(RuntimeError, match="station_stage"):
                ledger.set_stage("40540", "cta_response_received")

    def test_station_uploaded_raises_not_returns_false(self, mock_logger, mock_db_env):
        """station_uploaded() raises on error, not silent fail."""
        with patch('src.storage.collection_ledger.psycopg.connect') as mock_connect:
            mock_conn = MagicMock()
            mock_cursor = MagicMock()
            mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
            mock_conn.execute.side_effect = [
                Mock(fetchone=Mock(return_value=[True])),
                Mock(fetchone=Mock(return_value=[False])),
                Mock(fetchone=Mock(return_value=[0])),
            ]
            mock_connect.return_value = mock_conn
            
            ledger = CollectionAttemptLedger({"40540": "Monroe"}, logger=mock_logger)
            mock_cursor.execute.side_effect = OSError("Connection lost")
            
            with pytest.raises(RuntimeError, match="station_uploaded"):
                ledger.station_uploaded(
                    "40540",
                    attempt_count=1,
                    http_status=200,
                    cta_error_code=None,
                    source_file="cta_40540_20261107_090000.xml",
                    object_key="raw/2026/11/07/cta_40540_20261107_090000.xml",
                    duration_seconds=2.5,
                )

    def test_station_failed_raises_not_returns_false(self, mock_logger, mock_db_env):
        """station_failed() raises on error, not silent fail."""
        with patch('src.storage.collection_ledger.psycopg.connect') as mock_connect:
            mock_conn = MagicMock()
            mock_cursor = MagicMock()
            mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
            mock_conn.execute.side_effect = [
                Mock(fetchone=Mock(return_value=[True])),
                Mock(fetchone=Mock(return_value=[False])),
                Mock(fetchone=Mock(return_value=[0])),
            ]
            mock_connect.return_value = mock_conn
            
            ledger = CollectionAttemptLedger({"40540": "Monroe"}, logger=mock_logger)
            mock_cursor.execute.side_effect = OSError("DB disconnected")
            
            with pytest.raises(RuntimeError, match="station_failed"):
                ledger.station_failed(
                    "40540",
                    stage="cta_request",
                    attempt_count=3,
                    error_type="http_timeout",
                    http_status=None,
                    cta_error_code=None,
                    storage_error_code=None,
                    storage_http_status=None,
                    duration_seconds=241,
                )

    def test_finish_raises_not_returns_false(self, mock_logger, mock_db_env):
        """finish() raises on error, not silent fail."""
        with patch('src.storage.collection_ledger.psycopg.connect') as mock_connect:
            mock_conn = MagicMock()
            mock_cursor = MagicMock()
            mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
            mock_conn.execute.side_effect = [
                Mock(fetchone=Mock(return_value=[True])),
                Mock(fetchone=Mock(return_value=[False])),
                Mock(fetchone=Mock(return_value=[0])),
            ]
            mock_connect.return_value = mock_conn
            
            ledger = CollectionAttemptLedger({"40540": "Monroe"}, logger=mock_logger)
            mock_cursor.execute.side_effect = OSError("Network error")
            
            with pytest.raises(RuntimeError, match="run_finish"):
                ledger.finish(uploaded_count=0, failed_count=1)

    def test_ledger_disabled_after_first_error(self, mock_logger, mock_db_env):
        """After first error, ledger is disabled and subsequent calls immediately raise."""
        with patch('src.storage.collection_ledger.psycopg.connect') as mock_connect:
            mock_conn = MagicMock()
            mock_cursor = MagicMock()
            mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
            mock_conn.execute.side_effect = [
                Mock(fetchone=Mock(return_value=[True])),
                Mock(fetchone=Mock(return_value=[False])),
                Mock(fetchone=Mock(return_value=[0])),
            ]
            mock_connect.return_value = mock_conn
            
            ledger = CollectionAttemptLedger({"40540": "Monroe"}, logger=mock_logger)
            assert ledger.enabled
            
            # First error
            mock_cursor.execute.side_effect = OSError("Error")
            try:
                ledger.begin_station("40540", started_at=datetime.now(timezone.utc))
            except RuntimeError:
                pass
            
            # Now ledger is disabled
            assert not ledger.enabled
            
            # Subsequent calls immediately raise without trying to execute
            with pytest.raises(RuntimeError, match="Ledger is disabled"):
                ledger.set_stage("40540", "xml_parsed")

    def test_connection_closed_on_error(self, mock_logger, mock_db_env):
        """Connection is properly closed when error occurs."""
        with patch('src.storage.collection_ledger.psycopg.connect') as mock_connect:
            mock_conn = MagicMock()
            mock_cursor = MagicMock()
            mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
            mock_conn.execute.side_effect = [
                Mock(fetchone=Mock(return_value=[True])),
                Mock(fetchone=Mock(return_value=[False])),
                Mock(fetchone=Mock(return_value=[0])),
            ]
            mock_connect.return_value = mock_conn
            
            ledger = CollectionAttemptLedger({"40540": "Monroe"}, logger=mock_logger)
            mock_cursor.execute.side_effect = OSError("Error")
            
            try:
                ledger.begin_station("40540", started_at=datetime.now(timezone.utc))
            except RuntimeError:
                pass
            
            # Verify connection was closed
            assert mock_conn.rollback.called, "Connection should be rolled back"
            assert mock_conn.close.called, "Connection should be closed"


class TestLedgerInitStillFailsClosed:
    """Verify initialization failures still raise (no regression)."""

    @pytest.fixture
    def mock_logger(self):
        return Mock()

    def test_init_database_unavailable_raises(self, mock_logger, monkeypatch):
        """Database connection failure during init raises."""
        monkeypatch.setenv("DATABASE_URL", "postgresql://test@localhost/cta")
        
        with patch('src.storage.collection_ledger.psycopg.connect') as mock_connect:
            mock_connect.side_effect = OSError("Database unreachable")
            
            with pytest.raises(OSError, match="Database unreachable"):
                CollectionAttemptLedger({"40540": "Monroe"}, logger=mock_logger)

    def test_init_lock_contention_raises(self, mock_logger, monkeypatch):
        """Advisory lock failure during init raises CollectionSkipped."""
        monkeypatch.setenv("DATABASE_URL", "postgresql://test@localhost/cta")
        
        with patch('src.storage.collection_ledger.psycopg.connect') as mock_connect:
            mock_conn = MagicMock()
            mock_conn.execute.side_effect = [
                Mock(fetchone=Mock(return_value=[False])),  # lock failed
            ]
            mock_connect.return_value = mock_conn
            
            with pytest.raises(CollectionSkipped, match="collection_in_progress"):
                CollectionAttemptLedger({"40540": "Monroe"}, logger=mock_logger)

    def test_init_interval_already_collected_raises(self, mock_logger, monkeypatch):
        """Duplicate interval detection during init raises CollectionSkipped."""
        monkeypatch.setenv("DATABASE_URL", "postgresql://test@localhost/cta")
        
        with patch('src.storage.collection_ledger.psycopg.connect') as mock_connect:
            mock_conn = MagicMock()
            mock_conn.execute.side_effect = [
                Mock(fetchone=Mock(return_value=[True])),   # lock
                Mock(fetchone=Mock(return_value=[True])),   # interval already collected
            ]
            mock_connect.return_value = mock_conn
            
            with pytest.raises(CollectionSkipped, match="interval_already_collected"):
                CollectionAttemptLedger({"40540": "Monroe"}, logger=mock_logger)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
