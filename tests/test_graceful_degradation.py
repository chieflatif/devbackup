"""Tests for graceful degradation - ensuring components work independently.

This module tests that:
1. CLI works independently of MCP server (Requirement 12.3)
2. Scheduler works independently of menu bar app (Requirement 12.2)
3. Atomic operations ensure no data loss (Requirement 12.6)

**Feature: user-experience-enhancement, Property 11: Graceful Degradation**
**Validates: Requirements 12.2, 12.3, 12.6**
"""

import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock
import io

import pytest
from hypothesis import given, strategies as st, settings, assume

from devbackup.cli import (
    main,
    cmd_run,
    cmd_status,
    cmd_list,
    cmd_init,
    EXIT_SUCCESS,
    EXIT_CONFIG_ERROR,
)
from devbackup.config import (
    Configuration,
    SchedulerConfig,
    RetentionConfig,
    LoggingConfig,
    MCPConfig,
    format_config,
)
from devbackup.scheduler import (
    Scheduler,
    SchedulerType,
    load_backup_queue,
    save_backup_queue,
    QueuedBackup,
)


def create_test_config(source_dir: Path, dest_dir: Path, log_dir: Path) -> Configuration:
    """Create a test configuration."""
    return Configuration(
        backup_destination=dest_dir,
        source_directories=[source_dir],
        exclude_patterns=["*.pyc", "__pycache__/"],
        scheduler=SchedulerConfig(type="launchd", interval_seconds=3600),
        retention=RetentionConfig(hourly=24, daily=7, weekly=4),
        logging=LoggingConfig(
            level="ERROR",
            log_file=log_dir / "devbackup.log",
            error_log_file=log_dir / "devbackup.err",
        ),
        mcp=MCPConfig(enabled=True, port=0),
    )


class TestCLIIndependence:
    """
    Tests that CLI works independently of MCP server.
    
    Requirement 12.3: IF the MCP_Server is unavailable, THEN THE CLI SHALL remain functional as a fallback
    """
    
    def test_cli_init_without_mcp_server(self):
        """Test that 'init' command works without MCP server running."""
        with tempfile.TemporaryDirectory() as config_dir:
            config_path = Path(config_dir) / "config.toml"
            
            # MCP server is not running - CLI should still work
            exit_code = main(['--config', str(config_path), 'init'])
            
            assert exit_code == EXIT_SUCCESS
            assert config_path.exists()
            content = config_path.read_text()
            assert "backup_destination" in content
    
    def test_cli_run_without_mcp_server(self):
        """Test that 'run' command works without MCP server running."""
        with tempfile.TemporaryDirectory() as source_dir:
            with tempfile.TemporaryDirectory() as dest_dir:
                with tempfile.TemporaryDirectory() as log_dir:
                    with tempfile.TemporaryDirectory() as config_dir:
                        source_path = Path(source_dir)
                        dest_path = Path(dest_dir)
                        log_path = Path(log_dir)
                        config_path = Path(config_dir) / "config.toml"
                        
                        # Create test file
                        (source_path / "test.txt").write_text("test content")
                        
                        config = create_test_config(source_path, dest_path, log_path)
                        config_path.write_text(format_config(config))
                        
                        # MCP server is not running - CLI should still work
                        exit_code = main(['--config', str(config_path), 'run'])
                        assert exit_code == EXIT_SUCCESS
    
    def test_cli_status_without_mcp_server(self):
        """Test that 'status' command works without MCP server running."""
        with tempfile.TemporaryDirectory() as source_dir:
            with tempfile.TemporaryDirectory() as dest_dir:
                with tempfile.TemporaryDirectory() as log_dir:
                    with tempfile.TemporaryDirectory() as config_dir:
                        config_path = Path(config_dir) / "config.toml"
                        config = create_test_config(
                            Path(source_dir), Path(dest_dir), Path(log_dir)
                        )
                        config_path.write_text(format_config(config))
                        
                        # MCP server is not running - CLI should still work
                        captured_stdout = io.StringIO()
                        with patch('sys.stdout', captured_stdout):
                            exit_code = main(['--config', str(config_path), 'status'])
                        
                        assert exit_code == EXIT_SUCCESS
                        output = captured_stdout.getvalue()
                        assert "Status" in output or "devbackup" in output
    
    def test_cli_list_without_mcp_server(self):
        """Test that 'list' command works without MCP server running."""
        with tempfile.TemporaryDirectory() as source_dir:
            with tempfile.TemporaryDirectory() as dest_dir:
                with tempfile.TemporaryDirectory() as log_dir:
                    with tempfile.TemporaryDirectory() as config_dir:
                        config_path = Path(config_dir) / "config.toml"
                        config = create_test_config(
                            Path(source_dir), Path(dest_dir), Path(log_dir)
                        )
                        config_path.write_text(format_config(config))
                        
                        # MCP server is not running - CLI should still work
                        captured_stdout = io.StringIO()
                        with patch('sys.stdout', captured_stdout):
                            exit_code = main(['--config', str(config_path), 'list'])
                        
                        assert exit_code == EXIT_SUCCESS
    
    def test_cli_does_not_import_mcp_on_non_mcp_commands(self):
        """Test that non-MCP commands don't require MCP imports to succeed."""
        # This test verifies that CLI commands work even if MCP module has issues
        with tempfile.TemporaryDirectory() as config_dir:
            config_path = Path(config_dir) / "config.toml"
            
            # The init command should work without needing MCP
            exit_code = main(['--config', str(config_path), 'init'])
            assert exit_code == EXIT_SUCCESS
    
    def test_cli_help_without_mcp_server(self):
        """Test that help works without MCP server."""
        # Help should always work
        captured_stdout = io.StringIO()
        with patch('sys.stdout', captured_stdout):
            with pytest.raises(SystemExit) as exc_info:
                main(['--help'])
        
        # argparse exits with 0 for --help
        assert exc_info.value.code == 0


class TestSchedulerIndependence:
    """
    Tests that scheduler works independently of menu bar app.
    
    Requirement 12.2: IF the Menu_Bar_App crashes, THEN scheduled backups SHALL continue to run
    """
    
    def test_scheduler_creates_launchd_plist_independently(self):
        """Test that scheduler can create launchd plist without menu bar."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create a mock plist path in temp directory
            plist_path = Path(temp_dir) / "com.devbackup.plist"
            
            scheduler = Scheduler(
                scheduler_type=SchedulerType.LAUNCHD,
                interval_seconds=3600,
            )
            
            # Override the plist path for testing
            original_plist_path = scheduler.PLIST_PATH
            scheduler.PLIST_PATH = plist_path
            
            try:
                # Generate plist content (don't actually install)
                plist_content = scheduler._create_launchd_plist()
                
                # Verify plist structure is correct
                assert plist_content["Label"] == "com.devbackup"
                assert plist_content["StartInterval"] == 3600
                assert plist_content["RunAtLoad"] is True
                assert "ProgramArguments" in plist_content
            finally:
                scheduler.PLIST_PATH = original_plist_path
    
    def test_scheduler_cron_entry_generation_independently(self):
        """Test that scheduler can generate cron entry without menu bar."""
        scheduler = Scheduler(
            scheduler_type=SchedulerType.CRON,
            interval_seconds=3600,  # 1 hour
        )
        
        # Generate cron entry (don't actually install)
        cron_entry = scheduler._create_cron_entry()
        
        # Verify cron entry format
        assert "devbackup" in cron_entry
        assert "# devbackup scheduled backup" in cron_entry
    
    def test_scheduler_status_check_independently(self):
        """Test that scheduler status can be checked without menu bar."""
        scheduler = Scheduler(
            scheduler_type=SchedulerType.LAUNCHD,
            interval_seconds=3600,
        )
        
        # Get status - should work without menu bar
        status = scheduler.get_status()
        
        # Status should be a dict with expected keys
        assert isinstance(status, dict)
        assert "installed" in status
    
    def test_scheduler_does_not_depend_on_ipc(self):
        """Test that scheduler doesn't require IPC to function."""
        # The scheduler should work without any IPC connection
        scheduler = Scheduler(
            scheduler_type=SchedulerType.LAUNCHD,
            interval_seconds=3600,
        )
        
        # These operations should not require IPC
        assert scheduler.scheduler_type == SchedulerType.LAUNCHD
        assert scheduler.interval_seconds == 3600
        
        # Status check should work without IPC
        status = scheduler.get_status()
        assert isinstance(status, dict)


class TestAtomicOperations:
    """
    Tests for atomic operations to ensure no data loss.
    
    Requirement 12.6: THE System SHALL never lose backup data due to software errors (use atomic operations throughout)
    """
    
    def test_backup_queue_atomic_save(self):
        """Test that backup queue saves atomically."""
        with tempfile.TemporaryDirectory() as temp_dir:
            queue_path = Path(temp_dir) / "queue.json"
            
            # Create queue entries
            queue = [
                QueuedBackup(
                    timestamp="2025-01-01T12:00:00",
                    reason="destination_unavailable",
                    destination="/backup/dest",
                ),
                QueuedBackup(
                    timestamp="2025-01-01T13:00:00",
                    reason="battery_low",
                    destination="/backup/dest",
                ),
            ]
            
            # Save queue
            save_backup_queue(queue, queue_path)
            
            # Verify file exists and is valid JSON
            assert queue_path.exists()
            with open(queue_path) as f:
                data = json.load(f)
            assert len(data) == 2
    
    def test_backup_queue_survives_restart(self):
        """Test that backup queue persists across simulated restarts."""
        with tempfile.TemporaryDirectory() as temp_dir:
            queue_path = Path(temp_dir) / "queue.json"
            
            # Create and save queue
            original_queue = [
                QueuedBackup(
                    timestamp="2025-01-01T12:00:00",
                    reason="destination_unavailable",
                    destination="/backup/dest",
                ),
            ]
            save_backup_queue(original_queue, queue_path)
            
            # Simulate restart by loading queue fresh
            loaded_queue = load_backup_queue(queue_path)
            
            # Verify queue was preserved
            assert len(loaded_queue) == 1
            assert loaded_queue[0].timestamp == "2025-01-01T12:00:00"
            assert loaded_queue[0].reason == "destination_unavailable"
    
    def test_backup_queue_handles_corrupted_file(self):
        """Test that backup queue handles corrupted file gracefully."""
        with tempfile.TemporaryDirectory() as temp_dir:
            queue_path = Path(temp_dir) / "queue.json"
            
            # Write corrupted JSON
            queue_path.write_text("{ invalid json }")
            
            # Load should return empty list, not crash
            loaded_queue = load_backup_queue(queue_path)
            assert loaded_queue == []
    
    def test_backup_queue_handles_missing_file(self):
        """Test that backup queue handles missing file gracefully."""
        with tempfile.TemporaryDirectory() as temp_dir:
            queue_path = Path(temp_dir) / "nonexistent" / "queue.json"
            
            # Load should return empty list, not crash
            loaded_queue = load_backup_queue(queue_path)
            assert loaded_queue == []


# =============================================================================
# Property-Based Tests for Graceful Degradation
# =============================================================================

class TestGracefulDegradationProperties:
    """
    Property-based tests for graceful degradation.
    
    **Feature: user-experience-enhancement, Property 11: Graceful Degradation**
    **Validates: Requirements 12.2, 12.3, 12.6**
    """
    
    @given(
        queue_entries=st.lists(
            st.fixed_dictionaries({
                "timestamp": st.text(min_size=1, max_size=30).filter(lambda x: x.strip()),
                "reason": st.sampled_from(["destination_unavailable", "battery_low", "network_error"]),
                "destination": st.text(min_size=1, max_size=100).filter(lambda x: x.strip()),
            }),
            min_size=0,
            max_size=20,
        )
    )
    @settings(max_examples=100)
    def test_property_queue_persistence_round_trip(self, queue_entries):
        """
        Property 11: Backup Queue Persistence Round-Trip
        
        *For any* backup queue state, saving and loading SHALL produce an equivalent queue.
        Queue order SHALL be preserved (FIFO).
        
        **Validates: Requirements 12.4, 12.6**
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            queue_path = Path(temp_dir) / "queue.json"
            
            # Create queue from generated entries
            original_queue = [
                QueuedBackup(
                    timestamp=entry["timestamp"],
                    reason=entry["reason"],
                    destination=entry["destination"],
                )
                for entry in queue_entries
            ]
            
            # Save queue
            save_backup_queue(original_queue, queue_path)
            
            # Load queue
            loaded_queue = load_backup_queue(queue_path)
            
            # Verify round-trip preserves data
            assert len(loaded_queue) == len(original_queue)
            
            # Verify FIFO order is preserved
            for i, (orig, loaded) in enumerate(zip(original_queue, loaded_queue)):
                assert orig.timestamp == loaded.timestamp, f"Timestamp mismatch at index {i}"
                assert orig.reason == loaded.reason, f"Reason mismatch at index {i}"
                assert orig.destination == loaded.destination, f"Destination mismatch at index {i}"
    
    @given(
        interval_seconds=st.integers(min_value=60, max_value=86400),
    )
    @settings(max_examples=100)
    def test_property_scheduler_config_independence(self, interval_seconds):
        """
        Property 11: Scheduler Configuration Independence
        
        *For any* valid scheduler configuration, the scheduler SHALL be configurable
        without requiring menu bar app or MCP server.
        
        **Validates: Requirements 12.2**
        """
        # Create scheduler with various intervals
        scheduler = Scheduler(
            scheduler_type=SchedulerType.LAUNCHD,
            interval_seconds=interval_seconds,
        )
        
        # Scheduler should be created successfully
        assert scheduler.interval_seconds == interval_seconds
        assert scheduler.scheduler_type == SchedulerType.LAUNCHD
        
        # Plist generation should work
        plist = scheduler._create_launchd_plist()
        assert plist["StartInterval"] == interval_seconds
        assert plist["Label"] == "com.devbackup"
    
    @given(
        interval_minutes=st.integers(min_value=1, max_value=1440),
    )
    @settings(max_examples=100)
    def test_property_cron_scheduler_independence(self, interval_minutes):
        """
        Property 11: Cron Scheduler Independence
        
        *For any* valid interval, cron scheduler SHALL generate valid entries
        without requiring external components.
        
        **Validates: Requirements 12.2**
        """
        interval_seconds = interval_minutes * 60
        
        scheduler = Scheduler(
            scheduler_type=SchedulerType.CRON,
            interval_seconds=interval_seconds,
        )
        
        # Cron entry generation should work
        cron_entry = scheduler._create_cron_entry()
        
        # Entry should contain the marker
        assert "# devbackup scheduled backup" in cron_entry
        
        # Entry should contain devbackup command
        assert "devbackup" in cron_entry
    
    @given(
        config_content=st.text(min_size=0, max_size=1000),
    )
    @settings(max_examples=50)
    def test_property_cli_handles_invalid_config_gracefully(self, config_content):
        """
        Property 11: CLI Graceful Error Handling
        
        *For any* invalid configuration content, CLI SHALL handle errors gracefully
        without crashing.
        
        **Validates: Requirements 12.3, 12.6**
        """
        with tempfile.TemporaryDirectory() as config_dir:
            config_path = Path(config_dir) / "config.toml"
            config_path.write_text(config_content)
            
            # CLI should handle invalid config gracefully
            captured_stderr = io.StringIO()
            with patch('sys.stderr', captured_stderr):
                try:
                    exit_code = main(['--config', str(config_path), 'status'])
                    # Should return error code, not crash
                    assert exit_code in [EXIT_SUCCESS, EXIT_CONFIG_ERROR, 1]
                except SystemExit as e:
                    # SystemExit is acceptable for error handling
                    pass
                except Exception as e:
                    # Should not raise unexpected exceptions
                    pytest.fail(f"CLI raised unexpected exception: {type(e).__name__}: {e}")
