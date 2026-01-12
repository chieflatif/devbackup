"""Tests for the main backup orchestration module.

Tests the run_backup function and verifies proper error handling,
lock management, and cleanup behavior.
"""

import os
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from devbackup.backup import (
    run_backup,
    BackupResult,
    EXIT_SUCCESS,
    EXIT_CONFIG_ERROR,
    EXIT_LOCK_ERROR,
    EXIT_DESTINATION_ERROR,
    EXIT_SNAPSHOT_ERROR,
    EXIT_SPACE_ERROR,
)
from devbackup.config import (
    Configuration,
    SchedulerConfig,
    RetentionConfig,
    LoggingConfig,
    MCPConfig,
    ConfigurationError,
)
from devbackup.lock import LockError


@pytest.fixture
def temp_dirs():
    """Create temporary source and destination directories."""
    with tempfile.TemporaryDirectory() as source_dir:
        with tempfile.TemporaryDirectory() as dest_dir:
            with tempfile.TemporaryDirectory() as log_dir:
                # Create some test files in source
                source_path = Path(source_dir)
                (source_path / "file1.txt").write_text("content1")
                (source_path / "file2.txt").write_text("content2")
                (source_path / "subdir").mkdir()
                (source_path / "subdir" / "file3.txt").write_text("content3")
                
                yield {
                    "source": source_path,
                    "dest": Path(dest_dir),
                    "log_dir": Path(log_dir),
                }


@pytest.fixture
def test_config(temp_dirs):
    """Create a test configuration."""
    return Configuration(
        backup_destination=temp_dirs["dest"],
        source_directories=[temp_dirs["source"]],
        exclude_patterns=["*.pyc", "__pycache__/"],
        scheduler=SchedulerConfig(type="launchd", interval_seconds=3600),
        retention=RetentionConfig(hourly=24, daily=7, weekly=4),
        logging=LoggingConfig(
            level="DEBUG",
            log_file=temp_dirs["log_dir"] / "devbackup.log",
            error_log_file=temp_dirs["log_dir"] / "devbackup.err",
        ),
        mcp=MCPConfig(enabled=True, port=0),
    )


class TestRunBackupSuccess:
    """Tests for successful backup operations."""
    
    def test_successful_backup_returns_success(self, test_config):
        """Test that a successful backup returns success result."""
        result = run_backup(config=test_config)
        
        assert result.success is True
        assert result.exit_code == EXIT_SUCCESS
        assert result.snapshot_result is not None
        assert result.snapshot_result.success is True
        assert result.snapshot_result.snapshot_path is not None
        assert result.error_message is None
    
    def test_successful_backup_creates_snapshot(self, test_config, temp_dirs):
        """Test that a successful backup creates a snapshot directory."""
        result = run_backup(config=test_config)
        
        assert result.success is True
        
        # Verify snapshot was created
        snapshots = list(temp_dirs["dest"].iterdir())
        assert len(snapshots) == 1
        
        # Verify snapshot contains the files
        snapshot_path = snapshots[0]
        assert (snapshot_path / "file1.txt").exists()
        assert (snapshot_path / "file2.txt").exists()
        assert (snapshot_path / "subdir" / "file3.txt").exists()
    
    def test_successful_backup_releases_lock(self, test_config):
        """Test that lock is released after successful backup."""
        from devbackup.lock import LockManager
        
        result = run_backup(config=test_config)
        
        assert result.success is True
        
        # Verify lock is not held
        lock_manager = LockManager()
        assert not lock_manager.is_locked()
    
    def test_incremental_backup_uses_hard_links(self, test_config, temp_dirs):
        """Test that incremental backups use hard links for unchanged files."""
        import time
        
        # First backup
        result1 = run_backup(config=test_config)
        assert result1.success is True
        
        # Wait to ensure different timestamp
        time.sleep(1.1)
        
        # Second backup without changes
        result2 = run_backup(config=test_config)
        assert result2.success is True
        
        # Get both snapshots
        snapshots = sorted(temp_dirs["dest"].iterdir())
        assert len(snapshots) == 2
        
        # Check that unchanged files have the same inode (hard links)
        file1_snap1 = snapshots[0] / "file1.txt"
        file1_snap2 = snapshots[1] / "file1.txt"
        
        assert file1_snap1.stat().st_ino == file1_snap2.stat().st_ino


class TestRunBackupConfigErrors:
    """Tests for configuration error handling."""
    
    def test_missing_config_file_returns_config_error(self, temp_dirs):
        """Test that missing config file returns config error."""
        result = run_backup(config_path=Path("/nonexistent/config.toml"))
        
        assert result.success is False
        assert result.exit_code == EXIT_CONFIG_ERROR
        assert "not found" in result.error_message.lower()
    
    def test_invalid_config_returns_config_error(self, temp_dirs):
        """Test that invalid config returns config error."""
        # Create an invalid config file
        config_file = temp_dirs["log_dir"] / "invalid.toml"
        config_file.write_text("invalid toml content [[[")
        
        result = run_backup(config_path=config_file)
        
        assert result.success is False
        assert result.exit_code == EXIT_CONFIG_ERROR


class TestRunBackupDestinationErrors:
    """Tests for destination validation error handling."""
    
    def test_nonexistent_destination_returns_destination_error(self, test_config):
        """Test that nonexistent destination returns destination error."""
        test_config.backup_destination = Path("/nonexistent/destination")
        
        result = run_backup(config=test_config)
        
        assert result.success is False
        assert result.exit_code == EXIT_DESTINATION_ERROR
        assert "not found" in result.error_message.lower()
    
    def test_destination_error_releases_lock(self, test_config):
        """Test that lock is released after destination error."""
        from devbackup.lock import LockManager
        
        test_config.backup_destination = Path("/nonexistent/destination")
        
        result = run_backup(config=test_config)
        
        assert result.success is False
        
        # Verify lock is not held
        lock_manager = LockManager()
        assert not lock_manager.is_locked()


class TestRunBackupSourceErrors:
    """Tests for source directory error handling."""
    
    def test_nonexistent_source_logs_warning_and_continues(self, test_config, temp_dirs):
        """Test that nonexistent source logs warning but continues with valid sources."""
        # Add a nonexistent source
        test_config.source_directories.append(Path("/nonexistent/source"))
        
        result = run_backup(config=test_config)
        
        # Should still succeed with the valid source
        assert result.success is True
        assert result.exit_code == EXIT_SUCCESS
    
    def test_all_invalid_sources_returns_error(self, test_config):
        """Test that all invalid sources returns error."""
        test_config.source_directories = [
            Path("/nonexistent/source1"),
            Path("/nonexistent/source2"),
        ]
        
        result = run_backup(config=test_config)
        
        assert result.success is False
        assert result.exit_code == EXIT_SNAPSHOT_ERROR
        assert "invalid" in result.error_message.lower()


class TestRunBackupLockErrors:
    """Tests for lock error handling."""
    
    def test_lock_held_returns_lock_error(self, test_config):
        """Test that held lock returns lock error."""
        from devbackup.lock import LockManager
        
        # Acquire lock in another context
        lock_manager = LockManager()
        lock_manager.acquire()
        
        try:
            result = run_backup(config=test_config)
            
            assert result.success is False
            assert result.exit_code == EXIT_LOCK_ERROR
            assert "lock" in result.error_message.lower()
        finally:
            lock_manager.release()


class TestRunBackupCleanup:
    """Tests for cleanup behavior."""
    
    def test_cleans_up_incomplete_snapshots(self, test_config, temp_dirs):
        """Test that incomplete snapshots from previous runs are cleaned up."""
        # Create an incomplete snapshot
        incomplete_dir = temp_dirs["dest"] / "in_progress_2025-01-01-120000"
        incomplete_dir.mkdir(parents=True)
        (incomplete_dir / "file.txt").write_text("incomplete")
        
        result = run_backup(config=test_config)
        
        assert result.success is True
        assert result.incomplete_cleaned == 1
        
        # Verify incomplete snapshot was removed
        assert not incomplete_dir.exists()
    
    def test_applies_retention_policy(self, test_config, temp_dirs):
        """Test that retention policy is applied after backup."""
        import time
        
        # Set very restrictive retention
        test_config.retention = RetentionConfig(hourly=1, daily=0, weekly=0)
        
        # Create multiple backups with delays to ensure different timestamps
        for i in range(3):
            result = run_backup(config=test_config)
            assert result.success is True
            if i < 2:  # Don't sleep after the last backup
                time.sleep(1.1)
        
        # Should only keep 1 snapshot due to retention policy
        snapshots = [
            d for d in temp_dirs["dest"].iterdir()
            if d.is_dir() and not d.name.startswith(".")
        ]
        assert len(snapshots) == 1


class TestRunBackupLogging:
    """Tests for logging behavior."""
    
    def test_creates_log_files(self, test_config, temp_dirs):
        """Test that log files are created."""
        result = run_backup(config=test_config)
        
        assert result.success is True
        
        # Verify log files were created
        assert test_config.logging.log_file.exists()
    
    def test_logs_backup_start_and_completion(self, test_config, temp_dirs):
        """Test that backup start and completion are logged."""
        result = run_backup(config=test_config)
        
        assert result.success is True
        
        # Read log file
        log_content = test_config.logging.log_file.read_text()
        
        assert "Backup started" in log_content
        assert "Backup completed" in log_content



class TestRunBackupSpaceValidation:
    """Tests for space validation during backup.
    
    Requirements: 2.1, 2.2, 2.5
    """
    
    def test_space_validation_passes_with_sufficient_space(self, test_config, temp_dirs):
        """Test that backup succeeds when sufficient space is available."""
        result = run_backup(config=test_config)
        
        assert result.success is True
        assert result.exit_code == EXIT_SUCCESS
    
    def test_space_validation_fails_with_insufficient_space(self, test_config, temp_dirs):
        """Test that backup fails when insufficient space is available.
        
        Requirements: 2.2, 2.5
        """
        from unittest.mock import patch
        from devbackup.space import SpaceError
        
        # Mock validate_space to raise SpaceError
        with patch('devbackup.backup.validate_space') as mock_validate:
            mock_validate.side_effect = SpaceError(
                "Insufficient disk space",
                available_bytes=100,
                required_bytes=1000,
            )
            
            result = run_backup(config=test_config)
            
            assert result.success is False
            assert result.exit_code == EXIT_SPACE_ERROR
            assert "Insufficient" in result.error_message
    
    def test_space_validation_before_in_progress(self, test_config, temp_dirs):
        """Test that space validation happens before creating in_progress.
        
        Requirements: 2.5
        """
        from unittest.mock import patch
        from devbackup.space import SpaceError
        
        # Mock validate_space to raise SpaceError
        with patch('devbackup.backup.validate_space') as mock_validate:
            mock_validate.side_effect = SpaceError(
                "Insufficient disk space",
                available_bytes=100,
                required_bytes=1000,
            )
            
            result = run_backup(config=test_config)
            
            assert result.success is False
            
            # Verify no in_progress directory was created
            in_progress_dirs = [
                d for d in temp_dirs["dest"].iterdir()
                if d.name.startswith("in_progress_")
            ]
            assert len(in_progress_dirs) == 0
    
    def test_space_validation_releases_lock_on_failure(self, test_config, temp_dirs):
        """Test that lock is released when space validation fails."""
        from unittest.mock import patch
        from devbackup.space import SpaceError
        from devbackup.lock import LockManager
        
        # Mock validate_space to raise SpaceError
        with patch('devbackup.backup.validate_space') as mock_validate:
            mock_validate.side_effect = SpaceError(
                "Insufficient disk space",
                available_bytes=100,
                required_bytes=1000,
            )
            
            result = run_backup(config=test_config)
            
            assert result.success is False
            
            # Verify lock is not held
            lock_manager = LockManager()
            assert not lock_manager.is_locked()
    
    def test_space_warning_logged_but_continues(self, test_config, temp_dirs):
        """Test that low space warning is logged but backup continues."""
        from unittest.mock import patch
        from devbackup.space import SpaceValidationResult
        
        # Mock validate_space to return a warning
        with patch('devbackup.backup.validate_space') as mock_validate:
            mock_validate.return_value = SpaceValidationResult(
                sufficient=True,
                available_bytes=500 * 1024 * 1024,  # 500MB
                estimated_bytes=100 * 1024 * 1024,  # 100MB
                warning="Low disk space warning: only 0.50GB free",
            )
            
            result = run_backup(config=test_config)
            
            # Backup should still succeed
            assert result.success is True
            assert result.exit_code == EXIT_SUCCESS
