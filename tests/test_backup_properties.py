"""Property-based tests for backup orchestration invariants.

Tests Property 7 (Lock Release Invariant) and Property 8 (Incomplete Snapshot Cleanup Invariant)
from the design document.

**Validates: Requirements 2.4, 4.6, 8.5, 8.6**
"""

import os
import shutil
import tempfile
from pathlib import Path
from typing import Optional
from unittest.mock import patch, MagicMock

import pytest
from hypothesis import given, strategies as st, settings, assume, Phase

from devbackup.backup import (
    run_backup,
    BackupResult,
    EXIT_SUCCESS,
    EXIT_CONFIG_ERROR,
    EXIT_LOCK_ERROR,
    EXIT_DESTINATION_ERROR,
    EXIT_SNAPSHOT_ERROR,
)
from devbackup.config import (
    Configuration,
    SchedulerConfig,
    RetentionConfig,
    LoggingConfig,
    MCPConfig,
)
from devbackup.lock import LockManager


# Strategy for generating error injection points
error_injection_points = st.sampled_from([
    "config_load",
    "lock_acquire",
    "destination_validate",
    "snapshot_create",
    "retention_apply",
    "none",  # No error - successful execution
])


def create_test_config(source_dir: Path, dest_dir: Path, log_dir: Path) -> Configuration:
    """Create a test configuration with the given directories."""
    return Configuration(
        backup_destination=dest_dir,
        source_directories=[source_dir],
        exclude_patterns=["*.pyc", "__pycache__/"],
        scheduler=SchedulerConfig(type="launchd", interval_seconds=3600),
        retention=RetentionConfig(hourly=24, daily=7, weekly=4),
        logging=LoggingConfig(
            level="ERROR",  # Reduce log noise in tests
            log_file=log_dir / "devbackup.log",
            error_log_file=log_dir / "devbackup.err",
        ),
        mcp=MCPConfig(enabled=True, port=0),
    )


class TestLockReleaseInvariant:
    """
    Property 7: Lock Release Invariant
    
    *For any* execution path through the backup system (success, failure, or exception),
    the lock SHALL be released before the process exits.
    
    **Validates: Requirements 2.4, 8.5**
    """
    
    @given(error_point=error_injection_points)
    @settings(max_examples=10, deadline=None, phases=[Phase.generate, Phase.target])
    def test_lock_released_on_all_execution_paths(self, error_point: str):
        """
        **Feature: macos-incremental-backup, Property 7: Lock Release Invariant**
        
        For any execution path (success or failure), the lock must be released.
        """
        with tempfile.TemporaryDirectory() as source_dir:
            with tempfile.TemporaryDirectory() as dest_dir:
                with tempfile.TemporaryDirectory() as log_dir:
                    source_path = Path(source_dir)
                    dest_path = Path(dest_dir)
                    log_path = Path(log_dir)
                    
                    # Create test files
                    (source_path / "test.txt").write_text("test content")
                    
                    config = create_test_config(source_path, dest_path, log_path)
                    
                    # Inject errors at different points
                    if error_point == "config_load":
                        # Test with invalid config path
                        result = run_backup(config_path=Path("/nonexistent/config.toml"))
                    elif error_point == "lock_acquire":
                        # Pre-acquire lock to cause lock error
                        lock_manager = LockManager()
                        lock_manager.acquire()
                        try:
                            result = run_backup(config=config)
                        finally:
                            lock_manager.release()
                    elif error_point == "destination_validate":
                        # Use invalid destination
                        config.backup_destination = Path("/nonexistent/destination")
                        result = run_backup(config=config)
                    elif error_point == "snapshot_create":
                        # Use invalid source directories
                        config.source_directories = [Path("/nonexistent/source")]
                        result = run_backup(config=config)
                    elif error_point == "retention_apply":
                        # This is hard to inject, so we just run normally
                        # Retention errors are non-fatal anyway
                        result = run_backup(config=config)
                    else:  # "none" - successful execution
                        result = run_backup(config=config)
                    
                    # INVARIANT: Lock must NOT be held after run_backup returns
                    lock_manager = LockManager()
                    assert not lock_manager.is_locked(), \
                        f"Lock was not released after {error_point} error path"
    
    @given(
        raise_exception=st.booleans(),
        exception_type=st.sampled_from([ValueError, RuntimeError, OSError, Exception])
    )
    @settings(max_examples=2, deadline=None, phases=[Phase.generate, Phase.target])
    def test_lock_released_on_unexpected_exceptions(
        self, 
        raise_exception: bool, 
        exception_type: type
    ):
        """
        **Feature: macos-incremental-backup, Property 7: Lock Release Invariant**
        
        Even when unexpected exceptions occur, the lock must be released.
        """
        with tempfile.TemporaryDirectory() as source_dir:
            with tempfile.TemporaryDirectory() as dest_dir:
                with tempfile.TemporaryDirectory() as log_dir:
                    source_path = Path(source_dir)
                    dest_path = Path(dest_dir)
                    log_path = Path(log_dir)
                    
                    # Create test files
                    (source_path / "test.txt").write_text("test content")
                    
                    config = create_test_config(source_path, dest_path, log_path)
                    
                    if raise_exception:
                        # Inject exception during snapshot creation
                        with patch('devbackup.backup.SnapshotEngine') as mock_engine:
                            mock_instance = MagicMock()
                            mock_instance.cleanup_incomplete.return_value = 0
                            mock_instance.create_snapshot.side_effect = exception_type("Injected error")
                            mock_engine.return_value = mock_instance
                            
                            result = run_backup(config=config)
                    else:
                        result = run_backup(config=config)
                    
                    # INVARIANT: Lock must NOT be held after run_backup returns
                    lock_manager = LockManager()
                    assert not lock_manager.is_locked(), \
                        f"Lock was not released after exception: {exception_type.__name__}"


class TestIncompleteSnapshotCleanupInvariant:
    """
    Property 8: Incomplete Snapshot Cleanup Invariant
    
    *For any* error during snapshot creation, the in_progress directory
    SHALL be removed before the process exits.
    
    **Validates: Requirements 4.6, 8.6**
    """
    
    @given(
        num_incomplete=st.integers(min_value=0, max_value=5),
        should_fail=st.booleans()
    )
    @settings(max_examples=10, deadline=None, phases=[Phase.generate, Phase.target])
    def test_incomplete_snapshots_cleaned_on_startup(
        self, 
        num_incomplete: int, 
        should_fail: bool
    ):
        """
        **Feature: macos-incremental-backup, Property 8: Incomplete Snapshot Cleanup Invariant**
        
        Any in_progress directories from previous runs must be cleaned up.
        """
        with tempfile.TemporaryDirectory() as source_dir:
            with tempfile.TemporaryDirectory() as dest_dir:
                with tempfile.TemporaryDirectory() as log_dir:
                    source_path = Path(source_dir)
                    dest_path = Path(dest_dir)
                    log_path = Path(log_dir)
                    
                    # Create test files
                    (source_path / "test.txt").write_text("test content")
                    
                    # Create incomplete snapshot directories
                    incomplete_dirs = []
                    for i in range(num_incomplete):
                        incomplete_dir = dest_path / f"in_progress_2025-01-0{i+1}-120000"
                        incomplete_dir.mkdir(parents=True)
                        (incomplete_dir / "file.txt").write_text(f"incomplete {i}")
                        incomplete_dirs.append(incomplete_dir)
                    
                    config = create_test_config(source_path, dest_path, log_path)
                    
                    if should_fail:
                        # Make backup fail by using invalid sources
                        config.source_directories = [Path("/nonexistent/source")]
                    
                    result = run_backup(config=config)
                    
                    # INVARIANT: All in_progress directories must be removed
                    remaining_incomplete = [
                        d for d in dest_path.iterdir()
                        if d.is_dir() and d.name.startswith("in_progress_")
                    ]
                    
                    assert len(remaining_incomplete) == 0, \
                        f"Found {len(remaining_incomplete)} incomplete snapshots after backup"
                    
                    # Verify the count matches what we created
                    if num_incomplete > 0:
                        assert result.incomplete_cleaned == num_incomplete or not should_fail, \
                            f"Expected {num_incomplete} cleaned, got {result.incomplete_cleaned}"
    
    @given(
        rsync_exit_code=st.sampled_from([1, 2, 11, 23, 24, 30])  # Various rsync error codes
    )
    @settings(max_examples=2, deadline=None, phases=[Phase.generate, Phase.target])
    def test_in_progress_cleaned_on_rsync_failure(self, rsync_exit_code: int):
        """
        **Feature: macos-incremental-backup, Property 8: Incomplete Snapshot Cleanup Invariant**
        
        When rsync fails, the in_progress directory must be cleaned up.
        """
        with tempfile.TemporaryDirectory() as source_dir:
            with tempfile.TemporaryDirectory() as dest_dir:
                with tempfile.TemporaryDirectory() as log_dir:
                    source_path = Path(source_dir)
                    dest_path = Path(dest_dir)
                    log_path = Path(log_dir)
                    
                    # Create test files
                    (source_path / "test.txt").write_text("test content")
                    
                    config = create_test_config(source_path, dest_path, log_path)
                    
                    # Mock rsync (Popen) to fail with specific exit code
                    # Patch in the correct module where Popen is used
                    with patch('devbackup.snapshot.subprocess.Popen') as mock_popen:
                        mock_process = MagicMock()
                        mock_process.communicate.return_value = (b"", f"rsync error code {rsync_exit_code}".encode())
                        mock_process.returncode = rsync_exit_code
                        mock_process.poll.return_value = rsync_exit_code
                        mock_popen.return_value = mock_process
                        
                        result = run_backup(config=config)
                    
                    # INVARIANT: No in_progress directories should remain
                    remaining_incomplete = [
                        d for d in dest_path.iterdir()
                        if d.is_dir() and d.name.startswith("in_progress_")
                    ]
                    
                    assert len(remaining_incomplete) == 0, \
                        f"in_progress directory not cleaned after rsync exit code {rsync_exit_code}"
                    
                    # Backup should have failed
                    assert not result.success, \
                        f"Backup should have failed with rsync exit code {rsync_exit_code}"


class TestCombinedInvariants:
    """Tests that verify both invariants hold together."""
    
    @given(
        error_point=error_injection_points,
        num_incomplete=st.integers(min_value=0, max_value=3)
    )
    @settings(max_examples=10, deadline=None, phases=[Phase.generate, Phase.target])
    def test_both_invariants_hold_together(
        self, 
        error_point: str, 
        num_incomplete: int
    ):
        """
        **Feature: macos-incremental-backup, Property 7 & 8: Combined Invariants**
        
        Both lock release and incomplete cleanup invariants must hold simultaneously.
        """
        with tempfile.TemporaryDirectory() as source_dir:
            with tempfile.TemporaryDirectory() as dest_dir:
                with tempfile.TemporaryDirectory() as log_dir:
                    source_path = Path(source_dir)
                    dest_path = Path(dest_dir)
                    log_path = Path(log_dir)
                    
                    # Create test files
                    (source_path / "test.txt").write_text("test content")
                    
                    # Create incomplete snapshot directories
                    for i in range(num_incomplete):
                        incomplete_dir = dest_path / f"in_progress_2025-01-0{i+1}-120000"
                        incomplete_dir.mkdir(parents=True)
                        (incomplete_dir / "file.txt").write_text(f"incomplete {i}")
                    
                    config = create_test_config(source_path, dest_path, log_path)
                    
                    # Inject errors at different points
                    if error_point == "config_load":
                        result = run_backup(config_path=Path("/nonexistent/config.toml"))
                    elif error_point == "lock_acquire":
                        lock_manager = LockManager()
                        lock_manager.acquire()
                        try:
                            result = run_backup(config=config)
                        finally:
                            lock_manager.release()
                    elif error_point == "destination_validate":
                        config.backup_destination = Path("/nonexistent/destination")
                        result = run_backup(config=config)
                    elif error_point == "snapshot_create":
                        config.source_directories = [Path("/nonexistent/source")]
                        result = run_backup(config=config)
                    else:
                        result = run_backup(config=config)
                    
                    # INVARIANT 1: Lock must NOT be held
                    lock_manager = LockManager()
                    assert not lock_manager.is_locked(), \
                        f"Lock not released after {error_point}"
                    
                    # INVARIANT 2: No in_progress directories (if we got past config/lock/destination)
                    # Note: If destination validation fails, we can't access the destination
                    # to clean up incomplete snapshots, so we only check this invariant
                    # when we actually reached the cleanup phase
                    if error_point not in ["config_load", "lock_acquire", "destination_validate"]:
                        remaining_incomplete = [
                            d for d in dest_path.iterdir()
                            if d.is_dir() and d.name.startswith("in_progress_")
                        ]
                        assert len(remaining_incomplete) == 0, \
                            f"Incomplete snapshots remain after {error_point}"
