"""Property-based tests for RetentionManager.

**Property 5: Retention Safety During Active Backup**
**Validates: Requirements 5.1, 5.3, 5.4**

**Property 6: Retention Policy Correctness**
**Validates: Requirements 5.2, 5.3, 5.4, 5.5, 5.6**

Tests that:
- The N most recent snapshots are kept (hourly)
- The first snapshot of each day for last N days is kept (daily)
- The first snapshot of each week (Sunday start) for last N weeks is kept (weekly)
- Snapshots outside all retention windows are deleted
- Protected snapshots (link-dest targets) are preserved during active backups
"""

import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Set

import pytest
from hypothesis import given, settings, assume, HealthCheck
from hypothesis import strategies as st

from devbackup.retention import RetentionManager


# Strategy for generating retention config values
retention_config_strategy = st.fixed_dictionaries({
    "hourly": st.integers(min_value=0, max_value=48),
    "daily": st.integers(min_value=0, max_value=14),
    "weekly": st.integers(min_value=0, max_value=8),
})


def timestamp_to_dirname(ts: datetime) -> str:
    """Convert datetime to snapshot directory name format."""
    return ts.strftime("%Y-%m-%d-%H%M%S")


def create_snapshot_dirs(tmp_path: Path, timestamps: List[datetime]) -> List[Path]:
    """Create snapshot directories for given timestamps."""
    snapshots = []
    for ts in timestamps:
        dirname = timestamp_to_dirname(ts)
        snap_path = tmp_path / dirname
        snap_path.mkdir(exist_ok=True)
        snapshots.append(snap_path)
    return snapshots


def get_week_start(dt: datetime) -> datetime:
    """Get the Sunday that starts the week containing the given datetime."""
    days_since_sunday = (dt.weekday() + 1) % 7
    sunday = dt - timedelta(days=days_since_sunday)
    return datetime(sunday.year, sunday.month, sunday.day)


class TestRetentionPolicyCorrectnessProperty:
    """
    Property 6: Retention Policy Correctness
    
    *For any* set of snapshots and retention configuration (H hourly, D daily, W weekly),
    applying the retention policy SHALL keep:
    - The H most recent snapshots
    - The earliest snapshot of each of the last D calendar days
    - The earliest snapshot of each of the last W weeks (Sunday-start)
    - No other snapshots
    
    **Validates: Requirements 5.2, 5.3, 5.4, 5.5, 5.6**
    """
    
    @given(
        retention_config=retention_config_strategy,
        num_snapshots=st.integers(min_value=1, max_value=50),
        seed=st.integers(min_value=0, max_value=10000),
    )
    @settings(
        max_examples=10,
        deadline=None,
        suppress_health_check=[HealthCheck.too_slow],
    )
    def test_retention_policy_correctness_property(
        self,
        retention_config: dict,
        num_snapshots: int,
        seed: int,
    ):
        """
        Feature: macos-incremental-backup, Property 6: Retention Policy Correctness
        
        For any set of snapshots and retention config, verify that:
        1. All N most recent hourly snapshots are kept
        2. First-of-day snapshots for last N days are kept
        3. First-of-week snapshots for last N weeks are kept
        4. Only snapshots matching these criteria are kept
        """
        import random
        random.seed(seed)
        
        hourly = retention_config["hourly"]
        daily = retention_config["daily"]
        weekly = retention_config["weekly"]
        
        # Skip if all retention values are 0 (nothing to keep)
        assume(hourly > 0 or daily > 0 or weekly > 0)
        
        # Use a temporary directory for each test run
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            
            # Generate random timestamps spread over 60 days
            base_time = datetime(2025, 1, 15, 12, 0, 0)
            timestamps = []
            for _ in range(num_snapshots):
                minutes_back = random.randint(0, 60 * 24 * 60)
                ts = base_time - timedelta(minutes=minutes_back)
                # Round to minute to avoid duplicate directory names
                ts = ts.replace(second=0, microsecond=0)
                timestamps.append(ts)
            
            # Remove duplicates (same minute)
            timestamps = list(set(timestamps))
            assume(len(timestamps) >= 1)
            
            # Create snapshot directories
            snapshots = create_snapshot_dirs(tmp_path, timestamps)
            
            # Create manager and get snapshots to keep
            manager = RetentionManager(tmp_path, hourly, daily, weekly)
            to_keep = manager.get_snapshots_to_keep(snapshots)
            
            # Sort timestamps descending (most recent first)
            sorted_timestamps = sorted(timestamps, reverse=True)
            most_recent_time = sorted_timestamps[0]
            
            # Build expected set of snapshots to keep
            expected_to_keep: Set[Path] = set()
            
            # 1. N most recent hourly snapshots
            for i, ts in enumerate(sorted_timestamps):
                if i < hourly:
                    expected_to_keep.add(tmp_path / timestamp_to_dirname(ts))
            
            # 2. First-of-day for last N days
            for days_ago in range(daily):
                target_date = (most_recent_time - timedelta(days=days_ago)).date()
                # Find earliest snapshot on that day
                day_snapshots = [
                    ts for ts in timestamps
                    if ts.date() == target_date
                ]
                if day_snapshots:
                    earliest = min(day_snapshots)
                    expected_to_keep.add(tmp_path / timestamp_to_dirname(earliest))
            
            # 3. First-of-week for last N weeks
            current_week_start = get_week_start(most_recent_time)
            for weeks_ago in range(weekly):
                target_week_start = current_week_start - timedelta(weeks=weeks_ago)
                target_week_end = target_week_start + timedelta(days=7)
                # Find earliest snapshot in that week
                week_snapshots = [
                    ts for ts in timestamps
                    if target_week_start <= ts < target_week_end
                ]
                if week_snapshots:
                    earliest = min(week_snapshots)
                    expected_to_keep.add(tmp_path / timestamp_to_dirname(earliest))
            
            # Verify: all expected snapshots are kept
            for expected in expected_to_keep:
                assert expected in to_keep, \
                    f"Expected snapshot {expected.name} to be kept but it wasn't"
            
            # Verify: no unexpected snapshots are kept
            for kept in to_keep:
                assert kept in expected_to_keep, \
                    f"Snapshot {kept.name} was kept but shouldn't have been"


class TestRetentionSafetyDuringActiveBackupProperty:
    """
    Property 5: Retention Safety During Active Backup
    
    *For any* retention operation while an in_progress backup exists,
    the most recent complete snapshot and any snapshot used as link-dest
    SHALL be preserved.
    
    **Validates: Requirements 5.1, 5.3, 5.4**
    """
    
    @given(
        retention_config=retention_config_strategy,
        num_snapshots=st.integers(min_value=1, max_value=30),
        num_in_progress=st.integers(min_value=1, max_value=3),
        seed=st.integers(min_value=0, max_value=10000),
    )
    @settings(
        max_examples=10,
        deadline=None,
        suppress_health_check=[HealthCheck.too_slow],
    )
    def test_retention_safety_during_active_backup_property(
        self,
        retention_config: dict,
        num_snapshots: int,
        num_in_progress: int,
        seed: int,
    ):
        """
        Feature: backup-robustness, Property 5: Retention Safety During Active Backup
        
        For any retention operation while in_progress backup exists:
        1. The most recent complete snapshot SHALL be preserved
        2. The link-dest target SHALL NOT be deleted
        3. The in_progress directory SHALL NOT be touched
        
        **Validates: Requirements 5.1, 5.3, 5.4**
        """
        import random
        random.seed(seed)
        
        hourly = retention_config["hourly"]
        daily = retention_config["daily"]
        weekly = retention_config["weekly"]
        
        # Use a temporary directory for each test run
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            
            # Generate random timestamps spread over 30 days
            base_time = datetime(2025, 1, 15, 12, 0, 0)
            timestamps = []
            for _ in range(num_snapshots):
                minutes_back = random.randint(0, 30 * 24 * 60)
                ts = base_time - timedelta(minutes=minutes_back)
                # Round to minute to avoid duplicate directory names
                ts = ts.replace(second=0, microsecond=0)
                timestamps.append(ts)
            
            # Remove duplicates (same minute)
            timestamps = list(set(timestamps))
            assume(len(timestamps) >= 1)
            
            # Create snapshot directories with some content
            snapshots = []
            for ts in timestamps:
                dirname = timestamp_to_dirname(ts)
                snap_path = tmp_path / dirname
                snap_path.mkdir(exist_ok=True)
                # Add a file so the snapshot has content
                (snap_path / "test.txt").write_text(f"snapshot {dirname}")
                snapshots.append(snap_path)
            
            # Find the most recent snapshot (this is the link-dest target)
            sorted_timestamps = sorted(timestamps, reverse=True)
            most_recent_ts = sorted_timestamps[0]
            most_recent_snapshot = tmp_path / timestamp_to_dirname(most_recent_ts)
            
            # Create in_progress directories (after the most recent snapshot)
            in_progress_dirs = []
            for i in range(num_in_progress):
                # in_progress timestamps are after the most recent snapshot
                in_progress_ts = most_recent_ts + timedelta(minutes=i + 1)
                in_progress_name = f"in_progress_{timestamp_to_dirname(in_progress_ts)}"
                in_progress_path = tmp_path / in_progress_name
                in_progress_path.mkdir()
                (in_progress_path / "test.txt").write_text("in progress")
                in_progress_dirs.append(in_progress_path)
            
            # Create manager and apply retention
            manager = RetentionManager(tmp_path, hourly, daily, weekly)
            result = manager.apply_retention()
            
            # Property 1: Most recent complete snapshot SHALL be preserved
            # (Requirements 5.3, 5.4)
            assert most_recent_snapshot.exists(), \
                f"Most recent snapshot {most_recent_snapshot.name} was deleted " \
                f"but should be protected as link-dest target"
            
            assert most_recent_snapshot in result.kept_snapshots, \
                f"Most recent snapshot {most_recent_snapshot.name} not in kept_snapshots"
            
            # Property 2: Most recent snapshot SHALL NOT be in deleted list
            assert most_recent_snapshot not in result.deleted_snapshots, \
                f"Most recent snapshot {most_recent_snapshot.name} was in deleted_snapshots " \
                f"but should be protected"
            
            # Property 3: All in_progress directories SHALL still exist
            # (they should not be touched by retention)
            for in_progress in in_progress_dirs:
                assert in_progress.exists(), \
                    f"in_progress directory {in_progress.name} was deleted " \
                    f"but should not be touched by retention"
    
    @given(
        retention_config=st.fixed_dictionaries({
            "hourly": st.just(0),  # Force 0 hourly to test protection
            "daily": st.just(0),   # Force 0 daily to test protection
            "weekly": st.just(0),  # Force 0 weekly to test protection
        }),
        num_snapshots=st.integers(min_value=2, max_value=10),
        seed=st.integers(min_value=0, max_value=10000),
    )
    @settings(
        max_examples=10,
        deadline=None,
        suppress_health_check=[HealthCheck.too_slow],
    )
    def test_link_dest_protected_even_with_zero_retention(
        self,
        retention_config: dict,
        num_snapshots: int,
        seed: int,
    ):
        """
        Feature: backup-robustness, Property 5: Link-dest Protection Override
        
        Even when retention policy would delete all snapshots (0 hourly, 0 daily, 0 weekly),
        the most recent snapshot SHALL be preserved if an in_progress backup exists.
        
        **Validates: Requirements 5.1, 5.3, 5.4**
        """
        import random
        random.seed(seed)
        
        # Use a temporary directory for each test run
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            
            # Generate random timestamps
            base_time = datetime(2025, 1, 15, 12, 0, 0)
            timestamps = []
            for i in range(num_snapshots):
                ts = base_time - timedelta(hours=i)
                timestamps.append(ts)
            
            # Create snapshot directories with content
            for ts in timestamps:
                dirname = timestamp_to_dirname(ts)
                snap_path = tmp_path / dirname
                snap_path.mkdir()
                (snap_path / "test.txt").write_text(f"snapshot {dirname}")
            
            # Find the most recent snapshot
            most_recent_ts = max(timestamps)
            most_recent_snapshot = tmp_path / timestamp_to_dirname(most_recent_ts)
            
            # Create an in_progress directory
            in_progress_ts = most_recent_ts + timedelta(minutes=1)
            in_progress_name = f"in_progress_{timestamp_to_dirname(in_progress_ts)}"
            in_progress_path = tmp_path / in_progress_name
            in_progress_path.mkdir()
            
            # Create manager with zero retention and apply
            manager = RetentionManager(tmp_path, hourly=0, daily=0, weekly=0)
            result = manager.apply_retention()
            
            # The most recent snapshot MUST be preserved despite zero retention
            assert most_recent_snapshot.exists(), \
                f"Most recent snapshot {most_recent_snapshot.name} was deleted " \
                f"despite being link-dest target for in_progress backup"
            
            assert most_recent_snapshot in result.kept_snapshots, \
                f"Most recent snapshot should be in kept_snapshots due to protection"
            
            # All other snapshots should be deleted (they're not protected)
            for ts in timestamps:
                if ts != most_recent_ts:
                    snap_path = tmp_path / timestamp_to_dirname(ts)
                    assert not snap_path.exists(), \
                        f"Snapshot {snap_path.name} should have been deleted " \
                        f"(not protected, zero retention)"
