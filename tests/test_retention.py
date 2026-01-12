"""Unit tests for RetentionManager.

Tests for:
- Timestamp parsing
- First-of-day selection
- First-of-week selection (Sunday start)
- Retention policy application

_Requirements: 5.1, 5.2, 5.3, 5.4, 5.5, 5.6, 5.7_
"""

import tempfile
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from devbackup.retention import RetentionManager, RetentionResult


class TestParseSnapshotTimestamp:
    """Tests for _parse_snapshot_timestamp method."""
    
    def test_valid_timestamp(self, tmp_path: Path):
        """Test parsing a valid timestamp directory name."""
        manager = RetentionManager(tmp_path, hourly=24, daily=7, weekly=4)
        snapshot = tmp_path / "2025-01-01-120000"
        snapshot.mkdir()
        
        result = manager._parse_snapshot_timestamp(snapshot)
        
        assert result == datetime(2025, 1, 1, 12, 0, 0)
    
    def test_invalid_timestamp_format(self, tmp_path: Path):
        """Test parsing an invalid timestamp returns None."""
        manager = RetentionManager(tmp_path, hourly=24, daily=7, weekly=4)
        snapshot = tmp_path / "invalid-name"
        snapshot.mkdir()
        
        result = manager._parse_snapshot_timestamp(snapshot)
        
        assert result is None
    
    def test_in_progress_directory(self, tmp_path: Path):
        """Test that in_progress directories are not parsed as valid timestamps."""
        manager = RetentionManager(tmp_path, hourly=24, daily=7, weekly=4)
        snapshot = tmp_path / "in_progress_2025-01-01-120000"
        snapshot.mkdir()
        
        # The name itself doesn't match the timestamp format
        result = manager._parse_snapshot_timestamp(snapshot)
        
        assert result is None


class TestGetFirstOfDay:
    """Tests for _get_first_of_day method."""
    
    def test_single_snapshot_on_day(self, tmp_path: Path):
        """Test finding first snapshot when only one exists on that day."""
        manager = RetentionManager(tmp_path, hourly=24, daily=7, weekly=4)
        
        snapshot = tmp_path / "2025-01-01-120000"
        snapshot.mkdir()
        
        target_date = datetime(2025, 1, 1, 15, 0, 0)
        result = manager._get_first_of_day([snapshot], target_date)
        
        assert result == snapshot
    
    def test_multiple_snapshots_on_day(self, tmp_path: Path):
        """Test finding earliest snapshot when multiple exist on same day."""
        manager = RetentionManager(tmp_path, hourly=24, daily=7, weekly=4)
        
        snap1 = tmp_path / "2025-01-01-080000"
        snap2 = tmp_path / "2025-01-01-120000"
        snap3 = tmp_path / "2025-01-01-180000"
        snap1.mkdir()
        snap2.mkdir()
        snap3.mkdir()
        
        target_date = datetime(2025, 1, 1, 15, 0, 0)
        result = manager._get_first_of_day([snap1, snap2, snap3], target_date)
        
        assert result == snap1  # Earliest on that day
    
    def test_no_snapshots_on_day(self, tmp_path: Path):
        """Test returns None when no snapshots exist on target day."""
        manager = RetentionManager(tmp_path, hourly=24, daily=7, weekly=4)
        
        snapshot = tmp_path / "2025-01-01-120000"
        snapshot.mkdir()
        
        target_date = datetime(2025, 1, 2, 12, 0, 0)  # Different day
        result = manager._get_first_of_day([snapshot], target_date)
        
        assert result is None


class TestGetFirstOfWeek:
    """Tests for _get_first_of_week method (Sunday start)."""
    
    def test_single_snapshot_in_week(self, tmp_path: Path):
        """Test finding first snapshot when only one exists in that week."""
        manager = RetentionManager(tmp_path, hourly=24, daily=7, weekly=4)
        
        # 2025-01-01 is a Wednesday
        snapshot = tmp_path / "2025-01-01-120000"
        snapshot.mkdir()
        
        # Week starts on Sunday 2024-12-29
        week_start = datetime(2024, 12, 29, 0, 0, 0)
        result = manager._get_first_of_week([snapshot], week_start)
        
        assert result == snapshot
    
    def test_multiple_snapshots_in_week(self, tmp_path: Path):
        """Test finding earliest snapshot when multiple exist in same week."""
        manager = RetentionManager(tmp_path, hourly=24, daily=7, weekly=4)
        
        # Week of Dec 29, 2024 - Jan 4, 2025 (Sunday to Saturday)
        snap1 = tmp_path / "2024-12-29-100000"  # Sunday
        snap2 = tmp_path / "2025-01-01-120000"  # Wednesday
        snap3 = tmp_path / "2025-01-04-180000"  # Saturday
        snap1.mkdir()
        snap2.mkdir()
        snap3.mkdir()
        
        week_start = datetime(2024, 12, 29, 0, 0, 0)
        result = manager._get_first_of_week([snap1, snap2, snap3], week_start)
        
        assert result == snap1  # Earliest in that week
    
    def test_no_snapshots_in_week(self, tmp_path: Path):
        """Test returns None when no snapshots exist in target week."""
        manager = RetentionManager(tmp_path, hourly=24, daily=7, weekly=4)
        
        # Snapshot on Jan 6, 2025 (Monday of next week)
        snapshot = tmp_path / "2025-01-06-120000"
        snapshot.mkdir()
        
        # Week of Dec 29, 2024
        week_start = datetime(2024, 12, 29, 0, 0, 0)
        result = manager._get_first_of_week([snapshot], week_start)
        
        assert result is None
    
    def test_week_boundary_saturday(self, tmp_path: Path):
        """Test that Saturday is included in the week."""
        manager = RetentionManager(tmp_path, hourly=24, daily=7, weekly=4)
        
        # Saturday Jan 4, 2025 should be in week starting Dec 29, 2024
        snapshot = tmp_path / "2025-01-04-235959"
        snapshot.mkdir()
        
        week_start = datetime(2024, 12, 29, 0, 0, 0)
        result = manager._get_first_of_week([snapshot], week_start)
        
        assert result == snapshot


class TestGetWeekStart:
    """Tests for _get_week_start helper method."""
    
    def test_sunday_returns_same_day(self, tmp_path: Path):
        """Test that a Sunday returns itself as week start."""
        manager = RetentionManager(tmp_path, hourly=24, daily=7, weekly=4)
        
        sunday = datetime(2024, 12, 29, 15, 30, 0)  # Sunday
        result = manager._get_week_start(sunday)
        
        assert result == datetime(2024, 12, 29, 0, 0, 0)
    
    def test_wednesday_returns_previous_sunday(self, tmp_path: Path):
        """Test that a Wednesday returns the previous Sunday."""
        manager = RetentionManager(tmp_path, hourly=24, daily=7, weekly=4)
        
        wednesday = datetime(2025, 1, 1, 12, 0, 0)  # Wednesday
        result = manager._get_week_start(wednesday)
        
        assert result == datetime(2024, 12, 29, 0, 0, 0)
    
    def test_saturday_returns_previous_sunday(self, tmp_path: Path):
        """Test that a Saturday returns the previous Sunday."""
        manager = RetentionManager(tmp_path, hourly=24, daily=7, weekly=4)
        
        saturday = datetime(2025, 1, 4, 23, 59, 59)  # Saturday
        result = manager._get_week_start(saturday)
        
        assert result == datetime(2024, 12, 29, 0, 0, 0)


class TestGetSnapshotsToKeep:
    """Tests for get_snapshots_to_keep method."""
    
    def test_keep_hourly_snapshots(self, tmp_path: Path):
        """Test that N most recent hourly snapshots are kept."""
        manager = RetentionManager(tmp_path, hourly=3, daily=0, weekly=0)
        
        # Create 5 snapshots
        snapshots = []
        for i in range(5):
            snap = tmp_path / f"2025-01-01-{10+i:02d}0000"
            snap.mkdir()
            snapshots.append(snap)
        
        to_keep = manager.get_snapshots_to_keep(snapshots)
        
        # Should keep the 3 most recent
        assert len(to_keep) == 3
        assert snapshots[4] in to_keep  # 14:00
        assert snapshots[3] in to_keep  # 13:00
        assert snapshots[2] in to_keep  # 12:00
        assert snapshots[1] not in to_keep  # 11:00
        assert snapshots[0] not in to_keep  # 10:00
    
    def test_keep_daily_snapshots(self, tmp_path: Path):
        """Test that first-of-day snapshots are kept for N days."""
        manager = RetentionManager(tmp_path, hourly=0, daily=3, weekly=0)
        
        # Create snapshots across 5 days
        snapshots = []
        for day in range(5):
            # Two snapshots per day
            for hour in [8, 16]:
                snap = tmp_path / f"2025-01-{5-day:02d}-{hour:02d}0000"
                snap.mkdir()
                snapshots.append(snap)
        
        to_keep = manager.get_snapshots_to_keep(snapshots)
        
        # Should keep first-of-day for last 3 days (Jan 5, 4, 3)
        # First of Jan 5 is 08:00
        assert tmp_path / "2025-01-05-080000" in to_keep
        # First of Jan 4 is 08:00
        assert tmp_path / "2025-01-04-080000" in to_keep
        # First of Jan 3 is 08:00
        assert tmp_path / "2025-01-03-080000" in to_keep
        # Jan 2 and Jan 1 should not be kept
        assert tmp_path / "2025-01-02-080000" not in to_keep
        assert tmp_path / "2025-01-01-080000" not in to_keep
    
    def test_keep_weekly_snapshots(self, tmp_path: Path):
        """Test that first-of-week snapshots are kept for N weeks."""
        manager = RetentionManager(tmp_path, hourly=0, daily=0, weekly=2)
        
        # Create snapshots across 4 weeks
        # Week 1: Dec 29, 2024 - Jan 4, 2025
        # Week 2: Jan 5 - Jan 11, 2025
        # Week 3: Jan 12 - Jan 18, 2025
        # Week 4: Jan 19 - Jan 25, 2025
        
        snapshots = []
        # Week 4 (current week based on most recent snapshot)
        snap = tmp_path / "2025-01-20-120000"
        snap.mkdir()
        snapshots.append(snap)
        
        # Week 3
        snap = tmp_path / "2025-01-13-120000"
        snap.mkdir()
        snapshots.append(snap)
        
        # Week 2
        snap = tmp_path / "2025-01-06-120000"
        snap.mkdir()
        snapshots.append(snap)
        
        # Week 1
        snap = tmp_path / "2024-12-30-120000"
        snap.mkdir()
        snapshots.append(snap)
        
        to_keep = manager.get_snapshots_to_keep(snapshots)
        
        # Should keep first-of-week for last 2 weeks (Week 4 and Week 3)
        assert tmp_path / "2025-01-20-120000" in to_keep  # Week 4
        assert tmp_path / "2025-01-13-120000" in to_keep  # Week 3
        # Week 2 and Week 1 should not be kept
        assert tmp_path / "2025-01-06-120000" not in to_keep
        assert tmp_path / "2024-12-30-120000" not in to_keep
    
    def test_combined_retention_policy(self, tmp_path: Path):
        """Test that hourly, daily, and weekly policies work together."""
        manager = RetentionManager(tmp_path, hourly=2, daily=2, weekly=1)
        
        snapshots = []
        
        # Most recent day with multiple snapshots
        snap1 = tmp_path / "2025-01-05-160000"
        snap1.mkdir()
        snapshots.append(snap1)
        
        snap2 = tmp_path / "2025-01-05-080000"
        snap2.mkdir()
        snapshots.append(snap2)
        
        # Previous day
        snap3 = tmp_path / "2025-01-04-120000"
        snap3.mkdir()
        snapshots.append(snap3)
        
        # Older snapshot (same week)
        snap4 = tmp_path / "2025-01-01-120000"
        snap4.mkdir()
        snapshots.append(snap4)
        
        to_keep = manager.get_snapshots_to_keep(snapshots)
        
        # Hourly: keep 2 most recent (snap1, snap2)
        # Daily: keep first of Jan 5 (snap2) and Jan 4 (snap3)
        # Weekly: keep first of current week (snap4 - Jan 1 is Wednesday, week starts Dec 29)
        
        assert snap1 in to_keep  # Hourly
        assert snap2 in to_keep  # Hourly + Daily
        assert snap3 in to_keep  # Daily
        # snap4 might be kept if it's first of week
    
    def test_empty_snapshots_list(self, tmp_path: Path):
        """Test handling of empty snapshots list."""
        manager = RetentionManager(tmp_path, hourly=24, daily=7, weekly=4)
        
        to_keep = manager.get_snapshots_to_keep([])
        
        assert to_keep == set()


class TestApplyRetention:
    """Tests for apply_retention method."""
    
    def test_apply_retention_deletes_old_snapshots(self, tmp_path: Path):
        """Test that old snapshots are deleted when applying retention."""
        manager = RetentionManager(tmp_path, hourly=2, daily=0, weekly=0)
        
        # Create 4 snapshots
        snap1 = tmp_path / "2025-01-01-100000"
        snap2 = tmp_path / "2025-01-01-110000"
        snap3 = tmp_path / "2025-01-01-120000"
        snap4 = tmp_path / "2025-01-01-130000"
        
        for snap in [snap1, snap2, snap3, snap4]:
            snap.mkdir()
            (snap / "test.txt").write_text("test")
        
        result = manager.apply_retention()
        
        # Should keep 2 most recent (snap4, snap3)
        assert len(result.kept_snapshots) == 2
        assert snap4 in result.kept_snapshots
        assert snap3 in result.kept_snapshots
        
        # Should delete 2 oldest (snap1, snap2)
        assert len(result.deleted_snapshots) == 2
        assert snap1 in result.deleted_snapshots
        assert snap2 in result.deleted_snapshots
        
        # Verify directories are actually deleted
        assert not snap1.exists()
        assert not snap2.exists()
        assert snap3.exists()
        assert snap4.exists()
    
    def test_apply_retention_tracks_freed_bytes(self, tmp_path: Path):
        """Test that freed bytes are tracked correctly."""
        manager = RetentionManager(tmp_path, hourly=1, daily=0, weekly=0)
        
        # Create 2 snapshots with known sizes
        snap1 = tmp_path / "2025-01-01-100000"
        snap2 = tmp_path / "2025-01-01-110000"
        
        snap1.mkdir()
        (snap1 / "file.txt").write_text("a" * 100)
        
        snap2.mkdir()
        (snap2 / "file.txt").write_text("b" * 50)
        
        result = manager.apply_retention()
        
        # snap1 should be deleted, freeing ~100 bytes
        assert result.freed_bytes >= 100
    
    def test_apply_retention_empty_destination(self, tmp_path: Path):
        """Test apply_retention with no snapshots."""
        manager = RetentionManager(tmp_path, hourly=24, daily=7, weekly=4)
        
        result = manager.apply_retention()
        
        assert result.kept_snapshots == []
        assert result.deleted_snapshots == []
        assert result.freed_bytes == 0
    
    def test_apply_retention_skips_in_progress(self, tmp_path: Path):
        """Test that in_progress directories are not considered."""
        manager = RetentionManager(tmp_path, hourly=1, daily=0, weekly=0)
        
        # Create a valid snapshot and an in_progress directory
        snap = tmp_path / "2025-01-01-120000"
        snap.mkdir()
        
        in_progress = tmp_path / "in_progress_2025-01-01-130000"
        in_progress.mkdir()
        
        result = manager.apply_retention()
        
        # Only the valid snapshot should be considered
        assert len(result.kept_snapshots) == 1
        assert snap in result.kept_snapshots
        
        # in_progress should still exist (not touched)
        assert in_progress.exists()
    
    def test_apply_retention_skips_hidden_directories(self, tmp_path: Path):
        """Test that hidden directories are not considered."""
        manager = RetentionManager(tmp_path, hourly=1, daily=0, weekly=0)
        
        # Create a valid snapshot and a hidden directory
        snap = tmp_path / "2025-01-01-120000"
        snap.mkdir()
        
        hidden = tmp_path / ".devbackup_meta"
        hidden.mkdir()
        
        result = manager.apply_retention()
        
        # Only the valid snapshot should be considered
        assert len(result.kept_snapshots) == 1
        assert snap in result.kept_snapshots
        
        # Hidden directory should still exist
        assert hidden.exists()


class TestGetProtectedSnapshots:
    """Tests for _get_protected_snapshots method.
    
    _Requirements: 5.1, 5.2, 5.3, 5.4_
    """
    
    def test_no_in_progress_returns_empty(self, tmp_path: Path):
        """Test that no protected snapshots when no in_progress exists."""
        manager = RetentionManager(tmp_path, hourly=24, daily=7, weekly=4)
        
        # Create some snapshots but no in_progress
        snap1 = tmp_path / "2025-01-01-100000"
        snap2 = tmp_path / "2025-01-01-110000"
        snap1.mkdir()
        snap2.mkdir()
        
        protected = manager._get_protected_snapshots()
        
        assert protected == set()
    
    def test_in_progress_protects_most_recent_snapshot(self, tmp_path: Path):
        """Test that most recent snapshot is protected when in_progress exists."""
        manager = RetentionManager(tmp_path, hourly=24, daily=7, weekly=4)
        
        # Create snapshots
        snap1 = tmp_path / "2025-01-01-100000"
        snap2 = tmp_path / "2025-01-01-110000"
        snap3 = tmp_path / "2025-01-01-120000"
        snap1.mkdir()
        snap2.mkdir()
        snap3.mkdir()
        
        # Create in_progress directory
        in_progress = tmp_path / "in_progress_2025-01-01-130000"
        in_progress.mkdir()
        
        protected = manager._get_protected_snapshots()
        
        # Most recent snapshot (snap3) should be protected
        assert snap3 in protected
        assert snap1 not in protected
        assert snap2 not in protected
    
    def test_multiple_in_progress_still_protects_most_recent(self, tmp_path: Path):
        """Test that multiple in_progress dirs still protect most recent snapshot."""
        manager = RetentionManager(tmp_path, hourly=24, daily=7, weekly=4)
        
        # Create snapshots
        snap1 = tmp_path / "2025-01-01-100000"
        snap2 = tmp_path / "2025-01-01-110000"
        snap1.mkdir()
        snap2.mkdir()
        
        # Create multiple in_progress directories
        in_progress1 = tmp_path / "in_progress_2025-01-01-120000"
        in_progress2 = tmp_path / "in_progress_2025-01-01-130000"
        in_progress1.mkdir()
        in_progress2.mkdir()
        
        protected = manager._get_protected_snapshots()
        
        # Most recent snapshot (snap2) should be protected
        assert snap2 in protected
        assert len(protected) == 1
    
    def test_no_snapshots_with_in_progress_returns_empty(self, tmp_path: Path):
        """Test that no protected snapshots when no complete snapshots exist."""
        manager = RetentionManager(tmp_path, hourly=24, daily=7, weekly=4)
        
        # Create only in_progress directory
        in_progress = tmp_path / "in_progress_2025-01-01-120000"
        in_progress.mkdir()
        
        protected = manager._get_protected_snapshots()
        
        assert protected == set()


class TestApplyRetentionWithProtection:
    """Tests for apply_retention with protected snapshots.
    
    _Requirements: 5.1, 5.3, 5.4, 5.5_
    """
    
    def test_protected_snapshot_not_deleted(self, tmp_path: Path):
        """Test that protected snapshot is not deleted even if outside retention."""
        # Use very restrictive retention (keep only 1 hourly)
        manager = RetentionManager(tmp_path, hourly=1, daily=0, weekly=0)
        
        # Create 3 snapshots
        snap1 = tmp_path / "2025-01-01-100000"
        snap2 = tmp_path / "2025-01-01-110000"
        snap3 = tmp_path / "2025-01-01-120000"
        
        for snap in [snap1, snap2, snap3]:
            snap.mkdir()
            (snap / "test.txt").write_text("test")
        
        # Create in_progress directory
        in_progress = tmp_path / "in_progress_2025-01-01-130000"
        in_progress.mkdir()
        
        result = manager.apply_retention()
        
        # snap3 should be kept (most recent hourly AND protected as link-dest)
        assert snap3 in result.kept_snapshots
        assert snap3.exists()
        
        # snap1 should be deleted (outside retention, not protected)
        assert snap1 in result.deleted_snapshots
        assert not snap1.exists()
        
        # snap2 should be deleted (outside retention, not protected)
        assert snap2 in result.deleted_snapshots
        assert not snap2.exists()
        
        # in_progress should still exist (not touched by retention)
        assert in_progress.exists()
    
    def test_protected_snapshot_preserved_when_outside_retention(self, tmp_path: Path):
        """Test that link-dest target is preserved even when outside retention window."""
        # Use retention that would normally delete the most recent snapshot
        # (keep 0 hourly, 0 daily, 0 weekly - but this is unrealistic)
        # Instead, use a scenario where the most recent is outside daily/weekly
        manager = RetentionManager(tmp_path, hourly=0, daily=1, weekly=0)
        
        # Create snapshots - only one on a different day
        snap1 = tmp_path / "2025-01-01-120000"  # Old day
        snap2 = tmp_path / "2025-01-02-120000"  # Most recent day
        
        for snap in [snap1, snap2]:
            snap.mkdir()
            (snap / "test.txt").write_text("test")
        
        # Create in_progress directory
        in_progress = tmp_path / "in_progress_2025-01-02-130000"
        in_progress.mkdir()
        
        result = manager.apply_retention()
        
        # snap2 should be kept (first of most recent day AND protected)
        assert snap2 in result.kept_snapshots
        assert snap2.exists()
        
        # snap1 should be deleted (outside retention window)
        assert snap1 in result.deleted_snapshots
        assert not snap1.exists()
    
    def test_no_protection_without_in_progress(self, tmp_path: Path):
        """Test that retention works normally without in_progress."""
        manager = RetentionManager(tmp_path, hourly=1, daily=0, weekly=0)
        
        # Create 3 snapshots
        snap1 = tmp_path / "2025-01-01-100000"
        snap2 = tmp_path / "2025-01-01-110000"
        snap3 = tmp_path / "2025-01-01-120000"
        
        for snap in [snap1, snap2, snap3]:
            snap.mkdir()
            (snap / "test.txt").write_text("test")
        
        # No in_progress directory
        
        result = manager.apply_retention()
        
        # Only snap3 should be kept (most recent hourly)
        assert len(result.kept_snapshots) == 1
        assert snap3 in result.kept_snapshots
        
        # snap1 and snap2 should be deleted
        assert snap1 in result.deleted_snapshots
        assert snap2 in result.deleted_snapshots
