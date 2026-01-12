"""Property-based tests for Smart Scheduling Behavior.

Feature: user-experience-enhancement
Property 7: Smart Scheduling Behavior

**Validates: Requirements 8.1, 8.3, 8.4**
"""

import tempfile
from pathlib import Path
from unittest.mock import patch

import hypothesis.strategies as st
from hypothesis import given, settings, assume

from devbackup.scheduler import (
    check_battery_for_backup,
    check_destination_available,
    check_backup_conditions,
    queue_backup,
    load_backup_queue,
    save_backup_queue,
    clear_backup_queue,
    BackupSkipReason,
    QueuedBackup,
)
from devbackup.battery import BatteryStatus


class TestSmartSchedulingBehavior:
    """Property 7: Smart Scheduling Behavior."""

    @given(
        battery_level=st.integers(min_value=0, max_value=19),
        threshold=st.integers(min_value=20, max_value=100),
    )
    @settings(max_examples=100, deadline=None)
    def test_battery_low_not_charging_skips_backup(self, battery_level, threshold):
        """*For any* battery level below threshold and not charging, backup SHALL be skipped."""
        assume(battery_level < threshold)
        mock_status = BatteryStatus(level=battery_level, is_charging=False, is_present=True)
        with patch('devbackup.battery.get_battery_status', return_value=mock_status):
            result = check_battery_for_backup(threshold=threshold)
        assert not result.should_proceed
        assert result.skip_reason == BackupSkipReason.BATTERY_LOW

    @given(
        battery_level=st.integers(min_value=0, max_value=100),
        threshold=st.integers(min_value=1, max_value=100),
    )
    @settings(max_examples=100, deadline=None)
    def test_battery_charging_allows_backup(self, battery_level, threshold):
        """*For any* battery level when charging, backup SHALL proceed."""
        mock_status = BatteryStatus(level=battery_level, is_charging=True, is_present=True)
        with patch('devbackup.battery.get_battery_status', return_value=mock_status):
            result = check_battery_for_backup(threshold=threshold)
        assert result.should_proceed
        assert result.skip_reason == BackupSkipReason.NONE

    @given(
        battery_level=st.integers(min_value=20, max_value=100),
        threshold=st.integers(min_value=1, max_value=20),
    )
    @settings(max_examples=100, deadline=None)
    def test_battery_above_threshold_allows_backup(self, battery_level, threshold):
        """*For any* battery level at or above threshold, backup SHALL proceed."""
        assume(battery_level >= threshold)
        mock_status = BatteryStatus(level=battery_level, is_charging=False, is_present=True)
        with patch('devbackup.battery.get_battery_status', return_value=mock_status):
            result = check_battery_for_backup(threshold=threshold)
        assert result.should_proceed
        assert result.skip_reason == BackupSkipReason.NONE

    @given(battery_level=st.integers(min_value=0, max_value=100))
    @settings(max_examples=100, deadline=None)
    def test_no_battery_always_allows_backup(self, battery_level):
        """*For any* system without a battery (desktop Mac), backup SHALL always proceed."""
        mock_status = BatteryStatus(level=battery_level, is_charging=False, is_present=False)
        with patch('devbackup.battery.get_battery_status', return_value=mock_status):
            result = check_battery_for_backup(threshold=20)
        assert result.should_proceed
        assert result.skip_reason == BackupSkipReason.NONE

    def test_destination_unavailable_skips_backup(self):
        """When destination is unavailable, backup SHALL be skipped."""
        with tempfile.TemporaryDirectory() as tmpdir:
            nonexistent_path = Path(tmpdir) / "nonexistent" / "backup"
            result = check_destination_available(nonexistent_path)
            assert not result.should_proceed
            assert result.skip_reason == BackupSkipReason.DESTINATION_UNAVAILABLE

    def test_destination_available_allows_backup(self):
        """When destination is available and writable, backup SHALL proceed."""
        with tempfile.TemporaryDirectory() as tmpdir:
            dest_path = Path(tmpdir)
            result = check_destination_available(dest_path)
            assert result.should_proceed
            assert result.skip_reason == BackupSkipReason.NONE

    @given(battery_level=st.integers(min_value=0, max_value=19))
    @settings(max_examples=100, deadline=None)
    def test_combined_conditions_battery_low(self, battery_level):
        """*For any* low battery condition, combined check SHALL skip backup."""
        mock_status = BatteryStatus(level=battery_level, is_charging=False, is_present=True)
        with tempfile.TemporaryDirectory() as tmpdir:
            dest_path = Path(tmpdir)
            with patch('devbackup.battery.get_battery_status', return_value=mock_status):
                result = check_backup_conditions(dest_path, battery_threshold=20)
            assert not result.should_proceed
            assert result.skip_reason == BackupSkipReason.BATTERY_LOW

    @given(battery_level=st.integers(min_value=20, max_value=100))
    @settings(max_examples=100, deadline=None)
    def test_combined_conditions_all_good(self, battery_level):
        """*For any* good battery level and available destination, backup SHALL proceed."""
        mock_status = BatteryStatus(level=battery_level, is_charging=False, is_present=True)
        with tempfile.TemporaryDirectory() as tmpdir:
            dest_path = Path(tmpdir)
            with patch('devbackup.battery.get_battery_status', return_value=mock_status):
                result = check_backup_conditions(dest_path, battery_threshold=20)
            assert result.should_proceed
            assert result.skip_reason == BackupSkipReason.NONE


class TestBackupQueuePersistence:
    """Property 7: Smart Scheduling Behavior - Queue Persistence."""

    @given(
        reason=st.text(
            min_size=1,
            max_size=100,
            alphabet=st.characters(whitelist_categories=('L', 'N'), whitelist_characters=' '),
        ),
    )
    @settings(max_examples=100, deadline=None)
    def test_queue_backup_persists(self, reason):
        """*For any* queued backup, it SHALL be persisted and retrievable."""
        with tempfile.TemporaryDirectory() as tmpdir:
            queue_path = Path(tmpdir) / "queue.json"
            dest_path = Path(tmpdir) / "backup"
            queue_backup(dest_path, reason, queue_path=queue_path)
            loaded_queue = load_backup_queue(queue_path)
            assert len(loaded_queue) == 1
            assert loaded_queue[0].reason == reason
            assert loaded_queue[0].destination == str(dest_path)

    @given(num_entries=st.integers(min_value=1, max_value=10))
    @settings(max_examples=100, deadline=None)
    def test_queue_preserves_order(self, num_entries):
        """*For any* number of queued backups, order SHALL be preserved (FIFO)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            queue_path = Path(tmpdir) / "queue.json"
            dest_path = Path(tmpdir) / "backup"
            for i in range(num_entries):
                queue_backup(dest_path, f"reason_{i}", queue_path=queue_path)
            loaded_queue = load_backup_queue(queue_path)
            assert len(loaded_queue) == num_entries
            for i, entry in enumerate(loaded_queue):
                assert entry.reason == f"reason_{i}"

    def test_queue_survives_clear_and_reload(self):
        """Queue operations SHALL be atomic and survive process restarts."""
        with tempfile.TemporaryDirectory() as tmpdir:
            queue_path = Path(tmpdir) / "queue.json"
            dest_path = Path(tmpdir) / "backup"
            queue_backup(dest_path, "reason_1", queue_path=queue_path)
            queue_backup(dest_path, "reason_2", queue_path=queue_path)
            queue = load_backup_queue(queue_path)
            assert len(queue) == 2
            cleared = clear_backup_queue(queue_path)
            assert cleared == 2
            queue = load_backup_queue(queue_path)
            assert len(queue) == 0

    @given(
        reasons=st.lists(
            st.text(min_size=1, max_size=50, alphabet=st.characters(whitelist_categories=('L', 'N'))),
            min_size=1,
            max_size=5,
        ),
    )
    @settings(max_examples=100, deadline=None)
    def test_queue_round_trip(self, reasons):
        """*For any* list of queued backups, save and load SHALL produce equivalent queue."""
        with tempfile.TemporaryDirectory() as tmpdir:
            queue_path = Path(tmpdir) / "queue.json"
            dest_path = Path(tmpdir) / "backup"
            original_queue = [
                QueuedBackup(
                    timestamp=f"2025-01-0{(i % 9) + 1}T10:00:00",
                    reason=reason,
                    destination=str(dest_path),
                )
                for i, reason in enumerate(reasons)
            ]
            save_backup_queue(original_queue, queue_path)
            loaded_queue = load_backup_queue(queue_path)
            assert len(loaded_queue) == len(original_queue)
            for orig, loaded in zip(original_queue, loaded_queue):
                assert orig.timestamp == loaded.timestamp
                assert orig.reason == loaded.reason
                assert orig.destination == loaded.destination
