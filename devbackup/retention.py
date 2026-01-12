"""Retention manager for devbackup.

This module provides the RetentionManager class that manages snapshot
retention according to hourly/daily/weekly policy.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional, Set
import logging
import shutil


# Logger for retention operations
logger = logging.getLogger(__name__)


@dataclass
class RetentionResult:
    """Result of applying retention policy."""
    kept_snapshots: List[Path]
    deleted_snapshots: List[Path]
    freed_bytes: int


class RetentionManager:
    """
    Manages snapshot retention according to hourly/daily/weekly policy.
    
    Retention policy:
    - Keep N most recent hourly snapshots
    - Keep first snapshot of each day for last N days
    - Keep first snapshot of each week (Sunday start) for last N weeks
    """
    
    # Timestamp format for snapshot directories (matches SnapshotEngine)
    TIMESTAMP_FORMAT = "%Y-%m-%d-%H%M%S"
    
    def __init__(self, destination: Path, hourly: int, daily: int, weekly: int):
        """
        Initialize the retention manager.
        
        Args:
            destination: Path to the backup destination directory
            hourly: Number of most recent hourly snapshots to keep
            daily: Number of days to keep first-of-day snapshots
            weekly: Number of weeks to keep first-of-week snapshots
        """
        self.destination = Path(destination)
        self.hourly = hourly
        self.daily = daily
        self.weekly = weekly
    
    def _parse_snapshot_timestamp(self, snapshot: Path) -> Optional[datetime]:
        """
        Parse YYYY-MM-DD-HHMMSS directory name to datetime.
        
        Args:
            snapshot: Path to snapshot directory
        
        Returns:
            Parsed datetime, or None if name doesn't match format
        """
        try:
            return datetime.strptime(snapshot.name, self.TIMESTAMP_FORMAT)
        except ValueError:
            return None

    def _get_first_of_day(
        self,
        snapshots: List[Path],
        date: datetime
    ) -> Optional[Path]:
        """
        Get earliest snapshot for a given calendar day.
        
        Args:
            snapshots: List of snapshot paths to search
            date: The date to find the first snapshot for
        
        Returns:
            Path to the earliest snapshot on that day, or None if none found
        """
        # Filter snapshots to those on the given date
        day_snapshots = []
        for snapshot in snapshots:
            ts = self._parse_snapshot_timestamp(snapshot)
            if ts is None:
                continue
            # Compare year, month, day only
            if ts.date() == date.date():
                day_snapshots.append((ts, snapshot))
        
        if not day_snapshots:
            return None
        
        # Sort by timestamp ascending and return the earliest
        day_snapshots.sort(key=lambda x: x[0])
        return day_snapshots[0][1]
    
    def _get_first_of_week(
        self,
        snapshots: List[Path],
        week_start: datetime
    ) -> Optional[Path]:
        """
        Get earliest snapshot for a given week (Sunday start).
        
        Args:
            snapshots: List of snapshot paths to search
            week_start: The Sunday that starts the week
        
        Returns:
            Path to the earliest snapshot in that week, or None if none found
        """
        # Calculate week end (Saturday 23:59:59)
        week_end = week_start + timedelta(days=7)
        
        # Filter snapshots to those in the given week
        week_snapshots = []
        for snapshot in snapshots:
            ts = self._parse_snapshot_timestamp(snapshot)
            if ts is None:
                continue
            # Check if timestamp falls within the week
            if week_start <= ts < week_end:
                week_snapshots.append((ts, snapshot))
        
        if not week_snapshots:
            return None
        
        # Sort by timestamp ascending and return the earliest
        week_snapshots.sort(key=lambda x: x[0])
        return week_snapshots[0][1]
    
    def _get_week_start(self, dt: datetime) -> datetime:
        """
        Get the Sunday that starts the week containing the given datetime.
        
        Args:
            dt: A datetime
        
        Returns:
            datetime for midnight on the Sunday starting that week
        """
        # Python weekday(): Monday=0, Sunday=6
        # We want Sunday=0, so we need to adjust
        days_since_sunday = (dt.weekday() + 1) % 7
        sunday = dt - timedelta(days=days_since_sunday)
        # Return midnight on that Sunday
        return datetime(sunday.year, sunday.month, sunday.day)
    
    def _list_valid_snapshots(self) -> List[Path]:
        """
        List all valid snapshot directories in the destination.
        
        Returns:
            List of paths to valid snapshot directories
        """
        if not self.destination.exists():
            return []
        
        snapshots = []
        for entry in self.destination.iterdir():
            if not entry.is_dir():
                continue
            # Skip in-progress directories
            if entry.name.startswith("in_progress_"):
                continue
            # Skip metadata/hidden directories
            if entry.name.startswith("."):
                continue
            # Verify it's a valid timestamp format
            if self._parse_snapshot_timestamp(entry) is not None:
                snapshots.append(entry)
        
        return snapshots

    def _list_in_progress_directories(self) -> List[Path]:
        """
        List all in_progress directories in the destination.
        
        Returns:
            List of paths to in_progress directories
        """
        if not self.destination.exists():
            return []
        
        in_progress = []
        for entry in self.destination.iterdir():
            if entry.is_dir() and entry.name.startswith("in_progress_"):
                in_progress.append(entry)
        
        return in_progress

    def _get_protected_snapshots(self) -> Set[Path]:
        """
        Get snapshots that must be protected from deletion during retention.
        
        Protected snapshots include:
        - The most recent complete snapshot when an in_progress backup exists
          (this is the link-dest target for the in_progress backup)
        
        This prevents corruption of in-progress backups that rely on hard links
        to the previous snapshot.
        
        Requirements: 5.1, 5.2, 5.3, 5.4
        
        Returns:
            Set of snapshot paths that must not be deleted
        """
        protected: Set[Path] = set()
        
        # Check for in_progress directories
        in_progress_dirs = self._list_in_progress_directories()
        
        if not in_progress_dirs:
            # No active backups, nothing to protect
            return protected
        
        # Get all valid snapshots sorted by timestamp (most recent first)
        snapshots = self._list_valid_snapshots()
        if not snapshots:
            return protected
        
        # Sort snapshots by timestamp descending
        parsed_snapshots = []
        for snapshot in snapshots:
            ts = self._parse_snapshot_timestamp(snapshot)
            if ts is not None:
                parsed_snapshots.append((ts, snapshot))
        
        if not parsed_snapshots:
            return protected
        
        parsed_snapshots.sort(key=lambda x: x[0], reverse=True)
        
        # The most recent complete snapshot is the link-dest target
        # for any in_progress backup (Requirements 5.1, 5.3, 5.4)
        most_recent_snapshot = parsed_snapshots[0][1]
        protected.add(most_recent_snapshot)
        
        logger.debug(
            f"Protected snapshot {most_recent_snapshot.name} as link-dest target "
            f"for {len(in_progress_dirs)} in-progress backup(s)"
        )
        
        return protected

    def get_snapshots_to_keep(self, snapshots: List[Path]) -> Set[Path]:
        """
        Determine which snapshots to keep based on retention policy.
        
        Keeps:
        - N most recent hourly snapshots
        - First snapshot of each day for last N days
        - First snapshot of each week (Sunday) for last N weeks
        
        Args:
            snapshots: List of snapshot paths to evaluate
        
        Returns:
            Set of snapshot paths to keep
        """
        if not snapshots:
            return set()
        
        # Parse timestamps and sort by timestamp descending (most recent first)
        parsed = []
        for snapshot in snapshots:
            ts = self._parse_snapshot_timestamp(snapshot)
            if ts is not None:
                parsed.append((ts, snapshot))
        
        if not parsed:
            return set()
        
        parsed.sort(key=lambda x: x[0], reverse=True)
        
        to_keep: Set[Path] = set()
        
        # 1. Keep N most recent hourly snapshots
        for i, (ts, snapshot) in enumerate(parsed):
            if i < self.hourly:
                to_keep.add(snapshot)
        
        # Get the current time (use most recent snapshot as reference)
        now = parsed[0][0]
        
        # 2. Keep first snapshot of each day for last N days
        for days_ago in range(self.daily):
            target_date = now - timedelta(days=days_ago)
            first_of_day = self._get_first_of_day(snapshots, target_date)
            if first_of_day is not None:
                to_keep.add(first_of_day)
        
        # 3. Keep first snapshot of each week for last N weeks
        current_week_start = self._get_week_start(now)
        for weeks_ago in range(self.weekly):
            target_week_start = current_week_start - timedelta(weeks=weeks_ago)
            first_of_week = self._get_first_of_week(snapshots, target_week_start)
            if first_of_week is not None:
                to_keep.add(first_of_week)
        
        return to_keep
    
    def apply_retention(self) -> RetentionResult:
        """
        Apply retention policy and delete expired snapshots.
        
        Protects snapshots that are being used as link-dest targets for
        in-progress backups (Requirements 5.1, 5.4, 5.5).
        
        Returns:
            RetentionResult with lists of kept and deleted snapshots
        """
        snapshots = self._list_valid_snapshots()
        
        if not snapshots:
            return RetentionResult(
                kept_snapshots=[],
                deleted_snapshots=[],
                freed_bytes=0,
            )
        
        to_keep = self.get_snapshots_to_keep(snapshots)
        
        # Get protected snapshots (link-dest targets for in-progress backups)
        # Requirements: 5.1, 5.2, 5.3, 5.4
        protected = self._get_protected_snapshots()
        
        kept_snapshots = []
        deleted_snapshots = []
        freed_bytes = 0
        
        for snapshot in snapshots:
            if snapshot in to_keep:
                kept_snapshots.append(snapshot)
            elif snapshot in protected:
                # Snapshot is protected due to active backup (Requirement 5.4, 5.5)
                kept_snapshots.append(snapshot)
                logger.info(
                    f"Preserved snapshot {snapshot.name} - "
                    f"link-dest target for in-progress backup"
                )
            else:
                # Calculate size before deletion
                size = self._get_directory_size(snapshot)
                
                # Delete the snapshot
                try:
                    shutil.rmtree(snapshot)
                    deleted_snapshots.append(snapshot)
                    freed_bytes += size
                except OSError:
                    # If deletion fails, keep it in the kept list
                    kept_snapshots.append(snapshot)
        
        # Sort results by timestamp for consistent output
        kept_snapshots.sort(key=lambda p: p.name, reverse=True)
        deleted_snapshots.sort(key=lambda p: p.name, reverse=True)
        
        return RetentionResult(
            kept_snapshots=kept_snapshots,
            deleted_snapshots=deleted_snapshots,
            freed_bytes=freed_bytes,
        )
    
    def _get_directory_size(self, path: Path) -> int:
        """
        Calculate total size of a directory in bytes.
        
        Args:
            path: Directory path
        
        Returns:
            Total size in bytes
        """
        total_size = 0
        try:
            for root, dirs, files in path.walk():
                for f in files:
                    file_path = root / f
                    try:
                        # Use lstat to not follow symlinks
                        total_size += file_path.lstat().st_size
                    except OSError:
                        pass
        except OSError:
            pass
        return total_size
