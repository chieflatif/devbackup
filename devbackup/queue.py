"""Backup queue persistence for devbackup.

This module provides a persistent queue for backup requests that cannot
be completed due to destination unavailability. The queue survives process
restarts and processes queued backups when the destination becomes available.

Requirements: 12.1, 12.4
"""

import json
import os
import time
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import List, Optional
import logging


# Default queue file location
DEFAULT_QUEUE_PATH = Path.home() / ".cache" / "devbackup" / "queue.json"


class QueueError(Exception):
    """Raised when queue operations fail."""
    pass


@dataclass
class QueuedBackup:
    """A queued backup request.
    
    Attributes:
        source_directories: List of source directory paths to back up
        backup_destination: Path to backup destination
        queued_at: Unix timestamp when the backup was queued
        reason: Reason the backup was queued (e.g., "destination_unavailable")
        retry_count: Number of times this backup has been retried
    """
    source_directories: List[str]
    backup_destination: str
    queued_at: float
    reason: str = "destination_unavailable"
    retry_count: int = 0
    
    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "source_directories": self.source_directories,
            "backup_destination": self.backup_destination,
            "queued_at": self.queued_at,
            "reason": self.reason,
            "retry_count": self.retry_count,
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> "QueuedBackup":
        """Create from dictionary (JSON deserialization)."""
        return cls(
            source_directories=data["source_directories"],
            backup_destination=data["backup_destination"],
            queued_at=data["queued_at"],
            reason=data.get("reason", "destination_unavailable"),
            retry_count=data.get("retry_count", 0),
        )


@dataclass
class BackupQueue:
    """Persistent backup queue.
    
    Manages a FIFO queue of backup requests that couldn't be completed
    due to destination unavailability. The queue is persisted to disk
    and survives process restarts.
    
    Requirements: 12.1, 12.4
    """
    queue_path: Path = field(default_factory=lambda: DEFAULT_QUEUE_PATH)
    _items: List[QueuedBackup] = field(default_factory=list, init=False)
    _logger: Optional[logging.Logger] = field(default=None, init=False)
    
    def __post_init__(self):
        """Initialize the queue by loading from disk if exists."""
        self._logger = logging.getLogger("devbackup.queue")
        self._load()
    
    def _ensure_queue_dir(self) -> None:
        """Ensure the queue directory exists."""
        self.queue_path.parent.mkdir(parents=True, exist_ok=True)
    
    def _load(self) -> None:
        """Load queue from disk.
        
        If the queue file doesn't exist or is corrupted, starts with empty queue.
        """
        if not self.queue_path.exists():
            self._items = []
            return
        
        try:
            content = self.queue_path.read_text()
            if not content.strip():
                self._items = []
                return
            
            data = json.loads(content)
            self._items = [QueuedBackup.from_dict(item) for item in data.get("queue", [])]
            if self._logger:
                self._logger.debug(f"Loaded {len(self._items)} queued backup(s) from {self.queue_path}")
        except (json.JSONDecodeError, KeyError, TypeError) as e:
            # Corrupted queue file - start fresh
            if self._logger:
                self._logger.warning(f"Corrupted queue file, starting fresh: {e}")
            self._items = []
    
    def _save(self) -> None:
        """Save queue to disk atomically.
        
        Uses atomic write (write to temp file, then rename) to prevent
        data loss if the process is interrupted during write.
        """
        self._ensure_queue_dir()
        
        data = {
            "version": 1,
            "queue": [item.to_dict() for item in self._items],
        }
        
        # Atomic write: write to temp file, then rename
        temp_path = self.queue_path.with_suffix(".tmp")
        try:
            temp_path.write_text(json.dumps(data, indent=2))
            temp_path.replace(self.queue_path)
        except OSError as e:
            raise QueueError(f"Failed to save queue: {e}")
    
    def enqueue(
        self,
        source_directories: List[Path],
        backup_destination: Path,
        reason: str = "destination_unavailable",
    ) -> QueuedBackup:
        """Add a backup request to the queue.
        
        Args:
            source_directories: List of source directories to back up
            backup_destination: Path to backup destination
            reason: Reason the backup is being queued
        
        Returns:
            The queued backup item
        
        Requirements: 12.1
        """
        item = QueuedBackup(
            source_directories=[str(p) for p in source_directories],
            backup_destination=str(backup_destination),
            queued_at=time.time(),
            reason=reason,
            retry_count=0,
        )
        
        self._items.append(item)
        self._save()
        
        if self._logger:
            self._logger.info(
                f"Queued backup for {len(source_directories)} source(s) to {backup_destination} "
                f"(reason: {reason})"
            )
        
        return item
    
    def dequeue(self) -> Optional[QueuedBackup]:
        """Remove and return the oldest backup request from the queue.
        
        Returns:
            The oldest queued backup, or None if queue is empty
        
        Requirements: 12.4 (FIFO ordering)
        """
        if not self._items:
            return None
        
        item = self._items.pop(0)
        self._save()
        
        if self._logger:
            self._logger.debug(f"Dequeued backup to {item.backup_destination}")
        
        return item
    
    def peek(self) -> Optional[QueuedBackup]:
        """Return the oldest backup request without removing it.
        
        Returns:
            The oldest queued backup, or None if queue is empty
        """
        if not self._items:
            return None
        return self._items[0]
    
    def is_empty(self) -> bool:
        """Check if the queue is empty."""
        return len(self._items) == 0
    
    def size(self) -> int:
        """Return the number of items in the queue."""
        return len(self._items)
    
    def clear(self) -> int:
        """Clear all items from the queue.
        
        Returns:
            Number of items that were cleared
        """
        count = len(self._items)
        self._items = []
        self._save()
        
        if self._logger and count > 0:
            self._logger.info(f"Cleared {count} item(s) from backup queue")
        
        return count
    
    def get_all(self) -> List[QueuedBackup]:
        """Return all queued backups without removing them.
        
        Returns:
            List of all queued backups (oldest first)
        """
        return list(self._items)
    
    def increment_retry(self, item: QueuedBackup) -> None:
        """Increment the retry count for a queued item and re-add to queue.
        
        Used when a queued backup fails and needs to be retried later.
        
        Args:
            item: The queued backup item to retry
        """
        item.retry_count += 1
        self._items.append(item)
        self._save()
        
        if self._logger:
            self._logger.debug(
                f"Re-queued backup to {item.backup_destination} "
                f"(retry #{item.retry_count})"
            )
    
    def remove_by_destination(self, destination: Path) -> int:
        """Remove all queued backups for a specific destination.
        
        Args:
            destination: The backup destination path
        
        Returns:
            Number of items removed
        """
        dest_str = str(destination)
        original_count = len(self._items)
        self._items = [
            item for item in self._items
            if item.backup_destination != dest_str
        ]
        removed = original_count - len(self._items)
        
        if removed > 0:
            self._save()
            if self._logger:
                self._logger.info(
                    f"Removed {removed} queued backup(s) for destination {destination}"
                )
        
        return removed


def get_default_queue() -> BackupQueue:
    """Get the default backup queue instance.
    
    Returns:
        BackupQueue instance using the default queue path
    """
    return BackupQueue(queue_path=DEFAULT_QUEUE_PATH)
