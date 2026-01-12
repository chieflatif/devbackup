"""Property-based tests for Backup Queue Persistence.

**Property 10: Backup Queue Persistence**
**Validates: Requirements 12.1, 12.4**

Tests that:
- For any backup that cannot complete due to destination unavailability,
  the backup request SHALL be added to a persistent queue
- The queue SHALL survive process restarts
- Queued backups SHALL be executed when the destination becomes available
- Queue order SHALL be preserved (FIFO)
"""

import json
import os
import tempfile
import time
from pathlib import Path
from typing import List

import pytest
from hypothesis import given, settings, assume, HealthCheck
from hypothesis import strategies as st

from devbackup.queue import (
    BackupQueue,
    QueuedBackup,
    QueueError,
    DEFAULT_QUEUE_PATH,
    get_default_queue,
)


# Strategy for generating valid source directory paths
# Avoid paths ending with '.' as Path normalization removes trailing dots
def _normalize_path_component(s: str) -> str:
    """Normalize a path component to avoid Path normalization issues."""
    # Strip slashes and dots from ends to avoid normalization changes
    cleaned = s.strip('/').rstrip('.')
    return cleaned if cleaned else "dir"

source_path_strategy = st.text(
    alphabet=st.characters(whitelist_categories=('L', 'N'), whitelist_characters='_-'),
    min_size=1,
    max_size=50,
).map(lambda s: f"/path/to/{_normalize_path_component(s)}")

# Strategy for generating lists of source directories
source_dirs_strategy = st.lists(
    source_path_strategy,
    min_size=1,
    max_size=5,
)

# Strategy for generating backup destination paths
# Avoid paths ending with '.' as Path normalization removes trailing dots
destination_strategy = st.text(
    alphabet=st.characters(whitelist_categories=('L', 'N'), whitelist_characters='_-'),
    min_size=1,
    max_size=50,
).map(lambda s: f"/backup/{_normalize_path_component(s)}")

# Strategy for generating queue reasons
reason_strategy = st.sampled_from([
    "destination_unavailable",
    "network_error",
    "permission_denied",
    "disk_full",
])

# Strategy for generating timestamps
timestamp_strategy = st.floats(
    min_value=1600000000.0,  # ~2020
    max_value=2000000000.0,  # ~2033
    allow_nan=False,
    allow_infinity=False,
)


class TestBackupQueuePersistenceProperty:
    """
    Property 10: Backup Queue Persistence
    
    *For any* backup that cannot complete due to destination unavailability:
    - The backup request SHALL be added to a persistent queue
    - The queue SHALL survive process restarts
    - Queued backups SHALL be executed when the destination becomes available
    - Queue order SHALL be preserved (FIFO)
    
    **Validates: Requirements 12.1, 12.4**
    """
    
    @given(
        source_dirs=source_dirs_strategy,
        destination=destination_strategy,
        reason=reason_strategy,
    )
    @settings(
        max_examples=100,
        deadline=None,
        suppress_health_check=[HealthCheck.too_slow],
    )
    def test_enqueue_persists_to_disk(
        self,
        source_dirs: List[str],
        destination: str,
        reason: str,
    ):
        """
        Feature: user-experience-enhancement, Property 10: Backup Queue Persistence
        
        Enqueued backups SHALL be persisted to disk immediately.
        
        **Validates: Requirements 12.1**
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            queue_path = Path(tmp_dir) / "queue.json"
            queue = BackupQueue(queue_path=queue_path)
            
            # Enqueue a backup
            item = queue.enqueue(
                source_directories=[Path(s) for s in source_dirs],
                backup_destination=Path(destination),
                reason=reason,
            )
            
            # Verify file exists
            assert queue_path.exists(), "Queue file should be created"
            
            # Verify content is valid JSON
            content = queue_path.read_text()
            data = json.loads(content)
            
            # Verify queue contains the item
            assert len(data["queue"]) == 1
            assert data["queue"][0]["backup_destination"] == destination
            assert data["queue"][0]["source_directories"] == source_dirs
            assert data["queue"][0]["reason"] == reason
    
    @given(
        source_dirs=source_dirs_strategy,
        destination=destination_strategy,
    )
    @settings(
        max_examples=100,
        deadline=None,
        suppress_health_check=[HealthCheck.too_slow],
    )
    def test_queue_survives_restart(
        self,
        source_dirs: List[str],
        destination: str,
    ):
        """
        Feature: user-experience-enhancement, Property 10: Backup Queue Persistence
        
        Queue SHALL survive process restarts (simulated by creating new instance).
        
        **Validates: Requirements 12.4**
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            queue_path = Path(tmp_dir) / "queue.json"
            
            # Create first queue instance and enqueue
            queue1 = BackupQueue(queue_path=queue_path)
            queue1.enqueue(
                source_directories=[Path(s) for s in source_dirs],
                backup_destination=Path(destination),
            )
            
            # Simulate restart by creating new queue instance
            queue2 = BackupQueue(queue_path=queue_path)
            
            # Verify queue was loaded
            assert queue2.size() == 1, "Queue should have 1 item after restart"
            
            # Verify item data is preserved
            item = queue2.peek()
            assert item is not None
            assert item.backup_destination == destination
            assert item.source_directories == source_dirs
    
    @given(
        items=st.lists(
            st.tuples(source_dirs_strategy, destination_strategy),
            min_size=2,
            max_size=10,
        ),
    )
    @settings(
        max_examples=100,
        deadline=None,
        suppress_health_check=[HealthCheck.too_slow],
    )
    def test_fifo_order_preserved(
        self,
        items: List[tuple],
    ):
        """
        Feature: user-experience-enhancement, Property 10: Backup Queue Persistence
        
        Queue order SHALL be preserved (FIFO).
        
        **Validates: Requirements 12.4**
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            queue_path = Path(tmp_dir) / "queue.json"
            queue = BackupQueue(queue_path=queue_path)
            
            # Enqueue all items
            for source_dirs, destination in items:
                queue.enqueue(
                    source_directories=[Path(s) for s in source_dirs],
                    backup_destination=Path(destination),
                )
            
            # Dequeue and verify order
            for i, (expected_sources, expected_dest) in enumerate(items):
                item = queue.dequeue()
                assert item is not None, f"Item {i} should not be None"
                assert item.backup_destination == expected_dest, \
                    f"Item {i} destination mismatch: {item.backup_destination} != {expected_dest}"
                assert item.source_directories == expected_sources, \
                    f"Item {i} sources mismatch"
            
            # Queue should be empty
            assert queue.is_empty(), "Queue should be empty after dequeuing all items"
    
    @given(
        items=st.lists(
            st.tuples(source_dirs_strategy, destination_strategy),
            min_size=2,
            max_size=10,
        ),
    )
    @settings(
        max_examples=100,
        deadline=None,
        suppress_health_check=[HealthCheck.too_slow],
    )
    def test_fifo_order_preserved_across_restart(
        self,
        items: List[tuple],
    ):
        """
        Feature: user-experience-enhancement, Property 10: Backup Queue Persistence
        
        FIFO order SHALL be preserved across process restarts.
        
        **Validates: Requirements 12.4**
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            queue_path = Path(tmp_dir) / "queue.json"
            
            # Enqueue all items
            queue1 = BackupQueue(queue_path=queue_path)
            for source_dirs, destination in items:
                queue1.enqueue(
                    source_directories=[Path(s) for s in source_dirs],
                    backup_destination=Path(destination),
                )
            
            # Simulate restart
            queue2 = BackupQueue(queue_path=queue_path)
            
            # Dequeue and verify order
            for i, (expected_sources, expected_dest) in enumerate(items):
                item = queue2.dequeue()
                assert item is not None, f"Item {i} should not be None after restart"
                assert item.backup_destination == expected_dest, \
                    f"Item {i} destination mismatch after restart"
                assert item.source_directories == expected_sources, \
                    f"Item {i} sources mismatch after restart"


class TestQueuedBackupRoundTrip:
    """
    Tests for QueuedBackup serialization round-trip.
    
    **Validates: Requirements 12.1, 12.4**
    """
    
    @given(
        source_dirs=source_dirs_strategy,
        destination=destination_strategy,
        queued_at=timestamp_strategy,
        reason=reason_strategy,
        retry_count=st.integers(min_value=0, max_value=100),
    )
    @settings(
        max_examples=100,
        deadline=None,
        suppress_health_check=[HealthCheck.too_slow],
    )
    def test_queued_backup_round_trip(
        self,
        source_dirs: List[str],
        destination: str,
        queued_at: float,
        reason: str,
        retry_count: int,
    ):
        """
        Feature: user-experience-enhancement, Property 10: Backup Queue Persistence
        
        QueuedBackup serialization/deserialization SHALL preserve all fields.
        
        **Validates: Requirements 12.1, 12.4**
        """
        original = QueuedBackup(
            source_directories=source_dirs,
            backup_destination=destination,
            queued_at=queued_at,
            reason=reason,
            retry_count=retry_count,
        )
        
        # Round-trip through dict
        data = original.to_dict()
        restored = QueuedBackup.from_dict(data)
        
        # Verify all fields preserved
        assert restored.source_directories == original.source_directories
        assert restored.backup_destination == original.backup_destination
        assert restored.queued_at == original.queued_at
        assert restored.reason == original.reason
        assert restored.retry_count == original.retry_count
    
    @given(
        source_dirs=source_dirs_strategy,
        destination=destination_strategy,
    )
    @settings(
        max_examples=100,
        deadline=None,
        suppress_health_check=[HealthCheck.too_slow],
    )
    def test_queued_backup_json_round_trip(
        self,
        source_dirs: List[str],
        destination: str,
    ):
        """
        Feature: user-experience-enhancement, Property 10: Backup Queue Persistence
        
        QueuedBackup SHALL survive JSON file round-trip.
        
        **Validates: Requirements 12.1, 12.4**
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            queue_path = Path(tmp_dir) / "queue.json"
            
            # Create and enqueue
            queue1 = BackupQueue(queue_path=queue_path)
            original = queue1.enqueue(
                source_directories=[Path(s) for s in source_dirs],
                backup_destination=Path(destination),
            )
            
            # Load from file
            queue2 = BackupQueue(queue_path=queue_path)
            restored = queue2.peek()
            
            assert restored is not None
            assert restored.source_directories == original.source_directories
            assert restored.backup_destination == original.backup_destination
            assert restored.reason == original.reason


class TestQueueOperations:
    """
    Tests for queue operations.
    
    **Validates: Requirements 12.1, 12.4**
    """
    
    @given(
        num_items=st.integers(min_value=1, max_value=20),
    )
    @settings(
        max_examples=100,
        deadline=None,
        suppress_health_check=[HealthCheck.too_slow],
    )
    def test_size_matches_enqueued_count(
        self,
        num_items: int,
    ):
        """
        Feature: user-experience-enhancement, Property 10: Backup Queue Persistence
        
        Queue size SHALL match number of enqueued items.
        
        **Validates: Requirements 12.1**
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            queue_path = Path(tmp_dir) / "queue.json"
            queue = BackupQueue(queue_path=queue_path)
            
            for i in range(num_items):
                queue.enqueue(
                    source_directories=[Path(f"/src/{i}")],
                    backup_destination=Path(f"/dest/{i}"),
                )
            
            assert queue.size() == num_items
    
    @given(
        num_items=st.integers(min_value=1, max_value=20),
    )
    @settings(
        max_examples=100,
        deadline=None,
        suppress_health_check=[HealthCheck.too_slow],
    )
    def test_clear_removes_all_items(
        self,
        num_items: int,
    ):
        """
        Feature: user-experience-enhancement, Property 10: Backup Queue Persistence
        
        Clear SHALL remove all items from queue.
        
        **Validates: Requirements 12.1**
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            queue_path = Path(tmp_dir) / "queue.json"
            queue = BackupQueue(queue_path=queue_path)
            
            for i in range(num_items):
                queue.enqueue(
                    source_directories=[Path(f"/src/{i}")],
                    backup_destination=Path(f"/dest/{i}"),
                )
            
            cleared = queue.clear()
            
            assert cleared == num_items
            assert queue.is_empty()
            assert queue.size() == 0
    
    def test_dequeue_from_empty_returns_none(self):
        """
        Feature: user-experience-enhancement, Property 10: Backup Queue Persistence
        
        Dequeue from empty queue SHALL return None.
        
        **Validates: Requirements 12.4**
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            queue_path = Path(tmp_dir) / "queue.json"
            queue = BackupQueue(queue_path=queue_path)
            
            assert queue.dequeue() is None
    
    def test_peek_from_empty_returns_none(self):
        """
        Feature: user-experience-enhancement, Property 10: Backup Queue Persistence
        
        Peek from empty queue SHALL return None.
        
        **Validates: Requirements 12.4**
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            queue_path = Path(tmp_dir) / "queue.json"
            queue = BackupQueue(queue_path=queue_path)
            
            assert queue.peek() is None
    
    @given(
        source_dirs=source_dirs_strategy,
        destination=destination_strategy,
    )
    @settings(
        max_examples=100,
        deadline=None,
        suppress_health_check=[HealthCheck.too_slow],
    )
    def test_peek_does_not_remove_item(
        self,
        source_dirs: List[str],
        destination: str,
    ):
        """
        Feature: user-experience-enhancement, Property 10: Backup Queue Persistence
        
        Peek SHALL NOT remove item from queue.
        
        **Validates: Requirements 12.4**
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            queue_path = Path(tmp_dir) / "queue.json"
            queue = BackupQueue(queue_path=queue_path)
            
            queue.enqueue(
                source_directories=[Path(s) for s in source_dirs],
                backup_destination=Path(destination),
            )
            
            # Peek multiple times
            item1 = queue.peek()
            item2 = queue.peek()
            item3 = queue.peek()
            
            # All should return the same item
            assert item1 is not None
            assert item1.backup_destination == item2.backup_destination == item3.backup_destination
            
            # Queue should still have 1 item
            assert queue.size() == 1


class TestQueueAtomicOperations:
    """
    Tests for atomic queue operations.
    
    **Validates: Requirements 12.4**
    """
    
    @given(
        source_dirs=source_dirs_strategy,
        destination=destination_strategy,
    )
    @settings(
        max_examples=100,
        deadline=None,
        suppress_health_check=[HealthCheck.too_slow],
    )
    def test_atomic_write_creates_temp_file(
        self,
        source_dirs: List[str],
        destination: str,
    ):
        """
        Feature: user-experience-enhancement, Property 10: Backup Queue Persistence
        
        Queue writes SHALL be atomic (no partial writes).
        
        **Validates: Requirements 12.4**
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            queue_path = Path(tmp_dir) / "queue.json"
            queue = BackupQueue(queue_path=queue_path)
            
            # Enqueue item
            queue.enqueue(
                source_directories=[Path(s) for s in source_dirs],
                backup_destination=Path(destination),
            )
            
            # Verify no temp file left behind
            temp_path = queue_path.with_suffix(".tmp")
            assert not temp_path.exists(), "Temp file should not exist after write"
            
            # Verify main file is valid JSON
            content = queue_path.read_text()
            data = json.loads(content)  # Should not raise
            assert "queue" in data
    
    def test_corrupted_queue_file_starts_fresh(self):
        """
        Feature: user-experience-enhancement, Property 10: Backup Queue Persistence
        
        Corrupted queue file SHALL result in empty queue (graceful recovery).
        
        **Validates: Requirements 12.4**
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            queue_path = Path(tmp_dir) / "queue.json"
            
            # Write corrupted content
            queue_path.write_text("not valid json {{{")
            
            # Create queue - should start fresh
            queue = BackupQueue(queue_path=queue_path)
            
            assert queue.is_empty()
            assert queue.size() == 0


class TestRetryBehavior:
    """
    Tests for retry behavior.
    
    **Validates: Requirements 12.1, 12.4**
    """
    
    @given(
        source_dirs=source_dirs_strategy,
        destination=destination_strategy,
        initial_retry=st.integers(min_value=0, max_value=10),
    )
    @settings(
        max_examples=100,
        deadline=None,
        suppress_health_check=[HealthCheck.too_slow],
    )
    def test_increment_retry_increases_count(
        self,
        source_dirs: List[str],
        destination: str,
        initial_retry: int,
    ):
        """
        Feature: user-experience-enhancement, Property 10: Backup Queue Persistence
        
        Increment retry SHALL increase retry count by 1.
        
        **Validates: Requirements 12.1**
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            queue_path = Path(tmp_dir) / "queue.json"
            queue = BackupQueue(queue_path=queue_path)
            
            # Create item with initial retry count
            item = QueuedBackup(
                source_directories=source_dirs,
                backup_destination=destination,
                queued_at=time.time(),
                retry_count=initial_retry,
            )
            
            # Increment retry
            queue.increment_retry(item)
            
            # Verify retry count increased
            assert item.retry_count == initial_retry + 1
            
            # Verify item is in queue
            assert queue.size() == 1
            queued_item = queue.peek()
            assert queued_item.retry_count == initial_retry + 1
