"""Tests for lock management.

Feature: macos-incremental-backup
"""

import multiprocessing
import os
import tempfile
import time
import threading
from pathlib import Path
from typing import Tuple, List

import hypothesis.strategies as st
from hypothesis import given, settings

from devbackup.lock import LockError, LockManager


def _try_acquire_lock_worker(lock_path_str: str, timeout: int, process_id: int, result_queue):
    """
    Worker function that tries to acquire lock.
    Must be at module level for multiprocessing pickling on macOS.
    """
    from devbackup.lock import LockError, LockManager
    from pathlib import Path
    import time
    
    lock_path = Path(lock_path_str)
    try:
        manager = LockManager(lock_path=lock_path, timeout=timeout)
        manager.acquire()
        # Hold the lock for a bit to ensure overlap
        time.sleep(0.5)
        manager.release()
        result_queue.put((process_id, "success", None))
    except LockError as e:
        result_queue.put((process_id, "lock_error", str(e)))
    except Exception as e:
        result_queue.put((process_id, "other_error", str(e)))


class TestLockMutualExclusion:
    """
    Property 4: Lock Mutual Exclusion
    
    For any two concurrent processes attempting to acquire the same lock,
    exactly one SHALL succeed and the other SHALL receive a LockError.
    
    **Validates: Requirements 2.3**
    """

    @given(hold_time=st.floats(min_value=1.0, max_value=2.0))
    @settings(max_examples=10, deadline=120000)
    def test_mutual_exclusion_property(self, hold_time: float):
        """
        Feature: macos-incremental-backup, Property 4: Lock Mutual Exclusion
        
        For any two concurrent processes attempting to acquire the same lock,
        exactly one succeeds and the other receives a LockError.
        
        Uses subprocess to spawn actual separate processes since flock provides
        process-level (not thread-level) mutual exclusion.
        """
        import subprocess
        import json
        
        with tempfile.TemporaryDirectory() as tmpdir:
            lock_path = Path(tmpdir) / "test.lock"
            
            # Timeout must be shorter than hold_time to ensure the second process
            # times out while the first still holds the lock
            timeout = 0.5
            
            # Python script that tries to acquire lock and reports result
            worker_script = f'''
import sys
import time
sys.path.insert(0, ".")
from devbackup.lock import LockManager, LockError
from pathlib import Path
import json

lock_path = Path("{lock_path}")
timeout = {timeout}
hold_time = {hold_time}
process_id = int(sys.argv[1])

try:
    manager = LockManager(lock_path=lock_path, timeout=timeout)
    manager.acquire()
    time.sleep(hold_time)
    manager.release()
    print(json.dumps({{"id": process_id, "status": "success", "error": None}}))
except LockError as e:
    print(json.dumps({{"id": process_id, "status": "lock_error", "error": str(e)}}))
except Exception as e:
    print(json.dumps({{"id": process_id, "status": "other_error", "error": str(e)}}))
'''
            
            script_path = Path(tmpdir) / "worker.py"
            script_path.write_text(worker_script)
            
            # Start two processes that will compete for the lock
            p1 = subprocess.Popen(
                ["python", str(script_path), "1"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            
            # Small delay then start second process
            time.sleep(0.1)
            
            p2 = subprocess.Popen(
                ["python", str(script_path), "2"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            
            # Wait for both to complete
            stdout1, stderr1 = p1.communicate(timeout=30)
            stdout2, stderr2 = p2.communicate(timeout=30)
            
            # Parse results
            results = []
            for stdout, stderr in [(stdout1, stderr1), (stdout2, stderr2)]:
                try:
                    result = json.loads(stdout.decode().strip())
                    results.append((result["id"], result["status"], result["error"]))
                except (json.JSONDecodeError, KeyError) as e:
                    # If we can't parse, treat as other_error
                    results.append((0, "other_error", f"Parse error: {e}, stdout: {stdout}, stderr: {stderr}"))
            
            # Verify mutual exclusion: exactly one success, one lock_error
            successes = [r for r in results if r[1] == "success"]
            lock_errors = [r for r in results if r[1] == "lock_error"]
            other_errors = [r for r in results if r[1] == "other_error"]
            
            # We should have exactly 2 results
            assert len(results) == 2, f"Expected 2 results, got {len(results)}: {results}"
            
            # No unexpected errors
            assert len(other_errors) == 0, f"Unexpected errors: {other_errors}"
            
            # Exactly one success and one lock error (mutual exclusion)
            assert len(successes) == 1, f"Expected exactly 1 success, got {len(successes)}: {results}"
            assert len(lock_errors) == 1, f"Expected exactly 1 lock_error, got {len(lock_errors)}: {results}"


class TestLockManagerAcquireRelease:
    """Unit tests for LockManager acquire/release cycle."""

    def test_acquire_creates_lock_file_with_pid(self):
        """Test that acquiring a lock creates the lock file with current PID."""
        with tempfile.TemporaryDirectory() as tmpdir:
            lock_path = Path(tmpdir) / "test.lock"
            manager = LockManager(lock_path=lock_path, timeout=5)
            
            assert not lock_path.exists()
            
            manager.acquire()
            
            assert lock_path.exists()
            assert manager.get_lock_holder_pid() == os.getpid()
            
            manager.release()
            
            # Lock file should be removed after release
            assert not lock_path.exists()

    def test_release_removes_lock_file(self):
        """Test that releasing a lock removes the lock file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            lock_path = Path(tmpdir) / "test.lock"
            manager = LockManager(lock_path=lock_path, timeout=5)
            
            manager.acquire()
            assert lock_path.exists()
            
            manager.release()
            assert not lock_path.exists()

    def test_context_manager_acquires_and_releases(self):
        """Test that context manager properly acquires and releases lock."""
        with tempfile.TemporaryDirectory() as tmpdir:
            lock_path = Path(tmpdir) / "test.lock"
            
            with LockManager(lock_path=lock_path, timeout=5) as manager:
                assert lock_path.exists()
                assert manager.get_lock_holder_pid() == os.getpid()
            
            # After context exit, lock should be released
            assert not lock_path.exists()

    def test_context_manager_releases_on_exception(self):
        """Test that context manager releases lock even when exception occurs."""
        with tempfile.TemporaryDirectory() as tmpdir:
            lock_path = Path(tmpdir) / "test.lock"
            
            try:
                with LockManager(lock_path=lock_path, timeout=5):
                    assert lock_path.exists()
                    raise ValueError("Test exception")
            except ValueError:
                pass
            
            # Lock should still be released
            assert not lock_path.exists()


class TestStaleLockCleanup:
    """Unit tests for stale lock detection and cleanup."""

    def test_stale_lock_with_dead_pid_is_cleaned(self):
        """Test that a lock file with a non-existent PID is considered stale."""
        with tempfile.TemporaryDirectory() as tmpdir:
            lock_path = Path(tmpdir) / "test.lock"
            
            # Create a lock file with a PID that doesn't exist
            # Use a very high PID that's unlikely to exist
            lock_path.parent.mkdir(parents=True, exist_ok=True)
            lock_path.write_text("999999999")
            
            manager = LockManager(lock_path=lock_path, timeout=5)
            
            # Should be able to acquire despite existing lock file
            manager.acquire()
            assert manager.get_lock_holder_pid() == os.getpid()
            
            manager.release()

    def test_is_locked_returns_false_for_unlocked(self):
        """Test is_locked returns False when no lock is held."""
        with tempfile.TemporaryDirectory() as tmpdir:
            lock_path = Path(tmpdir) / "test.lock"
            manager = LockManager(lock_path=lock_path, timeout=5)
            
            assert not manager.is_locked()

    def test_is_locked_returns_true_when_locked(self):
        """Test is_locked returns True when lock is held."""
        with tempfile.TemporaryDirectory() as tmpdir:
            lock_path = Path(tmpdir) / "test.lock"
            manager = LockManager(lock_path=lock_path, timeout=5)
            
            manager.acquire()
            
            # Create another manager to check lock status
            checker = LockManager(lock_path=lock_path, timeout=1)
            assert checker.is_locked()
            
            manager.release()


class TestLockTimeout:
    """Unit tests for lock timeout behavior."""

    def test_timeout_raises_lock_error(self):
        """Test that timeout raises LockError when lock is held."""
        with tempfile.TemporaryDirectory() as tmpdir:
            lock_path = Path(tmpdir) / "test.lock"
            
            # First manager acquires lock
            manager1 = LockManager(lock_path=lock_path, timeout=5)
            manager1.acquire()
            
            # Second manager should timeout
            manager2 = LockManager(lock_path=lock_path, timeout=1)
            
            try:
                manager2.acquire()
                assert False, "Expected LockError"
            except LockError as e:
                assert "timeout" in str(e).lower() or "held" in str(e).lower()
            finally:
                manager1.release()

    def test_lock_creates_parent_directories(self):
        """Test that acquiring lock creates parent directories if needed."""
        with tempfile.TemporaryDirectory() as tmpdir:
            lock_path = Path(tmpdir) / "nested" / "dirs" / "test.lock"
            manager = LockManager(lock_path=lock_path, timeout=5)
            
            assert not lock_path.parent.exists()
            
            manager.acquire()
            
            assert lock_path.parent.exists()
            assert lock_path.exists()
            
            manager.release()
