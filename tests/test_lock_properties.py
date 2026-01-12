"""Property-based tests for atomic lock mutual exclusion.

Feature: backup-robustness
Property 4: Atomic Lock Mutual Exclusion

For any N concurrent processes attempting to acquire the same lock,
exactly one SHALL succeed and N-1 SHALL fail, with no race conditions.

**Validates: Requirements 4.4, 4.5**
"""

import json
import subprocess
import tempfile
import time
from pathlib import Path

import hypothesis.strategies as st
from hypothesis import given, settings


class TestAtomicLockMutualExclusion:
    """
    Property 4: Atomic Lock Mutual Exclusion
    
    For any N concurrent processes attempting to acquire the same lock,
    exactly one SHALL succeed and N-1 SHALL fail, with no race conditions.
    
    **Validates: Requirements 4.4, 4.5**
    """

    @given(
        num_processes=st.integers(min_value=2, max_value=5),
        hold_time=st.floats(min_value=0.5, max_value=1.5),
    )
    @settings(max_examples=10, deadline=180000)
    def test_atomic_lock_mutual_exclusion_property(
        self, num_processes: int, hold_time: float
    ):
        """
        Feature: backup-robustness, Property 4: Atomic Lock Mutual Exclusion
        
        For any N concurrent processes attempting to acquire the same lock,
        exactly one succeeds and N-1 receive a LockError.
        
        This tests the atomic nature of lock acquisition - no race conditions
        should allow multiple processes to acquire the lock simultaneously.
        
        **Validates: Requirements 4.4, 4.5**
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            lock_path = Path(tmpdir) / "test.lock"
            
            # Timeout must be shorter than hold_time to ensure processes
            # timeout while the winner still holds the lock
            timeout = 0.3
            
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
    # Record the time we acquired the lock
    acquire_time = time.time()
    time.sleep(hold_time)
    manager.release()
    print(json.dumps({{"id": process_id, "status": "success", "error": None, "acquire_time": acquire_time}}))
except LockError as e:
    print(json.dumps({{"id": process_id, "status": "lock_error", "error": str(e), "acquire_time": None}}))
except Exception as e:
    print(json.dumps({{"id": process_id, "status": "other_error", "error": str(e), "acquire_time": None}}))
'''
            
            script_path = Path(tmpdir) / "worker.py"
            script_path.write_text(worker_script)
            
            # Start N processes that will compete for the lock
            processes = []
            for i in range(num_processes):
                p = subprocess.Popen(
                    ["python", str(script_path), str(i + 1)],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )
                processes.append(p)
                # Small stagger to increase race condition likelihood
                time.sleep(0.02)
            
            # Wait for all to complete
            results = []
            for p in processes:
                stdout, stderr = p.communicate(timeout=30)
                try:
                    result = json.loads(stdout.decode().strip())
                    results.append((result["id"], result["status"], result["error"]))
                except (json.JSONDecodeError, KeyError) as e:
                    results.append((0, "other_error", f"Parse error: {e}, stdout: {stdout}, stderr: {stderr}"))
            
            # Verify mutual exclusion: exactly one success, rest are lock_errors
            successes = [r for r in results if r[1] == "success"]
            lock_errors = [r for r in results if r[1] == "lock_error"]
            other_errors = [r for r in results if r[1] == "other_error"]
            
            # We should have exactly N results
            assert len(results) == num_processes, (
                f"Expected {num_processes} results, got {len(results)}: {results}"
            )
            
            # No unexpected errors
            assert len(other_errors) == 0, f"Unexpected errors: {other_errors}"
            
            # Exactly one success (mutual exclusion property)
            assert len(successes) == 1, (
                f"Expected exactly 1 success, got {len(successes)}: {results}"
            )
            
            # All others should be lock errors
            assert len(lock_errors) == num_processes - 1, (
                f"Expected {num_processes - 1} lock_errors, got {len(lock_errors)}: {results}"
            )

    @given(
        stale_pid=st.integers(min_value=900000, max_value=999999),
    )
    @settings(max_examples=10, deadline=60000)
    def test_stale_lock_atomic_replacement(self, stale_pid: int):
        """
        Feature: backup-robustness, Property 4: Atomic Lock Mutual Exclusion
        
        For any stale lock (containing a dead PID), a new process SHALL
        atomically replace the PID while holding the flock, ensuring no
        race condition during stale lock cleanup.
        
        **Validates: Requirements 4.3, 4.4**
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            lock_path = Path(tmpdir) / "test.lock"
            
            # Create a lock file with a stale PID (very high PID unlikely to exist)
            lock_path.parent.mkdir(parents=True, exist_ok=True)
            lock_path.write_text(str(stale_pid))
            
            # Import here to avoid issues with subprocess
            from devbackup.lock import LockManager
            import os
            
            manager = LockManager(lock_path=lock_path, timeout=5)
            
            # Should be able to acquire despite existing stale lock file
            result = manager.acquire()
            
            # Verify acquisition succeeded
            assert result is True
            
            # Verify our PID was written atomically
            assert manager.get_lock_holder_pid() == os.getpid()
            
            # Verify the stale PID was replaced
            assert manager.get_lock_holder_pid() != stale_pid
            
            manager.release()
