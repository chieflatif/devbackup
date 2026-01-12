"""Property-based tests for signal handler cleanup invariant.

Tests Property 1 (Signal Cleanup Invariant) from the backup-robustness design document.

**Validates: Requirements 1.1, 1.2, 1.3**
"""

import json
import os
import signal
import subprocess
import tempfile
import time
from pathlib import Path

import pytest
from hypothesis import given, strategies as st, settings, Phase


# Strategy for generating signal types
signal_types = st.sampled_from([signal.SIGTERM, signal.SIGINT])

# Strategy for generating backup states
backup_states = st.sampled_from([
    "before_rsync",      # Signal before rsync starts
    "during_rsync",      # Signal while rsync is running
    "after_rsync",       # Signal after rsync completes but before rename
])

# Strategy for generating file counts in in_progress directory
file_counts = st.integers(min_value=0, max_value=10)


class TestSignalCleanupInvariant:
    """
    Property 1: Signal Cleanup Invariant
    
    *For any* backup process that receives SIGTERM or SIGINT, the in_progress 
    directory SHALL be removed and the lock SHALL be released before the 
    process exits.
    
    **Validates: Requirements 1.1, 1.2, 1.3**
    """
    
    @given(
        sig=signal_types,
        num_files=file_counts,
    )
    @settings(max_examples=10, deadline=None, phases=[Phase.generate, Phase.target])
    def test_signal_cleanup_invariant(self, sig: signal.Signals, num_files: int):
        """
        **Feature: backup-robustness, Property 1: Signal Cleanup Invariant**
        
        For any signal (SIGTERM or SIGINT), the in_progress directory must be
        removed and the lock must be released before the process exits.
        
        **Validates: Requirements 1.1, 1.2, 1.3**
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            in_progress = tmpdir_path / "in_progress_test"
            lock_path = tmpdir_path / "test.lock"
            ready_file = tmpdir_path / "ready"
            
            # Python script that sets up signal handler and waits for signal
            worker_script = f'''
import sys
import time
import signal
sys.path.insert(0, ".")
from devbackup.signal_handler import SignalHandler
from devbackup.lock import LockManager
from pathlib import Path

in_progress = Path("{in_progress}")
lock_path = Path("{lock_path}")
ready_file = Path("{ready_file}")
num_files = {num_files}

# Create in_progress directory with files
in_progress.mkdir(parents=True, exist_ok=True)
for i in range(num_files):
    (in_progress / f"file_{{i}}.txt").write_text(f"content {{i}}")

# Acquire lock
lock_manager = LockManager(lock_path=lock_path)
lock_manager.acquire()

# Register signal handler
handler = SignalHandler()
handler.register(in_progress_path=in_progress, lock_manager=lock_manager)

# Signal that we're ready
ready_file.write_text("ready")

# Wait for signal (will be interrupted)
time.sleep(60)
'''
            
            script_path = tmpdir_path / "worker.py"
            script_path.write_text(worker_script)
            
            # Start subprocess
            proc = subprocess.Popen(
                ["python", str(script_path)],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            
            try:
                # Wait for subprocess to be ready
                for _ in range(100):  # 10 seconds max
                    time.sleep(0.1)
                    if ready_file.exists():
                        break
                else:
                    pytest.fail("Subprocess did not become ready in time")
                
                # Verify in_progress exists before signal
                assert in_progress.exists(), "in_progress should exist before signal"
                assert lock_path.exists(), "lock should exist before signal"
                
                # Verify file count
                if num_files > 0:
                    files_in_dir = list(in_progress.iterdir())
                    assert len(files_in_dir) == num_files, \
                        f"Expected {num_files} files, found {len(files_in_dir)}"
                
                # Send signal
                proc.send_signal(sig)
                
                # Wait for process to exit
                proc.wait(timeout=10)
                
                # INVARIANT 1: Exit code must be 128 + signal_number
                expected_exit_code = 128 + sig
                assert proc.returncode == expected_exit_code, \
                    f"Expected exit code {expected_exit_code}, got {proc.returncode}"
                
                # INVARIANT 2: in_progress directory must be removed
                assert not in_progress.exists(), \
                    f"in_progress directory should be cleaned up after {sig.name}"
                
                # INVARIANT 3: Lock must be released
                assert not lock_path.exists(), \
                    f"Lock should be released after {sig.name}"
                
            finally:
                # Ensure subprocess is terminated
                if proc.poll() is None:
                    proc.kill()
                    proc.wait()
    
    @given(sig=signal_types)
    @settings(max_examples=20, deadline=None, phases=[Phase.generate, Phase.target])
    def test_signal_cleanup_with_rsync_process(self, sig: signal.Signals):
        """
        **Feature: backup-robustness, Property 1: Signal Cleanup Invariant**
        
        When a signal is received during rsync execution, the rsync subprocess
        must be terminated before cleanup.
        
        **Validates: Requirements 1.5**
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            in_progress = tmpdir_path / "in_progress_test"
            lock_path = tmpdir_path / "test.lock"
            ready_file = tmpdir_path / "ready"
            rsync_terminated_file = tmpdir_path / "rsync_terminated"
            
            # Python script that simulates rsync with signal handler
            worker_script = f'''
import sys
import time
import signal
import subprocess
sys.path.insert(0, ".")
from devbackup.signal_handler import SignalHandler
from devbackup.lock import LockManager
from pathlib import Path

in_progress = Path("{in_progress}")
lock_path = Path("{lock_path}")
ready_file = Path("{ready_file}")
rsync_terminated_file = Path("{rsync_terminated_file}")

# Create in_progress directory
in_progress.mkdir(parents=True, exist_ok=True)

# Acquire lock
lock_manager = LockManager(lock_path=lock_path)
lock_manager.acquire()

# Register signal handler
handler = SignalHandler()
handler.register(in_progress_path=in_progress, lock_manager=lock_manager)

# Start a long-running subprocess to simulate rsync
# Using sleep as a stand-in for rsync
rsync_proc = subprocess.Popen(["sleep", "60"])
handler.set_rsync_process(rsync_proc)

# Signal that we're ready
ready_file.write_text("ready")

# Wait for signal (will be interrupted)
time.sleep(60)
'''
            
            script_path = tmpdir_path / "worker.py"
            script_path.write_text(worker_script)
            
            # Start subprocess
            proc = subprocess.Popen(
                ["python", str(script_path)],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            
            try:
                # Wait for subprocess to be ready
                for _ in range(100):  # 10 seconds max
                    time.sleep(0.1)
                    if ready_file.exists():
                        break
                else:
                    pytest.fail("Subprocess did not become ready in time")
                
                # Small delay to ensure rsync subprocess is started
                time.sleep(0.2)
                
                # Send signal
                proc.send_signal(sig)
                
                # Wait for process to exit
                proc.wait(timeout=15)
                
                # INVARIANT: Exit code must be 128 + signal_number
                expected_exit_code = 128 + sig
                assert proc.returncode == expected_exit_code, \
                    f"Expected exit code {expected_exit_code}, got {proc.returncode}"
                
                # INVARIANT: in_progress directory must be removed
                assert not in_progress.exists(), \
                    f"in_progress directory should be cleaned up after {sig.name}"
                
                # INVARIANT: Lock must be released
                assert not lock_path.exists(), \
                    f"Lock should be released after {sig.name}"
                
            finally:
                # Ensure subprocess is terminated
                if proc.poll() is None:
                    proc.kill()
                    proc.wait()
    
    @given(
        sig=signal_types,
        has_in_progress=st.booleans(),
        has_lock=st.booleans(),
    )
    @settings(max_examples=50, deadline=None, phases=[Phase.generate, Phase.target])
    def test_signal_cleanup_partial_state(
        self, 
        sig: signal.Signals, 
        has_in_progress: bool,
        has_lock: bool,
    ):
        """
        **Feature: backup-robustness, Property 1: Signal Cleanup Invariant**
        
        Signal cleanup must work correctly regardless of which resources
        are currently held (in_progress, lock, or both).
        
        **Validates: Requirements 1.1, 1.2, 1.3**
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            in_progress = tmpdir_path / "in_progress_test"
            lock_path = tmpdir_path / "test.lock"
            ready_file = tmpdir_path / "ready"
            
            # Python script with configurable state
            worker_script = f'''
import sys
import time
import signal
sys.path.insert(0, ".")
from devbackup.signal_handler import SignalHandler
from devbackup.lock import LockManager
from pathlib import Path

in_progress = Path("{in_progress}")
lock_path = Path("{lock_path}")
ready_file = Path("{ready_file}")
has_in_progress = {has_in_progress}
has_lock = {has_lock}

# Conditionally create in_progress directory
if has_in_progress:
    in_progress.mkdir(parents=True, exist_ok=True)
    (in_progress / "test.txt").write_text("test")

# Conditionally acquire lock
lock_manager = None
if has_lock:
    lock_manager = LockManager(lock_path=lock_path)
    lock_manager.acquire()

# Register signal handler with whatever state we have
handler = SignalHandler()
handler.register(
    in_progress_path=in_progress if has_in_progress else None,
    lock_manager=lock_manager,
)

# Signal that we're ready
ready_file.write_text("ready")

# Wait for signal (will be interrupted)
time.sleep(60)
'''
            
            script_path = tmpdir_path / "worker.py"
            script_path.write_text(worker_script)
            
            # Start subprocess
            proc = subprocess.Popen(
                ["python", str(script_path)],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            
            try:
                # Wait for subprocess to be ready
                for _ in range(100):  # 10 seconds max
                    time.sleep(0.1)
                    if ready_file.exists():
                        break
                else:
                    pytest.fail("Subprocess did not become ready in time")
                
                # Verify initial state
                if has_in_progress:
                    assert in_progress.exists(), "in_progress should exist before signal"
                if has_lock:
                    assert lock_path.exists(), "lock should exist before signal"
                
                # Send signal
                proc.send_signal(sig)
                
                # Wait for process to exit
                proc.wait(timeout=10)
                
                # INVARIANT: Exit code must be 128 + signal_number
                expected_exit_code = 128 + sig
                assert proc.returncode == expected_exit_code, \
                    f"Expected exit code {expected_exit_code}, got {proc.returncode}"
                
                # INVARIANT: in_progress directory must be removed (if it existed)
                if has_in_progress:
                    assert not in_progress.exists(), \
                        f"in_progress directory should be cleaned up after {sig.name}"
                
                # INVARIANT: Lock must be released (if it was held)
                if has_lock:
                    assert not lock_path.exists(), \
                        f"Lock should be released after {sig.name}"
                
            finally:
                # Ensure subprocess is terminated
                if proc.poll() is None:
                    proc.kill()
                    proc.wait()
