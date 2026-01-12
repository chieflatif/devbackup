"""Tests for signal handler.

Feature: backup-robustness
"""

import os
import signal
import tempfile
import shutil
from pathlib import Path
from unittest.mock import Mock, patch
import subprocess

import pytest

from devbackup.signal_handler import SignalHandler
from devbackup.lock import LockManager


class TestSignalHandlerRegistration:
    """Unit tests for SignalHandler registration and unregistration."""

    def test_register_sets_up_handlers(self):
        """Test that register() sets up signal handlers."""
        handler = SignalHandler()
        
        assert not handler.is_registered
        
        handler.register()
        
        assert handler.is_registered
        
        handler.unregister()
        
        assert not handler.is_registered

    def test_register_with_in_progress_path(self):
        """Test that register() stores in_progress_path."""
        with tempfile.TemporaryDirectory() as tmpdir:
            in_progress = Path(tmpdir) / "in_progress_test"
            in_progress.mkdir()
            
            handler = SignalHandler()
            handler.register(in_progress_path=in_progress)
            
            assert handler._in_progress_path == in_progress
            
            handler.unregister()
            
            assert handler._in_progress_path is None

    def test_register_with_lock_manager(self):
        """Test that register() stores lock_manager."""
        with tempfile.TemporaryDirectory() as tmpdir:
            lock_path = Path(tmpdir) / "test.lock"
            lock_manager = LockManager(lock_path=lock_path)
            
            handler = SignalHandler()
            handler.register(lock_manager=lock_manager)
            
            assert handler._lock_manager == lock_manager
            
            handler.unregister()
            
            assert handler._lock_manager is None

    def test_unregister_restores_original_handlers(self):
        """Test that unregister() restores original signal handlers."""
        # Get original handlers
        original_sigterm = signal.getsignal(signal.SIGTERM)
        original_sigint = signal.getsignal(signal.SIGINT)
        
        handler = SignalHandler()
        handler.register()
        
        # Handlers should be changed
        assert signal.getsignal(signal.SIGTERM) != original_sigterm
        assert signal.getsignal(signal.SIGINT) != original_sigint
        
        handler.unregister()
        
        # Handlers should be restored
        assert signal.getsignal(signal.SIGTERM) == original_sigterm
        assert signal.getsignal(signal.SIGINT) == original_sigint

    def test_set_in_progress_path_after_registration(self):
        """Test that set_in_progress_path() updates the path."""
        with tempfile.TemporaryDirectory() as tmpdir:
            in_progress = Path(tmpdir) / "in_progress_test"
            in_progress.mkdir()
            
            handler = SignalHandler()
            handler.register()
            
            assert handler._in_progress_path is None
            
            handler.set_in_progress_path(in_progress)
            
            assert handler._in_progress_path == in_progress
            
            handler.unregister()


class TestSignalHandlerRsyncProcess:
    """Unit tests for rsync process tracking."""

    def test_set_rsync_process(self):
        """Test that set_rsync_process() stores the process."""
        handler = SignalHandler()
        
        mock_process = Mock(spec=subprocess.Popen)
        handler.set_rsync_process(mock_process)
        
        assert handler._rsync_process == mock_process

    def test_set_rsync_process_to_none(self):
        """Test that set_rsync_process(None) clears the process."""
        handler = SignalHandler()
        
        mock_process = Mock(spec=subprocess.Popen)
        handler.set_rsync_process(mock_process)
        handler.set_rsync_process(None)
        
        assert handler._rsync_process is None


class TestSignalHandlerCleanup:
    """Unit tests for cleanup functionality."""

    def test_cleanup_removes_in_progress_directory(self):
        """Test that cleanup() removes in_progress directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            in_progress = Path(tmpdir) / "in_progress_test"
            in_progress.mkdir()
            (in_progress / "test_file.txt").write_text("test")
            
            handler = SignalHandler()
            handler.register(in_progress_path=in_progress)
            
            assert in_progress.exists()
            
            result = handler.cleanup()
            
            assert result is True
            assert not in_progress.exists()
            
            handler.unregister()

    def test_cleanup_releases_lock(self):
        """Test that cleanup() releases the lock."""
        with tempfile.TemporaryDirectory() as tmpdir:
            lock_path = Path(tmpdir) / "test.lock"
            lock_manager = LockManager(lock_path=lock_path)
            lock_manager.acquire()
            
            handler = SignalHandler()
            handler.register(lock_manager=lock_manager)
            
            assert lock_path.exists()
            
            result = handler.cleanup()
            
            assert result is True
            assert not lock_path.exists()
            
            handler.unregister()

    def test_cleanup_terminates_rsync_process(self):
        """Test that cleanup() terminates rsync subprocess."""
        handler = SignalHandler()
        
        mock_process = Mock(spec=subprocess.Popen)
        mock_process.terminate = Mock()
        mock_process.wait = Mock()
        
        handler.set_rsync_process(mock_process)
        
        result = handler.cleanup()
        
        assert result is True
        mock_process.terminate.assert_called_once()
        mock_process.wait.assert_called_once()

    def test_cleanup_returns_false_when_nothing_to_clean(self):
        """Test that cleanup() returns False when nothing to clean."""
        handler = SignalHandler()
        handler.register()
        
        result = handler.cleanup()
        
        assert result is False
        
        handler.unregister()


class TestSignalHandlerIntegration:
    """Integration tests for signal handling."""

    def test_signal_handler_in_subprocess(self):
        """
        Test that signal handler properly cleans up in a subprocess.
        
        This test spawns a subprocess that:
        1. Creates an in_progress directory
        2. Registers signal handler
        3. Receives SIGTERM
        4. Should clean up and exit with 128 + SIGTERM
        """
        import json
        
        with tempfile.TemporaryDirectory() as tmpdir:
            in_progress = Path(tmpdir) / "in_progress_test"
            lock_path = Path(tmpdir) / "test.lock"
            result_file = Path(tmpdir) / "result.json"
            
            # Python script that sets up signal handler and waits for signal
            worker_script = f'''
import sys
import time
import signal
import json
sys.path.insert(0, ".")
from devbackup.signal_handler import SignalHandler
from devbackup.lock import LockManager
from pathlib import Path

in_progress = Path("{in_progress}")
lock_path = Path("{lock_path}")
result_file = Path("{result_file}")

# Create in_progress directory
in_progress.mkdir(parents=True, exist_ok=True)
(in_progress / "test.txt").write_text("test")

# Acquire lock
lock_manager = LockManager(lock_path=lock_path)
lock_manager.acquire()

# Register signal handler
handler = SignalHandler()
handler.register(in_progress_path=in_progress, lock_manager=lock_manager)

# Write that we're ready
result_file.write_text(json.dumps({{"status": "ready", "pid": {os.getpid()}}}))

# Wait for signal (will be interrupted by SIGTERM)
time.sleep(30)

# If we get here, signal wasn't received
result_file.write_text(json.dumps({{"status": "timeout"}}))
'''
            
            script_path = Path(tmpdir) / "worker.py"
            script_path.write_text(worker_script)
            
            # Start subprocess
            proc = subprocess.Popen(
                ["python", str(script_path)],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            
            # Wait for subprocess to be ready
            import time
            for _ in range(50):  # 5 seconds max
                time.sleep(0.1)
                if result_file.exists():
                    result = json.loads(result_file.read_text())
                    if result.get("status") == "ready":
                        break
            
            # Verify in_progress exists before signal
            assert in_progress.exists(), "in_progress should exist before signal"
            assert lock_path.exists(), "lock should exist before signal"
            
            # Send SIGTERM
            proc.send_signal(signal.SIGTERM)
            
            # Wait for process to exit
            proc.wait(timeout=10)
            
            # Verify exit code is 128 + SIGTERM (143 on most systems)
            expected_exit_code = 128 + signal.SIGTERM
            assert proc.returncode == expected_exit_code, \
                f"Expected exit code {expected_exit_code}, got {proc.returncode}"
            
            # Verify cleanup happened
            assert not in_progress.exists(), "in_progress should be cleaned up after signal"
            assert not lock_path.exists(), "lock should be released after signal"
