"""Signal handling for graceful shutdown during backup operations.

This module provides the SignalHandler class that handles OS signals
(SIGTERM, SIGINT) for graceful shutdown, ensuring cleanup of in_progress
directories and lock release when the process is interrupted.

Requirements: 1.1, 1.2, 1.3, 1.4, 1.5
"""

import signal
import sys
import shutil
import logging
from pathlib import Path
from typing import Optional, Dict, Any
import subprocess


class SignalHandler:
    """
    Handles OS signals for graceful shutdown during backup operations.
    
    Ensures cleanup of in_progress directories and lock release on termination.
    
    Usage:
        handler = SignalHandler()
        handler.register(in_progress_path=path, lock_manager=lock)
        handler.set_rsync_process(process)
        # ... do backup ...
        handler.unregister()
    """
    
    def __init__(self):
        """Initialize SignalHandler with empty state."""
        self._in_progress_path: Optional[Path] = None
        self._lock_manager: Optional[Any] = None  # LockManager type
        self._rsync_process: Optional[subprocess.Popen] = None
        self._original_handlers: Dict[int, Any] = {}
        self._registered = False
        self._logger = logging.getLogger(__name__)
    
    def register(
        self,
        in_progress_path: Optional[Path] = None,
        lock_manager: Optional[Any] = None,
    ) -> None:
        """
        Register signal handlers for SIGTERM and SIGINT.
        
        Note: Signal handlers can only be registered from the main thread.
        If called from a non-main thread, this method will log a warning
        and skip signal registration, but still store the paths for cleanup.
        
        Args:
            in_progress_path: Path to in_progress directory to clean up on signal
            lock_manager: Lock manager to release on signal
        
        Requirements: 1.1, 1.2
        """
        self._in_progress_path = in_progress_path
        self._lock_manager = lock_manager
        
        # Signal handlers can only be registered from the main thread
        # Check if we're in the main thread before attempting registration
        import threading
        if threading.current_thread() is not threading.main_thread():
            self._logger.debug(
                "Signal handlers not registered: not running in main thread"
            )
            # Still mark as "registered" so cleanup paths are tracked
            self._registered = True
            return
        
        # Store original handlers and register our handlers
        try:
            for sig in (signal.SIGTERM, signal.SIGINT):
                self._original_handlers[sig] = signal.signal(sig, self._handle_signal)
            self._registered = True
            self._logger.debug("Signal handlers registered")
        except ValueError as e:
            # This can happen if signal() is called from a non-main thread
            self._logger.debug(f"Signal handlers not registered: {e}")
            self._registered = True
    
    def set_in_progress_path(self, path: Optional[Path]) -> None:
        """
        Update the in_progress path after registration.
        
        This allows setting the path after the directory is created.
        
        Args:
            path: Path to in_progress directory
        """
        self._in_progress_path = path
    
    def set_rsync_process(self, process: Optional[subprocess.Popen]) -> None:
        """
        Set the rsync subprocess for termination on signal.
        
        Args:
            process: The rsync subprocess to terminate on signal
        
        Requirements: 1.5
        """
        self._rsync_process = process
    
    def unregister(self) -> None:
        """
        Restore original signal handlers.
        
        Should be called after backup completes (success or failure).
        Only restores handlers if they were actually registered (main thread only).
        """
        if not self._registered:
            return
        
        # Restore original handlers (only if we actually registered them)
        if self._original_handlers:
            import threading
            if threading.current_thread() is threading.main_thread():
                try:
                    for sig, handler in self._original_handlers.items():
                        signal.signal(sig, handler)
                except ValueError:
                    # Ignore errors if we can't restore handlers
                    pass
        
        self._original_handlers.clear()
        self._in_progress_path = None
        self._lock_manager = None
        self._rsync_process = None
        self._registered = False
        self._logger.debug("Signal handlers unregistered")
    
    def _handle_signal(self, signum: int, frame: Any) -> None:
        """
        Handle termination signal.
        
        Process:
        1. Terminate rsync subprocess if running
        2. Clean up in_progress directory
        3. Release lock
        4. Exit with 128 + signal_number (Unix convention)
        
        Args:
            signum: Signal number received
            frame: Current stack frame (unused)
        
        Requirements: 1.1, 1.2, 1.3, 1.4, 1.5
        """
        sig_name = signal.Signals(signum).name
        self._logger.warning(f"Received {sig_name}, initiating graceful shutdown")
        
        # Step 1: Terminate rsync subprocess if running (Requirement 1.5)
        if self._rsync_process is not None:
            try:
                self._rsync_process.terminate()
                # Give it a moment to terminate gracefully
                try:
                    self._rsync_process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    # Force kill if it doesn't terminate
                    self._rsync_process.kill()
                    self._rsync_process.wait()
                self._logger.debug("Rsync subprocess terminated")
            except Exception as e:
                self._logger.warning(f"Error terminating rsync process: {e}")
        
        # Step 2: Clean up in_progress directory (Requirements 1.1, 1.2)
        if self._in_progress_path is not None and self._in_progress_path.exists():
            try:
                shutil.rmtree(self._in_progress_path)
                self._logger.info(f"Cleaned up in_progress directory: {self._in_progress_path}")
            except Exception as e:
                self._logger.warning(f"Error cleaning up in_progress directory: {e}")
        
        # Step 3: Release lock (Requirement 1.3)
        if self._lock_manager is not None:
            try:
                self._lock_manager.release()
                self._logger.debug("Lock released")
            except Exception as e:
                self._logger.warning(f"Error releasing lock: {e}")
        
        # Step 4: Exit with 128 + signal_number (Requirement 1.4)
        # This is the standard Unix convention for signal exits
        exit_code = 128 + signum
        self._logger.info(f"Exiting with code {exit_code}")
        sys.exit(exit_code)
    
    @property
    def is_registered(self) -> bool:
        """Return whether signal handlers are currently registered."""
        return self._registered
    
    def cleanup(self) -> bool:
        """
        Perform cleanup without exiting.
        
        This is useful for testing or manual cleanup scenarios.
        
        Returns:
            True if cleanup was performed, False if nothing to clean up
        """
        cleaned = False
        
        # Terminate rsync if running
        if self._rsync_process is not None:
            try:
                self._rsync_process.terminate()
                self._rsync_process.wait(timeout=5)
                cleaned = True
            except Exception:
                try:
                    self._rsync_process.kill()
                    self._rsync_process.wait()
                    cleaned = True
                except Exception:
                    pass
        
        # Clean up in_progress directory
        if self._in_progress_path is not None and self._in_progress_path.exists():
            try:
                shutil.rmtree(self._in_progress_path)
                cleaned = True
            except Exception:
                pass
        
        # Release lock
        if self._lock_manager is not None:
            try:
                self._lock_manager.release()
                cleaned = True
            except Exception:
                pass
        
        return cleaned
