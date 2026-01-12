"""Lock management for devbackup.

This module provides the LockManager class that prevents concurrent backup
executions using fcntl.flock for atomic locking with PID tracking.
"""

import fcntl
import os
import time
from pathlib import Path
from typing import Optional


class LockError(Exception):
    """Raised when lock cannot be acquired."""
    pass


class LockManager:
    """
    Manages exclusive lock for backup process.
    Uses flock for atomic locking with PID tracking.
    
    Implements truly atomic lock acquisition:
    - Uses a single atomic flock operation (no check-then-acquire pattern)
    - Stale detection happens while holding the lock
    - Atomic PID replacement when stale lock is detected
    
    Implements context manager protocol for safe lock handling.
    """
    
    DEFAULT_LOCK_PATH = Path.home() / ".cache/devbackup/backup.lock"
    
    def __init__(self, lock_path: Optional[Path] = None, timeout: int = 5):
        """
        Initialize LockManager.
        
        Args:
            lock_path: Path to lock file. Defaults to ~/.cache/devbackup/backup.lock
            timeout: Timeout in seconds for acquiring lock. Default 5 seconds.
        """
        self.lock_path = lock_path if lock_path is not None else self.DEFAULT_LOCK_PATH
        self.timeout = timeout
        self._lock_fd: Optional[int] = None
        self._acquired = False
    
    def acquire(self) -> bool:
        """
        Atomically acquire exclusive lock.
        
        Process:
        1. Open lock file with O_CREAT (creates if doesn't exist)
        2. Acquire flock (blocking or non-blocking based on timeout)
        3. While holding lock, check if PID is stale
        4. If stale, overwrite PID; if not stale and different PID, release and fail
        5. Write our PID
        
        This eliminates the TOCTOU race condition by performing stale detection
        while holding the lock.
        
        Returns True if lock acquired.
        
        Raises:
            LockError: If lock is held by another process and timeout expires.
        """
        # Ensure parent directory exists
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Open lock file (create if doesn't exist)
        try:
            self._lock_fd = os.open(
                str(self.lock_path),
                os.O_RDWR | os.O_CREAT,
                0o644
            )
        except OSError as e:
            raise LockError(f"Cannot open lock file {self.lock_path}: {e}")
        
        # Try to acquire lock with timeout
        start_time = time.time()
        while True:
            try:
                fcntl.flock(self._lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                # Lock acquired - now check for stale PID while holding the lock
                existing_pid = self._read_pid_from_fd()
                
                if existing_pid is not None and existing_pid != os.getpid():
                    # Check if the existing PID is still running
                    if self._is_process_running(existing_pid):
                        # Process is still running - this shouldn't happen since we hold the lock
                        # But if it does, release and fail
                        fcntl.flock(self._lock_fd, fcntl.LOCK_UN)
                        os.close(self._lock_fd)
                        self._lock_fd = None
                        raise LockError(
                            f"Lock held by running process {existing_pid}"
                        )
                    # Process is dead (stale lock) - we can take over
                    # Fall through to write our PID
                
                # Lock acquired successfully - write our PID atomically
                self._acquired = True
                self._write_pid()
                return True
                
            except BlockingIOError:
                # Lock is held by another process
                elapsed = time.time() - start_time
                if elapsed >= self.timeout:
                    # Close our file descriptor since we couldn't get the lock
                    os.close(self._lock_fd)
                    self._lock_fd = None
                    holder_pid = self.get_lock_holder_pid()
                    if holder_pid:
                        raise LockError(
                            f"Lock held by process {holder_pid} after {self.timeout}s timeout"
                        )
                    else:
                        raise LockError(
                            f"Lock held by another process after {self.timeout}s timeout"
                        )
                # Wait a bit before retrying
                time.sleep(0.1)
    
    def release(self) -> None:
        """Release the lock and remove lock file."""
        if self._lock_fd is not None:
            try:
                # Release the lock
                fcntl.flock(self._lock_fd, fcntl.LOCK_UN)
            except OSError:
                pass  # Ignore errors during unlock
            
            try:
                os.close(self._lock_fd)
            except OSError:
                pass  # Ignore errors during close
            
            self._lock_fd = None
            self._acquired = False
        
        # Remove the lock file
        try:
            if self.lock_path.exists():
                self.lock_path.unlink()
        except OSError:
            pass  # Ignore errors during removal
    
    def is_locked(self) -> bool:
        """Check if lock is currently held (by any process)."""
        if not self.lock_path.exists():
            return False
        
        try:
            fd = os.open(str(self.lock_path), os.O_RDONLY)
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                # We got the lock, so it wasn't held
                fcntl.flock(fd, fcntl.LOCK_UN)
                return False
            except BlockingIOError:
                # Lock is held
                return True
            finally:
                os.close(fd)
        except OSError:
            return False
    
    def get_lock_holder_pid(self) -> Optional[int]:
        """Return PID of process holding lock, or None."""
        if not self.lock_path.exists():
            return None
        
        try:
            content = self.lock_path.read_text().strip()
            if content:
                return int(content)
        except (OSError, ValueError):
            pass
        
        return None
    
    def _read_pid_from_fd(self) -> Optional[int]:
        """Read PID from the lock file using the open file descriptor."""
        if self._lock_fd is None:
            return None
        
        try:
            # Seek to beginning and read
            os.lseek(self._lock_fd, 0, os.SEEK_SET)
            content = os.read(self._lock_fd, 32).decode().strip()
            if content:
                return int(content)
        except (OSError, ValueError):
            pass
        
        return None
    
    def _is_process_running(self, pid: int) -> bool:
        """Check if a process with given PID is running."""
        try:
            # Sending signal 0 doesn't actually send a signal,
            # but checks if the process exists and we have permission
            os.kill(pid, 0)
            return True
        except ProcessLookupError:
            # Process doesn't exist
            return False
        except PermissionError:
            # Process exists but we don't have permission to signal it
            return True
    
    def _write_pid(self) -> None:
        """Write current process PID to lock file atomically."""
        if self._lock_fd is not None:
            try:
                # Truncate and write PID
                os.ftruncate(self._lock_fd, 0)
                os.lseek(self._lock_fd, 0, os.SEEK_SET)
                os.write(self._lock_fd, str(os.getpid()).encode())
            except OSError:
                pass  # Best effort
    
    def __enter__(self) -> "LockManager":
        """Context manager entry - acquire lock."""
        self.acquire()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        """Context manager exit - release lock."""
        self.release()
        return False  # Don't suppress exceptions
