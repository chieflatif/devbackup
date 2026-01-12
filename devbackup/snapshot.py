"""Snapshot engine for devbackup.

This module provides the SnapshotEngine class that creates incremental
snapshots using rsync with hard links for space efficiency.
"""

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set
import logging
import os
import shutil
import subprocess
import tempfile
import time

from devbackup.progress import ProgressInfo, ProgressReporter
from devbackup.retry import (
    RetryConfig,
    RetryResult,
    RetryAttempt,
    is_retryable_error,
    retry_with_backoff,
)
from devbackup.verify import IntegrityVerifier


# Logger for symlink warnings
logger = logging.getLogger(__name__)


class SnapshotError(Exception):
    """Raised when snapshot creation fails."""
    pass


@dataclass
class SnapshotResult:
    """Result of a snapshot operation."""
    success: bool
    snapshot_path: Optional[Path]
    files_transferred: int  # Files that were actually copied (changed)
    total_size: int
    duration_seconds: float
    error_message: Optional[str]
    total_files: int = 0  # Total files in backup (including unchanged)
    retry_result: Optional[RetryResult] = None


@dataclass
class SnapshotInfo:
    """Information about a snapshot."""
    path: Path
    timestamp: datetime
    size_bytes: int
    file_count: int


class SnapshotEngine:
    """
    Creates incremental snapshots using rsync with hard links.
    
    The engine creates timestamped snapshot directories in the format
    YYYY-MM-DD-HHMMSS. Unchanged files are hard-linked to the previous
    snapshot for space efficiency.
    """
    
    # Timestamp format for snapshot directories
    TIMESTAMP_FORMAT = "%Y-%m-%d-%H%M%S"
    
    # Prefix for in-progress snapshots
    IN_PROGRESS_PREFIX = "in_progress_"
    
    def __init__(
        self,
        destination: Path,
        exclude_patterns: List[str],
        retry_config: Optional[RetryConfig] = None,
    ):
        """
        Initialize the snapshot engine.
        
        Args:
            destination: Path to the backup destination directory
            exclude_patterns: List of rsync exclude patterns
            retry_config: Optional retry configuration for transient failures
                         (Requirements: 10.1, 10.2)
        """
        self.destination = Path(destination)
        self.exclude_patterns = exclude_patterns
        self.retry_config = retry_config or RetryConfig()
        self._current_progress_reporter: Optional[ProgressReporter] = None

    def _generate_timestamp(self) -> str:
        """
        Generate YYYY-MM-DD-HHMMSS timestamp for snapshot directory name.
        
        Returns:
            Formatted timestamp string
        """
        return datetime.now().strftime(self.TIMESTAMP_FORMAT)
    
    def _generate_unique_snapshot_name(self) -> str:
        """
        Generate a unique snapshot name, handling timestamp collisions.
        
        If a snapshot with the same timestamp already exists, appends a
        sequence number (01-99). If 99 snapshots exist for the same second,
        waits one second and retries.
        
        Requirements: 8.1, 8.2, 8.3, 8.4
        
        Returns:
            Unique snapshot name in format YYYY-MM-DD-HHMMSS or YYYY-MM-DD-HHMMSS-NN
        """
        max_retries = 3  # Maximum times to wait and retry for a new second
        
        for _ in range(max_retries):
            timestamp = self._generate_timestamp()
            
            # Check if base timestamp is available
            base_path = self.destination / timestamp
            in_progress_path = self.destination / f"{self.IN_PROGRESS_PREFIX}{timestamp}"
            
            if not base_path.exists() and not in_progress_path.exists():
                return timestamp
            
            # Timestamp collision detected - try sequence numbers 01-99
            # Requirements: 8.1, 8.2
            for seq in range(1, 100):
                seq_name = f"{timestamp}-{seq:02d}"
                seq_path = self.destination / seq_name
                seq_in_progress = self.destination / f"{self.IN_PROGRESS_PREFIX}{seq_name}"
                
                if not seq_path.exists() and not seq_in_progress.exists():
                    logger.debug(f"Timestamp collision detected, using sequence number: {seq_name}")
                    return seq_name
            
            # All 99 sequence numbers exhausted - wait and retry
            # Requirement: 8.3
            logger.warning(f"All sequence numbers exhausted for {timestamp}, waiting 1 second")
            time.sleep(1)
        
        # After max retries, generate a new timestamp (should be different now)
        return self._generate_timestamp()
    
    def _snapshot_name_exists(self, name: str) -> bool:
        """
        Check if a snapshot name already exists (either complete or in-progress).
        
        Args:
            name: Snapshot name to check
            
        Returns:
            True if the name is already in use
        """
        complete_path = self.destination / name
        in_progress_path = self.destination / f"{self.IN_PROGRESS_PREFIX}{name}"
        return complete_path.exists() or in_progress_path.exists()
    
    def _parse_snapshot_name(self, name: str) -> Optional[datetime]:
        """
        Parse a snapshot name to extract its timestamp.
        
        Handles both formats:
        - YYYY-MM-DD-HHMMSS (base format)
        - YYYY-MM-DD-HHMMSS-NN (with sequence number)
        
        Args:
            name: Snapshot directory name
            
        Returns:
            datetime if valid snapshot name, None otherwise
        """
        # Try base format first
        try:
            return datetime.strptime(name, self.TIMESTAMP_FORMAT)
        except ValueError:
            pass
        
        # Try format with sequence number (YYYY-MM-DD-HHMMSS-NN)
        # The sequence number is 2 digits, so we strip the last 3 chars (-NN)
        if len(name) == 20 and name[-3] == '-':
            try:
                # Verify the sequence number is valid (01-99)
                seq_str = name[-2:]
                seq_num = int(seq_str)
                if 1 <= seq_num <= 99:
                    base_name = name[:-3]
                    return datetime.strptime(base_name, self.TIMESTAMP_FORMAT)
            except ValueError:
                pass
        
        return None
    
    def find_latest_snapshot(self) -> Optional[Path]:
        """
        Find the most recent complete snapshot directory.
        
        Ignores in_progress_* directories and returns the snapshot
        with the most recent timestamp. Handles both base format
        (YYYY-MM-DD-HHMMSS) and sequence format (YYYY-MM-DD-HHMMSS-NN).
        
        Returns:
            Path to the latest snapshot, or None if no snapshots exist
        """
        if not self.destination.exists():
            return None
        
        snapshots = []
        for entry in self.destination.iterdir():
            if not entry.is_dir():
                continue
            # Skip in-progress directories
            if entry.name.startswith(self.IN_PROGRESS_PREFIX):
                continue
            # Skip metadata directory
            if entry.name.startswith("."):
                continue
            # Try to parse as timestamp (handles both formats)
            timestamp = self._parse_snapshot_name(entry.name)
            if timestamp is not None:
                # Use the full name for sorting to handle sequence numbers correctly
                # e.g., 2025-01-01-120000-02 should come after 2025-01-01-120000-01
                snapshots.append((entry.name, entry))
        
        if not snapshots:
            return None
        
        # Sort by name descending (lexicographic sort works for our format)
        snapshots.sort(key=lambda x: x[0], reverse=True)
        return snapshots[0][1]
    
    def _create_exclude_file(self) -> Path:
        """
        Create a temporary file with exclude patterns for rsync.
        
        Returns:
            Path to the temporary exclude file
        """
        # Create a temporary file that won't be auto-deleted
        fd, path = tempfile.mkstemp(prefix="devbackup_exclude_", suffix=".txt")
        try:
            with os.fdopen(fd, 'w') as f:
                for pattern in self.exclude_patterns:
                    f.write(f"{pattern}\n")
        except Exception:
            os.close(fd)
            raise
        return Path(path)
    
    def _build_rsync_command(
        self,
        sources: List[Path],
        dest: Path,
        link_dest: Optional[Path] = None,
        with_progress: bool = False
    ) -> List[str]:
        """
        Build rsync command with all required flags.
        
        Flags used:
        - -a (archive): preserves permissions, timestamps, symlinks, etc.
        - -v (verbose): verbose output for logging
        - --delete: remove files from dest that don't exist in source
        - --link-dest: create hard links to unchanged files in reference dir
        - --exclude-from: file containing exclude patterns
        - --progress: show progress during transfer (Requirements: 6.1)
        - --stats: show file transfer statistics at end
        
        Note: We use --progress instead of --info=progress2 for broader
        compatibility with older rsync versions (including macOS openrsync).
        
        Args:
            sources: List of source directories to back up
            dest: Destination directory for this snapshot
            link_dest: Optional path to previous snapshot for hard linking
            with_progress: If True, add --progress for progress reporting
        
        Returns:
            List of command arguments for subprocess
        """
        cmd = ["rsync", "-av", "--delete", "--stats"]
        
        # Add progress reporting flag (Requirements: 6.1)
        # Use --progress for broader compatibility (works with openrsync on macOS)
        if with_progress:
            cmd.append("--progress")
        
        # Add link-dest if we have a previous snapshot
        if link_dest is not None:
            cmd.append(f"--link-dest={link_dest}")
        
        # Create exclude file and add to command
        exclude_file = self._create_exclude_file()
        cmd.append(f"--exclude-from={exclude_file}")
        
        # Store exclude file path for cleanup
        self._current_exclude_file = exclude_file
        
        # Add source directories (with trailing slash to copy contents)
        for source in sources:
            # Ensure trailing slash so rsync copies contents, not the dir itself
            source_str = str(source)
            if not source_str.endswith("/"):
                source_str += "/"
            cmd.append(source_str)
        
        # Add destination
        cmd.append(str(dest) + "/")
        
        return cmd

    def create_snapshot(
        self,
        sources: List[Path],
        signal_handler: Optional[Any] = None,
        progress_callback: Optional[Callable[[ProgressInfo], None]] = None,
    ) -> SnapshotResult:
        """
        Create a new incremental snapshot.
        
        Process:
        1. Generate unique snapshot name (handles timestamp collisions)
        2. Find latest complete snapshot for --link-dest
        3. Create in_progress_TIMESTAMP directory
        4. Execute rsync with appropriate flags (with retry on transient failures)
        5. On success: atomically rename to final TIMESTAMP
        6. On failure: delete in_progress directory
        
        Args:
            sources: List of source directories to back up
            signal_handler: Optional SignalHandler for graceful shutdown support
            progress_callback: Optional callback for progress updates (Requirements: 6.1, 6.2)
        
        Returns:
            SnapshotResult with success status and statistics
        """
        start_time = time.time()
        
        # Ensure destination exists before checking for collisions
        # Requirement: 8.4 - detect collisions before creating in_progress
        self.destination.mkdir(parents=True, exist_ok=True)
        
        # Generate unique snapshot name, handling timestamp collisions
        # Requirements: 8.1, 8.2, 8.3, 8.4
        snapshot_name = self._generate_unique_snapshot_name()
        
        # Create in-progress directory
        in_progress_name = f"{self.IN_PROGRESS_PREFIX}{snapshot_name}"
        in_progress_path = self.destination / in_progress_name
        final_path = self.destination / snapshot_name
        
        # Initialize tracking variables
        self._current_exclude_file: Optional[Path] = None
        files_transferred = 0
        total_size = 0
        retry_result: Optional[RetryResult] = None
        
        # Initialize progress reporter if callback provided (Requirements: 6.1, 6.2)
        progress_reporter: Optional[ProgressReporter] = None
        if progress_callback is not None:
            progress_reporter = ProgressReporter(callback=progress_callback)
            self._current_progress_reporter = progress_reporter
        else:
            self._current_progress_reporter = None
        
        try:
            # Create in-progress directory
            in_progress_path.mkdir(parents=True, exist_ok=True)
            
            # Update signal handler with in_progress path for cleanup on signal
            # Requirements: 1.1, 1.2
            if signal_handler is not None:
                signal_handler.set_in_progress_path(in_progress_path)
            
            # Find latest snapshot for hard linking
            link_dest = self.find_latest_snapshot()
            
            # Build rsync command (with progress if callback provided)
            cmd = self._build_rsync_command(
                sources, 
                in_progress_path, 
                link_dest,
                with_progress=(progress_callback is not None)
            )
            
            # Execute rsync with retry logic (Requirements: 10.1, 10.2, 10.4, 10.5)
            # Get timeout from retry config (default 1 hour)
            rsync_timeout = self.retry_config.rsync_timeout_seconds if hasattr(self.retry_config, 'rsync_timeout_seconds') else 3600
            
            def execute_rsync() -> tuple[int, str, tuple[str, str]]:
                """Execute rsync and return (return_code, error_message, (stdout, stderr))."""
                rsync_process = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    # Use binary mode and decode manually to handle encoding errors
                )
                
                # Set rsync process in signal handler for termination on signal
                if signal_handler is not None:
                    signal_handler.set_rsync_process(rsync_process)
                
                try:
                    # Read output in real-time for progress reporting (Requirements: 6.1, 6.2)
                    stdout_lines = []
                    if progress_reporter is not None and rsync_process.stdout:
                        # For progress reporting, we read line by line
                        # Use a thread to handle timeout
                        import threading
                        timeout_event = threading.Event()
                        
                        def read_output():
                            try:
                                for line_bytes in rsync_process.stdout:
                                    if timeout_event.is_set():
                                        break
                                    # Decode with error handling for special characters
                                    line = line_bytes.decode('utf-8', errors='replace')
                                    stdout_lines.append(line)
                                    progress_reporter.parse_rsync_output(line)
                            except Exception:
                                pass
                        
                        reader_thread = threading.Thread(target=read_output, daemon=True)
                        reader_thread.start()
                        reader_thread.join(timeout=rsync_timeout)
                        
                        if reader_thread.is_alive():
                            # Timeout occurred
                            timeout_event.set()
                            rsync_process.terminate()
                            try:
                                rsync_process.wait(timeout=5)
                            except subprocess.TimeoutExpired:
                                rsync_process.kill()
                            
                            # Clear rsync process from signal handler
                            if signal_handler is not None:
                                signal_handler.set_rsync_process(None)
                            
                            return 30, f"rsync timed out after {rsync_timeout} seconds", (''.join(stdout_lines), '')
                        
                        stdout = ''.join(stdout_lines)
                        stderr_bytes = rsync_process.stderr.read() if rsync_process.stderr else b''
                        stderr = stderr_bytes.decode('utf-8', errors='replace') if stderr_bytes else ''
                        rsync_process.wait()
                    else:
                        # Wait for rsync to complete with timeout
                        try:
                            stdout_bytes, stderr_bytes = rsync_process.communicate(timeout=rsync_timeout)
                            stdout = stdout_bytes.decode('utf-8', errors='replace') if stdout_bytes else ''
                            stderr = stderr_bytes.decode('utf-8', errors='replace') if stderr_bytes else ''
                        except subprocess.TimeoutExpired:
                            rsync_process.terminate()
                            try:
                                rsync_process.wait(timeout=5)
                            except subprocess.TimeoutExpired:
                                rsync_process.kill()
                            
                            # Clear rsync process from signal handler
                            if signal_handler is not None:
                                signal_handler.set_rsync_process(None)
                            
                            return 30, f"rsync timed out after {rsync_timeout} seconds", ('', '')
                    
                    returncode = rsync_process.returncode
                    
                    # Clear rsync process from signal handler
                    if signal_handler is not None:
                        signal_handler.set_rsync_process(None)
                    
                    error_msg = stderr.strip() if stderr else f"rsync exited with code {returncode}"
                    return returncode, error_msg, (stdout, stderr)
                except Exception as e:
                    # Clear rsync process from signal handler on any error
                    if signal_handler is not None:
                        signal_handler.set_rsync_process(None)
                    raise
            
            # Define retry callback for logging (Requirements: 10.4)
            def on_retry(attempt: RetryAttempt) -> None:
                logger.warning(
                    f"Rsync retry attempt {attempt.attempt_number}: "
                    f"error code {attempt.error_code} - {attempt.error_message}"
                )
            
            # Execute with retry logic (Requirements: 10.1, 10.2)
            retry_result, rsync_output = retry_with_backoff(
                operation=execute_rsync,
                max_retries=self.retry_config.max_retries,
                base_delay=self.retry_config.base_delay_seconds,
                max_delay=self.retry_config.max_delay_seconds,
                on_retry=on_retry,
            )
            
            # Clean up exclude file
            if self._current_exclude_file and self._current_exclude_file.exists():
                self._current_exclude_file.unlink()
                self._current_exclude_file = None
            
            # Check if rsync succeeded
            if not retry_result.success:
                # rsync failed - clean up in-progress directory
                if in_progress_path.exists():
                    shutil.rmtree(in_progress_path)
                
                # Clear in_progress path from signal handler
                if signal_handler is not None:
                    signal_handler.set_in_progress_path(None)
                
                # Build error message with retry history (Requirements: 10.5)
                error_msg = retry_result.final_error_message or f"rsync failed with code {retry_result.final_return_code}"
                if retry_result.attempts:
                    error_msg = f"{error_msg}\n{retry_result.retry_history}"
                
                return SnapshotResult(
                    success=False,
                    snapshot_path=None,
                    files_transferred=0,
                    total_size=0,
                    duration_seconds=time.time() - start_time,
                    error_message=error_msg,
                    retry_result=retry_result,
                )
            
            # Extract stdout from successful result
            stdout = rsync_output[0] if rsync_output else ""
            
            # Parse rsync output for statistics
            files_transferred, total_files, total_size = self._parse_rsync_output(stdout)
            
            # Report final progress (Requirements: 6.5)
            if progress_reporter is not None:
                progress_reporter.report_final(
                    files_transferred=files_transferred,
                    total_size=total_size,
                    duration_seconds=time.time() - start_time,
                )
            
            # Atomically rename to final name
            in_progress_path.rename(final_path)
            
            # Clear in_progress path from signal handler (snapshot is now complete)
            if signal_handler is not None:
                signal_handler.set_in_progress_path(None)
            
            # Create manifest for the snapshot (Requirements: 7.1)
            try:
                verifier = IntegrityVerifier()
                manifest = verifier.create_manifest(final_path)
                verifier.save_manifest(manifest, final_path)
                logger.debug(f"Created manifest for snapshot {final_path.name} with {manifest.file_count} files")
                # Use manifest file count as total if we didn't get it from rsync
                if total_files == 0:
                    total_files = manifest.file_count
            except Exception as manifest_error:
                # Log but don't fail the backup if manifest creation fails
                logger.warning(f"Failed to create manifest for snapshot: {manifest_error}")
            
            return SnapshotResult(
                success=True,
                snapshot_path=final_path,
                files_transferred=files_transferred,
                total_files=total_files,
                total_size=total_size,
                duration_seconds=time.time() - start_time,
                error_message=None,
                retry_result=retry_result,
            )
            
        except Exception as e:
            # Clean up on any error
            if self._current_exclude_file and self._current_exclude_file.exists():
                try:
                    self._current_exclude_file.unlink()
                except OSError:
                    pass
            
            if in_progress_path.exists():
                try:
                    shutil.rmtree(in_progress_path)
                except OSError:
                    pass
            
            # Clear signal handler state on error
            if signal_handler is not None:
                signal_handler.set_in_progress_path(None)
                signal_handler.set_rsync_process(None)
            
            return SnapshotResult(
                success=False,
                snapshot_path=None,
                files_transferred=0,
                total_size=0,
                duration_seconds=time.time() - start_time,
                error_message=str(e),
            )
    
    def _parse_rsync_output(self, output: str) -> tuple[int, int, int]:
        """
        Parse rsync verbose output to extract statistics.
        
        Args:
            output: rsync stdout output
        
        Returns:
            Tuple of (files_changed, total_files, total_size_bytes)
        """
        files_changed = 0
        total_files = 0
        total_size = 0
        
        # rsync outputs statistics at the end like:
        # "Number of files: 2,895 (reg: 2,500, dir: 395)"
        # "Number of created files: 5"
        # "Number of regular files transferred: 3"
        # 
        # Or with older rsync:
        # Lines that are file paths indicate transferred files
        
        lines = output.strip().split('\n')
        
        for line in lines:
            line = line.strip()
            
            # Parse "Number of files: X" for total count
            if line.startswith('Number of files:'):
                try:
                    # Format: "Number of files: 2,895" or "Number of files: 2,895 (reg: 2,500, dir: 395)"
                    parts = line.split(':')[1].strip()
                    num_str = parts.split('(')[0].strip().replace(',', '')
                    total_files = int(num_str)
                except (ValueError, IndexError):
                    pass
            
            # Parse "Number of regular files transferred: X" for changed files
            elif 'files transferred:' in line.lower():
                try:
                    num_str = line.split(':')[1].strip().replace(',', '')
                    files_changed = int(num_str)
                except (ValueError, IndexError):
                    pass
            
            # Parse "Number of created files: X" - also counts as changed
            elif 'created files:' in line.lower():
                try:
                    num_str = line.split(':')[1].strip().split('(')[0].strip().replace(',', '')
                    files_changed += int(num_str)
                except (ValueError, IndexError):
                    pass
            
            # Parse total size from "sent X bytes" line
            elif line.startswith('sent '):
                try:
                    parts = line.split()
                    if len(parts) >= 2:
                        total_size = int(parts[1].replace(',', ''))
                except (ValueError, IndexError):
                    pass
        
        # Fallback: if we didn't get stats, count file lines (old method)
        if total_files == 0:
            for line in lines:
                line = line.strip()
                if line and not line.startswith(('sending', 'sent', 'total', 'building', 
                                                  'Number', 'receiving', 'created', 'deleting')):
                    total_files += 1
            # In fallback mode, assume all listed files were transferred
            files_changed = total_files
        
        return files_changed, total_files, total_size

    def get_current_progress(self) -> Optional[ProgressInfo]:
        """
        Get the current backup progress.
        
        Returns the current progress information if a backup is in progress
        with progress reporting enabled, otherwise returns None.
        
        Returns:
            ProgressInfo if progress is available, None otherwise
        
        Requirements: 6.3
        """
        if self._current_progress_reporter is not None:
            return self._current_progress_reporter.get_current_progress()
        return None

    def list_snapshots(self) -> List[SnapshotInfo]:
        """
        List all complete snapshots with metadata.
        
        Returns snapshots sorted by name (which handles both base format
        YYYY-MM-DD-HHMMSS and sequence format YYYY-MM-DD-HHMMSS-NN), 
        most recent first.
        
        Returns:
            List of SnapshotInfo objects
        """
        if not self.destination.exists():
            return []
        
        snapshots = []
        for entry in self.destination.iterdir():
            if not entry.is_dir():
                continue
            # Skip in-progress directories
            if entry.name.startswith(self.IN_PROGRESS_PREFIX):
                continue
            # Skip metadata/hidden directories
            if entry.name.startswith("."):
                continue
            
            # Try to parse as timestamp (handles both formats)
            timestamp = self._parse_snapshot_name(entry.name)
            if timestamp is None:
                # Not a valid snapshot directory name
                continue
            
            # Calculate size and file count
            size_bytes, file_count = self._get_directory_stats(entry)
            
            snapshots.append(SnapshotInfo(
                path=entry,
                timestamp=timestamp,
                size_bytes=size_bytes,
                file_count=file_count,
            ))
        
        # Sort by path name descending (lexicographic sort works for our format)
        # This correctly orders both base and sequence formats
        snapshots.sort(key=lambda x: x.path.name, reverse=True)
        return snapshots
    
    def _get_directory_stats(self, path: Path) -> tuple[int, int]:
        """
        Calculate total size and file count for a directory.
        
        Does NOT follow symbolic links to prevent infinite loops from
        circular symlinks (Requirements 3.3, 3.5).
        
        Args:
            path: Directory path
        
        Returns:
            Tuple of (total_size_bytes, file_count)
        """
        total_size = 0
        file_count = 0
        visited_dirs: Set[int] = set()  # Track visited directory inodes for circular detection
        
        try:
            # followlinks=False prevents following symlinks (Requirement 3.5)
            for root, dirs, files in os.walk(path, followlinks=False):
                root_path = Path(root)
                
                # Check for circular symlink by tracking directory inodes
                try:
                    dir_inode = root_path.stat().st_ino
                    if dir_inode in visited_dirs:
                        # Circular symlink detected (Requirement 3.4)
                        logger.warning(f"Circular symlink detected, skipping: {root}")
                        dirs.clear()  # Don't descend into subdirectories
                        continue
                    visited_dirs.add(dir_inode)
                except OSError:
                    pass
                
                for f in files:
                    file_path = root_path / f
                    try:
                        stat_info = file_path.lstat()
                        total_size += stat_info.st_size
                        file_count += 1
                    except OSError:
                        # Skip files we can't stat
                        continue
        except OSError:
            pass
        
        return total_size, file_count
    
    def get_snapshot_by_timestamp(self, timestamp: str) -> Optional[Path]:
        """
        Find snapshot matching timestamp string.
        
        Args:
            timestamp: Timestamp string in YYYY-MM-DD-HHMMSS or YYYY-MM-DD-HHMMSS-NN format
        
        Returns:
            Path to the snapshot directory, or None if not found
        """
        snapshot_path = self.destination / timestamp
        
        # Verify it exists and is a valid snapshot (not in-progress)
        if snapshot_path.exists() and snapshot_path.is_dir():
            if not timestamp.startswith(self.IN_PROGRESS_PREFIX):
                # Verify it's a valid timestamp format (handles both formats)
                if self._parse_snapshot_name(timestamp) is not None:
                    return snapshot_path
        
        return None

    def cleanup_incomplete(self) -> int:
        """
        Remove any in_progress_* directories from previous interrupted runs.
        
        Returns:
            Count of directories removed
        """
        if not self.destination.exists():
            return 0
        
        removed_count = 0
        for entry in self.destination.iterdir():
            if entry.is_dir() and entry.name.startswith(self.IN_PROGRESS_PREFIX):
                try:
                    shutil.rmtree(entry)
                    removed_count += 1
                except OSError:
                    # Log but continue if we can't remove a directory
                    pass
        
        return removed_count
    
    def restore(
        self,
        snapshot: Path,
        source_path: str,
        destination: Optional[Path] = None,
        source_directories: Optional[List[Path]] = None
    ) -> bool:
        """
        Restore file or directory from snapshot.
        
        Args:
            snapshot: Path to snapshot directory
            source_path: Relative path within snapshot to restore
            destination: Where to restore (default: original location)
            source_directories: Original source directories (needed for restoring
                               to original location when destination is None)
        
        Returns:
            True if restore succeeded
        """
        # Validate snapshot exists
        if not snapshot.exists() or not snapshot.is_dir():
            return False
        
        # Find the source file/directory within the snapshot
        source = snapshot / source_path
        if not source.exists():
            return False
        
        # Determine destination path
        if destination is None:
            # Restore to original location
            # We need source_directories to determine the original path
            if source_directories is None or len(source_directories) == 0:
                return False
            
            # The snapshot structure mirrors the source directories
            # Try to find which source directory this path belongs to
            # For simplicity, use the first source directory as the base
            destination = source_directories[0] / source_path
        
        try:
            if source.is_dir():
                # Create parent directories if needed
                destination.parent.mkdir(parents=True, exist_ok=True)
                # Copy directory tree, overwriting existing files
                if destination.exists():
                    shutil.rmtree(destination)
                shutil.copytree(source, destination)
            else:
                # Create parent directories if needed
                destination.parent.mkdir(parents=True, exist_ok=True)
                # Copy file, preserving metadata
                shutil.copy2(source, destination)
            return True
        except (OSError, shutil.Error):
            return False
    
    def diff(
        self,
        snapshot: Path,
        source_directories: List[Path],
        source_path: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Compare snapshot with current state of source directories.
        
        Does NOT follow symbolic links to prevent infinite loops from
        circular symlinks (Requirements 3.1, 3.5).
        
        Args:
            snapshot: Path to snapshot directory
            source_directories: List of current source directories to compare against
            source_path: Specific path to compare (optional, relative to snapshot)
        
        Returns:
            Dict with added, modified, deleted files (relative paths)
        """
        result = {
            "added": [],
            "modified": [],
            "deleted": [],
        }
        
        if not snapshot.exists() or not snapshot.is_dir():
            return result
        
        # Track visited directory inodes for circular symlink detection
        visited_dirs: Set[int] = set()
        
        # Build set of files in snapshot
        snapshot_files: Dict[str, Path] = {}
        
        if source_path:
            # Compare specific path only
            snapshot_base = snapshot / source_path
            if snapshot_base.exists():
                if snapshot_base.is_file():
                    snapshot_files[source_path] = snapshot_base
                else:
                    # followlinks=False prevents following symlinks (Requirement 3.1)
                    for root, dirs, files in os.walk(snapshot_base, followlinks=False):
                        root_path = Path(root)
                        
                        # Check for circular symlink (Requirement 3.4)
                        try:
                            dir_inode = root_path.stat().st_ino
                            if dir_inode in visited_dirs:
                                logger.warning(f"Circular symlink detected in snapshot diff, skipping: {root}")
                                dirs.clear()
                                continue
                            visited_dirs.add(dir_inode)
                        except OSError:
                            pass
                        
                        for f in files:
                            file_path = root_path / f
                            rel_path = str(file_path.relative_to(snapshot))
                            snapshot_files[rel_path] = file_path
        else:
            # Compare entire snapshot
            # followlinks=False prevents following symlinks (Requirement 3.1)
            for root, dirs, files in os.walk(snapshot, followlinks=False):
                root_path = Path(root)
                
                # Check for circular symlink (Requirement 3.4)
                try:
                    dir_inode = root_path.stat().st_ino
                    if dir_inode in visited_dirs:
                        logger.warning(f"Circular symlink detected in snapshot diff, skipping: {root}")
                        dirs.clear()
                        continue
                    visited_dirs.add(dir_inode)
                except OSError:
                    pass
                
                for f in files:
                    file_path = root_path / f
                    rel_path = str(file_path.relative_to(snapshot))
                    snapshot_files[rel_path] = file_path
        
        # Reset visited dirs for source directories
        visited_dirs.clear()
        
        # Build set of files in current source directories
        current_files: Dict[str, Path] = {}
        for source_dir in source_directories:
            if not source_dir.exists():
                continue
            
            if source_path:
                # Compare specific path only
                source_base = source_dir / source_path
                if source_base.exists():
                    if source_base.is_file():
                        current_files[source_path] = source_base
                    else:
                        # followlinks=False prevents following symlinks (Requirement 3.1)
                        for root, dirs, files in os.walk(source_base, followlinks=False):
                            root_path = Path(root)
                            
                            # Check for circular symlink (Requirement 3.4)
                            try:
                                dir_inode = root_path.stat().st_ino
                                if dir_inode in visited_dirs:
                                    logger.warning(f"Circular symlink detected in source diff, skipping: {root}")
                                    dirs.clear()
                                    continue
                                visited_dirs.add(dir_inode)
                            except OSError:
                                pass
                            
                            for f in files:
                                file_path = root_path / f
                                rel_path = str(file_path.relative_to(source_dir))
                                current_files[rel_path] = file_path
            else:
                # followlinks=False prevents following symlinks (Requirement 3.1)
                for root, dirs, files in os.walk(source_dir, followlinks=False):
                    root_path = Path(root)
                    
                    # Check for circular symlink (Requirement 3.4)
                    try:
                        dir_inode = root_path.stat().st_ino
                        if dir_inode in visited_dirs:
                            logger.warning(f"Circular symlink detected in source diff, skipping: {root}")
                            dirs.clear()
                            continue
                        visited_dirs.add(dir_inode)
                    except OSError:
                        pass
                    
                    for f in files:
                        file_path = root_path / f
                        rel_path = str(file_path.relative_to(source_dir))
                        current_files[rel_path] = file_path
        
        # Find added files (in current but not in snapshot)
        for rel_path in current_files:
            if rel_path not in snapshot_files:
                result["added"].append(rel_path)
        
        # Find deleted files (in snapshot but not in current)
        for rel_path in snapshot_files:
            if rel_path not in current_files:
                result["deleted"].append(rel_path)
        
        # Find modified files (in both but different content)
        for rel_path in snapshot_files:
            if rel_path in current_files:
                snapshot_file = snapshot_files[rel_path]
                current_file = current_files[rel_path]
                
                try:
                    # Compare by size and modification time first (fast check)
                    snap_stat = snapshot_file.stat()
                    curr_stat = current_file.stat()
                    
                    if snap_stat.st_size != curr_stat.st_size:
                        result["modified"].append(rel_path)
                    elif snap_stat.st_mtime != curr_stat.st_mtime:
                        # Size same but mtime different - compare content
                        if self._files_differ(snapshot_file, current_file):
                            result["modified"].append(rel_path)
                except OSError:
                    # If we can't stat, consider it modified
                    result["modified"].append(rel_path)
        
        # Sort results for consistent output
        result["added"].sort()
        result["modified"].sort()
        result["deleted"].sort()
        
        return result
    
    def _files_differ(self, file1: Path, file2: Path, chunk_size: int = 8192) -> bool:
        """
        Compare two files by content.
        
        Args:
            file1: First file path
            file2: Second file path
            chunk_size: Size of chunks to read at a time
        
        Returns:
            True if files have different content
        """
        try:
            with open(file1, 'rb') as f1, open(file2, 'rb') as f2:
                while True:
                    chunk1 = f1.read(chunk_size)
                    chunk2 = f2.read(chunk_size)
                    if chunk1 != chunk2:
                        return True
                    if not chunk1:  # EOF reached
                        return False
        except OSError:
            return True
    
    def search(
        self,
        pattern: str,
        snapshot: Optional[Path] = None
    ) -> List[Dict[str, Any]]:
        """
        Search for files matching pattern across snapshots.
        
        Does NOT follow symbolic links to prevent infinite loops from
        circular symlinks (Requirements 3.2, 3.5).
        
        Args:
            pattern: Glob pattern or filename to search for
            snapshot: Specific snapshot to search (None = all snapshots)
        
        Returns:
            List of matches with snapshot, path, size, modified time
        """
        import fnmatch
        
        results = []
        
        # Determine which snapshots to search
        if snapshot is not None:
            if not snapshot.exists() or not snapshot.is_dir():
                return results
            snapshots_to_search = [snapshot]
        else:
            # Search all snapshots
            snapshots_to_search = [info.path for info in self.list_snapshots()]
        
        for snap_path in snapshots_to_search:
            # Track visited directory inodes for circular symlink detection
            visited_dirs: Set[int] = set()
            
            # Walk through all files in the snapshot
            # followlinks=False prevents following symlinks (Requirement 3.2)
            for root, dirs, files in os.walk(snap_path, followlinks=False):
                root_path = Path(root)
                
                # Check for circular symlink (Requirement 3.4)
                try:
                    dir_inode = root_path.stat().st_ino
                    if dir_inode in visited_dirs:
                        logger.warning(f"Circular symlink detected in search, skipping: {root}")
                        dirs.clear()  # Don't descend into subdirectories
                        continue
                    visited_dirs.add(dir_inode)
                except OSError:
                    pass
                
                for filename in files:
                    # Check if filename matches pattern
                    if fnmatch.fnmatch(filename, pattern):
                        file_path = root_path / filename
                        rel_path = str(file_path.relative_to(snap_path))
                        
                        try:
                            stat_info = file_path.stat()
                            results.append({
                                "snapshot": snap_path.name,
                                "path": rel_path,
                                "size": stat_info.st_size,
                                "modified": datetime.fromtimestamp(stat_info.st_mtime).isoformat(),
                            })
                        except OSError:
                            # Skip files we can't stat
                            continue
        
        return results
