"""Main backup orchestration for devbackup.

This module provides the main backup function that orchestrates all components:
- Load configuration
- Acquire lock
- Validate destination
- Clean up incomplete snapshots
- Create snapshot
- Apply retention
- Release lock

All error cases are handled with proper cleanup to ensure:
- Lock is always released (Property 7)
- Incomplete snapshots are cleaned up (Property 8)
- Signal handling for graceful shutdown (Property 1 - backup-robustness)

Backup queue persistence (Requirements: 12.1, 12.4):
- Queue backups when destination unavailable
- Process queue when destination returns
- Queue survives process restarts
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Callable, List, Optional, Tuple
import logging
import time

from devbackup.progress import ProgressInfo
from devbackup.config import (
    Configuration,
    ConfigurationError,
    ValidationError,
    parse_config,
)
from devbackup.lock import LockManager, LockError
from devbackup.destination import DestinationError, validate_destination
from devbackup.snapshot import SnapshotEngine, SnapshotResult
from devbackup.retention import RetentionManager, RetentionResult
from devbackup.retry import RetryConfig as RetryConfigClass
from devbackup.logger import (
    setup_logging,
    get_logger,
    log_backup_start,
    log_backup_completion,
    log_backup_error,
)
from devbackup.signal_handler import SignalHandler
from devbackup.space import SpaceError, validate_space
from devbackup.notify import Notifier
from devbackup.queue import BackupQueue, QueuedBackup, get_default_queue


# Exit codes as defined in design document
EXIT_SUCCESS = 0
EXIT_CONFIG_ERROR = 1
EXIT_LOCK_ERROR = 2
EXIT_DESTINATION_ERROR = 3
EXIT_SNAPSHOT_ERROR = 4
EXIT_RETENTION_ERROR = 5
EXIT_SPACE_ERROR = 6  # New exit code for space validation failure
EXIT_BATTERY_SKIP = 7  # Exit code when backup skipped due to low battery


class BackupError(Exception):
    """Base exception for backup errors."""
    
    def __init__(self, message: str, exit_code: int):
        super().__init__(message)
        self.exit_code = exit_code


@dataclass
class BackupResult:
    """Result of a backup operation."""
    success: bool
    exit_code: int
    snapshot_result: Optional[SnapshotResult] = None
    retention_result: Optional[RetentionResult] = None
    error_message: Optional[str] = None
    incomplete_cleaned: int = 0
    queued: bool = False  # True if backup was queued instead of executed
    skipped_battery: bool = False  # True if backup was skipped due to low battery


def queue_backup(
    config: Configuration,
    reason: str = "destination_unavailable",
    queue: Optional[BackupQueue] = None,
) -> QueuedBackup:
    """
    Queue a backup request for later execution.
    
    Used when the backup destination is unavailable. The backup will be
    executed when the destination becomes available.
    
    Args:
        config: Configuration with source directories and destination
        reason: Reason the backup is being queued
        queue: Optional BackupQueue instance (uses default if not provided)
    
    Returns:
        The queued backup item
    
    Requirements: 12.1
    """
    if queue is None:
        queue = get_default_queue()
    
    return queue.enqueue(
        source_directories=config.source_directories,
        backup_destination=config.backup_destination,
        reason=reason,
    )


def process_queue(
    config: Optional[Configuration] = None,
    queue: Optional[BackupQueue] = None,
    max_items: int = 10,
) -> List[Tuple[QueuedBackup, BackupResult]]:
    """
    Process queued backups when destination becomes available.
    
    Attempts to execute queued backups in FIFO order. Stops processing
    if a backup fails due to destination unavailability.
    
    Args:
        config: Optional configuration to use for all backups
        queue: Optional BackupQueue instance (uses default if not provided)
        max_items: Maximum number of queued items to process
    
    Returns:
        List of (QueuedBackup, BackupResult) tuples for processed items
    
    Requirements: 12.4
    """
    if queue is None:
        queue = get_default_queue()
    
    results: List[Tuple[QueuedBackup, BackupResult]] = []
    processed = 0
    
    while not queue.is_empty() and processed < max_items:
        item = queue.dequeue()
        if item is None:
            break
        
        # Create a config from the queued item if not provided
        if config is not None:
            backup_config = config
        else:
            # Create minimal config from queued item
            # Configuration dataclass has default values for all optional fields
            backup_config = Configuration(
                backup_destination=Path(item.backup_destination),
                source_directories=[Path(s) for s in item.source_directories],
            )
        
        # Try to run the backup
        result = run_backup(config=backup_config, queue_on_dest_error=False)
        results.append((item, result))
        processed += 1
        
        # If destination is unavailable, re-queue and stop processing
        if result.exit_code == EXIT_DESTINATION_ERROR:
            queue.increment_retry(item)
            break
    
    return results


def check_and_process_queue(
    config: Configuration,
    queue: Optional[BackupQueue] = None,
) -> List[Tuple[QueuedBackup, BackupResult]]:
    """
    Check if destination is available and process queue if so.
    
    This is a convenience function that first validates the destination,
    then processes the queue if the destination is available.
    
    Args:
        config: Configuration with backup destination
        queue: Optional BackupQueue instance (uses default if not provided)
    
    Returns:
        List of (QueuedBackup, BackupResult) tuples for processed items,
        or empty list if destination unavailable or queue empty
    
    Requirements: 12.1, 12.4
    """
    if queue is None:
        queue = get_default_queue()
    
    # Check if queue is empty
    if queue.is_empty():
        return []
    
    # Check if destination is available
    try:
        validate_destination(config.backup_destination)
    except DestinationError:
        # Destination still unavailable
        return []
    
    # Process the queue
    return process_queue(config=config, queue=queue)


def run_backup(
    config_path: Optional[Path] = None,
    config: Optional[Configuration] = None,
    queue_on_dest_error: bool = True,
    check_battery: bool = True,
    battery_threshold: int = 20,
    progress_callback: Optional[Callable[["ProgressInfo"], None]] = None,
) -> BackupResult:
    """
    Run a complete backup operation.
    
    This function orchestrates the entire backup process:
    1. Load configuration (if not provided)
    2. Check battery status (skip if below threshold and not charging)
    3. Set up logging
    4. Register signal handlers for graceful shutdown
    5. Acquire exclusive lock
    6. Validate backup destination (queue if unavailable and queue_on_dest_error=True)
    7. Clean up any incomplete snapshots from previous runs
    8. Validate source directories
    9. Validate disk space (Requirements: 2.1, 2.2, 2.5)
    10. Create new snapshot
    11. Apply retention policy
    12. Unregister signal handlers
    13. Release lock
    
    All error cases are handled with proper cleanup to ensure:
    - Lock is always released (Property 7: Lock Release Invariant)
    - Incomplete snapshots are cleaned up (Property 8: Incomplete Snapshot Cleanup)
    - Signal handling for graceful shutdown (Property 1: Signal Cleanup Invariant)
    - Space validation before in_progress creation (Property 2: Space Validation Correctness)
    - Backup queued when destination unavailable (Requirements: 12.1, 12.4)
    - Battery-aware scheduling (Requirement 8.3)
    
    Args:
        config_path: Path to configuration file. If None, uses default path.
        config: Pre-loaded Configuration object. If provided, config_path is ignored.
        queue_on_dest_error: If True, queue backup when destination unavailable.
                            If False, return error immediately. Default True.
        check_battery: If True, check battery level before backup. Default True.
        battery_threshold: Minimum battery level to allow backup (default 20%).
        progress_callback: Optional callback for progress updates during backup.
                          Called with ProgressInfo object containing percent_complete,
                          files_transferred, bytes_transferred, etc.
    
    Returns:
        BackupResult with success status, exit code, and operation details.
        If queued, success=False but queued=True.
        If skipped due to battery, success=False but skipped_battery=True.
    
    Requirements: 1.1, 1.2, 1.3, 2.1, 2.2, 2.5, 8.1, 8.2, 8.3, 8.4, 8.5, 8.6, 8.7, 11.2, 11.3, 12.1, 12.4
    """
    logger: Optional[logging.Logger] = None
    lock_manager: Optional[LockManager] = None
    signal_handler: Optional[SignalHandler] = None
    notifier: Optional[Notifier] = None
    start_time = time.time()
    
    try:
        # Step 1: Load configuration
        if config is None:
            try:
                config = parse_config(config_path)
            except ConfigurationError as e:
                return BackupResult(
                    success=False,
                    exit_code=EXIT_CONFIG_ERROR,
                    error_message=str(e),
                )
            except ValidationError as e:
                return BackupResult(
                    success=False,
                    exit_code=EXIT_CONFIG_ERROR,
                    error_message=str(e),
                )
        
        # Step 2: Check battery status before proceeding
        # Requirements: 8.3 - Skip backups when battery is below 20% and not charging
        if check_battery:
            from devbackup.battery import check_battery_for_backup as battery_check
            should_proceed, battery_message = battery_check(threshold=battery_threshold)
            if not should_proceed:
                return BackupResult(
                    success=False,
                    exit_code=EXIT_BATTERY_SKIP,
                    error_message=battery_message,
                    skipped_battery=True,
                )
        
        # Step 3: Set up logging
        try:
            logger = setup_logging(config.logging)
        except Exception as e:
            # If logging setup fails, continue with basic logging
            logger = get_logger()
            logger.warning(f"Failed to set up logging: {e}")
        
        log_backup_start(logger, config.source_directories, config.backup_destination)
        
        # Initialize notifier with config
        # Requirements: 11.2, 11.3
        notifier = Notifier(config.notifications)
        
        # Step 4: Acquire exclusive lock
        lock_manager = LockManager()
        try:
            lock_manager.acquire()
            logger.debug("Lock acquired successfully")
        except LockError as e:
            log_backup_error(logger, e, "lock acquisition")
            # Send failure notification
            # Requirements: 11.3
            if notifier:
                notifier.notify_failure(str(e), time.time() - start_time)
            return BackupResult(
                success=False,
                exit_code=EXIT_LOCK_ERROR,
                error_message=str(e),
            )
        
        # Step 5: Register signal handlers for graceful shutdown
        # Requirements: 1.1, 1.2, 1.3
        signal_handler = SignalHandler()
        signal_handler.register(lock_manager=lock_manager)
        logger.debug("Signal handlers registered")
        
        # From this point on, we must ensure lock is released and signal handlers unregistered
        try:
            # Step 6: Validate destination
            try:
                validate_destination(config.backup_destination)
                logger.debug(f"Destination validated: {config.backup_destination}")
            except DestinationError as e:
                log_backup_error(logger, e, "destination validation")
                
                # Queue backup if destination unavailable and queueing enabled
                # Requirements: 12.1
                if queue_on_dest_error:
                    try:
                        queue_backup(config, reason="destination_unavailable")
                        logger.info(
                            f"Backup queued - destination unavailable: {config.backup_destination}"
                        )
                        return BackupResult(
                            success=False,
                            exit_code=EXIT_DESTINATION_ERROR,
                            error_message=str(e),
                            queued=True,
                        )
                    except Exception as queue_error:
                        logger.warning(f"Failed to queue backup: {queue_error}")
                
                # Send failure notification
                # Requirements: 11.3
                if notifier:
                    notifier.notify_failure(str(e), time.time() - start_time)
                return BackupResult(
                    success=False,
                    exit_code=EXIT_DESTINATION_ERROR,
                    error_message=str(e),
                )
            
            # Step 7: Clean up incomplete snapshots from previous runs
            # Requirements: 8.7
            # Create retry config from configuration
            retry_config = RetryConfigClass(
                max_retries=config.retry.retry_count,
                base_delay_seconds=config.retry.retry_delay_seconds,
                rsync_timeout_seconds=config.retry.rsync_timeout_seconds,
            )
            snapshot_engine = SnapshotEngine(
                destination=config.backup_destination,
                exclude_patterns=config.exclude_patterns,
                retry_config=retry_config,
            )
            incomplete_cleaned = snapshot_engine.cleanup_incomplete()
            if incomplete_cleaned > 0:
                logger.info(f"Cleaned up {incomplete_cleaned} incomplete snapshot(s)")
            
            # Step 8: Validate source directories
            # Requirements: 8.2, 8.3
            valid_sources = []
            for source in config.source_directories:
                if source.exists():
                    valid_sources.append(source)
                else:
                    # Requirements: 8.2 - log warning and continue
                    logger.warning(f"Source directory does not exist: {source}")
            
            # Requirements: 8.3 - exit if all sources invalid
            if not valid_sources:
                error_msg = "All source directories are invalid"
                log_backup_error(logger, Exception(error_msg), "source validation")
                # Send failure notification
                # Requirements: 11.3
                if notifier:
                    notifier.notify_failure(error_msg, time.time() - start_time)
                return BackupResult(
                    success=False,
                    exit_code=EXIT_SNAPSHOT_ERROR,
                    error_message=error_msg,
                    incomplete_cleaned=incomplete_cleaned,
                )
            
            # Step 9: Validate disk space before creating in_progress
            # Requirements: 2.1, 2.2, 2.5
            try:
                space_result = validate_space(
                    destination=config.backup_destination,
                    sources=valid_sources,
                    exclude_patterns=config.exclude_patterns,
                )
                if space_result.warning:
                    logger.warning(space_result.warning)
                logger.debug(
                    f"Space validation passed: {space_result.available_bytes / (1024**3):.2f}GB available, "
                    f"{space_result.estimated_bytes / (1024**3):.2f}GB estimated"
                )
            except SpaceError as e:
                log_backup_error(logger, e, "space validation")
                # Send failure notification
                # Requirements: 11.3
                if notifier:
                    notifier.notify_failure(str(e), time.time() - start_time)
                return BackupResult(
                    success=False,
                    exit_code=EXIT_SPACE_ERROR,
                    error_message=str(e),
                    incomplete_cleaned=incomplete_cleaned,
                )
            
            # Step 10: Create snapshot
            # Requirements: 8.1, 8.4, 8.6
            # Update signal handler with in_progress path before snapshot creation
            # The in_progress path will be set by the snapshot engine
            logger.info("Creating snapshot...")
            snapshot_result = snapshot_engine.create_snapshot(
                valid_sources,
                signal_handler=signal_handler,
                progress_callback=progress_callback,
            )
            
            if not snapshot_result.success:
                # Requirements: 8.1, 8.6 - error logged, in_progress cleaned by engine
                log_backup_error(
                    logger,
                    Exception(snapshot_result.error_message or "Unknown error"),
                    "snapshot creation"
                )
                # Send failure notification
                # Requirements: 11.3
                if notifier:
                    notifier.notify_failure(
                        snapshot_result.error_message or "Unknown error",
                        time.time() - start_time,
                    )
                return BackupResult(
                    success=False,
                    exit_code=EXIT_SNAPSHOT_ERROR,
                    snapshot_result=snapshot_result,
                    error_message=snapshot_result.error_message,
                    incomplete_cleaned=incomplete_cleaned,
                )
            
            logger.info(f"Snapshot created: {snapshot_result.snapshot_path}")
            
            # Step 11: Apply retention policy
            retention_manager = RetentionManager(
                destination=config.backup_destination,
                hourly=config.retention.hourly,
                daily=config.retention.daily,
                weekly=config.retention.weekly,
            )
            
            try:
                retention_result = retention_manager.apply_retention()
                if retention_result.deleted_snapshots:
                    logger.info(
                        f"Retention policy applied: deleted {len(retention_result.deleted_snapshots)} "
                        f"snapshot(s), freed {retention_result.freed_bytes} bytes"
                    )
                else:
                    logger.debug("Retention policy applied: no snapshots deleted")
            except Exception as e:
                # Retention failure is not critical - snapshot was created successfully
                # Log error but don't fail the backup
                log_backup_error(logger, e, "retention policy")
                retention_result = None
            
            # Log successful completion
            log_backup_completion(
                logger,
                duration_seconds=snapshot_result.duration_seconds,
                files_transferred=snapshot_result.files_transferred,
                total_size=snapshot_result.total_size,
                snapshot_path=snapshot_result.snapshot_path,
            )
            
            # Send success notification
            # Requirements: 11.2
            if notifier:
                notifier.notify_success(
                    snapshot_name=snapshot_result.snapshot_path.name if snapshot_result.snapshot_path else "unknown",
                    duration_seconds=snapshot_result.duration_seconds,
                    files_transferred=snapshot_result.files_transferred,
                )
            
            return BackupResult(
                success=True,
                exit_code=EXIT_SUCCESS,
                snapshot_result=snapshot_result,
                retention_result=retention_result,
                incomplete_cleaned=incomplete_cleaned,
            )
            
        finally:
            # Step 12: Unregister signal handlers
            if signal_handler is not None:
                try:
                    signal_handler.unregister()
                    if logger:
                        logger.debug("Signal handlers unregistered")
                except Exception as e:
                    if logger:
                        logger.warning(f"Error unregistering signal handlers: {e}")
            
            # Step 13: Release lock (Property 7: Lock Release Invariant)
            # This ensures lock is ALWAYS released, even on exceptions
            if lock_manager is not None:
                try:
                    lock_manager.release()
                    if logger:
                        logger.debug("Lock released")
                except Exception as e:
                    if logger:
                        logger.warning(f"Error releasing lock: {e}")
    
    except Exception as e:
        # Catch-all for unexpected errors
        if logger:
            log_backup_error(logger, e, "unexpected error")
        # Send failure notification
        # Requirements: 11.3
        if notifier:
            notifier.notify_failure(str(e), time.time() - start_time)
        return BackupResult(
            success=False,
            exit_code=EXIT_SNAPSHOT_ERROR,
            error_message=f"Unexpected error: {e}",
        )
