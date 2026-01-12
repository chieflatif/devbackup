"""devbackup - Incremental backup solution for macOS development projects."""

__version__ = "0.1.0"

from devbackup.config import (
    Configuration,
    ConfigurationError,
    ValidationError,
    parse_config,
    format_config,
    create_default_config,
)
from devbackup.lock import LockManager, LockError
from devbackup.destination import (
    DestinationError,
    validate_destination,
    is_volume_mounted,
    is_writable,
    get_available_space,
)
from devbackup.snapshot import (
    SnapshotEngine,
    SnapshotError,
    SnapshotResult,
    SnapshotInfo,
)
from devbackup.scheduler import (
    Scheduler,
    SchedulerError,
    SchedulerType,
    parse_launchd_plist,
    parse_cron_interval_from_entry,
)
from devbackup.logger import (
    LoggingError,
    setup_logging,
    get_logger,
    log_backup_start,
    log_backup_completion,
    log_backup_error,
    log_rsync_output,
)
from devbackup.backup import (
    BackupError,
    BackupResult,
    run_backup,
    EXIT_SUCCESS,
    EXIT_CONFIG_ERROR,
    EXIT_LOCK_ERROR,
    EXIT_DESTINATION_ERROR,
    EXIT_SNAPSHOT_ERROR,
    EXIT_RETENTION_ERROR,
)
from devbackup.retention import (
    RetentionManager,
    RetentionResult,
)

__all__ = [
    "Configuration",
    "ConfigurationError",
    "ValidationError",
    "parse_config",
    "format_config",
    "create_default_config",
    "LockManager",
    "LockError",
    "DestinationError",
    "validate_destination",
    "is_volume_mounted",
    "is_writable",
    "get_available_space",
    "SnapshotEngine",
    "SnapshotError",
    "SnapshotResult",
    "SnapshotInfo",
    "Scheduler",
    "SchedulerError",
    "SchedulerType",
    "parse_launchd_plist",
    "parse_cron_interval_from_entry",
    "LoggingError",
    "setup_logging",
    "get_logger",
    "log_backup_start",
    "log_backup_completion",
    "log_backup_error",
    "log_rsync_output",
    "BackupError",
    "BackupResult",
    "run_backup",
    "EXIT_SUCCESS",
    "EXIT_CONFIG_ERROR",
    "EXIT_LOCK_ERROR",
    "EXIT_DESTINATION_ERROR",
    "EXIT_SNAPSHOT_ERROR",
    "EXIT_RETENTION_ERROR",
    "RetentionManager",
    "RetentionResult",
]
