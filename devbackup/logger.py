"""Logging configuration for devbackup.

This module provides logging setup and utility functions for the backup system.
Supports DEBUG, INFO, and ERROR log levels with separate log and error files.
Includes automatic log rotation with gzip compression.
Provides structured logging with error codes for diagnostic purposes.

Requirements: 9.5 - Log issues in a way that the Agent can read and explain
"""

import gzip
import json
import logging
import os
import shutil
from dataclasses import dataclass, asdict
from datetime import datetime
from enum import Enum
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Dict, Optional

from devbackup.config import LoggingConfig


# Logger name for the devbackup package
LOGGER_NAME = "devbackup"

# Default rotation settings
DEFAULT_MAX_BYTES = 10 * 1024 * 1024  # 10MB
DEFAULT_BACKUP_COUNT = 5

# Valid log levels
VALID_LOG_LEVELS = {"DEBUG", "INFO", "ERROR"}


class ErrorCode(Enum):
    """
    Error codes for structured logging and troubleshooting.
    
    Each error code maps to a specific error category and has associated
    troubleshooting guidance that the Agent can use to help users.
    
    Requirements: 9.5
    """
    # Discovery errors (1xxx)
    DISCOVERY_NO_PROJECTS = "E1001"
    DISCOVERY_SCAN_FAILED = "E1002"
    DISCOVERY_NO_DESTINATIONS = "E1003"
    DISCOVERY_PERMISSION_DENIED = "E1004"
    
    # Destination errors (2xxx)
    DESTINATION_NOT_FOUND = "E2001"
    DESTINATION_UNAVAILABLE = "E2002"
    DESTINATION_READ_ONLY = "E2003"
    DESTINATION_NETWORK_ERROR = "E2004"
    
    # Space errors (3xxx)
    SPACE_INSUFFICIENT = "E3001"
    SPACE_QUOTA_EXCEEDED = "E3002"
    SPACE_CALCULATION_FAILED = "E3003"
    
    # Configuration errors (4xxx)
    CONFIG_NOT_FOUND = "E4001"
    CONFIG_INVALID = "E4002"
    CONFIG_PARSE_ERROR = "E4003"
    CONFIG_MISSING_REQUIRED = "E4004"
    
    # Lock errors (5xxx)
    LOCK_HELD = "E5001"
    LOCK_STALE = "E5002"
    LOCK_ACQUIRE_FAILED = "E5003"
    
    # Restore errors (6xxx)
    RESTORE_FILE_NOT_FOUND = "E6001"
    RESTORE_SNAPSHOT_NOT_FOUND = "E6002"
    RESTORE_PERMISSION_DENIED = "E6003"
    RESTORE_FAILED = "E6004"
    
    # IPC errors (7xxx)
    IPC_CONNECTION_FAILED = "E7001"
    IPC_TIMEOUT = "E7002"
    IPC_INVALID_MESSAGE = "E7003"
    
    # Backup errors (8xxx)
    BACKUP_RSYNC_FAILED = "E8001"
    BACKUP_INTERRUPTED = "E8002"
    BACKUP_VERIFICATION_FAILED = "E8003"
    BACKUP_SNAPSHOT_FAILED = "E8004"
    
    # Scheduler errors (9xxx)
    SCHEDULER_INSTALL_FAILED = "E9001"
    SCHEDULER_UNINSTALL_FAILED = "E9002"
    SCHEDULER_NOT_RUNNING = "E9003"
    
    # General errors (0xxx)
    UNKNOWN_ERROR = "E0001"
    INTERNAL_ERROR = "E0002"


# Troubleshooting guidance for each error code
ERROR_GUIDANCE: Dict[ErrorCode, str] = {
    ErrorCode.DISCOVERY_NO_PROJECTS: "No projects were found. Make sure you're in a folder with project files like package.json, pyproject.toml, or .git.",
    ErrorCode.DISCOVERY_SCAN_FAILED: "Failed to scan for projects. Check folder permissions.",
    ErrorCode.DISCOVERY_NO_DESTINATIONS: "No backup destinations found. Connect an external drive or specify a backup folder.",
    ErrorCode.DISCOVERY_PERMISSION_DENIED: "Permission denied while scanning. Grant access to the folder in System Preferences > Privacy & Security.",
    
    ErrorCode.DESTINATION_NOT_FOUND: "The backup destination doesn't exist. Make sure the drive is connected or the folder exists.",
    ErrorCode.DESTINATION_UNAVAILABLE: "The backup destination is unavailable. Check if the drive is connected and mounted.",
    ErrorCode.DESTINATION_READ_ONLY: "The backup destination is read-only. Check drive permissions or try a different location.",
    ErrorCode.DESTINATION_NETWORK_ERROR: "Network error accessing backup destination. Check your network connection.",
    
    ErrorCode.SPACE_INSUFFICIENT: "Not enough space on the backup drive. Delete old backups or use a larger drive.",
    ErrorCode.SPACE_QUOTA_EXCEEDED: "Storage quota exceeded. Free up space or increase your quota.",
    ErrorCode.SPACE_CALCULATION_FAILED: "Could not calculate available space. The drive may be disconnected.",
    
    ErrorCode.CONFIG_NOT_FOUND: "No configuration file found. Run backup setup to create one.",
    ErrorCode.CONFIG_INVALID: "The configuration file is invalid. Try running backup setup again.",
    ErrorCode.CONFIG_PARSE_ERROR: "Could not parse the configuration file. Check for syntax errors.",
    ErrorCode.CONFIG_MISSING_REQUIRED: "Required configuration values are missing. Run backup setup to fix.",
    
    ErrorCode.LOCK_HELD: "Another backup is already running. Wait for it to finish.",
    ErrorCode.LOCK_STALE: "Found a stale lock file. It will be cleaned up automatically.",
    ErrorCode.LOCK_ACQUIRE_FAILED: "Could not acquire backup lock. Check file permissions.",
    
    ErrorCode.RESTORE_FILE_NOT_FOUND: "The file wasn't found in any backup. It may have been created after the last backup.",
    ErrorCode.RESTORE_SNAPSHOT_NOT_FOUND: "The requested backup snapshot doesn't exist.",
    ErrorCode.RESTORE_PERMISSION_DENIED: "Permission denied during restore. Check folder permissions.",
    ErrorCode.RESTORE_FAILED: "Restore operation failed. Try restoring to a different location.",
    
    ErrorCode.IPC_CONNECTION_FAILED: "Could not connect to the backup service. The menu bar app may need to be restarted.",
    ErrorCode.IPC_TIMEOUT: "Communication with backup service timed out. Try again.",
    ErrorCode.IPC_INVALID_MESSAGE: "Received invalid message from backup service. Try restarting the menu bar app.",
    
    ErrorCode.BACKUP_RSYNC_FAILED: "File synchronization failed. Check source and destination accessibility.",
    ErrorCode.BACKUP_INTERRUPTED: "Backup was interrupted. It will resume on the next scheduled run.",
    ErrorCode.BACKUP_VERIFICATION_FAILED: "Backup verification failed. Some files may not have been backed up correctly.",
    ErrorCode.BACKUP_SNAPSHOT_FAILED: "Could not create backup snapshot. Check destination permissions.",
    
    ErrorCode.SCHEDULER_INSTALL_FAILED: "Could not install automatic backup schedule. Check system permissions.",
    ErrorCode.SCHEDULER_UNINSTALL_FAILED: "Could not remove automatic backup schedule.",
    ErrorCode.SCHEDULER_NOT_RUNNING: "Automatic backups are not running. Try reinstalling the schedule.",
    
    ErrorCode.UNKNOWN_ERROR: "An unexpected error occurred. Check the logs for more details.",
    ErrorCode.INTERNAL_ERROR: "An internal error occurred. Please report this issue.",
}


@dataclass
class StructuredLogEntry:
    """
    A structured log entry that can be parsed by the Agent.
    
    Contains all required fields for diagnostic purposes:
    - timestamp: ISO 8601 formatted timestamp
    - level: Severity level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    - error_code: Error code from ErrorCode enum (for errors/warnings)
    - message: Human-readable message
    - context: Additional context information
    - guidance: Troubleshooting guidance (for errors/warnings)
    
    Requirements: 9.5
    """
    timestamp: str
    level: str
    message: str
    error_code: Optional[str] = None
    context: Optional[Dict[str, Any]] = None
    guidance: Optional[str] = None
    
    def to_json(self) -> str:
        """Serialize to JSON string for logging."""
        data = {k: v for k, v in asdict(self).items() if v is not None}
        return json.dumps(data, default=str)
    
    @classmethod
    def from_json(cls, json_str: str) -> "StructuredLogEntry":
        """Parse from JSON string."""
        data = json.loads(json_str)
        return cls(**data)
    
    @classmethod
    def create(
        cls,
        level: str,
        message: str,
        error_code: Optional[ErrorCode] = None,
        context: Optional[Dict[str, Any]] = None,
    ) -> "StructuredLogEntry":
        """
        Create a structured log entry with automatic timestamp and guidance.
        
        Args:
            level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            message: Human-readable message
            error_code: Optional error code for errors/warnings
            context: Optional additional context
        
        Returns:
            StructuredLogEntry with all fields populated
        """
        timestamp = datetime.now().isoformat()
        code_str = error_code.value if error_code else None
        guidance = ERROR_GUIDANCE.get(error_code) if error_code else None
        
        return cls(
            timestamp=timestamp,
            level=level,
            message=message,
            error_code=code_str,
            context=context,
            guidance=guidance,
        )


def get_error_guidance(error_code: ErrorCode) -> str:
    """
    Get troubleshooting guidance for an error code.
    
    Args:
        error_code: The error code to get guidance for
    
    Returns:
        Human-readable troubleshooting guidance
    """
    return ERROR_GUIDANCE.get(error_code, ERROR_GUIDANCE[ErrorCode.UNKNOWN_ERROR])


class LoggingError(Exception):
    """Raised when logging setup fails."""
    pass


class GzipRotatingFileHandler(RotatingFileHandler):
    """
    Rotating file handler that compresses rotated files with gzip.
    
    Extends RotatingFileHandler to add gzip compression when log files
    are rotated. Rotated files are named with .gz extension.
    
    Requirements: 9.1, 9.3
    """
    
    def rotation_filename(self, default_name: str) -> str:
        """
        Return the filename for a rotated log file.
        
        Appends .gz extension to indicate gzip compression.
        
        Args:
            default_name: The default rotated filename (e.g., "app.log.1")
        
        Returns:
            Filename with .gz extension (e.g., "app.log.1.gz")
        """
        return default_name + ".gz"
    
    def rotate(self, source: str, dest: str) -> None:
        """
        Rotate and compress the log file.
        
        Compresses the source file using gzip and writes to dest.
        The source file is removed after successful compression.
        
        Args:
            source: Path to the current log file
            dest: Path for the rotated (compressed) file
        """
        if not os.path.exists(source):
            return
        
        try:
            with open(source, 'rb') as f_in:
                with gzip.open(dest, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            os.remove(source)
        except (OSError, IOError) as e:
            # If compression fails, fall back to simple rename
            if os.path.exists(source):
                # Remove .gz extension for fallback
                fallback_dest = dest[:-3] if dest.endswith('.gz') else dest
                try:
                    os.rename(source, fallback_dest)
                except OSError:
                    pass  # Best effort - don't fail logging


def _ensure_log_directory(log_path: Path) -> None:
    """Ensure the parent directory for a log file exists."""
    log_dir = log_path.parent
    if not log_dir.exists():
        try:
            log_dir.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            raise LoggingError(f"Failed to create log directory {log_dir}: {e}")


def _get_log_level(level_str: str) -> int:
    """Convert log level string to logging constant."""
    level_str = level_str.upper()
    if level_str not in VALID_LOG_LEVELS:
        raise LoggingError(
            f"Invalid log level '{level_str}'. Must be one of: {', '.join(VALID_LOG_LEVELS)}"
        )
    return getattr(logging, level_str)


def setup_logging(
    config: Optional[LoggingConfig] = None,
    log_file: Optional[Path] = None,
    error_log_file: Optional[Path] = None,
    level: Optional[str] = None,
    max_bytes: Optional[int] = None,
    backup_count: Optional[int] = None,
) -> logging.Logger:
    """
    Configure logging for devbackup.
    
    Sets up logging with:
    - A rotating file handler for general logs (log_file)
    - A rotating file handler for error logs only (error_log_file)
    - Console output for immediate feedback
    - Automatic gzip compression of rotated files
    
    Args:
        config: LoggingConfig object with settings. If provided, other args are ignored.
        log_file: Path to main log file (used if config is None)
        error_log_file: Path to error log file (used if config is None)
        level: Log level string: "DEBUG", "INFO", or "ERROR" (used if config is None)
        max_bytes: Maximum log file size before rotation (default 10MB)
        backup_count: Number of rotated files to keep (default 5)
    
    Returns:
        Configured logger instance
    
    Raises:
        LoggingError: If log directory cannot be created or level is invalid
    
    Requirements: 9.1, 9.2, 9.4
    """
    # Use config if provided, otherwise use individual parameters
    if config is not None:
        log_file = config.log_file
        error_log_file = config.error_log_file
        level = config.level
        # Use config values for rotation if available, otherwise defaults
        if max_bytes is None:
            max_bytes = getattr(config, 'log_max_bytes', DEFAULT_MAX_BYTES)
        if backup_count is None:
            backup_count = getattr(config, 'log_backup_count', DEFAULT_BACKUP_COUNT)
    else:
        # Apply defaults if not specified
        if log_file is None:
            log_file = Path.home() / ".local/log/devbackup.log"
        if error_log_file is None:
            error_log_file = Path.home() / ".local/log/devbackup.err"
        if level is None:
            level = "INFO"
        if max_bytes is None:
            max_bytes = DEFAULT_MAX_BYTES
        if backup_count is None:
            backup_count = DEFAULT_BACKUP_COUNT
    
    # Expand ~ in paths
    log_file = Path(os.path.expanduser(str(log_file)))
    error_log_file = Path(os.path.expanduser(str(error_log_file)))
    
    # Ensure log directories exist
    _ensure_log_directory(log_file)
    _ensure_log_directory(error_log_file)
    
    # Get numeric log level
    log_level = _get_log_level(level)
    
    # Get or create logger
    logger = logging.getLogger(LOGGER_NAME)
    
    # Clear any existing handlers to avoid duplicates
    logger.handlers.clear()
    
    # Set the logger level
    logger.setLevel(logging.DEBUG)  # Allow all levels, handlers will filter
    
    # Create formatters
    detailed_formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    
    # Rotating file handler for main log (respects configured level)
    file_handler = GzipRotatingFileHandler(
        log_file,
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding="utf-8",
    )
    file_handler.setLevel(log_level)
    file_handler.setFormatter(detailed_formatter)
    logger.addHandler(file_handler)
    
    # Rotating file handler for errors only
    error_handler = GzipRotatingFileHandler(
        error_log_file,
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding="utf-8",
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(detailed_formatter)
    logger.addHandler(error_handler)
    
    # Console handler for immediate feedback
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_handler.setFormatter(detailed_formatter)
    logger.addHandler(console_handler)
    
    return logger


def get_logger() -> logging.Logger:
    """
    Get the devbackup logger instance.
    
    Returns:
        The devbackup logger. If setup_logging hasn't been called,
        returns a logger with default configuration.
    """
    return logging.getLogger(LOGGER_NAME)


def log_backup_start(
    logger: logging.Logger,
    source_directories: list,
    destination: Path,
) -> None:
    """
    Log the start of a backup operation.
    
    Args:
        logger: Logger instance
        source_directories: List of source directory paths
        destination: Backup destination path
    
    Requirements: 7.6
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    sources_str = ", ".join(str(s) for s in source_directories)
    logger.info(f"Backup started at {timestamp}")
    logger.info(f"Source directories: {sources_str}")
    logger.info(f"Destination: {destination}")


def log_backup_completion(
    logger: logging.Logger,
    duration_seconds: float,
    files_transferred: int,
    total_size: int,
    snapshot_path: Optional[Path] = None,
) -> None:
    """
    Log the completion of a backup operation.
    
    Args:
        logger: Logger instance
        duration_seconds: How long the backup took
        files_transferred: Number of files transferred
        total_size: Total size in bytes
        snapshot_path: Path to the created snapshot
    
    Requirements: 7.7
    """
    # Format size for readability
    if total_size >= 1024 * 1024 * 1024:
        size_str = f"{total_size / (1024 * 1024 * 1024):.2f} GB"
    elif total_size >= 1024 * 1024:
        size_str = f"{total_size / (1024 * 1024):.2f} MB"
    elif total_size >= 1024:
        size_str = f"{total_size / 1024:.2f} KB"
    else:
        size_str = f"{total_size} bytes"
    
    logger.info(f"Backup completed successfully")
    logger.info(f"Duration: {duration_seconds:.2f} seconds")
    logger.info(f"Files transferred: {files_transferred}")
    logger.info(f"Total size: {size_str}")
    if snapshot_path:
        logger.info(f"Snapshot: {snapshot_path}")


def log_backup_error(
    logger: logging.Logger,
    error: Exception,
    context: Optional[str] = None,
) -> None:
    """
    Log a backup error.
    
    Args:
        logger: Logger instance
        error: The exception that occurred
        context: Additional context about what was happening
    """
    if context:
        logger.error(f"Backup failed during {context}: {error}")
    else:
        logger.error(f"Backup failed: {error}")


def log_rsync_output(logger: logging.Logger, output: str) -> None:
    """
    Log rsync output (only at DEBUG level).
    
    Args:
        logger: Logger instance
        output: rsync stdout/stderr output
    
    Requirements: 7.3
    """
    if output.strip():
        for line in output.strip().split("\n"):
            logger.debug(f"rsync: {line}")


# Structured logging functions for diagnostic purposes
# Requirements: 9.5

def log_structured(
    logger: logging.Logger,
    level: str,
    message: str,
    error_code: Optional[ErrorCode] = None,
    context: Optional[Dict[str, Any]] = None,
) -> StructuredLogEntry:
    """
    Log a structured entry that can be parsed by the Agent.
    
    Creates a StructuredLogEntry with all required fields and logs it
    as a JSON string that can be parsed for diagnostic purposes.
    
    Args:
        logger: Logger instance
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        message: Human-readable message
        error_code: Optional error code for errors/warnings
        context: Optional additional context dictionary
    
    Returns:
        The StructuredLogEntry that was logged
    
    Requirements: 9.5
    """
    entry = StructuredLogEntry.create(
        level=level,
        message=message,
        error_code=error_code,
        context=context,
    )
    
    # Log the JSON representation
    log_level = getattr(logging, level.upper(), logging.INFO)
    logger.log(log_level, entry.to_json())
    
    return entry


def log_structured_error(
    logger: logging.Logger,
    message: str,
    error_code: ErrorCode,
    context: Optional[Dict[str, Any]] = None,
) -> StructuredLogEntry:
    """
    Log a structured error entry with error code and guidance.
    
    Convenience function for logging errors with automatic guidance lookup.
    
    Args:
        logger: Logger instance
        message: Human-readable error message
        error_code: Error code from ErrorCode enum
        context: Optional additional context dictionary
    
    Returns:
        The StructuredLogEntry that was logged
    
    Requirements: 9.5
    """
    return log_structured(
        logger=logger,
        level="ERROR",
        message=message,
        error_code=error_code,
        context=context,
    )


def log_structured_warning(
    logger: logging.Logger,
    message: str,
    error_code: Optional[ErrorCode] = None,
    context: Optional[Dict[str, Any]] = None,
) -> StructuredLogEntry:
    """
    Log a structured warning entry.
    
    Args:
        logger: Logger instance
        message: Human-readable warning message
        error_code: Optional error code
        context: Optional additional context dictionary
    
    Returns:
        The StructuredLogEntry that was logged
    
    Requirements: 9.5
    """
    return log_structured(
        logger=logger,
        level="WARNING",
        message=message,
        error_code=error_code,
        context=context,
    )


def log_structured_info(
    logger: logging.Logger,
    message: str,
    context: Optional[Dict[str, Any]] = None,
) -> StructuredLogEntry:
    """
    Log a structured info entry.
    
    Args:
        logger: Logger instance
        message: Human-readable info message
        context: Optional additional context dictionary
    
    Returns:
        The StructuredLogEntry that was logged
    
    Requirements: 9.5
    """
    return log_structured(
        logger=logger,
        level="INFO",
        message=message,
        error_code=None,
        context=context,
    )


def parse_structured_log(log_line: str) -> Optional[StructuredLogEntry]:
    """
    Parse a structured log entry from a log line.
    
    Extracts the JSON portion from a log line and parses it into
    a StructuredLogEntry. Returns None if parsing fails.
    
    Args:
        log_line: A line from the log file
    
    Returns:
        StructuredLogEntry if parsing succeeds, None otherwise
    
    Requirements: 9.5
    """
    try:
        # Try to find JSON in the log line
        # Log format: "2025-01-07 10:30:00 - devbackup - ERROR - {json}"
        json_start = log_line.find('{')
        if json_start == -1:
            return None
        
        json_str = log_line[json_start:]
        return StructuredLogEntry.from_json(json_str)
    except (json.JSONDecodeError, KeyError, TypeError):
        return None


def get_recent_errors(
    log_file: Path,
    max_entries: int = 10,
) -> list:
    """
    Get recent structured error entries from a log file.
    
    Reads the log file and extracts recent structured error entries
    that can be used for diagnostic purposes.
    
    Args:
        log_file: Path to the log file
        max_entries: Maximum number of entries to return
    
    Returns:
        List of StructuredLogEntry objects for recent errors
    
    Requirements: 9.5
    """
    errors = []
    
    if not log_file.exists():
        return errors
    
    try:
        with open(log_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        # Process lines in reverse order (most recent first)
        for line in reversed(lines):
            if len(errors) >= max_entries:
                break
            
            entry = parse_structured_log(line)
            if entry and entry.level in ("ERROR", "CRITICAL"):
                errors.append(entry)
        
        # Return in chronological order
        return list(reversed(errors))
    except (OSError, IOError):
        return errors


def map_exception_to_error_code(exception: Exception) -> ErrorCode:
    """
    Map an exception to an appropriate error code.
    
    Analyzes the exception type and message to determine the most
    appropriate error code for structured logging.
    
    Args:
        exception: The exception to map
    
    Returns:
        The most appropriate ErrorCode
    
    Requirements: 9.5
    """
    exc_type = type(exception).__name__
    exc_msg = str(exception).lower()
    
    # Map by exception type name
    type_mappings = {
        "FileNotFoundError": ErrorCode.DESTINATION_NOT_FOUND,
        "PermissionError": ErrorCode.DISCOVERY_PERMISSION_DENIED,
        "OSError": ErrorCode.UNKNOWN_ERROR,
        "IOError": ErrorCode.UNKNOWN_ERROR,
        "ConfigurationError": ErrorCode.CONFIG_INVALID,
        "LockError": ErrorCode.LOCK_HELD,
        "DiscoveryError": ErrorCode.DISCOVERY_SCAN_FAILED,
        "DestinationError": ErrorCode.DESTINATION_UNAVAILABLE,
        "SpaceError": ErrorCode.SPACE_INSUFFICIENT,
        "RestoreError": ErrorCode.RESTORE_FAILED,
        "IPCError": ErrorCode.IPC_CONNECTION_FAILED,
    }
    
    if exc_type in type_mappings:
        return type_mappings[exc_type]
    
    # Map by message content
    if "no space" in exc_msg or "disk full" in exc_msg:
        return ErrorCode.SPACE_INSUFFICIENT
    if "permission" in exc_msg:
        return ErrorCode.DISCOVERY_PERMISSION_DENIED
    if "not found" in exc_msg:
        return ErrorCode.DESTINATION_NOT_FOUND
    if "lock" in exc_msg:
        return ErrorCode.LOCK_HELD
    if "config" in exc_msg:
        return ErrorCode.CONFIG_INVALID
    if "network" in exc_msg or "connection" in exc_msg:
        return ErrorCode.DESTINATION_NETWORK_ERROR
    if "timeout" in exc_msg:
        return ErrorCode.IPC_TIMEOUT
    
    return ErrorCode.UNKNOWN_ERROR
