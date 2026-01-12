"""Configuration management for devbackup.

This module provides dataclasses for configuration and functions for
parsing/formatting TOML configuration files.
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional
import tomllib


class ConfigurationError(Exception):
    """Raised when configuration file is missing or malformed."""
    pass


class ValidationError(Exception):
    """Raised when configuration values have invalid types."""
    pass


# Default exclude patterns for development projects
DEFAULT_EXCLUDES: List[str] = [
    "node_modules/",
    ".git/",
    "__pycache__/",
    "*.pyc",
    ".pytest_cache/",
    "build/",
    "dist/",
    ".next/",
    "target/",
    "*.log",
    ".DS_Store",
    "*.tmp",
    ".env.local",
    "coverage/",
    ".nyc_output/",
    "vendor/",
]


@dataclass
class SchedulerConfig:
    """Configuration for backup scheduling."""
    type: str = "launchd"  # "launchd" or "cron"
    interval_seconds: int = 3600  # 1 hour default


@dataclass
class RetentionConfig:
    """Configuration for snapshot retention policy."""
    hourly: int = 24
    daily: int = 7
    weekly: int = 4


@dataclass
class LoggingConfig:
    """Configuration for logging."""
    level: str = "INFO"  # "DEBUG", "INFO", "ERROR"
    log_file: Path = field(
        default_factory=lambda: Path.home() / ".local/log/devbackup.log"
    )
    error_log_file: Path = field(
        default_factory=lambda: Path.home() / ".local/log/devbackup.err"
    )
    log_max_size_mb: int = 10  # Maximum log file size in MB before rotation
    log_backup_count: int = 5  # Number of rotated log files to keep
    
    @property
    def log_max_bytes(self) -> int:
        """Return max size in bytes for use with RotatingFileHandler."""
        return self.log_max_size_mb * 1024 * 1024


@dataclass
class MCPConfig:
    """Configuration for MCP server."""
    enabled: bool = True
    port: int = 0  # 0 = stdio transport (default for Cursor)


@dataclass
class DiscoveryConfig:
    """Configuration for project discovery."""
    scan_depth: int = 5  # Maximum directory depth to scan for projects


@dataclass
class RetryConfig:
    """Configuration for retry behavior on transient failures.
    
    Requirements: 10.2
    """
    retry_count: int = 3  # Maximum number of retry attempts
    retry_delay_seconds: float = 5.0  # Base delay for exponential backoff
    rsync_timeout_seconds: int = 3600  # Timeout for rsync operations (1 hour default)


@dataclass
class NotificationConfig:
    """Configuration for notifications.
    
    Requirements: 11.5
    """
    notify_on_success: bool = False
    notify_on_failure: bool = True


@dataclass
class Configuration:
    """Main configuration for devbackup."""
    backup_destination: Path
    source_directories: List[Path]
    exclude_patterns: List[str] = field(
        default_factory=lambda: DEFAULT_EXCLUDES.copy()
    )
    scheduler: SchedulerConfig = field(default_factory=SchedulerConfig)
    retention: RetentionConfig = field(default_factory=RetentionConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    mcp: MCPConfig = field(default_factory=MCPConfig)
    discovery: DiscoveryConfig = field(default_factory=DiscoveryConfig)
    retry: RetryConfig = field(default_factory=RetryConfig)
    notifications: NotificationConfig = field(default_factory=NotificationConfig)


# Default config file path
DEFAULT_CONFIG_PATH = Path.home() / ".config/devbackup/config.toml"

# Required keys in configuration
REQUIRED_KEYS = ["backup_destination", "source_directories"]


def _validate_type(value: Any, expected_type: type, key: str) -> None:
    """Validate that a value has the expected type."""
    if not isinstance(value, expected_type):
        raise ValidationError(
            f"Key '{key}' has invalid type: expected {expected_type.__name__}, "
            f"got {type(value).__name__}"
        )


def _parse_scheduler_config(data: Dict[str, Any]) -> SchedulerConfig:
    """Parse scheduler configuration from dict."""
    scheduler_data = data.get("scheduler", {})
    
    sched_type = scheduler_data.get("type", "launchd")
    if sched_type is not None:
        _validate_type(sched_type, str, "scheduler.type")
    
    interval = scheduler_data.get("interval_seconds", 3600)
    if interval is not None:
        _validate_type(interval, int, "scheduler.interval_seconds")
    
    return SchedulerConfig(
        type=sched_type,
        interval_seconds=interval,
    )


def _parse_retention_config(data: Dict[str, Any]) -> RetentionConfig:
    """Parse retention configuration from dict."""
    retention_data = data.get("retention", {})
    
    hourly = retention_data.get("hourly", 24)
    _validate_type(hourly, int, "retention.hourly")
    
    daily = retention_data.get("daily", 7)
    _validate_type(daily, int, "retention.daily")
    
    weekly = retention_data.get("weekly", 4)
    _validate_type(weekly, int, "retention.weekly")
    
    return RetentionConfig(hourly=hourly, daily=daily, weekly=weekly)


def _parse_logging_config(data: Dict[str, Any]) -> LoggingConfig:
    """Parse logging configuration from dict."""
    logging_data = data.get("logging", {})
    
    level = logging_data.get("level", "INFO")
    _validate_type(level, str, "logging.level")
    
    log_file = logging_data.get(
        "log_file", 
        str(Path.home() / ".local/log/devbackup.log")
    )
    _validate_type(log_file, str, "logging.log_file")
    
    error_log_file = logging_data.get(
        "error_log_file",
        str(Path.home() / ".local/log/devbackup.err")
    )
    _validate_type(error_log_file, str, "logging.error_log_file")
    
    log_max_size_mb = logging_data.get("log_max_size_mb", 10)
    _validate_type(log_max_size_mb, int, "logging.log_max_size_mb")
    
    log_backup_count = logging_data.get("log_backup_count", 5)
    _validate_type(log_backup_count, int, "logging.log_backup_count")
    
    return LoggingConfig(
        level=level,
        log_file=Path(log_file),
        error_log_file=Path(error_log_file),
        log_max_size_mb=log_max_size_mb,
        log_backup_count=log_backup_count,
    )


def _parse_mcp_config(data: Dict[str, Any]) -> MCPConfig:
    """Parse MCP configuration from dict."""
    mcp_data = data.get("mcp", {})
    
    enabled = mcp_data.get("enabled", True)
    _validate_type(enabled, bool, "mcp.enabled")
    
    port = mcp_data.get("port", 0)
    _validate_type(port, int, "mcp.port")
    
    return MCPConfig(enabled=enabled, port=port)


def _parse_discovery_config(data: Dict[str, Any]) -> DiscoveryConfig:
    """Parse discovery configuration from dict."""
    discovery_data = data.get("discovery", {})
    
    scan_depth = discovery_data.get("scan_depth", 5)
    _validate_type(scan_depth, int, "discovery.scan_depth")
    
    return DiscoveryConfig(scan_depth=scan_depth)


def _parse_retry_config(data: Dict[str, Any]) -> RetryConfig:
    """Parse retry configuration from dict.
    
    Requirements: 10.2
    """
    retry_data = data.get("retry", {})
    
    retry_count = retry_data.get("retry_count", 3)
    _validate_type(retry_count, int, "retry.retry_count")
    
    retry_delay_seconds = retry_data.get("retry_delay_seconds", 5.0)
    # Allow both int and float for delay
    if not isinstance(retry_delay_seconds, (int, float)):
        raise ValidationError(
            f"Key 'retry.retry_delay_seconds' has invalid type: expected int or float, "
            f"got {type(retry_delay_seconds).__name__}"
        )
    
    rsync_timeout_seconds = retry_data.get("rsync_timeout_seconds", 3600)
    _validate_type(rsync_timeout_seconds, int, "retry.rsync_timeout_seconds")
    
    return RetryConfig(
        retry_count=retry_count,
        retry_delay_seconds=float(retry_delay_seconds),
        rsync_timeout_seconds=rsync_timeout_seconds,
    )


def _parse_notifications_config(data: Dict[str, Any]) -> NotificationConfig:
    """Parse notifications configuration from dict.
    
    Requirements: 11.5
    """
    notifications_data = data.get("notifications", {})
    
    notify_on_success = notifications_data.get("notify_on_success", False)
    _validate_type(notify_on_success, bool, "notifications.notify_on_success")
    
    notify_on_failure = notifications_data.get("notify_on_failure", True)
    _validate_type(notify_on_failure, bool, "notifications.notify_on_failure")
    
    return NotificationConfig(
        notify_on_success=notify_on_success,
        notify_on_failure=notify_on_failure,
    )


def parse_config_string(toml_content: str) -> Configuration:
    """
    Parse TOML string into Configuration object.
    
    Args:
        toml_content: TOML formatted string
    
    Returns:
        Configuration object
    
    Raises:
        ConfigurationError: If required key is missing
        ValidationError: If value has wrong type
    """
    try:
        data = tomllib.loads(toml_content)
    except tomllib.TOMLDecodeError as e:
        raise ConfigurationError(f"Invalid TOML format: {e}")
    
    # Get main section (may be nested under [main] or at root)
    main_data = data.get("main", data)
    
    # Check required keys
    for key in REQUIRED_KEYS:
        if key not in main_data:
            raise ConfigurationError(f"Missing required configuration key: '{key}'")
    
    # Parse backup_destination
    backup_dest = main_data["backup_destination"]
    _validate_type(backup_dest, str, "backup_destination")
    
    # Parse source_directories
    source_dirs = main_data["source_directories"]
    _validate_type(source_dirs, list, "source_directories")
    for i, src in enumerate(source_dirs):
        _validate_type(src, str, f"source_directories[{i}]")
    
    # Parse exclude_patterns (optional, has defaults)
    exclude_patterns = main_data.get("exclude_patterns", DEFAULT_EXCLUDES.copy())
    _validate_type(exclude_patterns, list, "exclude_patterns")
    for i, pattern in enumerate(exclude_patterns):
        _validate_type(pattern, str, f"exclude_patterns[{i}]")
    
    return Configuration(
        backup_destination=Path(backup_dest),
        source_directories=[Path(s) for s in source_dirs],
        exclude_patterns=exclude_patterns,
        scheduler=_parse_scheduler_config(data),
        retention=_parse_retention_config(data),
        logging=_parse_logging_config(data),
        mcp=_parse_mcp_config(data),
        discovery=_parse_discovery_config(data),
        retry=_parse_retry_config(data),
        notifications=_parse_notifications_config(data),
    )


def parse_config(config_path: Optional[Path] = None) -> Configuration:
    """
    Parse TOML configuration file into Configuration object.
    
    Args:
        config_path: Path to config file. Defaults to ~/.config/devbackup/config.toml
    
    Returns:
        Configuration object
    
    Raises:
        ConfigurationError: If file doesn't exist or required key missing
        ValidationError: If value has wrong type
    """
    if config_path is None:
        config_path = DEFAULT_CONFIG_PATH
    
    if not config_path.exists():
        raise ConfigurationError(
            f"Configuration file not found: {config_path}"
        )
    
    try:
        content = config_path.read_text()
    except PermissionError:
        raise ConfigurationError(
            f"Permission denied reading configuration file: {config_path}"
        )
    except OSError as e:
        raise ConfigurationError(
            f"Error reading configuration file {config_path}: {e}"
        )
    
    return parse_config_string(content)


def _escape_toml_string(s: str) -> str:
    """Escape a string for TOML basic string format."""
    # Must escape backslashes first, then quotes
    return s.replace("\\", "\\\\").replace('"', '\\"')


def format_config(config: Configuration) -> str:
    """
    Format Configuration object back to TOML string.
    
    Used for round-trip testing and config generation.
    
    Args:
        config: Configuration object to format
    
    Returns:
        TOML formatted string
    """
    lines = []
    
    # Main section
    lines.append("[main]")
    lines.append(f'backup_destination = "{_escape_toml_string(str(config.backup_destination))}"')
    
    # Source directories as array
    lines.append("source_directories = [")
    for src in config.source_directories:
        lines.append(f'    "{_escape_toml_string(str(src))}",')
    lines.append("]")
    
    # Exclude patterns
    if config.exclude_patterns:
        lines.append("exclude_patterns = [")
        for pattern in config.exclude_patterns:
            lines.append(f'    "{_escape_toml_string(pattern)}",')
        lines.append("]")
    else:
        lines.append("exclude_patterns = []")
    
    lines.append("")
    
    # Scheduler section
    lines.append("[scheduler]")
    lines.append(f'type = "{_escape_toml_string(config.scheduler.type)}"')
    lines.append(f"interval_seconds = {config.scheduler.interval_seconds}")
    lines.append("")
    
    # Retention section
    lines.append("[retention]")
    lines.append(f"hourly = {config.retention.hourly}")
    lines.append(f"daily = {config.retention.daily}")
    lines.append(f"weekly = {config.retention.weekly}")
    lines.append("")
    
    # Logging section
    lines.append("[logging]")
    lines.append(f'level = "{_escape_toml_string(config.logging.level)}"')
    lines.append(f'log_file = "{_escape_toml_string(str(config.logging.log_file))}"')
    lines.append(f'error_log_file = "{_escape_toml_string(str(config.logging.error_log_file))}"')
    lines.append(f"log_max_size_mb = {config.logging.log_max_size_mb}")
    lines.append(f"log_backup_count = {config.logging.log_backup_count}")
    lines.append("")
    
    # MCP section
    lines.append("[mcp]")
    lines.append(f"enabled = {'true' if config.mcp.enabled else 'false'}")
    lines.append(f"port = {config.mcp.port}")
    lines.append("")
    
    # Discovery section
    lines.append("[discovery]")
    lines.append(f"scan_depth = {config.discovery.scan_depth}")
    lines.append("")
    
    # Retry section
    lines.append("[retry]")
    lines.append(f"retry_count = {config.retry.retry_count}")
    lines.append(f"retry_delay_seconds = {config.retry.retry_delay_seconds}")
    lines.append(f"rsync_timeout_seconds = {config.retry.rsync_timeout_seconds}")
    lines.append("")
    
    # Notifications section
    lines.append("[notifications]")
    lines.append(f"notify_on_success = {'true' if config.notifications.notify_on_success else 'false'}")
    lines.append(f"notify_on_failure = {'true' if config.notifications.notify_on_failure else 'false'}")
    
    return "\n".join(lines)


def create_default_config() -> str:
    """
    Generate default configuration TOML for `devbackup init`.
    
    Returns:
        TOML formatted string with default configuration
    """
    template = '''# devbackup configuration file
# See documentation for all options

[main]
# Absolute path to backup destination
backup_destination = "/Volumes/BackupDrive/DevBackups"

# List of source directories to back up (your project directories)
source_directories = [
    "~/Projects",
    "~/Code"
]

# Patterns to exclude (rsync format) - sensible defaults for dev projects
exclude_patterns = [
'''
    
    for pattern in DEFAULT_EXCLUDES:
        template += f'    "{pattern}",\n'
    
    template += ''']

[scheduler]
# "launchd" (recommended for macOS) or "cron"
type = "launchd"

# Backup interval in seconds (3600 = 1 hour)
interval_seconds = 3600

[retention]
# Number of snapshots to keep
hourly = 24
daily = 7
weekly = 4

[logging]
# Log level: DEBUG, INFO, ERROR
level = "INFO"
log_file = "~/.local/log/devbackup.log"
error_log_file = "~/.local/log/devbackup.err"
# Log rotation settings
log_max_size_mb = 10
log_backup_count = 5

[mcp]
# Enable MCP server for Cursor integration
enabled = true

[discovery]
# Maximum directory depth to scan for projects (default 5)
# Increase if your projects are deeply nested
scan_depth = 5

[retry]
# Retry settings for transient failures
retry_count = 3
retry_delay_seconds = 5.0
# Timeout for rsync operations in seconds (default 1 hour)
rsync_timeout_seconds = 3600

[notifications]
# macOS notification settings
notify_on_success = false
notify_on_failure = true
'''
    
    return template
