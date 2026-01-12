"""Smart defaults engine for devbackup.

This module generates intelligent default configurations based on discovered
projects and destinations, eliminating the need for manual configuration.

Requirements: 1.5, 8.1
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Set

from devbackup.config import (
    Configuration,
    LoggingConfig,
    MCPConfig,
    NotificationConfig,
    RetentionConfig,
    RetryConfig,
    SchedulerConfig,
)
from devbackup.discovery import DiscoveredDestination, DiscoveredProject


class DefaultsError(Exception):
    """Raised when smart defaults generation fails."""
    pass


# Project-type specific exclude patterns
# Requirements: 4.3
PROJECT_EXCLUDE_PATTERNS: Dict[str, List[str]] = {
    "python": [
        "__pycache__/",
        "*.pyc",
        "*.pyo",
        "*.pyd",
        ".venv/",
        "venv/",
        ".env/",
        "env/",
        ".pytest_cache/",
        ".mypy_cache/",
        ".tox/",
        ".eggs/",
        "*.egg-info/",
        ".coverage",
        "htmlcov/",
        ".hypothesis/",
    ],
    "node": [
        "node_modules/",
        ".next/",
        ".nuxt/",
        "dist/",
        "build/",
        ".cache/",
        "coverage/",
        ".nyc_output/",
        ".parcel-cache/",
        ".turbo/",
    ],
    "rust": [
        "target/",
        "Cargo.lock",
    ],
    "go": [
        "vendor/",
        "bin/",
    ],
    "xcode": [
        "DerivedData/",
        "build/",
        "Pods/",
        ".build/",
        "*.xcuserstate",
    ],
    "generic": [],
}

# Universal exclude patterns that apply to all project types
UNIVERSAL_EXCLUDE_PATTERNS: List[str] = [
    ".git/",
    ".DS_Store",
    "*.log",
    "*.tmp",
    "*.swp",
    "*.swo",
    "*~",
    ".env.local",
    ".env.*.local",
    "tmp/",
    "temp/",
    "logs/",
    ".idea/",
    ".vscode/",
    "*.bak",
]


@dataclass
class SmartDefaultsConfig:
    """Configuration options for smart defaults generation."""
    
    # Scheduler defaults
    scheduler_type: str = "launchd"
    interval_seconds: int = 3600  # 1 hour (hourly backups)
    
    # Retention defaults (24 hourly, 7 daily, 4 weekly)
    retention_hourly: int = 24
    retention_daily: int = 7
    retention_weekly: int = 4
    
    # Logging defaults
    log_level: str = "INFO"
    log_max_size_mb: int = 10
    log_backup_count: int = 5
    
    # MCP defaults
    mcp_enabled: bool = True
    mcp_port: int = 0  # stdio transport
    
    # Retry defaults
    retry_count: int = 3
    retry_delay_seconds: float = 5.0
    
    # Notification defaults
    notify_on_success: bool = False
    notify_on_failure: bool = True


class SmartDefaults:
    """Generates smart default configurations based on environment.
    
    This class analyzes discovered projects and destinations to generate
    appropriate configuration without requiring manual setup.
    
    Requirements: 1.5, 8.1
    """
    
    def __init__(
        self,
        defaults_config: Optional[SmartDefaultsConfig] = None,
    ) -> None:
        """Initialize the smart defaults engine.
        
        Args:
            defaults_config: Optional configuration for default values.
                           Uses SmartDefaultsConfig() if not provided.
        """
        self.defaults_config = defaults_config or SmartDefaultsConfig()
    
    def get_exclude_patterns(
        self,
        project_types: Set[str],
    ) -> List[str]:
        """Get exclude patterns appropriate for the given project types.
        
        Combines universal exclude patterns with project-type-specific patterns
        to create a comprehensive exclusion list.
        
        Args:
            project_types: Set of project types (e.g., {"python", "node"})
            
        Returns:
            Sorted list of unique exclude patterns
            
        Requirements: 4.3
        """
        patterns: Set[str] = set()
        
        # Always include universal patterns
        patterns.update(UNIVERSAL_EXCLUDE_PATTERNS)
        
        # Add project-specific patterns
        for project_type in project_types:
            type_patterns = PROJECT_EXCLUDE_PATTERNS.get(project_type, [])
            patterns.update(type_patterns)
        
        return sorted(patterns)
    
    def _create_scheduler_config(self) -> SchedulerConfig:
        """Create scheduler configuration with smart defaults.
        
        Returns:
            SchedulerConfig with hourly backup schedule
            
        Requirements: 8.1
        """
        return SchedulerConfig(
            type=self.defaults_config.scheduler_type,
            interval_seconds=self.defaults_config.interval_seconds,
        )
    
    def _create_retention_config(self) -> RetentionConfig:
        """Create retention configuration with smart defaults.
        
        Returns:
            RetentionConfig with 24/7/4 retention policy
        """
        return RetentionConfig(
            hourly=self.defaults_config.retention_hourly,
            daily=self.defaults_config.retention_daily,
            weekly=self.defaults_config.retention_weekly,
        )
    
    def _create_logging_config(self) -> LoggingConfig:
        """Create logging configuration with smart defaults.
        
        Returns:
            LoggingConfig with sensible defaults
        """
        return LoggingConfig(
            level=self.defaults_config.log_level,
            log_file=Path.home() / ".local/log/devbackup.log",
            error_log_file=Path.home() / ".local/log/devbackup.err",
            log_max_size_mb=self.defaults_config.log_max_size_mb,
            log_backup_count=self.defaults_config.log_backup_count,
        )
    
    def _create_mcp_config(self) -> MCPConfig:
        """Create MCP configuration with smart defaults.
        
        Returns:
            MCPConfig with MCP enabled for Cursor integration
        """
        return MCPConfig(
            enabled=self.defaults_config.mcp_enabled,
            port=self.defaults_config.mcp_port,
        )
    
    def _create_retry_config(self) -> RetryConfig:
        """Create retry configuration with smart defaults.
        
        Returns:
            RetryConfig with sensible retry settings
        """
        return RetryConfig(
            retry_count=self.defaults_config.retry_count,
            retry_delay_seconds=self.defaults_config.retry_delay_seconds,
        )
    
    def _create_notification_config(self) -> NotificationConfig:
        """Create notification configuration with smart defaults.
        
        Returns:
            NotificationConfig with failure notifications enabled
        """
        return NotificationConfig(
            notify_on_success=self.defaults_config.notify_on_success,
            notify_on_failure=self.defaults_config.notify_on_failure,
        )
    
    def generate_config(
        self,
        projects: List[DiscoveredProject],
        destination: DiscoveredDestination,
    ) -> Configuration:
        """Generate a complete configuration from discovered components.
        
        Creates a Configuration object with:
        - Source directories from discovered projects
        - Backup destination from selected destination
        - Smart exclude patterns based on project types
        - Hourly schedule (default)
        - Sensible retention (24 hourly, 7 daily, 4 weekly)
        - Notifications enabled for failures
        
        Args:
            projects: List of discovered projects to back up
            destination: Selected backup destination
            
        Returns:
            Complete Configuration object ready for use
            
        Raises:
            DefaultsError: If no projects provided or destination is invalid
            
        Requirements: 1.5, 8.1
        """
        if not projects:
            raise DefaultsError(
                "No projects provided. At least one project is required "
                "to generate a configuration."
            )
        
        if destination is None:
            raise DefaultsError(
                "No destination provided. A backup destination is required "
                "to generate a configuration."
            )
        
        # Extract source directories from projects
        source_directories = [project.path for project in projects]
        
        # Collect all project types for exclude pattern generation
        project_types: Set[str] = {project.project_type for project in projects}
        
        # Generate appropriate exclude patterns
        exclude_patterns = self.get_exclude_patterns(project_types)
        
        # Create the backup destination path
        # Add a "devbackup" subdirectory to keep backups organized
        backup_destination = destination.path / "devbackup"
        
        # Build the complete configuration
        return Configuration(
            backup_destination=backup_destination,
            source_directories=source_directories,
            exclude_patterns=exclude_patterns,
            scheduler=self._create_scheduler_config(),
            retention=self._create_retention_config(),
            logging=self._create_logging_config(),
            mcp=self._create_mcp_config(),
            retry=self._create_retry_config(),
            notifications=self._create_notification_config(),
        )
    
    def generate_config_toml(
        self,
        projects: List[DiscoveredProject],
        destination: DiscoveredDestination,
    ) -> str:
        """Generate a TOML configuration string from discovered components.
        
        Convenience method that generates a Configuration and formats it
        as a TOML string ready to be written to a file.
        
        Args:
            projects: List of discovered projects to back up
            destination: Selected backup destination
            
        Returns:
            TOML formatted configuration string
            
        Raises:
            DefaultsError: If no projects provided or destination is invalid
        """
        from devbackup.config import format_config
        
        config = self.generate_config(projects, destination)
        return format_config(config)
