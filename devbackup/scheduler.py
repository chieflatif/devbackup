"""Scheduler management for devbackup.

This module provides the Scheduler class for managing backup scheduling
via launchd (macOS) or cron.

Smart scheduling features:
- Battery-aware: Skip backups when battery is below 20% and not charging (Requirement 8.3)
- Destination-aware: Queue backups when destination is unavailable (Requirements 8.4, 12.1)
"""

import json
import os
import subprocess
import sys
from dataclasses import dataclass, asdict
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Optional, List
import plistlib
import re


class SchedulerError(Exception):
    """Raised when scheduler operations fail."""
    pass


class BackupSkipReason(Enum):
    """Reasons why a backup might be skipped."""
    BATTERY_LOW = "battery_low"
    DESTINATION_UNAVAILABLE = "destination_unavailable"
    NONE = "none"


@dataclass
class ScheduleCheckResult:
    """Result of checking if backup should proceed."""
    should_proceed: bool
    skip_reason: BackupSkipReason
    message: str
    queued: bool = False  # True if backup was queued for later


@dataclass
class QueuedBackup:
    """A backup that was queued due to conditions not being met."""
    timestamp: str  # ISO format
    reason: str
    destination: str
    
    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: dict) -> "QueuedBackup":
        """Create from dictionary."""
        return cls(
            timestamp=data["timestamp"],
            reason=data["reason"],
            destination=data["destination"],
        )


class SchedulerType(Enum):
    """Supported scheduler types."""
    LAUNCHD = "launchd"
    CRON = "cron"


class Scheduler:
    """
    Manages backup scheduling via launchd or cron.
    
    Attributes:
        scheduler_type: Type of scheduler (launchd or cron)
        interval_seconds: Backup interval in seconds
        devbackup_command: Path to devbackup command
    """
    
    PLIST_PATH = Path.home() / "Library/LaunchAgents/com.devbackup.plist"
    LABEL = "com.devbackup"
    CRON_MARKER = "# devbackup scheduled backup"
    
    def __init__(
        self,
        scheduler_type: SchedulerType,
        interval_seconds: int,
        devbackup_command: Optional[Path] = None,
        log_file: Optional[Path] = None,
        error_log_file: Optional[Path] = None,
    ):
        """
        Initialize scheduler.
        
        Args:
            scheduler_type: Type of scheduler (launchd or cron)
            interval_seconds: Backup interval in seconds
            devbackup_command: Path to devbackup command (auto-detected if None)
            log_file: Path to stdout log file (for launchd)
            error_log_file: Path to stderr log file (for launchd)
        """
        self.scheduler_type = scheduler_type
        self.interval_seconds = interval_seconds
        self.devbackup_command = devbackup_command or self._find_devbackup_command()
        self.log_file = log_file or (Path.home() / ".local/log/devbackup.log")
        self.error_log_file = error_log_file or (Path.home() / ".local/log/devbackup.err")

    def _find_devbackup_command(self) -> Path:
        """Find the devbackup command path."""
        # Try to find devbackup in PATH
        result = subprocess.run(
            ["which", "devbackup"],
            capture_output=True,
            text=True,
        )
        if result.returncode == 0 and result.stdout.strip():
            return Path(result.stdout.strip())
        
        # Fall back to python -m devbackup
        return Path(sys.executable)
    
    def _get_program_arguments(self) -> list:
        """Get the program arguments for the scheduler."""
        if self.devbackup_command == Path(sys.executable):
            # Using python -m devbackup
            return [str(self.devbackup_command), "-m", "devbackup", "run"]
        else:
            # Using devbackup directly
            return [str(self.devbackup_command), "run"]
    
    # =========================================================================
    # launchd Implementation
    # =========================================================================
    
    def _create_launchd_plist(self) -> dict:
        """
        Generate launchd plist dictionary.
        
        Returns:
            Dictionary suitable for plistlib.dumps()
        """
        # Ensure log directories exist
        self.log_file.parent.mkdir(parents=True, exist_ok=True)
        self.error_log_file.parent.mkdir(parents=True, exist_ok=True)
        
        return {
            "Label": self.LABEL,
            "ProgramArguments": self._get_program_arguments(),
            "StartInterval": self.interval_seconds,
            "RunAtLoad": True,
            "StandardOutPath": str(self.log_file),
            "StandardErrorPath": str(self.error_log_file),
        }
    
    def _create_launchd_plist_xml(self) -> str:
        """
        Generate launchd plist XML content.
        
        Returns:
            XML string for the plist file
        """
        plist_dict = self._create_launchd_plist()
        return plistlib.dumps(plist_dict).decode("utf-8")
    
    def _install_launchd(self) -> None:
        """Install launchd scheduler."""
        # Ensure LaunchAgents directory exists
        self.PLIST_PATH.parent.mkdir(parents=True, exist_ok=True)
        
        # Unload existing job if present
        if self.PLIST_PATH.exists():
            self._uninstall_launchd()
        
        # Write plist file
        plist_content = self._create_launchd_plist()
        with open(self.PLIST_PATH, "wb") as f:
            plistlib.dump(plist_content, f)
        
        # Load the job
        result = subprocess.run(
            ["launchctl", "load", str(self.PLIST_PATH)],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            # Clean up plist file on failure
            self.PLIST_PATH.unlink(missing_ok=True)
            raise SchedulerError(
                f"Failed to load launchd job: {result.stderr or result.stdout}"
            )
    
    def _uninstall_launchd(self) -> None:
        """Uninstall launchd scheduler."""
        if not self.PLIST_PATH.exists():
            return
        
        # Unload the job (ignore errors if not loaded)
        subprocess.run(
            ["launchctl", "unload", str(self.PLIST_PATH)],
            capture_output=True,
            text=True,
        )
        
        # Remove plist file
        self.PLIST_PATH.unlink(missing_ok=True)
    
    def _is_launchd_installed(self) -> bool:
        """Check if launchd scheduler is installed."""
        return self.PLIST_PATH.exists()
    
    def _get_launchd_status(self) -> dict:
        """
        Get launchd scheduler status.
        
        Returns:
            Dictionary with status information
        """
        status = {
            "installed": self._is_launchd_installed(),
            "running": False,
            "interval_seconds": None,
            "last_exit_status": None,
            "pid": None,
        }
        
        if not status["installed"]:
            return status
        
        # Read interval from plist
        try:
            with open(self.PLIST_PATH, "rb") as f:
                plist_data = plistlib.load(f)
                status["interval_seconds"] = plist_data.get("StartInterval")
        except Exception:
            pass
        
        # Check if job is loaded and get status
        result = subprocess.run(
            ["launchctl", "list", self.LABEL],
            capture_output=True,
            text=True,
        )
        
        if result.returncode == 0:
            # Parse output: PID\tStatus\tLabel
            lines = result.stdout.strip().split("\n")
            for line in lines:
                parts = line.split("\t")
                if len(parts) >= 3 and parts[2] == self.LABEL:
                    pid_str = parts[0]
                    exit_status_str = parts[1]
                    
                    if pid_str != "-":
                        status["running"] = True
                        try:
                            status["pid"] = int(pid_str)
                        except ValueError:
                            pass
                    
                    if exit_status_str != "-":
                        try:
                            status["last_exit_status"] = int(exit_status_str)
                        except ValueError:
                            pass
                    break
        
        return status

    # =========================================================================
    # cron Implementation
    # =========================================================================
    
    def _create_cron_entry(self) -> str:
        """
        Generate crontab entry string.
        
        Returns:
            Crontab entry line
        """
        # Convert interval_seconds to cron schedule
        # cron has limited granularity, so we approximate
        minutes = self.interval_seconds // 60
        
        if minutes <= 0:
            minutes = 1
        
        if minutes < 60:
            # Run every N minutes
            schedule = f"*/{minutes} * * * *"
        elif minutes < 1440:  # Less than a day
            # Run every N hours
            hours = minutes // 60
            schedule = f"0 */{hours} * * *"
        else:
            # Run daily
            schedule = "0 0 * * *"
        
        program_args = self._get_program_arguments()
        command = " ".join(program_args)
        
        return f"{schedule} {command} {self.CRON_MARKER}"
    
    def _get_current_crontab(self) -> str:
        """Get current user's crontab content."""
        result = subprocess.run(
            ["crontab", "-l"],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            # No crontab exists
            return ""
        return result.stdout
    
    def _set_crontab(self, content: str) -> None:
        """Set user's crontab content."""
        result = subprocess.run(
            ["crontab", "-"],
            input=content,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            raise SchedulerError(
                f"Failed to update crontab: {result.stderr or result.stdout}"
            )
    
    def _install_cron(self) -> None:
        """Install cron scheduler."""
        current = self._get_current_crontab()
        
        # Remove any existing devbackup entries
        lines = [
            line for line in current.split("\n")
            if self.CRON_MARKER not in line
        ]
        
        # Add new entry
        new_entry = self._create_cron_entry()
        lines.append(new_entry)
        
        # Filter empty lines at the end but keep one newline
        while lines and lines[-1] == "":
            lines.pop()
        
        new_content = "\n".join(lines) + "\n"
        self._set_crontab(new_content)
    
    def _uninstall_cron(self) -> None:
        """Uninstall cron scheduler."""
        current = self._get_current_crontab()
        
        # Remove devbackup entries
        lines = [
            line for line in current.split("\n")
            if self.CRON_MARKER not in line
        ]
        
        # Filter empty lines at the end
        while lines and lines[-1] == "":
            lines.pop()
        
        if lines:
            new_content = "\n".join(lines) + "\n"
        else:
            new_content = ""
        
        self._set_crontab(new_content)
    
    def _is_cron_installed(self) -> bool:
        """Check if cron scheduler is installed."""
        current = self._get_current_crontab()
        return self.CRON_MARKER in current
    
    def _get_cron_status(self) -> dict:
        """
        Get cron scheduler status.
        
        Returns:
            Dictionary with status information
        """
        current = self._get_current_crontab()
        installed = self.CRON_MARKER in current
        
        status = {
            "installed": installed,
            "running": installed,  # cron is always "running" if installed
            "interval_seconds": None,
            "cron_entry": None,
        }
        
        if installed:
            # Find the entry and parse interval
            for line in current.split("\n"):
                if self.CRON_MARKER in line:
                    status["cron_entry"] = line.replace(self.CRON_MARKER, "").strip()
                    # Try to parse interval from cron expression
                    status["interval_seconds"] = self._parse_cron_interval(line)
                    break
        
        return status
    
    def _parse_cron_interval(self, cron_line: str) -> Optional[int]:
        """
        Parse interval from cron line.
        
        Args:
            cron_line: Full cron entry line
        
        Returns:
            Interval in seconds, or None if cannot parse
        """
        # Extract cron schedule (first 5 fields)
        parts = cron_line.split()
        if len(parts) < 5:
            return None
        
        minute, hour, day, month, weekday = parts[:5]
        
        # Try to parse */N patterns
        if minute.startswith("*/"):
            try:
                return int(minute[2:]) * 60
            except ValueError:
                pass
        
        if hour.startswith("*/"):
            try:
                return int(hour[2:]) * 3600
            except ValueError:
                pass
        
        # Default patterns
        if minute == "0" and hour == "0":
            return 86400  # Daily
        
        return None

    # =========================================================================
    # Public Interface
    # =========================================================================
    
    def install(self) -> None:
        """
        Install scheduler (create plist or crontab entry).
        
        Raises:
            SchedulerError: If installation fails
        """
        if self.scheduler_type == SchedulerType.LAUNCHD:
            self._install_launchd()
        elif self.scheduler_type == SchedulerType.CRON:
            self._install_cron()
        else:
            raise SchedulerError(f"Unknown scheduler type: {self.scheduler_type}")
    
    def uninstall(self) -> None:
        """
        Remove scheduler configuration.
        
        Raises:
            SchedulerError: If uninstallation fails
        """
        if self.scheduler_type == SchedulerType.LAUNCHD:
            self._uninstall_launchd()
        elif self.scheduler_type == SchedulerType.CRON:
            self._uninstall_cron()
        else:
            raise SchedulerError(f"Unknown scheduler type: {self.scheduler_type}")
    
    def is_installed(self) -> bool:
        """
        Check if scheduler is currently installed.
        
        Returns:
            True if scheduler is installed, False otherwise
        """
        if self.scheduler_type == SchedulerType.LAUNCHD:
            return self._is_launchd_installed()
        elif self.scheduler_type == SchedulerType.CRON:
            return self._is_cron_installed()
        else:
            return False
    
    def get_status(self) -> dict:
        """
        Get scheduler status (running, next run time, etc.).
        
        Returns:
            Dictionary with status information:
            - installed: bool
            - running: bool
            - interval_seconds: int or None
            - Additional fields depending on scheduler type
        """
        if self.scheduler_type == SchedulerType.LAUNCHD:
            return self._get_launchd_status()
        elif self.scheduler_type == SchedulerType.CRON:
            return self._get_cron_status()
        else:
            return {"installed": False, "running": False, "interval_seconds": None}


def parse_launchd_plist(plist_path: Path) -> Optional[int]:
    """
    Parse a launchd plist file and extract the StartInterval.
    
    Args:
        plist_path: Path to the plist file
    
    Returns:
        StartInterval value in seconds, or None if not found
    """
    if not plist_path.exists():
        return None
    
    try:
        with open(plist_path, "rb") as f:
            plist_data = plistlib.load(f)
            return plist_data.get("StartInterval")
    except Exception:
        return None


def parse_cron_interval_from_entry(cron_entry: str) -> Optional[int]:
    """
    Parse interval from a cron entry string.
    
    Args:
        cron_entry: Cron schedule string (e.g., "*/30 * * * *")
    
    Returns:
        Interval in seconds, or None if cannot parse
    """
    parts = cron_entry.split()
    if len(parts) < 5:
        return None
    
    minute, hour, day, month, weekday = parts[:5]
    
    # Parse */N minute pattern
    if minute.startswith("*/"):
        try:
            return int(minute[2:]) * 60
        except ValueError:
            pass
    
    # Parse */N hour pattern
    if hour.startswith("*/"):
        try:
            return int(hour[2:]) * 3600
        except ValueError:
            pass
    
    # Daily at midnight
    if minute == "0" and hour == "0" and day == "*":
        return 86400
    
    # Hourly
    if minute == "0" and hour == "*":
        return 3600
    
    return None


# =============================================================================
# Smart Scheduling Functions
# =============================================================================

# Default queue file location
BACKUP_QUEUE_PATH = Path.home() / ".cache" / "devbackup" / "queue.json"


def check_battery_for_backup(threshold: int = 20) -> ScheduleCheckResult:
    """
    Check if battery status allows backup to proceed.
    
    Requirements: 8.3 - Skip backups when battery is below 20% and not charging
    
    Args:
        threshold: Minimum battery level to allow backup (default 20%)
    
    Returns:
        ScheduleCheckResult indicating if backup should proceed
    """
    from devbackup.battery import get_battery_status, BatteryError
    
    try:
        status = get_battery_status()
        
        if not status.is_present:
            return ScheduleCheckResult(
                should_proceed=True,
                skip_reason=BackupSkipReason.NONE,
                message="No battery detected (desktop Mac)",
            )
        
        if status.is_charging:
            return ScheduleCheckResult(
                should_proceed=True,
                skip_reason=BackupSkipReason.NONE,
                message=f"Battery at {status.level}% and charging",
            )
        
        if status.level >= threshold:
            return ScheduleCheckResult(
                should_proceed=True,
                skip_reason=BackupSkipReason.NONE,
                message=f"Battery at {status.level}%",
            )
        
        return ScheduleCheckResult(
            should_proceed=False,
            skip_reason=BackupSkipReason.BATTERY_LOW,
            message=(
                f"Battery at {status.level}% and not charging. "
                f"Backup skipped to preserve battery (threshold: {threshold}%)"
            ),
        )
        
    except BatteryError as e:
        # If we can't determine battery status, allow backup to proceed
        # This is a safe default - better to backup than skip
        return ScheduleCheckResult(
            should_proceed=True,
            skip_reason=BackupSkipReason.NONE,
            message=f"Could not determine battery status: {e}",
        )


def check_destination_available(destination: Path) -> ScheduleCheckResult:
    """
    Check if backup destination is available.
    
    Requirements: 8.4, 12.1 - Skip backups when destination unavailable, queue for later
    
    Args:
        destination: Path to backup destination
    
    Returns:
        ScheduleCheckResult indicating if backup should proceed
    """
    from devbackup.destination import is_volume_mounted, is_writable
    
    # Check if volume is mounted (for external drives)
    if not is_volume_mounted(destination):
        return ScheduleCheckResult(
            should_proceed=False,
            skip_reason=BackupSkipReason.DESTINATION_UNAVAILABLE,
            message=f"Backup destination not available: {destination} (volume not mounted)",
        )
    
    # Check if destination exists
    if not destination.exists():
        return ScheduleCheckResult(
            should_proceed=False,
            skip_reason=BackupSkipReason.DESTINATION_UNAVAILABLE,
            message=f"Backup destination not found: {destination}",
        )
    
    # Check if destination is writable
    if not is_writable(destination):
        return ScheduleCheckResult(
            should_proceed=False,
            skip_reason=BackupSkipReason.DESTINATION_UNAVAILABLE,
            message=f"Backup destination not writable: {destination}",
        )
    
    return ScheduleCheckResult(
        should_proceed=True,
        skip_reason=BackupSkipReason.NONE,
        message=f"Destination available: {destination}",
    )


def check_backup_conditions(
    destination: Path,
    battery_threshold: int = 20,
) -> ScheduleCheckResult:
    """
    Check all conditions for backup to proceed.
    
    Checks battery status and destination availability.
    
    Requirements: 8.3, 8.4, 12.1
    
    Args:
        destination: Path to backup destination
        battery_threshold: Minimum battery level (default 20%)
    
    Returns:
        ScheduleCheckResult indicating if backup should proceed
    """
    # Check battery first (faster check)
    battery_result = check_battery_for_backup(battery_threshold)
    if not battery_result.should_proceed:
        return battery_result
    
    # Check destination availability
    dest_result = check_destination_available(destination)
    if not dest_result.should_proceed:
        return dest_result
    
    return ScheduleCheckResult(
        should_proceed=True,
        skip_reason=BackupSkipReason.NONE,
        message="All conditions met for backup",
    )


def queue_backup(
    destination: Path,
    reason: str,
    queue_path: Optional[Path] = None,
) -> QueuedBackup:
    """
    Add a backup to the queue for later execution.
    
    Requirements: 12.1, 12.4 - Queue backups when destination unavailable
    
    Args:
        destination: Path to backup destination
        reason: Reason for queueing
        queue_path: Path to queue file (default: ~/.cache/devbackup/queue.json)
    
    Returns:
        QueuedBackup that was added to queue
    """
    if queue_path is None:
        queue_path = BACKUP_QUEUE_PATH
    
    # Ensure queue directory exists
    queue_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Create queued backup entry
    queued = QueuedBackup(
        timestamp=datetime.now().isoformat(),
        reason=reason,
        destination=str(destination),
    )
    
    # Load existing queue
    queue = load_backup_queue(queue_path)
    
    # Add new entry
    queue.append(queued)
    
    # Save queue
    save_backup_queue(queue, queue_path)
    
    return queued


def load_backup_queue(queue_path: Optional[Path] = None) -> List[QueuedBackup]:
    """
    Load the backup queue from disk.
    
    Requirements: 12.4 - Queue survives restarts
    
    Args:
        queue_path: Path to queue file
    
    Returns:
        List of queued backups
    """
    if queue_path is None:
        queue_path = BACKUP_QUEUE_PATH
    
    if not queue_path.exists():
        return []
    
    try:
        with open(queue_path, "r") as f:
            data = json.load(f)
            return [QueuedBackup.from_dict(item) for item in data]
    except (json.JSONDecodeError, KeyError, TypeError):
        # Corrupted queue file - return empty
        return []


def save_backup_queue(
    queue: List[QueuedBackup],
    queue_path: Optional[Path] = None,
) -> None:
    """
    Save the backup queue to disk.
    
    Requirements: 12.4 - Queue survives restarts
    
    Args:
        queue: List of queued backups
        queue_path: Path to queue file
    """
    if queue_path is None:
        queue_path = BACKUP_QUEUE_PATH
    
    # Ensure directory exists
    queue_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Write atomically using temp file
    temp_path = queue_path.with_suffix(".tmp")
    try:
        with open(temp_path, "w") as f:
            json.dump([q.to_dict() for q in queue], f, indent=2)
        temp_path.replace(queue_path)
    except Exception:
        # Clean up temp file on error
        temp_path.unlink(missing_ok=True)
        raise


def clear_backup_queue(queue_path: Optional[Path] = None) -> int:
    """
    Clear all entries from the backup queue.
    
    Args:
        queue_path: Path to queue file
    
    Returns:
        Number of entries cleared
    """
    if queue_path is None:
        queue_path = BACKUP_QUEUE_PATH
    
    queue = load_backup_queue(queue_path)
    count = len(queue)
    
    if queue_path.exists():
        queue_path.unlink()
    
    return count


def process_backup_queue(
    queue_path: Optional[Path] = None,
) -> List[QueuedBackup]:
    """
    Get queued backups that should be processed.
    
    This returns the queue contents and clears the queue.
    The caller is responsible for actually running the backups.
    
    Requirements: 12.1 - Process queue when destination returns
    
    Args:
        queue_path: Path to queue file
    
    Returns:
        List of queued backups to process
    """
    if queue_path is None:
        queue_path = BACKUP_QUEUE_PATH
    
    queue = load_backup_queue(queue_path)
    
    if queue:
        # Clear the queue after reading
        clear_backup_queue(queue_path)
    
    return queue
