"""Plain language translator for devbackup.

This module provides translation of technical messages to friendly,
non-technical language for non-technical users.

Requirements: 2.5, 2.8, 6.1, 6.3, 6.4, 6.5, 9.1
"""

from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


# Technical jargon blocklist - these terms should never appear in user-facing output
TECHNICAL_BLOCKLIST = [
    "snapshot",
    "rsync",
    "pid",
    "lock",
    "daemon",
    "stderr",
    "stdout",
    "exit code",
    "exception",
    "traceback",
    "ISO 8601",
    "epoch",
    "bytes",
]


class PlainLanguageTranslator:
    """Translates technical messages to plain language.
    
    This class converts technical backup system messages into friendly,
    non-technical language suitable for users who have no experience
    with terminal commands or configuration files.
    
    Requirements: 6.1, 6.4, 6.5
    """
    
    # Size translations - maps byte ranges to friendly descriptions
    # Note: Avoid using "bytes", "gigabytes", etc. as they contain blocklisted terms
    SIZE_DESCRIPTIONS: List[Tuple[int, int, str]] = [
        (0, 1_000_000, "a few files"),
        (1_000_000, 100_000_000, "a small folder"),
        (100_000_000, 1_000_000_000, "about the size of a movie"),
        (1_000_000_000, 10_000_000_000, "a few GB of data"),
        (10_000_000_000, float('inf'), "quite a lot of data"),
    ]
    
    # Time translations - maps seconds ago to friendly descriptions
    TIME_DESCRIPTIONS: List[Tuple[int, int, str]] = [
        (0, 60, "just now"),
        (60, 3600, "{minutes} minutes ago"),
        (3600, 86400, "{hours} hours ago"),
        (86400, 604800, "{days} days ago"),
        (604800, float('inf'), "on {date}"),
    ]
    
    # File count translations
    FILE_COUNT_DESCRIPTIONS: List[Tuple[int, int, str]] = [
        (0, 1, "no files"),
        (1, 2, "1 file"),
        (2, 10, "a few files"),
        (10, 100, "several files"),
        (100, 1000, "hundreds of files"),
        (1000, 10000, "thousands of files"),
        (10000, float('inf'), "all your project files"),
    ]
    
    # Error type to plain language mapping
    ERROR_TRANSLATIONS: Dict[str, Tuple[str, str]] = {
        # (problem description, suggested solution)
        "DestinationError": (
            "I can't find your backup drive",
            "Is it plugged in? If you're using an external drive, make sure it's connected."
        ),
        "SpaceError": (
            "Your backup drive is getting full",
            "I can delete old backups to make room if you'd like."
        ),
        "LockError": (
            "A backup is already running",
            "I'll let you know when it's done."
        ),
        "ConfigurationError": (
            "I need to set up backups first",
            "Want me to do that now?"
        ),
        "ConfigError": (
            "Something's wrong with the backup settings",
            "Want me to set things up fresh?"
        ),
        "DiscoveryError": (
            "I couldn't find any projects to back up",
            "Are you in the right folder?"
        ),
        "RestoreError": (
            "I couldn't find that file in your backups",
            "It might have been created after your last backup."
        ),
        "IPCError": (
            "The backup status indicator isn't responding",
            "Your backups are still running though."
        ),
        "PermissionError": (
            "I don't have permission to access some files",
            "You might need to grant access in System Settings."
        ),
        "FileNotFoundError": (
            "I couldn't find the file you're looking for",
            "It may have been moved or deleted."
        ),
        "OSError": (
            "Something went wrong with the file system",
            "Try again, and if it keeps happening, restart your Mac."
        ),
    }
    
    def translate_size(self, size_bytes: int) -> str:
        """Translate byte count to friendly description.
        
        Args:
            size_bytes: Size in bytes
            
        Returns:
            Human-friendly size description
            
        Requirements: 6.4
        """
        if size_bytes < 0:
            size_bytes = 0
            
        for min_bytes, max_bytes, description in self.SIZE_DESCRIPTIONS:
            if min_bytes <= size_bytes < max_bytes:
                return description
        
        return "quite a lot of data"
    
    def translate_size_precise(self, size_bytes: int) -> str:
        """Translate byte count to precise but friendly description.
        
        Args:
            size_bytes: Size in bytes
            
        Returns:
            Human-friendly size with approximate value (e.g., "about 1.2 GB")
            
        Requirements: 6.4
        """
        if size_bytes < 0:
            size_bytes = 0
            
        if size_bytes < 1_000:
            return "a tiny amount"
        elif size_bytes < 1_000_000:
            kb = size_bytes / 1_000
            return f"about {kb:.0f} KB"
        elif size_bytes < 1_000_000_000:
            mb = size_bytes / 1_000_000
            if mb < 10:
                return f"about {mb:.1f} MB"
            return f"about {mb:.0f} MB"
        elif size_bytes < 1_000_000_000_000:
            gb = size_bytes / 1_000_000_000
            if gb < 10:
                return f"about {gb:.1f} GB"
            return f"about {gb:.0f} GB"
        else:
            tb = size_bytes / 1_000_000_000_000
            return f"about {tb:.1f} TB"
    
    def translate_time(self, timestamp: datetime) -> str:
        """Translate timestamp to relative time description.
        
        Args:
            timestamp: The datetime to translate
            
        Returns:
            Human-friendly relative time (e.g., "2 hours ago")
            
        Requirements: 6.5
        """
        now = datetime.now()
        
        # Handle future timestamps
        if timestamp > now:
            return self._translate_future_time(timestamp, now)
        
        delta = now - timestamp
        seconds_ago = delta.total_seconds()
        
        for min_secs, max_secs, template in self.TIME_DESCRIPTIONS:
            if min_secs <= seconds_ago < max_secs:
                if "{minutes}" in template:
                    minutes = int(seconds_ago / 60)
                    return template.format(minutes=minutes)
                elif "{hours}" in template:
                    hours = int(seconds_ago / 3600)
                    return template.format(hours=hours)
                elif "{days}" in template:
                    days = int(seconds_ago / 86400)
                    return template.format(days=days)
                elif "{date}" in template:
                    # Format as friendly date
                    return template.format(date=timestamp.strftime("%B %d"))
                return template
        
        return f"on {timestamp.strftime('%B %d')}"
    
    def _translate_future_time(self, timestamp: datetime, now: datetime) -> str:
        """Translate future timestamp to friendly description."""
        delta = timestamp - now
        seconds_until = delta.total_seconds()
        
        if seconds_until < 60:
            return "in a moment"
        elif seconds_until < 3600:
            minutes = int(seconds_until / 60)
            return f"in about {minutes} minutes"
        elif seconds_until < 86400:
            hours = int(seconds_until / 3600)
            if hours == 1:
                return "in about an hour"
            return f"in about {hours} hours"
        else:
            days = int(seconds_until / 86400)
            if days == 1:
                return "tomorrow"
            return f"in {days} days"
    
    def translate_file_count(self, count: int) -> str:
        """Translate file count to friendly description.
        
        Args:
            count: Number of files
            
        Returns:
            Human-friendly file count description
            
        Requirements: 6.4
        """
        if count < 0:
            count = 0
            
        for min_count, max_count, description in self.FILE_COUNT_DESCRIPTIONS:
            if min_count <= count < max_count:
                return description
        
        return "all your project files"
    
    def translate_error(self, error: Exception) -> str:
        """Translate exception to plain language with solution.
        
        Args:
            error: The exception to translate
            
        Returns:
            Plain language error message with suggested solution
            
        Requirements: 9.1
        """
        error_type = type(error).__name__
        
        # Check for known error types
        if error_type in self.ERROR_TRANSLATIONS:
            problem, solution = self.ERROR_TRANSLATIONS[error_type]
            return f"{problem}. {solution}"
        
        # Check for partial matches (e.g., "BackupDestinationError" contains "DestinationError")
        for known_type, (problem, solution) in self.ERROR_TRANSLATIONS.items():
            if known_type in error_type:
                return f"{problem}. {solution}"
        
        # Generic fallback
        return (
            "Something unexpected happened. "
            "Try again, and if it keeps happening, I can help troubleshoot."
        )
    
    def translate_status(self, status: Dict[str, Any]) -> str:
        """Translate status dictionary to friendly summary.
        
        Args:
            status: Status dictionary with technical values
            
        Returns:
            Human-friendly status summary
            
        Requirements: 2.5, 2.8
        """
        parts = []
        
        # Handle backup status
        if "status" in status:
            status_value = status["status"]
            if status_value == "protected":
                parts.append("Your files are safe")
            elif status_value == "backing_up":
                parts.append("Backing up your files right now")
            elif status_value == "warning":
                parts.append("Your backups need attention")
            elif status_value == "error":
                parts.append("There's a problem with your backups")
            else:
                parts.append("Checking your backup status")
        
        # Handle last backup time
        if "last_backup" in status:
            last_backup = status["last_backup"]
            if isinstance(last_backup, datetime):
                time_str = self.translate_time(last_backup)
                parts.append(f"Last backed up {time_str}")
            elif isinstance(last_backup, str) and last_backup:
                parts.append(f"Last backed up {last_backup}")
        
        # Handle next scheduled backup
        if "next_backup" in status:
            next_backup = status["next_backup"]
            if isinstance(next_backup, datetime):
                time_str = self._translate_future_time(next_backup, datetime.now())
                parts.append(f"Next backup {time_str}")
            elif isinstance(next_backup, str) and next_backup:
                parts.append(f"Next backup {next_backup}")
        
        # Handle total size
        if "total_size" in status:
            size = status["total_size"]
            if isinstance(size, int):
                size_str = self.translate_size_precise(size)
                parts.append(f"Total backup size: {size_str}")
        
        # Handle file count
        if "files_transferred" in status or "file_count" in status:
            count = status.get("files_transferred") or status.get("file_count", 0)
            if isinstance(count, int):
                count_str = self.translate_file_count(count)
                parts.append(f"Backed up {count_str}")
        
        # Handle snapshot count
        if "total_snapshots" in status or "snapshot_count" in status:
            count = status.get("total_snapshots") or status.get("snapshot_count", 0)
            if isinstance(count, int):
                if count == 1:
                    parts.append("1 backup version saved")
                else:
                    parts.append(f"{count} backup versions saved")
        
        if not parts:
            return "Your backup status is being checked"
        
        return ". ".join(parts) + "."
    
    def success_backup(self, snapshot_name: str, duration: float) -> str:
        """Generate friendly success message for completed backup.
        
        Args:
            snapshot_name: Name of the created snapshot (not shown to user)
            duration: Duration in seconds
            
        Returns:
            Friendly success message
            
        Requirements: 6.1
        """
        duration_str = self._friendly_duration(duration)
        return f"All done! Your projects are safely backed up. This took about {duration_str}."
    
    def _friendly_duration(self, seconds: float) -> str:
        """Convert duration in seconds to friendly string."""
        if seconds < 1:
            return "a moment"
        elif seconds < 60:
            return f"{int(seconds)} seconds"
        elif seconds < 3600:
            minutes = int(seconds / 60)
            if minutes == 1:
                return "a minute"
            return f"{minutes} minutes"
        else:
            hours = int(seconds / 3600)
            if hours == 1:
                return "an hour"
            return f"{hours} hours"
    
    def error_destination_missing(self, path: Optional[Path] = None) -> str:
        """Generate friendly message for missing backup destination.
        
        Args:
            path: The missing path (not shown to user)
            
        Returns:
            Friendly error message with solution
            
        Requirements: 6.3
        """
        return (
            "I couldn't find your backup drive. "
            "If you're using an external drive, make sure it's plugged in. "
            "Otherwise, I can set up a backup folder on your Mac."
        )
    
    def error_space_insufficient(self, available: int, required: int) -> str:
        """Generate friendly message for insufficient space.
        
        Args:
            available: Available space in bytes
            required: Required space in bytes
            
        Returns:
            Friendly error message with solution
            
        Requirements: 6.3
        """
        available_str = self.translate_size_precise(available)
        return (
            f"Your backup drive is getting full (only {available_str} left). "
            "I can delete old backups to make room if you'd like."
        )
    
    def error_backup_running(self) -> str:
        """Generate friendly message when backup is already running.
        
        Returns:
            Friendly message
            
        Requirements: 6.3
        """
        return "A backup is already running. I'll let you know when it's done."
    
    def describe_projects(
        self,
        projects: List[Dict[str, Any]],
    ) -> str:
        """Generate friendly description of discovered projects.
        
        Args:
            projects: List of project dictionaries with name and size
            
        Returns:
            Friendly project list description
            
        Requirements: 4.7
        """
        if not projects:
            return "I couldn't find any projects to back up."
        
        if len(projects) == 1:
            p = projects[0]
            name = p.get("name", "your project")
            size = p.get("estimated_size_bytes", 0)
            size_str = self.translate_size_precise(size)
            return f"I found 1 project: {name} ({size_str})"
        
        parts = [f"I found {len(projects)} projects:"]
        for p in projects:
            name = p.get("name", "Unknown")
            size = p.get("estimated_size_bytes", 0)
            size_str = self.translate_size_precise(size)
            parts.append(f"  - {name} ({size_str})")
        
        return "\n".join(parts)
    
    def describe_destination(
        self,
        destination: Dict[str, Any],
    ) -> str:
        """Generate friendly description of backup destination.
        
        Args:
            destination: Destination dictionary with name and available space
            
        Returns:
            Friendly destination description
            
        Requirements: 5.7
        """
        name = destination.get("name", "your backup location")
        available = destination.get("available_bytes", 0)
        available_str = self.translate_size_precise(available)
        dest_type = destination.get("destination_type", "local")
        
        if dest_type == "external":
            return (
                f"Your external drive '{name}' has {available_str} free - "
                "that's a great place for backups!"
            )
        elif dest_type == "icloud":
            return (
                f"iCloud Drive has {available_str} free. "
                "Your backups will sync across your devices."
            )
        elif dest_type == "network":
            return (
                f"Network drive '{name}' has {available_str} free."
            )
        else:
            return (
                f"I can use a folder on your Mac with {available_str} free. "
                "Note: this won't protect against drive failure."
            )
    
    def contains_technical_jargon(self, text: str) -> bool:
        """Check if text contains technical jargon from blocklist.
        
        Args:
            text: Text to check
            
        Returns:
            True if text contains blocklisted terms
        """
        text_lower = text.lower()
        for term in TECHNICAL_BLOCKLIST:
            if term.lower() in text_lower:
                return True
        return False
    
    def sanitize_output(self, text: str) -> str:
        """Remove or replace technical jargon in output text.
        
        Args:
            text: Text that may contain technical terms
            
        Returns:
            Sanitized text with technical terms replaced
        """
        result = text
        
        # Replace common technical terms with friendly alternatives
        replacements = {
            "snapshot": "backup version",
            "rsync": "backup",
            "pid": "process",
            "lock": "busy",
            "daemon": "background service",
            "stderr": "error output",
            "stdout": "output",
            "exit code": "result",
            "exception": "error",
            "traceback": "error details",
            "ISO 8601": "date format",
            "epoch": "timestamp",
            "bytes": "",  # Remove "bytes" - use friendly sizes instead
        }
        
        for technical, friendly in replacements.items():
            # Case-insensitive replacement
            import re
            pattern = re.compile(re.escape(technical), re.IGNORECASE)
            result = pattern.sub(friendly, result)
        
        # Clean up any double spaces
        while "  " in result:
            result = result.replace("  ", " ")
        
        return result.strip()
