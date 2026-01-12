"""Notification system for devbackup.

Provides macOS notifications for backup events using osascript.

Requirements: 11.1-11.5
"""

import subprocess
import logging
from typing import Optional

from devbackup.config import NotificationConfig

logger = logging.getLogger(__name__)


class Notifier:
    """
    Sends macOS notifications for backup events.
    
    Uses osascript to display native macOS notifications.
    
    Requirements: 11.1, 11.2, 11.3, 11.4
    """
    
    def __init__(self, config: Optional[NotificationConfig] = None):
        self.config = config or NotificationConfig()
    
    def notify_success(
        self,
        snapshot_name: str,
        duration_seconds: float,
        files_transferred: int,
    ) -> bool:
        """
        Send success notification if enabled.
        
        Args:
            snapshot_name: Name of the completed snapshot
            duration_seconds: Backup duration in seconds
            files_transferred: Number of files transferred
        
        Returns:
            True if notification was sent, False otherwise
        
        Requirements: 11.2, 11.4
        """
        if not self.config.notify_on_success:
            return False
        
        duration_str = self._format_duration(duration_seconds)
        message = f"Snapshot: {snapshot_name}\nFiles: {files_transferred}\nDuration: {duration_str}"
        
        return self._send_notification(
            title="devbackup: Backup Complete",
            message=message,
            sound=True,
        )

    
    def notify_failure(
        self,
        error_message: str,
        duration_seconds: float,
    ) -> bool:
        """
        Send failure notification if enabled.
        
        Args:
            error_message: Error message describing the failure
            duration_seconds: Time elapsed before failure
        
        Returns:
            True if notification was sent, False otherwise
        
        Requirements: 11.3, 11.4
        """
        if not self.config.notify_on_failure:
            return False
        
        duration_str = self._format_duration(duration_seconds)
        # Truncate long error messages
        if len(error_message) > 100:
            error_message = error_message[:97] + "..."
        
        message = f"Error: {error_message}\nDuration: {duration_str}"
        
        return self._send_notification(
            title="devbackup: Backup Failed",
            message=message,
            sound=True,
        )
    
    def _send_notification(
        self,
        title: str,
        message: str,
        sound: bool = True,
    ) -> bool:
        """
        Send macOS notification via osascript.
        
        Uses AppleScript:
        display notification "message" with title "title" sound name "default"
        
        Args:
            title: Notification title
            message: Notification body
            sound: Whether to play a sound
        
        Returns:
            True if notification was sent successfully
        
        Requirements: 11.1
        """
        # Escape quotes in message and title for AppleScript
        escaped_title = title.replace('"', '\\"').replace("'", "'\\''")
        escaped_message = message.replace('"', '\\"').replace("'", "'\\''")
        
        if sound:
            script = f'display notification "{escaped_message}" with title "{escaped_title}" sound name "default"'
        else:
            script = f'display notification "{escaped_message}" with title "{escaped_title}"'
        
        try:
            result = subprocess.run(
                ["osascript", "-e", script],
                capture_output=True,
                text=True,
                timeout=5,
            )
            if result.returncode != 0:
                logger.warning(f"Notification failed: {result.stderr}")
                return False
            return True
        except subprocess.TimeoutExpired:
            logger.warning("Notification timed out")
            return False
        except FileNotFoundError:
            logger.warning("osascript not found - notifications not available")
            return False
        except Exception as e:
            logger.warning(f"Notification error: {e}")
            return False
    
    def _format_duration(self, seconds: float) -> str:
        """Format duration in human-readable form."""
        if seconds < 60:
            return f"{seconds:.1f}s"
        elif seconds < 3600:
            minutes = int(seconds // 60)
            secs = int(seconds % 60)
            return f"{minutes}m {secs}s"
        else:
            hours = int(seconds // 3600)
            minutes = int((seconds % 3600) // 60)
            return f"{hours}h {minutes}m"
