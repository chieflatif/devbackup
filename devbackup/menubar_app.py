#!/usr/bin/env python3
"""
DevBackup Menu Bar App

A simple macOS menu bar application for DevBackup status and control.
Uses rumps (Ridiculously Uncomplicated macOS Python Statusbar apps).

This app is designed for non-technical users:
- Starts automatically at login (optional)
- Run backups with one click from the menu bar
- All control via menu bar - no terminal needed
"""

import os
import subprocess
import threading
import sys
from datetime import datetime
from pathlib import Path

import rumps

from devbackup.config import DEFAULT_CONFIG_PATH


# LaunchAgent plist path for auto-start at login
LAUNCH_AGENT_PATH = Path.home() / "Library/LaunchAgents/com.devbackup.menubar.plist"

# File to store last backup info
LAST_BACKUP_FILE = Path.home() / ".config/devbackup/last_backup.txt"


def has_config() -> bool:
    """Check if configuration exists."""
    return DEFAULT_CONFIG_PATH.exists()


def is_autostart_enabled() -> bool:
    """Check if auto-start at login is enabled."""
    return LAUNCH_AGENT_PATH.exists()


def save_last_backup(files_changed: int, total_files: int, success: bool):
    """Save last backup info to file."""
    try:
        LAST_BACKUP_FILE.parent.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().isoformat()
        status = "success" if success else "failed"
        LAST_BACKUP_FILE.write_text(f"{timestamp}|{files_changed}|{total_files}|{status}")
    except Exception:
        pass


def get_last_backup_info() -> tuple[str, int, int, bool] | None:
    """Get last backup info from file or snapshots.
    
    Returns tuple of (time_ago_string, files_changed, total_files, success) or None.
    """
    # First try our saved file
    if LAST_BACKUP_FILE.exists():
        try:
            content = LAST_BACKUP_FILE.read_text().strip()
            parts = content.split("|")
            if len(parts) >= 4:
                timestamp = datetime.fromisoformat(parts[0])
                files_changed = int(parts[1])
                total_files = int(parts[2])
                success = parts[3] == "success"
                time_ago = format_time_ago(timestamp)
                return (time_ago, files_changed, total_files, success)
            elif len(parts) >= 3:
                # Old format compatibility
                timestamp = datetime.fromisoformat(parts[0])
                files = int(parts[1])
                success = parts[2] == "success"
                time_ago = format_time_ago(timestamp)
                return (time_ago, 0, files, success)
        except Exception:
            pass
    
    # Fall back to checking snapshot directories
    try:
        from devbackup.config import parse_config
        config = parse_config()
        backup_path = config.backup_destination
        
        if not backup_path.exists():
            return None
        
        # Find most recent snapshot directory
        snapshots = []
        for entry in backup_path.iterdir():
            if entry.is_dir() and not entry.name.startswith((".", "in_progress")):
                # Try to parse timestamp from name (YYYY-MM-DD-HHMMSS)
                try:
                    timestamp = datetime.strptime(entry.name[:17], "%Y-%m-%d-%H%M%S")
                    snapshots.append((timestamp, entry))
                except ValueError:
                    continue
        
        if snapshots:
            snapshots.sort(key=lambda x: x[0], reverse=True)
            latest_time, latest_path = snapshots[0]
            time_ago = format_time_ago(latest_time)
            # Count files in snapshot (quick estimate)
            file_count = sum(1 for _ in latest_path.rglob("*") if _.is_file())
            return (time_ago, 0, file_count, True)  # Can't know changed count from disk
    except Exception:
        pass
    
    return None


def format_time_ago(timestamp: datetime) -> str:
    """Format a timestamp as a human-readable 'time ago' string."""
    now = datetime.now()
    diff = now - timestamp
    
    seconds = int(diff.total_seconds())
    
    if seconds < 60:
        return "just now"
    elif seconds < 3600:
        minutes = seconds // 60
        return f"{minutes} min ago"
    elif seconds < 86400:
        hours = seconds // 3600
        return f"{hours} hour{'s' if hours != 1 else ''} ago"
    elif seconds < 604800:
        days = seconds // 86400
        return f"{days} day{'s' if days != 1 else ''} ago"
    else:
        return timestamp.strftime("%b %d, %Y")


def enable_autostart() -> bool:
    """Enable auto-start at login by creating a LaunchAgent plist."""
    try:
        # Ensure LaunchAgents directory exists
        LAUNCH_AGENT_PATH.parent.mkdir(parents=True, exist_ok=True)
        
        # Get the Python executable path
        python_path = sys.executable
        
        plist_content = f'''<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.devbackup.menubar</string>
    <key>ProgramArguments</key>
    <array>
        <string>{python_path}</string>
        <string>-m</string>
        <string>devbackup.menubar_app</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <false/>
    <key>StandardOutPath</key>
    <string>/tmp/devbackup-menubar.log</string>
    <key>StandardErrorPath</key>
    <string>/tmp/devbackup-menubar.err</string>
</dict>
</plist>
'''
        LAUNCH_AGENT_PATH.write_text(plist_content)
        
        # Load the agent (will start at next login, or we can load it now)
        subprocess.run(
            ["launchctl", "load", str(LAUNCH_AGENT_PATH)],
            capture_output=True
        )
        return True
    except Exception:
        return False


def disable_autostart() -> bool:
    """Disable auto-start at login by removing the LaunchAgent plist."""
    try:
        if LAUNCH_AGENT_PATH.exists():
            # Unload the agent first
            subprocess.run(
                ["launchctl", "unload", str(LAUNCH_AGENT_PATH)],
                capture_output=True
            )
            LAUNCH_AGENT_PATH.unlink()
        return True
    except Exception:
        return False


class DevBackupMenuBar(rumps.App):
    """Menu bar application for DevBackup."""
    
    def __init__(self):
        super().__init__(
            name="DevBackup",
            title="DB",  # Menu bar text
            quit_button=None,  # We'll add our own
        )
        
        # Status menu item (non-clickable header)
        self.status_item = rumps.MenuItem("Starting...", callback=None)
        self.status_item.set_callback(None)
        
        # Last backup info item (non-clickable)
        self.last_backup_item = rumps.MenuItem("Last backup: checking...", callback=None)
        self.last_backup_item.set_callback(None)
        
        # Build menu
        self.autostart_item = rumps.MenuItem(
            "Start at Login", 
            callback=self.toggle_autostart
        )
        self.autostart_item.state = is_autostart_enabled()
        
        self.menu = [
            self.status_item,
            self.last_backup_item,
            None,  # Separator
            rumps.MenuItem("Back Up Now", callback=self.backup_now),
            rumps.MenuItem("Browse Backups", callback=self.browse_backups),
            None,  # Separator
            rumps.MenuItem("Preferences...", callback=self.open_preferences),
            self.autostart_item,
            None,  # Separator
            rumps.MenuItem("Quit", callback=self.quit_app),
        ]
        
        # Track if backup is running
        self._backup_in_progress = False
        
        # Check if setup is needed
        if not has_config():
            self.status_item.title = "⚠️ Setup required"
            self.last_backup_item.title = "No backups yet"
            self.title = "!"
        else:
            # Update last backup info immediately
            self.update_last_backup_info()
            
            # Set initial status
            self.update_status(None)
            
            # Start status update timer
            self.timer = rumps.Timer(self.update_status, 30)
            self.timer.start()
    
    def update_last_backup_info(self):
        """Update the last backup info in the menu."""
        info = get_last_backup_info()
        if info:
            time_ago, files_changed, total_files, success = info
            if success:
                if files_changed > 0:
                    self.last_backup_item.title = f"✓ Last: {time_ago} • {files_changed} changed / {total_files} total"
                elif total_files > 0:
                    self.last_backup_item.title = f"✓ Last: {time_ago} • {total_files} files"
                else:
                    self.last_backup_item.title = f"✓ Last backup: {time_ago}"
            else:
                self.last_backup_item.title = f"✕ Last backup failed: {time_ago}"
        else:
            self.last_backup_item.title = "No backups yet"
    
    def update_status(self, _):
        """Update status display."""
        # If no config, show setup required
        if not has_config():
            self.status_item.title = "⚠️ Setup required"
            self.last_backup_item.title = "No backups yet"
            self.title = "!"
            return
        
        # Update last backup info
        self.update_last_backup_info()
        
        # If backup is in progress, don't change status
        if self._backup_in_progress:
            return
        
        # Check if we have any backups
        info = get_last_backup_info()
        if info:
            time_ago, _, _, success = info
            if success:
                self.status_item.title = "🟢 Ready to back up"
                self.title = "DB"
            else:
                self.status_item.title = "🟡 Last backup failed"
                self.title = "⚠"
        else:
            self.status_item.title = "🟢 Ready to back up"
            self.title = "DB"
    
    def backup_now(self, _):
        """Trigger immediate backup with progress display."""
        # Check if setup is needed first
        if not has_config():
            rumps.alert(
                title="Setup Required",
                message="Please configure DevBackup first.\n\nClick 'Preferences...' to set up your backup sources and destination."
            )
            return
        
        # Update UI immediately to show backup starting
        self._backup_in_progress = True
        self.status_item.title = "🔵 Scanning files..."
        self.title = "⟳"
        
        # Shared state for progress
        self._backup_running = True
        self._backup_start_time = None
        self._backup_progress = {
            "files": 0, 
            "percent": None, 
            "done": False, 
            "success": False, 
            "message": "",
            "current_file": None
        }
        
        def update_progress(progress_info):
            """Callback to update progress state."""
            try:
                if progress_info.percent_complete is not None:
                    # Only show percentage if it's meaningful (not jumping straight to 100)
                    if progress_info.percent_complete < 100 or self._backup_progress["percent"] is not None:
                        self._backup_progress["percent"] = int(progress_info.percent_complete)
                if progress_info.files_transferred:
                    self._backup_progress["files"] = progress_info.files_transferred
                if progress_info.current_file:
                    self._backup_progress["current_file"] = progress_info.current_file
            except Exception:
                pass
        
        def do_backup():
            """Run the backup in background."""
            import time
            self._backup_start_time = time.time()
            
            try:
                from devbackup.backup import run_backup
                
                result = run_backup(progress_callback=update_progress)
                
                duration = time.time() - self._backup_start_time
                
                self._backup_progress["done"] = True
                self._backup_progress["success"] = result.success
                
                if result.success:
                    # Get file counts from result
                    files_changed = 0
                    total_files = 0
                    if result.snapshot_result:
                        files_changed = result.snapshot_result.files_transferred or 0
                        total_files = result.snapshot_result.total_files or 0
                    
                    # If we didn't get total from rsync, use what we tracked
                    if total_files == 0 and self._backup_progress["files"] > 0:
                        total_files = self._backup_progress["files"]
                    
                    # Store for display
                    self._backup_progress["files_changed"] = files_changed
                    self._backup_progress["total_files"] = total_files
                    
                    # Format duration nicely
                    if duration < 60:
                        duration_str = f"{int(duration)} sec"
                    else:
                        duration_str = f"{int(duration/60)}m {int(duration%60)}s"
                    
                    self._backup_progress["duration_str"] = duration_str
                    
                    # Build message showing changed vs total
                    if files_changed > 0:
                        self._backup_progress["message"] = f"{files_changed} changed, {total_files} total"
                    else:
                        self._backup_progress["message"] = f"No changes ({total_files} files checked)"
                else:
                    self._backup_progress["message"] = result.error_message or "Unknown error"
                    
            except Exception as e:
                self._backup_progress["done"] = True
                self._backup_progress["success"] = False
                self._backup_progress["message"] = str(e)
        
        def check_progress(_):
            """Timer callback to update UI from main thread."""
            if not self._backup_running:
                return
            
            progress = self._backup_progress
            
            if progress["done"]:
                # Backup finished - stop the timer
                self._backup_running = False
                self._backup_in_progress = False
                if hasattr(self, '_progress_timer') and self._progress_timer:
                    self._progress_timer.stop()
                
                if progress["success"]:
                    # Get file counts for display
                    files_changed = progress.get("files_changed", 0)
                    total_files = progress.get("total_files", 0)
                    duration = progress.get("duration_str", "")
                    
                    # Save last backup info
                    save_last_backup(files_changed, total_files, True)
                    
                    # Update menu bar status
                    self.status_item.title = "🟢 Backup complete"
                    if files_changed > 0:
                        self.last_backup_item.title = f"✓ Last: {files_changed} changed / {total_files} total"
                    else:
                        self.last_backup_item.title = f"✓ Last: No changes ({total_files} files)"
                    self.title = "✓"
                    
                    # Play success sound
                    subprocess.run(["afplay", "/System/Library/Sounds/Glass.aiff"], capture_output=True)
                    
                    # Show toast notification with all the details
                    if files_changed > 0:
                        rumps.notification(
                            title="✅ Backup Complete",
                            subtitle=f"{files_changed} files changed • {duration}",
                            message=f"{total_files} total files protected",
                            sound=False  # Already played sound above
                        )
                    else:
                        rumps.notification(
                            title="✅ Backup Complete", 
                            subtitle=f"No changes • {duration}",
                            message=f"{total_files} files checked, all up to date",
                            sound=False
                        )
                    
                    # Keep the completion message visible for 8 seconds
                    def show_completion():
                        import time
                        time.sleep(8)
                        self.update_status(None)
                    threading.Thread(target=show_completion, daemon=True).start()
                    return  # Don't reset immediately
                else:
                    # Save failed backup info
                    save_last_backup(0, 0, False)
                    
                    self.status_item.title = "🔴 Backup failed"
                    self.last_backup_item.title = "✕ Last backup failed: just now"
                    self.title = "✕"
                    
                    # Play error sound
                    subprocess.run(["afplay", "/System/Library/Sounds/Basso.aiff"], capture_output=True)
                    
                    # Show toast notification for failure (less intrusive than alert)
                    error_msg = progress.get("message", "Unknown error")
                    # Truncate long error messages for toast
                    if len(error_msg) > 100:
                        error_msg = error_msg[:97] + "..."
                    
                    rumps.notification(
                        title="❌ Backup Failed",
                        subtitle="Check your settings",
                        message=error_msg,
                        sound=False  # Already played sound above
                    )
                
                # Reset status after delay
                def reset_status():
                    import time
                    time.sleep(3)
                    self.update_status(None)
                threading.Thread(target=reset_status, daemon=True).start()
                
            else:
                # Still running - update progress display
                import time
                elapsed = int(time.time() - self._backup_start_time) if self._backup_start_time else 0
                
                if progress["percent"] is not None and progress["percent"] < 100:
                    self.status_item.title = f"🔵 Backing up... {progress['percent']}%"
                    self.title = f"{progress['percent']}%"
                elif progress["files"] > 0:
                    self.status_item.title = f"🔵 Copying {progress['files']} files... ({elapsed}s)"
                    self.title = "⟳"
                else:
                    self.status_item.title = f"🔵 Scanning files... ({elapsed}s)"
                    self.title = "⟳"
        
        # Start progress timer (updates every 0.5 seconds)
        self._progress_timer = rumps.Timer(check_progress, 0.5)
        self._progress_timer.start()
        
        # Start backup in background thread
        thread = threading.Thread(target=do_backup, daemon=True)
        thread.start()
    
    def browse_backups(self, _):
        """Open backup folder in Finder."""
        # Get destination from config
        try:
            from devbackup.config import parse_config
            config = parse_config()
            backup_path = config.backup_destination
            
            if backup_path.exists():
                subprocess.run(["open", str(backup_path)])
            else:
                rumps.alert(
                    title="Backup Folder Not Found",
                    message=f"The backup destination doesn't exist yet:\n{backup_path}\n\nRun a backup first to create it."
                )
        except Exception as e:
            rumps.alert(
                title="Cannot Open Backups",
                message=f"Could not find backup location: {e}"
            )
    
    def quit_app(self, _):
        """Quit the menu bar app."""
        rumps.quit_application()
    
    def toggle_autostart(self, sender):
        """Toggle auto-start at login."""
        if sender.state:
            # Currently enabled, disable it
            if disable_autostart():
                sender.state = False
                rumps.notification(
                    title="DevBackup",
                    subtitle="",
                    message="Auto-start at login disabled"
                )
            else:
                rumps.alert(
                    title="Error",
                    message="Failed to disable auto-start"
                )
        else:
            # Currently disabled, enable it
            if enable_autostart():
                sender.state = True
                rumps.notification(
                    title="DevBackup",
                    subtitle="",
                    message="DevBackup will now start automatically at login"
                )
            else:
                rumps.alert(
                    title="Error",
                    message="Failed to enable auto-start"
                )
    
    def open_preferences(self, _):
        """Open preferences/setup wizard."""
        # Launch preferences as a separate process
        subprocess.Popen(
            [sys.executable, "-m", "devbackup.preferences_ui"],
            start_new_session=True,
        )
        
        # After preferences close, check if config now exists and update
        def check_config():
            import time
            # Check periodically for config changes
            for _ in range(30):  # Check for up to 60 seconds
                time.sleep(2)
                if has_config():
                    # Config exists now - update UI
                    self.status_item.title = "🟢 Ready to back up"
                    self.title = "DB"
                    
                    # Start status update timer if not already running
                    if not hasattr(self, 'timer') or not self.timer:
                        self.timer = rumps.Timer(self.update_status, 30)
                        self.timer.start()
                    
                    self.update_status(None)
                    break
        
        thread = threading.Thread(target=check_config, daemon=True)
        thread.start()


def main():
    """Run the menu bar app."""
    app = DevBackupMenuBar()
    app.run()


if __name__ == "__main__":
    main()
