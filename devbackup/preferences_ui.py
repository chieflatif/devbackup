#!/usr/bin/env python3
"""
DevBackup Preferences UI

Uses native macOS dialogs via osascript.
Shows current settings and lets you change them one at a time.
"""

import subprocess
import sys
from pathlib import Path
from typing import List, Optional, Tuple

from devbackup.config import DEFAULT_CONFIG_PATH


# Backup interval options: (display text, seconds)
INTERVAL_OPTIONS = [
    ("Every 15 minutes", 900),
    ("Every 30 minutes", 1800),
    ("Every hour", 3600),
    ("Every 2 hours", 7200),
    ("Every 4 hours", 14400),
    ("Every 8 hours", 28800),
    ("Once a day", 86400),
]


def interval_to_text(seconds: int) -> str:
    """Convert interval in seconds to display text."""
    for text, secs in INTERVAL_OPTIONS:
        if secs == seconds:
            return text
    return "Every hour"


def text_to_interval(text: str) -> int:
    """Convert display text to interval in seconds."""
    for t, secs in INTERVAL_OPTIONS:
        if t == text:
            return secs
    return 3600


def load_config() -> Tuple[List[str], str, int]:
    """Load existing configuration. Returns (sources, destination, interval)."""
    sources = []
    destination = ""
    interval = 3600
    
    try:
        if DEFAULT_CONFIG_PATH.exists():
            from devbackup.config import parse_config
            config = parse_config(DEFAULT_CONFIG_PATH)
            sources = [str(p) for p in config.source_directories]
            destination = str(config.backup_destination)
            interval = config.scheduler.interval_seconds
    except Exception:
        pass
    
    return sources, destination, interval


def choose_folder(prompt: str) -> Optional[str]:
    """Open native folder picker."""
    script = f'''
    try
        set selectedFolder to choose folder with prompt "{prompt}"
        return POSIX path of selectedFolder
    on error
        return ""
    end try
    '''
    result = subprocess.run(['osascript', '-e', script], capture_output=True, text=True, timeout=300)
    path = result.stdout.strip().rstrip('/')
    return path if path else None


def choose_folders_multi(prompt: str) -> List[str]:
    """Open native folder picker with multiple selection."""
    script = f'''
    try
        set selectedFolders to choose folder with prompt "{prompt}" with multiple selections allowed
        set folderPaths to {{}}
        repeat with f in selectedFolders
            set end of folderPaths to POSIX path of f
        end repeat
        set AppleScript's text item delimiters to "|||"
        return folderPaths as text
    on error
        return ""
    end try
    '''
    result = subprocess.run(['osascript', '-e', script], capture_output=True, text=True, timeout=300)
    if result.stdout.strip():
        return [p.strip().rstrip('/') for p in result.stdout.strip().split('|||') if p.strip()]
    return []


def choose_interval() -> Optional[int]:
    """Show interval selection dialog."""
    options = [opt[0] for opt in INTERVAL_OPTIONS]
    options_str = '", "'.join(options)
    
    script = f'''
    set intervalOptions to {{"{options_str}"}}
    set selectedInterval to choose from list intervalOptions with prompt "How often should DevBackup check for changes?" with title "Backup Frequency" default items {{"Every hour"}}
    
    if selectedInterval is false then
        return ""
    else
        return item 1 of selectedInterval
    end if
    '''
    
    result = subprocess.run(['osascript', '-e', script], capture_output=True, text=True, timeout=300)
    choice = result.stdout.strip()
    
    if choice:
        return text_to_interval(choice)
    return None


def show_main_menu(sources: List[str], destination: str, interval: int) -> Optional[str]:
    """Show main preferences menu. Returns selected action or None."""
    
    # Format current settings
    if sources:
        folder_names = [Path(s).name for s in sources[:3]]
        source_text = ", ".join(folder_names)
        if len(sources) > 3:
            source_text += f" (+{len(sources)-3} more)"
    else:
        source_text = "Not set"
    
    dest_text = Path(destination).name if destination else "Not set"
    freq_text = interval_to_text(interval)
    
    # Use choose from list for menu
    script = f'''
    set menuItems to {{"📁 Folders: {source_text}", "💾 Destination: {dest_text}", "⏱ Frequency: {freq_text}", "───────────────", "✅ Save and Close", "❌ Cancel"}}
    
    set selectedItem to choose from list menuItems with prompt "DevBackup Preferences" & return & return & "Select an item to change it:" with title "DevBackup Preferences" default items {{"✅ Save and Close"}}
    
    if selectedItem is false then
        return "cancel"
    else
        return item 1 of selectedItem
    end if
    '''
    
    result = subprocess.run(['osascript', '-e', script], capture_output=True, text=True, timeout=300)
    
    if result.returncode != 0:
        return None
    
    return result.stdout.strip()


def write_config(sources: List[str], destination: str, interval_seconds: int = 3600) -> bool:
    """Write configuration to TOML file."""
    try:
        DEFAULT_CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
        
        source_lines = "\n".join(f'    "{s}",' for s in sources)
        
        config_content = f'''# DevBackup Configuration

[main]
backup_destination = "{destination}"
source_directories = [
{source_lines}
]
exclude_patterns = [
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
    ".venv/",
    "venv/",
    ".idea/",
    ".vscode/",
]

[scheduler]
type = "launchd"
interval_seconds = {interval_seconds}

[retention]
hourly = 24
daily = 7
weekly = 4

[logging]
level = "INFO"
log_file = "~/.local/log/devbackup.log"
error_log_file = "~/.local/log/devbackup.err"
log_max_size_mb = 10
log_backup_count = 5

[mcp]
enabled = true
port = 0

[discovery]
scan_depth = 5

[retry]
retry_count = 3
retry_delay_seconds = 5.0
rsync_timeout_seconds = 3600

[notifications]
notify_on_success = false
notify_on_failure = true
'''
        
        DEFAULT_CONFIG_PATH.write_text(config_content)
        return True
    except Exception as e:
        show_alert("Error", f"Failed to save: {e}")
        return False


def show_alert(title: str, message: str):
    """Show alert dialog."""
    # Escape quotes and newlines
    message = message.replace('"', '\\"').replace('\n', '\\n')
    script = f'display dialog "{message}" with title "{title}" buttons {{"OK"}} default button "OK"'
    subprocess.run(['osascript', '-e', script], capture_output=True)


def show_preferences():
    """Main entry point - show preferences dialog."""
    # Load current config
    sources, destination, interval = load_config()
    
    while True:
        choice = show_main_menu(sources, destination, interval)
        
        if choice is None or choice == "cancel" or "Cancel" in choice:
            return
        
        if "Save" in choice:
            # Validate
            if not sources:
                show_alert("Missing Setting", "Please select folders to back up.")
                continue
            if not destination:
                show_alert("Missing Setting", "Please select a backup destination.")
                continue
            
            # Save
            if write_config(sources, destination, interval):
                show_alert("Saved", f"Configuration saved!\\n\\nBackups will check for changes {interval_to_text(interval).lower()}.")
            return
        
        if "Folders" in choice:
            new_sources = choose_folders_multi("Select folders to back up (Cmd+click for multiple)")
            if new_sources:
                sources = new_sources
        
        elif "Destination" in choice:
            new_dest = choose_folder("Select backup destination")
            if new_dest:
                destination = new_dest
        
        elif "Frequency" in choice:
            new_interval = choose_interval()
            if new_interval:
                interval = new_interval
        
        # Loop back to show updated menu


if __name__ == "__main__":
    show_preferences()
