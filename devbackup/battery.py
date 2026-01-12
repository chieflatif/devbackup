"""Battery status checking for devbackup.

This module provides functions to check battery level and charging status
on macOS for smart scheduling decisions.

Requirements: 8.3 - Skip backups when battery is below 20% and not charging
"""

import subprocess
from dataclasses import dataclass
from typing import Optional


class BatteryError(Exception):
    """Raised when battery status cannot be determined."""
    pass


@dataclass
class BatteryStatus:
    """Battery status information."""
    level: int  # Battery percentage (0-100)
    is_charging: bool  # True if connected to power
    is_present: bool  # True if battery is present (False for desktops)
    
    def should_skip_backup(self, threshold: int = 20) -> bool:
        """
        Determine if backup should be skipped based on battery status.
        
        Args:
            threshold: Minimum battery level to allow backup (default 20%)
        
        Returns:
            True if backup should be skipped (low battery and not charging)
        """
        # If no battery (desktop Mac), never skip
        if not self.is_present:
            return False
        
        # If charging, never skip
        if self.is_charging:
            return False
        
        # Skip if below threshold
        return self.level < threshold


def get_battery_status() -> BatteryStatus:
    """
    Get current battery status on macOS.
    
    Uses pmset command to get battery information.
    
    Returns:
        BatteryStatus with current level and charging state
    
    Raises:
        BatteryError: If battery status cannot be determined
    """
    try:
        result = subprocess.run(
            ["pmset", "-g", "batt"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        
        if result.returncode != 0:
            raise BatteryError(f"pmset command failed: {result.stderr}")
        
        return _parse_pmset_output(result.stdout)
        
    except subprocess.TimeoutExpired:
        raise BatteryError("Timeout getting battery status")
    except FileNotFoundError:
        # pmset not found - likely not macOS or testing environment
        # Return a safe default (assume plugged in)
        return BatteryStatus(level=100, is_charging=True, is_present=False)
    except Exception as e:
        raise BatteryError(f"Error getting battery status: {e}")


def _parse_pmset_output(output: str) -> BatteryStatus:
    """
    Parse pmset -g batt output to extract battery status.
    
    Example output:
    Now drawing from 'Battery Power'
     -InternalBattery-0 (id=1234567)	75%; discharging; 3:45 remaining present: true
    
    Or when charging:
    Now drawing from 'AC Power'
     -InternalBattery-0 (id=1234567)	85%; charging; 1:30 remaining present: true
    
    Or for desktop (no battery):
    Now drawing from 'AC Power'
    
    Args:
        output: Raw output from pmset -g batt
    
    Returns:
        BatteryStatus parsed from output
    """
    lines = output.strip().split('\n')
    
    # Check if drawing from AC Power (first line)
    is_on_ac = 'AC Power' in lines[0] if lines else False
    
    # Look for battery line
    battery_line = None
    for line in lines[1:]:
        if 'InternalBattery' in line or '%' in line:
            battery_line = line
            break
    
    # No battery found - desktop Mac
    if battery_line is None:
        return BatteryStatus(level=100, is_charging=True, is_present=False)
    
    # Parse battery percentage
    level = _extract_percentage(battery_line)
    
    # Parse charging status
    is_charging = _is_charging(battery_line, is_on_ac)
    
    return BatteryStatus(level=level, is_charging=is_charging, is_present=True)


def _extract_percentage(line: str) -> int:
    """
    Extract battery percentage from pmset output line.
    
    Args:
        line: Line containing battery info
    
    Returns:
        Battery percentage (0-100)
    """
    import re
    
    # Look for pattern like "75%"
    match = re.search(r'(\d+)%', line)
    if match:
        return int(match.group(1))
    
    # Default to 100 if can't parse
    return 100


def _is_charging(line: str, is_on_ac: bool) -> bool:
    """
    Determine if battery is charging from pmset output.
    
    Args:
        line: Line containing battery info
        is_on_ac: Whether drawing from AC power
    
    Returns:
        True if charging or connected to power
    """
    line_lower = line.lower()
    
    # Explicit charging status
    if 'charging' in line_lower and 'discharging' not in line_lower:
        return True
    
    # "charged" means fully charged and on AC
    if 'charged' in line_lower:
        return True
    
    # "finishing charge" means almost done charging
    if 'finishing charge' in line_lower:
        return True
    
    # If on AC power but not explicitly discharging, consider it charging
    if is_on_ac and 'discharging' not in line_lower:
        return True
    
    return False


def check_battery_for_backup(threshold: int = 20) -> tuple[bool, str]:
    """
    Check if battery status allows backup to proceed.
    
    Args:
        threshold: Minimum battery level to allow backup (default 20%)
    
    Returns:
        Tuple of (should_proceed, reason_message)
        - should_proceed: True if backup can proceed
        - reason_message: Human-readable explanation
    """
    try:
        status = get_battery_status()
        
        if not status.is_present:
            return True, "No battery detected (desktop Mac)"
        
        if status.is_charging:
            return True, f"Battery at {status.level}% and charging"
        
        if status.level >= threshold:
            return True, f"Battery at {status.level}%"
        
        return False, (
            f"Battery at {status.level}% and not charging. "
            f"Backup skipped to preserve battery (threshold: {threshold}%)"
        )
        
    except BatteryError as e:
        # If we can't determine battery status, allow backup to proceed
        # This is a safe default - better to backup than skip
        return True, f"Could not determine battery status: {e}"
