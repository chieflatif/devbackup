"""Destination validation for devbackup.

This module provides functions to validate that the backup destination
is available, writable, and mounted (for removable drives).
"""

import os
import stat
from pathlib import Path
from typing import Optional


class DestinationError(Exception):
    """Raised when backup destination is invalid."""
    pass


def validate_destination(destination: Path) -> None:
    """
    Validate backup destination is available and writable.
    
    Checks:
    1. Path exists
    2. Path is writable
    3. Volume is mounted (for removable drives)
    
    Args:
        destination: Path to the backup destination directory
    
    Raises:
        DestinationError: If destination doesn't exist, isn't writable,
                         or volume isn't mounted
    """
    # Convert to Path if string
    if isinstance(destination, str):
        destination = Path(destination)
    
    # Check if volume is mounted (for paths on /Volumes)
    if not is_volume_mounted(destination):
        raise DestinationError(
            f"Destination not found: {destination} (volume may not be mounted)"
        )
    
    # Check if path exists
    if not destination.exists():
        raise DestinationError(f"Destination not found: {destination}")
    
    # Check if path is a directory
    if not destination.is_dir():
        raise DestinationError(
            f"Destination is not a directory: {destination}"
        )
    
    # Check if path is writable
    if not is_writable(destination):
        raise DestinationError(f"Destination not writable: {destination}")


def is_volume_mounted(path: Path) -> bool:
    """
    Check if path is on a mounted volume (for removable drives).
    
    For paths under /Volumes/, checks if the volume mount point exists.
    For other paths, returns True (assumes local filesystem).
    
    Args:
        path: Path to check
    
    Returns:
        True if volume is mounted or path is not on /Volumes/
    """
    # Convert to Path if string
    if isinstance(path, str):
        path = Path(path)
    
    # Resolve to absolute path
    try:
        abs_path = path.resolve()
    except OSError:
        # If we can't resolve, check if it's under /Volumes
        abs_path = path
    
    path_str = str(abs_path)
    
    # Check if path is under /Volumes/
    if path_str.startswith("/Volumes/"):
        # Extract the volume name (first component after /Volumes/)
        parts = path_str.split("/")
        if len(parts) >= 3:
            volume_name = parts[2]
            volume_path = Path(f"/Volumes/{volume_name}")
            # Check if the volume mount point exists and is a directory
            return volume_path.exists() and volume_path.is_dir()
    
    # For non-/Volumes/ paths, assume mounted (local filesystem)
    return True


def is_writable(path: Path) -> bool:
    """
    Check if path is writable.
    
    Uses os.access to check write permission, and also attempts
    to create a temporary file as a more reliable check.
    
    Args:
        path: Path to check
    
    Returns:
        True if path is writable
    """
    # Convert to Path if string
    if isinstance(path, str):
        path = Path(path)
    
    # First check using os.access
    if not os.access(path, os.W_OK):
        return False
    
    # Also try to actually write a test file for more reliable check
    test_file = path / ".devbackup_write_test"
    try:
        test_file.touch()
        test_file.unlink()
        return True
    except (OSError, PermissionError):
        return False


def get_available_space(path: Path) -> int:
    """
    Return available space in bytes at path.
    
    Args:
        path: Path to check
    
    Returns:
        Available space in bytes
    
    Raises:
        DestinationError: If unable to determine available space
    """
    # Convert to Path if string
    if isinstance(path, str):
        path = Path(path)
    
    try:
        stat_result = os.statvfs(path)
        # f_bavail = free blocks available to non-superuser
        # f_frsize = fragment size
        return stat_result.f_bavail * stat_result.f_frsize
    except OSError as e:
        raise DestinationError(
            f"Unable to determine available space at {path}: {e}"
        )
