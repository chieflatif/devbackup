"""Space validation for devbackup.

This module provides disk space validation before backup operations to ensure
sufficient space is available at the destination.

Requirements: 2.1, 2.2, 2.3, 2.4, 2.5
"""

from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional
import fnmatch
import os
import shutil


class SpaceError(Exception):
    """Raised when insufficient disk space is available.
    
    Requirements: 2.2
    """
    
    def __init__(self, message: str, available_bytes: int, required_bytes: int):
        super().__init__(message)
        self.available_bytes = available_bytes
        self.required_bytes = required_bytes


@dataclass
class SpaceValidationResult:
    """Result of space validation.
    
    Attributes:
        sufficient: Whether there is enough space for the backup
        available_bytes: Available disk space at destination
        estimated_bytes: Estimated size of the backup
        warning: Optional warning message (e.g., low disk space)
    """
    sufficient: bool
    available_bytes: int
    estimated_bytes: int
    warning: Optional[str] = None


def _matches_exclude_pattern(path: Path, patterns: List[str], base_path: Path) -> bool:
    """Check if a path matches any exclude pattern.
    
    Args:
        path: Path to check
        patterns: List of rsync-style exclude patterns
        base_path: Base path for relative pattern matching
    
    Returns:
        True if the path matches any exclude pattern
    """
    # Get relative path for pattern matching
    try:
        rel_path = path.relative_to(base_path)
        rel_str = str(rel_path)
    except ValueError:
        rel_str = str(path)
    
    # Also check just the filename
    filename = path.name
    
    for pattern in patterns:
        # Handle directory patterns (ending with /)
        pattern_clean = pattern.rstrip('/')
        is_dir_pattern = pattern.endswith('/')
        
        # Check if pattern matches filename
        if fnmatch.fnmatch(filename, pattern_clean):
            if is_dir_pattern and path.is_dir():
                return True
            elif not is_dir_pattern:
                return True
        
        # Check if pattern matches relative path
        if fnmatch.fnmatch(rel_str, pattern_clean):
            if is_dir_pattern and path.is_dir():
                return True
            elif not is_dir_pattern:
                return True
        
        # Check if any path component matches (for patterns like "node_modules/")
        for part in rel_path.parts if 'rel_path' in dir() else []:
            if fnmatch.fnmatch(part, pattern_clean):
                if is_dir_pattern and path.is_dir():
                    return True
                elif not is_dir_pattern:
                    return True
    
    return False


def estimate_backup_size(
    sources: List[Path],
    exclude_patterns: List[str],
) -> int:
    """Estimate the size of a backup by calculating source sizes minus exclusions.
    
    Walks through all source directories, summing file sizes while respecting
    exclude patterns. Does not follow symbolic links to prevent infinite loops.
    
    Args:
        sources: List of source directories to back up
        exclude_patterns: Patterns to exclude from size calculation
    
    Returns:
        Estimated size in bytes
    
    Requirements: 2.3
    """
    total_size = 0
    
    for source in sources:
        if not source.exists():
            continue
        
        if source.is_file():
            # Single file source
            if not _matches_exclude_pattern(source, exclude_patterns, source.parent):
                try:
                    total_size += source.stat().st_size
                except OSError:
                    pass
            continue
        
        # Walk directory tree without following symlinks
        # Requirements: 3.5 (symlink safety)
        for root, dirs, files in os.walk(source, followlinks=False):
            root_path = Path(root)
            
            # Filter out excluded directories (modifying dirs in-place)
            dirs[:] = [
                d for d in dirs
                if not _matches_exclude_pattern(root_path / d, exclude_patterns, source)
            ]
            
            # Sum file sizes, excluding matched patterns
            for filename in files:
                file_path = root_path / filename
                
                if _matches_exclude_pattern(file_path, exclude_patterns, source):
                    continue
                
                try:
                    # Use lstat to not follow symlinks
                    stat_info = file_path.lstat()
                    # Only count regular files (not symlinks)
                    if not file_path.is_symlink():
                        total_size += stat_info.st_size
                except OSError:
                    # Skip files we can't stat
                    continue
    
    return total_size


def validate_space(
    destination: Path,
    sources: List[Path],
    exclude_patterns: List[str],
    buffer_percent: float = 0.1,
    min_free_bytes: int = 1024 * 1024 * 1024,  # 1GB
) -> SpaceValidationResult:
    """Validate that sufficient disk space is available for backup.
    
    Checks available space at the destination against the estimated backup size
    plus a configurable buffer. Also warns if free space is below a minimum
    threshold regardless of backup size.
    
    Args:
        destination: Backup destination path
        sources: Source directories to back up
        exclude_patterns: Patterns to exclude
        buffer_percent: Additional buffer as fraction (default 10%)
        min_free_bytes: Minimum free space warning threshold (default 1GB)
    
    Returns:
        SpaceValidationResult with validation status
    
    Raises:
        SpaceError: If insufficient space is available
    
    Requirements: 2.1, 2.2, 2.4, 2.5
    """
    # Get available space at destination
    # If destination doesn't exist, check parent directory
    check_path = destination
    while not check_path.exists() and check_path.parent != check_path:
        check_path = check_path.parent
    
    try:
        disk_usage = shutil.disk_usage(check_path)
        available_bytes = disk_usage.free
    except OSError as e:
        raise SpaceError(
            f"Cannot determine available space at {destination}: {e}",
            available_bytes=0,
            required_bytes=0,
        )
    
    # Estimate backup size
    estimated_bytes = estimate_backup_size(sources, exclude_patterns)
    
    # Calculate required space with buffer
    required_bytes = int(estimated_bytes * (1 + buffer_percent))
    
    # Check for minimum free space warning
    warning = None
    if available_bytes < min_free_bytes:
        warning = (
            f"Low disk space warning: only {available_bytes / (1024**3):.2f}GB "
            f"free at destination (minimum recommended: {min_free_bytes / (1024**3):.2f}GB)"
        )
    
    # Check if we have enough space
    if available_bytes < required_bytes:
        raise SpaceError(
            f"Insufficient disk space: {available_bytes / (1024**3):.2f}GB available, "
            f"{required_bytes / (1024**3):.2f}GB required "
            f"(estimated {estimated_bytes / (1024**3):.2f}GB + {buffer_percent*100:.0f}% buffer)",
            available_bytes=available_bytes,
            required_bytes=required_bytes,
        )
    
    return SpaceValidationResult(
        sufficient=True,
        available_bytes=available_bytes,
        estimated_bytes=estimated_bytes,
        warning=warning,
    )
