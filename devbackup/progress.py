"""Progress reporting for devbackup.

This module provides the ProgressReporter class that parses rsync output
and reports backup progress in real-time.

Requirements: 6.1, 6.2, 6.3, 6.4, 6.5
"""

from dataclasses import dataclass, field
from typing import Callable, Optional
import re


@dataclass
class ProgressInfo:
    """Current backup progress information.
    
    Requirements: 6.4 - Report files transferred, total files, bytes transferred, transfer rate
    """
    files_transferred: int = 0
    total_files: Optional[int] = None
    bytes_transferred: int = 0
    total_bytes: Optional[int] = None
    transfer_rate: float = 0.0  # bytes per second
    current_file: Optional[str] = None
    percent_complete: Optional[float] = None


class ProgressReporter:
    """
    Reports backup progress by parsing rsync output.
    
    Uses rsync's --info=progress2 for real-time progress updates.
    
    Requirements: 6.1, 6.4, 6.5
    """
    
    # Regex pattern for rsync --info=progress2 output
    # Format: "1,234,567  12%  123.45kB/s  0:01:23"
    # Or: "         1,234,567 100%  123.45MB/s    0:00:01 (xfr#1, to-chk=99/100)"
    PROGRESS_PATTERN = re.compile(
        r'^\s*'
        r'(?P<bytes>[\d,]+)\s+'
        r'(?P<percent>\d+)%\s+'
        r'(?P<rate>[\d.]+)(?P<rate_unit>[kKMG]?B)/s\s+'
        r'(?P<time>\d+:\d+:\d+)'
        r'(?:\s+\(xfr#(?P<xfr>\d+),\s*to-chk=(?P<to_chk>\d+)/(?P<total>\d+)\))?'
    )
    
    # Regex pattern for rsync --progress output (per-file progress)
    # Format: "             13 100%  436.46KB/s   00:00:00 (xfer#1, to-check=0/1)"
    PROGRESS_PER_FILE_PATTERN = re.compile(
        r'^\s*'
        r'(?P<bytes>[\d,]+)\s+'
        r'(?P<percent>\d+)%\s+'
        r'(?P<rate>[\d.]+)(?P<rate_unit>[kKMG]?B)/s\s+'
        r'(?P<time>\d+:\d+:\d+)'
        r'(?:\s+\(xfer#(?P<xfr>\d+),\s*to-check=(?P<to_chk>\d+)/(?P<total>\d+)\))?'
    )
    
    # Rate unit multipliers
    RATE_MULTIPLIERS = {
        'B': 1,
        'kB': 1024,
        'KB': 1024,
        'MB': 1024 * 1024,
        'GB': 1024 * 1024 * 1024,
    }
    
    def __init__(self, callback: Optional[Callable[[ProgressInfo], None]] = None):
        """
        Initialize the progress reporter.
        
        Args:
            callback: Optional callback function called with ProgressInfo on each update
        """
        self.callback = callback
        self._current_progress = ProgressInfo()
        self._files_seen: int = 0
    
    def parse_rsync_output(self, line: str) -> Optional[ProgressInfo]:
        """
        Parse a line of rsync output and update progress.
        
        Handles --info=progress2 format:
        "1,234,567  12%  123.45kB/s  0:01:23"
        "1,234,567 100%  123.45MB/s    0:00:01 (xfr#1, to-chk=99/100)"
        
        Args:
            line: A line of rsync output
        
        Returns:
            Updated ProgressInfo if progress was parsed, None otherwise
        
        Requirements: 6.1, 6.4
        """
        line = line.strip()
        if not line:
            return None
        
        # Try to match progress2 format
        match = self.PROGRESS_PATTERN.match(line)
        if match:
            groups = match.groupdict()
            
            # Parse bytes transferred (remove commas)
            bytes_str = groups['bytes'].replace(',', '')
            bytes_transferred = int(bytes_str)
            
            # Parse percent
            percent = int(groups['percent'])
            
            # Parse transfer rate
            rate_value = float(groups['rate'])
            rate_unit = groups['rate_unit']
            multiplier = self.RATE_MULTIPLIERS.get(rate_unit, 1)
            transfer_rate = rate_value * multiplier
            
            # Parse file counts if available (xfr#N, to-chk=M/T)
            files_transferred = None
            total_files = None
            if groups.get('xfr'):
                files_transferred = int(groups['xfr'])
            if groups.get('total'):
                total_files = int(groups['total'])
                if groups.get('to_chk'):
                    to_check = int(groups['to_chk'])
                    # Files transferred = total - remaining
                    files_transferred = total_files - to_check
            
            # Calculate total bytes from percent if we have it
            total_bytes = None
            if percent > 0 and bytes_transferred > 0:
                total_bytes = int(bytes_transferred * 100 / percent)
            
            # Update current progress
            self._current_progress = ProgressInfo(
                files_transferred=files_transferred if files_transferred is not None else self._files_seen,
                total_files=total_files,
                bytes_transferred=bytes_transferred,
                total_bytes=total_bytes,
                transfer_rate=transfer_rate,
                current_file=self._current_progress.current_file,
                percent_complete=float(percent),
            )
            
            # Call callback if provided
            if self.callback:
                self.callback(self._current_progress)
            
            return self._current_progress
        
        # Check if this is a file being transferred (verbose output)
        # Lines that don't start with special prefixes are usually file names
        if not line.startswith(('sending', 'sent', 'total', 'building', 'receiving', 
                                'created', 'deleting', 'rsync', 'Number', 'Total',
                                'Literal', 'Matched', 'File', 'cannot', 'skipping')):
            # This might be a filename being transferred
            self._files_seen += 1
            self._current_progress.current_file = line
            self._current_progress.files_transferred = self._files_seen
            
            if self.callback:
                self.callback(self._current_progress)
            
            return self._current_progress
        
        return None
    
    def get_current_progress(self) -> ProgressInfo:
        """
        Return current progress information.
        
        Returns:
            Current ProgressInfo object
        """
        return self._current_progress
    
    def report_final(self, files_transferred: int, total_size: int, duration_seconds: float) -> ProgressInfo:
        """
        Report final backup statistics.
        
        Args:
            files_transferred: Total files transferred
            total_size: Total bytes transferred
            duration_seconds: Total backup duration
        
        Returns:
            Final ProgressInfo with complete statistics
        
        Requirements: 6.5
        """
        # Calculate average transfer rate
        transfer_rate = total_size / duration_seconds if duration_seconds > 0 else 0.0
        
        self._current_progress = ProgressInfo(
            files_transferred=files_transferred,
            total_files=files_transferred,  # Final count
            bytes_transferred=total_size,
            total_bytes=total_size,
            transfer_rate=transfer_rate,
            current_file=None,
            percent_complete=100.0,
        )
        
        if self.callback:
            self.callback(self._current_progress)
        
        return self._current_progress
    
    def reset(self) -> None:
        """Reset progress tracking for a new backup."""
        self._current_progress = ProgressInfo()
        self._files_seen = 0
