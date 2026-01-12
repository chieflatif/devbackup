"""Health check system for devbackup.

Provides health checks for backup snapshots to verify integrity and detect issues.

Requirements: 12.1-12.6
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional
import logging
import re

from devbackup.verify import IntegrityVerifier

logger = logging.getLogger(__name__)


@dataclass
class SnapshotHealth:
    """Health status of a single snapshot.
    
    Requirements: 12.2, 12.3
    """
    snapshot_name: str
    timestamp: Optional[datetime]
    readable: bool
    has_manifest: bool
    manifest_valid: bool
    file_count: int = 0
    corrupted_files: List[str] = field(default_factory=list)
    missing_files: List[str] = field(default_factory=list)
    error: Optional[str] = None


@dataclass
class HealthCheckResult:
    """Result of health check across all snapshots.
    
    Requirements: 12.4, 12.6
    """
    total_snapshots: int
    healthy_snapshots: int
    unhealthy_snapshots: int
    snapshots: List[SnapshotHealth] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)


class HealthChecker:
    """
    Performs health checks on backup snapshots.
    
    Requirements: 12.2, 12.3, 12.4, 12.6
    """
    
    # Pattern for snapshot directory names: YYYY-MM-DD-HHMMSS or YYYY-MM-DD-HHMMSS-NN
    SNAPSHOT_PATTERN = re.compile(r'^(\d{4}-\d{2}-\d{2}-\d{6})(?:-(\d{2}))?$')
    
    def __init__(self, destination: Path, verifier: Optional[IntegrityVerifier] = None):
        """
        Initialize health checker.
        
        Args:
            destination: Backup destination directory
            verifier: IntegrityVerifier instance (created if not provided)
        """
        self.destination = destination
        self.verifier = verifier or IntegrityVerifier()
    
    def check_all(
        self,
        min_age_days: Optional[int] = None,
    ) -> HealthCheckResult:
        """
        Check health of all snapshots.
        
        Args:
            min_age_days: Only check snapshots older than N days (None = check all)
        
        Returns:
            HealthCheckResult with status of all snapshots
        
        Requirements: 12.4, 12.6
        """
        snapshots: List[SnapshotHealth] = []
        errors: List[str] = []
        
        if not self.destination.exists():
            errors.append(f"Destination does not exist: {self.destination}")
            return HealthCheckResult(
                total_snapshots=0,
                healthy_snapshots=0,
                unhealthy_snapshots=0,
                snapshots=[],
                errors=errors,
            )
        
        # Find all snapshot directories
        snapshot_dirs = self._find_snapshots()
        
        # Filter by age if specified
        if min_age_days is not None:
            cutoff = datetime.now() - timedelta(days=min_age_days)
            filtered_dirs = []
            for snapshot_dir in snapshot_dirs:
                timestamp = self._parse_timestamp(snapshot_dir.name)
                if timestamp and timestamp < cutoff:
                    filtered_dirs.append(snapshot_dir)
            snapshot_dirs = filtered_dirs
        
        # Check each snapshot
        for snapshot_dir in snapshot_dirs:
            try:
                health = self.check_snapshot(snapshot_dir)
                snapshots.append(health)
            except Exception as e:
                errors.append(f"Error checking {snapshot_dir.name}: {e}")
                snapshots.append(SnapshotHealth(
                    snapshot_name=snapshot_dir.name,
                    timestamp=self._parse_timestamp(snapshot_dir.name),
                    readable=False,
                    has_manifest=False,
                    manifest_valid=False,
                    error=str(e),
                ))
        
        # Count healthy/unhealthy
        healthy = sum(1 for s in snapshots if self._is_healthy(s))
        unhealthy = len(snapshots) - healthy
        
        return HealthCheckResult(
            total_snapshots=len(snapshots),
            healthy_snapshots=healthy,
            unhealthy_snapshots=unhealthy,
            snapshots=snapshots,
            errors=errors,
        )
    
    def check_snapshot(self, snapshot_path: Path) -> SnapshotHealth:
        """
        Check health of a single snapshot.
        
        Args:
            snapshot_path: Path to snapshot directory
        
        Returns:
            SnapshotHealth with status of the snapshot
        
        Requirements: 12.2, 12.3
        """
        snapshot_name = snapshot_path.name
        timestamp = self._parse_timestamp(snapshot_name)
        
        # Check if readable
        readable = snapshot_path.exists() and snapshot_path.is_dir()
        if not readable:
            return SnapshotHealth(
                snapshot_name=snapshot_name,
                timestamp=timestamp,
                readable=False,
                has_manifest=False,
                manifest_valid=False,
                error="Snapshot directory not readable",
            )
        
        # Check for manifest
        manifest = self.verifier.load_manifest(snapshot_path)
        has_manifest = manifest is not None
        
        if not has_manifest:
            # No manifest - can't verify integrity, but snapshot exists
            return SnapshotHealth(
                snapshot_name=snapshot_name,
                timestamp=timestamp,
                readable=True,
                has_manifest=False,
                manifest_valid=False,
            )
        
        # Verify integrity using manifest
        verification = self.verifier.verify_snapshot(snapshot_path)
        
        return SnapshotHealth(
            snapshot_name=snapshot_name,
            timestamp=timestamp,
            readable=True,
            has_manifest=True,
            manifest_valid=verification.success,
            file_count=verification.files_verified,
            corrupted_files=verification.corrupted_files,
            missing_files=verification.missing_files,
            error=verification.errors[0] if verification.errors else None,
        )
    
    def _find_snapshots(self) -> List[Path]:
        """Find all snapshot directories in destination."""
        snapshots = []
        try:
            for item in self.destination.iterdir():
                if item.is_dir() and self.SNAPSHOT_PATTERN.match(item.name):
                    # Skip in_progress directories
                    if not item.name.endswith('_in_progress'):
                        snapshots.append(item)
        except PermissionError:
            logger.warning(f"Permission denied reading {self.destination}")
        
        # Sort by name (chronological order)
        snapshots.sort(key=lambda p: p.name)
        return snapshots
    
    def _parse_timestamp(self, name: str) -> Optional[datetime]:
        """Parse timestamp from snapshot name."""
        match = self.SNAPSHOT_PATTERN.match(name)
        if not match:
            return None
        
        base_timestamp = match.group(1)
        try:
            return datetime.strptime(base_timestamp, "%Y-%m-%d-%H%M%S")
        except ValueError:
            return None
    
    def _is_healthy(self, health: SnapshotHealth) -> bool:
        """Determine if a snapshot is healthy."""
        if not health.readable:
            return False
        if health.has_manifest and not health.manifest_valid:
            return False
        if health.corrupted_files or health.missing_files:
            return False
        return True
