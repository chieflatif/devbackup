"""Integrity verification for devbackup snapshots.

This module provides the IntegrityVerifier class for creating and verifying
backup manifests using SHA-256 checksums.

Requirements: 7.1-7.6
"""

import hashlib
import json
import fnmatch
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional


@dataclass
class FileChecksum:
    """Checksum information for a single file."""
    path: str
    size: int
    mtime: float
    sha256: str


@dataclass
class Manifest:
    """Backup manifest containing file checksums."""
    snapshot_name: str
    created_at: str
    file_count: int
    total_size: int
    checksums: List[FileChecksum]


@dataclass
class VerificationResult:
    """Result of backup verification."""
    success: bool
    files_verified: int
    files_failed: int
    missing_files: List[str]
    corrupted_files: List[str]
    errors: List[str]


class IntegrityVerifier:
    """
    Verifies backup integrity using SHA-256 checksums.
    
    Requirements: 7.1, 7.2, 7.5, 7.6
    """
    
    MANIFEST_FILENAME = ".devbackup_manifest.json"
    
    def create_manifest(self, snapshot_path: Path) -> Manifest:
        """
        Create a manifest file for a snapshot.
        
        Calculates SHA-256 checksums for all files in the snapshot.
        
        Args:
            snapshot_path: Path to the snapshot directory
        
        Returns:
            Manifest object with all file checksums
        
        Requirements: 7.1, 7.2
        """
        checksums: List[FileChecksum] = []
        total_size = 0
        
        for file_path in snapshot_path.rglob("*"):
            if file_path.is_file() and file_path.name != self.MANIFEST_FILENAME:
                try:
                    stat = file_path.stat()
                    checksum = self._calculate_checksum(file_path)
                    relative_path = str(file_path.relative_to(snapshot_path))
                    
                    checksums.append(FileChecksum(
                        path=relative_path,
                        size=stat.st_size,
                        mtime=stat.st_mtime,
                        sha256=checksum,
                    ))
                    total_size += stat.st_size
                except (OSError, IOError):
                    # Skip files that can't be read
                    pass
        
        return Manifest(
            snapshot_name=snapshot_path.name,
            created_at=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            file_count=len(checksums),
            total_size=total_size,
            checksums=checksums,
        )
    
    def save_manifest(self, manifest: Manifest, snapshot_path: Path) -> None:
        """
        Save manifest to snapshot directory.
        
        Args:
            manifest: Manifest object to save
            snapshot_path: Path to the snapshot directory
        """
        manifest_path = snapshot_path / self.MANIFEST_FILENAME
        
        # Convert to dict for JSON serialization
        data = {
            "snapshot_name": manifest.snapshot_name,
            "created_at": manifest.created_at,
            "file_count": manifest.file_count,
            "total_size": manifest.total_size,
            "checksums": [asdict(c) for c in manifest.checksums],
        }
        
        with open(manifest_path, "w") as f:
            json.dump(data, f, indent=2)
    
    def load_manifest(self, snapshot_path: Path) -> Optional[Manifest]:
        """
        Load manifest from snapshot directory.
        
        Args:
            snapshot_path: Path to the snapshot directory
        
        Returns:
            Manifest object if found, None otherwise
        """
        manifest_path = snapshot_path / self.MANIFEST_FILENAME
        
        if not manifest_path.exists():
            return None
        
        try:
            with open(manifest_path, "r") as f:
                data = json.load(f)
            
            checksums = [
                FileChecksum(
                    path=c["path"],
                    size=c["size"],
                    mtime=c["mtime"],
                    sha256=c["sha256"],
                )
                for c in data.get("checksums", [])
            ]
            
            return Manifest(
                snapshot_name=data["snapshot_name"],
                created_at=data["created_at"],
                file_count=data["file_count"],
                total_size=data["total_size"],
                checksums=checksums,
            )
        except (json.JSONDecodeError, KeyError, OSError):
            return None
    
    def verify_snapshot(
        self,
        snapshot_path: Path,
        pattern: Optional[str] = None,
    ) -> VerificationResult:
        """
        Verify snapshot integrity against its manifest.
        
        Args:
            snapshot_path: Path to snapshot to verify
            pattern: Optional glob pattern to filter files
        
        Returns:
            VerificationResult with verification status
        
        Requirements: 7.5, 7.6
        """
        manifest = self.load_manifest(snapshot_path)
        
        if manifest is None:
            return VerificationResult(
                success=False,
                files_verified=0,
                files_failed=0,
                missing_files=[],
                corrupted_files=[],
                errors=["Manifest file not found"],
            )
        
        missing_files: List[str] = []
        corrupted_files: List[str] = []
        errors: List[str] = []
        files_verified = 0
        
        for file_checksum in manifest.checksums:
            # Apply pattern filter if specified
            if pattern and not fnmatch.fnmatch(file_checksum.path, pattern):
                continue
            
            file_path = snapshot_path / file_checksum.path
            
            if not file_path.exists():
                missing_files.append(file_checksum.path)
                continue
            
            try:
                current_checksum = self._calculate_checksum(file_path)
                if current_checksum != file_checksum.sha256:
                    corrupted_files.append(file_checksum.path)
                else:
                    files_verified += 1
            except (OSError, IOError) as e:
                errors.append(f"Error reading {file_checksum.path}: {e}")
        
        success = len(missing_files) == 0 and len(corrupted_files) == 0 and len(errors) == 0
        
        return VerificationResult(
            success=success,
            files_verified=files_verified,
            files_failed=len(missing_files) + len(corrupted_files),
            missing_files=missing_files,
            corrupted_files=corrupted_files,
            errors=errors,
        )
    
    def _calculate_checksum(self, file_path: Path) -> str:
        """
        Calculate SHA-256 checksum of a file.
        
        Args:
            file_path: Path to the file
        
        Returns:
            Hex-encoded SHA-256 checksum
        """
        sha256_hash = hashlib.sha256()
        
        with open(file_path, "rb") as f:
            # Read in chunks to handle large files
            for chunk in iter(lambda: f.read(8192), b""):
                sha256_hash.update(chunk)
        
        return sha256_hash.hexdigest()
