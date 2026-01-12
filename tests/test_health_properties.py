"""Property-based tests for health check completeness.

Property 12: Health Check Completeness
For any health check, all snapshots (optionally filtered by age) SHALL be
checked for readability, manifest validity, and file integrity.

Requirements: 12.2, 12.3, 12.4, 12.6
"""

import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List

from hypothesis import given, strategies as st, settings, assume

from devbackup.health import HealthChecker, SnapshotHealth, HealthCheckResult
from devbackup.verify import IntegrityVerifier


def create_snapshot_structure(
    base_dir: Path,
    snapshot_name: str,
    files: Dict[str, bytes],
    create_manifest: bool = True,
) -> Path:
    """Create a snapshot directory with files and optionally a manifest."""
    snapshot_dir = base_dir / snapshot_name
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    
    # Create files
    for rel_path, content in files.items():
        file_path = snapshot_dir / rel_path
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_path.write_bytes(content)
    
    # Create manifest if requested
    if create_manifest and files:
        verifier = IntegrityVerifier()
        manifest = verifier.create_manifest(snapshot_dir)
        verifier.save_manifest(manifest, snapshot_dir)
    
    return snapshot_dir


class TestHealthCheckCompleteness:
    """Property 12: Health Check Completeness"""
    
    @given(
        num_snapshots=st.integers(min_value=1, max_value=5),
        files_per_snapshot=st.integers(min_value=1, max_value=5),
    )
    @settings(max_examples=30)
    def test_all_snapshots_checked(
        self,
        num_snapshots: int,
        files_per_snapshot: int,
    ):
        """
        Property: All snapshots are checked in health check.
        
        Validates: Requirements 12.4, 12.6
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            dest = Path(tmp_dir)
            
            # Create snapshots with different timestamps
            base_time = datetime.now() - timedelta(days=10)
            snapshot_names = []
            
            for i in range(num_snapshots):
                timestamp = base_time + timedelta(hours=i)
                name = timestamp.strftime("%Y-%m-%d-%H%M%S")
                snapshot_names.append(name)
                
                files = {f"file{j}.txt": f"content{j}".encode() for j in range(files_per_snapshot)}
                create_snapshot_structure(dest, name, files, create_manifest=True)
            
            # Run health check
            health_checker = HealthChecker(dest)
            result = health_checker.check_all()
            
            # Verify all snapshots were checked
            assert result.total_snapshots == num_snapshots
            assert len(result.snapshots) == num_snapshots
            
            # Verify each snapshot was checked
            checked_names = {s.snapshot_name for s in result.snapshots}
            for name in snapshot_names:
                assert name in checked_names
    
    @given(
        num_snapshots=st.integers(min_value=2, max_value=5),
    )
    @settings(max_examples=20)
    def test_age_filter_works(self, num_snapshots: int):
        """
        Property: Age filter correctly filters snapshots.
        
        Validates: Requirements 12.6
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            dest = Path(tmp_dir)
            
            # Create snapshots: some old, some recent
            old_count = num_snapshots // 2
            recent_count = num_snapshots - old_count
            
            # Old snapshots (10+ days ago)
            old_base = datetime.now() - timedelta(days=15)
            for i in range(old_count):
                timestamp = old_base + timedelta(hours=i)
                name = timestamp.strftime("%Y-%m-%d-%H%M%S")
                files = {"file.txt": b"content"}
                create_snapshot_structure(dest, name, files, create_manifest=True)
            
            # Recent snapshots (1 day ago)
            recent_base = datetime.now() - timedelta(days=1)
            for i in range(recent_count):
                timestamp = recent_base + timedelta(hours=i)
                name = timestamp.strftime("%Y-%m-%d-%H%M%S")
                files = {"file.txt": b"content"}
                create_snapshot_structure(dest, name, files, create_manifest=True)
            
            # Check all snapshots
            health_checker = HealthChecker(dest)
            all_result = health_checker.check_all()
            assert all_result.total_snapshots == num_snapshots
            
            # Check only old snapshots (min_age_days=5)
            old_result = health_checker.check_all(min_age_days=5)
            assert old_result.total_snapshots == old_count
    
    @given(
        num_files=st.integers(min_value=1, max_value=10),
    )
    @settings(max_examples=30)
    def test_readability_checked(self, num_files: int):
        """
        Property: Snapshot readability is verified.
        
        Validates: Requirements 12.2
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            dest = Path(tmp_dir)
            
            # Create a readable snapshot
            timestamp = datetime.now() - timedelta(days=1)
            name = timestamp.strftime("%Y-%m-%d-%H%M%S")
            files = {f"file{i}.txt": f"content{i}".encode() for i in range(num_files)}
            create_snapshot_structure(dest, name, files, create_manifest=True)
            
            # Run health check
            health_checker = HealthChecker(dest)
            result = health_checker.check_all()
            
            # Verify readability was checked
            assert result.total_snapshots == 1
            assert result.snapshots[0].readable is True
    
    @given(
        num_files=st.integers(min_value=1, max_value=10),
    )
    @settings(max_examples=30)
    def test_manifest_validity_checked(self, num_files: int):
        """
        Property: Manifest validity is verified.
        
        Validates: Requirements 12.3
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            dest = Path(tmp_dir)
            
            # Create snapshot with manifest
            timestamp = datetime.now() - timedelta(days=1)
            name = timestamp.strftime("%Y-%m-%d-%H%M%S")
            files = {f"file{i}.txt": f"content{i}".encode() for i in range(num_files)}
            create_snapshot_structure(dest, name, files, create_manifest=True)
            
            # Run health check
            health_checker = HealthChecker(dest)
            result = health_checker.check_all()
            
            # Verify manifest was checked
            assert result.total_snapshots == 1
            assert result.snapshots[0].has_manifest is True
            assert result.snapshots[0].manifest_valid is True
    
    @given(
        num_files=st.integers(min_value=1, max_value=5),
    )
    @settings(max_examples=30)
    def test_integrity_verified(self, num_files: int):
        """
        Property: File integrity is verified against manifest.
        
        Validates: Requirements 12.3
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            dest = Path(tmp_dir)
            
            # Create snapshot with manifest
            timestamp = datetime.now() - timedelta(days=1)
            name = timestamp.strftime("%Y-%m-%d-%H%M%S")
            files = {f"file{i}.txt": f"content{i}".encode() for i in range(num_files)}
            snapshot_dir = create_snapshot_structure(dest, name, files, create_manifest=True)
            
            # Run health check
            health_checker = HealthChecker(dest)
            result = health_checker.check_all()
            
            # Verify integrity was checked
            assert result.total_snapshots == 1
            assert result.snapshots[0].file_count == num_files
            assert result.snapshots[0].corrupted_files == []
            assert result.snapshots[0].missing_files == []


class TestHealthCheckDetectsIssues:
    """Tests for health check issue detection."""
    
    def test_detects_missing_manifest(self):
        """
        Property: Snapshots without manifests are detected.
        
        Validates: Requirements 12.3
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            dest = Path(tmp_dir)
            
            # Create snapshot without manifest
            timestamp = datetime.now() - timedelta(days=1)
            name = timestamp.strftime("%Y-%m-%d-%H%M%S")
            files = {"file.txt": b"content"}
            create_snapshot_structure(dest, name, files, create_manifest=False)
            
            # Run health check
            health_checker = HealthChecker(dest)
            result = health_checker.check_all()
            
            # Verify missing manifest detected
            assert result.total_snapshots == 1
            assert result.snapshots[0].has_manifest is False
    
    def test_detects_corrupted_files(self):
        """
        Property: Corrupted files are detected.
        
        Validates: Requirements 12.3
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            dest = Path(tmp_dir)
            
            # Create snapshot with manifest
            timestamp = datetime.now() - timedelta(days=1)
            name = timestamp.strftime("%Y-%m-%d-%H%M%S")
            files = {"file.txt": b"original content"}
            snapshot_dir = create_snapshot_structure(dest, name, files, create_manifest=True)
            
            # Corrupt a file after manifest creation
            (snapshot_dir / "file.txt").write_bytes(b"corrupted content")
            
            # Run health check
            health_checker = HealthChecker(dest)
            result = health_checker.check_all()
            
            # Verify corruption detected
            assert result.total_snapshots == 1
            assert result.unhealthy_snapshots == 1
            assert len(result.snapshots[0].corrupted_files) == 1
    
    def test_detects_missing_files(self):
        """
        Property: Missing files are detected.
        
        Validates: Requirements 12.3
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            dest = Path(tmp_dir)
            
            # Create snapshot with manifest
            timestamp = datetime.now() - timedelta(days=1)
            name = timestamp.strftime("%Y-%m-%d-%H%M%S")
            files = {"file1.txt": b"content1", "file2.txt": b"content2"}
            snapshot_dir = create_snapshot_structure(dest, name, files, create_manifest=True)
            
            # Delete a file after manifest creation
            (snapshot_dir / "file1.txt").unlink()
            
            # Run health check
            health_checker = HealthChecker(dest)
            result = health_checker.check_all()
            
            # Verify missing file detected
            assert result.total_snapshots == 1
            assert result.unhealthy_snapshots == 1
            assert len(result.snapshots[0].missing_files) == 1
    
    def test_healthy_vs_unhealthy_counts(self):
        """
        Property: Healthy and unhealthy counts are accurate.
        
        Validates: Requirements 12.4
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            dest = Path(tmp_dir)
            
            # Create healthy snapshot
            timestamp1 = datetime.now() - timedelta(days=2)
            name1 = timestamp1.strftime("%Y-%m-%d-%H%M%S")
            files1 = {"file.txt": b"content"}
            create_snapshot_structure(dest, name1, files1, create_manifest=True)
            
            # Create unhealthy snapshot (corrupted)
            timestamp2 = datetime.now() - timedelta(days=1)
            name2 = timestamp2.strftime("%Y-%m-%d-%H%M%S")
            files2 = {"file.txt": b"original"}
            snapshot_dir2 = create_snapshot_structure(dest, name2, files2, create_manifest=True)
            (snapshot_dir2 / "file.txt").write_bytes(b"corrupted")
            
            # Run health check
            health_checker = HealthChecker(dest)
            result = health_checker.check_all()
            
            # Verify counts
            assert result.total_snapshots == 2
            assert result.healthy_snapshots == 1
            assert result.unhealthy_snapshots == 1


class TestHealthCheckEdgeCases:
    """Tests for edge cases in health check."""
    
    def test_empty_destination(self):
        """
        Property: Empty destination is handled gracefully.
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            dest = Path(tmp_dir)
            
            health_checker = HealthChecker(dest)
            result = health_checker.check_all()
            
            assert result.total_snapshots == 0
            assert result.healthy_snapshots == 0
            assert result.unhealthy_snapshots == 0
    
    def test_nonexistent_destination(self):
        """
        Property: Nonexistent destination is handled gracefully.
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            dest = Path(tmp_dir) / "nonexistent"
            
            health_checker = HealthChecker(dest)
            result = health_checker.check_all()
            
            assert result.total_snapshots == 0
            assert len(result.errors) > 0
    
    def test_skips_in_progress_directories(self):
        """
        Property: In-progress directories are skipped.
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            dest = Path(tmp_dir)
            
            # Create a normal snapshot
            timestamp = datetime.now() - timedelta(days=1)
            name = timestamp.strftime("%Y-%m-%d-%H%M%S")
            files = {"file.txt": b"content"}
            create_snapshot_structure(dest, name, files, create_manifest=True)
            
            # Create an in_progress directory (should be skipped)
            in_progress = dest / f"{name}_in_progress"
            in_progress.mkdir()
            (in_progress / "file.txt").write_bytes(b"incomplete")
            
            # Run health check
            health_checker = HealthChecker(dest)
            result = health_checker.check_all()
            
            # Only the complete snapshot should be checked
            assert result.total_snapshots == 1
            assert result.snapshots[0].snapshot_name == name
