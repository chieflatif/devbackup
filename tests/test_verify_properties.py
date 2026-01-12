"""Property-based tests for IntegrityVerifier.

Tests Property 6 (Manifest Completeness) and Property 7 (Verification Detects Corruption).
Requirements: 7.1, 7.2, 7.5
"""

import hashlib
import tempfile
from pathlib import Path
from typing import Dict

from hypothesis import given, settings, assume
from hypothesis import strategies as st

from devbackup.verify import IntegrityVerifier


@st.composite
def file_structure_strategy(draw, max_files: int = 5):
    """Generate a dictionary of non-conflicting file paths to content."""
    num_files = draw(st.integers(min_value=1, max_value=max_files))
    files: Dict[str, bytes] = {}
    used_prefixes = set()
    
    for i in range(num_files):
        # Use simple indexed names to avoid conflicts
        filename = f"file_{i}.txt"
        depth = draw(st.integers(min_value=0, max_value=2))
        if depth > 0:
            dirs = [f"dir_{i}_{j}" for j in range(depth)]
            rel_path = "/".join(dirs) + "/" + filename
        else:
            rel_path = filename
        
        content = draw(st.binary(min_size=0, max_size=512))
        files[rel_path] = content
    
    return files


def create_snapshot(base_path: Path, files: Dict[str, bytes]) -> None:
    """Create actual files from a file structure dict."""
    for rel_path, content in files.items():
        file_path = base_path / rel_path
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_path.write_bytes(content)


def sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


class TestManifestCompleteness:
    """Property 6: Manifest Completeness. Validates: Requirements 7.1, 7.2"""

    @given(files=file_structure_strategy())
    @settings(max_examples=10, deadline=None)
    def test_manifest_contains_all_files(self, files: Dict[str, bytes]):
        """Feature: backup-robustness, Property 6: Manifest Completeness"""
        with tempfile.TemporaryDirectory() as tmp_dir:
            snapshot_path = Path(tmp_dir) / "snapshot"
            snapshot_path.mkdir()
            create_snapshot(snapshot_path, files)
            verifier = IntegrityVerifier()
            manifest = verifier.create_manifest(snapshot_path)
            assert manifest.file_count == len(files)
            manifest_paths = {c.path for c in manifest.checksums}
            for rel_path in files.keys():
                assert rel_path in manifest_paths

    @given(files=file_structure_strategy())
    @settings(max_examples=10, deadline=None)
    def test_manifest_checksums_correct(self, files: Dict[str, bytes]):
        """Feature: backup-robustness, Property 6: Manifest Completeness"""
        with tempfile.TemporaryDirectory() as tmp_dir:
            snapshot_path = Path(tmp_dir) / "snapshot"
            snapshot_path.mkdir()
            create_snapshot(snapshot_path, files)
            verifier = IntegrityVerifier()
            manifest = verifier.create_manifest(snapshot_path)
            by_path = {c.path: c for c in manifest.checksums}
            for rel_path, content in files.items():
                assert by_path[rel_path].sha256 == sha256(content)

    @given(files=file_structure_strategy())
    @settings(max_examples=10, deadline=None)
    def test_manifest_sizes_correct(self, files: Dict[str, bytes]):
        """Feature: backup-robustness, Property 6: Manifest Completeness"""
        with tempfile.TemporaryDirectory() as tmp_dir:
            snapshot_path = Path(tmp_dir) / "snapshot"
            snapshot_path.mkdir()
            create_snapshot(snapshot_path, files)
            verifier = IntegrityVerifier()
            manifest = verifier.create_manifest(snapshot_path)
            by_path = {c.path: c for c in manifest.checksums}
            for rel_path, content in files.items():
                assert by_path[rel_path].size == len(content)

    @given(files=file_structure_strategy())
    @settings(max_examples=10, deadline=None)
    def test_manifest_total_size(self, files: Dict[str, bytes]):
        """Feature: backup-robustness, Property 6: Manifest Completeness"""
        with tempfile.TemporaryDirectory() as tmp_dir:
            snapshot_path = Path(tmp_dir) / "snapshot"
            snapshot_path.mkdir()
            create_snapshot(snapshot_path, files)
            verifier = IntegrityVerifier()
            manifest = verifier.create_manifest(snapshot_path)
            assert manifest.total_size == sum(len(c) for c in files.values())


class TestVerificationDetectsCorruption:
    """Property 7: Verification Detects Corruption. Validates: Requirements 7.5"""

    @given(files=file_structure_strategy(), extra=st.binary(min_size=1, max_size=50))
    @settings(max_examples=10, deadline=None)
    def test_detects_corrupted_files(self, files: Dict[str, bytes], extra: bytes):
        """Feature: backup-robustness, Property 7: Verification Detects Corruption"""
        with tempfile.TemporaryDirectory() as tmp_dir:
            snapshot_path = Path(tmp_dir) / "snapshot"
            snapshot_path.mkdir()
            create_snapshot(snapshot_path, files)
            verifier = IntegrityVerifier()
            manifest = verifier.create_manifest(snapshot_path)
            verifier.save_manifest(manifest, snapshot_path)
            # Corrupt first file
            target = list(files.keys())[0]
            path = snapshot_path / target
            original = path.read_bytes()
            corrupted = original + extra
            assume(sha256(corrupted) != sha256(original))
            path.write_bytes(corrupted)
            result = verifier.verify_snapshot(snapshot_path)
            assert not result.success
            assert target in result.corrupted_files

    @given(files=file_structure_strategy())
    @settings(max_examples=10, deadline=None)
    def test_detects_missing_files(self, files: Dict[str, bytes]):
        """Feature: backup-robustness, Property 7: Verification Detects Corruption"""
        with tempfile.TemporaryDirectory() as tmp_dir:
            snapshot_path = Path(tmp_dir) / "snapshot"
            snapshot_path.mkdir()
            create_snapshot(snapshot_path, files)
            verifier = IntegrityVerifier()
            manifest = verifier.create_manifest(snapshot_path)
            verifier.save_manifest(manifest, snapshot_path)
            # Delete first file
            target = list(files.keys())[0]
            (snapshot_path / target).unlink()
            result = verifier.verify_snapshot(snapshot_path)
            assert not result.success
            assert target in result.missing_files

    @given(files=file_structure_strategy())
    @settings(max_examples=10, deadline=None)
    def test_intact_snapshot_passes(self, files: Dict[str, bytes]):
        """Feature: backup-robustness, Property 7: Verification Detects Corruption"""
        with tempfile.TemporaryDirectory() as tmp_dir:
            snapshot_path = Path(tmp_dir) / "snapshot"
            snapshot_path.mkdir()
            create_snapshot(snapshot_path, files)
            verifier = IntegrityVerifier()
            manifest = verifier.create_manifest(snapshot_path)
            verifier.save_manifest(manifest, snapshot_path)
            result = verifier.verify_snapshot(snapshot_path)
            assert result.success
            assert result.files_verified == len(files)


class TestManifestRoundTrip:
    """Test manifest save/load preserves data."""

    @given(files=file_structure_strategy())
    @settings(max_examples=10, deadline=None)
    def test_round_trip(self, files: Dict[str, bytes]):
        """Feature: backup-robustness, Property 6: Manifest Completeness"""
        with tempfile.TemporaryDirectory() as tmp_dir:
            snapshot_path = Path(tmp_dir) / "snapshot"
            snapshot_path.mkdir()
            create_snapshot(snapshot_path, files)
            verifier = IntegrityVerifier()
            original = verifier.create_manifest(snapshot_path)
            verifier.save_manifest(original, snapshot_path)
            loaded = verifier.load_manifest(snapshot_path)
            assert loaded is not None
            assert loaded.file_count == original.file_count
            assert loaded.total_size == original.total_size
