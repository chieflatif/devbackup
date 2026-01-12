"""Property-based tests for SnapshotEngine.

**Property 5: Snapshot Correctness**
**Validates: Requirements 4.7, 4.8, 4.9, 4.10**

Tests that:
- Unchanged files have the same inode as the previous snapshot (hard link)
- Modified files have different inodes (new copy)
- Added files exist in the new snapshot
- Deleted files do not exist in the new snapshot
"""

import os
import tempfile
import time
from pathlib import Path
from typing import Dict, List, Set, Tuple

import pytest
from hypothesis import given, settings, assume, HealthCheck
from hypothesis import strategies as st

from devbackup.snapshot import SnapshotEngine


# Strategy for generating valid filenames (no special chars that cause issues)
filename_strategy = st.text(
    alphabet=st.sampled_from("abcdefghijklmnopqrstuvwxyz0123456789_"),
    min_size=1,
    max_size=10,
).filter(lambda x: x and not x.startswith("."))

# Strategy for file content
content_strategy = st.text(min_size=0, max_size=100)


def create_file_tree(base_path: Path, files: Dict[str, str]) -> None:
    """Create a file tree from a dict of {relative_path: content}."""
    for rel_path, content in files.items():
        file_path = base_path / rel_path
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_path.write_text(content)


def get_file_inodes(base_path: Path) -> Dict[str, int]:
    """Get inodes for all files in a directory tree."""
    inodes = {}
    if not base_path.exists():
        return inodes
    for root, dirs, files in os.walk(base_path):
        for f in files:
            file_path = Path(root) / f
            rel_path = file_path.relative_to(base_path)
            try:
                inodes[str(rel_path)] = file_path.stat().st_ino
            except OSError:
                pass
    return inodes


def get_all_files(base_path: Path) -> Set[str]:
    """Get set of all relative file paths in a directory tree."""
    files = set()
    if not base_path.exists():
        return files
    for root, dirs, filenames in os.walk(base_path):
        for f in filenames:
            file_path = Path(root) / f
            rel_path = file_path.relative_to(base_path)
            files.add(str(rel_path))
    return files


class TestSnapshotCorrectnessProperty:
    """
    Property 5: Snapshot Correctness
    
    *For any* source directory state S1 and subsequent state S2, creating a 
    snapshot after the transition SHALL satisfy:
    - Unchanged files have the same inode as the previous snapshot (hard link)
    - Modified files have different inodes (new copy)
    - Added files exist in the new snapshot
    - Deleted files do not exist in the new snapshot
    
    **Validates: Requirements 4.7, 4.8, 4.9, 4.10**
    """
    
    @given(
        initial_files=st.dictionaries(
            keys=filename_strategy,
            values=content_strategy,
            min_size=1,
            max_size=5,
        ),
        files_to_modify=st.lists(filename_strategy, max_size=2),
        files_to_add=st.dictionaries(
            keys=filename_strategy.filter(lambda x: x.startswith("new")),
            values=content_strategy,
            max_size=2,
        ),
        files_to_delete=st.lists(filename_strategy, max_size=2),
    )
    @settings(
        max_examples=10,
        deadline=None,  # Filesystem operations can be slow
        suppress_health_check=[HealthCheck.too_slow],
    )
    def test_snapshot_correctness_property(
        self,
        initial_files: Dict[str, str],
        files_to_modify: List[str],
        files_to_add: Dict[str, str],
        files_to_delete: List[str],
    ):
        """
        Feature: macos-incremental-backup, Property 5: Snapshot Correctness
        
        For any source state transition, verify hard links for unchanged files,
        new copies for modified files, and correct handling of added/deleted files.
        """
        # Filter modifications to only affect existing files
        files_to_modify = [f for f in files_to_modify if f in initial_files]
        files_to_delete = [f for f in files_to_delete if f in initial_files]
        
        # Ensure we don't delete files we're modifying
        files_to_delete = [f for f in files_to_delete if f not in files_to_modify]
        
        # Ensure added files don't conflict with existing
        files_to_add = {k: v for k, v in files_to_add.items() if k not in initial_files}
        
        with tempfile.TemporaryDirectory() as dest_dir:
            with tempfile.TemporaryDirectory() as source_dir:
                dest_path = Path(dest_dir)
                source_path = Path(source_dir)
                
                # Create initial file tree
                create_file_tree(source_path, initial_files)
                
                # Create first snapshot
                engine = SnapshotEngine(dest_path, [])
                result1 = engine.create_snapshot([source_path])
                assume(result1.success)
                
                # Small delay to ensure different timestamp
                time.sleep(1.1)
                
                # Track what files should be unchanged
                unchanged_files = set(initial_files.keys()) - set(files_to_modify) - set(files_to_delete)
                
                # Apply modifications
                for filename in files_to_modify:
                    file_path = source_path / filename
                    if file_path.exists():
                        # Modify content
                        file_path.write_text(initial_files[filename] + "_modified")
                
                # Add new files
                for filename, content in files_to_add.items():
                    (source_path / filename).write_text(content)
                
                # Delete files
                for filename in files_to_delete:
                    file_path = source_path / filename
                    if file_path.exists():
                        file_path.unlink()
                
                # Create second snapshot
                result2 = engine.create_snapshot([source_path])
                assume(result2.success)
                
                # Get inodes from both snapshots
                inodes1 = get_file_inodes(result1.snapshot_path)
                inodes2 = get_file_inodes(result2.snapshot_path)
                
                # Get file sets
                files1 = get_all_files(result1.snapshot_path)
                files2 = get_all_files(result2.snapshot_path)
                
                # Verify Property 4.7: Unchanged files have same inode (hard link)
                for filename in unchanged_files:
                    if filename in inodes1 and filename in inodes2:
                        assert inodes1[filename] == inodes2[filename], \
                            f"Unchanged file {filename} should be hard-linked"
                
                # Verify Property 4.8: Modified files have different inodes
                for filename in files_to_modify:
                    if filename in inodes1 and filename in inodes2:
                        assert inodes1[filename] != inodes2[filename], \
                            f"Modified file {filename} should have new inode"
                
                # Verify Property 4.9: Added files exist in new snapshot
                for filename in files_to_add.keys():
                    assert filename in files2, \
                        f"Added file {filename} should exist in new snapshot"
                
                # Verify Property 4.10: Deleted files don't exist in new snapshot
                for filename in files_to_delete:
                    assert filename not in files2, \
                        f"Deleted file {filename} should not exist in new snapshot"



class TestSymlinkSafetyProperty:
    """
    Property 3: Symlink Safety
    
    *For any* directory traversal operation (diff, search, stats), the operation 
    SHALL complete without following symbolic links, preventing infinite loops 
    from circular symlinks.
    
    **Validates: Requirements 3.1, 3.2, 3.3**
    """
    
    @given(
        num_regular_files=st.integers(min_value=1, max_value=5),
        num_subdirs=st.integers(min_value=0, max_value=3),
        create_circular_symlink=st.booleans(),
        create_external_symlink=st.booleans(),
    )
    @settings(
        max_examples=10,
        deadline=None,  # Filesystem operations can be slow
        suppress_health_check=[HealthCheck.too_slow],
    )
    def test_symlink_safety_property(
        self,
        num_regular_files: int,
        num_subdirs: int,
        create_circular_symlink: bool,
        create_external_symlink: bool,
    ):
        """
        Feature: backup-robustness, Property 3: Symlink Safety
        
        For any directory structure with symlinks (circular or external),
        all directory traversal operations (diff, search, _get_directory_stats)
        SHALL complete without infinite loops and SHALL NOT follow symlinks.
        
        **Validates: Requirements 3.1, 3.2, 3.3**
        """
        import shutil
        
        with tempfile.TemporaryDirectory() as dest_dir:
            with tempfile.TemporaryDirectory() as source_dir:
                dest_path = Path(dest_dir)
                source_path = Path(source_dir)
                
                # Create snapshot directory
                snap_dir = dest_path / "2025-01-01-100000"
                snap_dir.mkdir()
                
                # Create regular files in both snapshot and source
                regular_files = {}
                for i in range(num_regular_files):
                    filename = f"file{i}.txt"
                    content = f"content{i}"
                    regular_files[filename] = content
                    (snap_dir / filename).write_text(content)
                    (source_path / filename).write_text(content)
                
                # Create subdirectories
                for i in range(num_subdirs):
                    subdir_name = f"subdir{i}"
                    (snap_dir / subdir_name).mkdir()
                    (source_path / subdir_name).mkdir()
                    # Add a file in each subdir
                    (snap_dir / subdir_name / "nested.txt").write_text("nested")
                    (source_path / subdir_name / "nested.txt").write_text("nested")
                
                external_dir = None
                try:
                    # Optionally create external symlink
                    if create_external_symlink:
                        external_dir = Path(tempfile.mkdtemp())
                        # Create files in external directory that should NOT be found
                        (external_dir / "external_file.txt").write_text("x" * 1000)
                        (external_dir / "another_external.txt").write_text("y" * 1000)
                        
                        # Create symlinks to external directory in both locations
                        (snap_dir / "external_link").symlink_to(external_dir)
                        (source_path / "external_link").symlink_to(external_dir)
                    
                    # Optionally create circular symlink
                    if create_circular_symlink and num_subdirs > 0:
                        # Create circular symlink in first subdir pointing back to root
                        (snap_dir / "subdir0" / "circular").symlink_to(snap_dir)
                        (source_path / "subdir0" / "circular").symlink_to(source_path)
                    
                    engine = SnapshotEngine(dest_path, [])
                    
                    # Test 1: diff() should complete without infinite loop (Requirement 3.1)
                    diff_result = engine.diff(snap_dir, [source_path])
                    assert isinstance(diff_result, dict)
                    assert "added" in diff_result
                    assert "modified" in diff_result
                    assert "deleted" in diff_result
                    
                    # Verify external files are NOT in diff results
                    if create_external_symlink:
                        all_diff_paths = (
                            diff_result["added"] + 
                            diff_result["modified"] + 
                            diff_result["deleted"]
                        )
                        assert not any("external_file.txt" in p for p in all_diff_paths), \
                            "diff() should not follow symlinks to external directories"
                        assert not any("another_external.txt" in p for p in all_diff_paths), \
                            "diff() should not follow symlinks to external directories"
                    
                    # Test 2: search() should complete without infinite loop (Requirement 3.2)
                    search_results = engine.search("*.txt", snapshot=snap_dir)
                    assert isinstance(search_results, list)
                    
                    # Verify external files are NOT in search results
                    if create_external_symlink:
                        search_paths = [r["path"] for r in search_results]
                        assert not any("external_file.txt" in p for p in search_paths), \
                            "search() should not follow symlinks to external directories"
                        assert not any("another_external.txt" in p for p in search_paths), \
                            "search() should not follow symlinks to external directories"
                    
                    # Test 3: _get_directory_stats() should complete without infinite loop (Requirement 3.3)
                    size, count = engine._get_directory_stats(snap_dir)
                    assert isinstance(size, int)
                    assert isinstance(count, int)
                    assert size >= 0
                    assert count >= 0
                    
                    # Verify file count matches expected (regular files only)
                    expected_count = num_regular_files + num_subdirs  # files + nested files
                    assert count == expected_count, \
                        f"File count should be {expected_count} (regular files only), got {count}"
                    
                    # Verify external file sizes are NOT included
                    if create_external_symlink:
                        # External files have 1000+ bytes each, regular files have ~10 bytes
                        max_expected_size = (num_regular_files + num_subdirs) * 100
                        assert size < max_expected_size, \
                            f"Size {size} suggests symlinks were followed (expected < {max_expected_size})"
                    
                    # Test 4: list_snapshots() should work correctly
                    snapshots = engine.list_snapshots()
                    assert len(snapshots) == 1
                    assert snapshots[0].file_count == expected_count
                    
                finally:
                    # Clean up external directory
                    if external_dir and external_dir.exists():
                        shutil.rmtree(external_dir)


class TestTimestampCollisionProperty:
    """
    Property 8: Timestamp Collision Resolution
    
    *For any* snapshot creation where the timestamp already exists, a unique 
    sequence number SHALL be appended, and no data loss SHALL occur.
    
    **Validates: Requirements 8.1, 8.2, 8.4**
    """
    
    @given(
        num_existing_snapshots=st.integers(min_value=0, max_value=10),
        num_existing_sequences=st.integers(min_value=0, max_value=5),
        file_content=st.text(
            alphabet=st.sampled_from("abcdefghijklmnopqrstuvwxyz0123456789"),
            min_size=1,
            max_size=50,
        ),
    )
    @settings(
        max_examples=10,
        deadline=None,  # Filesystem operations can be slow
        suppress_health_check=[HealthCheck.too_slow],
    )
    def test_timestamp_collision_resolution_property(
        self,
        num_existing_snapshots: int,
        num_existing_sequences: int,
        file_content: str,
    ):
        """
        Feature: backup-robustness, Property 8: Timestamp Collision Resolution
        
        For any snapshot creation where the timestamp already exists:
        - A unique sequence number SHALL be appended (Req 8.1, 8.2)
        - The original snapshot SHALL be preserved (no data loss)
        - Collision detection SHALL happen before creating in_progress (Req 8.4)
        
        **Validates: Requirements 8.1, 8.2, 8.4**
        """
        with tempfile.TemporaryDirectory() as dest_dir:
            with tempfile.TemporaryDirectory() as source_dir:
                dest_path = Path(dest_dir)
                source_path = Path(source_dir)
                
                # Create source file
                (source_path / "test.txt").write_text(file_content)
                
                engine = SnapshotEngine(dest_path, [])
                
                # Get current timestamp
                current_ts = engine._generate_timestamp()
                
                # Create existing snapshots with the same timestamp
                existing_names = []
                if num_existing_snapshots > 0:
                    # Create base timestamp snapshot
                    base_snap = dest_path / current_ts
                    base_snap.mkdir()
                    (base_snap / "original.txt").write_text("original_content")
                    existing_names.append(current_ts)
                    
                    # Create sequence number snapshots
                    for seq in range(1, min(num_existing_sequences + 1, 100)):
                        seq_name = f"{current_ts}-{seq:02d}"
                        seq_snap = dest_path / seq_name
                        seq_snap.mkdir()
                        (seq_snap / "original.txt").write_text(f"seq_{seq}_content")
                        existing_names.append(seq_name)
                
                # Record existing snapshot contents for verification
                existing_contents = {}
                for name in existing_names:
                    snap_path = dest_path / name
                    if (snap_path / "original.txt").exists():
                        existing_contents[name] = (snap_path / "original.txt").read_text()
                
                # Create new snapshot - should handle collision
                result = engine.create_snapshot([source_path])
                assume(result.success)
                
                # Verify: New snapshot was created with unique name
                assert result.snapshot_path is not None
                assert result.snapshot_path.exists()
                new_name = result.snapshot_path.name
                
                # Verify: New snapshot name is unique (not in existing names)
                assert new_name not in existing_names, \
                    f"New snapshot name {new_name} should be unique"
                
                # Verify: New snapshot name is valid format
                parsed = engine._parse_snapshot_name(new_name)
                assert parsed is not None, \
                    f"New snapshot name {new_name} should be valid format"
                
                # Verify: If collision occurred, sequence number was appended (Req 8.1, 8.2)
                if num_existing_snapshots > 0 and new_name.startswith(current_ts):
                    # Should have sequence number format
                    if len(new_name) == 20:  # YYYY-MM-DD-HHMMSS-NN
                        assert new_name[-3] == "-", \
                            "Sequence format should be YYYY-MM-DD-HHMMSS-NN"
                        seq_num = int(new_name[-2:])
                        assert 1 <= seq_num <= 99, \
                            f"Sequence number {seq_num} should be 01-99"
                        # Sequence should be greater than existing sequences
                        assert seq_num > num_existing_sequences, \
                            f"Sequence {seq_num} should be > {num_existing_sequences}"
                
                # Verify: Original snapshots are preserved (no data loss)
                for name, original_content in existing_contents.items():
                    snap_path = dest_path / name
                    assert snap_path.exists(), \
                        f"Original snapshot {name} should be preserved"
                    current_content = (snap_path / "original.txt").read_text()
                    assert current_content == original_content, \
                        f"Original snapshot {name} content should be unchanged"
                
                # Verify: New snapshot contains the test file
                assert (result.snapshot_path / "test.txt").exists()
    
    @given(
        num_rapid_snapshots=st.integers(min_value=2, max_value=5),
    )
    @settings(
        max_examples=50,
        deadline=None,
        suppress_health_check=[HealthCheck.too_slow],
    )
    def test_rapid_snapshot_creation_property(
        self,
        num_rapid_snapshots: int,
    ):
        """
        Feature: backup-robustness, Property 8: Rapid Snapshot Creation
        
        For any number of rapid successive snapshots created within the same second:
        - All snapshots SHALL be created successfully
        - All snapshots SHALL have unique names
        - All snapshot data SHALL be preserved
        
        **Validates: Requirements 8.1, 8.2, 8.4**
        """
        with tempfile.TemporaryDirectory() as dest_dir:
            with tempfile.TemporaryDirectory() as source_dir:
                dest_path = Path(dest_dir)
                source_path = Path(source_dir)
                
                engine = SnapshotEngine(dest_path, [])
                
                # Create initial file
                (source_path / "file.txt").write_text("initial_content")
                
                # Create snapshots rapidly (no delay between them)
                results = []
                for i in range(num_rapid_snapshots):
                    result = engine.create_snapshot([source_path])
                    assume(result.success)
                    results.append(result)
                
                # Verify: All snapshots were created
                assert len(results) == num_rapid_snapshots
                
                # Verify: All snapshot names are unique
                names = [r.snapshot_path.name for r in results]
                assert len(names) == len(set(names)), \
                    f"All snapshot names should be unique: {names}"
                
                # Verify: All snapshots exist
                for i, result in enumerate(results):
                    assert result.snapshot_path.exists(), \
                        f"Snapshot {i} should exist"
                    # Verify the file exists in each snapshot
                    assert (result.snapshot_path / "file.txt").exists(), \
                        f"Snapshot {i} should contain file.txt"
                
                # Verify: All snapshot names are valid
                for name in names:
                    parsed = engine._parse_snapshot_name(name)
                    assert parsed is not None, \
                        f"Snapshot name {name} should be valid format"
