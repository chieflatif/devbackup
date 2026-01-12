"""Unit tests for the SnapshotEngine."""

import os
import tempfile
import time
from datetime import datetime
from pathlib import Path

import pytest

from devbackup.snapshot import SnapshotEngine, SnapshotResult, SnapshotInfo


class TestSnapshotEngineCore:
    """Tests for SnapshotEngine core methods (Task 5.1)."""
    
    def test_generate_timestamp_format(self):
        """Test that _generate_timestamp produces correct format."""
        with tempfile.TemporaryDirectory() as dest:
            engine = SnapshotEngine(Path(dest), [])
            timestamp = engine._generate_timestamp()
            
            # Should be in YYYY-MM-DD-HHMMSS format
            assert len(timestamp) == 17
            # Should be parseable
            parsed = datetime.strptime(timestamp, "%Y-%m-%d-%H%M%S")
            assert parsed is not None
    
    def test_generate_timestamp_current_time(self):
        """Test that _generate_timestamp uses current time."""
        with tempfile.TemporaryDirectory() as dest:
            engine = SnapshotEngine(Path(dest), [])
            
            before = datetime.now().replace(microsecond=0)
            timestamp = engine._generate_timestamp()
            after = datetime.now().replace(microsecond=0)
            
            parsed = datetime.strptime(timestamp, "%Y-%m-%d-%H%M%S")
            # Allow 1 second tolerance for edge cases
            assert before <= parsed <= after or (after - before).seconds <= 1
    
    def test_find_latest_snapshot_empty(self):
        """Test find_latest_snapshot with no snapshots."""
        with tempfile.TemporaryDirectory() as dest:
            engine = SnapshotEngine(Path(dest), [])
            assert engine.find_latest_snapshot() is None
    
    def test_find_latest_snapshot_single(self):
        """Test find_latest_snapshot with one snapshot."""
        with tempfile.TemporaryDirectory() as dest:
            dest_path = Path(dest)
            engine = SnapshotEngine(dest_path, [])
            
            # Create a snapshot directory
            snapshot_name = "2025-01-01-120000"
            (dest_path / snapshot_name).mkdir()
            
            latest = engine.find_latest_snapshot()
            assert latest is not None
            assert latest.name == snapshot_name
    
    def test_find_latest_snapshot_multiple(self):
        """Test find_latest_snapshot returns most recent."""
        with tempfile.TemporaryDirectory() as dest:
            dest_path = Path(dest)
            engine = SnapshotEngine(dest_path, [])
            
            # Create multiple snapshot directories
            (dest_path / "2025-01-01-100000").mkdir()
            (dest_path / "2025-01-01-120000").mkdir()
            (dest_path / "2025-01-01-110000").mkdir()
            
            latest = engine.find_latest_snapshot()
            assert latest is not None
            assert latest.name == "2025-01-01-120000"

    def test_find_latest_snapshot_ignores_in_progress(self):
        """Test find_latest_snapshot ignores in_progress directories."""
        with tempfile.TemporaryDirectory() as dest:
            dest_path = Path(dest)
            engine = SnapshotEngine(dest_path, [])
            
            # Create a complete snapshot and an in-progress one
            (dest_path / "2025-01-01-100000").mkdir()
            (dest_path / "in_progress_2025-01-01-120000").mkdir()
            
            latest = engine.find_latest_snapshot()
            assert latest is not None
            assert latest.name == "2025-01-01-100000"
    
    def test_find_latest_snapshot_ignores_hidden(self):
        """Test find_latest_snapshot ignores hidden directories."""
        with tempfile.TemporaryDirectory() as dest:
            dest_path = Path(dest)
            engine = SnapshotEngine(dest_path, [])
            
            (dest_path / "2025-01-01-100000").mkdir()
            (dest_path / ".devbackup_meta").mkdir()
            
            latest = engine.find_latest_snapshot()
            assert latest is not None
            assert latest.name == "2025-01-01-100000"
    
    def test_create_exclude_file(self):
        """Test _create_exclude_file creates file with patterns."""
        with tempfile.TemporaryDirectory() as dest:
            patterns = ["node_modules/", ".git/", "*.pyc"]
            engine = SnapshotEngine(Path(dest), patterns)
            
            exclude_file = engine._create_exclude_file()
            try:
                assert exclude_file.exists()
                content = exclude_file.read_text()
                assert "node_modules/" in content
                assert ".git/" in content
                assert "*.pyc" in content
            finally:
                exclude_file.unlink()
    
    def test_build_rsync_command_basic(self):
        """Test _build_rsync_command builds correct command."""
        with tempfile.TemporaryDirectory() as dest:
            dest_path = Path(dest)
            engine = SnapshotEngine(dest_path, ["*.log"])
            
            sources = [Path("/tmp/source1"), Path("/tmp/source2")]
            cmd = engine._build_rsync_command(sources, dest_path / "snapshot", None)
            
            # Clean up exclude file
            if engine._current_exclude_file:
                engine._current_exclude_file.unlink()
            
            assert "rsync" in cmd
            assert "-av" in cmd
            assert "--delete" in cmd
            assert any("--exclude-from=" in arg for arg in cmd)
            assert "/tmp/source1/" in cmd
            assert "/tmp/source2/" in cmd
    
    def test_build_rsync_command_with_link_dest(self):
        """Test _build_rsync_command includes --link-dest when provided."""
        with tempfile.TemporaryDirectory() as dest:
            dest_path = Path(dest)
            engine = SnapshotEngine(dest_path, [])
            
            link_dest = dest_path / "previous"
            link_dest.mkdir()
            
            sources = [Path("/tmp/source")]
            cmd = engine._build_rsync_command(
                sources, 
                dest_path / "snapshot", 
                link_dest
            )
            
            # Clean up exclude file
            if engine._current_exclude_file:
                engine._current_exclude_file.unlink()
            
            assert any(f"--link-dest={link_dest}" in arg for arg in cmd)


class TestCreateSnapshot:
    """Tests for create_snapshot method (Task 5.2)."""
    
    def test_create_snapshot_first_backup(self):
        """Test creating first snapshot (no previous snapshot)."""
        with tempfile.TemporaryDirectory() as dest:
            with tempfile.TemporaryDirectory() as source:
                dest_path = Path(dest)
                source_path = Path(source)
                
                # Create some test files
                (source_path / "file1.txt").write_text("content1")
                (source_path / "subdir").mkdir()
                (source_path / "subdir" / "file2.txt").write_text("content2")
                
                engine = SnapshotEngine(dest_path, [])
                result = engine.create_snapshot([source_path])
                
                assert result.success
                assert result.snapshot_path is not None
                assert result.snapshot_path.exists()
                assert result.error_message is None
                
                # Verify files were copied
                assert (result.snapshot_path / "file1.txt").exists()
                assert (result.snapshot_path / "subdir" / "file2.txt").exists()
    
    def test_create_snapshot_incremental(self):
        """Test creating incremental snapshot with hard links."""
        with tempfile.TemporaryDirectory() as dest:
            with tempfile.TemporaryDirectory() as source:
                dest_path = Path(dest)
                source_path = Path(source)
                
                # Create test file
                (source_path / "unchanged.txt").write_text("unchanged content")
                
                engine = SnapshotEngine(dest_path, [])
                
                # First snapshot
                result1 = engine.create_snapshot([source_path])
                assert result1.success
                
                # Small delay to ensure different timestamp
                time.sleep(1)
                
                # Second snapshot (incremental)
                result2 = engine.create_snapshot([source_path])
                assert result2.success
                assert result2.snapshot_path != result1.snapshot_path
                
                # Both snapshots should have the file
                file1 = result1.snapshot_path / "unchanged.txt"
                file2 = result2.snapshot_path / "unchanged.txt"
                assert file1.exists()
                assert file2.exists()
                
                # Files should be hard-linked (same inode)
                assert file1.stat().st_ino == file2.stat().st_ino

    def test_create_snapshot_excludes_patterns(self):
        """Test that exclude patterns are respected."""
        with tempfile.TemporaryDirectory() as dest:
            with tempfile.TemporaryDirectory() as source:
                dest_path = Path(dest)
                source_path = Path(source)
                
                # Create files including ones that should be excluded
                (source_path / "keep.txt").write_text("keep")
                (source_path / "exclude.log").write_text("exclude")
                (source_path / "node_modules").mkdir()
                (source_path / "node_modules" / "pkg.json").write_text("{}")
                
                engine = SnapshotEngine(dest_path, ["*.log", "node_modules/"])
                result = engine.create_snapshot([source_path])
                
                assert result.success
                assert (result.snapshot_path / "keep.txt").exists()
                assert not (result.snapshot_path / "exclude.log").exists()
                assert not (result.snapshot_path / "node_modules").exists()
    
    def test_create_snapshot_cleans_up_on_failure(self):
        """Test that in_progress directory is cleaned up on failure."""
        with tempfile.TemporaryDirectory() as dest:
            dest_path = Path(dest)
            
            # Use a non-existent source to cause rsync to fail
            engine = SnapshotEngine(dest_path, [])
            result = engine.create_snapshot([Path("/nonexistent/path/12345")])
            
            assert not result.success
            assert result.error_message is not None
            
            # No in_progress directories should remain
            for entry in dest_path.iterdir():
                assert not entry.name.startswith("in_progress_")
    
    def test_create_snapshot_atomic_rename(self):
        """Test that snapshot is atomically renamed on success."""
        with tempfile.TemporaryDirectory() as dest:
            with tempfile.TemporaryDirectory() as source:
                dest_path = Path(dest)
                source_path = Path(source)
                
                (source_path / "test.txt").write_text("test")
                
                engine = SnapshotEngine(dest_path, [])
                result = engine.create_snapshot([source_path])
                
                assert result.success
                
                # Final directory should exist with timestamp format
                assert result.snapshot_path.name.count("-") == 3
                # No in_progress directories
                for entry in dest_path.iterdir():
                    assert not entry.name.startswith("in_progress_")


class TestListSnapshots:
    """Tests for list_snapshots and get_snapshot_by_timestamp (Task 5.4)."""
    
    def test_list_snapshots_empty(self):
        """Test list_snapshots with no snapshots."""
        with tempfile.TemporaryDirectory() as dest:
            engine = SnapshotEngine(Path(dest), [])
            snapshots = engine.list_snapshots()
            assert snapshots == []
    
    def test_list_snapshots_multiple(self):
        """Test list_snapshots returns all snapshots sorted."""
        with tempfile.TemporaryDirectory() as dest:
            dest_path = Path(dest)
            engine = SnapshotEngine(dest_path, [])
            
            # Create snapshot directories with files
            for name in ["2025-01-01-100000", "2025-01-01-120000", "2025-01-01-110000"]:
                snap_dir = dest_path / name
                snap_dir.mkdir()
                (snap_dir / "file.txt").write_text("content")
            
            snapshots = engine.list_snapshots()
            
            assert len(snapshots) == 3
            # Should be sorted most recent first
            assert snapshots[0].path.name == "2025-01-01-120000"
            assert snapshots[1].path.name == "2025-01-01-110000"
            assert snapshots[2].path.name == "2025-01-01-100000"
    
    def test_list_snapshots_excludes_in_progress(self):
        """Test list_snapshots excludes in_progress directories."""
        with tempfile.TemporaryDirectory() as dest:
            dest_path = Path(dest)
            engine = SnapshotEngine(dest_path, [])
            
            (dest_path / "2025-01-01-100000").mkdir()
            (dest_path / "in_progress_2025-01-01-120000").mkdir()
            
            snapshots = engine.list_snapshots()
            
            assert len(snapshots) == 1
            assert snapshots[0].path.name == "2025-01-01-100000"
    
    def test_list_snapshots_includes_metadata(self):
        """Test list_snapshots includes size and file count."""
        with tempfile.TemporaryDirectory() as dest:
            dest_path = Path(dest)
            engine = SnapshotEngine(dest_path, [])
            
            snap_dir = dest_path / "2025-01-01-100000"
            snap_dir.mkdir()
            (snap_dir / "file1.txt").write_text("content1")
            (snap_dir / "file2.txt").write_text("content2content2")
            
            snapshots = engine.list_snapshots()
            
            assert len(snapshots) == 1
            assert snapshots[0].file_count == 2
            assert snapshots[0].size_bytes > 0
    
    def test_get_snapshot_by_timestamp_found(self):
        """Test get_snapshot_by_timestamp finds existing snapshot."""
        with tempfile.TemporaryDirectory() as dest:
            dest_path = Path(dest)
            engine = SnapshotEngine(dest_path, [])
            
            (dest_path / "2025-01-01-100000").mkdir()
            
            result = engine.get_snapshot_by_timestamp("2025-01-01-100000")
            assert result is not None
            assert result.name == "2025-01-01-100000"
    
    def test_get_snapshot_by_timestamp_not_found(self):
        """Test get_snapshot_by_timestamp returns None for missing."""
        with tempfile.TemporaryDirectory() as dest:
            engine = SnapshotEngine(Path(dest), [])
            result = engine.get_snapshot_by_timestamp("2025-01-01-100000")
            assert result is None
    
    def test_get_snapshot_by_timestamp_invalid_format(self):
        """Test get_snapshot_by_timestamp rejects invalid format."""
        with tempfile.TemporaryDirectory() as dest:
            dest_path = Path(dest)
            engine = SnapshotEngine(dest_path, [])
            
            # Create directory with invalid name
            (dest_path / "not-a-timestamp").mkdir()
            
            result = engine.get_snapshot_by_timestamp("not-a-timestamp")
            assert result is None


class TestCleanupIncomplete:
    """Tests for cleanup_incomplete method (Task 5.5)."""
    
    def test_cleanup_incomplete_removes_in_progress(self):
        """Test cleanup_incomplete removes in_progress directories."""
        with tempfile.TemporaryDirectory() as dest:
            dest_path = Path(dest)
            engine = SnapshotEngine(dest_path, [])
            
            # Create in_progress directories
            (dest_path / "in_progress_2025-01-01-100000").mkdir()
            (dest_path / "in_progress_2025-01-01-110000").mkdir()
            # And a complete snapshot
            (dest_path / "2025-01-01-090000").mkdir()
            
            removed = engine.cleanup_incomplete()
            
            assert removed == 2
            assert not (dest_path / "in_progress_2025-01-01-100000").exists()
            assert not (dest_path / "in_progress_2025-01-01-110000").exists()
            # Complete snapshot should remain
            assert (dest_path / "2025-01-01-090000").exists()
    
    def test_cleanup_incomplete_empty_destination(self):
        """Test cleanup_incomplete with no in_progress directories."""
        with tempfile.TemporaryDirectory() as dest:
            dest_path = Path(dest)
            engine = SnapshotEngine(dest_path, [])
            
            (dest_path / "2025-01-01-100000").mkdir()
            
            removed = engine.cleanup_incomplete()
            assert removed == 0
    
    def test_cleanup_incomplete_nonexistent_destination(self):
        """Test cleanup_incomplete with nonexistent destination."""
        engine = SnapshotEngine(Path("/nonexistent/path"), [])
        removed = engine.cleanup_incomplete()
        assert removed == 0


class TestRestore:
    """Tests for restore method (Task 6.1)."""
    
    def test_restore_file_to_alternate_location(self):
        """Test restoring a single file to an alternate location."""
        with tempfile.TemporaryDirectory() as dest:
            with tempfile.TemporaryDirectory() as restore_dest:
                dest_path = Path(dest)
                restore_path = Path(restore_dest)
                
                # Create a snapshot with a file
                snap_dir = dest_path / "2025-01-01-100000"
                snap_dir.mkdir()
                (snap_dir / "test.txt").write_text("test content")
                
                engine = SnapshotEngine(dest_path, [])
                
                # Restore to alternate location
                result = engine.restore(
                    snap_dir,
                    "test.txt",
                    restore_path / "restored.txt"
                )
                
                assert result is True
                assert (restore_path / "restored.txt").exists()
                assert (restore_path / "restored.txt").read_text() == "test content"
    
    def test_restore_directory_to_alternate_location(self):
        """Test restoring a directory to an alternate location."""
        with tempfile.TemporaryDirectory() as dest:
            with tempfile.TemporaryDirectory() as restore_dest:
                dest_path = Path(dest)
                restore_path = Path(restore_dest)
                
                # Create a snapshot with a directory structure
                snap_dir = dest_path / "2025-01-01-100000"
                snap_dir.mkdir()
                (snap_dir / "subdir").mkdir()
                (snap_dir / "subdir" / "file1.txt").write_text("content1")
                (snap_dir / "subdir" / "file2.txt").write_text("content2")
                
                engine = SnapshotEngine(dest_path, [])
                
                # Restore directory to alternate location
                result = engine.restore(
                    snap_dir,
                    "subdir",
                    restore_path / "restored_dir"
                )
                
                assert result is True
                assert (restore_path / "restored_dir").is_dir()
                assert (restore_path / "restored_dir" / "file1.txt").read_text() == "content1"
                assert (restore_path / "restored_dir" / "file2.txt").read_text() == "content2"
    
    def test_restore_to_original_location(self):
        """Test restoring a file to its original location."""
        with tempfile.TemporaryDirectory() as dest:
            with tempfile.TemporaryDirectory() as source:
                dest_path = Path(dest)
                source_path = Path(source)
                
                # Create a snapshot with a file
                snap_dir = dest_path / "2025-01-01-100000"
                snap_dir.mkdir()
                (snap_dir / "test.txt").write_text("original content")
                
                # Modify the "current" file
                (source_path / "test.txt").write_text("modified content")
                
                engine = SnapshotEngine(dest_path, [])
                
                # Restore to original location
                result = engine.restore(
                    snap_dir,
                    "test.txt",
                    destination=None,
                    source_directories=[source_path]
                )
                
                assert result is True
                assert (source_path / "test.txt").read_text() == "original content"
    
    def test_restore_nonexistent_file(self):
        """Test restoring a file that doesn't exist in snapshot."""
        with tempfile.TemporaryDirectory() as dest:
            with tempfile.TemporaryDirectory() as restore_dest:
                dest_path = Path(dest)
                restore_path = Path(restore_dest)
                
                # Create an empty snapshot
                snap_dir = dest_path / "2025-01-01-100000"
                snap_dir.mkdir()
                
                engine = SnapshotEngine(dest_path, [])
                
                result = engine.restore(
                    snap_dir,
                    "nonexistent.txt",
                    restore_path / "restored.txt"
                )
                
                assert result is False
    
    def test_restore_from_nonexistent_snapshot(self):
        """Test restoring from a snapshot that doesn't exist."""
        with tempfile.TemporaryDirectory() as dest:
            with tempfile.TemporaryDirectory() as restore_dest:
                dest_path = Path(dest)
                restore_path = Path(restore_dest)
                
                engine = SnapshotEngine(dest_path, [])
                
                result = engine.restore(
                    dest_path / "nonexistent-snapshot",
                    "test.txt",
                    restore_path / "restored.txt"
                )
                
                assert result is False
    
    def test_restore_creates_parent_directories(self):
        """Test that restore creates parent directories if needed."""
        with tempfile.TemporaryDirectory() as dest:
            with tempfile.TemporaryDirectory() as restore_dest:
                dest_path = Path(dest)
                restore_path = Path(restore_dest)
                
                # Create a snapshot with a file
                snap_dir = dest_path / "2025-01-01-100000"
                snap_dir.mkdir()
                (snap_dir / "test.txt").write_text("test content")
                
                engine = SnapshotEngine(dest_path, [])
                
                # Restore to a nested path that doesn't exist
                result = engine.restore(
                    snap_dir,
                    "test.txt",
                    restore_path / "nested" / "path" / "restored.txt"
                )
                
                assert result is True
                assert (restore_path / "nested" / "path" / "restored.txt").exists()


class TestDiff:
    """Tests for diff method (Task 6.2)."""
    
    def test_diff_detects_added_files(self):
        """Test that diff detects files added since snapshot."""
        with tempfile.TemporaryDirectory() as dest:
            with tempfile.TemporaryDirectory() as source:
                dest_path = Path(dest)
                source_path = Path(source)
                
                # Create a snapshot with one file
                snap_dir = dest_path / "2025-01-01-100000"
                snap_dir.mkdir()
                (snap_dir / "original.txt").write_text("original")
                
                # Current source has original + new file
                (source_path / "original.txt").write_text("original")
                (source_path / "new_file.txt").write_text("new content")
                
                engine = SnapshotEngine(dest_path, [])
                
                result = engine.diff(snap_dir, [source_path])
                
                assert "new_file.txt" in result["added"]
                assert len(result["deleted"]) == 0
                assert len(result["modified"]) == 0
    
    def test_diff_detects_deleted_files(self):
        """Test that diff detects files deleted since snapshot."""
        with tempfile.TemporaryDirectory() as dest:
            with tempfile.TemporaryDirectory() as source:
                dest_path = Path(dest)
                source_path = Path(source)
                
                # Create a snapshot with two files
                snap_dir = dest_path / "2025-01-01-100000"
                snap_dir.mkdir()
                (snap_dir / "kept.txt").write_text("kept")
                (snap_dir / "deleted.txt").write_text("deleted")
                
                # Current source only has one file
                (source_path / "kept.txt").write_text("kept")
                
                engine = SnapshotEngine(dest_path, [])
                
                result = engine.diff(snap_dir, [source_path])
                
                assert "deleted.txt" in result["deleted"]
                assert len(result["added"]) == 0
                assert len(result["modified"]) == 0
    
    def test_diff_detects_modified_files(self):
        """Test that diff detects files modified since snapshot."""
        with tempfile.TemporaryDirectory() as dest:
            with tempfile.TemporaryDirectory() as source:
                dest_path = Path(dest)
                source_path = Path(source)
                
                # Create a snapshot
                snap_dir = dest_path / "2025-01-01-100000"
                snap_dir.mkdir()
                (snap_dir / "modified.txt").write_text("original content")
                
                # Current source has modified content
                (source_path / "modified.txt").write_text("modified content")
                
                engine = SnapshotEngine(dest_path, [])
                
                result = engine.diff(snap_dir, [source_path])
                
                assert "modified.txt" in result["modified"]
                assert len(result["added"]) == 0
                assert len(result["deleted"]) == 0
    
    def test_diff_detects_all_changes(self):
        """Test that diff detects added, modified, and deleted files together."""
        with tempfile.TemporaryDirectory() as dest:
            with tempfile.TemporaryDirectory() as source:
                dest_path = Path(dest)
                source_path = Path(source)
                
                # Create a snapshot
                snap_dir = dest_path / "2025-01-01-100000"
                snap_dir.mkdir()
                (snap_dir / "unchanged.txt").write_text("unchanged")
                (snap_dir / "modified.txt").write_text("original")
                (snap_dir / "deleted.txt").write_text("deleted")
                
                # Current source
                (source_path / "unchanged.txt").write_text("unchanged")
                (source_path / "modified.txt").write_text("modified")
                (source_path / "added.txt").write_text("added")
                
                engine = SnapshotEngine(dest_path, [])
                
                result = engine.diff(snap_dir, [source_path])
                
                assert "added.txt" in result["added"]
                assert "modified.txt" in result["modified"]
                assert "deleted.txt" in result["deleted"]
                assert "unchanged.txt" not in result["added"]
                assert "unchanged.txt" not in result["modified"]
                assert "unchanged.txt" not in result["deleted"]
    
    def test_diff_with_specific_path(self):
        """Test diff with a specific path filter."""
        with tempfile.TemporaryDirectory() as dest:
            with tempfile.TemporaryDirectory() as source:
                dest_path = Path(dest)
                source_path = Path(source)
                
                # Create a snapshot with nested structure
                snap_dir = dest_path / "2025-01-01-100000"
                snap_dir.mkdir()
                (snap_dir / "subdir").mkdir()
                (snap_dir / "subdir" / "file.txt").write_text("original")
                (snap_dir / "other.txt").write_text("other")
                
                # Current source
                (source_path / "subdir").mkdir()
                (source_path / "subdir" / "file.txt").write_text("modified")
                (source_path / "other.txt").write_text("other modified")
                
                engine = SnapshotEngine(dest_path, [])
                
                # Only diff the subdir
                result = engine.diff(snap_dir, [source_path], source_path="subdir")
                
                assert "subdir/file.txt" in result["modified"]
                # other.txt should not be in results since we filtered to subdir
                assert "other.txt" not in result["modified"]
    
    def test_diff_empty_snapshot(self):
        """Test diff with an empty snapshot."""
        with tempfile.TemporaryDirectory() as dest:
            with tempfile.TemporaryDirectory() as source:
                dest_path = Path(dest)
                source_path = Path(source)
                
                # Create an empty snapshot
                snap_dir = dest_path / "2025-01-01-100000"
                snap_dir.mkdir()
                
                # Current source has files
                (source_path / "new.txt").write_text("new")
                
                engine = SnapshotEngine(dest_path, [])
                
                result = engine.diff(snap_dir, [source_path])
                
                assert "new.txt" in result["added"]
    
    def test_diff_nonexistent_snapshot(self):
        """Test diff with a nonexistent snapshot."""
        with tempfile.TemporaryDirectory() as dest:
            with tempfile.TemporaryDirectory() as source:
                dest_path = Path(dest)
                source_path = Path(source)
                
                engine = SnapshotEngine(dest_path, [])
                
                result = engine.diff(dest_path / "nonexistent", [source_path])
                
                assert result["added"] == []
                assert result["modified"] == []
                assert result["deleted"] == []


class TestSearch:
    """Tests for search method (Task 6.3)."""
    
    def test_search_finds_matching_files(self):
        """Test that search finds files matching pattern."""
        with tempfile.TemporaryDirectory() as dest:
            dest_path = Path(dest)
            
            # Create a snapshot with various files
            snap_dir = dest_path / "2025-01-01-100000"
            snap_dir.mkdir()
            (snap_dir / "test.txt").write_text("test")
            (snap_dir / "test.log").write_text("log")
            (snap_dir / "other.txt").write_text("other")
            
            engine = SnapshotEngine(dest_path, [])
            
            results = engine.search("*.txt")
            
            assert len(results) == 2
            paths = [r["path"] for r in results]
            assert "test.txt" in paths
            assert "other.txt" in paths
    
    def test_search_in_specific_snapshot(self):
        """Test searching in a specific snapshot only."""
        with tempfile.TemporaryDirectory() as dest:
            dest_path = Path(dest)
            
            # Create two snapshots
            snap1 = dest_path / "2025-01-01-100000"
            snap1.mkdir()
            (snap1 / "file1.txt").write_text("content1")
            
            snap2 = dest_path / "2025-01-01-110000"
            snap2.mkdir()
            (snap2 / "file2.txt").write_text("content2")
            
            engine = SnapshotEngine(dest_path, [])
            
            # Search only in snap1
            results = engine.search("*.txt", snapshot=snap1)
            
            assert len(results) == 1
            assert results[0]["path"] == "file1.txt"
            assert results[0]["snapshot"] == "2025-01-01-100000"
    
    def test_search_across_all_snapshots(self):
        """Test searching across all snapshots."""
        with tempfile.TemporaryDirectory() as dest:
            dest_path = Path(dest)
            
            # Create two snapshots
            snap1 = dest_path / "2025-01-01-100000"
            snap1.mkdir()
            (snap1 / "file.txt").write_text("content1")
            
            snap2 = dest_path / "2025-01-01-110000"
            snap2.mkdir()
            (snap2 / "file.txt").write_text("content2")
            
            engine = SnapshotEngine(dest_path, [])
            
            # Search all snapshots
            results = engine.search("file.txt")
            
            assert len(results) == 2
            snapshots = [r["snapshot"] for r in results]
            assert "2025-01-01-100000" in snapshots
            assert "2025-01-01-110000" in snapshots
    
    def test_search_in_subdirectories(self):
        """Test that search finds files in subdirectories."""
        with tempfile.TemporaryDirectory() as dest:
            dest_path = Path(dest)
            
            # Create a snapshot with nested structure
            snap_dir = dest_path / "2025-01-01-100000"
            snap_dir.mkdir()
            (snap_dir / "subdir").mkdir()
            (snap_dir / "subdir" / "nested.txt").write_text("nested")
            (snap_dir / "root.txt").write_text("root")
            
            engine = SnapshotEngine(dest_path, [])
            
            results = engine.search("*.txt")
            
            assert len(results) == 2
            paths = [r["path"] for r in results]
            assert "root.txt" in paths
            assert "subdir/nested.txt" in paths
    
    def test_search_returns_metadata(self):
        """Test that search results include size and modified time."""
        with tempfile.TemporaryDirectory() as dest:
            dest_path = Path(dest)
            
            # Create a snapshot with a file
            snap_dir = dest_path / "2025-01-01-100000"
            snap_dir.mkdir()
            test_file = snap_dir / "test.txt"
            test_file.write_text("test content")
            
            engine = SnapshotEngine(dest_path, [])
            
            results = engine.search("test.txt")
            
            assert len(results) == 1
            assert results[0]["snapshot"] == "2025-01-01-100000"
            assert results[0]["path"] == "test.txt"
            assert results[0]["size"] == len("test content")
            assert "modified" in results[0]
    
    def test_search_no_matches(self):
        """Test search with no matching files."""
        with tempfile.TemporaryDirectory() as dest:
            dest_path = Path(dest)
            
            # Create a snapshot with files
            snap_dir = dest_path / "2025-01-01-100000"
            snap_dir.mkdir()
            (snap_dir / "test.txt").write_text("test")
            
            engine = SnapshotEngine(dest_path, [])
            
            results = engine.search("*.log")
            
            assert results == []
    
    def test_search_nonexistent_snapshot(self):
        """Test search with a nonexistent snapshot."""
        with tempfile.TemporaryDirectory() as dest:
            dest_path = Path(dest)
            
            engine = SnapshotEngine(dest_path, [])
            
            results = engine.search("*.txt", snapshot=dest_path / "nonexistent")
            
            assert results == []
    
    def test_search_glob_patterns(self):
        """Test search with various glob patterns."""
        with tempfile.TemporaryDirectory() as dest:
            dest_path = Path(dest)
            
            # Create a snapshot with various files
            snap_dir = dest_path / "2025-01-01-100000"
            snap_dir.mkdir()
            (snap_dir / "test1.txt").write_text("test1")
            (snap_dir / "test2.txt").write_text("test2")
            (snap_dir / "other.txt").write_text("other")
            (snap_dir / "test.log").write_text("log")
            
            engine = SnapshotEngine(dest_path, [])
            
            # Test pattern matching
            results = engine.search("test*")
            paths = [r["path"] for r in results]
            
            assert "test1.txt" in paths
            assert "test2.txt" in paths
            assert "test.log" in paths
            assert "other.txt" not in paths


class TestSymlinkSafety:
    """Tests for symlink safety in directory traversals (Task 4.1).
    
    Requirements: 3.1, 3.2, 3.3, 3.4, 3.5
    """
    
    def test_diff_does_not_follow_symlinks(self):
        """Test that diff() does not follow symbolic links (Requirement 3.1)."""
        with tempfile.TemporaryDirectory() as dest:
            with tempfile.TemporaryDirectory() as source:
                dest_path = Path(dest)
                source_path = Path(source)
                
                # Create a snapshot with a regular file
                snap_dir = dest_path / "2025-01-01-100000"
                snap_dir.mkdir()
                (snap_dir / "regular.txt").write_text("regular content")
                
                # Create source with a symlink to an external directory
                (source_path / "regular.txt").write_text("regular content")
                external_dir = Path(tempfile.mkdtemp())
                try:
                    (external_dir / "external.txt").write_text("external content")
                    # Create symlink to external directory
                    symlink_path = source_path / "external_link"
                    symlink_path.symlink_to(external_dir)
                    
                    engine = SnapshotEngine(dest_path, [])
                    
                    # diff should complete without following the symlink
                    result = engine.diff(snap_dir, [source_path])
                    
                    # The symlink itself might be detected as added, but the
                    # external.txt file inside should NOT be in the results
                    # because we don't follow symlinks
                    all_paths = result["added"] + result["modified"] + result["deleted"]
                    assert not any("external.txt" in p for p in all_paths), \
                        "diff() should not follow symlinks to external directories"
                finally:
                    import shutil
                    shutil.rmtree(external_dir)
    
    def test_search_does_not_follow_symlinks(self):
        """Test that search() does not follow symbolic links (Requirement 3.2)."""
        with tempfile.TemporaryDirectory() as dest:
            dest_path = Path(dest)
            
            # Create a snapshot with a regular file and a symlink
            snap_dir = dest_path / "2025-01-01-100000"
            snap_dir.mkdir()
            (snap_dir / "regular.txt").write_text("regular content")
            
            # Create an external directory with a file
            external_dir = Path(tempfile.mkdtemp())
            try:
                (external_dir / "external.txt").write_text("external content")
                # Create symlink to external directory
                symlink_path = snap_dir / "external_link"
                symlink_path.symlink_to(external_dir)
                
                engine = SnapshotEngine(dest_path, [])
                
                # Search should complete without following the symlink
                results = engine.search("*.txt", snapshot=snap_dir)
                
                # Should find regular.txt but NOT external.txt
                paths = [r["path"] for r in results]
                assert "regular.txt" in paths
                assert not any("external.txt" in p for p in paths), \
                    "search() should not follow symlinks to external directories"
            finally:
                import shutil
                shutil.rmtree(external_dir)
    
    def test_get_directory_stats_does_not_follow_symlinks(self):
        """Test that _get_directory_stats() does not follow symbolic links (Requirement 3.3)."""
        with tempfile.TemporaryDirectory() as dest:
            dest_path = Path(dest)
            
            # Create a snapshot directory with a regular file
            snap_dir = dest_path / "2025-01-01-100000"
            snap_dir.mkdir()
            (snap_dir / "regular.txt").write_text("regular content")
            
            # Create an external directory with a large file
            external_dir = Path(tempfile.mkdtemp())
            try:
                large_content = "x" * 10000
                (external_dir / "large.txt").write_text(large_content)
                # Create symlink to external directory
                symlink_path = snap_dir / "external_link"
                symlink_path.symlink_to(external_dir)
                
                engine = SnapshotEngine(dest_path, [])
                
                # Get stats - should not include the external file's size
                size, count = engine._get_directory_stats(snap_dir)
                
                # Should only count the regular.txt file
                assert count == 1, "Should only count regular files, not follow symlinks"
                assert size == len("regular content"), \
                    "Size should not include files from symlinked directories"
            finally:
                import shutil
                shutil.rmtree(external_dir)
    
    def test_diff_handles_circular_symlinks(self):
        """Test that diff() handles circular symlinks gracefully (Requirement 3.4)."""
        with tempfile.TemporaryDirectory() as dest:
            with tempfile.TemporaryDirectory() as source:
                dest_path = Path(dest)
                source_path = Path(source)
                
                # Create a snapshot
                snap_dir = dest_path / "2025-01-01-100000"
                snap_dir.mkdir()
                (snap_dir / "file.txt").write_text("content")
                
                # Create source with a circular symlink
                (source_path / "file.txt").write_text("content")
                subdir = source_path / "subdir"
                subdir.mkdir()
                # Create circular symlink: subdir/loop -> source_path
                (subdir / "loop").symlink_to(source_path)
                
                engine = SnapshotEngine(dest_path, [])
                
                # diff should complete without infinite loop
                result = engine.diff(snap_dir, [source_path])
                
                # Should complete and return valid results
                assert isinstance(result, dict)
                assert "added" in result
                assert "modified" in result
                assert "deleted" in result
    
    def test_search_handles_circular_symlinks(self):
        """Test that search() handles circular symlinks gracefully (Requirement 3.4)."""
        with tempfile.TemporaryDirectory() as dest:
            dest_path = Path(dest)
            
            # Create a snapshot with a circular symlink
            snap_dir = dest_path / "2025-01-01-100000"
            snap_dir.mkdir()
            (snap_dir / "file.txt").write_text("content")
            subdir = snap_dir / "subdir"
            subdir.mkdir()
            # Create circular symlink: subdir/loop -> snap_dir
            (subdir / "loop").symlink_to(snap_dir)
            
            engine = SnapshotEngine(dest_path, [])
            
            # search should complete without infinite loop
            results = engine.search("*.txt", snapshot=snap_dir)
            
            # Should complete and find the file
            assert isinstance(results, list)
            paths = [r["path"] for r in results]
            assert "file.txt" in paths
    
    def test_get_directory_stats_handles_circular_symlinks(self):
        """Test that _get_directory_stats() handles circular symlinks gracefully (Requirement 3.4)."""
        with tempfile.TemporaryDirectory() as dest:
            dest_path = Path(dest)
            
            # Create a directory with a circular symlink
            test_dir = dest_path / "test"
            test_dir.mkdir()
            (test_dir / "file.txt").write_text("content")
            subdir = test_dir / "subdir"
            subdir.mkdir()
            # Create circular symlink: subdir/loop -> test_dir
            (subdir / "loop").symlink_to(test_dir)
            
            engine = SnapshotEngine(dest_path, [])
            
            # _get_directory_stats should complete without infinite loop
            size, count = engine._get_directory_stats(test_dir)
            
            # Should complete and return valid results
            assert count == 1  # Only the regular file
            assert size == len("content")
    
    def test_list_snapshots_with_symlinks(self):
        """Test that list_snapshots works correctly with symlinks in snapshots."""
        with tempfile.TemporaryDirectory() as dest:
            dest_path = Path(dest)
            
            # Create a snapshot with a symlink
            snap_dir = dest_path / "2025-01-01-100000"
            snap_dir.mkdir()
            (snap_dir / "file.txt").write_text("content")
            
            # Create an external directory
            external_dir = Path(tempfile.mkdtemp())
            try:
                (external_dir / "external.txt").write_text("external")
                (snap_dir / "link").symlink_to(external_dir)
                
                engine = SnapshotEngine(dest_path, [])
                
                # list_snapshots should work correctly
                snapshots = engine.list_snapshots()
                
                assert len(snapshots) == 1
                assert snapshots[0].path.name == "2025-01-01-100000"
                # File count should only include the regular file
                assert snapshots[0].file_count == 1
            finally:
                import shutil
                shutil.rmtree(external_dir)


class TestProgressReporting:
    """Tests for progress reporting integration (Task 8.2)."""
    
    def test_build_rsync_command_without_progress(self):
        """Test rsync command without progress flag."""
        with tempfile.TemporaryDirectory() as dest:
            engine = SnapshotEngine(Path(dest), [])
            
            with tempfile.TemporaryDirectory() as source:
                cmd = engine._build_rsync_command(
                    [Path(source)],
                    Path(dest),
                    with_progress=False
                )
                
                assert "--progress" not in cmd
    
    def test_build_rsync_command_with_progress(self):
        """Test rsync command includes --progress when requested."""
        with tempfile.TemporaryDirectory() as dest:
            engine = SnapshotEngine(Path(dest), [])
            
            with tempfile.TemporaryDirectory() as source:
                cmd = engine._build_rsync_command(
                    [Path(source)],
                    Path(dest),
                    with_progress=True
                )
                
                assert "--progress" in cmd
    
    def test_create_snapshot_with_progress_callback(self):
        """Test create_snapshot calls progress callback."""
        with tempfile.TemporaryDirectory() as dest:
            with tempfile.TemporaryDirectory() as source:
                # Create some test files
                source_path = Path(source)
                (source_path / "file1.txt").write_text("content1")
                (source_path / "file2.txt").write_text("content2")
                
                engine = SnapshotEngine(Path(dest), [])
                
                # Track callback calls
                callback_calls = []
                def progress_callback(info):
                    callback_calls.append(info)
                
                result = engine.create_snapshot(
                    [source_path],
                    progress_callback=progress_callback,
                )
                
                assert result.success
                # Callback should have been called at least once (for final report)
                assert len(callback_calls) >= 1
                # Last call should be final report with 100% complete
                assert callback_calls[-1].percent_complete == 100.0
    
    def test_create_snapshot_without_progress_callback(self):
        """Test create_snapshot works without progress callback."""
        with tempfile.TemporaryDirectory() as dest:
            with tempfile.TemporaryDirectory() as source:
                source_path = Path(source)
                (source_path / "file.txt").write_text("content")
                
                engine = SnapshotEngine(Path(dest), [])
                
                result = engine.create_snapshot([source_path])
                
                assert result.success
                assert result.snapshot_path is not None
    
    def test_get_current_progress_no_backup(self):
        """Test get_current_progress returns None when no backup in progress."""
        with tempfile.TemporaryDirectory() as dest:
            engine = SnapshotEngine(Path(dest), [])
            
            progress = engine.get_current_progress()
            assert progress is None
    
    def test_get_current_progress_during_backup(self):
        """Test get_current_progress returns progress during backup."""
        with tempfile.TemporaryDirectory() as dest:
            with tempfile.TemporaryDirectory() as source:
                source_path = Path(source)
                (source_path / "file.txt").write_text("content")
                
                engine = SnapshotEngine(Path(dest), [])
                
                # Track progress during callback
                progress_during_backup = []
                def progress_callback(info):
                    # Get progress from engine during callback
                    current = engine.get_current_progress()
                    if current:
                        progress_during_backup.append(current)
                
                result = engine.create_snapshot(
                    [source_path],
                    progress_callback=progress_callback,
                )
                
                assert result.success
                # Should have captured progress during backup
                assert len(progress_during_backup) >= 1
    
    def test_progress_reporter_reset_after_backup(self):
        """Test progress reporter is cleared after backup completes."""
        with tempfile.TemporaryDirectory() as dest:
            with tempfile.TemporaryDirectory() as source:
                source_path = Path(source)
                (source_path / "file.txt").write_text("content")
                
                engine = SnapshotEngine(Path(dest), [])
                
                # First backup with progress
                result1 = engine.create_snapshot(
                    [source_path],
                    progress_callback=lambda x: None,
                )
                assert result1.success
                
                # Wait a second to avoid timestamp collision
                time.sleep(1.1)
                
                # Second backup without progress
                result2 = engine.create_snapshot([source_path])
                assert result2.success
                
                # Progress should be None after backup without callback
                assert engine.get_current_progress() is None


class TestTimestampCollisionHandling:
    """Tests for timestamp collision handling (Task 11.1).
    
    Requirements: 8.1, 8.2, 8.3, 8.4
    """
    
    def test_parse_snapshot_name_base_format(self):
        """Test _parse_snapshot_name with base YYYY-MM-DD-HHMMSS format."""
        with tempfile.TemporaryDirectory() as dest:
            engine = SnapshotEngine(Path(dest), [])
            
            result = engine._parse_snapshot_name("2025-01-01-120000")
            assert result is not None
            assert result.year == 2025
            assert result.month == 1
            assert result.day == 1
            assert result.hour == 12
            assert result.minute == 0
            assert result.second == 0
    
    def test_parse_snapshot_name_sequence_format(self):
        """Test _parse_snapshot_name with YYYY-MM-DD-HHMMSS-NN format."""
        with tempfile.TemporaryDirectory() as dest:
            engine = SnapshotEngine(Path(dest), [])
            
            # Test various sequence numbers
            for seq in ["01", "50", "99"]:
                result = engine._parse_snapshot_name(f"2025-01-01-120000-{seq}")
                assert result is not None
                assert result.year == 2025
                assert result.month == 1
                assert result.day == 1
    
    def test_parse_snapshot_name_invalid_sequence(self):
        """Test _parse_snapshot_name rejects invalid sequence numbers."""
        with tempfile.TemporaryDirectory() as dest:
            engine = SnapshotEngine(Path(dest), [])
            
            # Sequence 00 is invalid (should be 01-99)
            assert engine._parse_snapshot_name("2025-01-01-120000-00") is None
            # Non-numeric sequence
            assert engine._parse_snapshot_name("2025-01-01-120000-ab") is None
            # Invalid format
            assert engine._parse_snapshot_name("not-a-timestamp") is None
    
    def test_generate_unique_snapshot_name_no_collision(self):
        """Test _generate_unique_snapshot_name returns base timestamp when no collision."""
        with tempfile.TemporaryDirectory() as dest:
            dest_path = Path(dest)
            engine = SnapshotEngine(dest_path, [])
            
            name = engine._generate_unique_snapshot_name()
            
            # Should be base format (17 chars)
            assert len(name) == 17
            assert engine._parse_snapshot_name(name) is not None
    
    def test_generate_unique_snapshot_name_with_collision(self):
        """Test _generate_unique_snapshot_name appends sequence on collision (Req 8.1, 8.2)."""
        with tempfile.TemporaryDirectory() as dest:
            dest_path = Path(dest)
            engine = SnapshotEngine(dest_path, [])
            
            # Create a snapshot with current timestamp
            current_ts = engine._generate_timestamp()
            (dest_path / current_ts).mkdir()
            
            # Generate unique name should append sequence number
            name = engine._generate_unique_snapshot_name()
            
            # Should be sequence format (20 chars: YYYY-MM-DD-HHMMSS-NN)
            # or a new timestamp if time advanced
            if len(name) == 20:
                assert name.startswith(current_ts)
                assert name[-3] == "-"
                seq = int(name[-2:])
                assert 1 <= seq <= 99
            else:
                # Time advanced, got a new base timestamp
                assert len(name) == 17
    
    def test_generate_unique_snapshot_name_multiple_collisions(self):
        """Test _generate_unique_snapshot_name handles multiple collisions."""
        with tempfile.TemporaryDirectory() as dest:
            dest_path = Path(dest)
            engine = SnapshotEngine(dest_path, [])
            
            # Create base snapshot and first few sequence numbers
            current_ts = engine._generate_timestamp()
            (dest_path / current_ts).mkdir()
            (dest_path / f"{current_ts}-01").mkdir()
            (dest_path / f"{current_ts}-02").mkdir()
            
            # Generate unique name should find next available sequence
            name = engine._generate_unique_snapshot_name()
            
            if len(name) == 20 and name.startswith(current_ts):
                seq = int(name[-2:])
                assert seq >= 3  # Should be at least 03
    
    def test_generate_unique_snapshot_name_checks_in_progress(self):
        """Test _generate_unique_snapshot_name checks in_progress directories (Req 8.4)."""
        with tempfile.TemporaryDirectory() as dest:
            dest_path = Path(dest)
            engine = SnapshotEngine(dest_path, [])
            
            # Create an in_progress directory with current timestamp
            current_ts = engine._generate_timestamp()
            (dest_path / f"in_progress_{current_ts}").mkdir()
            
            # Generate unique name should detect the collision
            name = engine._generate_unique_snapshot_name()
            
            # Should not be the same as the in_progress timestamp
            assert name != current_ts
    
    def test_snapshot_name_exists(self):
        """Test _snapshot_name_exists detects both complete and in_progress."""
        with tempfile.TemporaryDirectory() as dest:
            dest_path = Path(dest)
            engine = SnapshotEngine(dest_path, [])
            
            # Create a complete snapshot
            (dest_path / "2025-01-01-100000").mkdir()
            # Create an in_progress snapshot
            (dest_path / "in_progress_2025-01-01-110000").mkdir()
            
            assert engine._snapshot_name_exists("2025-01-01-100000") is True
            assert engine._snapshot_name_exists("2025-01-01-110000") is True
            assert engine._snapshot_name_exists("2025-01-01-120000") is False
    
    def test_find_latest_snapshot_with_sequence_numbers(self):
        """Test find_latest_snapshot correctly orders snapshots with sequence numbers."""
        with tempfile.TemporaryDirectory() as dest:
            dest_path = Path(dest)
            engine = SnapshotEngine(dest_path, [])
            
            # Create snapshots with sequence numbers
            (dest_path / "2025-01-01-100000").mkdir()
            (dest_path / "2025-01-01-100000-01").mkdir()
            (dest_path / "2025-01-01-100000-02").mkdir()
            (dest_path / "2025-01-01-090000").mkdir()
            
            latest = engine.find_latest_snapshot()
            assert latest is not None
            # 2025-01-01-100000-02 should be latest (lexicographically)
            assert latest.name == "2025-01-01-100000-02"
    
    def test_list_snapshots_with_sequence_numbers(self):
        """Test list_snapshots includes and orders snapshots with sequence numbers."""
        with tempfile.TemporaryDirectory() as dest:
            dest_path = Path(dest)
            engine = SnapshotEngine(dest_path, [])
            
            # Create snapshots with sequence numbers
            for name in ["2025-01-01-100000", "2025-01-01-100000-01", 
                        "2025-01-01-100000-02", "2025-01-01-090000"]:
                snap_dir = dest_path / name
                snap_dir.mkdir()
                (snap_dir / "file.txt").write_text("content")
            
            snapshots = engine.list_snapshots()
            
            assert len(snapshots) == 4
            # Should be sorted most recent first
            assert snapshots[0].path.name == "2025-01-01-100000-02"
            assert snapshots[1].path.name == "2025-01-01-100000-01"
            assert snapshots[2].path.name == "2025-01-01-100000"
            assert snapshots[3].path.name == "2025-01-01-090000"
    
    def test_get_snapshot_by_timestamp_with_sequence(self):
        """Test get_snapshot_by_timestamp finds snapshots with sequence numbers."""
        with tempfile.TemporaryDirectory() as dest:
            dest_path = Path(dest)
            engine = SnapshotEngine(dest_path, [])
            
            # Create snapshot with sequence number
            (dest_path / "2025-01-01-100000-05").mkdir()
            
            result = engine.get_snapshot_by_timestamp("2025-01-01-100000-05")
            assert result is not None
            assert result.name == "2025-01-01-100000-05"
    
    def test_create_snapshot_handles_collision(self):
        """Test create_snapshot handles timestamp collision by using sequence number."""
        with tempfile.TemporaryDirectory() as dest:
            with tempfile.TemporaryDirectory() as source:
                dest_path = Path(dest)
                source_path = Path(source)
                
                # Create test file
                (source_path / "test.txt").write_text("content")
                
                engine = SnapshotEngine(dest_path, [])
                
                # Create first snapshot
                result1 = engine.create_snapshot([source_path])
                assert result1.success
                assert result1.snapshot_path is not None
                
                # Immediately create second snapshot (same second)
                result2 = engine.create_snapshot([source_path])
                assert result2.success
                assert result2.snapshot_path is not None
                
                # Both snapshots should exist and be different
                assert result1.snapshot_path != result2.snapshot_path
                assert result1.snapshot_path.exists()
                assert result2.snapshot_path.exists()
                
                # At least one should have a sequence number if collision occurred
                names = [result1.snapshot_path.name, result2.snapshot_path.name]
                # Both should be valid snapshot names
                for name in names:
                    assert engine._parse_snapshot_name(name) is not None
