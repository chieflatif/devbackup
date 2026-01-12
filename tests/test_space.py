"""Unit tests for space validation module.

Tests the SpaceValidator functionality for disk space validation before backups.
"""

import tempfile
from pathlib import Path

import pytest

from devbackup.space import (
    SpaceError,
    SpaceValidationResult,
    estimate_backup_size,
    validate_space,
)


class TestEstimateBackupSize:
    """Tests for estimate_backup_size function."""
    
    def test_empty_sources(self):
        """Empty source list returns zero size."""
        result = estimate_backup_size([], [])
        assert result == 0
    
    def test_nonexistent_source(self):
        """Nonexistent source is skipped."""
        result = estimate_backup_size([Path("/nonexistent/path")], [])
        assert result == 0
    
    def test_single_file_source(self):
        """Single file source returns file size."""
        with tempfile.TemporaryDirectory() as tmpdir:
            file_path = Path(tmpdir) / "test.txt"
            content = "Hello, World!"
            file_path.write_text(content)
            
            result = estimate_backup_size([file_path], [])
            assert result == len(content)
    
    def test_directory_with_files(self):
        """Directory with files returns total size."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            
            # Create some files
            (tmpdir_path / "file1.txt").write_text("content1")
            (tmpdir_path / "file2.txt").write_text("content22")
            
            result = estimate_backup_size([tmpdir_path], [])
            assert result == len("content1") + len("content22")
    
    def test_nested_directories(self):
        """Nested directories are included in size calculation."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            
            # Create nested structure
            subdir = tmpdir_path / "subdir"
            subdir.mkdir()
            (tmpdir_path / "root.txt").write_text("root")
            (subdir / "nested.txt").write_text("nested")
            
            result = estimate_backup_size([tmpdir_path], [])
            assert result == len("root") + len("nested")
    
    def test_exclude_pattern_file(self):
        """Files matching exclude patterns are excluded."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            
            (tmpdir_path / "include.txt").write_text("include")
            (tmpdir_path / "exclude.log").write_text("exclude")
            
            result = estimate_backup_size([tmpdir_path], ["*.log"])
            assert result == len("include")
    
    def test_exclude_pattern_directory(self):
        """Directories matching exclude patterns are excluded."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            
            # Create node_modules directory (should be excluded)
            node_modules = tmpdir_path / "node_modules"
            node_modules.mkdir()
            (node_modules / "package.json").write_text("large content here")
            
            # Create src directory (should be included)
            src = tmpdir_path / "src"
            src.mkdir()
            (src / "main.py").write_text("code")
            
            result = estimate_backup_size([tmpdir_path], ["node_modules/"])
            assert result == len("code")
    
    def test_multiple_sources(self):
        """Multiple source directories are summed."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            
            dir1 = tmpdir_path / "dir1"
            dir2 = tmpdir_path / "dir2"
            dir1.mkdir()
            dir2.mkdir()
            
            (dir1 / "file1.txt").write_text("content1")
            (dir2 / "file2.txt").write_text("content2")
            
            result = estimate_backup_size([dir1, dir2], [])
            assert result == len("content1") + len("content2")
    
    def test_symlink_not_followed(self):
        """Symbolic links are not followed."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            
            # Create a file
            real_file = tmpdir_path / "real.txt"
            real_file.write_text("real content")
            
            # Create a symlink
            symlink = tmpdir_path / "link.txt"
            symlink.symlink_to(real_file)
            
            # Size should only include the real file, not the symlink target again
            result = estimate_backup_size([tmpdir_path], [])
            assert result == len("real content")


class TestValidateSpace:
    """Tests for validate_space function."""
    
    def test_sufficient_space(self):
        """Returns success when space is sufficient."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            
            # Create a small source file
            source = tmpdir_path / "source"
            source.mkdir()
            (source / "file.txt").write_text("small")
            
            dest = tmpdir_path / "dest"
            
            result = validate_space(dest, [source], [])
            
            assert result.sufficient is True
            assert result.available_bytes > 0
            assert result.estimated_bytes == len("small")
    
    def test_insufficient_space_raises_error(self):
        """Raises SpaceError when space is insufficient."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            
            source = tmpdir_path / "source"
            source.mkdir()
            # Create a file with known size
            (source / "file.txt").write_text("content")
            
            dest = tmpdir_path / "dest"
            
            # Get actual available space
            import shutil
            disk_usage = shutil.disk_usage(tmpdir_path)
            available = disk_usage.free
            
            # Calculate buffer_percent that would require more than available
            # required = estimated * (1 + buffer_percent)
            # We want required > available
            # So buffer_percent > (available / estimated) - 1
            estimated = len("content")
            # Make required = available + 1GB to ensure it fails
            required = available + 1024 * 1024 * 1024
            buffer_percent = (required / estimated) - 1
            
            with pytest.raises(SpaceError) as exc_info:
                validate_space(dest, [source], [], buffer_percent=buffer_percent)
            
            assert exc_info.value.available_bytes > 0
            assert exc_info.value.required_bytes > exc_info.value.available_bytes
    
    def test_low_space_warning(self):
        """Returns warning when space is below minimum threshold."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            
            source = tmpdir_path / "source"
            source.mkdir()
            (source / "file.txt").write_text("small")
            
            dest = tmpdir_path / "dest"
            
            # Set min_free_bytes to something impossibly high to trigger warning
            result = validate_space(
                dest, 
                [source], 
                [], 
                min_free_bytes=10 * 1024 * 1024 * 1024 * 1024  # 10TB
            )
            
            assert result.sufficient is True
            assert result.warning is not None
            assert "Low disk space" in result.warning
    
    def test_destination_parent_checked(self):
        """Checks parent directory if destination doesn't exist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            
            source = tmpdir_path / "source"
            source.mkdir()
            (source / "file.txt").write_text("content")
            
            # Destination that doesn't exist yet
            dest = tmpdir_path / "nonexistent" / "backup"
            
            result = validate_space(dest, [source], [])
            
            assert result.sufficient is True
            assert result.available_bytes > 0
    
    def test_exclude_patterns_applied(self):
        """Exclude patterns reduce estimated size."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            
            source = tmpdir_path / "source"
            source.mkdir()
            (source / "include.txt").write_text("include")
            (source / "exclude.log").write_text("exclude this content")
            
            dest = tmpdir_path / "dest"
            
            result = validate_space(dest, [source], ["*.log"])
            
            # Estimated size should only include the .txt file
            assert result.estimated_bytes == len("include")


class TestSpaceError:
    """Tests for SpaceError exception."""
    
    def test_error_attributes(self):
        """SpaceError contains available and required bytes."""
        error = SpaceError("Test error", available_bytes=100, required_bytes=200)
        
        assert str(error) == "Test error"
        assert error.available_bytes == 100
        assert error.required_bytes == 200
