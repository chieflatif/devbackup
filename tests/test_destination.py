"""Unit tests for destination validation.

Feature: macos-incremental-backup
"""

import os
import stat
import tempfile
from pathlib import Path

import pytest

from devbackup.destination import (
    DestinationError,
    get_available_space,
    is_volume_mounted,
    is_writable,
    validate_destination,
)


class TestValidateDestination:
    """Unit tests for validate_destination function.
    
    **Validates: Requirements 3.1, 3.2, 3.3, 3.4**
    """

    def test_existing_writable_path_succeeds(self):
        """Test that an existing writable directory passes validation."""
        with tempfile.TemporaryDirectory() as tmpdir:
            dest = Path(tmpdir)
            
            # Should not raise any exception
            validate_destination(dest)

    def test_non_existent_path_raises_error(self):
        """Test that a non-existent path raises DestinationError."""
        non_existent = Path("/tmp/devbackup_test_nonexistent_12345")
        
        # Ensure it doesn't exist
        if non_existent.exists():
            non_existent.rmdir()
        
        with pytest.raises(DestinationError) as exc_info:
            validate_destination(non_existent)
        
        assert "not found" in str(exc_info.value).lower()
        assert str(non_existent) in str(exc_info.value)

    def test_non_writable_path_raises_error(self):
        """Test that a non-writable path raises DestinationError."""
        with tempfile.TemporaryDirectory() as tmpdir:
            dest = Path(tmpdir) / "readonly"
            dest.mkdir()
            
            # Make directory read-only
            original_mode = dest.stat().st_mode
            try:
                os.chmod(dest, stat.S_IRUSR | stat.S_IXUSR)
                
                with pytest.raises(DestinationError) as exc_info:
                    validate_destination(dest)
                
                assert "not writable" in str(exc_info.value).lower()
                assert str(dest) in str(exc_info.value)
            finally:
                # Restore permissions for cleanup
                os.chmod(dest, original_mode)

    def test_file_instead_of_directory_raises_error(self):
        """Test that a file (not directory) raises DestinationError."""
        with tempfile.TemporaryDirectory() as tmpdir:
            file_path = Path(tmpdir) / "not_a_dir.txt"
            file_path.touch()
            
            with pytest.raises(DestinationError) as exc_info:
                validate_destination(file_path)
            
            assert "not a directory" in str(exc_info.value).lower()


class TestIsVolumeMounted:
    """Unit tests for is_volume_mounted function.
    
    **Validates: Requirements 3.5**
    """

    def test_local_path_returns_true(self):
        """Test that a local filesystem path returns True."""
        with tempfile.TemporaryDirectory() as tmpdir:
            assert is_volume_mounted(Path(tmpdir))

    def test_tmp_path_returns_true(self):
        """Test that /tmp path returns True."""
        assert is_volume_mounted(Path("/tmp"))

    def test_home_path_returns_true(self):
        """Test that home directory returns True."""
        assert is_volume_mounted(Path.home())

    def test_unmounted_volume_returns_false(self):
        """Test that a non-existent volume under /Volumes returns False."""
        # Use a volume name that definitely doesn't exist
        fake_volume = Path("/Volumes/DevBackupTestVolume12345NonExistent")
        assert not is_volume_mounted(fake_volume)

    def test_mounted_volume_returns_true(self):
        """Test that /Volumes/Macintosh HD (if exists) returns True."""
        # This test checks a common macOS volume
        # Skip if not on macOS or volume doesn't exist
        mac_hd = Path("/Volumes/Macintosh HD")
        if mac_hd.exists():
            assert is_volume_mounted(mac_hd)

    def test_path_under_unmounted_volume_returns_false(self):
        """Test that a path under an unmounted volume returns False."""
        fake_path = Path("/Volumes/NonExistentVolume12345/some/nested/path")
        assert not is_volume_mounted(fake_path)


class TestIsWritable:
    """Unit tests for is_writable function.
    
    **Validates: Requirements 3.2, 3.4**
    """

    def test_writable_directory_returns_true(self):
        """Test that a writable directory returns True."""
        with tempfile.TemporaryDirectory() as tmpdir:
            assert is_writable(Path(tmpdir))

    def test_readonly_directory_returns_false(self):
        """Test that a read-only directory returns False."""
        with tempfile.TemporaryDirectory() as tmpdir:
            readonly_dir = Path(tmpdir) / "readonly"
            readonly_dir.mkdir()
            
            original_mode = readonly_dir.stat().st_mode
            try:
                os.chmod(readonly_dir, stat.S_IRUSR | stat.S_IXUSR)
                assert not is_writable(readonly_dir)
            finally:
                os.chmod(readonly_dir, original_mode)


class TestGetAvailableSpace:
    """Unit tests for get_available_space function."""

    def test_returns_positive_value_for_valid_path(self):
        """Test that available space is returned for a valid path."""
        with tempfile.TemporaryDirectory() as tmpdir:
            space = get_available_space(Path(tmpdir))
            assert space > 0

    def test_raises_error_for_invalid_path(self):
        """Test that DestinationError is raised for invalid path."""
        invalid_path = Path("/nonexistent/path/12345")
        
        with pytest.raises(DestinationError) as exc_info:
            get_available_space(invalid_path)
        
        assert "unable to determine" in str(exc_info.value).lower()
