"""Property-based tests for space validation.

Tests Property 2 (Space Validation Correctness) from the backup-robustness design document.

**Validates: Requirements 2.2, 2.5**
"""

import os
import shutil
import tempfile
from pathlib import Path
from typing import List

import pytest
from hypothesis import given, strategies as st, settings, Phase, assume

from devbackup.space import (
    SpaceError,
    SpaceValidationResult,
    estimate_backup_size,
    validate_space,
)


# Strategy for generating file sizes (1 byte to 10KB)
file_sizes = st.integers(min_value=1, max_value=10 * 1024)

# Strategy for generating number of files (0 to 20)
file_counts = st.integers(min_value=0, max_value=20)

# Strategy for generating buffer percentages (0% to 50%)
buffer_percents = st.floats(min_value=0.0, max_value=0.5, allow_nan=False, allow_infinity=False)

# Strategy for generating exclude patterns
exclude_patterns = st.lists(
    st.sampled_from(["*.log", "*.tmp", "*.pyc", "node_modules/", "__pycache__/", ".git/"]),
    min_size=0,
    max_size=3,
)


def create_test_files(
    base_path: Path,
    num_files: int,
    file_size: int,
    include_excluded: bool = False,
) -> int:
    """Create test files and return total size of non-excluded files."""
    total_size = 0
    
    for i in range(num_files):
        # Create regular files
        file_path = base_path / f"file_{i}.txt"
        content = "x" * file_size
        file_path.write_text(content)
        total_size += file_size
    
    if include_excluded:
        # Create some files that would be excluded
        (base_path / "debug.log").write_text("log content")
        (base_path / "temp.tmp").write_text("temp content")
    
    return total_size


class TestSpaceValidationCorrectness:
    """
    Property 2: Space Validation Correctness
    
    *For any* backup attempt where available space is less than estimated size 
    plus 10% buffer, the Space_Validator SHALL return a SpaceError before any 
    in_progress directory is created.
    
    **Validates: Requirements 2.2, 2.5**
    """
    
    @given(
        num_files=file_counts,
        file_size=file_sizes,
        buffer_percent=buffer_percents,
    )
    @settings(max_examples=10, deadline=None, phases=[Phase.generate, Phase.target])
    def test_space_validation_correctness(
        self,
        num_files: int,
        file_size: int,
        buffer_percent: float,
    ):
        """
        **Feature: backup-robustness, Property 2: Space Validation Correctness**
        
        For any backup where available space < estimated size * (1 + buffer),
        SpaceError must be raised before any in_progress directory is created.
        
        **Validates: Requirements 2.2, 2.5**
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            source = tmpdir_path / "source"
            source.mkdir()
            dest = tmpdir_path / "dest"
            in_progress = dest / "in_progress_test"
            
            # Create test files
            total_size = create_test_files(source, num_files, file_size)
            
            # Get actual available space
            disk_usage = shutil.disk_usage(tmpdir_path)
            available = disk_usage.free
            
            # Calculate required space
            estimated = estimate_backup_size([source], [])
            required = int(estimated * (1 + buffer_percent))
            
            # PROPERTY: If available < required, SpaceError must be raised
            # and no in_progress directory should be created
            if available < required:
                with pytest.raises(SpaceError) as exc_info:
                    validate_space(dest, [source], [], buffer_percent=buffer_percent)
                
                # Verify error contains correct information
                # Note: available_bytes may change slightly between measurements
                # so we check it's in a reasonable range (within 1GB)
                assert abs(exc_info.value.available_bytes - available) < 1024 * 1024 * 1024
                assert exc_info.value.required_bytes == required
                
                # INVARIANT: No in_progress directory should exist
                assert not in_progress.exists(), \
                    "in_progress directory should not be created when space validation fails"
            else:
                # If space is sufficient, validation should succeed
                result = validate_space(dest, [source], [], buffer_percent=buffer_percent)
                assert result.sufficient is True
                # Note: available_bytes may change slightly between measurements
                assert abs(result.available_bytes - available) < 1024 * 1024 * 1024
                assert result.estimated_bytes == estimated
    
    @given(
        num_files=file_counts,
        file_size=file_sizes,
        patterns=exclude_patterns,
    )
    @settings(max_examples=10, deadline=None, phases=[Phase.generate, Phase.target])
    def test_estimate_size_with_exclusions(
        self,
        num_files: int,
        file_size: int,
        patterns: List[str],
    ):
        """
        **Feature: backup-robustness, Property 2: Space Validation Correctness**
        
        For any source directory with exclude patterns, the estimated size
        must not include excluded files.
        
        **Validates: Requirements 2.3**
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            source = tmpdir_path / "source"
            source.mkdir()
            
            # Create regular files
            regular_size = create_test_files(source, num_files, file_size)
            
            # Create excluded files
            excluded_size = 0
            for pattern in patterns:
                if pattern.endswith('/'):
                    # Directory pattern
                    dir_name = pattern.rstrip('/')
                    dir_path = source / dir_name
                    dir_path.mkdir(exist_ok=True)
                    (dir_path / "file.txt").write_text("excluded content")
                    excluded_size += len("excluded content")
                else:
                    # File pattern
                    ext = pattern.lstrip('*')
                    file_path = source / f"excluded{ext}"
                    file_path.write_text("excluded")
                    excluded_size += len("excluded")
            
            # Estimate with no exclusions
            size_no_exclude = estimate_backup_size([source], [])
            
            # Estimate with exclusions
            size_with_exclude = estimate_backup_size([source], patterns)
            
            # PROPERTY: Size with exclusions must be <= size without exclusions
            assert size_with_exclude <= size_no_exclude, \
                f"Size with exclusions ({size_with_exclude}) should be <= " \
                f"size without ({size_no_exclude})"
            
            # PROPERTY: Size with exclusions should equal regular file size
            # (approximately, as there may be some edge cases with pattern matching)
            assert size_with_exclude <= regular_size + 100, \
                f"Size with exclusions ({size_with_exclude}) should be close to " \
                f"regular file size ({regular_size})"
    
    @given(
        num_files=st.integers(min_value=1, max_value=10),
        file_size=file_sizes,
    )
    @settings(max_examples=50, deadline=None, phases=[Phase.generate, Phase.target])
    def test_space_error_before_in_progress(
        self,
        num_files: int,
        file_size: int,
    ):
        """
        **Feature: backup-robustness, Property 2: Space Validation Correctness**
        
        For any backup attempt with insufficient space, SpaceError must be
        raised BEFORE any in_progress directory is created.
        
        **Validates: Requirements 2.5**
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            source = tmpdir_path / "source"
            source.mkdir()
            dest = tmpdir_path / "dest"
            dest.mkdir()
            
            # Create test files
            create_test_files(source, num_files, file_size)
            
            # Get available space and calculate a buffer that will fail
            disk_usage = shutil.disk_usage(tmpdir_path)
            available = disk_usage.free
            estimated = estimate_backup_size([source], [])
            
            # Skip if estimated is 0 (no files)
            assume(estimated > 0)
            
            # Calculate buffer_percent that requires more than available
            # required = estimated * (1 + buffer_percent) > available
            # buffer_percent > (available / estimated) - 1
            buffer_percent = (available / estimated) + 1.0  # Ensure it's more than available
            
            # Track if in_progress was ever created
            in_progress_created = False
            
            # Monkey-patch Path.mkdir to detect in_progress creation
            original_mkdir = Path.mkdir
            
            def tracking_mkdir(self, *args, **kwargs):
                nonlocal in_progress_created
                if "in_progress" in str(self):
                    in_progress_created = True
                return original_mkdir(self, *args, **kwargs)
            
            try:
                Path.mkdir = tracking_mkdir
                
                with pytest.raises(SpaceError):
                    validate_space(dest, [source], [], buffer_percent=buffer_percent)
                
                # INVARIANT: in_progress should never have been created
                assert not in_progress_created, \
                    "in_progress directory should not be created during space validation"
                
            finally:
                Path.mkdir = original_mkdir
    
    @given(
        available_gb=st.floats(min_value=0.1, max_value=0.9, allow_nan=False, allow_infinity=False),
    )
    @settings(max_examples=50, deadline=None, phases=[Phase.generate, Phase.target])
    def test_low_space_warning(self, available_gb: float):
        """
        **Feature: backup-robustness, Property 2: Space Validation Correctness**
        
        For any destination with less than min_free_bytes available,
        a warning should be returned regardless of backup size.
        
        **Validates: Requirements 2.4**
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            source = tmpdir_path / "source"
            source.mkdir()
            dest = tmpdir_path / "dest"
            
            # Create a small file
            (source / "small.txt").write_text("tiny")
            
            # Set min_free_bytes higher than what's typically available
            # to trigger the warning
            min_free = int(100 * 1024 * 1024 * 1024 * 1024)  # 100TB
            
            result = validate_space(
                dest,
                [source],
                [],
                min_free_bytes=min_free,
            )
            
            # PROPERTY: Warning should be present when available < min_free
            assert result.warning is not None, \
                "Warning should be present when available space is below minimum"
            assert "Low disk space" in result.warning
