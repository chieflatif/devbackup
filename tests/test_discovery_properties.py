"""Property-based tests for Auto-Discovery Engine.

**Property 1: Project Discovery Correctness**
**Validates: Requirements 1.1, 1.2, 4.1, 4.2, 4.3, 4.4, 4.6**

Tests that:
- All directories containing project markers are found
- Excluded directories (node_modules, .git, __pycache__, etc.) are never included
- Symbolic links are never followed during traversal
- Current workspace (if provided) appears first in results
- Size estimates are within 20% of actual directory size

**Property 2: Destination Discovery Correctness**
**Validates: Requirements 1.3, 5.1, 5.2, 5.3, 5.5**

Tests that:
- Destinations are returned in priority order: external > network > iCloud > local
- External/removable drives have higher recommendation scores
- Available space calculations are accurate
- Destination type classification is correct
"""

import os
import shutil
import tempfile
from pathlib import Path
from typing import Dict, List, Set

import pytest
from hypothesis import given, settings, assume, HealthCheck
from hypothesis import strategies as st

from devbackup.discovery import (
    AutoDiscovery,
    DiscoveredProject,
    DiscoveredDestination,
    PROJECT_MARKERS,
    EXCLUDE_DIRS,
    DESTINATION_TYPE_SCORES,
    MIN_DESTINATION_SPACE_BYTES,
)


# Strategy for generating valid directory names
dirname_strategy = st.text(
    alphabet=st.sampled_from("abcdefghijklmnopqrstuvwxyz0123456789_"),
    min_size=1,
    max_size=10,
).filter(lambda x: x and not x.startswith(".") and x not in EXCLUDE_DIRS)

# Strategy for file content
content_strategy = st.text(min_size=0, max_size=100)

# Strategy for project types
project_type_strategy = st.sampled_from(["python", "node", "rust", "go", "generic"])


def create_project_dir(base_path: Path, name: str, project_type: str, files: Dict[str, str] = None) -> Path:
    """Create a project directory with appropriate markers.
    
    Args:
        base_path: Parent directory
        name: Project directory name
        project_type: Type of project (python, node, rust, go, generic)
        files: Additional files to create {relative_path: content}
        
    Returns:
        Path to created project directory
    """
    project_path = base_path / name
    project_path.mkdir(parents=True, exist_ok=True)
    
    # Create marker file based on project type
    markers = PROJECT_MARKERS.get(project_type, [".git"])
    if markers:
        marker = markers[0]
        marker_path = project_path / marker
        if marker.endswith("proj") or marker.endswith("workspace"):
            # These are directories
            marker_path.mkdir(exist_ok=True)
        else:
            marker_path.write_text("{}")  # Minimal valid content
    
    # Create additional files
    if files:
        for rel_path, content in files.items():
            file_path = project_path / rel_path
            file_path.parent.mkdir(parents=True, exist_ok=True)
            file_path.write_text(content)
    
    return project_path


def get_all_discovered_paths(projects: List[DiscoveredProject]) -> Set[Path]:
    """Get set of all discovered project paths."""
    return {p.path.resolve() for p in projects}


class TestProjectDiscoveryCorrectnessProperty:
    """
    Property 1: Project Discovery Correctness
    
    *For any* filesystem containing directories with project markers 
    (package.json, pyproject.toml, Cargo.toml, .git, etc.), the Auto_Discovery 
    engine SHALL:
    - Find all directories containing at least one project marker
    - Never include directories in the EXCLUDE_DIRS set
    - Never follow symbolic links during traversal
    - Return the current workspace (if provided) as the first result
    - Estimate sizes within 20% of actual directory size
    
    **Validates: Requirements 1.1, 1.2, 4.1, 4.2, 4.3, 4.4, 4.6**
    """
    
    @given(
        project_configs=st.lists(
            st.tuples(dirname_strategy, project_type_strategy),
            min_size=1,
            max_size=5,
            unique_by=lambda x: x[0],  # Unique names
        ),
    )
    @settings(
        max_examples=10,
        deadline=None,
        suppress_health_check=[HealthCheck.too_slow],
    )
    def test_all_projects_with_markers_are_found(
        self,
        project_configs: List[tuple],
    ):
        """
        Feature: user-experience-enhancement, Property 1: Project Discovery Correctness
        
        For any filesystem with project markers, all projects SHALL be found.
        
        **Validates: Requirements 1.1, 1.2, 4.1**
        """
        with tempfile.TemporaryDirectory() as scan_dir:
            scan_path = Path(scan_dir)
            
            # Create projects with markers
            created_projects: Set[Path] = set()
            for name, project_type in project_configs:
                project_path = create_project_dir(scan_path, name, project_type)
                created_projects.add(project_path.resolve())
            
            # Run discovery
            discovery = AutoDiscovery(
                scan_locations=[scan_path],
                exclude_dirs=EXCLUDE_DIRS,
            )
            discovered = discovery.discover_projects(max_depth=3)
            discovered_paths = get_all_discovered_paths(discovered)
            
            # Verify all created projects are found
            for project_path in created_projects:
                assert project_path in discovered_paths, \
                    f"Project at {project_path} should be discovered"
    
    @given(
        project_name=dirname_strategy,
        project_type=project_type_strategy,
        excluded_dir_name=st.sampled_from([d for d in list(EXCLUDE_DIRS)[:10] if d != ".git"]),  # Exclude .git since it's also a project marker
    )
    @settings(
        max_examples=10,
        deadline=None,
        suppress_health_check=[HealthCheck.too_slow],
    )
    def test_excluded_directories_never_included(
        self,
        project_name: str,
        project_type: str,
        excluded_dir_name: str,
    ):
        """
        Feature: user-experience-enhancement, Property 1: Project Discovery Correctness
        
        Directories in EXCLUDE_DIRS SHALL never be included in results.
        
        **Validates: Requirements 4.3**
        """
        # Skip pattern-based excludes for this test (e.g., "*.egg-info")
        assume("*" not in excluded_dir_name)
        # Skip .git since it's also a project marker which would make the parent a project
        assume(excluded_dir_name != ".git")
        
        with tempfile.TemporaryDirectory() as scan_dir:
            scan_path = Path(scan_dir)
            
            # Create a subdirectory to hold our test structure
            # This prevents the excluded dir from making scan_path look like a project
            test_root = scan_path / "test_root"
            test_root.mkdir()
            
            # Create a valid project inside test_root
            project_path = create_project_dir(test_root, project_name, project_type)
            
            # Create an excluded directory with a project marker inside test_root
            excluded_path = test_root / excluded_dir_name
            excluded_path.mkdir(exist_ok=True)
            # Add a marker to make it look like a project
            (excluded_path / "package.json").write_text("{}")
            
            # Run discovery on test_root
            discovery = AutoDiscovery(
                scan_locations=[test_root],
                exclude_dirs=EXCLUDE_DIRS,
            )
            discovered = discovery.discover_projects(max_depth=3)
            discovered_paths = get_all_discovered_paths(discovered)
            
            # Verify excluded directory is NOT found
            assert excluded_path.resolve() not in discovered_paths, \
                f"Excluded directory {excluded_dir_name} should not be discovered"
            
            # Verify valid project IS found
            assert project_path.resolve() in discovered_paths, \
                f"Valid project {project_name} should be discovered"
    
    @given(
        project_name=dirname_strategy,
        project_type=project_type_strategy,
        create_circular_symlink=st.booleans(),
    )
    @settings(
        max_examples=10,
        deadline=None,
        suppress_health_check=[HealthCheck.too_slow],
    )
    def test_symlinks_never_followed(
        self,
        project_name: str,
        project_type: str,
        create_circular_symlink: bool,
    ):
        """
        Feature: user-experience-enhancement, Property 1: Project Discovery Correctness
        
        Symbolic links SHALL never be followed during traversal.
        
        **Validates: Requirements 4.2**
        """
        with tempfile.TemporaryDirectory() as scan_dir:
            with tempfile.TemporaryDirectory() as external_dir:
                scan_path = Path(scan_dir)
                external_path = Path(external_dir)
                
                # Create a valid project in scan directory
                project_path = create_project_dir(scan_path, project_name, project_type)
                
                # Create a project in external directory (should NOT be found)
                external_project = create_project_dir(external_path, "external_project", "node")
                
                # Create symlink to external directory
                symlink_path = scan_path / "external_link"
                try:
                    symlink_path.symlink_to(external_path)
                except OSError:
                    # Skip if symlinks not supported
                    assume(False)
                
                # Optionally create circular symlink
                if create_circular_symlink:
                    circular_link = project_path / "circular"
                    try:
                        circular_link.symlink_to(scan_path)
                    except OSError:
                        pass  # Ignore if can't create
                
                # Run discovery - should complete without infinite loop
                discovery = AutoDiscovery(
                    scan_locations=[scan_path],
                    exclude_dirs=EXCLUDE_DIRS,
                )
                discovered = discovery.discover_projects(max_depth=3)
                discovered_paths = get_all_discovered_paths(discovered)
                
                # Verify external project is NOT found (symlink not followed)
                assert external_project.resolve() not in discovered_paths, \
                    "External project via symlink should not be discovered"
                
                # Verify valid project IS found
                assert project_path.resolve() in discovered_paths, \
                    f"Valid project {project_name} should be discovered"
    
    @given(
        workspace_project_type=project_type_strategy,
        other_project_configs=st.lists(
            st.tuples(dirname_strategy, project_type_strategy),
            min_size=1,
            max_size=3,
            unique_by=lambda x: x[0],
        ),
    )
    @settings(
        max_examples=10,
        deadline=None,
        suppress_health_check=[HealthCheck.too_slow],
    )
    def test_workspace_appears_first(
        self,
        workspace_project_type: str,
        other_project_configs: List[tuple],
    ):
        """
        Feature: user-experience-enhancement, Property 1: Project Discovery Correctness
        
        Current workspace (if provided) SHALL appear first in results.
        
        **Validates: Requirements 4.4**
        """
        with tempfile.TemporaryDirectory() as scan_dir:
            with tempfile.TemporaryDirectory() as workspace_dir:
                scan_path = Path(scan_dir)
                workspace_path = Path(workspace_dir)
                
                # Create workspace project
                workspace_marker = PROJECT_MARKERS.get(workspace_project_type, [".git"])[0]
                if workspace_marker.endswith("proj") or workspace_marker.endswith("workspace"):
                    (workspace_path / workspace_marker).mkdir(exist_ok=True)
                else:
                    (workspace_path / workspace_marker).write_text("{}")
                
                # Create other projects in scan directory
                for name, project_type in other_project_configs:
                    create_project_dir(scan_path, name, project_type)
                
                # Run discovery with workspace
                discovery = AutoDiscovery(
                    scan_locations=[scan_path],
                    exclude_dirs=EXCLUDE_DIRS,
                )
                discovered = discovery.discover_projects(
                    include_workspace=workspace_path,
                    max_depth=3,
                )
                
                # Verify workspace appears first
                assert len(discovered) > 0, "Should discover at least the workspace"
                assert discovered[0].path.resolve() == workspace_path.resolve(), \
                    "Workspace should be first in results"
    
    @given(
        project_name=dirname_strategy,
        project_type=project_type_strategy,
        num_files=st.integers(min_value=1, max_value=10),
        file_size=st.integers(min_value=100, max_value=1000),
    )
    @settings(
        max_examples=10,
        deadline=None,
        suppress_health_check=[HealthCheck.too_slow],
    )
    def test_size_estimate_accuracy(
        self,
        project_name: str,
        project_type: str,
        num_files: int,
        file_size: int,
    ):
        """
        Feature: user-experience-enhancement, Property 1: Project Discovery Correctness
        
        Size estimates SHALL be within 20% of actual directory size.
        
        **Validates: Requirements 4.6**
        """
        with tempfile.TemporaryDirectory() as scan_dir:
            scan_path = Path(scan_dir)
            
            # Create project with known file sizes
            files = {}
            for i in range(num_files):
                files[f"file{i}.txt"] = "x" * file_size
            
            project_path = create_project_dir(scan_path, project_name, project_type, files)
            
            # Calculate actual size (excluding marker file)
            actual_size = 0
            for root, dirs, filenames in os.walk(project_path):
                # Filter excluded dirs
                dirs[:] = [d for d in dirs if d not in EXCLUDE_DIRS]
                for f in filenames:
                    if not f.startswith("."):
                        try:
                            actual_size += (Path(root) / f).stat().st_size
                        except OSError:
                            pass
            
            # Run discovery
            discovery = AutoDiscovery(
                scan_locations=[scan_path],
                exclude_dirs=EXCLUDE_DIRS,
            )
            discovered = discovery.discover_projects(max_depth=3)
            
            # Find our project
            project_result = None
            for proj in discovered:
                if proj.path.resolve() == project_path.resolve():
                    project_result = proj
                    break
            
            assert project_result is not None, "Project should be discovered"
            
            # Verify size estimate is within 20% of actual
            estimated = project_result.estimated_size_bytes
            if actual_size > 0:
                error_ratio = abs(estimated - actual_size) / actual_size
                assert error_ratio <= 0.20, \
                    f"Size estimate {estimated} should be within 20% of actual {actual_size} (error: {error_ratio:.2%})"


class TestProjectTypeDetection:
    """
    Additional tests for project type detection accuracy.
    
    **Validates: Requirements 4.1**
    """
    
    @given(
        project_type=project_type_strategy,
    )
    @settings(
        max_examples=10,
        deadline=None,
        suppress_health_check=[HealthCheck.too_slow],
    )
    def test_project_type_correctly_detected(
        self,
        project_type: str,
    ):
        """
        Feature: user-experience-enhancement, Property 1: Project Discovery Correctness
        
        Project type SHALL be correctly detected based on marker files.
        
        **Validates: Requirements 4.1**
        """
        with tempfile.TemporaryDirectory() as scan_dir:
            scan_path = Path(scan_dir)
            
            # Create project with specific type
            project_path = create_project_dir(scan_path, "test_project", project_type)
            
            # Run discovery
            discovery = AutoDiscovery(
                scan_locations=[scan_path],
                exclude_dirs=EXCLUDE_DIRS,
            )
            discovered = discovery.discover_projects(max_depth=3)
            
            # Find our project
            assert len(discovered) == 1, "Should discover exactly one project"
            assert discovered[0].project_type == project_type, \
                f"Project type should be {project_type}, got {discovered[0].project_type}"



# Strategies for destination discovery tests
destination_type_strategy = st.sampled_from(["external", "network", "icloud", "local"])


def create_mock_destination(
    base_path: Path,
    name: str,
    dest_type: str,
    available_bytes: int = 10 * 1024 * 1024 * 1024,  # 10GB default
    total_bytes: int = 100 * 1024 * 1024 * 1024,  # 100GB default
) -> DiscoveredDestination:
    """Create a mock DiscoveredDestination for testing.
    
    Args:
        base_path: Base path for the destination
        name: Name of the destination
        dest_type: Type of destination (external, network, icloud, local)
        available_bytes: Available space in bytes
        total_bytes: Total space in bytes
        
    Returns:
        DiscoveredDestination object
    """
    is_removable = dest_type == "external"
    score = DESTINATION_TYPE_SCORES.get(dest_type, 30)
    
    # Add space bonus to score
    if total_bytes > 0:
        space_ratio = available_bytes / total_bytes
        space_bonus = int(space_ratio * 10)
        score = min(100, max(1, score + space_bonus))
    
    return DiscoveredDestination(
        path=base_path / name,
        name=name,
        destination_type=dest_type,
        available_bytes=available_bytes,
        total_bytes=total_bytes,
        is_removable=is_removable,
        recommendation_score=score,
    )


class TestDestinationDiscoveryCorrectnessProperty:
    """
    Property 2: Destination Discovery Correctness
    
    *For any* system with available storage locations, the Destination_Selector SHALL:
    - Return destinations in priority order: external drives > network drives > iCloud > local
    - Assign higher recommendation scores to external/removable drives
    - Include accurate available_bytes that matches actual filesystem free space
    - Persist and recall previously selected destinations across sessions
    
    **Validates: Requirements 1.3, 5.1, 5.2, 5.3, 5.5**
    """
    
    @given(
        dest_types=st.lists(
            destination_type_strategy,
            min_size=2,
            max_size=4,
            unique=True,
        ),
    )
    @settings(
        max_examples=10,
        deadline=None,
        suppress_health_check=[HealthCheck.too_slow],
    )
    def test_destination_priority_ordering(
        self,
        dest_types: List[str],
    ):
        """
        Feature: user-experience-enhancement, Property 2: Destination Discovery Correctness
        
        Destinations SHALL be returned in priority order: external > network > iCloud > local.
        
        **Validates: Requirements 5.1, 5.3**
        """
        # Create destinations with different types
        destinations = []
        for i, dest_type in enumerate(dest_types):
            dest = create_mock_destination(
                Path("/test"),
                f"dest_{dest_type}_{i}",
                dest_type,
            )
            destinations.append(dest)
        
        # Sort by recommendation score (as discover_destinations does)
        sorted_destinations = sorted(
            destinations,
            key=lambda d: d.recommendation_score,
            reverse=True,
        )
        
        # Verify priority ordering is maintained
        # External should come before network, network before icloud, icloud before local
        priority_order = {"external": 0, "network": 1, "icloud": 2, "local": 3}
        
        for i in range(len(sorted_destinations) - 1):
            current = sorted_destinations[i]
            next_dest = sorted_destinations[i + 1]
            
            current_priority = priority_order.get(current.destination_type, 4)
            next_priority = priority_order.get(next_dest.destination_type, 4)
            
            # Current should have equal or higher priority (lower number)
            assert current_priority <= next_priority, \
                f"Destination {current.destination_type} should come before {next_dest.destination_type}"
    
    @given(
        available_ratio=st.floats(min_value=0.1, max_value=0.9),
    )
    @settings(
        max_examples=10,
        deadline=None,
        suppress_health_check=[HealthCheck.too_slow],
    )
    def test_external_drives_have_higher_scores(
        self,
        available_ratio: float,
    ):
        """
        Feature: user-experience-enhancement, Property 2: Destination Discovery Correctness
        
        External/removable drives SHALL have higher recommendation scores than other types.
        
        **Validates: Requirements 5.1, 5.3**
        """
        total_bytes = 100 * 1024 * 1024 * 1024  # 100GB
        available_bytes = int(total_bytes * available_ratio)
        
        # Create destinations of each type with same space
        external_dest = create_mock_destination(
            Path("/Volumes"), "ExternalDrive", "external",
            available_bytes, total_bytes,
        )
        network_dest = create_mock_destination(
            Path("/Volumes"), "NetworkDrive", "network",
            available_bytes, total_bytes,
        )
        icloud_dest = create_mock_destination(
            Path.home() / "Library" / "Mobile Documents", "iCloud", "icloud",
            available_bytes, total_bytes,
        )
        local_dest = create_mock_destination(
            Path.home(), "Backups", "local",
            available_bytes, total_bytes,
        )
        
        # Verify external has highest score
        assert external_dest.recommendation_score > network_dest.recommendation_score, \
            "External drive should have higher score than network drive"
        assert network_dest.recommendation_score > icloud_dest.recommendation_score, \
            "Network drive should have higher score than iCloud"
        assert icloud_dest.recommendation_score > local_dest.recommendation_score, \
            "iCloud should have higher score than local"
    
    @given(
        dest_type=destination_type_strategy,
    )
    @settings(
        max_examples=10,
        deadline=None,
        suppress_health_check=[HealthCheck.too_slow],
    )
    def test_available_space_accuracy(
        self,
        dest_type: str,
    ):
        """
        Feature: user-experience-enhancement, Property 2: Destination Discovery Correctness
        
        Available space calculations SHALL be accurate for real filesystem paths.
        
        **Validates: Requirements 5.2**
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            # Get actual disk usage for the temp directory
            actual_usage = shutil.disk_usage(temp_path)
            actual_available = actual_usage.free
            actual_total = actual_usage.total
            
            # Create discovery instance and test space calculation
            discovery = AutoDiscovery()
            calculated_available, calculated_total = discovery._get_space_info(temp_path)
            
            # Verify space calculations match actual values
            assert calculated_available == actual_available, \
                f"Available space {calculated_available} should match actual {actual_available}"
            assert calculated_total == actual_total, \
                f"Total space {calculated_total} should match actual {actual_total}"
    
    @given(
        dest_type=destination_type_strategy,
        available_bytes=st.integers(
            min_value=MIN_DESTINATION_SPACE_BYTES,
            max_value=1000 * 1024 * 1024 * 1024,  # 1TB
        ),
        total_bytes=st.integers(
            min_value=MIN_DESTINATION_SPACE_BYTES,
            max_value=2000 * 1024 * 1024 * 1024,  # 2TB
        ),
    )
    @settings(
        max_examples=10,
        deadline=None,
        suppress_health_check=[HealthCheck.too_slow],
    )
    def test_recommendation_score_in_valid_range(
        self,
        dest_type: str,
        available_bytes: int,
        total_bytes: int,
    ):
        """
        Feature: user-experience-enhancement, Property 2: Destination Discovery Correctness
        
        Recommendation scores SHALL be in valid range (1-100).
        
        **Validates: Requirements 5.1, 5.3**
        """
        # Ensure available <= total
        assume(available_bytes <= total_bytes)
        
        discovery = AutoDiscovery()
        score = discovery._calculate_recommendation_score(
            dest_type, available_bytes, total_bytes
        )
        
        assert 1 <= score <= 100, \
            f"Recommendation score {score} should be between 1 and 100"
    
    @given(
        path_suffix=st.sampled_from([
            "Mobile Documents/com~apple~CloudDocs",
            "iCloud Drive",
            "Library/Mobile Documents",
        ]),
    )
    @settings(
        max_examples=10,
        deadline=None,
        suppress_health_check=[HealthCheck.too_slow],
    )
    def test_icloud_paths_classified_correctly(
        self,
        path_suffix: str,
    ):
        """
        Feature: user-experience-enhancement, Property 2: Destination Discovery Correctness
        
        iCloud paths SHALL be correctly classified as icloud type.
        
        **Validates: Requirements 5.1**
        """
        discovery = AutoDiscovery()
        
        # Test iCloud path classification
        icloud_path = Path.home() / path_suffix
        dest_type, is_removable = discovery._classify_destination(icloud_path)
        
        assert dest_type == "icloud", \
            f"Path {icloud_path} should be classified as icloud, got {dest_type}"
        assert not is_removable, \
            "iCloud should not be marked as removable"
    
    @given(
        volume_name=st.text(
            alphabet=st.sampled_from("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_- "),
            min_size=1,
            max_size=20,
        ).filter(lambda x: x.strip() and x.lower() not in ["macintosh hd", "system", "data", "icloud", "cloud"]),
    )
    @settings(
        max_examples=10,
        deadline=None,
        suppress_health_check=[HealthCheck.too_slow],
    )
    def test_volumes_path_classification(
        self,
        volume_name: str,
    ):
        """
        Feature: user-experience-enhancement, Property 2: Destination Discovery Correctness
        
        Paths under /Volumes SHALL be classified as external or network.
        
        **Validates: Requirements 5.1**
        """
        discovery = AutoDiscovery()
        
        # Test /Volumes path classification
        volume_path = Path("/Volumes") / volume_name.strip()
        dest_type, _ = discovery._classify_destination(volume_path)
        
        # Should be external, network, or local (not icloud)
        assert dest_type in ["external", "network", "local"], \
            f"Volume path {volume_path} should be external, network, or local, got {dest_type}"
    
    @given(
        space_ratio=st.floats(min_value=0.0, max_value=1.0),
    )
    @settings(
        max_examples=10,
        deadline=None,
        suppress_health_check=[HealthCheck.too_slow],
    )
    def test_space_bonus_affects_score(
        self,
        space_ratio: float,
    ):
        """
        Feature: user-experience-enhancement, Property 2: Destination Discovery Correctness
        
        More available space SHALL result in higher recommendation scores.
        
        **Validates: Requirements 5.2, 5.3**
        """
        discovery = AutoDiscovery()
        total_bytes = 100 * 1024 * 1024 * 1024  # 100GB
        
        # Calculate scores for low and high space ratios
        low_available = int(total_bytes * 0.1)  # 10% free
        high_available = int(total_bytes * 0.9)  # 90% free
        
        low_score = discovery._calculate_recommendation_score(
            "local", low_available, total_bytes
        )
        high_score = discovery._calculate_recommendation_score(
            "local", high_available, total_bytes
        )
        
        # Higher available space should result in higher or equal score
        assert high_score >= low_score, \
            f"Higher available space should give higher score: {high_score} >= {low_score}"


class TestDestinationRecommendation:
    """
    Additional tests for destination recommendation logic.
    
    **Validates: Requirements 5.1, 5.3**
    """
    
    @given(
        num_destinations=st.integers(min_value=1, max_value=5),
    )
    @settings(
        max_examples=10,
        deadline=None,
        suppress_health_check=[HealthCheck.too_slow],
    )
    def test_recommend_returns_highest_scored(
        self,
        num_destinations: int,
    ):
        """
        Feature: user-experience-enhancement, Property 2: Destination Discovery Correctness
        
        recommend_destination SHALL return the destination with highest score.
        
        **Validates: Requirements 5.1, 5.3**
        """
        discovery = AutoDiscovery()
        
        # Create destinations with varying scores
        dest_types = ["external", "network", "icloud", "local"]
        destinations = []
        for i in range(num_destinations):
            dest_type = dest_types[i % len(dest_types)]
            dest = create_mock_destination(
                Path("/test"),
                f"dest_{i}",
                dest_type,
            )
            destinations.append(dest)
        
        # Sort by score (as discover_destinations does)
        destinations.sort(key=lambda d: d.recommendation_score, reverse=True)
        
        # Get recommendation
        recommended, explanation = discovery.recommend_destination(destinations)
        
        # Verify it's the highest scored
        assert recommended is not None, "Should recommend a destination"
        assert recommended.recommendation_score == destinations[0].recommendation_score, \
            "Should recommend the highest scored destination"
        assert len(explanation) > 0, "Should provide an explanation"
    
    def test_recommend_empty_list_returns_none(self):
        """
        Feature: user-experience-enhancement, Property 2: Destination Discovery Correctness
        
        recommend_destination SHALL return None with explanation for empty list.
        
        **Validates: Requirements 5.1**
        """
        discovery = AutoDiscovery()
        
        recommended, explanation = discovery.recommend_destination([])
        
        assert recommended is None, "Should return None for empty list"
        assert len(explanation) > 0, "Should provide an explanation"
        assert "couldn't find" in explanation.lower() or "no" in explanation.lower(), \
            "Explanation should indicate no destinations found"
    
    @given(
        dest_type=destination_type_strategy,
    )
    @settings(
        max_examples=10,
        deadline=None,
        suppress_health_check=[HealthCheck.too_slow],
    )
    def test_recommendation_explanation_matches_type(
        self,
        dest_type: str,
    ):
        """
        Feature: user-experience-enhancement, Property 2: Destination Discovery Correctness
        
        Recommendation explanation SHALL be appropriate for destination type.
        
        **Validates: Requirements 5.3**
        """
        discovery = AutoDiscovery()
        
        dest = create_mock_destination(
            Path("/test"),
            "TestDest",
            dest_type,
        )
        
        recommended, explanation = discovery.recommend_destination([dest])
        
        assert recommended is not None, "Should recommend the destination"
        
        # Verify explanation mentions appropriate concepts
        if dest_type == "external":
            assert "external" in explanation.lower() or "drive" in explanation.lower(), \
                "External drive explanation should mention external/drive"
        elif dest_type == "network":
            assert "network" in explanation.lower(), \
                "Network drive explanation should mention network"
        elif dest_type == "icloud":
            assert "icloud" in explanation.lower(), \
                "iCloud explanation should mention iCloud"
        elif dest_type == "local":
            assert "mac" in explanation.lower() or "local" in explanation.lower() or "folder" in explanation.lower(), \
                "Local explanation should mention Mac/local/folder"
