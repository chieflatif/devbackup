"""Auto-discovery engine for devbackup.

This module provides automatic detection of project directories and backup
destinations without requiring manual configuration.

Requirements: 1.1, 1.2, 1.3, 4.1, 4.3, 5.1, 5.2, 5.3
"""

import os
import shutil
import subprocess
from dataclasses import dataclass, field
from fnmatch import fnmatch
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple


class DiscoveryError(Exception):
    """Raised when auto-discovery fails."""
    pass


@dataclass
class DiscoveredProject:
    """A discovered project directory.
    
    Represents a project found during auto-discovery, including metadata
    about the project type and estimated size.
    
    Requirements: 1.1, 1.2, 4.1
    """
    path: Path
    name: str
    project_type: str  # "python", "node", "rust", "go", "xcode", "generic"
    estimated_size_bytes: int
    marker_files: List[str] = field(default_factory=list)  # Files that identified this as a project
    
    def __post_init__(self) -> None:
        """Ensure path is a Path object."""
        if isinstance(self.path, str):
            self.path = Path(self.path)


@dataclass
class DiscoveredDestination:
    """A discovered backup destination.
    
    Represents a potential backup destination found during auto-discovery,
    including storage capacity and recommendation scoring.
    
    Requirements: 1.3, 5.1, 5.2, 5.3
    """
    path: Path
    name: str
    destination_type: str  # "external", "network", "icloud", "local"
    available_bytes: int
    total_bytes: int
    is_removable: bool
    recommendation_score: int  # 1-100, higher is better
    
    def __post_init__(self) -> None:
        """Ensure path is a Path object."""
        if isinstance(self.path, str):
            self.path = Path(self.path)


# Project markers in priority order - used to identify project types
# Requirements: 4.1
PROJECT_MARKERS: Dict[str, List[str]] = {
    "python": ["pyproject.toml", "setup.py", "requirements.txt", "Pipfile"],
    "node": ["package.json"],
    "rust": ["Cargo.toml"],
    "go": ["go.mod"],
    "xcode": [".xcodeproj", ".xcworkspace"],
    "generic": [".git"],
}

# Directories to scan for projects
# Requirements: 1.2
SCAN_LOCATIONS: List[Path] = [
    Path.home() / "Documents",
    Path.home() / "Desktop",
    Path.home() / "Projects",
    Path.home() / "Code",
    Path.home() / "Developer",
]

# Directories to exclude from scanning
# Requirements: 4.3
EXCLUDE_DIRS: Set[str] = {
    "node_modules",
    ".git",
    "__pycache__",
    "build",
    "dist",
    ".next",
    "target",
    ".venv",
    "venv",
    ".cache",
    ".pytest_cache",
    ".mypy_cache",
    ".tox",
    ".eggs",
    "*.egg-info",
    ".gradle",
    ".idea",
    ".vscode",
    "Pods",
    "DerivedData",
    ".build",
    "vendor",
    "coverage",
    ".nyc_output",
    "tmp",
    "temp",
    "logs",
}

# Destination type priority scores (higher = better recommendation)
# Requirements: 5.1, 5.3
DESTINATION_TYPE_SCORES: Dict[str, int] = {
    "external": 90,  # External drives are safest
    "network": 70,   # Network drives are good but may be slower
    "icloud": 50,    # iCloud is convenient but has sync considerations
    "local": 30,     # Local folders don't protect against drive failure
}

# Common local backup folder names to check
# Requirements: 1.3
LOCAL_BACKUP_FOLDERS: List[str] = [
    "Backups",
    "Backup",
    "DevBackups",
    "devbackup",
]

# Minimum space required for a destination to be considered viable (1GB)
MIN_DESTINATION_SPACE_BYTES: int = 1024 * 1024 * 1024



class AutoDiscovery:
    """Auto-discovery engine for projects and destinations.
    
    Automatically detects project directories and backup destinations
    without requiring manual configuration.
    
    Requirements: 1.1, 1.2, 4.1, 4.2, 4.3, 4.4, 4.6
    """
    
    def __init__(
        self,
        scan_locations: Optional[List[Path]] = None,
        exclude_dirs: Optional[Set[str]] = None,
        project_markers: Optional[Dict[str, List[str]]] = None,
    ) -> None:
        """Initialize the auto-discovery engine.
        
        Args:
            scan_locations: Directories to scan for projects. Defaults to SCAN_LOCATIONS.
            exclude_dirs: Directory names to exclude. Defaults to EXCLUDE_DIRS.
            project_markers: Project type markers. Defaults to PROJECT_MARKERS.
        """
        self.scan_locations = scan_locations if scan_locations is not None else SCAN_LOCATIONS
        self.exclude_dirs = exclude_dirs if exclude_dirs is not None else EXCLUDE_DIRS
        self.project_markers = project_markers if project_markers is not None else PROJECT_MARKERS
    
    def _should_exclude_dir(self, dir_name: str) -> bool:
        """Check if a directory should be excluded from scanning.
        
        Args:
            dir_name: Name of the directory (not full path)
            
        Returns:
            True if the directory should be excluded
        """
        # Direct match
        if dir_name in self.exclude_dirs:
            return True
        
        # Pattern match (e.g., "*.egg-info")
        for pattern in self.exclude_dirs:
            if "*" in pattern and fnmatch(dir_name, pattern):
                return True
        
        return False
    
    def _detect_project_type(self, path: Path) -> Tuple[Optional[str], List[str]]:
        """Detect the project type based on marker files.
        
        Args:
            path: Directory path to check
            
        Returns:
            Tuple of (project_type, list of found marker files)
            Returns (None, []) if no project markers found
        """
        found_markers: List[str] = []
        detected_type: Optional[str] = None
        
        # Check markers in priority order (specific types before generic)
        priority_order = ["python", "node", "rust", "go", "xcode", "generic"]
        
        for project_type in priority_order:
            markers = self.project_markers.get(project_type, [])
            for marker in markers:
                marker_path = path / marker
                # Check both file and directory markers (e.g., .xcodeproj is a directory)
                if marker_path.exists():
                    found_markers.append(marker)
                    if detected_type is None:
                        detected_type = project_type
        
        return detected_type, found_markers
    
    def _calculate_size(
        self,
        path: Path,
        exclude_patterns: Optional[Set[str]] = None,
    ) -> int:
        """Calculate the estimated size of a directory, respecting exclude patterns.
        
        Args:
            path: Directory path to calculate size for
            exclude_patterns: Patterns to exclude from size calculation
            
        Returns:
            Estimated size in bytes
        """
        if exclude_patterns is None:
            exclude_patterns = self.exclude_dirs
        
        total_size = 0
        
        try:
            for root, dirs, files in os.walk(path, followlinks=False):
                # Filter out excluded directories in-place to prevent descending
                dirs[:] = [d for d in dirs if not self._should_exclude_dir(d)]
                
                for file in files:
                    # Skip hidden files and common excludes
                    if file.startswith("."):
                        continue
                    
                    file_path = Path(root) / file
                    try:
                        # Use lstat to not follow symlinks
                        total_size += file_path.lstat().st_size
                    except (OSError, PermissionError):
                        # Skip files we can't access
                        continue
        except (OSError, PermissionError):
            # If we can't walk the directory at all, return 0
            pass
        
        return total_size
    
    def _scan_directory(
        self,
        base_path: Path,
        max_depth: int,
        current_depth: int = 0,
    ) -> List[DiscoveredProject]:
        """Recursively scan a directory for projects.
        
        Args:
            base_path: Directory to scan
            max_depth: Maximum depth to scan
            current_depth: Current recursion depth
            
        Returns:
            List of discovered projects
        """
        projects: List[DiscoveredProject] = []
        
        if current_depth > max_depth:
            return projects
        
        if not base_path.exists() or not base_path.is_dir():
            return projects
        
        # Check if this directory is a project
        project_type, markers = self._detect_project_type(base_path)
        
        if project_type is not None:
            # This is a project - don't scan subdirectories
            estimated_size = self._calculate_size(base_path)
            project = DiscoveredProject(
                path=base_path,
                name=base_path.name,
                project_type=project_type,
                estimated_size_bytes=estimated_size,
                marker_files=markers,
            )
            projects.append(project)
            return projects
        
        # Not a project - scan subdirectories
        try:
            for entry in os.scandir(base_path):
                if not entry.is_dir(follow_symlinks=False):
                    continue
                
                # Skip excluded directories
                if self._should_exclude_dir(entry.name):
                    continue
                
                # Skip hidden directories
                if entry.name.startswith("."):
                    continue
                
                # Recursively scan subdirectory
                sub_projects = self._scan_directory(
                    Path(entry.path),
                    max_depth,
                    current_depth + 1,
                )
                projects.extend(sub_projects)
        except (OSError, PermissionError):
            # Skip directories we can't access
            pass
        
        return projects
    
    def discover_projects(
        self,
        include_workspace: Optional[Path] = None,
        max_depth: int = 3,
    ) -> List[DiscoveredProject]:
        """Discover project directories by scanning common locations.
        
        Scans predefined locations for directories containing project markers
        (package.json, pyproject.toml, Cargo.toml, .git, etc.).
        
        Args:
            include_workspace: Current workspace path to prioritize. If provided,
                              this will be scanned first and appear first in results.
            max_depth: Maximum directory depth to scan (default: 3)
            
        Returns:
            List of discovered projects, with workspace projects first if provided.
            
        Requirements: 1.1, 1.2, 4.1, 4.2, 4.3, 4.4, 4.6
        """
        all_projects: List[DiscoveredProject] = []
        seen_paths: Set[Path] = set()
        
        # First, scan the workspace if provided (prioritize it)
        workspace_projects: List[DiscoveredProject] = []
        if include_workspace is not None:
            workspace_path = Path(include_workspace).resolve()
            if workspace_path.exists() and workspace_path.is_dir():
                # Check if workspace itself is a project
                project_type, markers = self._detect_project_type(workspace_path)
                if project_type is not None:
                    estimated_size = self._calculate_size(workspace_path)
                    project = DiscoveredProject(
                        path=workspace_path,
                        name=workspace_path.name,
                        project_type=project_type,
                        estimated_size_bytes=estimated_size,
                        marker_files=markers,
                    )
                    workspace_projects.append(project)
                    seen_paths.add(workspace_path)
                else:
                    # Scan workspace for nested projects
                    ws_projects = self._scan_directory(workspace_path, max_depth)
                    for proj in ws_projects:
                        resolved = proj.path.resolve()
                        if resolved not in seen_paths:
                            workspace_projects.append(proj)
                            seen_paths.add(resolved)
        
        # Then scan standard locations
        other_projects: List[DiscoveredProject] = []
        for location in self.scan_locations:
            # Expand ~ in paths
            expanded_location = Path(location).expanduser()
            
            if not expanded_location.exists() or not expanded_location.is_dir():
                continue
            
            # Skip if this is the workspace (already scanned)
            if include_workspace is not None:
                workspace_resolved = Path(include_workspace).resolve()
                location_resolved = expanded_location.resolve()
                if location_resolved == workspace_resolved:
                    continue
                # Also skip if workspace is inside this location (will be found anyway)
                # but we want workspace projects to appear first
            
            projects = self._scan_directory(expanded_location, max_depth)
            for proj in projects:
                resolved = proj.path.resolve()
                if resolved not in seen_paths:
                    other_projects.append(proj)
                    seen_paths.add(resolved)
        
        # Combine with workspace projects first
        all_projects = workspace_projects + other_projects
        
        return all_projects
    
    def _get_volume_info_macos(self, mount_point: Path) -> Tuple[bool, bool]:
        """Get volume information on macOS using diskutil.
        
        Args:
            mount_point: Path to the mount point
            
        Returns:
            Tuple of (is_removable, is_network)
        """
        is_removable = False
        is_network = False
        
        try:
            # Use diskutil to get volume info
            result = subprocess.run(
                ["diskutil", "info", str(mount_point)],
                capture_output=True,
                text=True,
                timeout=5,
            )
            
            if result.returncode == 0:
                output = result.stdout.lower()
                # Check for removable/external indicators
                is_removable = (
                    "removable media: removable" in output or
                    "external" in output or
                    "usb" in output
                )
                # Check for network volume
                is_network = (
                    "protocol: smb" in output or
                    "protocol: afp" in output or
                    "protocol: nfs" in output or
                    "network" in output
                )
        except (subprocess.TimeoutExpired, subprocess.SubprocessError, OSError):
            # If diskutil fails, try to infer from path
            mount_str = str(mount_point).lower()
            is_removable = "/volumes/" in mount_str and mount_point.name.lower() not in ["macintosh hd", "system"]
            is_network = mount_str.startswith("/net/") or mount_str.startswith("//")
        
        return is_removable, is_network
    
    def _classify_destination(self, path: Path) -> Tuple[str, bool]:
        """Classify a destination path by type.
        
        Args:
            path: Path to classify
            
        Returns:
            Tuple of (destination_type, is_removable)
            destination_type is one of: "external", "network", "icloud", "local"
        """
        path_str = str(path).lower()
        
        # Check for iCloud
        if "mobile documents" in path_str or "icloud" in path_str:
            return "icloud", False
        
        # Check for network paths
        if path_str.startswith("/net/") or path_str.startswith("//"):
            return "network", False
        
        # Check for external volumes on macOS
        if path_str.startswith("/volumes/"):
            is_removable, is_network = self._get_volume_info_macos(path)
            if is_network:
                return "network", False
            if is_removable:
                return "external", True
            # Non-removable volume that's not network - could be external SSD
            # Check if it's not the main system volume
            if path.name.lower() not in ["macintosh hd", "system", "data"]:
                return "external", True
        
        # Default to local
        return "local", False
    
    def _get_space_info(self, path: Path) -> Tuple[int, int]:
        """Get available and total space for a path.
        
        Args:
            path: Path to check
            
        Returns:
            Tuple of (available_bytes, total_bytes)
        """
        try:
            # Find the actual mount point or existing parent
            check_path = path
            while not check_path.exists() and check_path.parent != check_path:
                check_path = check_path.parent
            
            if not check_path.exists():
                return 0, 0
            
            usage = shutil.disk_usage(check_path)
            return usage.free, usage.total
        except OSError:
            return 0, 0
    
    def _calculate_recommendation_score(
        self,
        destination_type: str,
        available_bytes: int,
        total_bytes: int,
    ) -> int:
        """Calculate a recommendation score for a destination.
        
        Higher scores indicate better destinations. Score is based on:
        - Base score from destination type (external > network > icloud > local)
        - Bonus for more available space
        
        Args:
            destination_type: Type of destination
            available_bytes: Available space in bytes
            total_bytes: Total space in bytes
            
        Returns:
            Recommendation score from 1-100
        """
        # Base score from destination type
        base_score = DESTINATION_TYPE_SCORES.get(destination_type, 30)
        
        # Add bonus for available space (up to 10 points)
        # More space = higher bonus
        if total_bytes > 0:
            space_ratio = available_bytes / total_bytes
            space_bonus = int(space_ratio * 10)
        else:
            space_bonus = 0
        
        # Ensure score is in valid range
        return min(100, max(1, base_score + space_bonus))
    
    def _scan_volumes(self) -> List[DiscoveredDestination]:
        """Scan /Volumes for external and network drives on macOS.
        
        Returns:
            List of discovered destinations from /Volumes
        """
        destinations: List[DiscoveredDestination] = []
        volumes_path = Path("/Volumes")
        
        if not volumes_path.exists():
            return destinations
        
        # System volumes and special volumes to skip
        skip_volumes = {
            "macintosh hd",
            "system",
            "data",
            "com.apple.timemachine.localsnapshots",
            ".timemachine",
            "preboot",
            "recovery",
            "vm",
        }
        
        try:
            for entry in os.scandir(volumes_path):
                if not entry.is_dir(follow_symlinks=False):
                    continue
                
                volume_path = Path(entry.path)
                
                # Skip system and special volumes
                if entry.name.lower() in skip_volumes:
                    continue
                
                # Skip hidden volumes (starting with .)
                if entry.name.startswith("."):
                    continue
                
                # Get space info
                available_bytes, total_bytes = self._get_space_info(volume_path)
                
                # Skip volumes with insufficient space
                if available_bytes < MIN_DESTINATION_SPACE_BYTES:
                    continue
                
                # Classify the destination
                dest_type, is_removable = self._classify_destination(volume_path)
                
                # Calculate recommendation score
                score = self._calculate_recommendation_score(
                    dest_type, available_bytes, total_bytes
                )
                
                destinations.append(DiscoveredDestination(
                    path=volume_path,
                    name=entry.name,
                    destination_type=dest_type,
                    available_bytes=available_bytes,
                    total_bytes=total_bytes,
                    is_removable=is_removable,
                    recommendation_score=score,
                ))
        except (OSError, PermissionError):
            pass
        
        return destinations
    
    def _scan_icloud(self) -> List[DiscoveredDestination]:
        """Scan for iCloud Drive as a backup destination.
        
        Returns:
            List containing iCloud destination if available
        """
        destinations: List[DiscoveredDestination] = []
        
        # iCloud Drive location on macOS
        icloud_path = Path.home() / "Library" / "Mobile Documents" / "com~apple~CloudDocs"
        
        if not icloud_path.exists():
            return destinations
        
        # Get space info
        available_bytes, total_bytes = self._get_space_info(icloud_path)
        
        # Skip if insufficient space
        if available_bytes < MIN_DESTINATION_SPACE_BYTES:
            return destinations
        
        # Calculate recommendation score
        score = self._calculate_recommendation_score(
            "icloud", available_bytes, total_bytes
        )
        
        destinations.append(DiscoveredDestination(
            path=icloud_path,
            name="iCloud Drive",
            destination_type="icloud",
            available_bytes=available_bytes,
            total_bytes=total_bytes,
            is_removable=False,
            recommendation_score=score,
        ))
        
        return destinations
    
    def _scan_local_folders(self) -> List[DiscoveredDestination]:
        """Scan for local backup folders.
        
        Returns:
            List of discovered local backup destinations
        """
        destinations: List[DiscoveredDestination] = []
        seen_folder_names: Set[str] = set()
        
        # Check common locations for backup folders
        # Order matters - prefer home directory over Documents
        search_locations = [
            Path.home(),
            Path.home() / "Documents",
        ]
        
        for base_path in search_locations:
            if not base_path.exists():
                continue
            
            for folder_name in LOCAL_BACKUP_FOLDERS:
                # Only suggest each folder name once (prefer first location found)
                if folder_name.lower() in seen_folder_names:
                    continue
                
                folder_path = base_path / folder_name
                
                # Get space info (works even if folder doesn't exist yet)
                available_bytes, total_bytes = self._get_space_info(folder_path)
                
                # Skip if insufficient space
                if available_bytes < MIN_DESTINATION_SPACE_BYTES:
                    continue
                
                # Calculate recommendation score
                score = self._calculate_recommendation_score(
                    "local", available_bytes, total_bytes
                )
                
                destinations.append(DiscoveredDestination(
                    path=folder_path,
                    name=folder_name,
                    destination_type="local",
                    available_bytes=available_bytes,
                    total_bytes=total_bytes,
                    is_removable=False,
                    recommendation_score=score,
                ))
                
                seen_folder_names.add(folder_name.lower())
        
        return destinations
    
    def discover_destinations(self) -> List[DiscoveredDestination]:
        """Discover available backup destinations.
        
        Scans for external drives, network drives, iCloud Drive, and local
        folders that could be used as backup destinations. Results are sorted
        by recommendation score (highest first).
        
        Returns:
            List of discovered destinations, sorted by recommendation score.
            
        Requirements: 1.3, 5.1, 5.2, 5.3
        """
        all_destinations: List[DiscoveredDestination] = []
        seen_paths: Set[Path] = set()
        
        # Scan in priority order: external/network volumes, iCloud, local
        
        # 1. Scan /Volumes for external and network drives
        volume_destinations = self._scan_volumes()
        for dest in volume_destinations:
            resolved = dest.path.resolve()
            if resolved not in seen_paths:
                all_destinations.append(dest)
                seen_paths.add(resolved)
        
        # 2. Scan for iCloud Drive
        icloud_destinations = self._scan_icloud()
        for dest in icloud_destinations:
            resolved = dest.path.resolve()
            if resolved not in seen_paths:
                all_destinations.append(dest)
                seen_paths.add(resolved)
        
        # 3. Scan for local backup folders
        local_destinations = self._scan_local_folders()
        for dest in local_destinations:
            resolved = dest.path.resolve() if dest.path.exists() else dest.path
            if resolved not in seen_paths:
                all_destinations.append(dest)
                seen_paths.add(resolved)
        
        # Sort by recommendation score (highest first)
        all_destinations.sort(key=lambda d: d.recommendation_score, reverse=True)
        
        return all_destinations
    
    def recommend_destination(
        self,
        destinations: List[DiscoveredDestination],
    ) -> Tuple[Optional[DiscoveredDestination], str]:
        """Recommend the best destination from a list.
        
        Selects the destination with the highest recommendation score and
        provides a plain language explanation of why it was chosen.
        
        Args:
            destinations: List of discovered destinations
            
        Returns:
            Tuple of (recommended destination or None, explanation string)
            
        Requirements: 5.1, 5.3
        """
        if not destinations:
            return None, (
                "I couldn't find any suitable backup destinations. "
                "You could plug in an external drive, or I can create a backup folder on your Mac."
            )
        
        # Get the highest-scored destination
        best = destinations[0]  # Already sorted by score
        
        # Generate explanation based on destination type
        if best.destination_type == "external":
            explanation = (
                f"I recommend using your external drive '{best.name}' for backups. "
                f"It has {best.available_bytes / (1024**3):.0f} GB free, and external drives "
                "are the safest option because they protect your files even if your Mac has problems."
            )
        elif best.destination_type == "network":
            explanation = (
                f"I found a network drive '{best.name}' that could work for backups. "
                f"It has {best.available_bytes / (1024**3):.0f} GB free. Network drives are good "
                "because they keep your backups separate from your Mac."
            )
        elif best.destination_type == "icloud":
            explanation = (
                "I can use iCloud Drive for your backups. "
                f"You have {best.available_bytes / (1024**3):.0f} GB free. "
                "This is convenient because your backups will sync across your devices, "
                "but an external drive would be safer for important files."
            )
        else:  # local
            explanation = (
                f"I can create a backup folder called '{best.name}' on your Mac. "
                f"You have {best.available_bytes / (1024**3):.0f} GB free. "
                "Note: This won't protect your files if your Mac's drive fails. "
                "For better protection, consider using an external drive."
            )
        
        return best, explanation
