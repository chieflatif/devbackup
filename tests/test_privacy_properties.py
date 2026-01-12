"""Property-based tests for Privacy Compliance.

**Property 9: Privacy Compliance**
**Validates: Requirements 10.3**

Tests that:
- No network requests SHALL be made to external servers during operations
- All backup data SHALL remain on user-controlled storage
- No telemetry or usage data SHALL be collected without explicit opt-in
"""

import socket
import tempfile
import unittest.mock
from pathlib import Path
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, patch

import pytest
from hypothesis import given, settings, assume, HealthCheck
from hypothesis import strategies as st

from devbackup.backup import run_backup, BackupResult
from devbackup.config import (
    Configuration,
    SchedulerConfig,
    RetentionConfig,
    LoggingConfig,
    MCPConfig,
    RetryConfig,
    NotificationConfig,
)
from devbackup.discovery import AutoDiscovery, DiscoveredProject, DiscoveredDestination
from devbackup.language import PlainLanguageTranslator
from devbackup.defaults import SmartDefaults
from devbackup.snapshot import SnapshotEngine


# Strategy for generating valid project names
project_name_strategy = st.text(
    alphabet=st.characters(whitelist_categories=('L', 'N')),
    min_size=1,
    max_size=20,
)

# Strategy for generating file content
file_content_strategy = st.binary(min_size=0, max_size=1000)

# Strategy for generating exclude patterns
exclude_pattern_strategy = st.lists(
    st.text(
        alphabet=st.characters(whitelist_categories=('L', 'N', 'P')),
        min_size=1,
        max_size=20,
    ),
    min_size=0,
    max_size=5,
)


class NetworkCallTracker:
    """
    Context manager that tracks and blocks network calls.
    
    Used to verify that no external network requests are made during
    backup operations.
    """
    
    def __init__(self):
        self.calls: List[Dict[str, Any]] = []
        self._original_socket = None
        self._original_create_connection = None
    
    def __enter__(self):
        """Block and track all socket connections."""
        self._original_socket = socket.socket
        self._original_create_connection = socket.create_connection
        
        tracker = self
        
        class TrackedSocket(socket.socket):
            """Socket wrapper that tracks connection attempts."""
            
            def connect(self, address):
                # Allow localhost/Unix socket connections (for IPC)
                if isinstance(address, str):
                    # Unix socket path
                    tracker.calls.append({
                        "type": "unix_socket",
                        "address": address,
                        "allowed": True,
                    })
                    return super().connect(address)
                
                host, port = address[:2]
                is_local = host in ('localhost', '127.0.0.1', '::1', '')
                
                tracker.calls.append({
                    "type": "tcp",
                    "host": host,
                    "port": port,
                    "allowed": is_local,
                })
                
                if not is_local:
                    raise ConnectionRefusedError(
                        f"Network call blocked by privacy test: {host}:{port}"
                    )
                
                return super().connect(address)
        
        def tracked_create_connection(address, *args, **kwargs):
            host, port = address[:2]
            is_local = host in ('localhost', '127.0.0.1', '::1', '')
            
            tracker.calls.append({
                "type": "create_connection",
                "host": host,
                "port": port,
                "allowed": is_local,
            })
            
            if not is_local:
                raise ConnectionRefusedError(
                    f"Network call blocked by privacy test: {host}:{port}"
                )
            
            return self._original_create_connection(address, *args, **kwargs)
        
        socket.socket = TrackedSocket
        socket.create_connection = tracked_create_connection
        
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Restore original socket implementation."""
        socket.socket = self._original_socket
        socket.create_connection = self._original_create_connection
        return False
    
    def get_external_calls(self) -> List[Dict[str, Any]]:
        """Return list of blocked external network calls."""
        return [c for c in self.calls if not c.get("allowed", True)]
    
    def has_external_calls(self) -> bool:
        """Check if any external network calls were attempted."""
        return len(self.get_external_calls()) > 0


class TestPrivacyComplianceProperty:
    """
    Property 9: Privacy Compliance
    
    *For any* operation performed by the backup system:
    - No network requests SHALL be made to external servers
    - All backup data SHALL remain on user-controlled storage
    - No telemetry or usage data SHALL be collected without explicit opt-in
    
    **Validates: Requirements 10.3**
    """
    
    @given(
        project_name=project_name_strategy,
        file_content=file_content_strategy,
    )
    @settings(
        max_examples=100,
        deadline=None,
        suppress_health_check=[HealthCheck.too_slow],
    )
    def test_backup_operation_no_external_network_calls(
        self,
        project_name: str,
        file_content: bytes,
    ):
        """
        Feature: user-experience-enhancement, Property 9: Privacy Compliance
        
        For any backup operation, no external network requests SHALL be made.
        
        **Validates: Requirements 10.3**
        """
        # Skip empty project names
        assume(len(project_name.strip()) > 0)
        
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            
            # Create source directory with test file
            source_dir = tmpdir_path / "source" / project_name
            source_dir.mkdir(parents=True)
            
            test_file = source_dir / "test_file.txt"
            test_file.write_bytes(file_content)
            
            # Create backup destination
            backup_dest = tmpdir_path / "backups"
            backup_dest.mkdir(parents=True)
            
            # Create configuration
            config = Configuration(
                backup_destination=backup_dest,
                source_directories=[source_dir],
                exclude_patterns=[],
            )
            
            # Track network calls during backup
            with NetworkCallTracker() as tracker:
                result = run_backup(config=config)
            
            # Verify no external network calls were made
            external_calls = tracker.get_external_calls()
            assert not tracker.has_external_calls(), (
                f"Backup operation made external network calls: {external_calls}"
            )
    
    @given(
        project_names=st.lists(project_name_strategy, min_size=1, max_size=3),
    )
    @settings(
        max_examples=100,
        deadline=None,
        suppress_health_check=[HealthCheck.too_slow],
    )
    def test_discovery_no_external_network_calls(
        self,
        project_names: List[str],
    ):
        """
        Feature: user-experience-enhancement, Property 9: Privacy Compliance
        
        For any auto-discovery operation, no external network requests SHALL be made.
        
        **Validates: Requirements 10.3**
        """
        # Filter out empty names
        project_names = [n for n in project_names if n.strip()]
        assume(len(project_names) > 0)
        
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            
            # Create project directories with markers
            for name in project_names:
                project_dir = tmpdir_path / name
                project_dir.mkdir(parents=True, exist_ok=True)
                # Add a project marker
                (project_dir / "pyproject.toml").write_text("[project]\nname = 'test'\n")
            
            # Track network calls during discovery
            # Use scan_locations parameter instead of patching class attribute
            discovery = AutoDiscovery(scan_locations=[tmpdir_path])
            
            with NetworkCallTracker() as tracker:
                # Discover projects
                projects = discovery.discover_projects()
            
            # Verify no external network calls were made
            external_calls = tracker.get_external_calls()
            assert not tracker.has_external_calls(), (
                f"Discovery operation made external network calls: {external_calls}"
            )
    
    @given(
        size_bytes=st.integers(min_value=0, max_value=10_000_000_000),
    )
    @settings(
        max_examples=100,
        deadline=None,
        suppress_health_check=[HealthCheck.too_slow],
    )
    def test_language_translation_no_external_network_calls(
        self,
        size_bytes: int,
    ):
        """
        Feature: user-experience-enhancement, Property 9: Privacy Compliance
        
        For any language translation operation, no external network requests SHALL be made.
        
        **Validates: Requirements 10.3**
        """
        translator = PlainLanguageTranslator()
        
        with NetworkCallTracker() as tracker:
            # Perform various translations
            translator.translate_size(size_bytes)
            translator.translate_file_count(size_bytes % 10000)
        
        # Verify no external network calls were made
        external_calls = tracker.get_external_calls()
        assert not tracker.has_external_calls(), (
            f"Translation operation made external network calls: {external_calls}"
        )
    
    @given(
        project_name=project_name_strategy,
        file_content=file_content_strategy,
    )
    @settings(
        max_examples=100,
        deadline=None,
        suppress_health_check=[HealthCheck.too_slow],
    )
    def test_backup_data_remains_local(
        self,
        project_name: str,
        file_content: bytes,
    ):
        """
        Feature: user-experience-enhancement, Property 9: Privacy Compliance
        
        For any backup operation, all data SHALL remain on user-controlled storage.
        
        **Validates: Requirements 10.3**
        """
        # Skip empty project names
        assume(len(project_name.strip()) > 0)
        
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            
            # Create source directory with test file
            source_dir = tmpdir_path / "source" / project_name
            source_dir.mkdir(parents=True)
            
            test_file = source_dir / "test_file.txt"
            test_file.write_bytes(file_content)
            
            # Create backup destination
            backup_dest = tmpdir_path / "backups"
            backup_dest.mkdir(parents=True)
            
            # Create configuration
            config = Configuration(
                backup_destination=backup_dest,
                source_directories=[source_dir],
                exclude_patterns=[],
            )
            
            # Run backup
            result = run_backup(config=config)
            
            if result.success:
                # Verify backup data is in the expected local location
                assert backup_dest.exists(), "Backup destination must exist"
                
                # Find snapshot directories
                snapshots = list(backup_dest.glob("20*"))  # Snapshot dirs start with year
                
                if snapshots:
                    # Verify data is in the snapshot
                    snapshot_dir = snapshots[0]
                    assert snapshot_dir.is_dir(), "Snapshot must be a directory"
                    
                    # All snapshot contents should be under the backup destination
                    for item in snapshot_dir.rglob("*"):
                        assert str(item).startswith(str(backup_dest)), (
                            f"Backup data must remain under backup destination: {item}"
                        )


class TestNoTelemetryCollection:
    """
    Tests verifying no telemetry or usage data is collected.
    
    **Validates: Requirements 10.3**
    """
    
    def test_config_has_no_telemetry_settings(self):
        """
        Feature: user-experience-enhancement, Property 9: Privacy Compliance
        
        Configuration SHALL NOT have telemetry settings enabled by default.
        
        **Validates: Requirements 10.3**
        """
        # Create default configuration
        config = Configuration(
            backup_destination=Path("/tmp/test"),
            source_directories=[Path("/tmp/source")],
        )
        
        # Verify no telemetry-related attributes exist or are disabled
        # Check that there's no telemetry_enabled or analytics_enabled attribute
        assert not hasattr(config, 'telemetry_enabled') or not config.telemetry_enabled, (
            "Configuration must not have telemetry enabled by default"
        )
        assert not hasattr(config, 'analytics_enabled') or not config.analytics_enabled, (
            "Configuration must not have analytics enabled by default"
        )
        assert not hasattr(config, 'crash_reporting') or not config.crash_reporting, (
            "Configuration must not have crash reporting enabled by default"
        )
    
    @given(
        project_name=project_name_strategy,
    )
    @settings(
        max_examples=100,
        deadline=None,
        suppress_health_check=[HealthCheck.too_slow],
    )
    def test_smart_defaults_no_telemetry(
        self,
        project_name: str,
    ):
        """
        Feature: user-experience-enhancement, Property 9: Privacy Compliance
        
        SmartDefaults SHALL NOT enable any telemetry or data collection.
        
        **Validates: Requirements 10.3**
        """
        assume(len(project_name.strip()) > 0)
        
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            
            # Create a mock project
            project_dir = tmpdir_path / project_name
            project_dir.mkdir(parents=True)
            (project_dir / "pyproject.toml").write_text("[project]\nname = 'test'\n")
            
            # Create mock destination
            dest_dir = tmpdir_path / "backups"
            dest_dir.mkdir(parents=True)
            
            project = DiscoveredProject(
                path=project_dir,
                name=project_name,
                project_type="python",
                estimated_size_bytes=1000,
                marker_files=["pyproject.toml"],
            )
            
            destination = DiscoveredDestination(
                path=dest_dir,
                name="backups",
                destination_type="local",
                available_bytes=1_000_000_000,
                total_bytes=2_000_000_000,
                is_removable=False,
                recommendation_score=50,
            )
            
            # Generate config using SmartDefaults
            defaults = SmartDefaults()
            config = defaults.generate_config([project], destination)
            
            # Verify no telemetry settings
            assert not hasattr(config, 'telemetry_enabled') or not config.telemetry_enabled
            assert not hasattr(config, 'analytics_enabled') or not config.analytics_enabled


class TestDataLocalityProperty:
    """
    Tests verifying all backup data remains on user-controlled storage.
    
    **Validates: Requirements 10.3**
    """
    
    @given(
        file_count=st.integers(min_value=1, max_value=10),
        file_content=file_content_strategy,
    )
    @settings(
        max_examples=100,
        deadline=None,
        suppress_health_check=[HealthCheck.too_slow],
    )
    def test_snapshot_data_locality(
        self,
        file_count: int,
        file_content: bytes,
    ):
        """
        Feature: user-experience-enhancement, Property 9: Privacy Compliance
        
        For any snapshot operation, all data SHALL remain in the configured destination.
        
        **Validates: Requirements 10.3**
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            
            # Create source directory with files
            source_dir = tmpdir_path / "source"
            source_dir.mkdir(parents=True)
            
            for i in range(file_count):
                test_file = source_dir / f"file_{i}.txt"
                test_file.write_bytes(file_content)
            
            # Create backup destination
            backup_dest = tmpdir_path / "backups"
            backup_dest.mkdir(parents=True)
            
            # Create snapshot engine
            engine = SnapshotEngine(
                destination=backup_dest,
                exclude_patterns=[],
            )
            
            # Create snapshot
            result = engine.create_snapshot([source_dir])
            
            if result.success and result.snapshot_path:
                # Verify all snapshot data is under the destination
                for item in result.snapshot_path.rglob("*"):
                    assert str(item).startswith(str(backup_dest)), (
                        f"Snapshot data must be under destination: {item}"
                    )
                
                # Verify no data was written elsewhere in tmpdir
                # (outside of source and backups)
                for item in tmpdir_path.rglob("*"):
                    item_str = str(item)
                    is_source = item_str.startswith(str(source_dir))
                    is_backup = item_str.startswith(str(backup_dest))
                    
                    assert is_source or is_backup, (
                        f"Data found outside source/backup directories: {item}"
                    )
    
    def test_no_cloud_upload_in_backup_flow(self):
        """
        Feature: user-experience-enhancement, Property 9: Privacy Compliance
        
        The backup flow SHALL NOT include any cloud upload functionality.
        
        **Validates: Requirements 10.3**
        """
        # Verify that backup.py doesn't import cloud-related modules
        import devbackup.backup as backup_module
        import inspect
        
        source = inspect.getsource(backup_module)
        
        # Check for cloud service imports
        cloud_indicators = [
            'boto3',  # AWS
            'google.cloud',  # GCP
            'azure',  # Azure
            's3',  # S3
            'cloudflare',
            'dropbox',
            'onedrive',
            'requests.post',  # HTTP POST (potential data upload)
            'urllib.request.urlopen',  # URL requests
            'httpx',  # HTTP client
            'aiohttp',  # Async HTTP client
        ]
        
        for indicator in cloud_indicators:
            assert indicator not in source.lower(), (
                f"Backup module should not contain cloud-related code: {indicator}"
            )


class TestPrivacyInNotifications:
    """
    Tests verifying notifications don't leak data externally.
    
    **Validates: Requirements 10.3**
    """
    
    @given(
        message=st.text(min_size=1, max_size=100),
    )
    @settings(
        max_examples=100,
        deadline=None,
        suppress_health_check=[HealthCheck.too_slow],
    )
    def test_notifications_are_local_only(
        self,
        message: str,
    ):
        """
        Feature: user-experience-enhancement, Property 9: Privacy Compliance
        
        Notifications SHALL only use local system notification mechanisms.
        
        **Validates: Requirements 10.3**
        """
        from devbackup.notify import Notifier, NotificationConfig
        
        config = NotificationConfig(
            notify_on_success=True,
            notify_on_failure=True,
        )
        
        notifier = Notifier(config)
        
        # Track network calls during notification
        with NetworkCallTracker() as tracker:
            # These should use local macOS notification center only
            notifier.notify_success(
                snapshot_name="test-snapshot",
                duration_seconds=10.0,
                files_transferred=100,
            )
            notifier.notify_failure(
                error_message=message,
                duration_seconds=5.0,
            )
        
        # Verify no external network calls were made
        external_calls = tracker.get_external_calls()
        assert not tracker.has_external_calls(), (
            f"Notification made external network calls: {external_calls}"
        )

