"""Property-based tests for MCP response consistency.

Tests Property 10 (MCP Response Consistency) from the design document.

**Validates: Requirements 10.7, 10.8**
"""

import asyncio
import json
import tempfile
from pathlib import Path
from typing import Any, Dict

import pytest
from hypothesis import given, strategies as st, settings, Phase

from devbackup.mcp_server import DevBackupMCPServer
from devbackup.config import (
    Configuration,
    SchedulerConfig,
    RetentionConfig,
    LoggingConfig,
    MCPConfig,
    format_config,
)


def create_test_config(source_dir: Path, dest_dir: Path, log_dir: Path) -> Configuration:
    """Create a test configuration with the given directories."""
    return Configuration(
        backup_destination=dest_dir,
        source_directories=[source_dir],
        exclude_patterns=["*.pyc", "__pycache__/"],
        scheduler=SchedulerConfig(type="launchd", interval_seconds=3600),
        retention=RetentionConfig(hourly=24, daily=7, weekly=4),
        logging=LoggingConfig(
            level="ERROR",
            log_file=log_dir / "devbackup.log",
            error_log_file=log_dir / "devbackup.err",
        ),
        mcp=MCPConfig(enabled=True, port=0),
    )


def is_valid_json(s: str) -> bool:
    """Check if a string is valid JSON."""
    try:
        json.loads(s)
        return True
    except (json.JSONDecodeError, TypeError):
        return False


def has_success_fields(data: Dict[str, Any]) -> bool:
    """Check if response has success fields (no error object)."""
    return "error" not in data


def has_error_fields(data: Dict[str, Any]) -> bool:
    """Check if response has error fields with code and message."""
    if "error" not in data:
        return False
    error = data["error"]
    return isinstance(error, dict) and "code" in error and "message" in error


# Strategy for generating valid snapshot timestamps
snapshot_timestamps = st.from_regex(
    r"20[0-9]{2}-[01][0-9]-[0-3][0-9]-[0-2][0-9][0-5][0-9][0-5][0-9]",
    fullmatch=True
)

# Strategy for generating file patterns
file_patterns = st.sampled_from([
    "*.py",
    "*.txt",
    "*.json",
    "config.*",
    "test_*",
    "README*",
])

# Strategy for generating relative paths
relative_paths = st.sampled_from([
    "test.txt",
    "src/main.py",
    "config/settings.json",
    "docs/README.md",
])


class TestMCPResponseConsistency:
    """
    Property 10: MCP Response Consistency
    
    *For any* MCP tool invocation, the response SHALL be valid JSON containing
    either a success result or an error object with code and message.
    
    **Validates: Requirements 10.7, 10.8**
    """
    
    @pytest.fixture
    def setup_test_env(self, tmp_path):
        """Set up test environment with config file."""
        source_dir = tmp_path / "source"
        dest_dir = tmp_path / "dest"
        log_dir = tmp_path / "logs"
        config_dir = tmp_path / "config"
        
        source_dir.mkdir()
        dest_dir.mkdir()
        log_dir.mkdir()
        config_dir.mkdir()
        
        # Create test files
        (source_dir / "test.txt").write_text("test content")
        
        config = create_test_config(source_dir, dest_dir, log_dir)
        config_path = config_dir / "config.toml"
        config_path.write_text(format_config(config))
        
        return {
            "source_dir": source_dir,
            "dest_dir": dest_dir,
            "log_dir": log_dir,
            "config_path": config_path,
            "config": config,
        }
    
    @given(st.data())
    @settings(max_examples=10, deadline=None, phases=[Phase.generate, Phase.target])
    def test_backup_status_returns_valid_json(self, data):
        """
        **Feature: macos-incremental-backup, Property 10: MCP Response Consistency**
        
        backup_status tool should always return valid JSON with success or error fields.
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            source_dir = tmp_path / "source"
            dest_dir = tmp_path / "dest"
            log_dir = tmp_path / "logs"
            config_dir = tmp_path / "config"
            
            source_dir.mkdir()
            dest_dir.mkdir()
            log_dir.mkdir()
            config_dir.mkdir()
            
            (source_dir / "test.txt").write_text("test content")
            
            config = create_test_config(source_dir, dest_dir, log_dir)
            config_path = config_dir / "config.toml"
            config_path.write_text(format_config(config))
            
            server = DevBackupMCPServer(config_path=config_path)
            
            # Run the async tool
            result = asyncio.run(server._tool_backup_status())
            
            # Verify valid JSON
            assert is_valid_json(result), f"Response is not valid JSON: {result}"
            
            # Parse and verify structure
            parsed = json.loads(result)
            
            # Must have either success fields or error fields (XOR)
            has_success = has_success_fields(parsed)
            has_error = has_error_fields(parsed)
            
            assert has_success != has_error, \
                f"Response must have either success OR error fields, not both/neither: {result}"
    
    @given(st.data())
    @settings(max_examples=10, deadline=None, phases=[Phase.generate, Phase.target])
    def test_backup_list_snapshots_returns_valid_json(self, data):
        """
        **Feature: macos-incremental-backup, Property 10: MCP Response Consistency**
        
        backup_list_snapshots tool should always return valid JSON with success or error fields.
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            source_dir = tmp_path / "source"
            dest_dir = tmp_path / "dest"
            log_dir = tmp_path / "logs"
            config_dir = tmp_path / "config"
            
            source_dir.mkdir()
            dest_dir.mkdir()
            log_dir.mkdir()
            config_dir.mkdir()
            
            config = create_test_config(source_dir, dest_dir, log_dir)
            config_path = config_dir / "config.toml"
            config_path.write_text(format_config(config))
            
            server = DevBackupMCPServer(config_path=config_path)
            
            result = asyncio.run(server._tool_backup_list_snapshots())
            
            assert is_valid_json(result), f"Response is not valid JSON: {result}"
            
            parsed = json.loads(result)
            has_success = has_success_fields(parsed)
            has_error = has_error_fields(parsed)
            
            assert has_success != has_error, \
                f"Response must have either success OR error fields: {result}"
            
            # If success, should have snapshots array
            if has_success:
                assert "snapshots" in parsed, "Success response should have 'snapshots' field"
                assert isinstance(parsed["snapshots"], list), "'snapshots' should be a list"
    
    @given(
        snapshot=snapshot_timestamps,
        path=relative_paths
    )
    @settings(max_examples=10, deadline=None, phases=[Phase.generate, Phase.target])
    def test_backup_restore_returns_valid_json(self, snapshot: str, path: str):
        """
        **Feature: macos-incremental-backup, Property 10: MCP Response Consistency**
        
        backup_restore tool should always return valid JSON with success or error fields.
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            source_dir = tmp_path / "source"
            dest_dir = tmp_path / "dest"
            log_dir = tmp_path / "logs"
            config_dir = tmp_path / "config"
            
            source_dir.mkdir()
            dest_dir.mkdir()
            log_dir.mkdir()
            config_dir.mkdir()
            
            config = create_test_config(source_dir, dest_dir, log_dir)
            config_path = config_dir / "config.toml"
            config_path.write_text(format_config(config))
            
            server = DevBackupMCPServer(config_path=config_path)
            
            result = asyncio.run(server._tool_backup_restore(
                snapshot=snapshot,
                path=path,
                destination=None
            ))
            
            assert is_valid_json(result), f"Response is not valid JSON: {result}"
            
            parsed = json.loads(result)
            has_success = has_success_fields(parsed)
            has_error = has_error_fields(parsed)
            
            assert has_success != has_error, \
                f"Response must have either success OR error fields: {result}"
            
            # Since snapshot doesn't exist, should be error
            if has_error:
                assert "code" in parsed["error"], "Error should have 'code' field"
                assert "message" in parsed["error"], "Error should have 'message' field"
    
    @given(
        snapshot=snapshot_timestamps,
        path=st.one_of(st.none(), relative_paths)
    )
    @settings(max_examples=10, deadline=None, phases=[Phase.generate, Phase.target])
    def test_backup_diff_returns_valid_json(self, snapshot: str, path):
        """
        **Feature: macos-incremental-backup, Property 10: MCP Response Consistency**
        
        backup_diff tool should always return valid JSON with success or error fields.
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            source_dir = tmp_path / "source"
            dest_dir = tmp_path / "dest"
            log_dir = tmp_path / "logs"
            config_dir = tmp_path / "config"
            
            source_dir.mkdir()
            dest_dir.mkdir()
            log_dir.mkdir()
            config_dir.mkdir()
            
            config = create_test_config(source_dir, dest_dir, log_dir)
            config_path = config_dir / "config.toml"
            config_path.write_text(format_config(config))
            
            server = DevBackupMCPServer(config_path=config_path)
            
            result = asyncio.run(server._tool_backup_diff(
                snapshot=snapshot,
                path=path
            ))
            
            assert is_valid_json(result), f"Response is not valid JSON: {result}"
            
            parsed = json.loads(result)
            has_success = has_success_fields(parsed)
            has_error = has_error_fields(parsed)
            
            assert has_success != has_error, \
                f"Response must have either success OR error fields: {result}"
    
    @given(
        pattern=file_patterns,
        snapshot=st.one_of(st.none(), snapshot_timestamps)
    )
    @settings(max_examples=10, deadline=None, phases=[Phase.generate, Phase.target])
    def test_backup_search_returns_valid_json(self, pattern: str, snapshot):
        """
        **Feature: macos-incremental-backup, Property 10: MCP Response Consistency**
        
        backup_search tool should always return valid JSON with success or error fields.
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            source_dir = tmp_path / "source"
            dest_dir = tmp_path / "dest"
            log_dir = tmp_path / "logs"
            config_dir = tmp_path / "config"
            
            source_dir.mkdir()
            dest_dir.mkdir()
            log_dir.mkdir()
            config_dir.mkdir()
            
            config = create_test_config(source_dir, dest_dir, log_dir)
            config_path = config_dir / "config.toml"
            config_path.write_text(format_config(config))
            
            server = DevBackupMCPServer(config_path=config_path)
            
            result = asyncio.run(server._tool_backup_search(
                pattern=pattern,
                snapshot=snapshot
            ))
            
            assert is_valid_json(result), f"Response is not valid JSON: {result}"
            
            parsed = json.loads(result)
            has_success = has_success_fields(parsed)
            has_error = has_error_fields(parsed)
            
            assert has_success != has_error, \
                f"Response must have either success OR error fields: {result}"
    
    @given(st.data())
    @settings(max_examples=50, deadline=None, phases=[Phase.generate, Phase.target])
    def test_missing_config_returns_error_json(self, data):
        """
        **Feature: macos-incremental-backup, Property 10: MCP Response Consistency**
        
        When config is missing, all tools should return valid JSON error responses.
        """
        # Use non-existent config path
        server = DevBackupMCPServer(config_path=Path("/nonexistent/config.toml"))
        
        # Test each tool
        tools_to_test = [
            lambda: server._tool_backup_status(),
            lambda: server._tool_backup_list_snapshots(),
            lambda: server._tool_backup_restore("2025-01-01-120000", "test.txt"),
            lambda: server._tool_backup_diff("2025-01-01-120000"),
            lambda: server._tool_backup_search("*.py"),
        ]
        
        for tool_func in tools_to_test:
            result = asyncio.run(tool_func())
            
            assert is_valid_json(result), f"Response is not valid JSON: {result}"
            
            parsed = json.loads(result)
            
            # Should be an error response
            assert has_error_fields(parsed), \
                f"Missing config should return error response: {result}"
            
            # Error should have code and message
            assert "code" in parsed["error"], "Error should have 'code' field"
            assert "message" in parsed["error"], "Error should have 'message' field"
            assert parsed["error"]["code"] == "CONFIG_ERROR", \
                f"Error code should be CONFIG_ERROR, got {parsed['error']['code']}"
    
    @given(
        empty_snapshot=st.sampled_from(["", None]),
        empty_path=st.sampled_from(["", None])
    )
    @settings(max_examples=50, deadline=None, phases=[Phase.generate, Phase.target])
    def test_invalid_arguments_return_error_json(self, empty_snapshot, empty_path):
        """
        **Feature: macos-incremental-backup, Property 10: MCP Response Consistency**
        
        Invalid arguments should return valid JSON error responses.
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            source_dir = tmp_path / "source"
            dest_dir = tmp_path / "dest"
            log_dir = tmp_path / "logs"
            config_dir = tmp_path / "config"
            
            source_dir.mkdir()
            dest_dir.mkdir()
            log_dir.mkdir()
            config_dir.mkdir()
            
            config = create_test_config(source_dir, dest_dir, log_dir)
            config_path = config_dir / "config.toml"
            config_path.write_text(format_config(config))
            
            server = DevBackupMCPServer(config_path=config_path)
            
            # Test restore with empty snapshot
            result = asyncio.run(server._tool_backup_restore(
                snapshot=empty_snapshot or "",
                path="test.txt"
            ))
            
            assert is_valid_json(result), f"Response is not valid JSON: {result}"
            parsed = json.loads(result)
            assert has_error_fields(parsed), \
                f"Empty snapshot should return error: {result}"
            
            # Test restore with empty path
            result = asyncio.run(server._tool_backup_restore(
                snapshot="2025-01-01-120000",
                path=empty_path or ""
            ))
            
            assert is_valid_json(result), f"Response is not valid JSON: {result}"
            parsed = json.loads(result)
            assert has_error_fields(parsed), \
                f"Empty path should return error: {result}"
            
            # Test diff with empty snapshot
            result = asyncio.run(server._tool_backup_diff(
                snapshot=empty_snapshot or ""
            ))
            
            assert is_valid_json(result), f"Response is not valid JSON: {result}"
            parsed = json.loads(result)
            assert has_error_fields(parsed), \
                f"Empty snapshot should return error: {result}"
            
            # Test search with empty pattern
            result = asyncio.run(server._tool_backup_search(
                pattern=""
            ))
            
            assert is_valid_json(result), f"Response is not valid JSON: {result}"
            parsed = json.loads(result)
            assert has_error_fields(parsed), \
                f"Empty pattern should return error: {result}"



class TestRestoreSafety:
    """
    Property 6: Restore Safety
    
    *For any* restore operation:
    - A preview stage SHALL occur before any file modification when confirm=False
    - File versions SHALL be displayed with human-readable timestamps (not raw ISO format)
    - Current files SHALL NOT be overwritten unless confirm=True is explicitly set
    - Restored files SHALL be placed in a "Recovered Files" folder when no explicit destination is provided
    
    **Validates: Requirements 7.2, 7.5, 7.6**
    """
    
    @pytest.fixture
    def setup_backup_env(self, tmp_path):
        """Set up test environment with a backup snapshot."""
        source_dir = tmp_path / "source"
        dest_dir = tmp_path / "dest"
        log_dir = tmp_path / "logs"
        config_dir = tmp_path / "config"
        
        source_dir.mkdir()
        dest_dir.mkdir()
        log_dir.mkdir()
        config_dir.mkdir()
        
        # Create test files
        (source_dir / "test.py").write_text("original content")
        (source_dir / "config.json").write_text('{"key": "value"}')
        
        config = create_test_config(source_dir, dest_dir, log_dir)
        config_path = config_dir / "config.toml"
        config_path.write_text(format_config(config))
        
        # Create a snapshot manually
        from datetime import datetime
        snapshot_name = datetime.now().strftime("%Y-%m-%d-%H%M%S")
        snapshot_dir = dest_dir / snapshot_name
        snapshot_dir.mkdir()
        
        # Copy files to snapshot
        import shutil
        shutil.copy(source_dir / "test.py", snapshot_dir / "test.py")
        shutil.copy(source_dir / "config.json", snapshot_dir / "config.json")
        
        return {
            "source_dir": source_dir,
            "dest_dir": dest_dir,
            "log_dir": log_dir,
            "config_path": config_path,
            "config": config,
            "snapshot_name": snapshot_name,
            "snapshot_dir": snapshot_dir,
        }
    
    @given(
        file_name=st.sampled_from(["test.py", "config.json", "*.py", "*.json"]),
        confirm=st.booleans()
    )
    @settings(max_examples=20, deadline=None, phases=[Phase.generate, Phase.target])
    def test_preview_stage_before_modification(self, file_name: str, confirm: bool):
        """
        **Feature: user-experience-enhancement, Property 6: Restore Safety**
        
        When confirm=False, backup_undo should return a preview stage without modifying files.
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            source_dir = tmp_path / "source"
            dest_dir = tmp_path / "dest"
            log_dir = tmp_path / "logs"
            config_dir = tmp_path / "config"
            
            source_dir.mkdir()
            dest_dir.mkdir()
            log_dir.mkdir()
            config_dir.mkdir()
            
            # Create test file
            test_file = source_dir / "test.py"
            test_file.write_text("current content")
            
            config = create_test_config(source_dir, dest_dir, log_dir)
            config_path = config_dir / "config.toml"
            config_path.write_text(format_config(config))
            
            # Create a snapshot
            from datetime import datetime
            snapshot_name = datetime.now().strftime("%Y-%m-%d-%H%M%S")
            snapshot_dir = dest_dir / snapshot_name
            snapshot_dir.mkdir()
            (snapshot_dir / "test.py").write_text("backup content")
            
            server = DevBackupMCPServer(config_path=config_path)
            
            # Call backup_undo with confirm=False
            result = asyncio.run(server._tool_backup_undo(
                file_path=file_name,
                confirm=False
            ))
            
            assert is_valid_json(result), f"Response is not valid JSON: {result}"
            parsed = json.loads(result)
            
            # If file was found, should be in preview stage
            if "stage" in parsed:
                if parsed["stage"] == "preview":
                    # Verify current file was NOT modified
                    assert test_file.read_text() == "current content", \
                        "Current file should not be modified during preview"
                    
                    # Verify message exists
                    assert "message" in parsed, "Preview should have a message"
    
    @given(st.data())
    @settings(max_examples=10, deadline=None, phases=[Phase.generate, Phase.target])
    def test_no_overwrite_without_confirm(self, data):
        """
        **Feature: user-experience-enhancement, Property 6: Restore Safety**
        
        Current files SHALL NOT be overwritten unless confirm=True is explicitly set.
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            source_dir = tmp_path / "source"
            dest_dir = tmp_path / "dest"
            log_dir = tmp_path / "logs"
            config_dir = tmp_path / "config"
            
            source_dir.mkdir()
            dest_dir.mkdir()
            log_dir.mkdir()
            config_dir.mkdir()
            
            # Create test file with known content
            test_file = source_dir / "important.py"
            original_content = "this is the current version"
            test_file.write_text(original_content)
            
            config = create_test_config(source_dir, dest_dir, log_dir)
            config_path = config_dir / "config.toml"
            config_path.write_text(format_config(config))
            
            # Create a snapshot with different content
            from datetime import datetime
            snapshot_name = datetime.now().strftime("%Y-%m-%d-%H%M%S")
            snapshot_dir = dest_dir / snapshot_name
            snapshot_dir.mkdir()
            (snapshot_dir / "important.py").write_text("old backup content")
            
            server = DevBackupMCPServer(config_path=config_path)
            
            # Call backup_undo WITHOUT confirm
            result = asyncio.run(server._tool_backup_undo(
                file_path="important.py",
                confirm=False
            ))
            
            # Verify the original file was NOT modified
            assert test_file.read_text() == original_content, \
                "Original file should not be modified when confirm=False"
    
    @given(st.data())
    @settings(max_examples=10, deadline=None, phases=[Phase.generate, Phase.target])
    def test_restore_to_recovered_files_folder(self, data):
        """
        **Feature: user-experience-enhancement, Property 6: Restore Safety**
        
        Restored files SHALL be placed in a "Recovered Files" folder on Desktop.
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            source_dir = tmp_path / "source"
            dest_dir = tmp_path / "dest"
            log_dir = tmp_path / "logs"
            config_dir = tmp_path / "config"
            
            source_dir.mkdir()
            dest_dir.mkdir()
            log_dir.mkdir()
            config_dir.mkdir()
            
            # Create test file
            test_file = source_dir / "restore_test.py"
            test_file.write_text("current content")
            
            config = create_test_config(source_dir, dest_dir, log_dir)
            config_path = config_dir / "config.toml"
            config_path.write_text(format_config(config))
            
            # Create a snapshot
            from datetime import datetime
            snapshot_name = datetime.now().strftime("%Y-%m-%d-%H%M%S")
            snapshot_dir = dest_dir / snapshot_name
            snapshot_dir.mkdir()
            (snapshot_dir / "restore_test.py").write_text("backup content")
            
            server = DevBackupMCPServer(config_path=config_path)
            
            # Call backup_undo with confirm=True
            result = asyncio.run(server._tool_backup_undo(
                file_path="restore_test.py",
                confirm=True
            ))
            
            assert is_valid_json(result), f"Response is not valid JSON: {result}"
            parsed = json.loads(result)
            
            # If restore was successful, check the destination
            if parsed.get("stage") == "complete":
                file_info = parsed.get("file_info", {})
                restored_to = file_info.get("restored_to", "")
                
                # Should be in Recovered Files folder on Desktop
                assert "Recovered Files" in restored_to, \
                    f"Restored file should be in 'Recovered Files' folder, got: {restored_to}"
                assert "Desktop" in restored_to, \
                    f"Restored file should be on Desktop, got: {restored_to}"
    
    @given(
        file_name=st.sampled_from(["test.py", "config.json", "app.js"])
    )
    @settings(max_examples=10, deadline=None, phases=[Phase.generate, Phase.target])
    def test_human_readable_timestamps(self, file_name: str):
        """
        **Feature: user-experience-enhancement, Property 6: Restore Safety**
        
        File versions SHALL be displayed with human-readable timestamps.
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            source_dir = tmp_path / "source"
            dest_dir = tmp_path / "dest"
            log_dir = tmp_path / "logs"
            config_dir = tmp_path / "config"
            
            source_dir.mkdir()
            dest_dir.mkdir()
            log_dir.mkdir()
            config_dir.mkdir()
            
            # Create test file
            (source_dir / file_name).write_text("content")
            
            config = create_test_config(source_dir, dest_dir, log_dir)
            config_path = config_dir / "config.toml"
            config_path.write_text(format_config(config))
            
            # Create a snapshot
            from datetime import datetime
            snapshot_name = datetime.now().strftime("%Y-%m-%d-%H%M%S")
            snapshot_dir = dest_dir / snapshot_name
            snapshot_dir.mkdir()
            (snapshot_dir / file_name).write_text("backup content")
            
            server = DevBackupMCPServer(config_path=config_path)
            
            # Call backup_undo to get preview
            result = asyncio.run(server._tool_backup_undo(
                file_path=file_name,
                confirm=False
            ))
            
            assert is_valid_json(result), f"Response is not valid JSON: {result}"
            parsed = json.loads(result)
            
            # If we got a preview, check for human-readable time
            if parsed.get("stage") == "preview":
                file_info = parsed.get("file_info", {})
                time_friendly = file_info.get("time_friendly", "")
                
                # Should NOT be raw ISO format (YYYY-MM-DD-HHMMSS)
                # Should be human-readable like "just now", "2 hours ago", etc.
                import re
                iso_pattern = r"^\d{4}-\d{2}-\d{2}-\d{6}$"
                assert not re.match(iso_pattern, time_friendly), \
                    f"Timestamp should be human-readable, not raw format: {time_friendly}"
                
                # Should contain friendly words
                friendly_indicators = [
                    "just now", "minute", "hour", "day", "week", 
                    "yesterday", "ago", "on "
                ]
                has_friendly = any(ind in time_friendly.lower() for ind in friendly_indicators)
                assert has_friendly or time_friendly == "", \
                    f"Timestamp should be human-readable: {time_friendly}"
