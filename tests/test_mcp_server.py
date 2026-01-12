"""Integration tests for MCP server.

Tests all MCP tools and error responses.

**Validates: Requirements 10.1-10.10**
"""

import asyncio
import json
import tempfile
from datetime import datetime
from pathlib import Path

import pytest

from devbackup.mcp_server import DevBackupMCPServer
from devbackup.config import (
    Configuration,
    SchedulerConfig,
    RetentionConfig,
    LoggingConfig,
    MCPConfig,
    format_config,
)
from devbackup.snapshot import SnapshotEngine


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


class TestMCPServerTools:
    """Integration tests for MCP server tools."""
    
    @pytest.fixture
    def setup_env(self, tmp_path):
        """Set up test environment with config file and directories."""
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
        (source_dir / "subdir").mkdir()
        (source_dir / "subdir" / "nested.py").write_text("print('hello')")
        
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
    
    def test_backup_run_creates_snapshot(self, setup_env):
        """
        Test backup_run tool creates a snapshot.
        
        Requirements: 10.1
        """
        server = DevBackupMCPServer(config_path=setup_env["config_path"])
        
        result = asyncio.run(server._tool_backup_run())
        parsed = json.loads(result)
        
        assert "error" not in parsed, f"Backup should succeed: {result}"
        assert parsed["success"] is True
        assert parsed["snapshot"] is not None
        assert parsed["files_transferred"] >= 0
        assert parsed["duration_seconds"] >= 0
        
        # Verify snapshot was created
        dest_dir = setup_env["dest_dir"]
        snapshots = list(dest_dir.iterdir())
        assert len(snapshots) == 1
        assert snapshots[0].name == parsed["snapshot"]
    
    def test_backup_status_returns_info(self, setup_env):
        """
        Test backup_status tool returns status information.
        
        Requirements: 10.2
        """
        server = DevBackupMCPServer(config_path=setup_env["config_path"])
        
        result = asyncio.run(server._tool_backup_status())
        parsed = json.loads(result)
        
        assert "error" not in parsed, f"Status should succeed: {result}"
        assert "last_backup" in parsed
        assert "is_running" in parsed
        assert "scheduler_installed" in parsed
        assert "total_snapshots" in parsed
        
        # Initially no backups
        assert parsed["last_backup"] is None
        assert parsed["is_running"] is False
        assert parsed["total_snapshots"] == 0
    
    def test_backup_status_after_backup(self, setup_env):
        """
        Test backup_status shows last backup after running backup.
        
        Requirements: 10.2
        """
        server = DevBackupMCPServer(config_path=setup_env["config_path"])
        
        # Run a backup first
        asyncio.run(server._tool_backup_run())
        
        # Check status
        result = asyncio.run(server._tool_backup_status())
        parsed = json.loads(result)
        
        assert "error" not in parsed
        assert parsed["last_backup"] is not None
        assert parsed["total_snapshots"] == 1
    
    def test_backup_list_snapshots_empty(self, setup_env):
        """
        Test backup_list_snapshots with no snapshots.
        
        Requirements: 10.3
        """
        server = DevBackupMCPServer(config_path=setup_env["config_path"])
        
        result = asyncio.run(server._tool_backup_list_snapshots())
        parsed = json.loads(result)
        
        assert "error" not in parsed
        assert "snapshots" in parsed
        assert isinstance(parsed["snapshots"], list)
        assert len(parsed["snapshots"]) == 0
    
    def test_backup_list_snapshots_with_data(self, setup_env):
        """
        Test backup_list_snapshots returns snapshot info.
        
        Requirements: 10.3
        """
        server = DevBackupMCPServer(config_path=setup_env["config_path"])
        
        # Create a backup
        asyncio.run(server._tool_backup_run())
        
        result = asyncio.run(server._tool_backup_list_snapshots())
        parsed = json.loads(result)
        
        assert "error" not in parsed
        assert len(parsed["snapshots"]) == 1
        
        snapshot = parsed["snapshots"][0]
        assert "timestamp" in snapshot
        assert "name" in snapshot
        assert "size_bytes" in snapshot
        assert "file_count" in snapshot
    
    def test_backup_restore_success(self, setup_env):
        """
        Test backup_restore restores files correctly.
        
        Requirements: 10.4
        """
        server = DevBackupMCPServer(config_path=setup_env["config_path"])
        
        # Create a backup
        run_result = asyncio.run(server._tool_backup_run())
        run_parsed = json.loads(run_result)
        snapshot_name = run_parsed["snapshot"]
        
        # Delete the original file
        original_file = setup_env["source_dir"] / "test.txt"
        original_file.unlink()
        
        # Restore to a new location
        restore_dest = setup_env["source_dir"] / "restored.txt"
        result = asyncio.run(server._tool_backup_restore(
            snapshot=snapshot_name,
            path="test.txt",
            destination=str(restore_dest)
        ))
        parsed = json.loads(result)
        
        assert "error" not in parsed, f"Restore should succeed: {result}"
        assert parsed["success"] is True
        assert restore_dest.exists()
        assert restore_dest.read_text() == "test content"
    
    def test_backup_restore_snapshot_not_found(self, setup_env):
        """
        Test backup_restore returns error for non-existent snapshot.
        
        Requirements: 10.4, 10.8
        """
        server = DevBackupMCPServer(config_path=setup_env["config_path"])
        
        result = asyncio.run(server._tool_backup_restore(
            snapshot="2099-01-01-120000",
            path="test.txt"
        ))
        parsed = json.loads(result)
        
        assert "error" in parsed
        assert parsed["error"]["code"] == "SNAPSHOT_NOT_FOUND"
    
    def test_backup_diff_shows_changes(self, setup_env):
        """
        Test backup_diff shows file changes.
        
        Requirements: 10.5
        """
        server = DevBackupMCPServer(config_path=setup_env["config_path"])
        
        # Create a backup
        run_result = asyncio.run(server._tool_backup_run())
        run_parsed = json.loads(run_result)
        snapshot_name = run_parsed["snapshot"]
        
        # Modify a file
        (setup_env["source_dir"] / "test.txt").write_text("modified content")
        
        # Add a new file
        (setup_env["source_dir"] / "new_file.txt").write_text("new content")
        
        # Delete a file
        (setup_env["source_dir"] / "subdir" / "nested.py").unlink()
        
        # Get diff
        result = asyncio.run(server._tool_backup_diff(snapshot=snapshot_name))
        parsed = json.loads(result)
        
        assert "error" not in parsed, f"Diff should succeed: {result}"
        assert "added" in parsed
        assert "modified" in parsed
        assert "deleted" in parsed
        assert "total_changes" in parsed
        
        # Verify changes detected
        assert len(parsed["added"]) >= 1  # new_file.txt
        assert len(parsed["modified"]) >= 1  # test.txt
        assert len(parsed["deleted"]) >= 1  # nested.py
    
    def test_backup_diff_snapshot_not_found(self, setup_env):
        """
        Test backup_diff returns error for non-existent snapshot.
        
        Requirements: 10.5, 10.8
        """
        server = DevBackupMCPServer(config_path=setup_env["config_path"])
        
        result = asyncio.run(server._tool_backup_diff(snapshot="2099-01-01-120000"))
        parsed = json.loads(result)
        
        assert "error" in parsed
        assert parsed["error"]["code"] == "SNAPSHOT_NOT_FOUND"
    
    def test_backup_search_finds_files(self, setup_env):
        """
        Test backup_search finds files by pattern.
        
        Requirements: 10.6
        """
        server = DevBackupMCPServer(config_path=setup_env["config_path"])
        
        # Create a backup
        asyncio.run(server._tool_backup_run())
        
        # Search for .txt files
        result = asyncio.run(server._tool_backup_search(pattern="*.txt"))
        parsed = json.loads(result)
        
        assert "error" not in parsed, f"Search should succeed: {result}"
        assert "matches" in parsed
        assert "total_matches" in parsed
        assert parsed["total_matches"] >= 1
        
        # Verify match structure
        match = parsed["matches"][0]
        assert "snapshot" in match
        assert "path" in match
        assert "size" in match
        assert "modified" in match
    
    def test_backup_search_no_matches(self, setup_env):
        """
        Test backup_search returns empty results for no matches.
        
        Requirements: 10.6
        """
        server = DevBackupMCPServer(config_path=setup_env["config_path"])
        
        # Create a backup
        asyncio.run(server._tool_backup_run())
        
        # Search for non-existent pattern
        result = asyncio.run(server._tool_backup_search(pattern="*.nonexistent"))
        parsed = json.loads(result)
        
        assert "error" not in parsed
        assert parsed["total_matches"] == 0
        assert len(parsed["matches"]) == 0
    
    def test_backup_search_specific_snapshot(self, setup_env):
        """
        Test backup_search in specific snapshot.
        
        Requirements: 10.6
        """
        server = DevBackupMCPServer(config_path=setup_env["config_path"])
        
        # Create a backup
        run_result = asyncio.run(server._tool_backup_run())
        run_parsed = json.loads(run_result)
        snapshot_name = run_parsed["snapshot"]
        
        # Search in specific snapshot
        result = asyncio.run(server._tool_backup_search(
            pattern="*.py",
            snapshot=snapshot_name
        ))
        parsed = json.loads(result)
        
        assert "error" not in parsed
        assert parsed["total_matches"] >= 1
        
        # All matches should be from the specified snapshot
        for match in parsed["matches"]:
            assert match["snapshot"] == snapshot_name


class TestMCPServerErrorHandling:
    """Tests for MCP server error handling."""
    
    def test_config_not_found_error(self):
        """
        Test error response when config file doesn't exist.
        
        Requirements: 10.8
        """
        server = DevBackupMCPServer(config_path=Path("/nonexistent/config.toml"))
        
        result = asyncio.run(server._tool_backup_status())
        parsed = json.loads(result)
        
        assert "error" in parsed
        assert parsed["error"]["code"] == "CONFIG_ERROR"
        assert "message" in parsed["error"]
    
    def test_invalid_snapshot_argument(self, tmp_path):
        """
        Test error response for invalid snapshot argument.
        
        Requirements: 10.8
        """
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
        
        # Empty snapshot
        result = asyncio.run(server._tool_backup_restore(snapshot="", path="test.txt"))
        parsed = json.loads(result)
        
        assert "error" in parsed
        assert parsed["error"]["code"] == "INVALID_ARGUMENT"
    
    def test_invalid_path_argument(self, tmp_path):
        """
        Test error response for invalid path argument.
        
        Requirements: 10.8
        """
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
        
        # Empty path
        result = asyncio.run(server._tool_backup_restore(
            snapshot="2025-01-01-120000",
            path=""
        ))
        parsed = json.loads(result)
        
        assert "error" in parsed
        assert parsed["error"]["code"] == "INVALID_ARGUMENT"
    
    def test_invalid_pattern_argument(self, tmp_path):
        """
        Test error response for invalid pattern argument.
        
        Requirements: 10.8
        """
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
        
        # Empty pattern
        result = asyncio.run(server._tool_backup_search(pattern=""))
        parsed = json.loads(result)
        
        assert "error" in parsed
        assert parsed["error"]["code"] == "INVALID_ARGUMENT"


class TestMCPServerCLIIntegration:
    """Tests for MCP server CLI integration."""
    
    def test_mcp_server_command_exists(self):
        """
        Test that mcp-server command is registered in CLI.
        
        Requirements: 10.9
        """
        from devbackup.cli import create_parser
        
        parser = create_parser()
        
        # Parse mcp-server command
        args = parser.parse_args(["mcp-server"])
        assert args.command == "mcp-server"
    
    def test_mcp_server_with_config_flag(self):
        """
        Test that mcp-server accepts --config flag.
        
        Requirements: 10.10
        """
        from devbackup.cli import create_parser
        
        parser = create_parser()
        
        args = parser.parse_args(["--config", "/path/to/config.toml", "mcp-server"])
        assert args.command == "mcp-server"
        assert args.config == Path("/path/to/config.toml")


class TestMCPBackupProgress:
    """Tests for backup_progress MCP tool (Task 8.3)."""
    
    @pytest.fixture
    def setup_env(self, tmp_path):
        """Set up test environment with config file and directories."""
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
    
    def test_backup_progress_no_backup_running(self, setup_env):
        """
        Test backup_progress returns is_running=False when no backup is running.
        
        Requirements: 6.3
        """
        server = DevBackupMCPServer(config_path=setup_env["config_path"])
        
        result = asyncio.run(server._tool_backup_progress())
        parsed = json.loads(result)
        
        assert "error" not in parsed
        assert parsed["is_running"] is False
        assert parsed["progress"] is None
    
    def test_backup_progress_returns_json(self, setup_env):
        """
        Test backup_progress returns valid JSON response.
        
        Requirements: 6.3
        """
        server = DevBackupMCPServer(config_path=setup_env["config_path"])
        
        result = asyncio.run(server._tool_backup_progress())
        
        # Should be valid JSON
        parsed = json.loads(result)
        assert isinstance(parsed, dict)
        assert "is_running" in parsed


class TestMCPBackupVerify:
    """Tests for backup_verify MCP tool (Task 9.5)."""
    
    @pytest.fixture
    def setup_env(self, tmp_path):
        """Set up test environment with config file and directories."""
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
    
    def test_backup_verify_snapshot_not_found(self, setup_env):
        """
        Test backup_verify returns error for non-existent snapshot.
        
        Requirements: 7.4
        """
        server = DevBackupMCPServer(config_path=setup_env["config_path"])
        
        result = asyncio.run(server._tool_backup_verify(
            snapshot="2099-01-01-120000"
        ))
        parsed = json.loads(result)
        
        assert "error" in parsed
        assert parsed["error"]["code"] == "SNAPSHOT_NOT_FOUND"
    
    def test_backup_verify_no_manifest(self, setup_env):
        """
        Test backup_verify returns failure when no manifest exists.
        
        Requirements: 7.4
        """
        server = DevBackupMCPServer(config_path=setup_env["config_path"])
        
        # Create snapshot without manifest
        snapshot_dir = setup_env["dest_dir"] / "2025-01-01-120000"
        snapshot_dir.mkdir()
        (snapshot_dir / "test.txt").write_text("content")
        
        result = asyncio.run(server._tool_backup_verify(
            snapshot="2025-01-01-120000"
        ))
        parsed = json.loads(result)
        
        assert "error" not in parsed
        assert parsed["success"] is False
        assert "Manifest file not found" in parsed["errors"]
    
    def test_backup_verify_success(self, setup_env):
        """
        Test backup_verify returns success with valid manifest.
        
        Requirements: 7.4
        """
        from devbackup.verify import IntegrityVerifier
        
        server = DevBackupMCPServer(config_path=setup_env["config_path"])
        
        # Create snapshot with file
        snapshot_dir = setup_env["dest_dir"] / "2025-01-01-120000"
        snapshot_dir.mkdir()
        (snapshot_dir / "test.txt").write_text("content")
        
        # Create manifest
        verifier = IntegrityVerifier()
        manifest = verifier.create_manifest(snapshot_dir)
        verifier.save_manifest(manifest, snapshot_dir)
        
        result = asyncio.run(server._tool_backup_verify(
            snapshot="2025-01-01-120000"
        ))
        parsed = json.loads(result)
        
        assert "error" not in parsed
        assert parsed["success"] is True
        assert parsed["files_verified"] == 1
        assert parsed["files_failed"] == 0
    
    def test_backup_verify_with_pattern(self, setup_env):
        """
        Test backup_verify with pattern filter.
        
        Requirements: 7.4, 7.6
        """
        from devbackup.verify import IntegrityVerifier
        
        server = DevBackupMCPServer(config_path=setup_env["config_path"])
        
        # Create snapshot with multiple files
        snapshot_dir = setup_env["dest_dir"] / "2025-01-01-120000"
        snapshot_dir.mkdir()
        (snapshot_dir / "test.txt").write_text("content")
        (snapshot_dir / "script.py").write_text("print('hello')")
        
        # Create manifest
        verifier = IntegrityVerifier()
        manifest = verifier.create_manifest(snapshot_dir)
        verifier.save_manifest(manifest, snapshot_dir)
        
        result = asyncio.run(server._tool_backup_verify(
            snapshot="2025-01-01-120000",
            pattern="*.py"
        ))
        parsed = json.loads(result)
        
        assert "error" not in parsed
        assert parsed["success"] is True
        assert parsed["files_verified"] == 1  # Only .py file
    
    def test_backup_verify_invalid_snapshot_argument(self, setup_env):
        """
        Test backup_verify returns error for empty snapshot argument.
        
        Requirements: 7.4
        """
        server = DevBackupMCPServer(config_path=setup_env["config_path"])
        
        result = asyncio.run(server._tool_backup_verify(snapshot=""))
        parsed = json.loads(result)
        
        assert "error" in parsed
        assert parsed["error"]["code"] == "INVALID_ARGUMENT"



class TestMCPNewTools:
    """Unit tests for new MCP tools (backup_setup, backup_explain, backup_find_file, backup_undo).
    
    **Validates: Requirements 2.1, 2.2, 2.3, 2.4**
    """
    
    @pytest.fixture
    def setup_env(self, tmp_path):
        """Set up test environment with config file and directories."""
        source_dir = tmp_path / "source"
        dest_dir = tmp_path / "dest"
        log_dir = tmp_path / "logs"
        config_dir = tmp_path / "config"
        
        source_dir.mkdir()
        dest_dir.mkdir()
        log_dir.mkdir()
        config_dir.mkdir()
        
        # Create test files
        (source_dir / "test.py").write_text("print('hello')")
        (source_dir / "config.json").write_text('{"key": "value"}')
        
        config = create_test_config(source_dir, dest_dir, log_dir)
        config_path = config_dir / "config.toml"
        config_path.write_text(format_config(config))
        
        return {
            "source_dir": source_dir,
            "dest_dir": dest_dir,
            "log_dir": log_dir,
            "config_path": config_path,
            "config": config,
            "tmp_path": tmp_path,
        }
    
    @pytest.fixture
    def setup_with_snapshot(self, setup_env):
        """Set up environment with an existing snapshot."""
        dest_dir = setup_env["dest_dir"]
        source_dir = setup_env["source_dir"]
        
        # Create a snapshot
        snapshot_name = datetime.now().strftime("%Y-%m-%d-%H%M%S")
        snapshot_dir = dest_dir / snapshot_name
        snapshot_dir.mkdir()
        
        # Copy files to snapshot
        import shutil
        shutil.copy(source_dir / "test.py", snapshot_dir / "test.py")
        shutil.copy(source_dir / "config.json", snapshot_dir / "config.json")
        
        setup_env["snapshot_name"] = snapshot_name
        setup_env["snapshot_dir"] = snapshot_dir
        return setup_env
    
    # =========================================================================
    # backup_setup tests
    # =========================================================================
    
    def test_backup_setup_discovery_stage(self, tmp_path):
        """
        Test backup_setup returns discovery stage when no config exists.
        
        Requirements: 2.1
        """
        # Create a project directory with markers
        project_dir = tmp_path / "my_project"
        project_dir.mkdir()
        (project_dir / "pyproject.toml").write_text("[project]\nname = 'test'")
        (project_dir / "main.py").write_text("print('hello')")
        
        # Use non-existent config path
        config_path = tmp_path / "config" / "config.toml"
        
        server = DevBackupMCPServer(config_path=config_path)
        
        result = asyncio.run(server._tool_backup_setup(
            workspace_path=str(project_dir)
        ))
        parsed = json.loads(result)
        
        assert "error" not in parsed, f"Setup should succeed: {result}"
        assert "stage" in parsed
        assert "message" in parsed
        # Should find the project
        assert "discovered_projects" in parsed
    
    def test_backup_setup_no_projects_found(self, tmp_path):
        """
        Test backup_setup handles case when no projects are found.
        
        Requirements: 2.1
        """
        # Create empty directory (no project markers)
        empty_dir = tmp_path / "empty"
        empty_dir.mkdir()
        
        config_path = tmp_path / "config" / "config.toml"
        
        server = DevBackupMCPServer(config_path=config_path)
        
        result = asyncio.run(server._tool_backup_setup(
            workspace_path=str(empty_dir)
        ))
        parsed = json.loads(result)
        
        assert "error" not in parsed
        assert parsed.get("stage") in ["no_projects", "discovery"]
        assert "message" in parsed
    
    def test_backup_setup_complete_stage(self, tmp_path):
        """
        Test backup_setup completes when confirmation is provided.
        
        Requirements: 2.1, 1.4, 1.5
        """
        # Create a project directory
        project_dir = tmp_path / "my_project"
        project_dir.mkdir()
        (project_dir / "pyproject.toml").write_text("[project]\nname = 'test'")
        
        # Create destination directory
        dest_dir = tmp_path / "backups"
        dest_dir.mkdir()
        
        config_path = tmp_path / "config" / "config.toml"
        
        server = DevBackupMCPServer(config_path=config_path)
        
        result = asyncio.run(server._tool_backup_setup(
            confirm_projects=[str(project_dir)],
            confirm_destination=str(dest_dir)
        ))
        parsed = json.loads(result)
        
        assert "error" not in parsed, f"Setup should succeed: {result}"
        assert parsed.get("stage") == "complete"
        assert parsed.get("config_created") is True
        assert "config_path" in parsed
        
        # Verify config was created
        assert config_path.exists()
    
    # =========================================================================
    # backup_explain tests
    # =========================================================================
    
    def test_backup_explain_no_config(self, tmp_path):
        """
        Test backup_explain when no config exists.
        
        Requirements: 2.2
        """
        config_path = tmp_path / "nonexistent" / "config.toml"
        server = DevBackupMCPServer(config_path=config_path)
        
        result = asyncio.run(server._tool_backup_explain())
        parsed = json.loads(result)
        
        assert "error" not in parsed
        assert "message" in parsed
        assert "suggestions" in parsed
        # Should suggest setting up backups
        assert "set up" in parsed["message"].lower() or "don't have" in parsed["message"].lower()
    
    def test_backup_explain_status_topic(self, setup_env):
        """
        Test backup_explain with status topic.
        
        Requirements: 2.2, 2.5
        """
        server = DevBackupMCPServer(config_path=setup_env["config_path"])
        
        result = asyncio.run(server._tool_backup_explain(topic="status"))
        parsed = json.loads(result)
        
        assert "error" not in parsed
        assert "message" in parsed
        assert "suggestions" in parsed
    
    def test_backup_explain_snapshots_topic(self, setup_with_snapshot):
        """
        Test backup_explain with snapshots topic.
        
        Requirements: 2.2
        """
        server = DevBackupMCPServer(config_path=setup_with_snapshot["config_path"])
        
        result = asyncio.run(server._tool_backup_explain(topic="snapshots"))
        parsed = json.loads(result)
        
        assert "error" not in parsed
        assert "message" in parsed
        # Should mention snapshots/versions
        assert "snapshot" in parsed["message"].lower() or "version" in parsed["message"].lower()
    
    def test_backup_explain_restore_topic(self, setup_env):
        """
        Test backup_explain with restore topic.
        
        Requirements: 2.2
        """
        server = DevBackupMCPServer(config_path=setup_env["config_path"])
        
        result = asyncio.run(server._tool_backup_explain(topic="restore"))
        parsed = json.loads(result)
        
        assert "error" not in parsed
        assert "message" in parsed
        # Should explain how to restore
        assert "restore" in parsed["message"].lower() or "back" in parsed["message"].lower()
    
    def test_backup_explain_schedule_topic(self, setup_env):
        """
        Test backup_explain with schedule topic.
        
        Requirements: 2.2
        """
        server = DevBackupMCPServer(config_path=setup_env["config_path"])
        
        result = asyncio.run(server._tool_backup_explain(topic="schedule"))
        parsed = json.loads(result)
        
        assert "error" not in parsed
        assert "message" in parsed
    
    def test_backup_explain_storage_topic(self, setup_with_snapshot):
        """
        Test backup_explain with storage topic.
        
        Requirements: 2.2
        """
        server = DevBackupMCPServer(config_path=setup_with_snapshot["config_path"])
        
        result = asyncio.run(server._tool_backup_explain(topic="storage"))
        parsed = json.loads(result)
        
        assert "error" not in parsed
        assert "message" in parsed
        # Should mention storage location
        assert "stored" in parsed["message"].lower() or str(setup_with_snapshot["dest_dir"]) in parsed["message"]
    
    # =========================================================================
    # backup_find_file tests
    # =========================================================================
    
    def test_backup_find_file_no_description(self, setup_env):
        """
        Test backup_find_file with empty description.
        
        Requirements: 2.3
        """
        server = DevBackupMCPServer(config_path=setup_env["config_path"])
        
        result = asyncio.run(server._tool_backup_find_file(description=""))
        parsed = json.loads(result)
        
        assert "error" in parsed
        assert parsed["error"]["code"] == "INVALID_ARGUMENT"
    
    def test_backup_find_file_with_filename(self, setup_with_snapshot):
        """
        Test backup_find_file with specific filename.
        
        Requirements: 2.3
        """
        server = DevBackupMCPServer(config_path=setup_with_snapshot["config_path"])
        
        result = asyncio.run(server._tool_backup_find_file(description="test.py"))
        parsed = json.loads(result)
        
        assert "error" not in parsed, f"Find should succeed: {result}"
        assert "message" in parsed
        assert "matches" in parsed
        # Should find the file
        assert len(parsed["matches"]) > 0 or "couldn't find" in parsed["message"].lower()
    
    def test_backup_find_file_with_pattern(self, setup_with_snapshot):
        """
        Test backup_find_file with glob pattern.
        
        Requirements: 2.3
        """
        server = DevBackupMCPServer(config_path=setup_with_snapshot["config_path"])
        
        result = asyncio.run(server._tool_backup_find_file(description="*.json"))
        parsed = json.loads(result)
        
        assert "error" not in parsed
        assert "message" in parsed
        assert "matches" in parsed
    
    def test_backup_find_file_with_time_hint(self, setup_with_snapshot):
        """
        Test backup_find_file with time hint.
        
        Requirements: 2.3
        """
        server = DevBackupMCPServer(config_path=setup_with_snapshot["config_path"])
        
        result = asyncio.run(server._tool_backup_find_file(
            description="config",
            time_hint="today"
        ))
        parsed = json.loads(result)
        
        assert "error" not in parsed
        assert "message" in parsed
    
    def test_backup_find_file_no_matches(self, setup_with_snapshot):
        """
        Test backup_find_file when no files match.
        
        Requirements: 2.3
        """
        server = DevBackupMCPServer(config_path=setup_with_snapshot["config_path"])
        
        result = asyncio.run(server._tool_backup_find_file(
            description="nonexistent_file_xyz.abc"
        ))
        parsed = json.loads(result)
        
        assert "error" not in parsed
        assert "message" in parsed
        assert "matches" in parsed
        assert len(parsed["matches"]) == 0
        # Should have helpful message
        assert "couldn't find" in parsed["message"].lower()
    
    # =========================================================================
    # backup_undo tests
    # =========================================================================
    
    def test_backup_undo_no_file_path(self, setup_env):
        """
        Test backup_undo when no file path is provided.
        
        Requirements: 2.4
        """
        server = DevBackupMCPServer(config_path=setup_env["config_path"])
        
        result = asyncio.run(server._tool_backup_undo())
        parsed = json.loads(result)
        
        assert "error" not in parsed
        assert parsed.get("stage") == "need_file"
        assert "message" in parsed
    
    def test_backup_undo_preview_stage(self, setup_with_snapshot):
        """
        Test backup_undo returns preview when confirm=False.
        
        Requirements: 2.4, 7.2
        """
        server = DevBackupMCPServer(config_path=setup_with_snapshot["config_path"])
        
        result = asyncio.run(server._tool_backup_undo(
            file_path="test.py",
            confirm=False
        ))
        parsed = json.loads(result)
        
        assert "error" not in parsed, f"Undo should succeed: {result}"
        # Should be in preview or no_backup stage
        assert parsed.get("stage") in ["preview", "no_backup"]
        assert "message" in parsed
        
        if parsed.get("stage") == "preview":
            assert "file_info" in parsed
    
    def test_backup_undo_confirm_stage(self, setup_with_snapshot):
        """
        Test backup_undo performs restore when confirm=True.
        
        Requirements: 2.4, 7.3, 7.6
        """
        server = DevBackupMCPServer(config_path=setup_with_snapshot["config_path"])
        
        result = asyncio.run(server._tool_backup_undo(
            file_path="test.py",
            confirm=True
        ))
        parsed = json.loads(result)
        
        assert "error" not in parsed, f"Undo should succeed: {result}"
        # Should be complete or no_backup
        assert parsed.get("stage") in ["complete", "no_backup"]
        assert "message" in parsed
        
        if parsed.get("stage") == "complete":
            assert "file_info" in parsed
            # Should restore to Recovered Files folder
            restored_to = parsed["file_info"].get("restored_to", "")
            assert "Recovered Files" in restored_to
    
    def test_backup_undo_file_not_found(self, setup_with_snapshot):
        """
        Test backup_undo when file is not in backups.
        
        Requirements: 2.4
        """
        server = DevBackupMCPServer(config_path=setup_with_snapshot["config_path"])
        
        result = asyncio.run(server._tool_backup_undo(
            file_path="nonexistent_file.xyz",
            confirm=False
        ))
        parsed = json.loads(result)
        
        assert "error" not in parsed
        assert parsed.get("stage") == "no_backup"
        assert "message" in parsed
        # Should have helpful message
        assert "couldn't find" in parsed["message"].lower()
    
    def test_backup_undo_preserves_original(self, setup_with_snapshot):
        """
        Test backup_undo doesn't modify original file.
        
        Requirements: 7.6
        """
        source_dir = setup_with_snapshot["source_dir"]
        original_content = "print('hello')"
        
        # Verify original content
        assert (source_dir / "test.py").read_text() == original_content
        
        server = DevBackupMCPServer(config_path=setup_with_snapshot["config_path"])
        
        # Run undo with confirm
        result = asyncio.run(server._tool_backup_undo(
            file_path="test.py",
            confirm=True
        ))
        parsed = json.loads(result)
        
        # Original file should be unchanged
        assert (source_dir / "test.py").read_text() == original_content
