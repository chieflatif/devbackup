"""Integration tests for devbackup.

This module contains end-to-end integration tests that verify:
- Full backup workflow (first backup, incremental, modifications)
- Concurrent backup prevention
- Error scenarios (disk full, destination missing, source missing)
- CLI commands end-to-end
- MCP tools end-to-end

**Validates: Requirements 2.3, 4.1-4.10, 8.1-8.3, 9.1-9.10, 10.1-10.10**
"""

import asyncio
import json
import multiprocessing
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Dict, Any

import pytest

from devbackup.backup import run_backup, BackupResult, EXIT_SUCCESS, EXIT_LOCK_ERROR
from devbackup.cli import main as cli_main
from devbackup.config import (
    Configuration,
    SchedulerConfig,
    RetentionConfig,
    LoggingConfig,
    MCPConfig,
    format_config,
)
from devbackup.lock import LockManager
from devbackup.mcp_server import DevBackupMCPServer
from devbackup.snapshot import SnapshotEngine


def create_test_config(
    source_dir: Path,
    dest_dir: Path,
    log_dir: Path,
    exclude_patterns: list = None
) -> Configuration:
    """Create a test configuration."""
    return Configuration(
        backup_destination=dest_dir,
        source_directories=[source_dir],
        exclude_patterns=exclude_patterns or ["*.pyc", "__pycache__/"],
        scheduler=SchedulerConfig(type="launchd", interval_seconds=3600),
        retention=RetentionConfig(hourly=24, daily=7, weekly=4),
        logging=LoggingConfig(
            level="DEBUG",
            log_file=log_dir / "devbackup.log",
            error_log_file=log_dir / "devbackup.err",
        ),
        mcp=MCPConfig(enabled=True, port=0),
    )


def write_config_file(config: Configuration, config_path: Path) -> None:
    """Write configuration to a TOML file."""
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(format_config(config))


# =============================================================================
# Task 16.1: Integration tests for full backup workflow
# =============================================================================

class TestFullBackupWorkflow:
    """
    Integration tests for the complete backup workflow.
    
    Tests first backup (full copy), incremental backup (hard links),
    backup with modifications, and concurrent backup prevention.
    
    **Validates: Requirements 4.1-4.10, 2.3**
    """
    
    @pytest.fixture
    def backup_env(self, tmp_path):
        """Set up a complete backup environment."""
        source_dir = tmp_path / "source"
        dest_dir = tmp_path / "dest"
        log_dir = tmp_path / "logs"
        config_dir = tmp_path / "config"
        
        source_dir.mkdir()
        dest_dir.mkdir()
        log_dir.mkdir()
        config_dir.mkdir()
        
        # Create test files with various content
        (source_dir / "file1.txt").write_text("content of file 1")
        (source_dir / "file2.txt").write_text("content of file 2")
        (source_dir / "subdir").mkdir()
        (source_dir / "subdir" / "nested.txt").write_text("nested content")
        (source_dir / "subdir" / "deep").mkdir()
        (source_dir / "subdir" / "deep" / "file.txt").write_text("deep content")
        
        config = create_test_config(source_dir, dest_dir, log_dir)
        config_path = config_dir / "config.toml"
        write_config_file(config, config_path)
        
        return {
            "source_dir": source_dir,
            "dest_dir": dest_dir,
            "log_dir": log_dir,
            "config_path": config_path,
            "config": config,
        }
    
    def test_first_backup_creates_full_copy(self, backup_env):
        """
        Test that the first backup creates a complete copy of all files.
        
        **Validates: Requirements 4.1, 4.3, 4.5, 4.9**
        """
        result = run_backup(config=backup_env["config"])
        
        assert result.success is True
        assert result.exit_code == EXIT_SUCCESS
        assert result.snapshot_result is not None
        assert result.snapshot_result.snapshot_path is not None
        
        snapshot_path = result.snapshot_result.snapshot_path
        
        # Verify all files were copied
        assert (snapshot_path / "file1.txt").exists()
        assert (snapshot_path / "file2.txt").exists()
        assert (snapshot_path / "subdir" / "nested.txt").exists()
        assert (snapshot_path / "subdir" / "deep" / "file.txt").exists()
        
        # Verify content is correct
        assert (snapshot_path / "file1.txt").read_text() == "content of file 1"
        assert (snapshot_path / "file2.txt").read_text() == "content of file 2"
        assert (snapshot_path / "subdir" / "nested.txt").read_text() == "nested content"
    
    def test_incremental_backup_uses_hard_links(self, backup_env):
        """
        Test that incremental backups use hard links for unchanged files.
        
        **Validates: Requirements 4.2, 4.7**
        """
        # First backup
        result1 = run_backup(config=backup_env["config"])
        assert result1.success is True
        snapshot1_path = result1.snapshot_result.snapshot_path
        
        # Wait to ensure different timestamp
        time.sleep(1.1)
        
        # Second backup without changes
        result2 = run_backup(config=backup_env["config"])
        assert result2.success is True
        snapshot2_path = result2.snapshot_result.snapshot_path
        
        # Verify both snapshots exist
        assert snapshot1_path.exists()
        assert snapshot2_path.exists()
        assert snapshot1_path != snapshot2_path
        
        # Verify unchanged files have the same inode (hard links)
        file1_snap1 = snapshot1_path / "file1.txt"
        file1_snap2 = snapshot2_path / "file1.txt"
        
        assert file1_snap1.stat().st_ino == file1_snap2.stat().st_ino
        
        # Verify nested files also use hard links
        nested_snap1 = snapshot1_path / "subdir" / "nested.txt"
        nested_snap2 = snapshot2_path / "subdir" / "nested.txt"
        
        assert nested_snap1.stat().st_ino == nested_snap2.stat().st_ino
    
    def test_backup_with_modified_files(self, backup_env):
        """
        Test that modified files are copied (not hard-linked).
        
        **Validates: Requirements 4.8**
        """
        # First backup
        result1 = run_backup(config=backup_env["config"])
        assert result1.success is True
        snapshot1_path = result1.snapshot_result.snapshot_path
        
        # Modify a file
        (backup_env["source_dir"] / "file1.txt").write_text("modified content")
        
        # Wait to ensure different timestamp
        time.sleep(1.1)
        
        # Second backup
        result2 = run_backup(config=backup_env["config"])
        assert result2.success is True
        snapshot2_path = result2.snapshot_result.snapshot_path
        
        # Verify modified file has different inode (new copy)
        file1_snap1 = snapshot1_path / "file1.txt"
        file1_snap2 = snapshot2_path / "file1.txt"
        
        assert file1_snap1.stat().st_ino != file1_snap2.stat().st_ino
        
        # Verify content is different
        assert file1_snap1.read_text() == "content of file 1"
        assert file1_snap2.read_text() == "modified content"
        
        # Verify unchanged files still use hard links
        file2_snap1 = snapshot1_path / "file2.txt"
        file2_snap2 = snapshot2_path / "file2.txt"
        
        assert file2_snap1.stat().st_ino == file2_snap2.stat().st_ino
    
    def test_backup_with_added_files(self, backup_env):
        """
        Test that newly added files are included in the snapshot.
        
        **Validates: Requirements 4.9**
        """
        # First backup
        result1 = run_backup(config=backup_env["config"])
        assert result1.success is True
        snapshot1_path = result1.snapshot_result.snapshot_path
        
        # Add new files
        (backup_env["source_dir"] / "new_file.txt").write_text("new content")
        (backup_env["source_dir"] / "new_dir").mkdir()
        (backup_env["source_dir"] / "new_dir" / "file.txt").write_text("new dir content")
        
        # Wait to ensure different timestamp
        time.sleep(1.1)
        
        # Second backup
        result2 = run_backup(config=backup_env["config"])
        assert result2.success is True
        snapshot2_path = result2.snapshot_result.snapshot_path
        
        # Verify new files exist in second snapshot
        assert (snapshot2_path / "new_file.txt").exists()
        assert (snapshot2_path / "new_dir" / "file.txt").exists()
        
        # Verify new files don't exist in first snapshot
        assert not (snapshot1_path / "new_file.txt").exists()
        assert not (snapshot1_path / "new_dir").exists()
    
    def test_backup_with_deleted_files(self, backup_env):
        """
        Test that deleted files are excluded from new snapshots.
        
        **Validates: Requirements 4.10**
        """
        # First backup
        result1 = run_backup(config=backup_env["config"])
        assert result1.success is True
        snapshot1_path = result1.snapshot_result.snapshot_path
        
        # Delete a file
        (backup_env["source_dir"] / "file1.txt").unlink()
        
        # Wait to ensure different timestamp
        time.sleep(1.1)
        
        # Second backup
        result2 = run_backup(config=backup_env["config"])
        assert result2.success is True
        snapshot2_path = result2.snapshot_result.snapshot_path
        
        # Verify deleted file exists in first snapshot but not second
        assert (snapshot1_path / "file1.txt").exists()
        assert not (snapshot2_path / "file1.txt").exists()
        
        # Verify other files still exist
        assert (snapshot2_path / "file2.txt").exists()
    
    def test_concurrent_backup_prevention(self, backup_env):
        """
        Test that concurrent backup attempts are prevented.
        
        **Validates: Requirements 2.3**
        """
        # Acquire lock manually
        lock_manager = LockManager()
        lock_manager.acquire()
        
        try:
            # Attempt backup while lock is held
            result = run_backup(config=backup_env["config"])
            
            assert result.success is False
            assert result.exit_code == EXIT_LOCK_ERROR
            assert "lock" in result.error_message.lower()
        finally:
            lock_manager.release()
    
    def test_in_progress_cleanup(self, backup_env):
        """
        Test that incomplete snapshots from previous runs are cleaned up.
        
        **Validates: Requirements 4.6, 8.7**
        """
        # Create an incomplete snapshot directory
        incomplete_dir = backup_env["dest_dir"] / "in_progress_2025-01-01-120000"
        incomplete_dir.mkdir()
        (incomplete_dir / "partial_file.txt").write_text("incomplete")
        
        # Run backup
        result = run_backup(config=backup_env["config"])
        
        assert result.success is True
        assert result.incomplete_cleaned == 1
        
        # Verify incomplete directory was removed
        assert not incomplete_dir.exists()
        
        # Verify new snapshot was created
        assert result.snapshot_result.snapshot_path.exists()
    
    def test_atomic_snapshot_rename(self, backup_env):
        """
        Test that snapshot is atomically renamed on success.
        
        **Validates: Requirements 4.5**
        """
        result = run_backup(config=backup_env["config"])
        
        assert result.success is True
        
        # Verify no in_progress directories exist
        in_progress_dirs = list(backup_env["dest_dir"].glob("in_progress_*"))
        assert len(in_progress_dirs) == 0
        
        # Verify snapshot has proper timestamp format
        snapshot_name = result.snapshot_result.snapshot_path.name
        assert len(snapshot_name) == 17  # YYYY-MM-DD-HHMMSS
        assert snapshot_name[4] == "-"
        assert snapshot_name[7] == "-"
        assert snapshot_name[10] == "-"



# =============================================================================
# Task 16.2: Integration tests for error scenarios
# =============================================================================

class TestErrorScenarios:
    """
    Integration tests for error handling scenarios.
    
    Tests disk full handling, destination missing, and source directory missing.
    
    **Validates: Requirements 8.1, 8.2, 8.3**
    """
    
    @pytest.fixture
    def error_env(self, tmp_path):
        """Set up environment for error testing."""
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
        write_config_file(config, config_path)
        
        return {
            "source_dir": source_dir,
            "dest_dir": dest_dir,
            "log_dir": log_dir,
            "config_path": config_path,
            "config": config,
        }
    
    def test_destination_missing_error(self, error_env):
        """
        Test error handling when destination directory is missing.
        
        **Validates: Requirements 8.1, 3.3**
        """
        # Remove destination directory
        shutil.rmtree(error_env["dest_dir"])
        
        result = run_backup(config=error_env["config"])
        
        assert result.success is False
        assert "not found" in result.error_message.lower() or "destination" in result.error_message.lower()
        
        # Verify lock was released
        lock_manager = LockManager()
        assert not lock_manager.is_locked()
    
    def test_source_directory_missing_warning(self, error_env):
        """
        Test that missing source directory logs warning but continues.
        
        **Validates: Requirements 8.2**
        """
        # Add a non-existent source directory
        error_env["config"].source_directories.append(Path("/nonexistent/source"))
        
        result = run_backup(config=error_env["config"])
        
        # Should still succeed with the valid source
        assert result.success is True
        assert result.exit_code == EXIT_SUCCESS
        
        # Verify snapshot was created
        assert result.snapshot_result.snapshot_path.exists()
    
    def test_all_sources_invalid_error(self, error_env):
        """
        Test error when all source directories are invalid.
        
        **Validates: Requirements 8.3**
        """
        # Replace all sources with invalid paths
        error_env["config"].source_directories = [
            Path("/nonexistent/source1"),
            Path("/nonexistent/source2"),
        ]
        
        result = run_backup(config=error_env["config"])
        
        assert result.success is False
        assert "invalid" in result.error_message.lower() or "source" in result.error_message.lower()
        
        # Verify lock was released
        lock_manager = LockManager()
        assert not lock_manager.is_locked()
    
    def test_destination_not_writable(self, error_env):
        """
        Test error handling when destination is not writable.
        
        **Validates: Requirements 3.4**
        """
        # Make destination read-only (skip on Windows)
        if sys.platform == "win32":
            pytest.skip("Permission test not reliable on Windows")
        
        # Create a read-only destination
        readonly_dest = error_env["dest_dir"] / "readonly"
        readonly_dest.mkdir()
        os.chmod(readonly_dest, 0o444)
        
        try:
            error_env["config"].backup_destination = readonly_dest
            
            result = run_backup(config=error_env["config"])
            
            assert result.success is False
            assert "writable" in result.error_message.lower() or "permission" in result.error_message.lower()
        finally:
            # Restore permissions for cleanup
            os.chmod(readonly_dest, 0o755)
    
    def test_lock_released_on_error(self, error_env):
        """
        Test that lock is always released even when errors occur.
        
        **Validates: Requirements 8.5, Property 7**
        """
        # Remove destination to cause an error
        shutil.rmtree(error_env["dest_dir"])
        
        result = run_backup(config=error_env["config"])
        
        assert result.success is False
        
        # Verify lock was released
        lock_manager = LockManager()
        assert not lock_manager.is_locked()
        
        # Verify we can acquire the lock
        lock_manager.acquire()
        lock_manager.release()
    
    def test_incomplete_snapshot_cleanup_on_error(self, error_env):
        """
        Test that incomplete snapshots are cleaned up on error.
        
        **Validates: Requirements 8.6, Property 8**
        """
        # First, run a successful backup
        result1 = run_backup(config=error_env["config"])
        assert result1.success is True
        
        # Now remove the source to cause an error on next backup
        shutil.rmtree(error_env["source_dir"])
        error_env["config"].source_directories = [error_env["source_dir"]]
        
        result2 = run_backup(config=error_env["config"])
        
        assert result2.success is False
        
        # Verify no in_progress directories remain
        in_progress_dirs = list(error_env["dest_dir"].glob("in_progress_*"))
        assert len(in_progress_dirs) == 0



# =============================================================================
# Task 16.3: Integration tests for CLI and MCP
# =============================================================================

class TestCLIIntegration:
    """
    End-to-end integration tests for CLI commands.
    
    Tests CLI commands with real file operations.
    
    **Validates: Requirements 9.1-9.10**
    """
    
    @pytest.fixture
    def cli_env(self, tmp_path):
        """Set up environment for CLI testing."""
        source_dir = tmp_path / "source"
        dest_dir = tmp_path / "dest"
        log_dir = tmp_path / "logs"
        config_dir = tmp_path / "config"
        
        source_dir.mkdir()
        dest_dir.mkdir()
        log_dir.mkdir()
        config_dir.mkdir()
        
        # Create test files
        (source_dir / "file1.txt").write_text("content 1")
        (source_dir / "file2.py").write_text("print('hello')")
        (source_dir / "subdir").mkdir()
        (source_dir / "subdir" / "nested.txt").write_text("nested")
        
        config = create_test_config(source_dir, dest_dir, log_dir)
        config_path = config_dir / "config.toml"
        write_config_file(config, config_path)
        
        return {
            "source_dir": source_dir,
            "dest_dir": dest_dir,
            "log_dir": log_dir,
            "config_path": config_path,
            "config": config,
        }
    
    def test_cli_run_creates_backup(self, cli_env):
        """
        Test 'devbackup run' creates a backup end-to-end.
        
        **Validates: Requirements 9.1**
        """
        exit_code = cli_main(["--config", str(cli_env["config_path"]), "run"])
        
        assert exit_code == 0
        
        # Verify snapshot was created
        snapshots = [d for d in cli_env["dest_dir"].iterdir() if d.is_dir()]
        assert len(snapshots) == 1
        
        # Verify files exist in snapshot
        snapshot = snapshots[0]
        assert (snapshot / "file1.txt").exists()
        assert (snapshot / "file2.py").exists()
    
    def test_cli_status_shows_info(self, cli_env, capsys):
        """
        Test 'devbackup status' shows backup status.
        
        **Validates: Requirements 9.2**
        """
        # Run a backup first
        cli_main(["--config", str(cli_env["config_path"]), "run"])
        
        # Check status
        exit_code = cli_main(["--config", str(cli_env["config_path"]), "status"])
        
        assert exit_code == 0
        
        captured = capsys.readouterr()
        assert "Status" in captured.out or "status" in captured.out.lower()
    
    def test_cli_list_shows_snapshots(self, cli_env, capsys):
        """
        Test 'devbackup list' shows all snapshots.
        
        **Validates: Requirements 9.3**
        """
        # Run a backup
        cli_main(["--config", str(cli_env["config_path"]), "run"])
        
        # List snapshots
        exit_code = cli_main(["--config", str(cli_env["config_path"]), "list"])
        
        assert exit_code == 0
        
        captured = capsys.readouterr()
        # Should show timestamp in output
        assert "20" in captured.out  # Year prefix
    
    def test_cli_list_json_output(self, cli_env):
        """
        Test 'devbackup list --json' outputs valid JSON.
        
        **Validates: Requirements 9.3**
        """
        import io
        from unittest.mock import patch
        
        # Run a backup
        cli_main(["--config", str(cli_env["config_path"]), "run"])
        
        # List snapshots as JSON with captured stdout
        captured_stdout = io.StringIO()
        with patch('sys.stdout', captured_stdout):
            exit_code = cli_main(["--config", str(cli_env["config_path"]), "list", "--json"])
        
        assert exit_code == 0
        
        output = captured_stdout.getvalue()
        data = json.loads(output)
        assert isinstance(data, list)
        assert len(data) == 1
        assert "timestamp" in data[0]
    
    def test_cli_restore_file(self, cli_env):
        """
        Test 'devbackup restore' restores a file.
        
        **Validates: Requirements 9.4**
        """
        # Run a backup
        cli_main(["--config", str(cli_env["config_path"]), "run"])
        
        # Get snapshot name
        snapshots = [d for d in cli_env["dest_dir"].iterdir() if d.is_dir()]
        snapshot_name = snapshots[0].name
        
        # Delete original file
        (cli_env["source_dir"] / "file1.txt").unlink()
        
        # Restore to new location
        restore_path = cli_env["source_dir"] / "restored.txt"
        exit_code = cli_main([
            "--config", str(cli_env["config_path"]),
            "restore", snapshot_name, "file1.txt",
            "--to", str(restore_path)
        ])
        
        assert exit_code == 0
        assert restore_path.exists()
        assert restore_path.read_text() == "content 1"
    
    def test_cli_diff_shows_changes(self, cli_env, capsys):
        """
        Test 'devbackup diff' shows file changes.
        
        **Validates: Requirements 9.3 (diff functionality)**
        """
        # Run a backup
        cli_main(["--config", str(cli_env["config_path"]), "run"])
        
        # Get snapshot name
        snapshots = [d for d in cli_env["dest_dir"].iterdir() if d.is_dir()]
        snapshot_name = snapshots[0].name
        
        # Modify a file
        (cli_env["source_dir"] / "file1.txt").write_text("modified content")
        
        # Add a new file
        (cli_env["source_dir"] / "new.txt").write_text("new")
        
        # Show diff
        exit_code = cli_main([
            "--config", str(cli_env["config_path"]),
            "diff", snapshot_name
        ])
        
        assert exit_code == 0
        
        captured = capsys.readouterr()
        # Should show added or modified
        assert "Added" in captured.out or "Modified" in captured.out or "modified" in captured.out.lower()
    
    def test_cli_search_finds_files(self, cli_env, capsys):
        """
        Test 'devbackup search' finds files by pattern.
        
        **Validates: Requirements 9.3 (search functionality)**
        """
        # Run a backup
        cli_main(["--config", str(cli_env["config_path"]), "run"])
        
        # Search for Python files
        exit_code = cli_main([
            "--config", str(cli_env["config_path"]),
            "search", "*.py"
        ])
        
        assert exit_code == 0
        
        captured = capsys.readouterr()
        assert "file2.py" in captured.out
    
    def test_cli_init_creates_config(self, cli_env):
        """
        Test 'devbackup init' creates default config.
        
        **Validates: Requirements 9.7**
        """
        new_config_path = cli_env["config_path"].parent / "new_config.toml"
        
        exit_code = cli_main(["--config", str(new_config_path), "init"])
        
        assert exit_code == 0
        assert new_config_path.exists()
        
        content = new_config_path.read_text()
        assert "backup_destination" in content
        assert "source_directories" in content
    
    def test_cli_error_exit_codes(self, cli_env, capsys):
        """
        Test that CLI returns non-zero exit codes on errors.
        
        **Validates: Requirements 9.8**
        """
        # Test with non-existent config
        exit_code = cli_main(["--config", "/nonexistent/config.toml", "run"])
        
        assert exit_code != 0
        
        captured = capsys.readouterr()
        assert len(captured.err) > 0 or "error" in captured.out.lower()
    
    def test_cli_verbose_output(self, cli_env, capsys):
        """
        Test that --verbose flag increases output detail.
        
        **Validates: Requirements 9.10**
        """
        exit_code = cli_main([
            "--config", str(cli_env["config_path"]),
            "--verbose",
            "run"
        ])
        
        assert exit_code == 0
        
        captured = capsys.readouterr()
        # Verbose output should include more details
        assert "Backup completed" in captured.out or "Starting" in captured.out


class TestMCPIntegration:
    """
    End-to-end integration tests for MCP server tools.
    
    Tests MCP tools with real file operations.
    
    **Validates: Requirements 10.1-10.10**
    """
    
    @pytest.fixture
    def mcp_env(self, tmp_path):
        """Set up environment for MCP testing."""
        source_dir = tmp_path / "source"
        dest_dir = tmp_path / "dest"
        log_dir = tmp_path / "logs"
        config_dir = tmp_path / "config"
        
        source_dir.mkdir()
        dest_dir.mkdir()
        log_dir.mkdir()
        config_dir.mkdir()
        
        # Create test files
        (source_dir / "app.py").write_text("print('app')")
        (source_dir / "config.json").write_text('{"key": "value"}')
        (source_dir / "data").mkdir()
        (source_dir / "data" / "file.txt").write_text("data content")
        
        config = create_test_config(source_dir, dest_dir, log_dir)
        config_path = config_dir / "config.toml"
        write_config_file(config, config_path)
        
        return {
            "source_dir": source_dir,
            "dest_dir": dest_dir,
            "log_dir": log_dir,
            "config_path": config_path,
            "config": config,
        }
    
    def test_mcp_backup_run_end_to_end(self, mcp_env):
        """
        Test backup_run MCP tool creates a backup.
        
        **Validates: Requirements 10.1**
        """
        server = DevBackupMCPServer(config_path=mcp_env["config_path"])
        
        result = asyncio.run(server._tool_backup_run())
        parsed = json.loads(result)
        
        assert "error" not in parsed
        assert parsed["success"] is True
        assert parsed["snapshot"] is not None
        
        # Verify snapshot was created
        snapshot_path = mcp_env["dest_dir"] / parsed["snapshot"]
        assert snapshot_path.exists()
        assert (snapshot_path / "app.py").exists()
    
    def test_mcp_backup_status_end_to_end(self, mcp_env):
        """
        Test backup_status MCP tool returns correct status.
        
        **Validates: Requirements 10.2**
        """
        server = DevBackupMCPServer(config_path=mcp_env["config_path"])
        
        # Run a backup first
        asyncio.run(server._tool_backup_run())
        
        # Check status
        result = asyncio.run(server._tool_backup_status())
        parsed = json.loads(result)
        
        assert "error" not in parsed
        assert parsed["last_backup"] is not None
        assert parsed["total_snapshots"] == 1
        assert parsed["is_running"] is False
    
    def test_mcp_backup_list_snapshots_end_to_end(self, mcp_env):
        """
        Test backup_list_snapshots MCP tool lists all snapshots.
        
        **Validates: Requirements 10.3**
        """
        server = DevBackupMCPServer(config_path=mcp_env["config_path"])
        
        # Create multiple backups
        asyncio.run(server._tool_backup_run())
        time.sleep(1.1)
        asyncio.run(server._tool_backup_run())
        
        # List snapshots
        result = asyncio.run(server._tool_backup_list_snapshots())
        parsed = json.loads(result)
        
        assert "error" not in parsed
        assert len(parsed["snapshots"]) == 2
        
        # Verify snapshot info
        for snap in parsed["snapshots"]:
            assert "timestamp" in snap
            assert "name" in snap
            assert "size_bytes" in snap
            assert "file_count" in snap
    
    def test_mcp_backup_restore_end_to_end(self, mcp_env):
        """
        Test backup_restore MCP tool restores files correctly.
        
        **Validates: Requirements 10.4**
        """
        server = DevBackupMCPServer(config_path=mcp_env["config_path"])
        
        # Create a backup
        run_result = asyncio.run(server._tool_backup_run())
        run_parsed = json.loads(run_result)
        snapshot_name = run_parsed["snapshot"]
        
        # Delete original file
        (mcp_env["source_dir"] / "app.py").unlink()
        
        # Restore to new location
        restore_dest = mcp_env["source_dir"] / "restored_app.py"
        result = asyncio.run(server._tool_backup_restore(
            snapshot=snapshot_name,
            path="app.py",
            destination=str(restore_dest)
        ))
        parsed = json.loads(result)
        
        assert "error" not in parsed
        assert parsed["success"] is True
        assert restore_dest.exists()
        assert restore_dest.read_text() == "print('app')"
    
    def test_mcp_backup_diff_end_to_end(self, mcp_env):
        """
        Test backup_diff MCP tool shows file changes.
        
        **Validates: Requirements 10.5**
        """
        server = DevBackupMCPServer(config_path=mcp_env["config_path"])
        
        # Create a backup
        run_result = asyncio.run(server._tool_backup_run())
        run_parsed = json.loads(run_result)
        snapshot_name = run_parsed["snapshot"]
        
        # Make changes
        (mcp_env["source_dir"] / "app.py").write_text("print('modified')")
        (mcp_env["source_dir"] / "new_file.txt").write_text("new")
        (mcp_env["source_dir"] / "config.json").unlink()
        
        # Get diff
        result = asyncio.run(server._tool_backup_diff(snapshot=snapshot_name))
        parsed = json.loads(result)
        
        assert "error" not in parsed
        assert "added" in parsed
        assert "modified" in parsed
        assert "deleted" in parsed
        assert parsed["total_changes"] >= 3
    
    def test_mcp_backup_search_end_to_end(self, mcp_env):
        """
        Test backup_search MCP tool finds files by pattern.
        
        **Validates: Requirements 10.6**
        """
        server = DevBackupMCPServer(config_path=mcp_env["config_path"])
        
        # Create a backup
        asyncio.run(server._tool_backup_run())
        
        # Search for Python files
        result = asyncio.run(server._tool_backup_search(pattern="*.py"))
        parsed = json.loads(result)
        
        assert "error" not in parsed
        assert parsed["total_matches"] >= 1
        
        # Verify match contains app.py
        paths = [m["path"] for m in parsed["matches"]]
        assert any("app.py" in p for p in paths)
    
    def test_mcp_json_response_format(self, mcp_env):
        """
        Test that all MCP tools return valid JSON responses.
        
        **Validates: Requirements 10.7**
        """
        server = DevBackupMCPServer(config_path=mcp_env["config_path"])
        
        # Test all tools return valid JSON
        tools_to_test = [
            server._tool_backup_status(),
            server._tool_backup_list_snapshots(),
        ]
        
        for tool_coro in tools_to_test:
            result = asyncio.run(tool_coro)
            # Should not raise
            parsed = json.loads(result)
            assert isinstance(parsed, dict)
    
    def test_mcp_error_response_format(self, mcp_env):
        """
        Test that MCP error responses have correct format.
        
        **Validates: Requirements 10.8**
        """
        server = DevBackupMCPServer(config_path=mcp_env["config_path"])
        
        # Test with non-existent snapshot
        result = asyncio.run(server._tool_backup_restore(
            snapshot="2099-01-01-120000",
            path="test.txt"
        ))
        parsed = json.loads(result)
        
        assert "error" in parsed
        assert "code" in parsed["error"]
        assert "message" in parsed["error"]
    
    def test_mcp_respects_locking(self, mcp_env):
        """
        Test that MCP server respects the same locking mechanism as CLI.
        
        **Validates: Requirements 10.9**
        """
        server = DevBackupMCPServer(config_path=mcp_env["config_path"])
        
        # Acquire lock manually
        lock_manager = LockManager()
        lock_manager.acquire()
        
        try:
            # Attempt backup via MCP
            result = asyncio.run(server._tool_backup_run())
            parsed = json.loads(result)
            
            assert "error" in parsed
            assert "BACKUP_FAILED" in parsed["error"]["code"] or "lock" in parsed["error"]["message"].lower()
        finally:
            lock_manager.release()
    
    def test_mcp_uses_same_config(self, mcp_env):
        """
        Test that MCP server uses the same config.toml as CLI.
        
        **Validates: Requirements 10.10**
        """
        # Run backup via CLI
        cli_main(["--config", str(mcp_env["config_path"]), "run"])
        
        # Get status via MCP
        server = DevBackupMCPServer(config_path=mcp_env["config_path"])
        result = asyncio.run(server._tool_backup_status())
        parsed = json.loads(result)
        
        # MCP should see the snapshot created by CLI
        assert parsed["total_snapshots"] == 1
        assert parsed["last_backup"] is not None
