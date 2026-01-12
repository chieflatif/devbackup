"""Property-based tests for CLI exit code consistency.

Tests Property 11 (CLI Exit Code Consistency) from the design document.

**Validates: Requirements 9.8**
"""

import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock
import sys
import io

import pytest
from hypothesis import given, strategies as st, settings, Phase

from devbackup.cli import (
    main,
    EXIT_SUCCESS,
    EXIT_CONFIG_ERROR,
    EXIT_GENERAL_ERROR,
)
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


# Strategy for generating CLI commands that should succeed
success_commands = st.sampled_from([
    ["--help"],
    ["--version"],
    ["run", "--help"],
    ["status", "--help"],
    ["list", "--help"],
    ["restore", "--help"],
    ["diff", "--help"],
    ["search", "--help"],
    ["install", "--help"],
    ["uninstall", "--help"],
    ["init", "--help"],
])

# Strategy for generating CLI commands that should fail due to missing config
config_required_commands = st.sampled_from([
    ["status"],
    ["list"],
    ["install"],
    ["uninstall"],
])


class TestCLIExitCodeConsistency:
    """
    Property 11: CLI Exit Code Consistency
    
    *For any* CLI command execution, the exit code SHALL be 0 on success
    and non-zero on failure, with error messages written to stderr.
    
    **Validates: Requirements 9.8**
    """
    
    @given(command=success_commands)
    @settings(max_examples=10, deadline=None, phases=[Phase.generate, Phase.target])
    def test_help_commands_return_zero(self, command: list):
        """
        **Feature: macos-incremental-backup, Property 11: CLI Exit Code Consistency**
        
        Help commands should always return exit code 0.
        """
        # Capture stdout/stderr
        captured_stdout = io.StringIO()
        captured_stderr = io.StringIO()
        
        with patch('sys.stdout', captured_stdout), patch('sys.stderr', captured_stderr):
            try:
                exit_code = main(command)
            except SystemExit as e:
                # argparse raises SystemExit for --help and --version
                exit_code = e.code if e.code is not None else 0
        
        # Help and version commands should exit with 0
        assert exit_code == 0, \
            f"Command {command} returned non-zero exit code: {exit_code}"
    
    @given(command=config_required_commands)
    @settings(max_examples=10, deadline=None, phases=[Phase.generate, Phase.target])
    def test_missing_config_returns_nonzero(self, command: list):
        """
        **Feature: macos-incremental-backup, Property 11: CLI Exit Code Consistency**
        
        Commands requiring config should return non-zero when config is missing.
        """
        # Use a non-existent config path
        full_command = ["--config", "/nonexistent/config.toml"] + command
        
        captured_stderr = io.StringIO()
        
        with patch('sys.stderr', captured_stderr):
            exit_code = main(full_command)
        
        # Should fail with config error
        assert exit_code != 0, \
            f"Command {command} should fail with missing config, got exit code: {exit_code}"
        
        # Should have error message in stderr
        stderr_output = captured_stderr.getvalue()
        assert len(stderr_output) > 0, \
            f"Command {command} should write error to stderr"
    
    @given(
        success=st.booleans(),
        verbose=st.booleans()
    )
    @settings(max_examples=10, deadline=None, phases=[Phase.generate, Phase.target])
    def test_run_command_exit_codes(self, success: bool, verbose: bool):
        """
        **Feature: macos-incremental-backup, Property 11: CLI Exit Code Consistency**
        
        The 'run' command should return 0 on success, non-zero on failure.
        """
        with tempfile.TemporaryDirectory() as source_dir:
            with tempfile.TemporaryDirectory() as dest_dir:
                with tempfile.TemporaryDirectory() as log_dir:
                    with tempfile.TemporaryDirectory() as config_dir:
                        source_path = Path(source_dir)
                        dest_path = Path(dest_dir)
                        log_path = Path(log_dir)
                        config_path = Path(config_dir) / "config.toml"
                        
                        # Create test files
                        (source_path / "test.txt").write_text("test content")
                        
                        config = create_test_config(source_path, dest_path, log_path)
                        
                        if not success:
                            # Make it fail by using invalid source
                            config.source_directories = [Path("/nonexistent/source")]
                        
                        # Write config file
                        config_path.write_text(format_config(config))
                        
                        # Build command
                        command = ["--config", str(config_path)]
                        if verbose:
                            command.append("--verbose")
                        command.append("run")
                        
                        captured_stderr = io.StringIO()
                        
                        with patch('sys.stderr', captured_stderr):
                            exit_code = main(command)
                        
                        if success:
                            assert exit_code == EXIT_SUCCESS, \
                                f"Successful run should return 0, got {exit_code}"
                        else:
                            assert exit_code != EXIT_SUCCESS, \
                                f"Failed run should return non-zero, got {exit_code}"
                            # Should have error in stderr
                            stderr_output = captured_stderr.getvalue()
                            assert len(stderr_output) > 0, \
                                "Failed run should write error to stderr"
    
    @given(json_output=st.booleans())
    @settings(max_examples=50, deadline=None, phases=[Phase.generate, Phase.target])
    def test_list_command_exit_codes(self, json_output: bool):
        """
        **Feature: macos-incremental-backup, Property 11: CLI Exit Code Consistency**
        
        The 'list' command should return 0 when config is valid.
        """
        with tempfile.TemporaryDirectory() as source_dir:
            with tempfile.TemporaryDirectory() as dest_dir:
                with tempfile.TemporaryDirectory() as log_dir:
                    with tempfile.TemporaryDirectory() as config_dir:
                        source_path = Path(source_dir)
                        dest_path = Path(dest_dir)
                        log_path = Path(log_dir)
                        config_path = Path(config_dir) / "config.toml"
                        
                        config = create_test_config(source_path, dest_path, log_path)
                        config_path.write_text(format_config(config))
                        
                        command = ["--config", str(config_path), "list"]
                        if json_output:
                            command.append("--json")
                        
                        exit_code = main(command)
                        
                        assert exit_code == EXIT_SUCCESS, \
                            f"List command should return 0, got {exit_code}"
    
    @given(force=st.booleans())
    @settings(max_examples=50, deadline=None, phases=[Phase.generate, Phase.target])
    def test_init_command_exit_codes(self, force: bool):
        """
        **Feature: macos-incremental-backup, Property 11: CLI Exit Code Consistency**
        
        The 'init' command should return 0 on success, non-zero if file exists without --force.
        """
        with tempfile.TemporaryDirectory() as config_dir:
            config_path = Path(config_dir) / "config.toml"
            
            # First init should always succeed
            command = ["--config", str(config_path), "init"]
            exit_code = main(command)
            assert exit_code == EXIT_SUCCESS, \
                f"First init should return 0, got {exit_code}"
            
            # Second init depends on --force flag
            command = ["--config", str(config_path), "init"]
            if force:
                command.append("--force")
            
            captured_stderr = io.StringIO()
            with patch('sys.stderr', captured_stderr):
                exit_code = main(command)
            
            if force:
                assert exit_code == EXIT_SUCCESS, \
                    f"Init with --force should return 0, got {exit_code}"
            else:
                assert exit_code != EXIT_SUCCESS, \
                    f"Init without --force on existing file should fail, got {exit_code}"
                stderr_output = captured_stderr.getvalue()
                assert len(stderr_output) > 0, \
                    "Init failure should write error to stderr"
    
    @given(
        snapshot_exists=st.booleans(),
        path_exists=st.booleans()
    )
    @settings(max_examples=50, deadline=None, phases=[Phase.generate, Phase.target])
    def test_restore_command_exit_codes(self, snapshot_exists: bool, path_exists: bool):
        """
        **Feature: macos-incremental-backup, Property 11: CLI Exit Code Consistency**
        
        The 'restore' command should return 0 on success, non-zero on failure.
        """
        with tempfile.TemporaryDirectory() as source_dir:
            with tempfile.TemporaryDirectory() as dest_dir:
                with tempfile.TemporaryDirectory() as log_dir:
                    with tempfile.TemporaryDirectory() as config_dir:
                        source_path = Path(source_dir)
                        dest_path = Path(dest_dir)
                        log_path = Path(log_dir)
                        config_path = Path(config_dir) / "config.toml"
                        
                        config = create_test_config(source_path, dest_path, log_path)
                        config_path.write_text(format_config(config))
                        
                        # Create a snapshot if needed
                        snapshot_timestamp = "2025-01-01-120000"
                        if snapshot_exists:
                            snapshot_dir = dest_path / snapshot_timestamp
                            snapshot_dir.mkdir(parents=True)
                            if path_exists:
                                (snapshot_dir / "test.txt").write_text("test content")
                        
                        # Build restore command
                        command = [
                            "--config", str(config_path),
                            "restore",
                            snapshot_timestamp,
                            "test.txt"
                        ]
                        
                        captured_stderr = io.StringIO()
                        with patch('sys.stderr', captured_stderr):
                            exit_code = main(command)
                        
                        if snapshot_exists and path_exists:
                            assert exit_code == EXIT_SUCCESS, \
                                f"Restore should succeed, got {exit_code}"
                        else:
                            assert exit_code != EXIT_SUCCESS, \
                                f"Restore should fail when snapshot/path missing, got {exit_code}"
                            stderr_output = captured_stderr.getvalue()
                            assert len(stderr_output) > 0, \
                                "Restore failure should write error to stderr"
