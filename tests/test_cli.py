"""Unit tests for CLI commands.

Tests each CLI command with valid inputs and error handling.

**Validates: Requirements 9.1-9.10**
"""

import json
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock
import sys
import io

import pytest

from devbackup.cli import (
    main,
    create_parser,
    cmd_run,
    cmd_status,
    cmd_list,
    cmd_restore,
    cmd_diff,
    cmd_search,
    cmd_install,
    cmd_uninstall,
    cmd_init,
    cmd_verify,
    _format_size,
    _format_interval,
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
    DEFAULT_CONFIG_PATH,
)


def create_test_config(source_dir: Path, dest_dir: Path, log_dir: Path) -> Configuration:
    """Create a test configuration."""
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


class TestArgumentParser:
    """Tests for CLI argument parser setup."""
    
    def test_parser_creation(self):
        """Test that parser is created with all subcommands."""
        parser = create_parser()
        assert parser is not None
        assert parser.prog == 'devbackup'
    
    def test_global_options(self):
        """Test global --config and --verbose options."""
        parser = create_parser()
        
        # Test --config
        args = parser.parse_args(['--config', '/path/to/config.toml', 'run'])
        assert args.config == Path('/path/to/config.toml')
        assert args.command == 'run'
        
        # Test --verbose
        args = parser.parse_args(['--verbose', 'run'])
        assert args.verbose is True
        
        # Test short forms
        args = parser.parse_args(['-c', '/path/config.toml', '-v', 'run'])
        assert args.config == Path('/path/config.toml')
        assert args.verbose is True
    
    def test_run_subcommand(self):
        """Test 'run' subcommand parsing."""
        parser = create_parser()
        args = parser.parse_args(['run'])
        assert args.command == 'run'
    
    def test_status_subcommand(self):
        """Test 'status' subcommand parsing."""
        parser = create_parser()
        args = parser.parse_args(['status'])
        assert args.command == 'status'
    
    def test_list_subcommand(self):
        """Test 'list' subcommand parsing."""
        parser = create_parser()
        
        args = parser.parse_args(['list'])
        assert args.command == 'list'
        assert args.json is False
        
        args = parser.parse_args(['list', '--json'])
        assert args.json is True
    
    def test_restore_subcommand(self):
        """Test 'restore' subcommand parsing."""
        parser = create_parser()
        
        args = parser.parse_args(['restore', '2025-01-01-120000', 'path/to/file'])
        assert args.command == 'restore'
        assert args.snapshot == '2025-01-01-120000'
        assert args.path == 'path/to/file'
        assert args.destination is None
        
        args = parser.parse_args(['restore', '2025-01-01-120000', 'file.txt', '--to', '/dest'])
        assert args.destination == Path('/dest')
    
    def test_diff_subcommand(self):
        """Test 'diff' subcommand parsing."""
        parser = create_parser()
        
        args = parser.parse_args(['diff', '2025-01-01-120000'])
        assert args.command == 'diff'
        assert args.snapshot == '2025-01-01-120000'
        assert args.path is None
        
        args = parser.parse_args(['diff', '2025-01-01-120000', '--path', 'src/'])
        assert args.path == 'src/'
    
    def test_search_subcommand(self):
        """Test 'search' subcommand parsing."""
        parser = create_parser()
        
        args = parser.parse_args(['search', '*.py'])
        assert args.command == 'search'
        assert args.pattern == '*.py'
        assert args.snapshot is None
        
        args = parser.parse_args(['search', '*.txt', '--snapshot', '2025-01-01-120000'])
        assert args.snapshot == '2025-01-01-120000'
    
    def test_install_subcommand(self):
        """Test 'install' subcommand parsing."""
        parser = create_parser()
        args = parser.parse_args(['install'])
        assert args.command == 'install'
    
    def test_uninstall_subcommand(self):
        """Test 'uninstall' subcommand parsing."""
        parser = create_parser()
        args = parser.parse_args(['uninstall'])
        assert args.command == 'uninstall'
    
    def test_init_subcommand(self):
        """Test 'init' subcommand parsing."""
        parser = create_parser()
        
        args = parser.parse_args(['init'])
        assert args.command == 'init'
        assert args.force is False
        
        args = parser.parse_args(['init', '--force'])
        assert args.force is True
        
        args = parser.parse_args(['init', '-f'])
        assert args.force is True
    
    def test_verify_subcommand(self):
        """Test 'verify' subcommand parsing."""
        parser = create_parser()
        
        args = parser.parse_args(['verify', '2025-01-01-120000'])
        assert args.command == 'verify'
        assert args.snapshot == '2025-01-01-120000'
        assert args.pattern is None
        assert args.json is False
        
        args = parser.parse_args(['verify', '2025-01-01-120000', '--pattern', '*.py'])
        assert args.pattern == '*.py'
        
        args = parser.parse_args(['verify', '2025-01-01-120000', '--json'])
        assert args.json is True


class TestRunCommand:
    """Tests for 'devbackup run' command."""
    
    def test_run_success(self):
        """Test successful backup run."""
        with tempfile.TemporaryDirectory() as source_dir:
            with tempfile.TemporaryDirectory() as dest_dir:
                with tempfile.TemporaryDirectory() as log_dir:
                    with tempfile.TemporaryDirectory() as config_dir:
                        source_path = Path(source_dir)
                        dest_path = Path(dest_dir)
                        log_path = Path(log_dir)
                        config_path = Path(config_dir) / "config.toml"
                        
                        # Create test file
                        (source_path / "test.txt").write_text("test content")
                        
                        config = create_test_config(source_path, dest_path, log_path)
                        config_path.write_text(format_config(config))
                        
                        exit_code = main(['--config', str(config_path), 'run'])
                        assert exit_code == EXIT_SUCCESS
    
    def test_run_with_verbose(self):
        """Test backup run with verbose output."""
        with tempfile.TemporaryDirectory() as source_dir:
            with tempfile.TemporaryDirectory() as dest_dir:
                with tempfile.TemporaryDirectory() as log_dir:
                    with tempfile.TemporaryDirectory() as config_dir:
                        source_path = Path(source_dir)
                        dest_path = Path(dest_dir)
                        log_path = Path(log_dir)
                        config_path = Path(config_dir) / "config.toml"
                        
                        (source_path / "test.txt").write_text("test content")
                        
                        config = create_test_config(source_path, dest_path, log_path)
                        config_path.write_text(format_config(config))
                        
                        captured_stdout = io.StringIO()
                        with patch('sys.stdout', captured_stdout):
                            exit_code = main(['--config', str(config_path), '-v', 'run'])
                        
                        assert exit_code == EXIT_SUCCESS
                        output = captured_stdout.getvalue()
                        assert "Starting backup" in output or "Backup completed" in output
    
    def test_run_missing_config(self):
        """Test run with missing config file."""
        captured_stderr = io.StringIO()
        with patch('sys.stderr', captured_stderr):
            exit_code = main(['--config', '/nonexistent/config.toml', 'run'])
        
        assert exit_code == EXIT_CONFIG_ERROR
        stderr_output = captured_stderr.getvalue()
        assert "Configuration" in stderr_output or "config" in stderr_output.lower()


class TestListCommand:
    """Tests for 'devbackup list' command."""
    
    def test_list_empty(self):
        """Test list with no snapshots."""
        with tempfile.TemporaryDirectory() as source_dir:
            with tempfile.TemporaryDirectory() as dest_dir:
                with tempfile.TemporaryDirectory() as log_dir:
                    with tempfile.TemporaryDirectory() as config_dir:
                        config_path = Path(config_dir) / "config.toml"
                        config = create_test_config(
                            Path(source_dir), Path(dest_dir), Path(log_dir)
                        )
                        config_path.write_text(format_config(config))
                        
                        captured_stdout = io.StringIO()
                        with patch('sys.stdout', captured_stdout):
                            exit_code = main(['--config', str(config_path), 'list'])
                        
                        assert exit_code == EXIT_SUCCESS
                        assert "No snapshots found" in captured_stdout.getvalue()
    
    def test_list_with_snapshots(self):
        """Test list with existing snapshots."""
        with tempfile.TemporaryDirectory() as source_dir:
            with tempfile.TemporaryDirectory() as dest_dir:
                with tempfile.TemporaryDirectory() as log_dir:
                    with tempfile.TemporaryDirectory() as config_dir:
                        dest_path = Path(dest_dir)
                        config_path = Path(config_dir) / "config.toml"
                        config = create_test_config(
                            Path(source_dir), dest_path, Path(log_dir)
                        )
                        config_path.write_text(format_config(config))
                        
                        # Create a snapshot directory
                        snapshot_dir = dest_path / "2025-01-01-120000"
                        snapshot_dir.mkdir()
                        (snapshot_dir / "test.txt").write_text("test")
                        
                        captured_stdout = io.StringIO()
                        with patch('sys.stdout', captured_stdout):
                            exit_code = main(['--config', str(config_path), 'list'])
                        
                        assert exit_code == EXIT_SUCCESS
                        output = captured_stdout.getvalue()
                        assert "2025-01-01" in output
    
    def test_list_json_output(self):
        """Test list with JSON output."""
        with tempfile.TemporaryDirectory() as source_dir:
            with tempfile.TemporaryDirectory() as dest_dir:
                with tempfile.TemporaryDirectory() as log_dir:
                    with tempfile.TemporaryDirectory() as config_dir:
                        dest_path = Path(dest_dir)
                        config_path = Path(config_dir) / "config.toml"
                        config = create_test_config(
                            Path(source_dir), dest_path, Path(log_dir)
                        )
                        config_path.write_text(format_config(config))
                        
                        # Create a snapshot
                        snapshot_dir = dest_path / "2025-01-01-120000"
                        snapshot_dir.mkdir()
                        (snapshot_dir / "test.txt").write_text("test")
                        
                        captured_stdout = io.StringIO()
                        with patch('sys.stdout', captured_stdout):
                            exit_code = main(['--config', str(config_path), 'list', '--json'])
                        
                        assert exit_code == EXIT_SUCCESS
                        output = captured_stdout.getvalue()
                        data = json.loads(output)
                        assert isinstance(data, list)
                        assert len(data) == 1
                        assert "timestamp" in data[0]
                        assert "size_bytes" in data[0]


class TestRestoreCommand:
    """Tests for 'devbackup restore' command."""
    
    def test_restore_success(self):
        """Test successful file restore."""
        with tempfile.TemporaryDirectory() as source_dir:
            with tempfile.TemporaryDirectory() as dest_dir:
                with tempfile.TemporaryDirectory() as log_dir:
                    with tempfile.TemporaryDirectory() as config_dir:
                        with tempfile.TemporaryDirectory() as restore_dir:
                            source_path = Path(source_dir)
                            dest_path = Path(dest_dir)
                            config_path = Path(config_dir) / "config.toml"
                            restore_path = Path(restore_dir) / "restored.txt"
                            
                            config = create_test_config(
                                source_path, dest_path, Path(log_dir)
                            )
                            config_path.write_text(format_config(config))
                            
                            # Create a snapshot with a file
                            snapshot_dir = dest_path / "2025-01-01-120000"
                            snapshot_dir.mkdir()
                            (snapshot_dir / "test.txt").write_text("original content")
                            
                            exit_code = main([
                                '--config', str(config_path),
                                'restore', '2025-01-01-120000', 'test.txt',
                                '--to', str(restore_path)
                            ])
                            
                            assert exit_code == EXIT_SUCCESS
                            assert restore_path.exists()
                            assert restore_path.read_text() == "original content"
    
    def test_restore_snapshot_not_found(self):
        """Test restore with non-existent snapshot."""
        with tempfile.TemporaryDirectory() as source_dir:
            with tempfile.TemporaryDirectory() as dest_dir:
                with tempfile.TemporaryDirectory() as log_dir:
                    with tempfile.TemporaryDirectory() as config_dir:
                        config_path = Path(config_dir) / "config.toml"
                        config = create_test_config(
                            Path(source_dir), Path(dest_dir), Path(log_dir)
                        )
                        config_path.write_text(format_config(config))
                        
                        captured_stderr = io.StringIO()
                        with patch('sys.stderr', captured_stderr):
                            exit_code = main([
                                '--config', str(config_path),
                                'restore', '2025-01-01-120000', 'test.txt'
                            ])
                        
                        assert exit_code == EXIT_GENERAL_ERROR
                        assert "Snapshot not found" in captured_stderr.getvalue()


class TestDiffCommand:
    """Tests for 'devbackup diff' command."""
    
    def test_diff_no_changes(self):
        """Test diff with no changes."""
        with tempfile.TemporaryDirectory() as source_dir:
            with tempfile.TemporaryDirectory() as dest_dir:
                with tempfile.TemporaryDirectory() as log_dir:
                    with tempfile.TemporaryDirectory() as config_dir:
                        source_path = Path(source_dir)
                        dest_path = Path(dest_dir)
                        config_path = Path(config_dir) / "config.toml"
                        
                        # Create source file
                        (source_path / "test.txt").write_text("content")
                        
                        config = create_test_config(source_path, dest_path, Path(log_dir))
                        config_path.write_text(format_config(config))
                        
                        # Create matching snapshot
                        snapshot_dir = dest_path / "2025-01-01-120000"
                        snapshot_dir.mkdir()
                        (snapshot_dir / "test.txt").write_text("content")
                        
                        captured_stdout = io.StringIO()
                        with patch('sys.stdout', captured_stdout):
                            exit_code = main([
                                '--config', str(config_path),
                                'diff', '2025-01-01-120000'
                            ])
                        
                        assert exit_code == EXIT_SUCCESS
                        assert "No changes" in captured_stdout.getvalue()
    
    def test_diff_with_changes(self):
        """Test diff with file changes."""
        with tempfile.TemporaryDirectory() as source_dir:
            with tempfile.TemporaryDirectory() as dest_dir:
                with tempfile.TemporaryDirectory() as log_dir:
                    with tempfile.TemporaryDirectory() as config_dir:
                        source_path = Path(source_dir)
                        dest_path = Path(dest_dir)
                        config_path = Path(config_dir) / "config.toml"
                        
                        # Create source with new file
                        (source_path / "new.txt").write_text("new content")
                        
                        config = create_test_config(source_path, dest_path, Path(log_dir))
                        config_path.write_text(format_config(config))
                        
                        # Create snapshot without the new file
                        snapshot_dir = dest_path / "2025-01-01-120000"
                        snapshot_dir.mkdir()
                        (snapshot_dir / "old.txt").write_text("old content")
                        
                        captured_stdout = io.StringIO()
                        with patch('sys.stdout', captured_stdout):
                            exit_code = main([
                                '--config', str(config_path),
                                'diff', '2025-01-01-120000'
                            ])
                        
                        assert exit_code == EXIT_SUCCESS
                        output = captured_stdout.getvalue()
                        assert "Added" in output or "Deleted" in output


class TestSearchCommand:
    """Tests for 'devbackup search' command."""
    
    def test_search_no_results(self):
        """Test search with no matching files."""
        with tempfile.TemporaryDirectory() as source_dir:
            with tempfile.TemporaryDirectory() as dest_dir:
                with tempfile.TemporaryDirectory() as log_dir:
                    with tempfile.TemporaryDirectory() as config_dir:
                        dest_path = Path(dest_dir)
                        config_path = Path(config_dir) / "config.toml"
                        
                        config = create_test_config(
                            Path(source_dir), dest_path, Path(log_dir)
                        )
                        config_path.write_text(format_config(config))
                        
                        # Create snapshot with different files
                        snapshot_dir = dest_path / "2025-01-01-120000"
                        snapshot_dir.mkdir()
                        (snapshot_dir / "test.txt").write_text("content")
                        
                        captured_stdout = io.StringIO()
                        with patch('sys.stdout', captured_stdout):
                            exit_code = main([
                                '--config', str(config_path),
                                'search', '*.py'
                            ])
                        
                        assert exit_code == EXIT_SUCCESS
                        assert "No files matching" in captured_stdout.getvalue()
    
    def test_search_with_results(self):
        """Test search with matching files."""
        with tempfile.TemporaryDirectory() as source_dir:
            with tempfile.TemporaryDirectory() as dest_dir:
                with tempfile.TemporaryDirectory() as log_dir:
                    with tempfile.TemporaryDirectory() as config_dir:
                        dest_path = Path(dest_dir)
                        config_path = Path(config_dir) / "config.toml"
                        
                        config = create_test_config(
                            Path(source_dir), dest_path, Path(log_dir)
                        )
                        config_path.write_text(format_config(config))
                        
                        # Create snapshot with matching files
                        snapshot_dir = dest_path / "2025-01-01-120000"
                        snapshot_dir.mkdir()
                        (snapshot_dir / "script.py").write_text("print('hello')")
                        
                        captured_stdout = io.StringIO()
                        with patch('sys.stdout', captured_stdout):
                            exit_code = main([
                                '--config', str(config_path),
                                'search', '*.py'
                            ])
                        
                        assert exit_code == EXIT_SUCCESS
                        output = captured_stdout.getvalue()
                        assert "script.py" in output
                        assert "Found" in output


class TestInitCommand:
    """Tests for 'devbackup init' command."""
    
    def test_init_creates_config(self):
        """Test init creates default config file."""
        with tempfile.TemporaryDirectory() as config_dir:
            config_path = Path(config_dir) / "config.toml"
            
            exit_code = main(['--config', str(config_path), 'init'])
            
            assert exit_code == EXIT_SUCCESS
            assert config_path.exists()
            content = config_path.read_text()
            assert "backup_destination" in content
            assert "source_directories" in content
    
    def test_init_refuses_overwrite(self):
        """Test init refuses to overwrite existing config."""
        with tempfile.TemporaryDirectory() as config_dir:
            config_path = Path(config_dir) / "config.toml"
            config_path.write_text("existing content")
            
            captured_stderr = io.StringIO()
            with patch('sys.stderr', captured_stderr):
                exit_code = main(['--config', str(config_path), 'init'])
            
            assert exit_code == EXIT_GENERAL_ERROR
            assert "already exists" in captured_stderr.getvalue()
            assert config_path.read_text() == "existing content"
    
    def test_init_force_overwrites(self):
        """Test init --force overwrites existing config."""
        with tempfile.TemporaryDirectory() as config_dir:
            config_path = Path(config_dir) / "config.toml"
            config_path.write_text("existing content")
            
            exit_code = main(['--config', str(config_path), 'init', '--force'])
            
            assert exit_code == EXIT_SUCCESS
            content = config_path.read_text()
            assert "backup_destination" in content


class TestStatusCommand:
    """Tests for 'devbackup status' command."""
    
    def test_status_no_backups(self):
        """Test status with no previous backups."""
        with tempfile.TemporaryDirectory() as source_dir:
            with tempfile.TemporaryDirectory() as dest_dir:
                with tempfile.TemporaryDirectory() as log_dir:
                    with tempfile.TemporaryDirectory() as config_dir:
                        config_path = Path(config_dir) / "config.toml"
                        config = create_test_config(
                            Path(source_dir), Path(dest_dir), Path(log_dir)
                        )
                        config_path.write_text(format_config(config))
                        
                        captured_stdout = io.StringIO()
                        with patch('sys.stdout', captured_stdout):
                            exit_code = main(['--config', str(config_path), 'status'])
                        
                        assert exit_code == EXIT_SUCCESS
                        output = captured_stdout.getvalue()
                        assert "Status" in output
                        assert "Never" in output or "Last backup" in output


class TestVerifyCommand:
    """Tests for 'devbackup verify' command."""
    
    def test_verify_snapshot_not_found(self):
        """Test verify with non-existent snapshot."""
        with tempfile.TemporaryDirectory() as source_dir:
            with tempfile.TemporaryDirectory() as dest_dir:
                with tempfile.TemporaryDirectory() as log_dir:
                    with tempfile.TemporaryDirectory() as config_dir:
                        config_path = Path(config_dir) / "config.toml"
                        config = create_test_config(
                            Path(source_dir), Path(dest_dir), Path(log_dir)
                        )
                        config_path.write_text(format_config(config))
                        
                        captured_stderr = io.StringIO()
                        with patch('sys.stderr', captured_stderr):
                            exit_code = main([
                                '--config', str(config_path),
                                'verify', '2025-01-01-120000'
                            ])
                        
                        assert exit_code == EXIT_GENERAL_ERROR
                        assert "Snapshot not found" in captured_stderr.getvalue()
    
    def test_verify_no_manifest(self):
        """Test verify with snapshot that has no manifest."""
        with tempfile.TemporaryDirectory() as source_dir:
            with tempfile.TemporaryDirectory() as dest_dir:
                with tempfile.TemporaryDirectory() as log_dir:
                    with tempfile.TemporaryDirectory() as config_dir:
                        dest_path = Path(dest_dir)
                        config_path = Path(config_dir) / "config.toml"
                        
                        config = create_test_config(
                            Path(source_dir), dest_path, Path(log_dir)
                        )
                        config_path.write_text(format_config(config))
                        
                        # Create snapshot without manifest
                        snapshot_dir = dest_path / "2025-01-01-120000"
                        snapshot_dir.mkdir()
                        (snapshot_dir / "test.txt").write_text("content")
                        
                        captured_stdout = io.StringIO()
                        with patch('sys.stdout', captured_stdout):
                            exit_code = main([
                                '--config', str(config_path),
                                'verify', '2025-01-01-120000'
                            ])
                        
                        assert exit_code == EXIT_GENERAL_ERROR
                        output = captured_stdout.getvalue()
                        assert "FAILED" in output
    
    def test_verify_success_with_manifest(self):
        """Test verify with valid manifest."""
        from devbackup.verify import IntegrityVerifier
        
        with tempfile.TemporaryDirectory() as source_dir:
            with tempfile.TemporaryDirectory() as dest_dir:
                with tempfile.TemporaryDirectory() as log_dir:
                    with tempfile.TemporaryDirectory() as config_dir:
                        dest_path = Path(dest_dir)
                        config_path = Path(config_dir) / "config.toml"
                        
                        config = create_test_config(
                            Path(source_dir), dest_path, Path(log_dir)
                        )
                        config_path.write_text(format_config(config))
                        
                        # Create snapshot with file
                        snapshot_dir = dest_path / "2025-01-01-120000"
                        snapshot_dir.mkdir()
                        (snapshot_dir / "test.txt").write_text("content")
                        
                        # Create manifest
                        verifier = IntegrityVerifier()
                        manifest = verifier.create_manifest(snapshot_dir)
                        verifier.save_manifest(manifest, snapshot_dir)
                        
                        captured_stdout = io.StringIO()
                        with patch('sys.stdout', captured_stdout):
                            exit_code = main([
                                '--config', str(config_path),
                                'verify', '2025-01-01-120000'
                            ])
                        
                        assert exit_code == EXIT_SUCCESS
                        output = captured_stdout.getvalue()
                        assert "PASSED" in output
    
    def test_verify_json_output(self):
        """Test verify with JSON output."""
        from devbackup.verify import IntegrityVerifier
        
        with tempfile.TemporaryDirectory() as source_dir:
            with tempfile.TemporaryDirectory() as dest_dir:
                with tempfile.TemporaryDirectory() as log_dir:
                    with tempfile.TemporaryDirectory() as config_dir:
                        dest_path = Path(dest_dir)
                        config_path = Path(config_dir) / "config.toml"
                        
                        config = create_test_config(
                            Path(source_dir), dest_path, Path(log_dir)
                        )
                        config_path.write_text(format_config(config))
                        
                        # Create snapshot with file
                        snapshot_dir = dest_path / "2025-01-01-120000"
                        snapshot_dir.mkdir()
                        (snapshot_dir / "test.txt").write_text("content")
                        
                        # Create manifest
                        verifier = IntegrityVerifier()
                        manifest = verifier.create_manifest(snapshot_dir)
                        verifier.save_manifest(manifest, snapshot_dir)
                        
                        captured_stdout = io.StringIO()
                        with patch('sys.stdout', captured_stdout):
                            exit_code = main([
                                '--config', str(config_path),
                                'verify', '2025-01-01-120000', '--json'
                            ])
                        
                        assert exit_code == EXIT_SUCCESS
                        output = captured_stdout.getvalue()
                        result = json.loads(output)
                        assert result["success"] is True
                        assert result["files_verified"] == 1


class TestHelperFunctions:
    """Tests for CLI helper functions."""
    
    def test_format_size(self):
        """Test size formatting."""
        assert _format_size(500) == "500 B"
        assert _format_size(1024) == "1.0 KB"
        assert _format_size(1536) == "1.5 KB"
        assert _format_size(1024 * 1024) == "1.0 MB"
        assert _format_size(1024 * 1024 * 1024) == "1.0 GB"
    
    def test_format_interval(self):
        """Test interval formatting."""
        assert _format_interval(30) == "30 seconds"
        assert _format_interval(60) == "1 minute"
        assert _format_interval(120) == "2 minutes"
        assert _format_interval(3600) == "1 hour"
        assert _format_interval(7200) == "2 hours"
        assert _format_interval(86400) == "1 day"
        assert _format_interval(172800) == "2 days"


class TestMainFunction:
    """Tests for main CLI entry point."""
    
    def test_no_command_shows_help(self):
        """Test that no command shows help."""
        captured_stdout = io.StringIO()
        with patch('sys.stdout', captured_stdout):
            exit_code = main([])
        
        assert exit_code == EXIT_SUCCESS
        output = captured_stdout.getvalue()
        assert "devbackup" in output or "usage" in output.lower()
    
    def test_keyboard_interrupt_handling(self):
        """Test that KeyboardInterrupt is handled gracefully."""
        with patch('devbackup.cli.cmd_run', side_effect=KeyboardInterrupt):
            captured_stderr = io.StringIO()
            with patch('sys.stderr', captured_stderr):
                exit_code = main(['run'])
            
            assert exit_code == 130  # Standard SIGINT exit code
            assert "Interrupted" in captured_stderr.getvalue()
    
    def test_unexpected_exception_handling(self):
        """Test that unexpected exceptions are handled."""
        with patch('devbackup.cli.cmd_run', side_effect=RuntimeError("Unexpected")):
            captured_stderr = io.StringIO()
            with patch('sys.stderr', captured_stderr):
                exit_code = main(['run'])
            
            assert exit_code == EXIT_GENERAL_ERROR
            assert "Error" in captured_stderr.getvalue()
