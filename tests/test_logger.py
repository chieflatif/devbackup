"""Tests for the logger module."""

import logging
import os
import tempfile
from pathlib import Path

import pytest

from devbackup.config import LoggingConfig
from devbackup.logger import (
    LOGGER_NAME,
    LoggingError,
    setup_logging,
    get_logger,
    log_backup_start,
    log_backup_completion,
    log_backup_error,
    log_rsync_output,
)


@pytest.fixture
def temp_log_dir():
    """Create a temporary directory for log files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def cleanup_logger():
    """Clean up logger handlers after each test."""
    yield
    logger = logging.getLogger(LOGGER_NAME)
    logger.handlers.clear()


class TestSetupLogging:
    """Tests for setup_logging function."""
    
    def test_setup_with_config(self, temp_log_dir, cleanup_logger):
        """Test setup_logging with LoggingConfig object."""
        config = LoggingConfig(
            level="INFO",
            log_file=temp_log_dir / "test.log",
            error_log_file=temp_log_dir / "test.err",
        )
        
        logger = setup_logging(config=config)
        
        assert logger.name == LOGGER_NAME
        assert len(logger.handlers) == 3  # file, error, console
    
    def test_setup_with_individual_params(self, temp_log_dir, cleanup_logger):
        """Test setup_logging with individual parameters."""
        log_file = temp_log_dir / "test.log"
        error_file = temp_log_dir / "test.err"
        
        logger = setup_logging(
            log_file=log_file,
            error_log_file=error_file,
            level="DEBUG",
        )
        
        assert logger.name == LOGGER_NAME
        assert len(logger.handlers) == 3
    
    def test_creates_log_directories(self, temp_log_dir, cleanup_logger):
        """Test that setup_logging creates parent directories."""
        nested_log = temp_log_dir / "nested" / "dir" / "test.log"
        nested_err = temp_log_dir / "nested" / "dir" / "test.err"
        
        setup_logging(
            log_file=nested_log,
            error_log_file=nested_err,
            level="INFO",
        )
        
        assert nested_log.parent.exists()
        assert nested_err.parent.exists()
    
    def test_invalid_log_level_raises_error(self, temp_log_dir, cleanup_logger):
        """Test that invalid log level raises LoggingError."""
        with pytest.raises(LoggingError) as exc_info:
            setup_logging(
                log_file=temp_log_dir / "test.log",
                error_log_file=temp_log_dir / "test.err",
                level="INVALID",
            )
        
        assert "Invalid log level" in str(exc_info.value)
    
    def test_debug_level(self, temp_log_dir, cleanup_logger):
        """Test DEBUG level logs all messages."""
        log_file = temp_log_dir / "test.log"
        
        logger = setup_logging(
            log_file=log_file,
            error_log_file=temp_log_dir / "test.err",
            level="DEBUG",
        )
        
        logger.debug("debug message")
        logger.info("info message")
        logger.error("error message")
        
        # Flush handlers
        for handler in logger.handlers:
            handler.flush()
        
        content = log_file.read_text()
        assert "debug message" in content
        assert "info message" in content
        assert "error message" in content
    
    def test_info_level(self, temp_log_dir, cleanup_logger):
        """Test INFO level filters debug messages."""
        log_file = temp_log_dir / "test.log"
        
        logger = setup_logging(
            log_file=log_file,
            error_log_file=temp_log_dir / "test.err",
            level="INFO",
        )
        
        logger.debug("debug message")
        logger.info("info message")
        logger.error("error message")
        
        for handler in logger.handlers:
            handler.flush()
        
        content = log_file.read_text()
        assert "debug message" not in content
        assert "info message" in content
        assert "error message" in content
    
    def test_error_level(self, temp_log_dir, cleanup_logger):
        """Test ERROR level filters info and debug messages."""
        log_file = temp_log_dir / "test.log"
        
        logger = setup_logging(
            log_file=log_file,
            error_log_file=temp_log_dir / "test.err",
            level="ERROR",
        )
        
        logger.debug("debug message")
        logger.info("info message")
        logger.error("error message")
        
        for handler in logger.handlers:
            handler.flush()
        
        content = log_file.read_text()
        assert "debug message" not in content
        assert "info message" not in content
        assert "error message" in content
    
    def test_error_log_file_only_errors(self, temp_log_dir, cleanup_logger):
        """Test that error log file only contains errors."""
        error_file = temp_log_dir / "test.err"
        
        logger = setup_logging(
            log_file=temp_log_dir / "test.log",
            error_log_file=error_file,
            level="DEBUG",
        )
        
        logger.debug("debug message")
        logger.info("info message")
        logger.warning("warning message")
        logger.error("error message")
        
        for handler in logger.handlers:
            handler.flush()
        
        content = error_file.read_text()
        assert "debug message" not in content
        assert "info message" not in content
        assert "warning message" not in content
        assert "error message" in content
    
    def test_clears_existing_handlers(self, temp_log_dir, cleanup_logger):
        """Test that setup_logging clears existing handlers."""
        # First setup
        logger = setup_logging(
            log_file=temp_log_dir / "test1.log",
            error_log_file=temp_log_dir / "test1.err",
            level="INFO",
        )
        initial_handlers = len(logger.handlers)
        
        # Second setup should replace handlers
        logger = setup_logging(
            log_file=temp_log_dir / "test2.log",
            error_log_file=temp_log_dir / "test2.err",
            level="DEBUG",
        )
        
        assert len(logger.handlers) == initial_handlers


class TestGetLogger:
    """Tests for get_logger function."""
    
    def test_returns_same_logger(self, cleanup_logger):
        """Test that get_logger returns the devbackup logger."""
        logger = get_logger()
        assert logger.name == LOGGER_NAME
    
    def test_returns_configured_logger(self, temp_log_dir, cleanup_logger):
        """Test that get_logger returns the configured logger."""
        setup_logging(
            log_file=temp_log_dir / "test.log",
            error_log_file=temp_log_dir / "test.err",
            level="DEBUG",
        )
        
        logger = get_logger()
        assert len(logger.handlers) == 3


class TestLogBackupStart:
    """Tests for log_backup_start function."""
    
    def test_logs_start_info(self, temp_log_dir, cleanup_logger):
        """Test that backup start is logged with all info."""
        log_file = temp_log_dir / "test.log"
        logger = setup_logging(
            log_file=log_file,
            error_log_file=temp_log_dir / "test.err",
            level="INFO",
        )
        
        sources = [Path("/Users/test/Projects"), Path("/Users/test/Code")]
        destination = Path("/Volumes/Backup")
        
        log_backup_start(logger, sources, destination)
        
        for handler in logger.handlers:
            handler.flush()
        
        content = log_file.read_text()
        assert "Backup started" in content
        assert "/Users/test/Projects" in content
        assert "/Users/test/Code" in content
        assert "/Volumes/Backup" in content


class TestLogBackupCompletion:
    """Tests for log_backup_completion function."""
    
    def test_logs_completion_stats(self, temp_log_dir, cleanup_logger):
        """Test that backup completion logs statistics."""
        log_file = temp_log_dir / "test.log"
        logger = setup_logging(
            log_file=log_file,
            error_log_file=temp_log_dir / "test.err",
            level="INFO",
        )
        
        log_backup_completion(
            logger,
            duration_seconds=45.5,
            files_transferred=100,
            total_size=1024 * 1024 * 50,  # 50 MB
            snapshot_path=Path("/Volumes/Backup/2025-01-02-120000"),
        )
        
        for handler in logger.handlers:
            handler.flush()
        
        content = log_file.read_text()
        assert "completed successfully" in content
        assert "45.50 seconds" in content
        assert "100" in content
        assert "50.00 MB" in content
        assert "2025-01-02-120000" in content
    
    def test_formats_size_gb(self, temp_log_dir, cleanup_logger):
        """Test size formatting for GB."""
        log_file = temp_log_dir / "test.log"
        logger = setup_logging(
            log_file=log_file,
            error_log_file=temp_log_dir / "test.err",
            level="INFO",
        )
        
        log_backup_completion(
            logger,
            duration_seconds=120,
            files_transferred=1000,
            total_size=1024 * 1024 * 1024 * 2,  # 2 GB
        )
        
        for handler in logger.handlers:
            handler.flush()
        
        content = log_file.read_text()
        assert "2.00 GB" in content
    
    def test_formats_size_kb(self, temp_log_dir, cleanup_logger):
        """Test size formatting for KB."""
        log_file = temp_log_dir / "test.log"
        logger = setup_logging(
            log_file=log_file,
            error_log_file=temp_log_dir / "test.err",
            level="INFO",
        )
        
        log_backup_completion(
            logger,
            duration_seconds=5,
            files_transferred=10,
            total_size=1024 * 500,  # 500 KB
        )
        
        for handler in logger.handlers:
            handler.flush()
        
        content = log_file.read_text()
        assert "500.00 KB" in content
    
    def test_formats_size_bytes(self, temp_log_dir, cleanup_logger):
        """Test size formatting for bytes."""
        log_file = temp_log_dir / "test.log"
        logger = setup_logging(
            log_file=log_file,
            error_log_file=temp_log_dir / "test.err",
            level="INFO",
        )
        
        log_backup_completion(
            logger,
            duration_seconds=1,
            files_transferred=1,
            total_size=500,
        )
        
        for handler in logger.handlers:
            handler.flush()
        
        content = log_file.read_text()
        assert "500 bytes" in content


class TestLogBackupError:
    """Tests for log_backup_error function."""
    
    def test_logs_error(self, temp_log_dir, cleanup_logger):
        """Test that errors are logged."""
        error_file = temp_log_dir / "test.err"
        logger = setup_logging(
            log_file=temp_log_dir / "test.log",
            error_log_file=error_file,
            level="INFO",
        )
        
        error = ValueError("Something went wrong")
        log_backup_error(logger, error)
        
        for handler in logger.handlers:
            handler.flush()
        
        content = error_file.read_text()
        assert "Backup failed" in content
        assert "Something went wrong" in content
    
    def test_logs_error_with_context(self, temp_log_dir, cleanup_logger):
        """Test that errors are logged with context."""
        error_file = temp_log_dir / "test.err"
        logger = setup_logging(
            log_file=temp_log_dir / "test.log",
            error_log_file=error_file,
            level="INFO",
        )
        
        error = OSError("Disk full")
        log_backup_error(logger, error, context="snapshot creation")
        
        for handler in logger.handlers:
            handler.flush()
        
        content = error_file.read_text()
        assert "snapshot creation" in content
        assert "Disk full" in content


class TestLogRsyncOutput:
    """Tests for log_rsync_output function."""
    
    def test_logs_at_debug_level(self, temp_log_dir, cleanup_logger):
        """Test that rsync output is logged at DEBUG level."""
        log_file = temp_log_dir / "test.log"
        logger = setup_logging(
            log_file=log_file,
            error_log_file=temp_log_dir / "test.err",
            level="DEBUG",
        )
        
        log_rsync_output(logger, "sending incremental file list\nfile1.txt\nfile2.txt")
        
        for handler in logger.handlers:
            handler.flush()
        
        content = log_file.read_text()
        assert "rsync: sending incremental file list" in content
        assert "rsync: file1.txt" in content
        assert "rsync: file2.txt" in content
    
    def test_not_logged_at_info_level(self, temp_log_dir, cleanup_logger):
        """Test that rsync output is not logged at INFO level."""
        log_file = temp_log_dir / "test.log"
        logger = setup_logging(
            log_file=log_file,
            error_log_file=temp_log_dir / "test.err",
            level="INFO",
        )
        
        log_rsync_output(logger, "sending incremental file list")
        
        for handler in logger.handlers:
            handler.flush()
        
        content = log_file.read_text()
        assert "rsync:" not in content
    
    def test_handles_empty_output(self, temp_log_dir, cleanup_logger):
        """Test that empty output doesn't cause issues."""
        log_file = temp_log_dir / "test.log"
        logger = setup_logging(
            log_file=log_file,
            error_log_file=temp_log_dir / "test.err",
            level="DEBUG",
        )
        
        log_rsync_output(logger, "")
        log_rsync_output(logger, "   ")
        
        for handler in logger.handlers:
            handler.flush()
        
        content = log_file.read_text()
        assert "rsync:" not in content
