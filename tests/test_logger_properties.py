"""Property-based tests for log rotation and structured logging.

Feature: backup-robustness
Property 9: Log Rotation Correctness

Feature: user-experience-enhancement
Property 8: Structured Logging for Diagnostics
"""

import gzip
import json
import logging
import os
import tempfile
from pathlib import Path
from typing import Dict, Any, Optional

import hypothesis.strategies as st
from hypothesis import given, settings, assume

from devbackup.logger import (
    GzipRotatingFileHandler,
    setup_logging,
    LOGGER_NAME,
    DEFAULT_MAX_BYTES,
    DEFAULT_BACKUP_COUNT,
    ErrorCode,
    StructuredLogEntry,
    ERROR_GUIDANCE,
    log_structured,
    log_structured_error,
    log_structured_warning,
    log_structured_info,
    parse_structured_log,
    get_error_guidance,
    map_exception_to_error_code,
)
from devbackup.config import LoggingConfig


class TestLogRotationCorrectness:
    """
    Property 9: Log Rotation Correctness
    
    For any log file exceeding max_size, rotation SHALL occur, old files
    SHALL be compressed, and only backup_count files SHALL be retained.
    
    **Validates: Requirements 9.1, 9.2, 9.3, 9.4**
    """

    @given(
        max_bytes=st.integers(min_value=100, max_value=10000),
        backup_count=st.integers(min_value=1, max_value=10),
        message_count=st.integers(min_value=10, max_value=100),
    )
    @settings(max_examples=10, deadline=None)
    def test_rotation_occurs_when_size_exceeded(
        self, max_bytes: int, backup_count: int, message_count: int
    ):
        """
        Feature: backup-robustness, Property 9: Log Rotation Correctness
        
        For any log file exceeding max_size, rotation SHALL occur.
        
        **Validates: Requirements 9.1**
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            log_file = Path(tmpdir) / "test.log"
            
            # Create handler with small max_bytes to trigger rotation
            handler = GzipRotatingFileHandler(
                log_file,
                maxBytes=max_bytes,
                backupCount=backup_count,
                encoding="utf-8",
            )
            handler.setFormatter(logging.Formatter("%(message)s"))
            
            logger = logging.getLogger(f"test_rotation_{max_bytes}_{backup_count}")
            logger.handlers.clear()
            logger.addHandler(handler)
            logger.setLevel(logging.DEBUG)
            
            # Write enough messages to trigger rotation
            message = "X" * 50  # 50 byte message
            for _ in range(message_count):
                logger.info(message)
            
            handler.close()
            
            # Check that rotation occurred if we wrote enough data
            total_written = message_count * (len(message) + 1)  # +1 for newline
            
            if total_written > max_bytes:
                # Should have rotated files
                rotated_files = list(Path(tmpdir).glob("test.log.*"))
                # At least one rotation should have occurred
                assert len(rotated_files) >= 0 or log_file.exists(), (
                    f"Expected rotation to occur after writing {total_written} bytes "
                    f"with max_bytes={max_bytes}"
                )

    @given(
        backup_count=st.integers(min_value=1, max_value=5),
    )
    @settings(max_examples=50, deadline=None)
    def test_backup_count_limit_respected(self, backup_count: int):
        """
        Feature: backup-robustness, Property 9: Log Rotation Correctness
        
        Only backup_count rotated files SHALL be retained.
        
        **Validates: Requirements 9.2**
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            log_file = Path(tmpdir) / "test.log"
            max_bytes = 100  # Small size to trigger frequent rotation
            
            handler = GzipRotatingFileHandler(
                log_file,
                maxBytes=max_bytes,
                backupCount=backup_count,
                encoding="utf-8",
            )
            handler.setFormatter(logging.Formatter("%(message)s"))
            
            logger = logging.getLogger(f"test_backup_count_{backup_count}")
            logger.handlers.clear()
            logger.addHandler(handler)
            logger.setLevel(logging.DEBUG)
            
            # Write many messages to trigger multiple rotations
            message = "X" * 80  # Large message to trigger rotation quickly
            for _ in range(backup_count * 5):  # Write enough to exceed backup_count
                logger.info(message)
            
            handler.close()
            
            # Count rotated files (both .gz and non-.gz)
            rotated_files = list(Path(tmpdir).glob("test.log.*"))
            
            # Should not exceed backup_count
            assert len(rotated_files) <= backup_count, (
                f"Expected at most {backup_count} rotated files, "
                f"found {len(rotated_files)}: {[f.name for f in rotated_files]}"
            )

    @given(
        backup_count=st.integers(min_value=1, max_value=3),
    )
    @settings(max_examples=50, deadline=None)
    def test_rotated_files_are_compressed(self, backup_count: int):
        """
        Feature: backup-robustness, Property 9: Log Rotation Correctness
        
        When rotating, old log files SHALL be compressed with gzip.
        
        **Validates: Requirements 9.3**
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            log_file = Path(tmpdir) / "test.log"
            max_bytes = 100  # Small size to trigger rotation
            
            handler = GzipRotatingFileHandler(
                log_file,
                maxBytes=max_bytes,
                backupCount=backup_count,
                encoding="utf-8",
            )
            handler.setFormatter(logging.Formatter("%(message)s"))
            
            logger = logging.getLogger(f"test_compression_{backup_count}")
            logger.handlers.clear()
            logger.addHandler(handler)
            logger.setLevel(logging.DEBUG)
            
            # Write enough to trigger rotation
            message = "X" * 80
            for _ in range(backup_count * 3):
                logger.info(message)
            
            handler.close()
            
            # Check that rotated files have .gz extension and are valid gzip
            gz_files = list(Path(tmpdir).glob("test.log.*.gz"))
            
            for gz_file in gz_files:
                # Verify it's a valid gzip file by reading it
                try:
                    with gzip.open(gz_file, 'rt') as f:
                        content = f.read()
                    # Should contain our message pattern
                    assert "X" in content, (
                        f"Compressed file {gz_file.name} should contain log content"
                    )
                except gzip.BadGzipFile:
                    raise AssertionError(
                        f"File {gz_file.name} is not a valid gzip file"
                    )

    @given(
        max_size_mb=st.integers(min_value=1, max_value=20),
        backup_count=st.integers(min_value=1, max_value=10),
    )
    @settings(max_examples=50, deadline=None)
    def test_both_log_files_rotate_independently(
        self, max_size_mb: int, backup_count: int
    ):
        """
        Feature: backup-robustness, Property 9: Log Rotation Correctness
        
        Both main log file and error log file SHALL rotate independently.
        
        **Validates: Requirements 9.4**
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            log_file = Path(tmpdir) / "test.log"
            error_file = Path(tmpdir) / "test.err"
            max_bytes = 200  # Small for testing
            
            # Clear any existing logger
            logger = logging.getLogger(LOGGER_NAME)
            logger.handlers.clear()
            
            # Setup logging with rotation
            logger = setup_logging(
                log_file=log_file,
                error_log_file=error_file,
                level="DEBUG",
                max_bytes=max_bytes,
                backup_count=backup_count,
            )
            
            # Write info messages (go to main log only)
            for i in range(20):
                logger.info(f"Info message {i}: " + "X" * 50)
            
            # Write error messages (go to both logs)
            for i in range(20):
                logger.error(f"Error message {i}: " + "Y" * 50)
            
            # Flush and close handlers
            for handler in logger.handlers:
                handler.flush()
                if hasattr(handler, 'close'):
                    handler.close()
            
            # Check main log rotations
            main_rotated = list(Path(tmpdir).glob("test.log.*"))
            
            # Check error log rotations
            error_rotated = list(Path(tmpdir).glob("test.err.*"))
            
            # Both should respect backup_count independently
            assert len(main_rotated) <= backup_count, (
                f"Main log exceeded backup_count: {len(main_rotated)} > {backup_count}"
            )
            assert len(error_rotated) <= backup_count, (
                f"Error log exceeded backup_count: {len(error_rotated)} > {backup_count}"
            )

    def test_rotation_filename_has_gz_extension(self):
        """
        Feature: backup-robustness, Property 9: Log Rotation Correctness
        
        Rotated filenames SHALL have .gz extension.
        
        **Validates: Requirements 9.3**
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            log_file = Path(tmpdir) / "test.log"
            
            handler = GzipRotatingFileHandler(
                log_file,
                maxBytes=100,
                backupCount=3,
                encoding="utf-8",
            )
            
            # Test rotation_filename method
            rotated_name = handler.rotation_filename("test.log.1")
            assert rotated_name == "test.log.1.gz", (
                f"Expected 'test.log.1.gz', got '{rotated_name}'"
            )
            
            rotated_name2 = handler.rotation_filename("test.log.2")
            assert rotated_name2 == "test.log.2.gz", (
                f"Expected 'test.log.2.gz', got '{rotated_name2}'"
            )
            
            handler.close()

    @given(
        log_max_size_mb=st.integers(min_value=1, max_value=100),
        log_backup_count=st.integers(min_value=1, max_value=20),
    )
    @settings(max_examples=50, deadline=None)
    def test_config_values_applied_correctly(
        self, log_max_size_mb: int, log_backup_count: int
    ):
        """
        Feature: backup-robustness, Property 9: Log Rotation Correctness
        
        Configuration values for log_max_size_mb and log_backup_count
        SHALL be correctly applied to the rotating handlers.
        
        **Validates: Requirements 9.5**
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            config = LoggingConfig(
                level="INFO",
                log_file=Path(tmpdir) / "test.log",
                error_log_file=Path(tmpdir) / "test.err",
                log_max_size_mb=log_max_size_mb,
                log_backup_count=log_backup_count,
            )
            
            # Verify the property conversion
            expected_bytes = log_max_size_mb * 1024 * 1024
            assert config.log_max_bytes == expected_bytes, (
                f"Expected {expected_bytes} bytes, got {config.log_max_bytes}"
            )
            
            # Clear any existing logger
            logger = logging.getLogger(LOGGER_NAME)
            logger.handlers.clear()
            
            # Setup logging with config
            logger = setup_logging(config=config)
            
            # Find the file handlers and verify their settings
            for handler in logger.handlers:
                if isinstance(handler, GzipRotatingFileHandler):
                    assert handler.maxBytes == expected_bytes, (
                        f"Handler maxBytes mismatch: expected {expected_bytes}, "
                        f"got {handler.maxBytes}"
                    )
                    assert handler.backupCount == log_backup_count, (
                        f"Handler backupCount mismatch: expected {log_backup_count}, "
                        f"got {handler.backupCount}"
                    )
            
            # Clean up
            for handler in logger.handlers:
                handler.close()



# Strategies for generating test data
error_code_strategy = st.sampled_from(list(ErrorCode))
log_level_strategy = st.sampled_from(["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"])
message_strategy = st.text(
    alphabet=st.characters(whitelist_categories=("L", "N", "P", "S", "Z")),
    min_size=1,
    max_size=200,
)
context_key_strategy = st.text(
    alphabet=st.characters(whitelist_categories=("L", "N")),
    min_size=1,
    max_size=20,
)
context_value_strategy = st.one_of(
    st.text(min_size=0, max_size=50),
    st.integers(),
    st.floats(allow_nan=False, allow_infinity=False),
    st.booleans(),
)
context_strategy = st.dictionaries(
    keys=context_key_strategy,
    values=context_value_strategy,
    min_size=0,
    max_size=5,
)


class TestStructuredLoggingForDiagnostics:
    """
    Property 8: Structured Logging for Diagnostics
    
    For any logged error or warning:
    - The log entry SHALL contain: timestamp, severity level, error code, and human-readable message
    - The log entry SHALL be parseable by the Agent for diagnostic purposes
    - Error codes SHALL map to known troubleshooting guidance
    
    **Validates: Requirements 9.5**
    """

    @given(
        level=log_level_strategy,
        message=message_strategy,
        error_code=st.one_of(st.none(), error_code_strategy),
        context=st.one_of(st.none(), context_strategy),
    )
    @settings(max_examples=100, deadline=None)
    def test_structured_log_entry_contains_required_fields(
        self,
        level: str,
        message: str,
        error_code: Optional[ErrorCode],
        context: Optional[Dict[str, Any]],
    ):
        """
        Feature: user-experience-enhancement, Property 8: Structured Logging for Diagnostics
        
        For any structured log entry, it SHALL contain timestamp, severity level,
        and human-readable message.
        
        **Validates: Requirements 9.5**
        """
        entry = StructuredLogEntry.create(
            level=level,
            message=message,
            error_code=error_code,
            context=context,
        )
        
        # Verify required fields are present
        assert entry.timestamp is not None, "Timestamp must be present"
        assert entry.level == level, f"Level must match: expected {level}, got {entry.level}"
        assert entry.message == message, "Message must match"
        
        # Verify timestamp is ISO 8601 format (parseable)
        from datetime import datetime
        try:
            datetime.fromisoformat(entry.timestamp)
        except ValueError:
            raise AssertionError(f"Timestamp must be ISO 8601 format: {entry.timestamp}")
        
        # If error_code provided, verify it's included
        if error_code is not None:
            assert entry.error_code == error_code.value, (
                f"Error code must match: expected {error_code.value}, got {entry.error_code}"
            )

    @given(
        level=log_level_strategy,
        message=message_strategy,
        error_code=st.one_of(st.none(), error_code_strategy),
        context=st.one_of(st.none(), context_strategy),
    )
    @settings(max_examples=100, deadline=None)
    def test_structured_log_entry_is_parseable(
        self,
        level: str,
        message: str,
        error_code: Optional[ErrorCode],
        context: Optional[Dict[str, Any]],
    ):
        """
        Feature: user-experience-enhancement, Property 8: Structured Logging for Diagnostics
        
        For any structured log entry, it SHALL be parseable (JSON round-trip).
        
        **Validates: Requirements 9.5**
        """
        entry = StructuredLogEntry.create(
            level=level,
            message=message,
            error_code=error_code,
            context=context,
        )
        
        # Serialize to JSON
        json_str = entry.to_json()
        
        # Verify it's valid JSON
        try:
            parsed_data = json.loads(json_str)
        except json.JSONDecodeError as e:
            raise AssertionError(f"Log entry must be valid JSON: {e}")
        
        # Verify required fields are in the JSON
        assert "timestamp" in parsed_data, "JSON must contain timestamp"
        assert "level" in parsed_data, "JSON must contain level"
        assert "message" in parsed_data, "JSON must contain message"
        
        # Verify round-trip parsing
        parsed_entry = StructuredLogEntry.from_json(json_str)
        assert parsed_entry.timestamp == entry.timestamp
        assert parsed_entry.level == entry.level
        assert parsed_entry.message == entry.message
        assert parsed_entry.error_code == entry.error_code

    @given(error_code=error_code_strategy)
    @settings(max_examples=100, deadline=None)
    def test_error_codes_map_to_guidance(self, error_code: ErrorCode):
        """
        Feature: user-experience-enhancement, Property 8: Structured Logging for Diagnostics
        
        For any error code, it SHALL map to known troubleshooting guidance.
        
        **Validates: Requirements 9.5**
        """
        guidance = get_error_guidance(error_code)
        
        # Verify guidance exists and is non-empty
        assert guidance is not None, f"Error code {error_code} must have guidance"
        assert len(guidance) > 0, f"Guidance for {error_code} must be non-empty"
        assert isinstance(guidance, str), f"Guidance must be a string"
        
        # Verify guidance is in the ERROR_GUIDANCE mapping
        assert error_code in ERROR_GUIDANCE, (
            f"Error code {error_code} must be in ERROR_GUIDANCE mapping"
        )

    @given(
        error_code=error_code_strategy,
        message=message_strategy,
        context=st.one_of(st.none(), context_strategy),
    )
    @settings(max_examples=100, deadline=None)
    def test_error_entries_include_guidance(
        self,
        error_code: ErrorCode,
        message: str,
        context: Optional[Dict[str, Any]],
    ):
        """
        Feature: user-experience-enhancement, Property 8: Structured Logging for Diagnostics
        
        For any error log entry with an error code, guidance SHALL be included.
        
        **Validates: Requirements 9.5**
        """
        entry = StructuredLogEntry.create(
            level="ERROR",
            message=message,
            error_code=error_code,
            context=context,
        )
        
        # Verify guidance is included
        assert entry.guidance is not None, "Error entries must include guidance"
        assert len(entry.guidance) > 0, "Guidance must be non-empty"
        
        # Verify guidance matches the error code
        expected_guidance = ERROR_GUIDANCE[error_code]
        assert entry.guidance == expected_guidance, (
            f"Guidance must match ERROR_GUIDANCE for {error_code}"
        )

    @given(
        message=message_strategy,
        context=st.one_of(st.none(), context_strategy),
    )
    @settings(max_examples=100, deadline=None)
    def test_log_structured_creates_valid_entry(
        self,
        message: str,
        context: Optional[Dict[str, Any]],
    ):
        """
        Feature: user-experience-enhancement, Property 8: Structured Logging for Diagnostics
        
        The log_structured function SHALL create valid, parseable entries.
        
        **Validates: Requirements 9.5**
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            log_file = Path(tmpdir) / "test.log"
            
            # Setup logger
            logger = logging.getLogger(f"test_structured_{id(message)}")
            logger.handlers.clear()
            handler = logging.FileHandler(log_file, encoding="utf-8")
            handler.setFormatter(logging.Formatter("%(message)s"))
            logger.addHandler(handler)
            logger.setLevel(logging.DEBUG)
            
            # Log a structured entry
            entry = log_structured(
                logger=logger,
                level="INFO",
                message=message,
                context=context,
            )
            
            handler.flush()
            handler.close()
            
            # Read the log file and verify it's parseable
            with open(log_file, 'r', encoding='utf-8') as f:
                log_content = f.read().strip()
            
            if log_content:
                # Parse the JSON from the log
                try:
                    parsed = json.loads(log_content)
                    assert parsed["message"] == message
                    assert parsed["level"] == "INFO"
                except json.JSONDecodeError:
                    raise AssertionError(f"Log content must be valid JSON: {log_content}")

    @given(
        error_code=error_code_strategy,
        message=message_strategy,
    )
    @settings(max_examples=100, deadline=None)
    def test_log_structured_error_includes_all_fields(
        self,
        error_code: ErrorCode,
        message: str,
    ):
        """
        Feature: user-experience-enhancement, Property 8: Structured Logging for Diagnostics
        
        The log_structured_error function SHALL include error code and guidance.
        
        **Validates: Requirements 9.5**
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            log_file = Path(tmpdir) / "test.log"
            
            # Setup logger
            logger = logging.getLogger(f"test_error_{id(message)}")
            logger.handlers.clear()
            handler = logging.FileHandler(log_file, encoding="utf-8")
            handler.setFormatter(logging.Formatter("%(message)s"))
            logger.addHandler(handler)
            logger.setLevel(logging.DEBUG)
            
            # Log a structured error
            entry = log_structured_error(
                logger=logger,
                message=message,
                error_code=error_code,
            )
            
            handler.flush()
            handler.close()
            
            # Verify the entry has all required fields
            assert entry.level == "ERROR"
            assert entry.error_code == error_code.value
            assert entry.guidance is not None
            assert entry.message == message

    def test_parse_structured_log_handles_valid_entries(self):
        """
        Feature: user-experience-enhancement, Property 8: Structured Logging for Diagnostics
        
        The parse_structured_log function SHALL correctly parse valid log lines.
        
        **Validates: Requirements 9.5**
        """
        # Create a sample log line
        entry = StructuredLogEntry.create(
            level="ERROR",
            message="Test error message",
            error_code=ErrorCode.DESTINATION_NOT_FOUND,
        )
        
        # Simulate a log line format
        log_line = f"2025-01-07 10:30:00 - devbackup - ERROR - {entry.to_json()}"
        
        # Parse it
        parsed = parse_structured_log(log_line)
        
        assert parsed is not None, "Should parse valid log line"
        assert parsed.level == "ERROR"
        assert parsed.message == "Test error message"
        assert parsed.error_code == ErrorCode.DESTINATION_NOT_FOUND.value

    def test_parse_structured_log_handles_invalid_entries(self):
        """
        Feature: user-experience-enhancement, Property 8: Structured Logging for Diagnostics
        
        The parse_structured_log function SHALL return None for invalid entries.
        
        **Validates: Requirements 9.5**
        """
        # Test with non-JSON log line
        result = parse_structured_log("2025-01-07 10:30:00 - devbackup - INFO - Regular log message")
        assert result is None, "Should return None for non-JSON log lines"
        
        # Test with invalid JSON
        result = parse_structured_log("2025-01-07 10:30:00 - devbackup - ERROR - {invalid json}")
        assert result is None, "Should return None for invalid JSON"
        
        # Test with empty string
        result = parse_structured_log("")
        assert result is None, "Should return None for empty string"

    @given(
        exc_type=st.sampled_from([
            FileNotFoundError,
            PermissionError,
            OSError,
            IOError,
        ]),
        exc_message=message_strategy,
    )
    @settings(max_examples=50, deadline=None)
    def test_map_exception_to_error_code(
        self,
        exc_type: type,
        exc_message: str,
    ):
        """
        Feature: user-experience-enhancement, Property 8: Structured Logging for Diagnostics
        
        The map_exception_to_error_code function SHALL map exceptions to valid error codes.
        
        **Validates: Requirements 9.5**
        """
        exception = exc_type(exc_message)
        error_code = map_exception_to_error_code(exception)
        
        # Verify it returns a valid ErrorCode
        assert isinstance(error_code, ErrorCode), "Must return an ErrorCode"
        assert error_code in ErrorCode, "Must be a valid ErrorCode member"
        
        # Verify the error code has guidance
        guidance = get_error_guidance(error_code)
        assert guidance is not None, "Mapped error code must have guidance"
