"""Property-based tests for configuration management.

Feature: macos-incremental-backup
"""

from pathlib import Path

import hypothesis.strategies as st
from hypothesis import given, settings

from devbackup.config import (
    Configuration,
    LoggingConfig,
    MCPConfig,
    RetentionConfig,
    SchedulerConfig,
    format_config,
    parse_config_string,
)


# Strategies for generating valid configuration values
valid_scheduler_types = st.sampled_from(["launchd", "cron"])
valid_log_levels = st.sampled_from(["DEBUG", "INFO", "ERROR"])

# Generate valid paths (non-empty, no null bytes)
valid_path_str = st.text(
    alphabet=st.characters(
        whitelist_categories=("L", "N", "P", "S"),
        blacklist_characters="\x00\n\r",
    ),
    min_size=1,
    max_size=50,
).filter(lambda s: s.strip() and not s.startswith("-"))

# Generate valid exclude patterns
valid_exclude_pattern = st.text(
    alphabet=st.characters(
        whitelist_categories=("L", "N", "P"),
        whitelist_characters="*./",
        blacklist_characters="\x00\n\r\"'\\",
    ),
    min_size=1,
    max_size=30,
).filter(lambda s: s.strip())


@st.composite
def scheduler_configs(draw):
    """Generate valid SchedulerConfig instances."""
    return SchedulerConfig(
        type=draw(valid_scheduler_types),
        interval_seconds=draw(st.integers(min_value=60, max_value=86400)),
    )


@st.composite
def retention_configs(draw):
    """Generate valid RetentionConfig instances."""
    return RetentionConfig(
        hourly=draw(st.integers(min_value=1, max_value=168)),
        daily=draw(st.integers(min_value=1, max_value=30)),
        weekly=draw(st.integers(min_value=1, max_value=52)),
    )


@st.composite
def logging_configs(draw):
    """Generate valid LoggingConfig instances."""
    return LoggingConfig(
        level=draw(valid_log_levels),
        log_file=Path("/tmp") / draw(valid_path_str),
        error_log_file=Path("/tmp") / draw(valid_path_str),
    )


@st.composite
def mcp_configs(draw):
    """Generate valid MCPConfig instances."""
    return MCPConfig(
        enabled=draw(st.booleans()),
        port=draw(st.integers(min_value=0, max_value=65535)),
    )


@st.composite
def configurations(draw):
    """Generate valid Configuration instances."""
    return Configuration(
        backup_destination=Path("/tmp/backup") / draw(valid_path_str),
        source_directories=[
            Path("/tmp/src") / draw(valid_path_str)
            for _ in range(draw(st.integers(min_value=1, max_value=3)))
        ],
        exclude_patterns=draw(
            st.lists(valid_exclude_pattern, min_size=0, max_size=5)
        ),
        scheduler=draw(scheduler_configs()),
        retention=draw(retention_configs()),
        logging=draw(logging_configs()),
        mcp=draw(mcp_configs()),
    )


class TestConfigurationRoundTrip:
    """
    Property 1: Configuration Round-Trip
    
    For any valid Configuration object, formatting it to TOML and then
    parsing the result SHALL produce an equivalent Configuration object.
    
    **Validates: Requirements 1.6, 1.7**
    """

    @given(config=configurations())
    @settings(max_examples=100)
    def test_round_trip_preserves_configuration(self, config: Configuration):
        """
        Feature: macos-incremental-backup, Property 1: Configuration Round-Trip
        
        For any valid Configuration object, parse(format(config)) == config
        """
        # Format to TOML string
        toml_str = format_config(config)
        
        # Parse back to Configuration
        parsed = parse_config_string(toml_str)
        
        # Verify equivalence
        assert parsed.backup_destination == config.backup_destination
        assert parsed.source_directories == config.source_directories
        assert parsed.exclude_patterns == config.exclude_patterns
        
        # Scheduler config
        assert parsed.scheduler.type == config.scheduler.type
        assert parsed.scheduler.interval_seconds == config.scheduler.interval_seconds
        
        # Retention config
        assert parsed.retention.hourly == config.retention.hourly
        assert parsed.retention.daily == config.retention.daily
        assert parsed.retention.weekly == config.retention.weekly
        
        # Logging config
        assert parsed.logging.level == config.logging.level
        assert parsed.logging.log_file == config.logging.log_file
        assert parsed.logging.error_log_file == config.logging.error_log_file
        
        # MCP config
        assert parsed.mcp.enabled == config.mcp.enabled
        assert parsed.mcp.port == config.mcp.port



class TestConfigurationMissingKeyDetection:
    """
    Property 2: Configuration Missing Key Detection
    
    For any TOML configuration with a required key removed, the Config_Parser
    SHALL raise a ConfigurationError that identifies the missing key.
    
    **Validates: Requirements 1.3**
    """

    @given(config=configurations(), key=st.sampled_from(["backup_destination", "source_directories"]))
    @settings(max_examples=100)
    def test_missing_required_key_raises_error(self, config: Configuration, key: str):
        """
        Feature: macos-incremental-backup, Property 2: Configuration Missing Key Detection
        
        For any valid config with a required key removed, parsing raises ConfigurationError
        containing the missing key name.
        """
        from devbackup.config import ConfigurationError
        import tomllib
        
        # Format to TOML and parse it back to dict to manipulate safely
        toml_str = format_config(config)
        data = tomllib.loads(toml_str)
        
        # Remove the required key from the main section
        if key in data.get("main", {}):
            del data["main"][key]
        
        # Reconstruct TOML without the key
        # Build a minimal valid TOML that's missing the required key
        lines = ["[main]"]
        main_data = data.get("main", {})
        
        if key != "backup_destination" and "backup_destination" in main_data:
            lines.append(f'backup_destination = "{_escape_toml_string(str(main_data["backup_destination"]))}"')
        
        if key != "source_directories" and "source_directories" in main_data:
            lines.append("source_directories = [")
            for src in main_data["source_directories"]:
                lines.append(f'    "{_escape_toml_string(str(src))}",')
            lines.append("]")
        
        modified_toml = "\n".join(lines)
        
        # Parsing should raise ConfigurationError mentioning the missing key
        try:
            parse_config_string(modified_toml)
            assert False, f"Expected ConfigurationError for missing key '{key}'"
        except ConfigurationError as e:
            assert key in str(e), f"Error message should mention missing key '{key}': {e}"


def _escape_toml_string(s: str) -> str:
    """Helper to escape strings for TOML (duplicated for test use)."""
    return s.replace("\\", "\\\\").replace('"', '\\"')


class TestConfigurationTypeValidation:
    """
    Property 3: Configuration Type Validation
    
    For any TOML configuration with a value replaced by an incompatible type,
    the Config_Parser SHALL raise a ValidationError.
    
    **Validates: Requirements 1.4**
    """

    @given(config=configurations())
    @settings(max_examples=100)
    def test_wrong_type_for_backup_destination_raises_error(self, config: Configuration):
        """
        Feature: macos-incremental-backup, Property 3: Configuration Type Validation
        
        Replacing backup_destination (string) with an integer raises ValidationError.
        """
        from devbackup.config import ValidationError
        
        # Format to TOML
        toml_str = format_config(config)
        
        # Replace backup_destination string with an integer
        lines = toml_str.split("\n")
        modified_lines = []
        for line in lines:
            if line.strip().startswith("backup_destination ="):
                modified_lines.append("backup_destination = 12345")
            else:
                modified_lines.append(line)
        
        modified_toml = "\n".join(modified_lines)
        
        # Parsing should raise ValidationError
        try:
            parse_config_string(modified_toml)
            assert False, "Expected ValidationError for wrong type"
        except ValidationError as e:
            assert "backup_destination" in str(e)
            assert "str" in str(e).lower() or "string" in str(e).lower()

    @given(config=configurations())
    @settings(max_examples=100)
    def test_wrong_type_for_source_directories_raises_error(self, config: Configuration):
        """
        Feature: macos-incremental-backup, Property 3: Configuration Type Validation
        
        Replacing source_directories (list) with a string raises ValidationError.
        """
        from devbackup.config import ValidationError
        
        # Build a minimal TOML with source_directories as wrong type
        toml_str = f'''[main]
backup_destination = "{_escape_toml_string(str(config.backup_destination))}"
source_directories = "not_a_list"
'''
        
        # Parsing should raise ValidationError
        try:
            parse_config_string(toml_str)
            assert False, "Expected ValidationError for wrong type"
        except ValidationError as e:
            assert "source_directories" in str(e)

    @given(config=configurations())
    @settings(max_examples=100)
    def test_wrong_type_for_interval_seconds_raises_error(self, config: Configuration):
        """
        Feature: macos-incremental-backup, Property 3: Configuration Type Validation
        
        Replacing interval_seconds (int) with a string raises ValidationError.
        """
        from devbackup.config import ValidationError
        
        # Format to TOML
        toml_str = format_config(config)
        
        # Replace interval_seconds integer with a string
        lines = toml_str.split("\n")
        modified_lines = []
        for line in lines:
            if line.strip().startswith("interval_seconds ="):
                modified_lines.append('interval_seconds = "not_an_int"')
            else:
                modified_lines.append(line)
        
        modified_toml = "\n".join(modified_lines)
        
        # Parsing should raise ValidationError
        try:
            parse_config_string(modified_toml)
            assert False, "Expected ValidationError for wrong type"
        except ValidationError as e:
            assert "interval_seconds" in str(e)
