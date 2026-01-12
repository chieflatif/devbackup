"""Property-based tests for scheduler management.

Feature: macos-incremental-backup
Property 9: Scheduler Interval Consistency
"""

import plistlib
import tempfile
from pathlib import Path

import hypothesis.strategies as st
from hypothesis import given, settings, assume

from devbackup.scheduler import (
    Scheduler,
    SchedulerType,
    parse_launchd_plist,
    parse_cron_interval_from_entry,
)


class TestSchedulerIntervalConsistency:
    """
    Property 9: Scheduler Interval Consistency
    
    For any scheduler configuration with interval I seconds, the generated
    scheduler config (plist or crontab) SHALL specify interval I.
    
    **Validates: Requirements 6.3**
    """

    @given(interval_seconds=st.integers(min_value=60, max_value=86400))
    @settings(max_examples=10, deadline=None)
    def test_launchd_interval_consistency(self, interval_seconds: int):
        """
        Feature: macos-incremental-backup, Property 9: Scheduler Interval Consistency
        
        For any interval I seconds, the generated launchd plist SHALL specify
        StartInterval = I.
        
        **Validates: Requirements 6.3**
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            scheduler = Scheduler(
                scheduler_type=SchedulerType.LAUNCHD,
                interval_seconds=interval_seconds,
                devbackup_command=Path("/usr/local/bin/devbackup"),
                log_file=Path(tmpdir) / "devbackup.log",
                error_log_file=Path(tmpdir) / "devbackup.err",
            )
            
            # Generate plist
            plist_dict = scheduler._create_launchd_plist()
            
            # Verify interval matches exactly
            assert plist_dict["StartInterval"] == interval_seconds, (
                f"Expected StartInterval={interval_seconds}, "
                f"got {plist_dict['StartInterval']}"
            )
            
            # Also verify XML round-trip preserves interval
            xml_content = scheduler._create_launchd_plist_xml()
            parsed = plistlib.loads(xml_content.encode("utf-8"))
            assert parsed["StartInterval"] == interval_seconds, (
                f"XML round-trip failed: expected {interval_seconds}, "
                f"got {parsed['StartInterval']}"
            )

    @given(interval_seconds=st.integers(min_value=60, max_value=86400))
    @settings(max_examples=10, deadline=None)
    def test_launchd_plist_file_interval_consistency(self, interval_seconds: int):
        """
        Feature: macos-incremental-backup, Property 9: Scheduler Interval Consistency
        
        For any interval I seconds, writing and reading a launchd plist file
        SHALL preserve the interval value.
        
        **Validates: Requirements 6.3**
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            plist_path = Path(tmpdir) / "test.plist"
            
            scheduler = Scheduler(
                scheduler_type=SchedulerType.LAUNCHD,
                interval_seconds=interval_seconds,
                devbackup_command=Path("/usr/local/bin/devbackup"),
                log_file=Path(tmpdir) / "devbackup.log",
                error_log_file=Path(tmpdir) / "devbackup.err",
            )
            
            # Write plist to file
            plist_dict = scheduler._create_launchd_plist()
            with open(plist_path, "wb") as f:
                plistlib.dump(plist_dict, f)
            
            # Read back using helper function
            read_interval = parse_launchd_plist(plist_path)
            
            assert read_interval == interval_seconds, (
                f"File round-trip failed: expected {interval_seconds}, "
                f"got {read_interval}"
            )

    @given(interval_minutes=st.integers(min_value=1, max_value=59))
    @settings(max_examples=10, deadline=None)
    def test_cron_minute_interval_consistency(self, interval_minutes: int):
        """
        Feature: macos-incremental-backup, Property 9: Scheduler Interval Consistency
        
        For any interval in minutes (1-59), the generated cron entry SHALL
        specify */N minute pattern that can be parsed back to the same interval.
        
        **Validates: Requirements 6.3**
        """
        interval_seconds = interval_minutes * 60
        
        scheduler = Scheduler(
            scheduler_type=SchedulerType.CRON,
            interval_seconds=interval_seconds,
            devbackup_command=Path("/usr/local/bin/devbackup"),
        )
        
        # Generate cron entry
        cron_entry = scheduler._create_cron_entry()
        
        # Parse interval back
        parsed_interval = parse_cron_interval_from_entry(cron_entry)
        
        assert parsed_interval == interval_seconds, (
            f"Cron round-trip failed for {interval_minutes} minutes: "
            f"expected {interval_seconds}s, got {parsed_interval}s. "
            f"Entry: {cron_entry}"
        )

    @given(interval_hours=st.integers(min_value=1, max_value=23))
    @settings(max_examples=10, deadline=None)
    def test_cron_hour_interval_consistency(self, interval_hours: int):
        """
        Feature: macos-incremental-backup, Property 9: Scheduler Interval Consistency
        
        For any interval in hours (1-23), the generated cron entry SHALL
        specify a pattern that can be parsed back to the same interval.
        
        **Validates: Requirements 6.3**
        """
        interval_seconds = interval_hours * 3600
        
        scheduler = Scheduler(
            scheduler_type=SchedulerType.CRON,
            interval_seconds=interval_seconds,
            devbackup_command=Path("/usr/local/bin/devbackup"),
        )
        
        # Generate cron entry
        cron_entry = scheduler._create_cron_entry()
        
        # Parse interval back
        parsed_interval = parse_cron_interval_from_entry(cron_entry)
        
        assert parsed_interval == interval_seconds, (
            f"Cron round-trip failed for {interval_hours} hours: "
            f"expected {interval_seconds}s, got {parsed_interval}s. "
            f"Entry: {cron_entry}"
        )

    def test_cron_daily_interval_consistency(self):
        """
        Feature: macos-incremental-backup, Property 9: Scheduler Interval Consistency
        
        For daily interval (86400 seconds), the generated cron entry SHALL
        specify "0 0 * * *" pattern.
        
        **Validates: Requirements 6.3**
        """
        interval_seconds = 86400  # 24 hours
        
        scheduler = Scheduler(
            scheduler_type=SchedulerType.CRON,
            interval_seconds=interval_seconds,
            devbackup_command=Path("/usr/local/bin/devbackup"),
        )
        
        # Generate cron entry
        cron_entry = scheduler._create_cron_entry()
        
        # Should be daily pattern
        assert "0 0 * * *" in cron_entry, (
            f"Expected daily pattern '0 0 * * *' in entry: {cron_entry}"
        )
        
        # Parse interval back
        parsed_interval = parse_cron_interval_from_entry(cron_entry)
        
        assert parsed_interval == interval_seconds, (
            f"Cron round-trip failed for daily: "
            f"expected {interval_seconds}s, got {parsed_interval}s"
        )
