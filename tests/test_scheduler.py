"""Tests for scheduler management.

Feature: macos-incremental-backup
"""

import os
import plistlib
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from devbackup.scheduler import (
    Scheduler,
    SchedulerError,
    SchedulerType,
    parse_launchd_plist,
    parse_cron_interval_from_entry,
)


class TestLaunchdPlistGeneration:
    """Unit tests for launchd plist generation."""

    def test_create_launchd_plist_contains_required_keys(self):
        """Test that generated plist contains all required keys."""
        with tempfile.TemporaryDirectory() as tmpdir:
            log_file = Path(tmpdir) / "logs" / "devbackup.log"
            error_log_file = Path(tmpdir) / "logs" / "devbackup.err"
            
            scheduler = Scheduler(
                scheduler_type=SchedulerType.LAUNCHD,
                interval_seconds=3600,
                devbackup_command=Path("/usr/local/bin/devbackup"),
                log_file=log_file,
                error_log_file=error_log_file,
            )
            
            plist = scheduler._create_launchd_plist()
            
            assert "Label" in plist
            assert plist["Label"] == "com.devbackup"
            assert "ProgramArguments" in plist
            assert "StartInterval" in plist
            assert plist["StartInterval"] == 3600
            assert "RunAtLoad" in plist
            assert plist["RunAtLoad"] is True
            assert "StandardOutPath" in plist
            assert "StandardErrorPath" in plist

    def test_create_launchd_plist_with_custom_interval(self):
        """Test plist generation with custom interval."""
        with tempfile.TemporaryDirectory() as tmpdir:
            scheduler = Scheduler(
                scheduler_type=SchedulerType.LAUNCHD,
                interval_seconds=1800,  # 30 minutes
                devbackup_command=Path("/usr/local/bin/devbackup"),
                log_file=Path(tmpdir) / "devbackup.log",
                error_log_file=Path(tmpdir) / "devbackup.err",
            )
            
            plist = scheduler._create_launchd_plist()
            
            assert plist["StartInterval"] == 1800

    def test_create_launchd_plist_xml_is_valid(self):
        """Test that generated XML is valid plist format."""
        with tempfile.TemporaryDirectory() as tmpdir:
            scheduler = Scheduler(
                scheduler_type=SchedulerType.LAUNCHD,
                interval_seconds=3600,
                devbackup_command=Path("/usr/local/bin/devbackup"),
                log_file=Path(tmpdir) / "devbackup.log",
                error_log_file=Path(tmpdir) / "devbackup.err",
            )
            
            xml_content = scheduler._create_launchd_plist_xml()
            
            # Should be parseable as plist
            parsed = plistlib.loads(xml_content.encode("utf-8"))
            assert parsed["Label"] == "com.devbackup"
            assert parsed["StartInterval"] == 3600

    def test_program_arguments_with_direct_command(self):
        """Test program arguments when using devbackup directly."""
        scheduler = Scheduler(
            scheduler_type=SchedulerType.LAUNCHD,
            interval_seconds=3600,
            devbackup_command=Path("/usr/local/bin/devbackup"),
        )
        
        args = scheduler._get_program_arguments()
        
        assert args == ["/usr/local/bin/devbackup", "run"]


class TestLaunchdInstallUninstall:
    """Unit tests for launchd install/uninstall operations."""

    def test_is_launchd_installed_returns_false_when_no_plist(self):
        """Test is_installed returns False when plist doesn't exist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Use a custom plist path that doesn't exist
            scheduler = Scheduler(
                scheduler_type=SchedulerType.LAUNCHD,
                interval_seconds=3600,
                devbackup_command=Path("/usr/local/bin/devbackup"),
            )
            # Override PLIST_PATH for testing
            scheduler.PLIST_PATH = Path(tmpdir) / "nonexistent.plist"
            
            assert scheduler._is_launchd_installed() is False

    def test_is_launchd_installed_returns_true_when_plist_exists(self):
        """Test is_installed returns True when plist exists."""
        with tempfile.TemporaryDirectory() as tmpdir:
            plist_path = Path(tmpdir) / "com.devbackup.plist"
            
            # Create a dummy plist
            plist_path.write_text("dummy")
            
            scheduler = Scheduler(
                scheduler_type=SchedulerType.LAUNCHD,
                interval_seconds=3600,
                devbackup_command=Path("/usr/local/bin/devbackup"),
            )
            scheduler.PLIST_PATH = plist_path
            
            assert scheduler._is_launchd_installed() is True

    def test_get_launchd_status_when_not_installed(self):
        """Test get_status returns correct info when not installed."""
        with tempfile.TemporaryDirectory() as tmpdir:
            scheduler = Scheduler(
                scheduler_type=SchedulerType.LAUNCHD,
                interval_seconds=3600,
                devbackup_command=Path("/usr/local/bin/devbackup"),
            )
            scheduler.PLIST_PATH = Path(tmpdir) / "nonexistent.plist"
            
            status = scheduler._get_launchd_status()
            
            assert status["installed"] is False
            assert status["running"] is False
            assert status["interval_seconds"] is None

    def test_get_launchd_status_reads_interval_from_plist(self):
        """Test get_status reads interval from existing plist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            plist_path = Path(tmpdir) / "com.devbackup.plist"
            
            # Create a valid plist
            plist_data = {
                "Label": "com.devbackup",
                "StartInterval": 7200,
                "ProgramArguments": ["/usr/local/bin/devbackup", "run"],
            }
            with open(plist_path, "wb") as f:
                plistlib.dump(plist_data, f)
            
            scheduler = Scheduler(
                scheduler_type=SchedulerType.LAUNCHD,
                interval_seconds=3600,
                devbackup_command=Path("/usr/local/bin/devbackup"),
            )
            scheduler.PLIST_PATH = plist_path
            
            status = scheduler._get_launchd_status()
            
            assert status["installed"] is True
            assert status["interval_seconds"] == 7200


class TestCronEntryGeneration:
    """Unit tests for cron entry generation."""

    def test_create_cron_entry_for_hourly(self):
        """Test cron entry for hourly backup (3600 seconds)."""
        scheduler = Scheduler(
            scheduler_type=SchedulerType.CRON,
            interval_seconds=3600,
            devbackup_command=Path("/usr/local/bin/devbackup"),
        )
        
        entry = scheduler._create_cron_entry()
        
        # Should run every hour
        assert "0 */1 * * *" in entry
        assert "/usr/local/bin/devbackup run" in entry
        assert scheduler.CRON_MARKER in entry

    def test_create_cron_entry_for_30_minutes(self):
        """Test cron entry for 30-minute interval."""
        scheduler = Scheduler(
            scheduler_type=SchedulerType.CRON,
            interval_seconds=1800,
            devbackup_command=Path("/usr/local/bin/devbackup"),
        )
        
        entry = scheduler._create_cron_entry()
        
        # Should run every 30 minutes
        assert "*/30 * * * *" in entry

    def test_create_cron_entry_for_daily(self):
        """Test cron entry for daily backup."""
        scheduler = Scheduler(
            scheduler_type=SchedulerType.CRON,
            interval_seconds=86400,  # 24 hours
            devbackup_command=Path("/usr/local/bin/devbackup"),
        )
        
        entry = scheduler._create_cron_entry()
        
        # Should run daily at midnight
        assert "0 0 * * *" in entry

    def test_create_cron_entry_for_2_hours(self):
        """Test cron entry for 2-hour interval."""
        scheduler = Scheduler(
            scheduler_type=SchedulerType.CRON,
            interval_seconds=7200,  # 2 hours
            devbackup_command=Path("/usr/local/bin/devbackup"),
        )
        
        entry = scheduler._create_cron_entry()
        
        # Should run every 2 hours
        assert "0 */2 * * *" in entry


class TestCronInstallUninstall:
    """Unit tests for cron install/uninstall operations."""

    def test_is_cron_installed_returns_false_when_no_entry(self):
        """Test is_installed returns False when no cron entry exists."""
        scheduler = Scheduler(
            scheduler_type=SchedulerType.CRON,
            interval_seconds=3600,
            devbackup_command=Path("/usr/local/bin/devbackup"),
        )
        
        with patch.object(scheduler, "_get_current_crontab", return_value=""):
            assert scheduler._is_cron_installed() is False

    def test_is_cron_installed_returns_true_when_entry_exists(self):
        """Test is_installed returns True when cron entry exists."""
        scheduler = Scheduler(
            scheduler_type=SchedulerType.CRON,
            interval_seconds=3600,
            devbackup_command=Path("/usr/local/bin/devbackup"),
        )
        
        crontab_content = f"*/30 * * * * /usr/local/bin/devbackup run {scheduler.CRON_MARKER}\n"
        
        with patch.object(scheduler, "_get_current_crontab", return_value=crontab_content):
            assert scheduler._is_cron_installed() is True

    def test_get_cron_status_when_not_installed(self):
        """Test get_status returns correct info when not installed."""
        scheduler = Scheduler(
            scheduler_type=SchedulerType.CRON,
            interval_seconds=3600,
            devbackup_command=Path("/usr/local/bin/devbackup"),
        )
        
        with patch.object(scheduler, "_get_current_crontab", return_value=""):
            status = scheduler._get_cron_status()
            
            assert status["installed"] is False
            assert status["running"] is False

    def test_get_cron_status_parses_interval(self):
        """Test get_status parses interval from cron entry."""
        scheduler = Scheduler(
            scheduler_type=SchedulerType.CRON,
            interval_seconds=3600,
            devbackup_command=Path("/usr/local/bin/devbackup"),
        )
        
        crontab_content = f"*/30 * * * * /usr/local/bin/devbackup run {scheduler.CRON_MARKER}\n"
        
        with patch.object(scheduler, "_get_current_crontab", return_value=crontab_content):
            status = scheduler._get_cron_status()
            
            assert status["installed"] is True
            assert status["interval_seconds"] == 1800  # 30 minutes


class TestPublicInterface:
    """Unit tests for public Scheduler interface."""

    def test_install_dispatches_to_launchd(self):
        """Test install() calls launchd implementation for launchd type."""
        scheduler = Scheduler(
            scheduler_type=SchedulerType.LAUNCHD,
            interval_seconds=3600,
            devbackup_command=Path("/usr/local/bin/devbackup"),
        )
        
        with patch.object(scheduler, "_install_launchd") as mock_install:
            scheduler.install()
            mock_install.assert_called_once()

    def test_install_dispatches_to_cron(self):
        """Test install() calls cron implementation for cron type."""
        scheduler = Scheduler(
            scheduler_type=SchedulerType.CRON,
            interval_seconds=3600,
            devbackup_command=Path("/usr/local/bin/devbackup"),
        )
        
        with patch.object(scheduler, "_install_cron") as mock_install:
            scheduler.install()
            mock_install.assert_called_once()

    def test_uninstall_dispatches_to_launchd(self):
        """Test uninstall() calls launchd implementation for launchd type."""
        scheduler = Scheduler(
            scheduler_type=SchedulerType.LAUNCHD,
            interval_seconds=3600,
            devbackup_command=Path("/usr/local/bin/devbackup"),
        )
        
        with patch.object(scheduler, "_uninstall_launchd") as mock_uninstall:
            scheduler.uninstall()
            mock_uninstall.assert_called_once()

    def test_uninstall_dispatches_to_cron(self):
        """Test uninstall() calls cron implementation for cron type."""
        scheduler = Scheduler(
            scheduler_type=SchedulerType.CRON,
            interval_seconds=3600,
            devbackup_command=Path("/usr/local/bin/devbackup"),
        )
        
        with patch.object(scheduler, "_uninstall_cron") as mock_uninstall:
            scheduler.uninstall()
            mock_uninstall.assert_called_once()

    def test_is_installed_dispatches_correctly(self):
        """Test is_installed() dispatches to correct implementation."""
        launchd_scheduler = Scheduler(
            scheduler_type=SchedulerType.LAUNCHD,
            interval_seconds=3600,
            devbackup_command=Path("/usr/local/bin/devbackup"),
        )
        
        cron_scheduler = Scheduler(
            scheduler_type=SchedulerType.CRON,
            interval_seconds=3600,
            devbackup_command=Path("/usr/local/bin/devbackup"),
        )
        
        with patch.object(launchd_scheduler, "_is_launchd_installed", return_value=True):
            assert launchd_scheduler.is_installed() is True
        
        with patch.object(cron_scheduler, "_is_cron_installed", return_value=False):
            assert cron_scheduler.is_installed() is False

    def test_get_status_dispatches_correctly(self):
        """Test get_status() dispatches to correct implementation."""
        launchd_scheduler = Scheduler(
            scheduler_type=SchedulerType.LAUNCHD,
            interval_seconds=3600,
            devbackup_command=Path("/usr/local/bin/devbackup"),
        )
        
        expected_status = {"installed": True, "running": False, "interval_seconds": 3600}
        
        with patch.object(launchd_scheduler, "_get_launchd_status", return_value=expected_status):
            status = launchd_scheduler.get_status()
            assert status == expected_status


class TestHelperFunctions:
    """Unit tests for helper functions."""

    def test_parse_launchd_plist_extracts_interval(self):
        """Test parse_launchd_plist extracts StartInterval."""
        with tempfile.TemporaryDirectory() as tmpdir:
            plist_path = Path(tmpdir) / "test.plist"
            
            plist_data = {
                "Label": "com.test",
                "StartInterval": 5400,
            }
            with open(plist_path, "wb") as f:
                plistlib.dump(plist_data, f)
            
            interval = parse_launchd_plist(plist_path)
            
            assert interval == 5400

    def test_parse_launchd_plist_returns_none_for_missing_file(self):
        """Test parse_launchd_plist returns None for missing file."""
        interval = parse_launchd_plist(Path("/nonexistent/path.plist"))
        assert interval is None

    def test_parse_cron_interval_from_entry_minutes(self):
        """Test parsing */N minute pattern."""
        assert parse_cron_interval_from_entry("*/15 * * * * cmd") == 900  # 15 minutes
        assert parse_cron_interval_from_entry("*/30 * * * * cmd") == 1800  # 30 minutes

    def test_parse_cron_interval_from_entry_hours(self):
        """Test parsing */N hour pattern."""
        assert parse_cron_interval_from_entry("0 */2 * * * cmd") == 7200  # 2 hours
        assert parse_cron_interval_from_entry("0 */4 * * * cmd") == 14400  # 4 hours

    def test_parse_cron_interval_from_entry_daily(self):
        """Test parsing daily pattern."""
        assert parse_cron_interval_from_entry("0 0 * * * cmd") == 86400  # daily

    def test_parse_cron_interval_from_entry_hourly(self):
        """Test parsing hourly pattern."""
        assert parse_cron_interval_from_entry("0 * * * * cmd") == 3600  # hourly

    def test_parse_cron_interval_from_entry_invalid(self):
        """Test parsing invalid cron entry."""
        assert parse_cron_interval_from_entry("invalid") is None
        assert parse_cron_interval_from_entry("") is None
