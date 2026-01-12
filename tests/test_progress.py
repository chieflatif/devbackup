"""Unit tests for the ProgressReporter."""

import pytest

from devbackup.progress import ProgressReporter, ProgressInfo


class TestProgressInfo:
    """Tests for ProgressInfo dataclass."""
    
    def test_default_values(self):
        """Test ProgressInfo default values."""
        info = ProgressInfo()
        assert info.files_transferred == 0
        assert info.total_files is None
        assert info.bytes_transferred == 0
        assert info.total_bytes is None
        assert info.transfer_rate == 0.0
        assert info.current_file is None
        assert info.percent_complete is None
    
    def test_custom_values(self):
        """Test ProgressInfo with custom values."""
        info = ProgressInfo(
            files_transferred=10,
            total_files=100,
            bytes_transferred=1024,
            total_bytes=10240,
            transfer_rate=512.0,
            current_file="test.txt",
            percent_complete=10.0,
        )
        assert info.files_transferred == 10
        assert info.total_files == 100
        assert info.bytes_transferred == 1024
        assert info.total_bytes == 10240
        assert info.transfer_rate == 512.0
        assert info.current_file == "test.txt"
        assert info.percent_complete == 10.0


class TestProgressReporter:
    """Tests for ProgressReporter class."""
    
    def test_init_default(self):
        """Test ProgressReporter initialization with defaults."""
        reporter = ProgressReporter()
        assert reporter.callback is None
        progress = reporter.get_current_progress()
        assert progress.files_transferred == 0
    
    def test_init_with_callback(self):
        """Test ProgressReporter initialization with callback."""
        callback_calls = []
        def callback(info):
            callback_calls.append(info)
        
        reporter = ProgressReporter(callback=callback)
        assert reporter.callback is callback
    
    def test_parse_progress2_basic(self):
        """Test parsing basic --info=progress2 output."""
        reporter = ProgressReporter()
        
        # Basic progress line
        line = "  1,234,567  50%  123.45kB/s  0:01:23"
        result = reporter.parse_rsync_output(line)
        
        assert result is not None
        assert result.bytes_transferred == 1234567
        assert result.percent_complete == 50.0
        assert result.transfer_rate == pytest.approx(123.45 * 1024, rel=0.01)
    
    def test_parse_progress2_with_file_counts(self):
        """Test parsing --info=progress2 output with file counts."""
        reporter = ProgressReporter()
        
        # Progress line with xfr and to-chk
        line = "  1,234,567 100%  123.45MB/s    0:00:01 (xfr#5, to-chk=95/100)"
        result = reporter.parse_rsync_output(line)
        
        assert result is not None
        assert result.bytes_transferred == 1234567
        assert result.percent_complete == 100.0
        assert result.transfer_rate == pytest.approx(123.45 * 1024 * 1024, rel=0.01)
        assert result.files_transferred == 5  # 100 - 95
        assert result.total_files == 100
    
    def test_parse_progress2_megabytes(self):
        """Test parsing with MB/s rate."""
        reporter = ProgressReporter()
        
        line = "  10,000,000  25%  50.00MB/s  0:00:30"
        result = reporter.parse_rsync_output(line)
        
        assert result is not None
        assert result.bytes_transferred == 10000000
        assert result.percent_complete == 25.0
        assert result.transfer_rate == pytest.approx(50.0 * 1024 * 1024, rel=0.01)
    
    def test_parse_progress2_gigabytes(self):
        """Test parsing with GB/s rate."""
        reporter = ProgressReporter()
        
        line = "  1,000,000,000  10%  1.50GB/s  0:00:05"
        result = reporter.parse_rsync_output(line)
        
        assert result is not None
        assert result.bytes_transferred == 1000000000
        assert result.percent_complete == 10.0
        assert result.transfer_rate == pytest.approx(1.5 * 1024 * 1024 * 1024, rel=0.01)
    
    def test_parse_empty_line(self):
        """Test parsing empty line returns None."""
        reporter = ProgressReporter()
        result = reporter.parse_rsync_output("")
        assert result is None
        
        result = reporter.parse_rsync_output("   ")
        assert result is None
    
    def test_parse_non_progress_line(self):
        """Test parsing non-progress lines."""
        reporter = ProgressReporter()
        
        # These should not match progress pattern
        assert reporter.parse_rsync_output("sending incremental file list") is None
        assert reporter.parse_rsync_output("sent 1234 bytes  received 56 bytes") is None
        assert reporter.parse_rsync_output("total size is 1234567  speedup is 1.23") is None
    
    def test_parse_filename_line(self):
        """Test parsing filename lines (verbose output)."""
        reporter = ProgressReporter()
        
        # A filename being transferred
        result = reporter.parse_rsync_output("src/main.py")
        
        assert result is not None
        assert result.current_file == "src/main.py"
        assert result.files_transferred == 1
    
    def test_callback_called_on_progress(self):
        """Test callback is called when progress is parsed."""
        callback_calls = []
        def callback(info):
            callback_calls.append(info)
        
        reporter = ProgressReporter(callback=callback)
        reporter.parse_rsync_output("  1,234,567  50%  100.00kB/s  0:01:00")
        
        assert len(callback_calls) == 1
        assert callback_calls[0].bytes_transferred == 1234567
    
    def test_get_current_progress(self):
        """Test get_current_progress returns latest state."""
        reporter = ProgressReporter()
        
        # Initial state
        progress = reporter.get_current_progress()
        assert progress.bytes_transferred == 0
        
        # After parsing
        reporter.parse_rsync_output("  1,000,000  25%  50.00kB/s  0:00:10")
        progress = reporter.get_current_progress()
        assert progress.bytes_transferred == 1000000
        assert progress.percent_complete == 25.0
    
    def test_report_final(self):
        """Test report_final creates final statistics."""
        reporter = ProgressReporter()
        
        result = reporter.report_final(
            files_transferred=100,
            total_size=1000000,
            duration_seconds=10.0,
        )
        
        assert result.files_transferred == 100
        assert result.total_files == 100
        assert result.bytes_transferred == 1000000
        assert result.total_bytes == 1000000
        assert result.transfer_rate == pytest.approx(100000.0, rel=0.01)
        assert result.percent_complete == 100.0
    
    def test_report_final_zero_duration(self):
        """Test report_final handles zero duration."""
        reporter = ProgressReporter()
        
        result = reporter.report_final(
            files_transferred=10,
            total_size=1000,
            duration_seconds=0.0,
        )
        
        assert result.transfer_rate == 0.0
    
    def test_report_final_callback(self):
        """Test report_final calls callback."""
        callback_calls = []
        def callback(info):
            callback_calls.append(info)
        
        reporter = ProgressReporter(callback=callback)
        reporter.report_final(
            files_transferred=50,
            total_size=500000,
            duration_seconds=5.0,
        )
        
        assert len(callback_calls) == 1
        assert callback_calls[0].percent_complete == 100.0
    
    def test_reset(self):
        """Test reset clears progress state."""
        reporter = ProgressReporter()
        
        # Parse some progress
        reporter.parse_rsync_output("  1,000,000  50%  100.00kB/s  0:00:30")
        assert reporter.get_current_progress().bytes_transferred == 1000000
        
        # Reset
        reporter.reset()
        progress = reporter.get_current_progress()
        assert progress.bytes_transferred == 0
        assert progress.files_transferred == 0
        assert progress.percent_complete is None
    
    def test_cumulative_file_count(self):
        """Test file count accumulates across multiple file lines."""
        reporter = ProgressReporter()
        
        reporter.parse_rsync_output("file1.txt")
        assert reporter.get_current_progress().files_transferred == 1
        
        reporter.parse_rsync_output("file2.txt")
        assert reporter.get_current_progress().files_transferred == 2
        
        reporter.parse_rsync_output("dir/file3.txt")
        assert reporter.get_current_progress().files_transferred == 3
    
    def test_total_bytes_calculated_from_percent(self):
        """Test total_bytes is calculated from percent and bytes_transferred."""
        reporter = ProgressReporter()
        
        # 50% complete with 500 bytes = 1000 total
        reporter.parse_rsync_output("  500  50%  100.00B/s  0:00:05")
        progress = reporter.get_current_progress()
        
        assert progress.bytes_transferred == 500
        assert progress.total_bytes == 1000
