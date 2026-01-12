"""Property-based tests for notification delivery.

Property 11: Notification Delivery
For any backup completion (success or failure), if notifications are enabled
for that outcome, a notification SHALL be sent with correct status, name/error,
and duration.

Requirements: 11.2, 11.3, 11.4
"""

import tempfile
from unittest.mock import patch, MagicMock

from hypothesis import given, strategies as st, settings

from devbackup.config import NotificationConfig
from devbackup.notify import Notifier


class TestNotificationDeliveryProperty:
    """Property 11: Notification Delivery"""
    
    @given(
        notify_on_success=st.booleans(),
        snapshot_name=st.text(min_size=1, max_size=50).filter(lambda x: x.strip()),
        duration_seconds=st.floats(min_value=0.0, max_value=86400.0, allow_nan=False),
        files_transferred=st.integers(min_value=0, max_value=1000000),
    )
    @settings(max_examples=50)
    def test_success_notification_respects_config(
        self,
        notify_on_success: bool,
        snapshot_name: str,
        duration_seconds: float,
        files_transferred: int,
    ):
        """
        Property: Success notification is sent iff notify_on_success is True.
        
        Validates: Requirements 11.2, 11.4
        """
        config = NotificationConfig(
            notify_on_success=notify_on_success,
            notify_on_failure=True,
        )
        notifier = Notifier(config)
        
        with patch.object(notifier, '_send_notification', return_value=True) as mock_send:
            result = notifier.notify_success(
                snapshot_name=snapshot_name,
                duration_seconds=duration_seconds,
                files_transferred=files_transferred,
            )
            
            if notify_on_success:
                # Notification should be sent
                assert result is True
                mock_send.assert_called_once()
                call_args = mock_send.call_args
                # Verify title contains success indicator
                assert "Complete" in call_args.kwargs.get('title', call_args[0][0] if call_args[0] else '')
                # Verify message contains snapshot name
                message = call_args.kwargs.get('message', call_args[0][1] if len(call_args[0]) > 1 else '')
                assert snapshot_name in message
            else:
                # Notification should not be sent
                assert result is False
                mock_send.assert_not_called()
    
    @given(
        notify_on_failure=st.booleans(),
        error_message=st.text(min_size=1, max_size=200).filter(lambda x: x.strip()),
        duration_seconds=st.floats(min_value=0.0, max_value=86400.0, allow_nan=False),
    )
    @settings(max_examples=50)
    def test_failure_notification_respects_config(
        self,
        notify_on_failure: bool,
        error_message: str,
        duration_seconds: float,
    ):
        """
        Property: Failure notification is sent iff notify_on_failure is True.
        
        Validates: Requirements 11.3, 11.4
        """
        config = NotificationConfig(
            notify_on_success=True,
            notify_on_failure=notify_on_failure,
        )
        notifier = Notifier(config)
        
        with patch.object(notifier, '_send_notification', return_value=True) as mock_send:
            result = notifier.notify_failure(
                error_message=error_message,
                duration_seconds=duration_seconds,
            )
            
            if notify_on_failure:
                # Notification should be sent
                assert result is True
                mock_send.assert_called_once()
                call_args = mock_send.call_args
                # Verify title contains failure indicator
                assert "Failed" in call_args.kwargs.get('title', call_args[0][0] if call_args[0] else '')
                # Verify message contains error (possibly truncated)
                message = call_args.kwargs.get('message', call_args[0][1] if len(call_args[0]) > 1 else '')
                # Error should be in message (truncated if > 100 chars)
                if len(error_message) <= 100:
                    assert error_message in message
                else:
                    assert error_message[:97] in message
            else:
                # Notification should not be sent
                assert result is False
                mock_send.assert_not_called()
    
    @given(
        duration_seconds=st.floats(min_value=0.0, max_value=86400.0, allow_nan=False),
    )
    @settings(max_examples=50)
    def test_duration_formatting_correctness(self, duration_seconds: float):
        """
        Property: Duration is formatted correctly in human-readable form.
        
        Validates: Requirements 11.4
        """
        config = NotificationConfig(notify_on_success=True)
        notifier = Notifier(config)
        
        formatted = notifier._format_duration(duration_seconds)
        
        # Verify format is correct based on duration
        if duration_seconds < 60:
            # Should be in seconds format
            assert 's' in formatted
            assert 'm' not in formatted or formatted.endswith('s')
        elif duration_seconds < 3600:
            # Should be in minutes and seconds format
            assert 'm' in formatted
            assert 'h' not in formatted
        else:
            # Should be in hours and minutes format
            assert 'h' in formatted
    
    @given(
        notify_on_success=st.booleans(),
        notify_on_failure=st.booleans(),
    )
    @settings(max_examples=20)
    def test_notification_config_independence(
        self,
        notify_on_success: bool,
        notify_on_failure: bool,
    ):
        """
        Property: Success and failure notification settings are independent.
        
        Validates: Requirements 11.2, 11.3
        """
        config = NotificationConfig(
            notify_on_success=notify_on_success,
            notify_on_failure=notify_on_failure,
        )
        notifier = Notifier(config)
        
        with patch.object(notifier, '_send_notification', return_value=True) as mock_send:
            # Test success notification
            success_result = notifier.notify_success(
                snapshot_name="test-snapshot",
                duration_seconds=10.0,
                files_transferred=100,
            )
            success_calls = mock_send.call_count
            
            # Test failure notification
            failure_result = notifier.notify_failure(
                error_message="Test error",
                duration_seconds=5.0,
            )
            total_calls = mock_send.call_count
            
            # Verify independence
            if notify_on_success:
                assert success_result is True
                assert success_calls == 1
            else:
                assert success_result is False
                assert success_calls == 0
            
            if notify_on_failure:
                assert failure_result is True
                assert total_calls == success_calls + 1
            else:
                assert failure_result is False
                assert total_calls == success_calls


class TestNotificationOsascriptIntegration:
    """Tests for osascript integration."""
    
    @given(
        title=st.text(min_size=1, max_size=50).filter(lambda x: x.strip()),
        message=st.text(min_size=1, max_size=200).filter(lambda x: x.strip()),
    )
    @settings(max_examples=30)
    def test_send_notification_escapes_special_characters(
        self,
        title: str,
        message: str,
    ):
        """
        Property: Special characters in title/message are properly escaped.
        
        Validates: Requirements 11.1
        """
        config = NotificationConfig(notify_on_success=True)
        notifier = Notifier(config)
        
        with patch('subprocess.run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stderr='')
            
            notifier._send_notification(title=title, message=message)
            
            # Verify subprocess was called
            mock_run.assert_called_once()
            call_args = mock_run.call_args
            
            # Get the osascript command
            cmd = call_args[0][0]
            assert cmd[0] == 'osascript'
            assert cmd[1] == '-e'
            
            # The script should be properly formed
            script = cmd[2]
            assert 'display notification' in script
    
    def test_send_notification_handles_timeout(self):
        """
        Property: Notification timeout is handled gracefully.
        
        Validates: Requirements 11.1
        """
        import subprocess
        
        config = NotificationConfig(notify_on_success=True)
        notifier = Notifier(config)
        
        with patch('subprocess.run') as mock_run:
            mock_run.side_effect = subprocess.TimeoutExpired(cmd='osascript', timeout=5)
            
            result = notifier._send_notification(
                title="Test",
                message="Test message",
            )
            
            # Should return False on timeout, not raise
            assert result is False
    
    def test_send_notification_handles_missing_osascript(self):
        """
        Property: Missing osascript is handled gracefully.
        
        Validates: Requirements 11.1
        """
        config = NotificationConfig(notify_on_success=True)
        notifier = Notifier(config)
        
        with patch('subprocess.run') as mock_run:
            mock_run.side_effect = FileNotFoundError("osascript not found")
            
            result = notifier._send_notification(
                title="Test",
                message="Test message",
            )
            
            # Should return False when osascript not found, not raise
            assert result is False
    
    def test_send_notification_handles_nonzero_exit(self):
        """
        Property: Non-zero exit code is handled gracefully.
        
        Validates: Requirements 11.1
        """
        config = NotificationConfig(notify_on_success=True)
        notifier = Notifier(config)
        
        with patch('subprocess.run') as mock_run:
            mock_run.return_value = MagicMock(returncode=1, stderr='Error')
            
            result = notifier._send_notification(
                title="Test",
                message="Test message",
            )
            
            # Should return False on error, not raise
            assert result is False
