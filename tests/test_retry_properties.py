"""Property-based tests for retry behavior correctness.

Tests Property 10 (Retry Behavior Correctness) from the backup-robustness design document.

**Validates: Requirements 10.1, 10.2, 10.4, 10.5**
"""

import time
from typing import List, Tuple

import pytest
from hypothesis import given, strategies as st, settings, Phase, assume

from devbackup.retry import (
    RETRYABLE_ERROR_CODES,
    RetryAttempt,
    RetryConfig,
    RetryResult,
    calculate_backoff_delay,
    is_retryable_error,
    retry_with_backoff,
)


# Strategy for generating retryable error codes
retryable_codes = st.sampled_from(list(RETRYABLE_ERROR_CODES))

# Strategy for generating non-retryable error codes (excluding 0 which is success)
non_retryable_codes = st.integers(min_value=1, max_value=255).filter(
    lambda x: x not in RETRYABLE_ERROR_CODES
)

# Strategy for generating retry counts
retry_counts = st.integers(min_value=1, max_value=5)

# Strategy for generating base delays (small for testing)
base_delays = st.floats(min_value=0.001, max_value=0.1)

# Strategy for generating failure sequences
failure_sequences = st.lists(
    st.sampled_from(list(RETRYABLE_ERROR_CODES)),
    min_size=1,
    max_size=10,
)


class TestRetryBehaviorCorrectness:
    """
    Property 10: Retry Behavior Correctness
    
    *For any* retryable rsync failure, the system SHALL retry up to retry_count 
    times with exponential backoff, logging each attempt.
    
    **Validates: Requirements 10.1, 10.2, 10.4, 10.5**
    """
    
    @given(
        error_code=retryable_codes,
        max_retries=retry_counts,
        base_delay=base_delays,
    )
    @settings(max_examples=10, deadline=None, phases=[Phase.generate, Phase.target])
    def test_retryable_errors_trigger_retry(
        self,
        error_code: int,
        max_retries: int,
        base_delay: float,
    ):
        """
        **Feature: backup-robustness, Property 10: Retry Behavior Correctness**
        
        For any retryable error code, the system should retry up to max_retries times.
        
        **Validates: Requirements 10.1, 10.2**
        """
        # Track how many times the operation was called
        call_count = 0
        
        def failing_operation() -> Tuple[int, str, None]:
            nonlocal call_count
            call_count += 1
            return error_code, f"Error {error_code}", None
        
        retry_result, _ = retry_with_backoff(
            operation=failing_operation,
            max_retries=max_retries,
            base_delay=base_delay,
            max_delay=1.0,  # Cap max delay for testing
        )
        
        # INVARIANT 1: Should have called operation max_retries + 1 times
        # (initial attempt + max_retries retries)
        assert call_count == max_retries + 1, \
            f"Expected {max_retries + 1} calls, got {call_count}"
        
        # INVARIANT 2: Result should indicate failure
        assert not retry_result.success, "Should fail after exhausting retries"
        
        # INVARIANT 3: Should have recorded max_retries retry attempts
        assert len(retry_result.attempts) == max_retries, \
            f"Expected {max_retries} retry attempts, got {len(retry_result.attempts)}"
        
        # INVARIANT 4: Final error code should match
        assert retry_result.final_return_code == error_code
    
    @given(
        error_code=non_retryable_codes,
        max_retries=retry_counts,
    )
    @settings(max_examples=10, deadline=None, phases=[Phase.generate, Phase.target])
    def test_non_retryable_errors_fail_immediately(
        self,
        error_code: int,
        max_retries: int,
    ):
        """
        **Feature: backup-robustness, Property 10: Retry Behavior Correctness**
        
        For any non-retryable error code, the system should fail immediately
        without retrying.
        
        **Validates: Requirements 10.3**
        """
        call_count = 0
        
        def failing_operation() -> Tuple[int, str, None]:
            nonlocal call_count
            call_count += 1
            return error_code, f"Non-retryable error {error_code}", None
        
        retry_result, _ = retry_with_backoff(
            operation=failing_operation,
            max_retries=max_retries,
            base_delay=0.001,
        )
        
        # INVARIANT 1: Should have called operation exactly once
        assert call_count == 1, \
            f"Expected 1 call for non-retryable error, got {call_count}"
        
        # INVARIANT 2: Result should indicate failure
        assert not retry_result.success
        
        # INVARIANT 3: Should have no retry attempts
        assert len(retry_result.attempts) == 0, \
            "Should have no retry attempts for non-retryable error"
        
        # INVARIANT 4: Final error code should match
        assert retry_result.final_return_code == error_code
    
    @given(
        attempt=st.integers(min_value=1, max_value=10),
        base_delay=st.floats(min_value=0.1, max_value=10.0),
    )
    @settings(max_examples=10, deadline=None, phases=[Phase.generate, Phase.target])
    def test_exponential_backoff_formula(
        self,
        attempt: int,
        base_delay: float,
    ):
        """
        **Feature: backup-robustness, Property 10: Retry Behavior Correctness**
        
        For any attempt number, the delay should follow the formula:
        delay = min(base_delay * 2^(attempt-1), max_delay)
        
        **Validates: Requirements 10.1**
        """
        max_delay = 1000.0  # Large enough to not cap for most cases
        expected_delay = min(base_delay * (2 ** (attempt - 1)), max_delay)
        
        actual_delay = calculate_backoff_delay(attempt, base_delay, max_delay)
        
        # INVARIANT: Delay should match exponential formula (capped at max_delay)
        assert abs(actual_delay - expected_delay) < 0.0001, \
            f"Expected delay {expected_delay}, got {actual_delay}"
    
    @given(
        attempt=st.integers(min_value=1, max_value=20),
        base_delay=st.floats(min_value=1.0, max_value=10.0),
        max_delay=st.floats(min_value=10.0, max_value=100.0),
    )
    @settings(max_examples=10, deadline=None, phases=[Phase.generate, Phase.target])
    def test_backoff_respects_max_delay(
        self,
        attempt: int,
        base_delay: float,
        max_delay: float,
    ):
        """
        **Feature: backup-robustness, Property 10: Retry Behavior Correctness**
        
        For any attempt, the delay should never exceed max_delay.
        
        **Validates: Requirements 10.1**
        """
        actual_delay = calculate_backoff_delay(attempt, base_delay, max_delay)
        
        # INVARIANT: Delay should never exceed max_delay
        assert actual_delay <= max_delay, \
            f"Delay {actual_delay} exceeds max_delay {max_delay}"
    
    @given(
        failures_before_success=st.integers(min_value=0, max_value=4),
        max_retries=st.integers(min_value=1, max_value=5),
    )
    @settings(max_examples=10, deadline=None, phases=[Phase.generate, Phase.target])
    def test_success_after_retries(
        self,
        failures_before_success: int,
        max_retries: int,
    ):
        """
        **Feature: backup-robustness, Property 10: Retry Behavior Correctness**
        
        If an operation succeeds after some retries, the result should indicate
        success and record the retry attempts.
        
        **Validates: Requirements 10.1, 10.2**
        """
        # Skip if we would fail before succeeding
        assume(failures_before_success <= max_retries)
        
        call_count = 0
        
        def eventually_succeeds() -> Tuple[int, str, str]:
            nonlocal call_count
            call_count += 1
            if call_count <= failures_before_success:
                return 10, "Socket I/O error", None  # Retryable error
            return 0, "", "success_result"
        
        retry_result, result = retry_with_backoff(
            operation=eventually_succeeds,
            max_retries=max_retries,
            base_delay=0.001,
        )
        
        # INVARIANT 1: Should succeed
        assert retry_result.success, "Should succeed after retries"
        
        # INVARIANT 2: Should have correct number of retry attempts
        assert len(retry_result.attempts) == failures_before_success, \
            f"Expected {failures_before_success} retry attempts, got {len(retry_result.attempts)}"
        
        # INVARIANT 3: Result should be returned
        assert result == "success_result"
        
        # INVARIANT 4: Total attempts should be failures + 1 (success)
        assert retry_result.total_attempts == failures_before_success + 1
    
    @given(
        max_retries=retry_counts,
        base_delay=base_delays,
    )
    @settings(max_examples=50, deadline=None, phases=[Phase.generate, Phase.target])
    def test_retry_callback_called_for_each_retry(
        self,
        max_retries: int,
        base_delay: float,
    ):
        """
        **Feature: backup-robustness, Property 10: Retry Behavior Correctness**
        
        The on_retry callback should be called for each retry attempt with
        correct attempt information.
        
        **Validates: Requirements 10.4**
        """
        callback_calls: List[RetryAttempt] = []
        
        def failing_operation() -> Tuple[int, str, None]:
            return 10, "Socket I/O error", None  # Retryable error
        
        def on_retry(attempt: RetryAttempt) -> None:
            callback_calls.append(attempt)
        
        retry_with_backoff(
            operation=failing_operation,
            max_retries=max_retries,
            base_delay=base_delay,
            max_delay=1.0,
            on_retry=on_retry,
        )
        
        # INVARIANT 1: Callback should be called max_retries times
        assert len(callback_calls) == max_retries, \
            f"Expected {max_retries} callback calls, got {len(callback_calls)}"
        
        # INVARIANT 2: Attempt numbers should be sequential
        for i, attempt in enumerate(callback_calls):
            assert attempt.attempt_number == i + 1, \
                f"Expected attempt number {i + 1}, got {attempt.attempt_number}"
        
        # INVARIANT 3: Each attempt should have the error code
        for attempt in callback_calls:
            assert attempt.error_code == 10
    
    @given(max_retries=retry_counts)
    @settings(max_examples=50, deadline=None, phases=[Phase.generate, Phase.target])
    def test_retry_history_on_final_failure(
        self,
        max_retries: int,
    ):
        """
        **Feature: backup-robustness, Property 10: Retry Behavior Correctness**
        
        When all retries are exhausted, the result should contain a complete
        retry history.
        
        **Validates: Requirements 10.5**
        """
        def failing_operation() -> Tuple[int, str, None]:
            return 23, "Partial transfer due to error", None
        
        retry_result, _ = retry_with_backoff(
            operation=failing_operation,
            max_retries=max_retries,
            base_delay=0.001,
        )
        
        # INVARIANT 1: Should have failed
        assert not retry_result.success
        
        # INVARIANT 2: Should have retry history
        history = retry_result.retry_history
        assert "Retry history" in history, "Should have retry history header"
        
        # INVARIANT 3: History should mention all attempts
        for i in range(1, max_retries + 1):
            assert f"Attempt {i}" in history, \
                f"History should mention attempt {i}"
        
        # INVARIANT 4: History should include error code
        assert "23" in history, "History should include error code"


class TestRetryableErrorCodes:
    """Tests for retryable error code detection."""
    
    @given(error_code=retryable_codes)
    @settings(max_examples=10, deadline=None, phases=[Phase.generate, Phase.target])
    def test_retryable_codes_are_detected(self, error_code: int):
        """
        **Feature: backup-robustness, Property 10: Retry Behavior Correctness**
        
        All defined retryable error codes should be detected as retryable.
        
        **Validates: Requirements 10.3**
        """
        assert is_retryable_error(error_code), \
            f"Error code {error_code} should be retryable"
    
    @given(error_code=non_retryable_codes)
    @settings(max_examples=10, deadline=None, phases=[Phase.generate, Phase.target])
    def test_non_retryable_codes_are_not_detected(self, error_code: int):
        """
        **Feature: backup-robustness, Property 10: Retry Behavior Correctness**
        
        Non-retryable error codes should not be detected as retryable.
        
        **Validates: Requirements 10.3**
        """
        assert not is_retryable_error(error_code), \
            f"Error code {error_code} should not be retryable"
    
    def test_success_code_is_not_retryable(self):
        """Success (0) should not be considered retryable."""
        assert not is_retryable_error(0), "Success code 0 should not be retryable"


class TestRetryConfig:
    """Tests for RetryConfig dataclass."""
    
    @given(
        retry_count=st.integers(min_value=0, max_value=10),
        base_delay=st.floats(min_value=0.1, max_value=60.0),
    )
    @settings(max_examples=50, deadline=None, phases=[Phase.generate, Phase.target])
    def test_retry_config_stores_values(
        self,
        retry_count: int,
        base_delay: float,
    ):
        """
        **Feature: backup-robustness, Property 10: Retry Behavior Correctness**
        
        RetryConfig should correctly store and return configuration values.
        
        **Validates: Requirements 10.2**
        """
        config = RetryConfig(
            max_retries=retry_count,
            base_delay_seconds=base_delay,
        )
        
        assert config.max_retries == retry_count
        assert config.base_delay_seconds == base_delay
