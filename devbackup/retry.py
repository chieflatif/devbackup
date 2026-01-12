"""Retry logic for devbackup.

This module provides retry functionality with exponential backoff for
handling transient failures during backup operations.

Requirements: 10.1, 10.2, 10.3, 10.4, 10.5
"""

import logging
import time
from dataclasses import dataclass, field
from typing import Callable, List, Optional, Set, TypeVar

logger = logging.getLogger(__name__)


# rsync exit codes that are considered retryable (transient failures)
# Requirements: 10.3
RETRYABLE_ERROR_CODES: Set[int] = {
    10,  # Error in socket I/O
    11,  # Error in file I/O
    12,  # Error in rsync protocol data stream
    23,  # Partial transfer due to error
    24,  # Partial transfer due to vanished source files
    30,  # Timeout in data send/receive
}


@dataclass
class RetryAttempt:
    """Information about a single retry attempt."""
    attempt_number: int
    error_code: int
    error_message: str
    delay_seconds: float


@dataclass
class RetryResult:
    """Result of a retry operation."""
    success: bool
    final_return_code: int
    final_error_message: Optional[str]
    attempts: List[RetryAttempt] = field(default_factory=list)
    total_attempts: int = 0
    
    @property
    def retry_history(self) -> str:
        """Format retry history as a human-readable string."""
        if not self.attempts:
            return "No retries attempted"
        
        lines = [f"Retry history ({len(self.attempts)} attempts):"]
        for attempt in self.attempts:
            lines.append(
                f"  Attempt {attempt.attempt_number}: "
                f"error code {attempt.error_code} - {attempt.error_message} "
                f"(waited {attempt.delay_seconds:.1f}s)"
            )
        return "\n".join(lines)


def is_retryable_error(return_code: int) -> bool:
    """
    Check if an rsync return code is retryable.
    
    Args:
        return_code: rsync exit code
    
    Returns:
        True if the error is transient and should be retried
    
    Requirements: 10.3
    """
    return return_code in RETRYABLE_ERROR_CODES


def calculate_backoff_delay(
    attempt: int,
    base_delay: float = 5.0,
    max_delay: float = 300.0,
) -> float:
    """
    Calculate exponential backoff delay for a retry attempt.
    
    Uses the formula: delay = base_delay * 2^(attempt-1)
    Capped at max_delay to prevent excessive waits.
    
    Args:
        attempt: Current attempt number (1-based)
        base_delay: Base delay in seconds (default 5s)
        max_delay: Maximum delay in seconds (default 300s = 5 minutes)
    
    Returns:
        Delay in seconds before the next retry
    
    Requirements: 10.1
    """
    delay = base_delay * (2 ** (attempt - 1))
    return min(delay, max_delay)


T = TypeVar('T')


def retry_with_backoff(
    operation: Callable[[], tuple[int, str, T]],
    max_retries: int = 3,
    base_delay: float = 5.0,
    max_delay: float = 300.0,
    on_retry: Optional[Callable[[RetryAttempt], None]] = None,
) -> tuple[RetryResult, Optional[T]]:
    """
    Execute an operation with retry logic and exponential backoff.
    
    The operation callable should return a tuple of:
    - return_code: int (0 for success, non-zero for failure)
    - error_message: str (error description if failed)
    - result: T (operation result, may be None on failure)
    
    Args:
        operation: Callable that returns (return_code, error_message, result)
        max_retries: Maximum number of retry attempts (default 3)
        base_delay: Base delay in seconds for exponential backoff
        max_delay: Maximum delay in seconds
        on_retry: Optional callback called before each retry
    
    Returns:
        Tuple of (RetryResult, operation_result)
    
    Requirements: 10.1, 10.2, 10.4, 10.5
    """
    attempts: List[RetryAttempt] = []
    total_attempts = 0
    
    for attempt in range(1, max_retries + 2):  # +2 because first attempt is not a retry
        total_attempts = attempt
        
        # Execute the operation
        return_code, error_message, result = operation()
        
        # Success - return immediately
        if return_code == 0:
            return RetryResult(
                success=True,
                final_return_code=0,
                final_error_message=None,
                attempts=attempts,
                total_attempts=total_attempts,
            ), result
        
        # Check if we should retry
        if not is_retryable_error(return_code):
            # Non-retryable error - fail immediately
            logger.debug(
                f"Non-retryable error code {return_code}: {error_message}"
            )
            return RetryResult(
                success=False,
                final_return_code=return_code,
                final_error_message=error_message,
                attempts=attempts,
                total_attempts=total_attempts,
            ), result
        
        # Check if we have retries left
        if attempt > max_retries:
            # All retries exhausted
            # Requirements: 10.5
            logger.error(
                f"All {max_retries} retries exhausted. "
                f"Final error code {return_code}: {error_message}"
            )
            return RetryResult(
                success=False,
                final_return_code=return_code,
                final_error_message=error_message,
                attempts=attempts,
                total_attempts=total_attempts,
            ), result
        
        # Calculate backoff delay
        delay = calculate_backoff_delay(attempt, base_delay, max_delay)
        
        # Record this attempt
        retry_attempt = RetryAttempt(
            attempt_number=attempt,
            error_code=return_code,
            error_message=error_message,
            delay_seconds=delay,
        )
        attempts.append(retry_attempt)
        
        # Log retry attempt (Requirements: 10.4)
        logger.warning(
            f"Retry attempt {attempt}/{max_retries}: "
            f"error code {return_code} - {error_message}. "
            f"Waiting {delay:.1f}s before retry."
        )
        
        # Call retry callback if provided
        if on_retry is not None:
            on_retry(retry_attempt)
        
        # Wait before retry
        time.sleep(delay)
    
    # Should not reach here, but handle it gracefully
    return RetryResult(
        success=False,
        final_return_code=-1,
        final_error_message="Unexpected retry loop exit",
        attempts=attempts,
        total_attempts=total_attempts,
    ), None


class RetryConfig:
    """Configuration for retry behavior."""
    
    def __init__(
        self,
        max_retries: int = 3,
        base_delay_seconds: float = 5.0,
        max_delay_seconds: float = 300.0,
        rsync_timeout_seconds: int = 3600,
    ):
        """
        Initialize retry configuration.
        
        Args:
            max_retries: Maximum number of retry attempts
            base_delay_seconds: Base delay for exponential backoff
            max_delay_seconds: Maximum delay between retries
            rsync_timeout_seconds: Timeout for rsync operations (default 1 hour)
        
        Requirements: 10.2
        """
        self.max_retries = max_retries
        self.base_delay_seconds = base_delay_seconds
        self.max_delay_seconds = max_delay_seconds
        self.rsync_timeout_seconds = rsync_timeout_seconds
    
    def __repr__(self) -> str:
        return (
            f"RetryConfig(max_retries={self.max_retries}, "
            f"base_delay_seconds={self.base_delay_seconds}, "
            f"max_delay_seconds={self.max_delay_seconds}, "
            f"rsync_timeout_seconds={self.rsync_timeout_seconds})"
        )
