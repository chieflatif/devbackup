"""Property-based tests for IPC Message Round-Trip.

**Property 5: IPC Message Round-Trip**
**Validates: Requirements 3.8**

Tests that:
- For any valid IPCMessage, sending it through serialization and deserialization
  SHALL produce an equivalent message
- Identical type field
- Equivalent payload (JSON serialization round-trip)
- No data loss or corruption
"""

import asyncio
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

import pytest
from hypothesis import given, settings, assume, HealthCheck
from hypothesis import strategies as st

from devbackup.ipc import (
    IPCMessage,
    IPCServer,
    IPCClient,
    IPCError,
    MessageType,
    BackupStatus,
    DEFAULT_SOCKET_PATH,
)


# Strategy for generating valid message types
message_type_strategy = st.sampled_from([
    MessageType.STATUS_REQUEST.value,
    MessageType.STATUS_RESPONSE.value,
    MessageType.BACKUP_TRIGGER.value,
    MessageType.BACKUP_RESPONSE.value,
    MessageType.BROWSE_REQUEST.value,
    MessageType.BROWSE_RESPONSE.value,
    MessageType.ERROR_RESPONSE.value,
])

# Strategy for generating JSON-serializable values
json_value_strategy = st.recursive(
    st.none() | st.booleans() | st.integers() | st.floats(allow_nan=False, allow_infinity=False) | st.text(),
    lambda children: st.lists(children, max_size=5) | st.dictionaries(st.text(min_size=1, max_size=20), children, max_size=5),
    max_leaves=10,
)

# Strategy for generating payload dictionaries
payload_strategy = st.dictionaries(
    st.text(min_size=1, max_size=50, alphabet=st.characters(whitelist_categories=('L', 'N', 'P'))),
    json_value_strategy,
    max_size=10,
)

# Strategy for generating optional message IDs
message_id_strategy = st.one_of(st.none(), st.text(min_size=1, max_size=50))

# Strategy for generating ISO format timestamps
timestamp_strategy = st.datetimes(
    min_value=datetime(2020, 1, 1),
    max_value=datetime(2030, 12, 31),
).map(lambda dt: dt.isoformat())


class TestIPCMessageRoundTripProperty:
    """
    Property 5: IPC Message Round-Trip
    
    *For any* valid IPCMessage, sending it through the Unix socket and receiving it
    SHALL produce an equivalent message with:
    - Identical type field
    - Equivalent payload (JSON serialization round-trip)
    - No data loss or corruption
    
    **Validates: Requirements 3.8**
    """
    
    @given(
        msg_type=message_type_strategy,
        payload=payload_strategy,
        message_id=message_id_strategy,
    )
    @settings(
        max_examples=100,
        deadline=None,
        suppress_health_check=[HealthCheck.too_slow],
    )
    def test_json_serialization_round_trip(
        self,
        msg_type: str,
        payload: Dict[str, Any],
        message_id: str,
    ):
        """
        Feature: user-experience-enhancement, Property 5: IPC Message Round-Trip
        
        JSON serialization and deserialization SHALL preserve message data.
        
        **Validates: Requirements 3.8**
        """
        # Create original message
        original = IPCMessage(
            type=msg_type,
            payload=payload,
            message_id=message_id,
        )
        
        # Serialize to JSON
        json_str = original.to_json()
        
        # Deserialize from JSON
        restored = IPCMessage.from_json(json_str)
        
        # Verify type is identical
        assert restored.type == original.type, \
            f"Type mismatch: {restored.type} != {original.type}"
        
        # Verify payload is equivalent
        assert restored.payload == original.payload, \
            f"Payload mismatch: {restored.payload} != {original.payload}"
        
        # Verify message_id is identical
        assert restored.message_id == original.message_id, \
            f"Message ID mismatch: {restored.message_id} != {original.message_id}"
        
        # Verify timestamp is preserved
        assert restored.timestamp == original.timestamp, \
            f"Timestamp mismatch: {restored.timestamp} != {original.timestamp}"
    
    @given(
        msg_type=message_type_strategy,
        payload=payload_strategy,
        message_id=message_id_strategy,
    )
    @settings(
        max_examples=100,
        deadline=None,
        suppress_health_check=[HealthCheck.too_slow],
    )
    def test_bytes_serialization_round_trip(
        self,
        msg_type: str,
        payload: Dict[str, Any],
        message_id: str,
    ):
        """
        Feature: user-experience-enhancement, Property 5: IPC Message Round-Trip
        
        Bytes serialization and deserialization SHALL preserve message data.
        
        **Validates: Requirements 3.8**
        """
        # Create original message
        original = IPCMessage(
            type=msg_type,
            payload=payload,
            message_id=message_id,
        )
        
        # Serialize to bytes
        data = original.to_bytes()
        
        # Deserialize from bytes
        restored = IPCMessage.from_bytes(data)
        
        # Verify type is identical
        assert restored.type == original.type, \
            f"Type mismatch: {restored.type} != {original.type}"
        
        # Verify payload is equivalent
        assert restored.payload == original.payload, \
            f"Payload mismatch: {restored.payload} != {original.payload}"
        
        # Verify message_id is identical
        assert restored.message_id == original.message_id, \
            f"Message ID mismatch: {restored.message_id} != {original.message_id}"
    
    @given(
        msg_type=message_type_strategy,
        payload=payload_strategy,
    )
    @settings(
        max_examples=100,
        deadline=None,
        suppress_health_check=[HealthCheck.too_slow],
    )
    def test_double_round_trip(
        self,
        msg_type: str,
        payload: Dict[str, Any],
    ):
        """
        Feature: user-experience-enhancement, Property 5: IPC Message Round-Trip
        
        Multiple round-trips SHALL produce identical results (idempotence).
        
        **Validates: Requirements 3.8**
        """
        # Create original message
        original = IPCMessage(
            type=msg_type,
            payload=payload,
        )
        
        # First round-trip
        json1 = original.to_json()
        restored1 = IPCMessage.from_json(json1)
        
        # Second round-trip
        json2 = restored1.to_json()
        restored2 = IPCMessage.from_json(json2)
        
        # Results should be identical
        assert restored1.type == restored2.type
        assert restored1.payload == restored2.payload
        assert restored1.message_id == restored2.message_id
        assert restored1.timestamp == restored2.timestamp
        
        # JSON representations should be identical
        assert json1 == json2, \
            f"JSON representations differ after double round-trip"


class TestIPCMessageValidation:
    """
    Tests for IPCMessage validation and error handling.
    
    **Validates: Requirements 3.8**
    """
    
    def test_missing_type_raises_error(self):
        """
        Feature: user-experience-enhancement, Property 5: IPC Message Round-Trip
        
        Messages missing 'type' field SHALL raise ValueError.
        
        **Validates: Requirements 3.8**
        """
        invalid_json = '{"payload": {}}'
        
        with pytest.raises(ValueError, match="missing required 'type' field"):
            IPCMessage.from_json(invalid_json)
    
    def test_invalid_json_raises_error(self):
        """
        Feature: user-experience-enhancement, Property 5: IPC Message Round-Trip
        
        Invalid JSON SHALL raise ValueError.
        
        **Validates: Requirements 3.8**
        """
        invalid_json = 'not valid json {'
        
        with pytest.raises(ValueError, match="Invalid JSON"):
            IPCMessage.from_json(invalid_json)
    
    @given(msg_type=message_type_strategy)
    @settings(
        max_examples=100,
        deadline=None,
        suppress_health_check=[HealthCheck.too_slow],
    )
    def test_empty_payload_preserved(self, msg_type: str):
        """
        Feature: user-experience-enhancement, Property 5: IPC Message Round-Trip
        
        Empty payloads SHALL be preserved through round-trip.
        
        **Validates: Requirements 3.8**
        """
        original = IPCMessage(type=msg_type, payload={})
        
        json_str = original.to_json()
        restored = IPCMessage.from_json(json_str)
        
        assert restored.payload == {}
    
    @given(msg_type=message_type_strategy)
    @settings(
        max_examples=100,
        deadline=None,
        suppress_health_check=[HealthCheck.too_slow],
    )
    def test_default_timestamp_generated(self, msg_type: str):
        """
        Feature: user-experience-enhancement, Property 5: IPC Message Round-Trip
        
        Messages SHALL have a timestamp generated by default.
        
        **Validates: Requirements 3.8**
        """
        message = IPCMessage(type=msg_type)
        
        # Timestamp should be set
        assert message.timestamp is not None
        assert len(message.timestamp) > 0
        
        # Should be valid ISO format
        try:
            datetime.fromisoformat(message.timestamp)
        except ValueError:
            pytest.fail(f"Timestamp '{message.timestamp}' is not valid ISO format")


class TestStatusPayloadRoundTrip:
    """
    Tests for status payload round-trip.
    
    **Validates: Requirements 3.8**
    """
    
    @given(
        status=st.sampled_from([s.value for s in BackupStatus]),
        last_backup=st.one_of(st.none(), st.text(min_size=1, max_size=50)),
        next_backup=st.one_of(st.none(), st.text(min_size=1, max_size=50)),
        total_snapshots=st.integers(min_value=0, max_value=10000),
        is_running=st.booleans(),
        destination_available=st.booleans(),
    )
    @settings(
        max_examples=100,
        deadline=None,
        suppress_health_check=[HealthCheck.too_slow],
    )
    def test_status_response_round_trip(
        self,
        status: str,
        last_backup: str,
        next_backup: str,
        total_snapshots: int,
        is_running: bool,
        destination_available: bool,
    ):
        """
        Feature: user-experience-enhancement, Property 5: IPC Message Round-Trip
        
        Status response payloads SHALL be preserved through round-trip.
        
        **Validates: Requirements 3.8**
        """
        payload = {
            "status": status,
            "last_backup": last_backup,
            "next_backup": next_backup,
            "total_snapshots": total_snapshots,
            "message": "Test message",
            "is_running": is_running,
            "destination_available": destination_available,
        }
        
        original = IPCMessage(
            type=MessageType.STATUS_RESPONSE.value,
            payload=payload,
        )
        
        # Round-trip through JSON
        json_str = original.to_json()
        restored = IPCMessage.from_json(json_str)
        
        # Verify all payload fields preserved
        assert restored.payload["status"] == status
        assert restored.payload["last_backup"] == last_backup
        assert restored.payload["next_backup"] == next_backup
        assert restored.payload["total_snapshots"] == total_snapshots
        assert restored.payload["is_running"] == is_running
        assert restored.payload["destination_available"] == destination_available


class TestIPCServerClientIntegration:
    """
    Integration tests for IPC server and client communication.
    
    These tests verify actual socket communication round-trip.
    
    **Validates: Requirements 3.8**
    """
    
    @pytest.fixture
    def temp_socket_path(self, tmp_path):
        """Create a temporary socket path for testing.
        
        Uses /tmp directly to avoid AF_UNIX path length limitations.
        """
        import tempfile
        import os
        # Use /tmp directly to keep path short (Unix socket paths have ~104 char limit)
        socket_dir = tempfile.mkdtemp(prefix="ipc_", dir="/tmp")
        socket_path = Path(socket_dir) / "s.sock"
        yield socket_path
        # Cleanup
        if socket_path.exists():
            socket_path.unlink()
        if Path(socket_dir).exists():
            os.rmdir(socket_dir)
    
    @pytest.mark.asyncio
    async def test_server_client_round_trip(self, temp_socket_path):
        """
        Feature: user-experience-enhancement, Property 5: IPC Message Round-Trip
        
        Messages sent through actual socket SHALL be preserved.
        
        **Validates: Requirements 3.8**
        """
        # Create server with echo handler
        server = IPCServer(socket_path=temp_socket_path)
        
        async def echo_handler(message: IPCMessage) -> IPCMessage:
            return IPCMessage(
                type=MessageType.STATUS_RESPONSE.value,
                payload=message.payload,
                message_id=message.message_id,
            )
        
        server.register_handler(MessageType.STATUS_REQUEST.value, echo_handler)
        
        # Start server
        await server.start()
        
        try:
            # Create client
            client = IPCClient(socket_path=temp_socket_path, timeout=5.0)
            
            # Send message
            test_payload = {
                "test_key": "test_value",
                "number": 42,
                "nested": {"a": 1, "b": 2},
            }
            
            request = IPCMessage(
                type=MessageType.STATUS_REQUEST.value,
                payload=test_payload,
                message_id="test-123",
            )
            
            response = await client.send_message(request)
            
            # Verify response
            assert response.type == MessageType.STATUS_RESPONSE.value
            assert response.payload == test_payload
            assert response.message_id == "test-123"
            
        finally:
            await server.stop()
    
    @pytest.mark.asyncio
    async def test_multiple_messages_round_trip(self, temp_socket_path):
        """
        Feature: user-experience-enhancement, Property 5: IPC Message Round-Trip
        
        Multiple sequential messages SHALL all be preserved.
        
        **Validates: Requirements 3.8**
        """
        # Create server with echo handler
        server = IPCServer(socket_path=temp_socket_path)
        
        async def echo_handler(message: IPCMessage) -> IPCMessage:
            return IPCMessage(
                type=MessageType.STATUS_RESPONSE.value,
                payload=message.payload,
                message_id=message.message_id,
            )
        
        server.register_handler(MessageType.STATUS_REQUEST.value, echo_handler)
        
        await server.start()
        
        try:
            client = IPCClient(socket_path=temp_socket_path, timeout=5.0)
            
            # Send multiple messages
            for i in range(5):
                test_payload = {"iteration": i, "data": f"test_{i}"}
                
                request = IPCMessage(
                    type=MessageType.STATUS_REQUEST.value,
                    payload=test_payload,
                    message_id=f"msg-{i}",
                )
                
                response = await client.send_message(request)
                
                assert response.payload == test_payload
                assert response.message_id == f"msg-{i}"
                
        finally:
            await server.stop()
    
    @pytest.mark.asyncio
    async def test_connection_to_nonexistent_socket_raises_error(self, temp_socket_path):
        """
        Feature: user-experience-enhancement, Property 5: IPC Message Round-Trip
        
        Connection to nonexistent socket SHALL raise IPCError.
        
        **Validates: Requirements 3.8**
        """
        # Use a path that definitely doesn't exist but is short enough
        nonexistent_path = Path("/tmp/nonexistent_ipc_test.sock")
        client = IPCClient(socket_path=nonexistent_path, timeout=1.0)
        
        request = IPCMessage(
            type=MessageType.STATUS_REQUEST.value,
            payload={},
        )
        
        with pytest.raises(IPCError):
            await client.send_message(request)


class TestSpecialCharactersRoundTrip:
    """
    Tests for handling special characters in messages.
    
    **Validates: Requirements 3.8**
    """
    
    @given(
        text=st.text(
            alphabet=st.characters(
                whitelist_categories=('L', 'N', 'P', 'S', 'Z'),
                whitelist_characters='\n\t\r',
            ),
            min_size=0,
            max_size=1000,
        ),
    )
    @settings(
        max_examples=100,
        deadline=None,
        suppress_health_check=[HealthCheck.too_slow],
    )
    def test_special_characters_preserved(self, text: str):
        """
        Feature: user-experience-enhancement, Property 5: IPC Message Round-Trip
        
        Special characters in payload SHALL be preserved through round-trip.
        
        **Validates: Requirements 3.8**
        """
        # Skip if text contains characters that break JSON
        assume('\x00' not in text)
        
        payload = {"message": text, "path": f"/path/to/{text}"}
        
        original = IPCMessage(
            type=MessageType.STATUS_RESPONSE.value,
            payload=payload,
        )
        
        # Round-trip
        json_str = original.to_json()
        restored = IPCMessage.from_json(json_str)
        
        assert restored.payload["message"] == text
    
    @given(
        unicode_text=st.text(
            alphabet=st.characters(
                whitelist_categories=('L', 'N', 'P', 'S'),
            ),
            min_size=1,
            max_size=100,
        ),
    )
    @settings(
        max_examples=100,
        deadline=None,
        suppress_health_check=[HealthCheck.too_slow],
    )
    def test_unicode_preserved(self, unicode_text: str):
        """
        Feature: user-experience-enhancement, Property 5: IPC Message Round-Trip
        
        Unicode characters SHALL be preserved through round-trip.
        
        **Validates: Requirements 3.8**
        """
        # Skip null characters
        assume('\x00' not in unicode_text)
        
        payload = {"text": unicode_text}
        
        original = IPCMessage(
            type=MessageType.STATUS_RESPONSE.value,
            payload=payload,
        )
        
        # Round-trip through bytes (simulates socket transmission)
        data = original.to_bytes()
        restored = IPCMessage.from_bytes(data)
        
        assert restored.payload["text"] == unicode_text
