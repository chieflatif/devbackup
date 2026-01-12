"""IPC system for menu bar communication.

This module provides Inter-Process Communication (IPC) between the devbackup
daemon and the macOS menu bar application using Unix domain sockets.

The IPC system enables:
- Status queries from the menu bar app
- Triggering immediate backups from the menu bar
- Opening backup locations in Finder

Requirements: 3.4, 3.5, 3.8
"""

import asyncio
import json
import os
import stat
from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Union
import logging


# Default socket path with user-only permissions
DEFAULT_SOCKET_PATH = Path.home() / ".cache" / "devbackup" / "ipc.sock"


class MessageType(str, Enum):
    """Types of IPC messages."""
    # Requests (from menu bar to daemon)
    STATUS_REQUEST = "status_request"
    BACKUP_TRIGGER = "backup_trigger"
    BROWSE_REQUEST = "browse_request"
    
    # Responses (from daemon to menu bar)
    STATUS_RESPONSE = "status_response"
    BACKUP_RESPONSE = "backup_response"
    BROWSE_RESPONSE = "browse_response"
    ERROR_RESPONSE = "error_response"


class BackupStatus(str, Enum):
    """Backup status states for menu bar display."""
    PROTECTED = "protected"  # Green checkmark - backups up to date
    BACKING_UP = "backing_up"  # Blue animated - backup in progress
    WARNING = "warning"  # Yellow - backup overdue or drive disconnected
    ERROR = "error"  # Red - backup failed


@dataclass
class IPCMessage:
    """Message format for IPC communication.
    
    All communication between the menu bar app and backup daemon
    uses this message format, serialized as JSON.
    
    Attributes:
        type: The message type (request or response)
        payload: Message-specific data dictionary
        timestamp: When the message was created (ISO format)
        message_id: Optional unique identifier for request/response matching
    
    Requirements: 3.8
    """
    type: str
    payload: Dict[str, Any] = field(default_factory=dict)
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    message_id: Optional[str] = None
    
    def to_json(self) -> str:
        """Serialize message to JSON string."""
        return json.dumps(asdict(self), default=str)
    
    @classmethod
    def from_json(cls, json_str: str) -> "IPCMessage":
        """Deserialize message from JSON string.
        
        Args:
            json_str: JSON string representation of message
            
        Returns:
            IPCMessage instance
            
        Raises:
            ValueError: If JSON is invalid or missing required fields
        """
        try:
            data = json.loads(json_str)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON: {e}")
        
        if "type" not in data:
            raise ValueError("Message missing required 'type' field")
        
        return cls(
            type=data["type"],
            payload=data.get("payload", {}),
            timestamp=data.get("timestamp", datetime.now().isoformat()),
            message_id=data.get("message_id"),
        )
    
    def to_bytes(self) -> bytes:
        """Serialize message to bytes for socket transmission.
        
        Format: JSON string followed by newline delimiter.
        """
        return (self.to_json() + "\n").encode("utf-8")
    
    @classmethod
    def from_bytes(cls, data: bytes) -> "IPCMessage":
        """Deserialize message from bytes.
        
        Args:
            data: Bytes received from socket
            
        Returns:
            IPCMessage instance
        """
        return cls.from_json(data.decode("utf-8").strip())


@dataclass
class StatusPayload:
    """Payload for status response messages.
    
    Attributes:
        status: Current backup status (protected, backing_up, warning, error)
        last_backup: Human-friendly last backup time (e.g., "2 hours ago")
        next_backup: Human-friendly next backup time (e.g., "in 58 minutes")
        total_snapshots: Number of backup versions available
        message: User-friendly status message
        is_running: Whether a backup is currently in progress
        destination_available: Whether the backup destination is accessible
    """
    status: str
    last_backup: Optional[str] = None
    next_backup: Optional[str] = None
    total_snapshots: int = 0
    message: str = "Your files are safe"
    is_running: bool = False
    destination_available: bool = True
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return asdict(self)


class IPCError(Exception):
    """Exception raised for IPC communication errors."""
    pass


class IPCServer:
    """Unix socket server for menu bar communication.
    
    Provides a local IPC server that the menu bar app can connect to
    for querying backup status and triggering operations.
    
    The socket is created with user-only permissions (mode 0600) for security.
    
    Requirements: 3.8
    """
    
    def __init__(
        self,
        socket_path: Optional[Path] = None,
        logger: Optional[logging.Logger] = None,
    ):
        """Initialize the IPC server.
        
        Args:
            socket_path: Path for Unix socket. Defaults to ~/.cache/devbackup/ipc.sock
            logger: Optional logger instance
        """
        self.socket_path = socket_path or DEFAULT_SOCKET_PATH
        self.logger = logger or logging.getLogger(__name__)
        self._server: Optional[asyncio.AbstractServer] = None
        self._handlers: Dict[str, Callable] = {}
        self._running = False
        
        # Register default handlers
        self._register_default_handlers()
    
    def _register_default_handlers(self) -> None:
        """Register default message handlers."""
        self._handlers[MessageType.STATUS_REQUEST.value] = self._default_status_handler
        self._handlers[MessageType.BACKUP_TRIGGER.value] = self._default_backup_handler
        self._handlers[MessageType.BROWSE_REQUEST.value] = self._default_browse_handler
    
    async def _default_status_handler(self, message: IPCMessage) -> IPCMessage:
        """Default handler for status requests.
        
        This is a placeholder that returns a basic status.
        The actual implementation should be provided by the backup system.
        """
        return IPCMessage(
            type=MessageType.STATUS_RESPONSE.value,
            payload={
                "status": BackupStatus.PROTECTED.value,
                "last_backup": None,
                "next_backup": None,
                "total_snapshots": 0,
                "message": "Status handler not configured",
                "is_running": False,
                "destination_available": True,
            },
            message_id=message.message_id,
        )
    
    async def _default_backup_handler(self, message: IPCMessage) -> IPCMessage:
        """Default handler for backup trigger requests."""
        return IPCMessage(
            type=MessageType.BACKUP_RESPONSE.value,
            payload={
                "success": False,
                "message": "Backup handler not configured",
            },
            message_id=message.message_id,
        )
    
    async def _default_browse_handler(self, message: IPCMessage) -> IPCMessage:
        """Default handler for browse requests."""
        return IPCMessage(
            type=MessageType.BROWSE_RESPONSE.value,
            payload={
                "success": False,
                "path": None,
                "message": "Browse handler not configured",
            },
            message_id=message.message_id,
        )
    
    def register_handler(
        self,
        message_type: Union[str, MessageType],
        handler: Callable[[IPCMessage], "asyncio.Future[IPCMessage]"],
    ) -> None:
        """Register a handler for a message type.
        
        Args:
            message_type: The message type to handle
            handler: Async function that takes IPCMessage and returns IPCMessage
        """
        if isinstance(message_type, MessageType):
            message_type = message_type.value
        self._handlers[message_type] = handler
    
    async def _handle_client(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        """Handle a client connection.
        
        Reads messages from the client, dispatches to handlers,
        and sends responses back.
        """
        client_addr = writer.get_extra_info("peername")
        self.logger.debug(f"Client connected: {client_addr}")
        
        try:
            while True:
                # Read until newline delimiter
                data = await reader.readline()
                if not data:
                    break
                
                try:
                    message = IPCMessage.from_bytes(data)
                    self.logger.debug(f"Received message: {message.type}")
                    
                    # Dispatch to handler
                    handler = self._handlers.get(message.type)
                    if handler:
                        response = await handler(message)
                    else:
                        response = IPCMessage(
                            type=MessageType.ERROR_RESPONSE.value,
                            payload={
                                "error": "UNKNOWN_MESSAGE_TYPE",
                                "message": f"Unknown message type: {message.type}",
                            },
                            message_id=message.message_id,
                        )
                    
                    # Send response
                    writer.write(response.to_bytes())
                    await writer.drain()
                    
                except ValueError as e:
                    self.logger.warning(f"Invalid message: {e}")
                    error_response = IPCMessage(
                        type=MessageType.ERROR_RESPONSE.value,
                        payload={
                            "error": "INVALID_MESSAGE",
                            "message": str(e),
                        },
                    )
                    writer.write(error_response.to_bytes())
                    await writer.drain()
                    
        except asyncio.CancelledError:
            pass
        except Exception as e:
            self.logger.error(f"Error handling client: {e}")
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass
            self.logger.debug(f"Client disconnected: {client_addr}")
    
    def _ensure_socket_directory(self) -> None:
        """Ensure the socket directory exists with proper permissions."""
        socket_dir = self.socket_path.parent
        socket_dir.mkdir(parents=True, exist_ok=True)
        
        # Set directory permissions to user-only (700)
        os.chmod(socket_dir, stat.S_IRWXU)
    
    def _cleanup_socket(self) -> None:
        """Remove existing socket file if present.
        
        Also handles stale sockets from crashed processes by checking
        if the socket is actually in use before removing it.
        """
        if self.socket_path.exists():
            # Check if socket is stale (no process listening)
            if self._is_socket_stale():
                self.logger.info(f"Removing stale socket: {self.socket_path}")
            try:
                self.socket_path.unlink()
            except OSError as e:
                self.logger.warning(f"Could not remove existing socket: {e}")
    
    def _is_socket_stale(self) -> bool:
        """Check if an existing socket is stale (no process listening).
        
        Attempts a quick connection to the socket. If it fails with
        connection refused, the socket is stale.
        
        Returns:
            True if socket exists but no process is listening
        """
        if not self.socket_path.exists():
            return False
        
        import socket
        
        try:
            # Try to connect to the socket
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            sock.settimeout(0.5)  # Quick timeout
            sock.connect(str(self.socket_path))
            sock.close()
            # Connection succeeded - socket is in use
            return False
        except ConnectionRefusedError:
            # No process listening - socket is stale
            return True
        except FileNotFoundError:
            # Socket doesn't exist
            return False
        except OSError:
            # Other error - assume stale to be safe
            return True
    
    async def start(self) -> None:
        """Start the IPC server.
        
        Creates the Unix socket and begins accepting connections.
        The socket is created with user-only permissions (mode 0600).
        
        Raises:
            IPCError: If server cannot be started
        """
        if self._running:
            return
        
        try:
            self._ensure_socket_directory()
            self._cleanup_socket()
            
            # Create Unix socket server
            self._server = await asyncio.start_unix_server(
                self._handle_client,
                path=str(self.socket_path),
            )
            
            # Set socket permissions to user-only (600)
            os.chmod(self.socket_path, stat.S_IRUSR | stat.S_IWUSR)
            
            self._running = True
            self.logger.info(f"IPC server started on {self.socket_path}")
            
        except Exception as e:
            raise IPCError(f"Failed to start IPC server: {e}")
    
    async def stop(self) -> None:
        """Stop the IPC server and clean up resources."""
        if not self._running:
            return
        
        self._running = False
        
        if self._server:
            self._server.close()
            await self._server.wait_closed()
            self._server = None
        
        self._cleanup_socket()
        self.logger.info("IPC server stopped")
    
    async def serve_forever(self) -> None:
        """Run the server until stopped.
        
        This is a convenience method that starts the server and
        waits for it to be stopped.
        """
        await self.start()
        
        if self._server:
            await self._server.serve_forever()
    
    @property
    def is_running(self) -> bool:
        """Check if the server is currently running."""
        return self._running


class IPCClient:
    """Client for connecting to the IPC server.
    
    Used by the menu bar app to communicate with the backup daemon.
    
    Requirements: 3.8
    """
    
    def __init__(
        self,
        socket_path: Optional[Path] = None,
        timeout: float = 5.0,
    ):
        """Initialize the IPC client.
        
        Args:
            socket_path: Path to Unix socket. Defaults to ~/.cache/devbackup/ipc.sock
            timeout: Connection and read timeout in seconds
        """
        self.socket_path = socket_path or DEFAULT_SOCKET_PATH
        self.timeout = timeout
    
    async def send_message(self, message: IPCMessage) -> IPCMessage:
        """Send a message and wait for response.
        
        Args:
            message: Message to send
            
        Returns:
            Response message from server
            
        Raises:
            IPCError: If connection fails or times out
        """
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_unix_connection(str(self.socket_path)),
                timeout=self.timeout,
            )
        except asyncio.TimeoutError:
            raise IPCError("Connection timed out")
        except FileNotFoundError:
            raise IPCError(f"IPC socket not found: {self.socket_path}")
        except ConnectionRefusedError:
            raise IPCError("Connection refused - is the backup daemon running?")
        except Exception as e:
            raise IPCError(f"Connection failed: {e}")
        
        try:
            # Send message
            writer.write(message.to_bytes())
            await writer.drain()
            
            # Read response
            data = await asyncio.wait_for(
                reader.readline(),
                timeout=self.timeout,
            )
            
            if not data:
                raise IPCError("Server closed connection without response")
            
            return IPCMessage.from_bytes(data)
            
        except asyncio.TimeoutError:
            raise IPCError("Response timed out")
        except ValueError as e:
            raise IPCError(f"Invalid response: {e}")
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass
    
    async def request_status(self) -> Dict[str, Any]:
        """Request current backup status.
        
        Returns:
            Status dictionary with keys:
            - status: "protected", "backing_up", "warning", or "error"
            - last_backup: Human-friendly last backup time
            - next_backup: Human-friendly next backup time
            - total_snapshots: Number of backup versions
            - message: User-friendly status message
            
        Raises:
            IPCError: If request fails
            
        Requirements: 3.4
        """
        message = IPCMessage(
            type=MessageType.STATUS_REQUEST.value,
            payload={},
        )
        
        response = await self.send_message(message)
        
        if response.type == MessageType.ERROR_RESPONSE.value:
            raise IPCError(response.payload.get("message", "Unknown error"))
        
        return response.payload
    
    async def trigger_backup(self) -> Dict[str, Any]:
        """Trigger an immediate backup.
        
        Returns:
            Result dictionary with keys:
            - success: Whether backup was triggered
            - message: User-friendly result message
            
        Raises:
            IPCError: If request fails
            
        Requirements: 3.4
        """
        message = IPCMessage(
            type=MessageType.BACKUP_TRIGGER.value,
            payload={},
        )
        
        response = await self.send_message(message)
        
        if response.type == MessageType.ERROR_RESPONSE.value:
            raise IPCError(response.payload.get("message", "Unknown error"))
        
        return response.payload
    
    async def request_browse_path(self) -> Dict[str, Any]:
        """Request the backup location path for browsing.
        
        Returns:
            Result dictionary with keys:
            - success: Whether path was retrieved
            - path: Path to backup location
            - message: User-friendly message
            
        Raises:
            IPCError: If request fails
            
        Requirements: 3.5
        """
        message = IPCMessage(
            type=MessageType.BROWSE_REQUEST.value,
            payload={},
        )
        
        response = await self.send_message(message)
        
        if response.type == MessageType.ERROR_RESPONSE.value:
            raise IPCError(response.payload.get("message", "Unknown error"))
        
        return response.payload



class IPCHandlers:
    """IPC handlers that integrate with the backup system.
    
    This class provides handler implementations that connect the IPC
    server to the actual backup functionality.
    
    Requirements: 3.4, 3.5
    """
    
    def __init__(
        self,
        config_path: Optional[Path] = None,
        logger: Optional[logging.Logger] = None,
    ):
        """Initialize handlers with backup system integration.
        
        Args:
            config_path: Path to configuration file
            logger: Optional logger instance
        """
        self.config_path = config_path
        self.logger = logger or logging.getLogger(__name__)
        self._config = None
    
    def _load_config(self):
        """Load configuration lazily."""
        if self._config is None:
            from devbackup.config import parse_config, ConfigurationError
            try:
                self._config = parse_config(self.config_path)
            except ConfigurationError:
                self._config = None
        return self._config
    
    async def handle_status_request(self, message: IPCMessage) -> IPCMessage:
        """Handle status request from menu bar.
        
        Returns current backup status including:
        - Overall status (protected, backing_up, warning, error)
        - Last backup time in friendly format
        - Next scheduled backup time
        - Total number of snapshots
        - User-friendly status message
        
        Requirements: 3.4
        """
        from devbackup.language import PlainLanguageTranslator
        
        translator = PlainLanguageTranslator()
        
        config = self._load_config()
        if config is None:
            return IPCMessage(
                type=MessageType.STATUS_RESPONSE.value,
                payload={
                    "status": BackupStatus.WARNING.value,
                    "last_backup": None,
                    "next_backup": None,
                    "total_snapshots": 0,
                    "message": "Backups not configured yet",
                    "is_running": False,
                    "destination_available": False,
                },
                message_id=message.message_id,
            )
        
        try:
            from devbackup.lock import LockManager
            from devbackup.snapshot import SnapshotEngine
            from devbackup.scheduler import Scheduler, SchedulerType
            from datetime import timedelta
            
            # Check if backup is running
            lock_manager = LockManager()
            is_running = lock_manager.is_locked()
            
            # Check destination availability
            destination_available = config.backup_destination.exists()
            
            # Get snapshot info
            snapshot_engine = SnapshotEngine(
                destination=config.backup_destination,
                exclude_patterns=config.exclude_patterns,
            )
            snapshots = snapshot_engine.list_snapshots()
            total_snapshots = len(snapshots)
            
            # Get last backup time
            last_backup = None
            last_backup_time = None
            if snapshots:
                last_backup_time = snapshots[0].timestamp
                last_backup = translator.translate_time(last_backup_time)
            
            # Get scheduler status and next backup time
            scheduler = Scheduler(
                scheduler_type=SchedulerType(config.scheduler.type),
                interval_seconds=config.scheduler.interval_seconds,
            )
            scheduler_status = scheduler.get_status()
            scheduler_installed = scheduler_status.get("installed", False)
            
            next_backup = None
            if scheduler_installed and last_backup_time:
                next_time = last_backup_time + timedelta(seconds=config.scheduler.interval_seconds)
                next_backup = translator._translate_future_time(next_time, datetime.now())
            
            # Determine overall status
            if is_running:
                status = BackupStatus.BACKING_UP.value
                status_message = "Backing up your files right now"
            elif not destination_available:
                status = BackupStatus.WARNING.value
                status_message = "Your backup drive isn't connected"
            elif not snapshots:
                status = BackupStatus.WARNING.value
                status_message = "No backups yet - run your first backup"
            elif last_backup_time:
                # Check if backup is overdue (more than 2x interval)
                time_since_backup = (datetime.now() - last_backup_time).total_seconds()
                if time_since_backup > config.scheduler.interval_seconds * 2:
                    status = BackupStatus.WARNING.value
                    status_message = "Your backup is overdue"
                else:
                    status = BackupStatus.PROTECTED.value
                    status_message = "Your files are safe"
            else:
                status = BackupStatus.PROTECTED.value
                status_message = "Your files are safe"
            
            return IPCMessage(
                type=MessageType.STATUS_RESPONSE.value,
                payload={
                    "status": status,
                    "last_backup": last_backup,
                    "next_backup": next_backup,
                    "total_snapshots": total_snapshots,
                    "message": status_message,
                    "is_running": is_running,
                    "destination_available": destination_available,
                },
                message_id=message.message_id,
            )
            
        except Exception as e:
            self.logger.error(f"Error getting status: {e}")
            return IPCMessage(
                type=MessageType.STATUS_RESPONSE.value,
                payload={
                    "status": BackupStatus.ERROR.value,
                    "last_backup": None,
                    "next_backup": None,
                    "total_snapshots": 0,
                    "message": "Couldn't check backup status",
                    "is_running": False,
                    "destination_available": False,
                },
                message_id=message.message_id,
            )
    
    async def handle_backup_trigger(self, message: IPCMessage) -> IPCMessage:
        """Handle backup trigger request from menu bar.
        
        Triggers an immediate backup and returns the result.
        
        Requirements: 3.4
        """
        config = self._load_config()
        if config is None:
            return IPCMessage(
                type=MessageType.BACKUP_RESPONSE.value,
                payload={
                    "success": False,
                    "message": "Backups not configured yet. Set up backups first.",
                },
                message_id=message.message_id,
            )
        
        try:
            from devbackup.backup import run_backup
            
            # Run backup in executor to avoid blocking
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                None,
                lambda: run_backup(config_path=self.config_path, config=config)
            )
            
            if result.success:
                return IPCMessage(
                    type=MessageType.BACKUP_RESPONSE.value,
                    payload={
                        "success": True,
                        "message": "All done! Your projects are safely backed up.",
                        "snapshot": result.snapshot_result.snapshot_path.name if result.snapshot_result and result.snapshot_result.snapshot_path else None,
                        "files_transferred": result.snapshot_result.files_transferred if result.snapshot_result else 0,
                    },
                    message_id=message.message_id,
                )
            else:
                # Translate error to friendly message
                from devbackup.language import PlainLanguageTranslator
                translator = PlainLanguageTranslator()
                
                error_msg = result.error_message or "Unknown error"
                friendly_msg = translator.sanitize_output(error_msg)
                
                return IPCMessage(
                    type=MessageType.BACKUP_RESPONSE.value,
                    payload={
                        "success": False,
                        "message": f"Backup couldn't complete: {friendly_msg}",
                    },
                    message_id=message.message_id,
                )
                
        except Exception as e:
            self.logger.error(f"Error triggering backup: {e}")
            return IPCMessage(
                type=MessageType.BACKUP_RESPONSE.value,
                payload={
                    "success": False,
                    "message": "Something went wrong. Try again in a moment.",
                },
                message_id=message.message_id,
            )
    
    async def handle_browse_request(self, message: IPCMessage) -> IPCMessage:
        """Handle browse request from menu bar.
        
        Returns the backup location path for opening in Finder.
        
        Requirements: 3.5
        """
        config = self._load_config()
        if config is None:
            return IPCMessage(
                type=MessageType.BROWSE_RESPONSE.value,
                payload={
                    "success": False,
                    "path": None,
                    "message": "Backups not configured yet.",
                },
                message_id=message.message_id,
            )
        
        backup_path = config.backup_destination
        
        if not backup_path.exists():
            return IPCMessage(
                type=MessageType.BROWSE_RESPONSE.value,
                payload={
                    "success": False,
                    "path": str(backup_path),
                    "message": "Your backup drive isn't connected right now.",
                },
                message_id=message.message_id,
            )
        
        return IPCMessage(
            type=MessageType.BROWSE_RESPONSE.value,
            payload={
                "success": True,
                "path": str(backup_path),
                "message": "Opening your backups folder...",
            },
            message_id=message.message_id,
        )
    
    def register_with_server(self, server: IPCServer) -> None:
        """Register all handlers with an IPC server.
        
        Args:
            server: The IPCServer to register handlers with
        """
        server.register_handler(
            MessageType.STATUS_REQUEST,
            self.handle_status_request,
        )
        server.register_handler(
            MessageType.BACKUP_TRIGGER,
            self.handle_backup_trigger,
        )
        server.register_handler(
            MessageType.BROWSE_REQUEST,
            self.handle_browse_request,
        )


def create_configured_server(
    config_path: Optional[Path] = None,
    socket_path: Optional[Path] = None,
    logger: Optional[logging.Logger] = None,
) -> IPCServer:
    """Create an IPC server with handlers configured for the backup system.
    
    This is a convenience function that creates an IPCServer with
    all handlers properly configured to integrate with devbackup.
    
    Args:
        config_path: Path to configuration file
        socket_path: Path for Unix socket
        logger: Optional logger instance
        
    Returns:
        Configured IPCServer ready to start
        
    Requirements: 3.8
    """
    server = IPCServer(socket_path=socket_path, logger=logger)
    handlers = IPCHandlers(config_path=config_path, logger=logger)
    handlers.register_with_server(server)
    return server
