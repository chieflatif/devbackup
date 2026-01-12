"""MCP Server for devbackup - enables AI agent integration.

This module provides the DevBackupMCPServer class that exposes backup
functionality to AI agents like Cursor via the Model Context Protocol (MCP).

Tools exposed:
- backup_run: Trigger an immediate backup
- backup_status: Get current backup status
- backup_list_snapshots: List all available snapshots
- backup_restore: Restore a file or directory from a snapshot
- backup_diff: Show differences between snapshot and current state
- backup_search: Search for files across snapshots

Requirements: 10.1-10.10
"""

import asyncio
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import Tool, TextContent

from devbackup.backup import run_backup, BackupResult
from devbackup.config import (
    Configuration,
    ConfigurationError,
    ValidationError,
    parse_config,
    format_config,
    DEFAULT_CONFIG_PATH,
)
from devbackup.defaults import SmartDefaults
from devbackup.discovery import AutoDiscovery, DiscoveredProject, DiscoveredDestination
from devbackup.health import HealthChecker
from devbackup.language import PlainLanguageTranslator
from devbackup.lock import LockManager
from devbackup.scheduler import Scheduler, SchedulerType
from devbackup.snapshot import SnapshotEngine
from devbackup.verify import IntegrityVerifier


class DevBackupMCPServer:
    """
    MCP Server exposing backup functionality to AI agents like Cursor.
    
    All tools return structured JSON responses as required by Requirements 10.7.
    Error responses include code and message as required by Requirements 10.8.
    The server respects the same locking mechanism as the CLI (Requirements 10.9).
    Configuration is loaded from the same config.toml file (Requirements 10.10).
    """
    
    def __init__(self, config_path: Optional[Path] = None):
        """
        Initialize the MCP server.
        
        Args:
            config_path: Path to configuration file. Defaults to ~/.config/devbackup/config.toml
        """
        self.config_path = config_path
        self._config: Optional[Configuration] = None
        self.server = Server("devbackup")
        self._translator = PlainLanguageTranslator()
        self._discovery = AutoDiscovery()
        self._defaults = SmartDefaults()
        self._register_tools()
    
    def _load_config(self) -> Configuration:
        """
        Load configuration from file.
        
        Returns:
            Configuration object
        
        Raises:
            ConfigurationError: If config file is missing or invalid
            ValidationError: If config values have wrong types
        """
        if self._config is None:
            self._config = parse_config(self.config_path)
        return self._config
    
    def _error_response(self, code: str, message: str) -> str:
        """
        Create a JSON error response.
        
        Args:
            code: Error code (e.g., "CONFIG_ERROR", "LOCK_ERROR")
            message: Human-readable error message
        
        Returns:
            JSON string with error object
        """
        return json.dumps({
            "error": {
                "code": code,
                "message": message
            }
        }, indent=2)
    
    def _success_response(self, data: Dict[str, Any]) -> str:
        """
        Create a JSON success response.
        
        Args:
            data: Response data dictionary
        
        Returns:
            JSON string with response data
        """
        return json.dumps(data, indent=2, default=str)
    
    def _register_tools(self):
        """Register all MCP tools with the server."""
        
        @self.server.list_tools()
        async def list_tools() -> List[Tool]:
            """Return list of available tools."""
            return [
                Tool(
                    name="backup_run",
                    description="Trigger an immediate backup. Returns JSON with success status, snapshot timestamp, files transferred, and duration.",
                    inputSchema={
                        "type": "object",
                        "properties": {},
                        "required": []
                    }
                ),
                Tool(
                    name="backup_status",
                    description="Get current backup status including last backup time, next scheduled backup, and whether a backup is currently running.",
                    inputSchema={
                        "type": "object",
                        "properties": {},
                        "required": []
                    }
                ),
                Tool(
                    name="backup_list_snapshots",
                    description="List all available snapshots with timestamps, sizes, and file counts.",
                    inputSchema={
                        "type": "object",
                        "properties": {},
                        "required": []
                    }
                ),
                Tool(
                    name="backup_restore",
                    description="Restore a file or directory from a snapshot to the original location or a specified destination.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "snapshot": {
                                "type": "string",
                                "description": "Snapshot timestamp (YYYY-MM-DD-HHMMSS)"
                            },
                            "path": {
                                "type": "string",
                                "description": "Path within snapshot to restore (relative to snapshot root)"
                            },
                            "destination": {
                                "type": "string",
                                "description": "Optional destination path. If not provided, restores to original location."
                            }
                        },
                        "required": ["snapshot", "path"]
                    }
                ),
                Tool(
                    name="backup_diff",
                    description="Show differences between a snapshot and the current state of source directories.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "snapshot": {
                                "type": "string",
                                "description": "Snapshot timestamp (YYYY-MM-DD-HHMMSS)"
                            },
                            "path": {
                                "type": "string",
                                "description": "Optional specific path to compare"
                            }
                        },
                        "required": ["snapshot"]
                    }
                ),
                Tool(
                    name="backup_search",
                    description="Search for files matching a pattern across all snapshots or a specific snapshot.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "pattern": {
                                "type": "string",
                                "description": "Glob pattern or filename to search for (e.g., '*.py', 'config.json')"
                            },
                            "snapshot": {
                                "type": "string",
                                "description": "Optional specific snapshot to search (YYYY-MM-DD-HHMMSS). If not provided, searches all snapshots."
                            }
                        },
                        "required": ["pattern"]
                    }
                ),
                Tool(
                    name="backup_progress",
                    description="Get current backup progress if a backup is running. Returns progress information including files transferred, bytes transferred, transfer rate, and percent complete.",
                    inputSchema={
                        "type": "object",
                        "properties": {},
                        "required": []
                    }
                ),
                Tool(
                    name="backup_verify",
                    description="Verify the integrity of a backup snapshot by checking file checksums against the manifest. Returns verification status including any missing or corrupted files.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "snapshot": {
                                "type": "string",
                                "description": "Snapshot timestamp (YYYY-MM-DD-HHMMSS)"
                            },
                            "pattern": {
                                "type": "string",
                                "description": "Optional glob pattern to filter files to verify (e.g., '*.py')"
                            }
                        },
                        "required": ["snapshot"]
                    }
                ),
                Tool(
                    name="backup_health",
                    description="Check health of all backup snapshots. Returns health status including readability, manifest validity, and any corrupted or missing files.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "min_age_days": {
                                "type": "integer",
                                "description": "Only check snapshots older than N days (optional)"
                            }
                        },
                        "required": []
                    }
                ),
                Tool(
                    name="backup_setup",
                    description="Interactive zero-config setup for backups. Discovers projects and destinations automatically. Call without arguments to start discovery, then call again with confirm_projects and confirm_destination to complete setup.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "workspace_path": {
                                "type": "string",
                                "description": "Optional current workspace path to prioritize in discovery"
                            },
                            "confirm_projects": {
                                "type": "array",
                                "items": {"type": "string"},
                                "description": "List of project paths to confirm for backup (from discovery stage)"
                            },
                            "confirm_destination": {
                                "type": "string",
                                "description": "Destination path to confirm for backup (from discovery stage)"
                            }
                        },
                        "required": []
                    }
                ),
                Tool(
                    name="backup_explain",
                    description="Explain backup status or concepts in plain, friendly language. Use this to help users understand their backup situation.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "topic": {
                                "type": "string",
                                "description": "Topic to explain: 'status', 'snapshots', 'restore', 'schedule', 'storage', or omit for general overview"
                            }
                        },
                        "required": []
                    }
                ),
                Tool(
                    name="backup_find_file",
                    description="Find files in backups using natural language descriptions. Helps users locate previous versions of files.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "description": {
                                "type": "string",
                                "description": "Natural description of the file (e.g., 'the config file I was editing yesterday', 'app.py')"
                            },
                            "time_hint": {
                                "type": "string",
                                "description": "Optional time hint like 'yesterday', 'last week', 'before the crash'"
                            }
                        },
                        "required": ["description"]
                    }
                ),
                Tool(
                    name="backup_undo",
                    description="Restore the most recent version of a file. Shows a preview before restoring and places recovered files on the Desktop by default.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "file_path": {
                                "type": "string",
                                "description": "Path to the file to restore (optional - uses context if not provided)"
                            },
                            "confirm": {
                                "type": "boolean",
                                "description": "Set to true to proceed with restore after preview"
                            }
                        },
                        "required": []
                    }
                ),
            ]
        
        @self.server.call_tool()
        async def call_tool(name: str, arguments: Dict[str, Any]) -> List[TextContent]:
            """Dispatch tool calls to appropriate handlers."""
            try:
                if name == "backup_run":
                    result = await self._tool_backup_run()
                elif name == "backup_status":
                    result = await self._tool_backup_status()
                elif name == "backup_list_snapshots":
                    result = await self._tool_backup_list_snapshots()
                elif name == "backup_restore":
                    result = await self._tool_backup_restore(
                        snapshot=arguments.get("snapshot", ""),
                        path=arguments.get("path", ""),
                        destination=arguments.get("destination"),
                    )
                elif name == "backup_diff":
                    result = await self._tool_backup_diff(
                        snapshot=arguments.get("snapshot", ""),
                        path=arguments.get("path"),
                    )
                elif name == "backup_search":
                    result = await self._tool_backup_search(
                        pattern=arguments.get("pattern", ""),
                        snapshot=arguments.get("snapshot"),
                    )
                elif name == "backup_progress":
                    result = await self._tool_backup_progress()
                elif name == "backup_verify":
                    result = await self._tool_backup_verify(
                        snapshot=arguments.get("snapshot", ""),
                        pattern=arguments.get("pattern"),
                    )
                elif name == "backup_health":
                    result = await self._tool_backup_health(
                        min_age_days=arguments.get("min_age_days"),
                    )
                elif name == "backup_setup":
                    result = await self._tool_backup_setup(
                        workspace_path=arguments.get("workspace_path"),
                        confirm_projects=arguments.get("confirm_projects"),
                        confirm_destination=arguments.get("confirm_destination"),
                    )
                elif name == "backup_explain":
                    result = await self._tool_backup_explain(
                        topic=arguments.get("topic"),
                    )
                elif name == "backup_find_file":
                    result = await self._tool_backup_find_file(
                        description=arguments.get("description", ""),
                        time_hint=arguments.get("time_hint"),
                    )
                elif name == "backup_undo":
                    result = await self._tool_backup_undo(
                        file_path=arguments.get("file_path"),
                        confirm=arguments.get("confirm", False),
                    )
                else:
                    result = self._error_response("UNKNOWN_TOOL", f"Unknown tool: {name}")
                
                return [TextContent(type="text", text=result)]
            except Exception as e:
                error_result = self._error_response("INTERNAL_ERROR", str(e))
                return [TextContent(type="text", text=error_result)]

    
    async def _tool_backup_run(self) -> str:
        """
        Trigger an immediate backup.
        
        Returns JSON with:
        - success: boolean
        - snapshot: timestamp of created snapshot (if successful)
        - files_transferred: number of files transferred
        - duration_seconds: backup duration
        - error: error message (if failed)
        
        Requirements: 10.1
        """
        try:
            config = self._load_config()
        except (ConfigurationError, ValidationError) as e:
            return self._error_response("CONFIG_ERROR", str(e))
        
        # Run backup in thread pool to avoid blocking
        loop = asyncio.get_event_loop()
        result: BackupResult = await loop.run_in_executor(
            None,
            lambda: run_backup(config_path=self.config_path, config=config)
        )
        
        if result.success:
            response = {
                "success": True,
                "snapshot": result.snapshot_result.snapshot_path.name if result.snapshot_result and result.snapshot_result.snapshot_path else None,
                "files_transferred": result.snapshot_result.files_transferred if result.snapshot_result else 0,
                "total_size": result.snapshot_result.total_size if result.snapshot_result else 0,
                "duration_seconds": result.snapshot_result.duration_seconds if result.snapshot_result else 0,
            }
            return self._success_response(response)
        else:
            return self._error_response("BACKUP_FAILED", result.error_message or "Unknown error")
    
    async def _tool_backup_status(self) -> str:
        """
        Get current backup status.
        
        Returns JSON with:
        - last_backup: timestamp of last successful backup (ISO format)
        - next_scheduled: timestamp of next scheduled backup (if scheduler installed)
        - is_running: whether backup is currently in progress
        - lock_holder_pid: PID if locked
        - scheduler_installed: whether scheduler is installed
        - scheduler_type: type of scheduler (launchd/cron)
        - total_snapshots: number of snapshots
        
        Requirements: 10.2
        """
        try:
            config = self._load_config()
        except (ConfigurationError, ValidationError) as e:
            return self._error_response("CONFIG_ERROR", str(e))
        
        # Check lock status
        lock_manager = LockManager()
        is_running = lock_manager.is_locked()
        lock_holder_pid = lock_manager.get_lock_holder_pid() if is_running else None
        
        # Get snapshot info
        snapshot_engine = SnapshotEngine(
            destination=config.backup_destination,
            exclude_patterns=config.exclude_patterns,
        )
        snapshots = snapshot_engine.list_snapshots()
        
        # Get last backup time
        last_backup = None
        if snapshots:
            last_backup = snapshots[0].timestamp.isoformat()
        
        # Get scheduler status
        scheduler_type = SchedulerType(config.scheduler.type)
        scheduler = Scheduler(
            scheduler_type=scheduler_type,
            interval_seconds=config.scheduler.interval_seconds,
        )
        scheduler_status = scheduler.get_status()
        scheduler_installed = scheduler_status.get("installed", False)
        
        # Calculate next scheduled backup
        next_scheduled = None
        if scheduler_installed and last_backup and snapshots:
            from datetime import timedelta
            next_time = snapshots[0].timestamp + timedelta(seconds=config.scheduler.interval_seconds)
            next_scheduled = next_time.isoformat()
        
        response = {
            "last_backup": last_backup,
            "next_scheduled": next_scheduled,
            "is_running": is_running,
            "lock_holder_pid": lock_holder_pid,
            "scheduler_installed": scheduler_installed,
            "scheduler_type": config.scheduler.type,
            "total_snapshots": len(snapshots),
        }
        
        return self._success_response(response)
    
    async def _tool_backup_list_snapshots(self) -> str:
        """
        List all available snapshots.
        
        Returns JSON array of snapshots with:
        - timestamp: snapshot timestamp (ISO format)
        - name: snapshot directory name (YYYY-MM-DD-HHMMSS)
        - size_bytes: total size in bytes
        - file_count: number of files
        
        Requirements: 10.3
        """
        try:
            config = self._load_config()
        except (ConfigurationError, ValidationError) as e:
            return self._error_response("CONFIG_ERROR", str(e))
        
        snapshot_engine = SnapshotEngine(
            destination=config.backup_destination,
            exclude_patterns=config.exclude_patterns,
        )
        snapshots = snapshot_engine.list_snapshots()
        
        result = []
        for snap in snapshots:
            result.append({
                "timestamp": snap.timestamp.isoformat(),
                "name": snap.path.name,
                "size_bytes": snap.size_bytes,
                "file_count": snap.file_count,
            })
        
        return self._success_response({"snapshots": result})
    
    async def _tool_backup_restore(
        self,
        snapshot: str,
        path: str,
        destination: Optional[str] = None
    ) -> str:
        """
        Restore a file or directory from a snapshot.
        
        Args:
            snapshot: Snapshot timestamp (YYYY-MM-DD-HHMMSS)
            path: Path within snapshot to restore
            destination: Where to restore (default: original location)
        
        Returns JSON with:
        - success: boolean
        - restored_path: path where files were restored
        - error: error message (if failed)
        
        Requirements: 10.4
        """
        if not snapshot:
            return self._error_response("INVALID_ARGUMENT", "snapshot is required")
        if not path:
            return self._error_response("INVALID_ARGUMENT", "path is required")
        
        # Sanitize path to prevent path traversal attacks
        # Normalize the path and ensure it doesn't escape the snapshot directory
        normalized_path = Path(path)
        
        # Check for absolute paths
        if normalized_path.is_absolute():
            return self._error_response("INVALID_ARGUMENT", "path must be relative, not absolute")
        
        # Check for path traversal attempts (.. components)
        try:
            # Resolve the path relative to a dummy root to check for traversal
            dummy_root = Path("/dummy_root")
            resolved = (dummy_root / normalized_path).resolve()
            if not str(resolved).startswith(str(dummy_root)):
                return self._error_response("INVALID_ARGUMENT", "path contains invalid traversal sequences")
        except (ValueError, RuntimeError):
            return self._error_response("INVALID_ARGUMENT", "path contains invalid characters")
        
        # Clean the path - remove any .. or . components
        clean_parts = []
        for part in normalized_path.parts:
            if part == '..':
                return self._error_response("INVALID_ARGUMENT", "path cannot contain '..' components")
            if part != '.':
                clean_parts.append(part)
        
        sanitized_path = str(Path(*clean_parts)) if clean_parts else ""
        if not sanitized_path:
            return self._error_response("INVALID_ARGUMENT", "path is empty after sanitization")
        
        try:
            config = self._load_config()
        except (ConfigurationError, ValidationError) as e:
            return self._error_response("CONFIG_ERROR", str(e))
        
        snapshot_engine = SnapshotEngine(
            destination=config.backup_destination,
            exclude_patterns=config.exclude_patterns,
        )
        
        # Find the snapshot
        snapshot_path = snapshot_engine.get_snapshot_by_timestamp(snapshot)
        if snapshot_path is None:
            return self._error_response("SNAPSHOT_NOT_FOUND", f"Snapshot not found: {snapshot}")
        
        # Determine destination path
        dest_path = Path(destination) if destination else None
        
        # Perform restore using sanitized path
        success = snapshot_engine.restore(
            snapshot=snapshot_path,
            source_path=sanitized_path,
            destination=dest_path,
            source_directories=config.source_directories,
        )
        
        if success:
            restored_to = str(dest_path) if dest_path else "original location"
            response = {
                "success": True,
                "restored_path": restored_to,
                "snapshot": snapshot,
                "source_path": sanitized_path,
            }
            return self._success_response(response)
        else:
            return self._error_response("RESTORE_FAILED", f"Failed to restore '{sanitized_path}' from snapshot {snapshot}")
    
    async def _tool_backup_diff(
        self,
        snapshot: str,
        path: Optional[str] = None
    ) -> str:
        """
        Show differences between snapshot and current state.
        
        Args:
            snapshot: Snapshot timestamp
            path: Specific path to compare (optional)
        
        Returns JSON with:
        - added: list of new files
        - modified: list of changed files
        - deleted: list of removed files
        
        Requirements: 10.5
        """
        if not snapshot:
            return self._error_response("INVALID_ARGUMENT", "snapshot is required")
        
        try:
            config = self._load_config()
        except (ConfigurationError, ValidationError) as e:
            return self._error_response("CONFIG_ERROR", str(e))
        
        snapshot_engine = SnapshotEngine(
            destination=config.backup_destination,
            exclude_patterns=config.exclude_patterns,
        )
        
        # Find the snapshot
        snapshot_path = snapshot_engine.get_snapshot_by_timestamp(snapshot)
        if snapshot_path is None:
            return self._error_response("SNAPSHOT_NOT_FOUND", f"Snapshot not found: {snapshot}")
        
        # Get diff
        diff_result = snapshot_engine.diff(
            snapshot=snapshot_path,
            source_directories=config.source_directories,
            source_path=path,
        )
        
        response = {
            "snapshot": snapshot,
            "path": path,
            "added": diff_result["added"],
            "modified": diff_result["modified"],
            "deleted": diff_result["deleted"],
            "total_changes": len(diff_result["added"]) + len(diff_result["modified"]) + len(diff_result["deleted"]),
        }
        
        return self._success_response(response)
    
    async def _tool_backup_search(
        self,
        pattern: str,
        snapshot: Optional[str] = None
    ) -> str:
        """
        Search for files across snapshots.
        
        Args:
            pattern: Glob pattern or filename
            snapshot: Specific snapshot (optional, default: all)
        
        Returns JSON array of matches with:
        - snapshot: which snapshot
        - path: file path
        - size: file size
        - modified: modification time
        
        Requirements: 10.6
        """
        if not pattern:
            return self._error_response("INVALID_ARGUMENT", "pattern is required")
        
        try:
            config = self._load_config()
        except (ConfigurationError, ValidationError) as e:
            return self._error_response("CONFIG_ERROR", str(e))
        
        snapshot_engine = SnapshotEngine(
            destination=config.backup_destination,
            exclude_patterns=config.exclude_patterns,
        )
        
        # Find specific snapshot if provided
        snapshot_path = None
        if snapshot:
            snapshot_path = snapshot_engine.get_snapshot_by_timestamp(snapshot)
            if snapshot_path is None:
                return self._error_response("SNAPSHOT_NOT_FOUND", f"Snapshot not found: {snapshot}")
        
        # Search
        results = snapshot_engine.search(
            pattern=pattern,
            snapshot=snapshot_path,
        )
        
        response = {
            "pattern": pattern,
            "snapshot": snapshot,
            "matches": results,
            "total_matches": len(results),
        }
        
        return self._success_response(response)
    
    async def _tool_backup_progress(self) -> str:
        """
        Get current backup progress.
        
        Returns JSON with:
        - is_running: boolean indicating if backup is in progress
        - progress: progress information (if running)
          - files_transferred: number of files transferred
          - total_files: total number of files (if known)
          - bytes_transferred: bytes transferred
          - total_bytes: total bytes (if known)
          - transfer_rate: bytes per second
          - percent_complete: percentage complete (if known)
          - current_file: file currently being transferred (if known)
        
        Requirements: 6.3
        """
        try:
            config = self._load_config()
        except (ConfigurationError, ValidationError) as e:
            return self._error_response("CONFIG_ERROR", str(e))
        
        # Check if backup is running
        lock_manager = LockManager()
        is_running = lock_manager.is_locked()
        
        if not is_running:
            response = {
                "is_running": False,
                "progress": None,
            }
            return self._success_response(response)
        
        # Get progress from snapshot engine
        snapshot_engine = SnapshotEngine(
            destination=config.backup_destination,
            exclude_patterns=config.exclude_patterns,
        )
        
        progress = snapshot_engine.get_current_progress()
        
        if progress is None:
            # Backup is running but no progress info available
            # (might be using a different process or progress not enabled)
            response = {
                "is_running": True,
                "progress": None,
                "message": "Backup is running but progress information is not available",
            }
            return self._success_response(response)
        
        response = {
            "is_running": True,
            "progress": {
                "files_transferred": progress.files_transferred,
                "total_files": progress.total_files,
                "bytes_transferred": progress.bytes_transferred,
                "total_bytes": progress.total_bytes,
                "transfer_rate": progress.transfer_rate,
                "percent_complete": progress.percent_complete,
                "current_file": progress.current_file,
            },
        }
        
        return self._success_response(response)
    
    async def _tool_backup_verify(
        self,
        snapshot: str,
        pattern: Optional[str] = None
    ) -> str:
        """
        Verify the integrity of a backup snapshot.
        
        Args:
            snapshot: Snapshot timestamp (YYYY-MM-DD-HHMMSS)
            pattern: Optional glob pattern to filter files
        
        Returns JSON with:
        - success: boolean indicating if verification passed
        - snapshot: snapshot timestamp
        - files_verified: number of files successfully verified
        - files_failed: number of files that failed verification
        - missing_files: list of files missing from snapshot
        - corrupted_files: list of files with checksum mismatches
        - errors: list of error messages
        
        Requirements: 7.4
        """
        if not snapshot:
            return self._error_response("INVALID_ARGUMENT", "snapshot is required")
        
        try:
            config = self._load_config()
        except (ConfigurationError, ValidationError) as e:
            return self._error_response("CONFIG_ERROR", str(e))
        
        snapshot_engine = SnapshotEngine(
            destination=config.backup_destination,
            exclude_patterns=config.exclude_patterns,
        )
        
        # Find the snapshot
        snapshot_path = snapshot_engine.get_snapshot_by_timestamp(snapshot)
        if snapshot_path is None:
            return self._error_response("SNAPSHOT_NOT_FOUND", f"Snapshot not found: {snapshot}")
        
        # Verify the snapshot
        verifier = IntegrityVerifier()
        result = verifier.verify_snapshot(
            snapshot_path=snapshot_path,
            pattern=pattern,
        )
        
        response = {
            "success": result.success,
            "snapshot": snapshot,
            "files_verified": result.files_verified,
            "files_failed": result.files_failed,
            "missing_files": result.missing_files,
            "corrupted_files": result.corrupted_files,
            "errors": result.errors,
        }
        
        return self._success_response(response)
    
    async def _tool_backup_health(
        self,
        min_age_days: Optional[int] = None
    ) -> str:
        """
        Check health of all backup snapshots.
        
        Args:
            min_age_days: Only check snapshots older than N days (optional)
        
        Returns JSON with:
        - total_snapshots: number of snapshots checked
        - healthy_snapshots: number of healthy snapshots
        - unhealthy_snapshots: number of unhealthy snapshots
        - snapshots: list of snapshot health details
        - errors: list of error messages
        
        Requirements: 12.5
        """
        try:
            config = self._load_config()
        except (ConfigurationError, ValidationError) as e:
            return self._error_response("CONFIG_ERROR", str(e))
        
        verifier = IntegrityVerifier()
        health_checker = HealthChecker(
            destination=config.backup_destination,
            verifier=verifier,
        )
        
        result = health_checker.check_all(min_age_days=min_age_days)
        
        response = {
            "total_snapshots": result.total_snapshots,
            "healthy_snapshots": result.healthy_snapshots,
            "unhealthy_snapshots": result.unhealthy_snapshots,
            "snapshots": [
                {
                    "name": s.snapshot_name,
                    "timestamp": s.timestamp.isoformat() if s.timestamp else None,
                    "readable": s.readable,
                    "has_manifest": s.has_manifest,
                    "manifest_valid": s.manifest_valid,
                    "file_count": s.file_count,
                    "corrupted_files": s.corrupted_files,
                    "missing_files": s.missing_files,
                    "error": s.error,
                }
                for s in result.snapshots
            ],
            "errors": result.errors,
        }
        
        return self._success_response(response)
    
    async def _tool_backup_setup(
        self,
        workspace_path: Optional[str] = None,
        confirm_projects: Optional[List[str]] = None,
        confirm_destination: Optional[str] = None,
    ) -> str:
        """
        Interactive setup tool for zero-config initialization.
        
        Flow:
        1. If no config exists, discover projects and destinations
        2. Present findings in plain language
        3. If confirm_projects provided, use those
        4. If confirm_destination provided, use that
        5. Create configuration and return success message
        
        Returns conversational JSON with:
        - stage: "discovery" | "confirm_projects" | "confirm_destination" | "complete"
        - message: Plain language message for the user
        - discovered_projects: List of found projects (if discovery stage)
        - discovered_destinations: List of found destinations (if discovery stage)
        - config_created: True if setup complete
        
        Requirements: 2.1, 1.4, 1.5, 1.6, 1.7
        """
        # Check if config already exists
        config_path = self.config_path or DEFAULT_CONFIG_PATH
        config_exists = config_path.exists()
        
        # If we have confirmation data, complete the setup
        if confirm_projects is not None and confirm_destination is not None:
            return await self._complete_setup(
                confirm_projects, confirm_destination, config_path
            )
        
        # Discovery stage - find projects and destinations
        workspace = Path(workspace_path) if workspace_path else None
        
        # Discover projects
        projects = self._discovery.discover_projects(include_workspace=workspace)
        
        # Discover destinations
        destinations = self._discovery.discover_destinations()
        
        if not projects:
            return self._success_response({
                "stage": "no_projects",
                "message": (
                    "I couldn't find any projects to back up. "
                    "Make sure you're in a folder with your code, or let me know "
                    "which folders you'd like to back up."
                ),
                "discovered_projects": [],
                "discovered_destinations": [],
            })
        
        if not destinations:
            return self._success_response({
                "stage": "no_destinations",
                "message": (
                    "I found your projects, but I couldn't find a good place to store backups. "
                    "Do you have an external drive you can plug in? "
                    "Or I can create a backup folder on your Mac (though that won't protect "
                    "against drive failure)."
                ),
                "discovered_projects": [
                    {
                        "path": str(p.path),
                        "name": p.name,
                        "project_type": p.project_type,
                        "estimated_size_bytes": p.estimated_size_bytes,
                        "size_friendly": self._translator.translate_size_precise(p.estimated_size_bytes),
                    }
                    for p in projects
                ],
                "discovered_destinations": [],
            })
        
        # Get recommendation
        recommended_dest, recommendation_reason = self._discovery.recommend_destination(destinations)
        
        # Build friendly project list
        project_list = self._translator.describe_projects([
            {
                "name": p.name,
                "estimated_size_bytes": p.estimated_size_bytes,
            }
            for p in projects
        ])
        
        # Build friendly destination description
        dest_description = ""
        if recommended_dest:
            dest_description = self._translator.describe_destination({
                "name": recommended_dest.name,
                "available_bytes": recommended_dest.available_bytes,
                "destination_type": recommended_dest.destination_type,
            })
        
        message = f"{project_list}\n\n{dest_description}\n\nWould you like me to set this up?"
        
        if config_exists:
            message = (
                "You already have backups configured, but I can update the settings.\n\n"
                + message
            )
        
        return self._success_response({
            "stage": "discovery",
            "message": message,
            "discovered_projects": [
                {
                    "path": str(p.path),
                    "name": p.name,
                    "project_type": p.project_type,
                    "estimated_size_bytes": p.estimated_size_bytes,
                    "size_friendly": self._translator.translate_size_precise(p.estimated_size_bytes),
                }
                for p in projects
            ],
            "discovered_destinations": [
                {
                    "path": str(d.path),
                    "name": d.name,
                    "destination_type": d.destination_type,
                    "available_bytes": d.available_bytes,
                    "available_friendly": self._translator.translate_size_precise(d.available_bytes),
                    "recommendation_score": d.recommendation_score,
                    "is_recommended": d == recommended_dest,
                }
                for d in destinations
            ],
            "recommendation": {
                "destination_path": str(recommended_dest.path) if recommended_dest else None,
                "reason": recommendation_reason,
            },
            "config_exists": config_exists,
        })
    
    async def _complete_setup(
        self,
        project_paths: List[str],
        destination_path: str,
        config_path: Path,
    ) -> str:
        """Complete the setup by creating config and installing scheduler."""
        # Convert paths to DiscoveredProject objects
        projects: List[DiscoveredProject] = []
        for path_str in project_paths:
            path = Path(path_str)
            if not path.exists():
                return self._error_response(
                    "INVALID_PATH",
                    f"Project path does not exist: {path_str}"
                )
            
            # Detect project type
            project_type, markers = self._discovery._detect_project_type(path)
            if project_type is None:
                project_type = "generic"
                markers = []
            
            # Calculate size
            size = self._discovery._calculate_size(path)
            
            projects.append(DiscoveredProject(
                path=path,
                name=path.name,
                project_type=project_type,
                estimated_size_bytes=size,
                marker_files=markers,
            ))
        
        # Create destination object
        dest_path = Path(destination_path)
        available, total = self._discovery._get_space_info(dest_path)
        dest_type, is_removable = self._discovery._classify_destination(dest_path)
        
        destination = DiscoveredDestination(
            path=dest_path,
            name=dest_path.name,
            destination_type=dest_type,
            available_bytes=available,
            total_bytes=total,
            is_removable=is_removable,
            recommendation_score=50,
        )
        
        # Generate configuration
        try:
            config = self._defaults.generate_config(projects, destination)
            config_toml = format_config(config)
        except Exception as e:
            return self._error_response("CONFIG_ERROR", str(e))
        
        # Create config directory if needed
        config_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Create backup destination directory if needed
        config.backup_destination.mkdir(parents=True, exist_ok=True)
        
        # Write configuration
        try:
            config_path.write_text(config_toml)
        except Exception as e:
            return self._error_response(
                "WRITE_ERROR",
                f"Couldn't save configuration: {e}"
            )
        
        # Install scheduler
        try:
            scheduler = Scheduler(
                scheduler_type=SchedulerType(config.scheduler.type),
                interval_seconds=config.scheduler.interval_seconds,
            )
            scheduler.install()
            scheduler_installed = True
        except Exception:
            scheduler_installed = False
        
        # Clear cached config so next load gets the new one
        self._config = None
        
        # Build success message
        project_names = ", ".join(p.name for p in projects)
        message = (
            f"All set! I've configured backups for {project_names}. "
            f"Your files will be backed up to '{destination.name}' every hour. "
        )
        
        if scheduler_installed:
            message += "Automatic backups are now running in the background."
        else:
            message += (
                "I couldn't set up automatic backups, but you can run them manually "
                "by asking me to 'back up my projects'."
            )
        
        return self._success_response({
            "stage": "complete",
            "message": message,
            "config_created": True,
            "config_path": str(config_path),
            "backup_destination": str(config.backup_destination),
            "scheduler_installed": scheduler_installed,
            "projects_configured": [str(p.path) for p in projects],
        })
    
    async def _tool_backup_explain(
        self,
        topic: Optional[str] = None,
    ) -> str:
        """
        Explain backup status or concepts in plain language.
        
        Topics:
        - "status": Current backup status
        - "snapshots": What snapshots are and how they work
        - "restore": How to restore files
        - "schedule": When backups happen
        - "storage": Where backups are stored and how much space is used
        - None: General overview
        
        Returns conversational JSON with:
        - message: Plain language explanation
        - suggestions: List of follow-up questions the user might ask
        
        Requirements: 2.2, 2.5, 2.8
        """
        # Check if config exists
        config_path = self.config_path or DEFAULT_CONFIG_PATH
        if not config_path.exists():
            return self._success_response({
                "message": (
                    "You don't have backups set up yet. Would you like me to help you "
                    "get started? I can automatically find your projects and set everything up."
                ),
                "suggestions": [
                    "Set up backups for my projects",
                    "What is devbackup?",
                    "Where will my backups be stored?",
                ],
            })
        
        try:
            config = self._load_config()
        except (ConfigurationError, ValidationError) as e:
            return self._error_response("CONFIG_ERROR", str(e))
        
        # Get current status info
        snapshot_engine = SnapshotEngine(
            destination=config.backup_destination,
            exclude_patterns=config.exclude_patterns,
        )
        snapshots = snapshot_engine.list_snapshots()
        
        scheduler = Scheduler(
            scheduler_type=SchedulerType(config.scheduler.type),
            interval_seconds=config.scheduler.interval_seconds,
        )
        scheduler_status = scheduler.get_status()
        
        if topic == "status":
            return self._explain_status(config, snapshots, scheduler_status)
        elif topic == "snapshots":
            return self._explain_snapshots(snapshots)
        elif topic == "restore":
            return self._explain_restore(snapshots)
        elif topic == "schedule":
            return self._explain_schedule(config, scheduler_status)
        elif topic == "storage":
            return self._explain_storage(config, snapshots)
        else:
            return self._explain_overview(config, snapshots, scheduler_status)
    
    def _explain_status(self, config: Configuration, snapshots: List, scheduler_status: dict) -> str:
        """Generate plain language status explanation."""
        if not snapshots:
            message = (
                "You haven't made any backups yet. Would you like me to run one now? "
                "It'll save a copy of all your project files."
            )
        else:
            last_backup = snapshots[0]
            time_str = self._translator.translate_time(last_backup.timestamp)
            
            if scheduler_status.get("installed", False):
                message = (
                    f"Your files are safe! The last backup was {time_str}. "
                    f"You have {len(snapshots)} backup versions saved, and automatic "
                    "backups are running in the background."
                )
            else:
                message = (
                    f"Your last backup was {time_str}. You have {len(snapshots)} "
                    "backup versions saved. Automatic backups aren't set up though - "
                    "would you like me to enable them?"
                )
        
        return self._success_response({
            "message": message,
            "suggestions": [
                "Back up my projects now",
                "Show me my backup history",
                "When is the next backup?",
            ],
        })
    
    def _explain_snapshots(self, snapshots: List) -> str:
        """Explain what snapshots are and list recent ones."""
        if not snapshots:
            message = (
                "Backup versions (we call them 'snapshots') are like save points for your files. "
                "Each time you back up, I save a new version. You don't have any yet - "
                "want me to create your first one?"
            )
        else:
            message = (
                "Each backup creates a 'snapshot' - a complete copy of your files at that moment. "
                f"You have {len(snapshots)} snapshots saved. Here are the most recent:\n\n"
            )
            for snap in snapshots[:5]:
                time_str = self._translator.translate_time(snap.timestamp)
                size_str = self._translator.translate_size_precise(snap.size_bytes)
                message += f"• {time_str} ({size_str})\n"
            
            if len(snapshots) > 5:
                message += f"\n...and {len(snapshots) - 5} more."
        
        return self._success_response({
            "message": message,
            "suggestions": [
                "Restore a file from a backup",
                "How much space are my backups using?",
                "Delete old backups",
            ],
        })
    
    def _explain_restore(self, snapshots: List) -> str:
        """Explain how to restore files."""
        if not snapshots:
            message = (
                "To restore files, you first need to have some backups. "
                "Want me to back up your projects now?"
            )
        else:
            message = (
                "I can help you get back old versions of your files. Just tell me:\n\n"
                "• Which file you're looking for (like 'app.py' or 'the config file')\n"
                "• When you last remember it being correct (like 'yesterday' or 'last week')\n\n"
                "I'll find it in your backups and put a copy on your Desktop so you can "
                "compare it with your current version."
            )
        
        return self._success_response({
            "message": message,
            "suggestions": [
                "Find app.py from yesterday",
                "Undo my recent changes",
                "Show me what changed since my last backup",
            ],
        })
    
    def _explain_schedule(self, config: Configuration, scheduler_status: dict) -> str:
        """Explain backup schedule."""
        interval_hours = config.scheduler.interval_seconds / 3600
        
        if scheduler_status.get("installed", False):
            if interval_hours == 1:
                message = "Your projects are backed up automatically every hour."
            elif interval_hours < 1:
                minutes = int(config.scheduler.interval_seconds / 60)
                message = f"Your projects are backed up automatically every {minutes} minutes."
            else:
                message = f"Your projects are backed up automatically every {int(interval_hours)} hours."
            
            message += " This happens in the background - you don't need to do anything."
        else:
            message = (
                "Automatic backups aren't set up right now. Would you like me to enable them? "
                "I can back up your projects every hour without you having to think about it."
            )
        
        return self._success_response({
            "message": message,
            "suggestions": [
                "Back up more frequently",
                "Back up less frequently",
                "Back up now",
            ],
        })
    
    def _explain_storage(self, config: Configuration, snapshots: List) -> str:
        """Explain storage usage."""
        total_size = sum(s.size_bytes for s in snapshots)
        size_str = self._translator.translate_size_precise(total_size)
        
        # Get destination info
        dest_path = config.backup_destination
        available, total = self._discovery._get_space_info(dest_path)
        available_str = self._translator.translate_size_precise(available)
        
        message = (
            f"Your backups are stored at: {dest_path}\n\n"
            f"Total backup size: {size_str} ({len(snapshots)} versions)\n"
            f"Space remaining: {available_str}\n\n"
        )
        
        if available < total_size * 2:
            message += (
                "You're starting to run low on space. I can delete older backups "
                "to free up room if you'd like."
            )
        else:
            message += "You have plenty of space for more backups."
        
        return self._success_response({
            "message": message,
            "suggestions": [
                "Delete old backups",
                "Change backup location",
                "How long are backups kept?",
            ],
        })
    
    def _explain_overview(self, config: Configuration, snapshots: List, scheduler_status: dict) -> str:
        """Generate general overview."""
        if not snapshots:
            message = (
                "devbackup keeps your project files safe by making regular copies. "
                "You haven't made any backups yet - want me to start?"
            )
        else:
            last_backup = snapshots[0]
            time_str = self._translator.translate_time(last_backup.timestamp)
            
            status = "protected" if scheduler_status.get("installed", False) else "needs_attention"
            status_dict = {
                "status": status,
                "last_backup": last_backup.timestamp,
                "total_snapshots": len(snapshots),
            }
            message = self._translator.translate_status(status_dict)
        
        return self._success_response({
            "message": message,
            "suggestions": [
                "What's my backup status?",
                "How do I restore a file?",
                "Where are my backups stored?",
            ],
        })
    
    async def _tool_backup_find_file(
        self,
        description: str,
        time_hint: Optional[str] = None,
    ) -> str:
        """
        Find files using natural language descriptions.
        
        Args:
            description: Natural description like "the config file I was editing yesterday"
            time_hint: Optional time hint like "yesterday", "last week", "before the crash"
        
        Returns conversational JSON with:
        - message: Plain language result
        - matches: List of matching files with friendly descriptions
        - suggestions: How to restore if matches found
        
        Requirements: 2.3
        """
        if not description:
            return self._error_response("INVALID_ARGUMENT", "Please describe the file you're looking for")
        
        try:
            config = self._load_config()
        except (ConfigurationError, ValidationError) as e:
            return self._error_response("CONFIG_ERROR", str(e))
        
        snapshot_engine = SnapshotEngine(
            destination=config.backup_destination,
            exclude_patterns=config.exclude_patterns,
        )
        
        # Extract filename pattern from description
        # Simple heuristic: look for file extensions or use the description as-is
        pattern = self._extract_file_pattern(description)
        
        # Search across snapshots
        results = snapshot_engine.search(pattern=pattern)
        
        # Filter by time hint if provided
        if time_hint and results:
            results = self._filter_by_time_hint(results, time_hint)
        
        if not results:
            return self._success_response({
                "message": (
                    f"I couldn't find any files matching '{description}' in your backups. "
                    "Try being more specific, or check if the file was created after your last backup."
                ),
                "matches": [],
                "suggestions": [
                    "Show me all my backups",
                    "Back up my projects now",
                ],
            })
        
        # Group by file path and show most recent versions
        unique_files = {}
        for match in results:
            file_path = match.get("path", "")
            if file_path not in unique_files:
                unique_files[file_path] = match
        
        matches = list(unique_files.values())[:10]  # Limit to 10 results
        
        if len(matches) == 1:
            match = matches[0]
            time_str = self._translator.translate_time(
                datetime.fromisoformat(match["modified"]) if "modified" in match else datetime.now()
            )
            message = (
                f"I found '{match.get('path', 'the file')}' in your backups. "
                f"The version I found is from {time_str}. "
                "Would you like me to restore it?"
            )
        else:
            message = f"I found {len(matches)} files that might be what you're looking for:\n\n"
            for match in matches[:5]:
                path = match.get("path", "unknown")
                snapshot = match.get("snapshot", "")
                message += f"• {path} (from backup {snapshot})\n"
            
            if len(matches) > 5:
                message += f"\n...and {len(matches) - 5} more."
            
            message += "\n\nWhich one would you like to restore?"
        
        return self._success_response({
            "message": message,
            "matches": [
                {
                    "path": m.get("path", ""),
                    "snapshot": m.get("snapshot", ""),
                    "size": m.get("size", 0),
                    "size_friendly": self._translator.translate_size_precise(m.get("size", 0)),
                }
                for m in matches
            ],
            "suggestions": [
                "Restore the first one",
                "Show me more details",
                "Search for something else",
            ],
        })
    
    def _extract_file_pattern(self, description: str) -> str:
        """Extract a file pattern from natural language description."""
        # If it looks like a filename, use it directly
        if "." in description and " " not in description:
            return f"*{description}*" if not description.startswith("*") else description
        
        # Look for common file extensions mentioned
        extensions = [".py", ".js", ".ts", ".json", ".toml", ".yaml", ".yml", ".md", ".txt"]
        for ext in extensions:
            if ext in description.lower():
                return f"*{ext}"
        
        # Look for common file types
        type_patterns = {
            "config": "*.{json,toml,yaml,yml}",
            "python": "*.py",
            "javascript": "*.js",
            "typescript": "*.ts",
            "readme": "README*",
            "package": "package.json",
        }
        
        desc_lower = description.lower()
        for keyword, pattern in type_patterns.items():
            if keyword in desc_lower:
                return pattern
        
        # Default: use the description as a glob pattern
        words = description.split()
        if words:
            # Use the most specific-looking word
            for word in words:
                if "." in word or len(word) > 3:
                    return f"*{word}*"
        
        return f"*{description}*"
    
    def _filter_by_time_hint(self, results: List[dict], time_hint: str) -> List[dict]:
        """Filter search results by time hint."""
        hint_lower = time_hint.lower()
        now = datetime.now()
        
        # Parse time hint
        if "yesterday" in hint_lower:
            cutoff = now.replace(hour=0, minute=0, second=0) - timedelta(days=1)
            end = now.replace(hour=0, minute=0, second=0)
        elif "last week" in hint_lower:
            cutoff = now - timedelta(days=7)
            end = now
        elif "today" in hint_lower:
            cutoff = now.replace(hour=0, minute=0, second=0)
            end = now
        else:
            # Default: no filtering
            return results
        
        filtered = []
        for result in results:
            # Try to parse the snapshot timestamp
            snapshot = result.get("snapshot", "")
            try:
                # Snapshot format: YYYY-MM-DD-HHMMSS
                snap_time = datetime.strptime(snapshot, "%Y-%m-%d-%H%M%S")
                if cutoff <= snap_time <= end:
                    filtered.append(result)
            except ValueError:
                # If we can't parse, include it
                filtered.append(result)
        
        return filtered if filtered else results
    
    async def _tool_backup_undo(
        self,
        file_path: Optional[str] = None,
        confirm: bool = False,
    ) -> str:
        """
        Restore the most recent version of a file.
        
        Args:
            file_path: Path to file (optional - uses last modified if not provided)
            confirm: Whether to proceed with restore
        
        Flow:
        1. If no file_path, explain that a file path is needed
        2. Find previous version in backups
        3. If not confirm, show preview and ask for confirmation
        4. If confirm, restore to Desktop/Recovered Files
        
        Returns conversational JSON with:
        - stage: "preview" | "complete" | "no_backup"
        - message: Plain language explanation
        - file_info: Details about the file and versions
        
        Requirements: 2.4, 7.2, 7.3, 7.6, 7.7
        """
        if not file_path:
            return self._success_response({
                "stage": "need_file",
                "message": (
                    "Which file would you like to restore? Tell me the filename "
                    "or describe it (like 'the config file' or 'app.py')."
                ),
                "suggestions": [
                    "Find a file in my backups",
                    "Show me recent changes",
                ],
            })
        
        try:
            config = self._load_config()
        except (ConfigurationError, ValidationError) as e:
            return self._error_response("CONFIG_ERROR", str(e))
        
        snapshot_engine = SnapshotEngine(
            destination=config.backup_destination,
            exclude_patterns=config.exclude_patterns,
        )
        
        # Search for the file
        pattern = file_path if "*" in file_path else f"*{Path(file_path).name}*"
        results = snapshot_engine.search(pattern=pattern)
        
        if not results:
            return self._success_response({
                "stage": "no_backup",
                "message": (
                    f"I couldn't find '{file_path}' in your backups. "
                    "It might have been created after your last backup, or the filename might be different."
                ),
                "suggestions": [
                    "Back up my projects now",
                    "Search for a different file",
                ],
            })
        
        # Find the most recent version
        # Sort by snapshot timestamp (most recent first)
        results.sort(key=lambda x: x.get("snapshot", ""), reverse=True)
        best_match = results[0]
        
        snapshot_name = best_match.get("snapshot", "")
        file_in_snapshot = best_match.get("path", "")
        
        # Get snapshot path
        snapshot_path = snapshot_engine.get_snapshot_by_timestamp(snapshot_name)
        if snapshot_path is None:
            return self._error_response("SNAPSHOT_NOT_FOUND", f"Backup version not found: {snapshot_name}")
        
        # Parse timestamp for friendly display
        try:
            snap_time = datetime.strptime(snapshot_name, "%Y-%m-%d-%H%M%S")
            time_str = self._translator.translate_time(snap_time)
        except ValueError:
            time_str = snapshot_name
        
        if not confirm:
            # Preview stage
            return self._success_response({
                "stage": "preview",
                "message": (
                    f"I found '{Path(file_in_snapshot).name}' from {time_str}. "
                    "I'll put a copy on your Desktop in a folder called 'Recovered Files'. "
                    "Your current file won't be changed. Ready to restore?"
                ),
                "file_info": {
                    "path": file_in_snapshot,
                    "snapshot": snapshot_name,
                    "time_friendly": time_str,
                    "size": best_match.get("size", 0),
                    "size_friendly": self._translator.translate_size_precise(best_match.get("size", 0)),
                },
                "suggestions": [
                    "Yes, restore it",
                    "Find a different version",
                    "Cancel",
                ],
            })
        
        # Confirm stage - do the restore
        recovered_dir = Path.home() / "Desktop" / "Recovered Files"
        recovered_dir.mkdir(parents=True, exist_ok=True)
        
        # Create unique filename if needed
        dest_filename = Path(file_in_snapshot).name
        dest_path = recovered_dir / dest_filename
        counter = 1
        while dest_path.exists():
            stem = Path(file_in_snapshot).stem
            suffix = Path(file_in_snapshot).suffix
            dest_path = recovered_dir / f"{stem}_{counter}{suffix}"
            counter += 1
        
        # Perform restore
        success = snapshot_engine.restore(
            snapshot=snapshot_path,
            source_path=file_in_snapshot,
            destination=dest_path,
            source_directories=config.source_directories,
        )
        
        if success:
            return self._success_response({
                "stage": "complete",
                "message": (
                    f"Done! I put the recovered file at:\n{dest_path}\n\n"
                    "You can compare it with your current version and copy over "
                    "what you need."
                ),
                "file_info": {
                    "restored_to": str(dest_path),
                    "original_path": file_in_snapshot,
                    "from_backup": time_str,
                },
            })
        else:
            return self._error_response(
                "RESTORE_FAILED",
                f"I couldn't restore the file. The backup might be corrupted."
            )

    async def run(self):
        """
        Start the MCP server using stdio transport for Cursor integration.
        
        Requirements: 10.9, 10.10
        """
        async with stdio_server() as (read_stream, write_stream):
            await self.server.run(
                read_stream,
                write_stream,
                self.server.create_initialization_options()
            )


def run_server(config_path: Optional[Path] = None):
    """
    Entry point for MCP server.
    
    This function is called by the CLI `devbackup mcp-server` command.
    
    Args:
        config_path: Optional path to configuration file
    """
    server = DevBackupMCPServer(config_path=config_path)
    asyncio.run(server.run())
