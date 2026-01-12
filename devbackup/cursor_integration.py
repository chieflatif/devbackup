"""Cursor IDE auto-integration for devbackup.

This module provides automatic MCP server registration with Cursor IDE,
enabling seamless backup functionality without manual configuration.

Requirements: 11.1, 11.6
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional
import json
import logging

logger = logging.getLogger(__name__)


class CursorIntegrationError(Exception):
    """Raised when Cursor integration fails."""
    pass


@dataclass
class RegistrationResult:
    """Result of a registration attempt."""
    success: bool
    config_path: Optional[Path]
    message: str
    already_registered: bool = False


class CursorIntegration:
    """Handles automatic Cursor MCP integration.
    
    This class manages the registration of devbackup's MCP server with
    Cursor IDE, allowing users to interact with backups through natural
    language conversations without manual configuration.
    """
    
    # Cursor config paths in priority order (first found is used)
    CURSOR_CONFIG_PATHS: List[Path] = [
        Path.home() / ".cursor" / "mcp.json",
        Path.home() / "Library" / "Application Support" / "Cursor" / "mcp.json",
    ]
    
    # The key used to identify devbackup in MCP config
    MCP_SERVER_KEY = "devbackup"
    
    def __init__(self, config_paths: Optional[List[Path]] = None) -> None:
        """Initialize CursorIntegration.
        
        Args:
            config_paths: Optional custom config paths for testing.
                         If None, uses default CURSOR_CONFIG_PATHS.
        """
        self._config_paths = config_paths or self.CURSOR_CONFIG_PATHS
    
    def get_registration_config(self) -> Dict[str, Any]:
        """Get the MCP configuration for devbackup.
        
        Returns:
            Dictionary containing the MCP server configuration.
        """
        return {
            self.MCP_SERVER_KEY: {
                "command": "devbackup",
                "args": ["mcp-server"],
                "env": {},
            }
        }
    
    def _find_existing_config_path(self) -> Optional[Path]:
        """Find an existing Cursor config file.
        
        Returns:
            Path to existing config file, or None if none exists.
        """
        for path in self._config_paths:
            if path.exists():
                return path
        return None
    
    def _get_preferred_config_path(self) -> Path:
        """Get the preferred config path for creating new config.
        
        Returns:
            The first path in the config paths list.
        """
        return self._config_paths[0]
    
    def _read_config(self, path: Path) -> Dict[str, Any]:
        """Read and parse a Cursor MCP config file.
        
        Args:
            path: Path to the config file.
            
        Returns:
            Parsed configuration dictionary.
            
        Raises:
            CursorIntegrationError: If file cannot be read or parsed.
        """
        try:
            content = path.read_text(encoding="utf-8")
            if not content.strip():
                return {"mcpServers": {}}
            return json.loads(content)
        except json.JSONDecodeError as e:
            raise CursorIntegrationError(
                f"Invalid JSON in Cursor config at {path}: {e}"
            )
        except OSError as e:
            raise CursorIntegrationError(
                f"Cannot read Cursor config at {path}: {e}"
            )
    
    def _write_config(self, path: Path, config: Dict[str, Any]) -> None:
        """Write configuration to a Cursor MCP config file.
        
        Args:
            path: Path to the config file.
            config: Configuration dictionary to write.
            
        Raises:
            CursorIntegrationError: If file cannot be written.
        """
        try:
            # Ensure parent directory exists
            path.parent.mkdir(parents=True, exist_ok=True)
            
            # Write with pretty formatting
            content = json.dumps(config, indent=2)
            path.write_text(content, encoding="utf-8")
        except OSError as e:
            raise CursorIntegrationError(
                f"Cannot write Cursor config to {path}: {e}"
            )
    
    def is_registered(self, config_path: Optional[Path] = None) -> bool:
        """Check if devbackup is registered with Cursor.
        
        Args:
            config_path: Optional specific config path to check.
                        If None, checks all known config paths.
        
        Returns:
            True if devbackup is registered in any config file.
        """
        paths_to_check = [config_path] if config_path else self._config_paths
        
        for path in paths_to_check:
            if path and path.exists():
                try:
                    config = self._read_config(path)
                    mcp_servers = config.get("mcpServers", {})
                    if self.MCP_SERVER_KEY in mcp_servers:
                        return True
                except CursorIntegrationError:
                    # Config file exists but is invalid, continue checking
                    continue
        
        return False
    
    def auto_register(self) -> RegistrationResult:
        """Automatically register devbackup MCP server with Cursor.
        
        This method:
        1. Checks if already registered (returns success if so)
        2. Finds existing config file or creates new one
        3. Adds devbackup to mcpServers section
        4. Preserves existing configuration
        
        Returns:
            RegistrationResult with success status and details.
        """
        # Check if already registered
        existing_path = self._find_existing_config_path()
        
        if existing_path and self.is_registered(existing_path):
            logger.info(f"devbackup already registered in {existing_path}")
            return RegistrationResult(
                success=True,
                config_path=existing_path,
                message="devbackup is already registered with Cursor",
                already_registered=True,
            )
        
        # Determine which config file to use
        config_path = existing_path or self._get_preferred_config_path()
        
        try:
            # Read existing config or create new one
            if config_path.exists():
                config = self._read_config(config_path)
            else:
                config = {"mcpServers": {}}
            
            # Ensure mcpServers section exists
            if "mcpServers" not in config:
                config["mcpServers"] = {}
            
            # Add devbackup configuration
            devbackup_config = self.get_registration_config()
            config["mcpServers"].update(devbackup_config)
            
            # Write updated config
            self._write_config(config_path, config)
            
            logger.info(f"Successfully registered devbackup in {config_path}")
            return RegistrationResult(
                success=True,
                config_path=config_path,
                message=f"devbackup has been registered with Cursor at {config_path}",
                already_registered=False,
            )
            
        except CursorIntegrationError as e:
            logger.error(f"Failed to register devbackup: {e}")
            return RegistrationResult(
                success=False,
                config_path=config_path,
                message=str(e),
                already_registered=False,
            )
    
    def unregister(self) -> RegistrationResult:
        """Remove devbackup registration from Cursor.
        
        Returns:
            RegistrationResult with success status and details.
        """
        existing_path = self._find_existing_config_path()
        
        if not existing_path:
            return RegistrationResult(
                success=True,
                config_path=None,
                message="No Cursor config file found",
                already_registered=False,
            )
        
        if not self.is_registered(existing_path):
            return RegistrationResult(
                success=True,
                config_path=existing_path,
                message="devbackup is not registered with Cursor",
                already_registered=False,
            )
        
        try:
            config = self._read_config(existing_path)
            
            if "mcpServers" in config and self.MCP_SERVER_KEY in config["mcpServers"]:
                del config["mcpServers"][self.MCP_SERVER_KEY]
                self._write_config(existing_path, config)
            
            logger.info(f"Successfully unregistered devbackup from {existing_path}")
            return RegistrationResult(
                success=True,
                config_path=existing_path,
                message="devbackup has been unregistered from Cursor",
                already_registered=False,
            )
            
        except CursorIntegrationError as e:
            logger.error(f"Failed to unregister devbackup: {e}")
            return RegistrationResult(
                success=False,
                config_path=existing_path,
                message=str(e),
                already_registered=False,
            )
    
    def get_config_status(self) -> Dict[str, Any]:
        """Get detailed status of Cursor integration.
        
        Returns:
            Dictionary with integration status details.
        """
        existing_path = self._find_existing_config_path()
        is_registered = self.is_registered()
        
        status = {
            "is_registered": is_registered,
            "config_path": str(existing_path) if existing_path else None,
            "config_exists": existing_path is not None and existing_path.exists(),
            "checked_paths": [str(p) for p in self._config_paths],
        }
        
        if is_registered and existing_path:
            try:
                config = self._read_config(existing_path)
                mcp_servers = config.get("mcpServers", {})
                devbackup_config = mcp_servers.get(self.MCP_SERVER_KEY, {})
                status["registered_config"] = devbackup_config
            except CursorIntegrationError:
                status["registered_config"] = None
        
        return status
