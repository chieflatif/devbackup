"""Unit tests for Cursor IDE auto-integration.

Feature: user-experience-enhancement
Requirements: 11.1, 11.6
"""

import json
from pathlib import Path
from typing import Dict, Any

import pytest

from devbackup.cursor_integration import (
    CursorIntegration,
    CursorIntegrationError,
    RegistrationResult,
)


class TestCursorIntegration:
    """Tests for CursorIntegration class."""
    
    def test_get_registration_config_returns_correct_structure(self):
        """Test that registration config has correct structure."""
        integration = CursorIntegration()
        config = integration.get_registration_config()
        
        assert "devbackup" in config
        assert config["devbackup"]["command"] == "devbackup"
        assert config["devbackup"]["args"] == ["mcp-server"]
        assert "env" in config["devbackup"]
    
    def test_is_registered_returns_false_when_no_config_exists(self, tmp_path: Path):
        """Test is_registered returns False when no config file exists."""
        config_path = tmp_path / ".cursor" / "mcp.json"
        integration = CursorIntegration(config_paths=[config_path])
        
        assert integration.is_registered() is False
    
    def test_is_registered_returns_false_when_devbackup_not_in_config(self, tmp_path: Path):
        """Test is_registered returns False when devbackup not in config."""
        config_path = tmp_path / ".cursor" / "mcp.json"
        config_path.parent.mkdir(parents=True)
        
        # Write config without devbackup
        config = {"mcpServers": {"other-server": {"command": "other"}}}
        config_path.write_text(json.dumps(config))
        
        integration = CursorIntegration(config_paths=[config_path])
        assert integration.is_registered() is False
    
    def test_is_registered_returns_true_when_devbackup_in_config(self, tmp_path: Path):
        """Test is_registered returns True when devbackup is in config."""
        config_path = tmp_path / ".cursor" / "mcp.json"
        config_path.parent.mkdir(parents=True)
        
        # Write config with devbackup
        config = {
            "mcpServers": {
                "devbackup": {
                    "command": "devbackup",
                    "args": ["mcp-server"],
                    "env": {},
                }
            }
        }
        config_path.write_text(json.dumps(config))
        
        integration = CursorIntegration(config_paths=[config_path])
        assert integration.is_registered() is True
    
    def test_auto_register_creates_config_when_none_exists(self, tmp_path: Path):
        """Test auto_register creates config file when none exists."""
        config_path = tmp_path / ".cursor" / "mcp.json"
        integration = CursorIntegration(config_paths=[config_path])
        
        result = integration.auto_register()
        
        assert result.success is True
        assert result.already_registered is False
        assert config_path.exists()
        
        # Verify config content
        config = json.loads(config_path.read_text())
        assert "mcpServers" in config
        assert "devbackup" in config["mcpServers"]
        assert config["mcpServers"]["devbackup"]["command"] == "devbackup"
    
    def test_auto_register_preserves_existing_servers(self, tmp_path: Path):
        """Test auto_register preserves existing MCP servers."""
        config_path = tmp_path / ".cursor" / "mcp.json"
        config_path.parent.mkdir(parents=True)
        
        # Write config with existing server
        existing_config = {
            "mcpServers": {
                "other-server": {
                    "command": "other",
                    "args": ["--flag"],
                }
            }
        }
        config_path.write_text(json.dumps(existing_config))
        
        integration = CursorIntegration(config_paths=[config_path])
        result = integration.auto_register()
        
        assert result.success is True
        
        # Verify both servers exist
        config = json.loads(config_path.read_text())
        assert "other-server" in config["mcpServers"]
        assert "devbackup" in config["mcpServers"]
    
    def test_auto_register_returns_already_registered_when_exists(self, tmp_path: Path):
        """Test auto_register returns already_registered when devbackup exists."""
        config_path = tmp_path / ".cursor" / "mcp.json"
        config_path.parent.mkdir(parents=True)
        
        # Write config with devbackup already registered
        config = {
            "mcpServers": {
                "devbackup": {
                    "command": "devbackup",
                    "args": ["mcp-server"],
                    "env": {},
                }
            }
        }
        config_path.write_text(json.dumps(config))
        
        integration = CursorIntegration(config_paths=[config_path])
        result = integration.auto_register()
        
        assert result.success is True
        assert result.already_registered is True
    
    def test_auto_register_uses_first_existing_config(self, tmp_path: Path):
        """Test auto_register uses first existing config file."""
        path1 = tmp_path / "config1" / "mcp.json"
        path2 = tmp_path / "config2" / "mcp.json"
        
        # Create only the second config file
        path2.parent.mkdir(parents=True)
        path2.write_text(json.dumps({"mcpServers": {}}))
        
        integration = CursorIntegration(config_paths=[path1, path2])
        result = integration.auto_register()
        
        assert result.success is True
        assert result.config_path == path2
    
    def test_auto_register_creates_parent_directories(self, tmp_path: Path):
        """Test auto_register creates parent directories if needed."""
        config_path = tmp_path / "deep" / "nested" / "path" / "mcp.json"
        integration = CursorIntegration(config_paths=[config_path])
        
        result = integration.auto_register()
        
        assert result.success is True
        assert config_path.exists()
    
    def test_unregister_removes_devbackup_from_config(self, tmp_path: Path):
        """Test unregister removes devbackup from config."""
        config_path = tmp_path / ".cursor" / "mcp.json"
        config_path.parent.mkdir(parents=True)
        
        # Write config with devbackup
        config = {
            "mcpServers": {
                "devbackup": {"command": "devbackup"},
                "other": {"command": "other"},
            }
        }
        config_path.write_text(json.dumps(config))
        
        integration = CursorIntegration(config_paths=[config_path])
        result = integration.unregister()
        
        assert result.success is True
        
        # Verify devbackup removed but other preserved
        updated_config = json.loads(config_path.read_text())
        assert "devbackup" not in updated_config["mcpServers"]
        assert "other" in updated_config["mcpServers"]
    
    def test_unregister_succeeds_when_not_registered(self, tmp_path: Path):
        """Test unregister succeeds when devbackup not registered."""
        config_path = tmp_path / ".cursor" / "mcp.json"
        config_path.parent.mkdir(parents=True)
        config_path.write_text(json.dumps({"mcpServers": {}}))
        
        integration = CursorIntegration(config_paths=[config_path])
        result = integration.unregister()
        
        assert result.success is True
    
    def test_get_config_status_returns_correct_info(self, tmp_path: Path):
        """Test get_config_status returns correct status information."""
        config_path = tmp_path / ".cursor" / "mcp.json"
        config_path.parent.mkdir(parents=True)
        
        config = {
            "mcpServers": {
                "devbackup": {
                    "command": "devbackup",
                    "args": ["mcp-server"],
                }
            }
        }
        config_path.write_text(json.dumps(config))
        
        integration = CursorIntegration(config_paths=[config_path])
        status = integration.get_config_status()
        
        assert status["is_registered"] is True
        assert status["config_exists"] is True
        assert status["config_path"] == str(config_path)
        assert "registered_config" in status
    
    def test_handles_invalid_json_gracefully(self, tmp_path: Path):
        """Test that invalid JSON in config is handled gracefully."""
        config_path = tmp_path / ".cursor" / "mcp.json"
        config_path.parent.mkdir(parents=True)
        config_path.write_text("{ invalid json }")
        
        integration = CursorIntegration(config_paths=[config_path])
        
        # is_registered should return False, not raise
        assert integration.is_registered() is False
    
    def test_handles_empty_config_file(self, tmp_path: Path):
        """Test that empty config file is handled correctly."""
        config_path = tmp_path / ".cursor" / "mcp.json"
        config_path.parent.mkdir(parents=True)
        config_path.write_text("")
        
        integration = CursorIntegration(config_paths=[config_path])
        result = integration.auto_register()
        
        assert result.success is True
        
        # Verify config was created properly
        config = json.loads(config_path.read_text())
        assert "devbackup" in config["mcpServers"]
