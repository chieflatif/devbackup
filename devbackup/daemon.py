#!/usr/bin/env python3
"""
DevBackup Daemon

Background service that handles backup operations and IPC communication.
This daemon is automatically started by the menu bar app.

Requirements: 3.8
"""

import asyncio
import logging
import signal
import sys
from pathlib import Path
from typing import Optional

from devbackup.ipc import create_configured_server, DEFAULT_SOCKET_PATH
from devbackup.config import DEFAULT_CONFIG_PATH


# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class BackupDaemon:
    """Background daemon for backup operations."""
    
    def __init__(
        self,
        config_path: Optional[Path] = None,
        socket_path: Optional[Path] = None,
    ):
        self.config_path = config_path or DEFAULT_CONFIG_PATH
        self.socket_path = socket_path or DEFAULT_SOCKET_PATH
        self.server = None
        self._running = False
    
    async def start(self):
        """Start the daemon."""
        logger.info("Starting DevBackup daemon...")
        
        # Create IPC server with backup handlers
        self.server = create_configured_server(
            config_path=self.config_path,
            socket_path=self.socket_path,
            logger=logger,
        )
        
        # Start the server
        await self.server.start()
        logger.info(f"IPC server listening on {self.socket_path}")
        
        self._running = True
        
        # Keep running until stopped
        while self._running:
            await asyncio.sleep(1)
    
    async def stop(self):
        """Stop the daemon."""
        logger.info("Stopping DevBackup daemon...")
        self._running = False
        
        if self.server:
            await self.server.stop()
        
        logger.info("Daemon stopped")
    
    def handle_signal(self, signum, frame):
        """Handle shutdown signals."""
        logger.info(f"Received signal {signum}, shutting down...")
        self._running = False


async def run_daemon(
    config_path: Optional[Path] = None,
    socket_path: Optional[Path] = None,
):
    """Run the backup daemon."""
    daemon = BackupDaemon(
        config_path=config_path,
        socket_path=socket_path,
    )
    
    # Set up signal handlers
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, daemon.handle_signal, sig, None)
    
    try:
        await daemon.start()
    except asyncio.CancelledError:
        pass
    finally:
        await daemon.stop()


def main():
    """Main entry point for the daemon."""
    try:
        asyncio.run(run_daemon())
    except KeyboardInterrupt:
        pass
    except Exception as e:
        logger.error(f"Daemon error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
