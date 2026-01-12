"""Command-line interface for devbackup.

This module provides the CLI for devbackup, supporting commands for:
- run: Trigger immediate backup
- status: Show backup status
- list: List snapshots
- restore: Restore from snapshot
- diff: Show changes since snapshot
- search: Search files in snapshots
- install: Install scheduler
- uninstall: Remove scheduler
- init: Create default config

Requirements: 9.1-9.10
"""

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

from devbackup.backup import run_backup, BackupResult
from devbackup.config import (
    Configuration,
    ConfigurationError,
    ValidationError,
    parse_config,
    create_default_config,
    DEFAULT_CONFIG_PATH,
)
from devbackup.lock import LockManager, LockError
from devbackup.snapshot import SnapshotEngine
from devbackup.scheduler import Scheduler, SchedulerType, SchedulerError
from devbackup.verify import IntegrityVerifier
from devbackup.health import HealthChecker


# Exit codes as defined in design document
EXIT_SUCCESS = 0
EXIT_CONFIG_ERROR = 1
EXIT_LOCK_ERROR = 2
EXIT_DESTINATION_ERROR = 3
EXIT_SNAPSHOT_ERROR = 4
EXIT_RETENTION_ERROR = 5
EXIT_GENERAL_ERROR = 1


def create_parser() -> argparse.ArgumentParser:
    """
    Create and configure the argument parser with all subcommands.
    
    Requirements: 9.9, 9.10
    """
    parser = argparse.ArgumentParser(
        prog='devbackup',
        description='Incremental backup for development projects'
    )
    parser.add_argument(
        '--version',
        action='version',
        version='%(prog)s 0.1.0'
    )
    parser.add_argument(
        '--config', '-c',
        type=Path,
        help='Path to config file (default: ~/.config/devbackup/config.toml)',
        metavar='PATH'
    )
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Verbose output'
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # devbackup run - Requirements: 9.1
    run_parser = subparsers.add_parser(
        'run',
        help='Run backup now'
    )
    
    # devbackup status - Requirements: 9.2
    status_parser = subparsers.add_parser(
        'status',
        help='Show backup status'
    )
    
    # devbackup list - Requirements: 9.3
    list_parser = subparsers.add_parser(
        'list',
        help='List snapshots'
    )
    list_parser.add_argument(
        '--json',
        action='store_true',
        help='Output as JSON'
    )
    
    # devbackup restore - Requirements: 9.4
    restore_parser = subparsers.add_parser(
        'restore',
        help='Restore from snapshot'
    )
    restore_parser.add_argument(
        'snapshot',
        help='Snapshot timestamp (YYYY-MM-DD-HHMMSS)'
    )
    restore_parser.add_argument(
        'path',
        help='Path to restore (relative to snapshot)'
    )
    restore_parser.add_argument(
        '--to',
        dest='destination',
        type=Path,
        help='Destination path (default: original location)'
    )
    
    # devbackup diff
    diff_parser = subparsers.add_parser(
        'diff',
        help='Show changes since snapshot'
    )
    diff_parser.add_argument(
        'snapshot',
        help='Snapshot timestamp (YYYY-MM-DD-HHMMSS)'
    )
    diff_parser.add_argument(
        '--path',
        help='Specific path to compare'
    )
    
    # devbackup search
    search_parser = subparsers.add_parser(
        'search',
        help='Search files in snapshots'
    )
    search_parser.add_argument(
        'pattern',
        help='File pattern to search (glob pattern)'
    )
    search_parser.add_argument(
        '--snapshot',
        help='Specific snapshot to search (default: all)'
    )
    
    # devbackup install - Requirements: 9.5
    install_parser = subparsers.add_parser(
        'install',
        help='Install scheduler'
    )
    
    # devbackup uninstall - Requirements: 9.6
    uninstall_parser = subparsers.add_parser(
        'uninstall',
        help='Remove scheduler'
    )
    
    # devbackup init - Requirements: 9.7
    init_parser = subparsers.add_parser(
        'init',
        help='Create default config'
    )
    init_parser.add_argument(
        '--force', '-f',
        action='store_true',
        help='Overwrite existing config file'
    )
    
    # devbackup verify - Requirements: 7.3
    verify_parser = subparsers.add_parser(
        'verify',
        help='Verify snapshot integrity'
    )
    verify_parser.add_argument(
        'snapshot',
        help='Snapshot timestamp (YYYY-MM-DD-HHMMSS)'
    )
    verify_parser.add_argument(
        '--pattern',
        help='Glob pattern to filter files (e.g., "*.py")'
    )
    verify_parser.add_argument(
        '--json',
        action='store_true',
        help='Output as JSON'
    )
    
    # devbackup mcp-server - Requirements: 10.9, 10.10
    mcp_parser = subparsers.add_parser(
        'mcp-server',
        help='Start MCP server for Cursor integration'
    )
    
    # devbackup health - Requirements: 12.1
    health_parser = subparsers.add_parser(
        'health',
        help='Check health of backup snapshots'
    )
    health_parser.add_argument(
        '--min-age-days',
        type=int,
        help='Only check snapshots older than N days'
    )
    health_parser.add_argument(
        '--json',
        action='store_true',
        help='Output as JSON'
    )
    
    # devbackup menubar - Launch menu bar app
    menubar_parser = subparsers.add_parser(
        'menubar',
        help='Launch menu bar status app'
    )
    
    # devbackup register-cursor - Register with Cursor IDE
    register_cursor_parser = subparsers.add_parser(
        'register-cursor',
        help='Register devbackup MCP server with Cursor IDE'
    )
    register_cursor_parser.add_argument(
        '--unregister',
        action='store_true',
        help='Remove devbackup registration from Cursor'
    )
    register_cursor_parser.add_argument(
        '--status',
        action='store_true',
        help='Show current registration status'
    )
    
    return parser


def load_config(config_path: Optional[Path], verbose: bool = False) -> Optional[Configuration]:
    """
    Load configuration from file.
    
    Returns None and prints error on failure.
    """
    try:
        config = parse_config(config_path)
        if verbose:
            print(f"Loaded config from: {config_path or DEFAULT_CONFIG_PATH}")
        return config
    except ConfigurationError as e:
        print(f"Configuration error: {e}", file=sys.stderr)
        return None
    except ValidationError as e:
        print(f"Validation error: {e}", file=sys.stderr)
        return None


def cmd_run(args: argparse.Namespace) -> int:
    """
    Execute the 'run' command - trigger immediate backup.
    
    Requirements: 9.1
    """
    if args.verbose:
        print("Starting backup...")
    
    result = run_backup(config_path=args.config)
    
    if result.success:
        if args.verbose:
            print(f"Backup completed successfully!")
            if result.snapshot_result:
                print(f"  Snapshot: {result.snapshot_result.snapshot_path}")
                print(f"  Files transferred: {result.snapshot_result.files_transferred}")
                print(f"  Duration: {result.snapshot_result.duration_seconds:.2f}s")
        else:
            print(f"Backup completed: {result.snapshot_result.snapshot_path if result.snapshot_result else 'unknown'}")
        return EXIT_SUCCESS
    else:
        print(f"Backup failed: {result.error_message}", file=sys.stderr)
        return result.exit_code


def cmd_status(args: argparse.Namespace) -> int:
    """
    Execute the 'status' command - show backup status.
    
    Requirements: 9.2
    """
    config = load_config(args.config, args.verbose)
    if config is None:
        return EXIT_CONFIG_ERROR
    
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
    
    # Get scheduler status
    scheduler_type = SchedulerType(config.scheduler.type)
    scheduler = Scheduler(
        scheduler_type=scheduler_type,
        interval_seconds=config.scheduler.interval_seconds,
    )
    scheduler_status = scheduler.get_status()
    
    # Display status
    print("devbackup Status")
    print("=" * 40)
    
    # Last backup
    if snapshots:
        last_snapshot = snapshots[0]
        print(f"Last backup: {last_snapshot.timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"  Size: {_format_size(last_snapshot.size_bytes)}")
        print(f"  Files: {last_snapshot.file_count}")
    else:
        print("Last backup: Never")
    
    print()
    
    # Current status
    if is_running:
        print(f"Status: Backup in progress (PID: {lock_holder_pid})")
    else:
        print("Status: Idle")
    
    print()
    
    # Scheduler status
    print(f"Scheduler: {config.scheduler.type}")
    if scheduler_status.get("installed"):
        interval = scheduler_status.get("interval_seconds", config.scheduler.interval_seconds)
        print(f"  Installed: Yes")
        print(f"  Interval: {_format_interval(interval)}")
    else:
        print(f"  Installed: No")
    
    print()
    
    # Snapshot count
    print(f"Total snapshots: {len(snapshots)}")
    
    return EXIT_SUCCESS


def cmd_list(args: argparse.Namespace) -> int:
    """
    Execute the 'list' command - list snapshots.
    
    Requirements: 9.3
    """
    config = load_config(args.config, args.verbose)
    if config is None:
        return EXIT_CONFIG_ERROR
    
    snapshot_engine = SnapshotEngine(
        destination=config.backup_destination,
        exclude_patterns=config.exclude_patterns,
    )
    snapshots = snapshot_engine.list_snapshots()
    
    if args.json:
        # JSON output
        output = []
        for snap in snapshots:
            output.append({
                "timestamp": snap.timestamp.isoformat(),
                "path": str(snap.path),
                "size_bytes": snap.size_bytes,
                "file_count": snap.file_count,
            })
        print(json.dumps(output, indent=2))
    else:
        # Human-readable output
        if not snapshots:
            print("No snapshots found.")
            return EXIT_SUCCESS
        
        print(f"{'Timestamp':<22} {'Size':>12} {'Files':>10}")
        print("-" * 46)
        for snap in snapshots:
            timestamp_str = snap.timestamp.strftime('%Y-%m-%d %H:%M:%S')
            size_str = _format_size(snap.size_bytes)
            print(f"{timestamp_str:<22} {size_str:>12} {snap.file_count:>10}")
        
        print("-" * 46)
        print(f"Total: {len(snapshots)} snapshot(s)")
    
    return EXIT_SUCCESS


def cmd_restore(args: argparse.Namespace) -> int:
    """
    Execute the 'restore' command - restore from snapshot.
    
    Requirements: 9.4
    """
    config = load_config(args.config, args.verbose)
    if config is None:
        return EXIT_CONFIG_ERROR
    
    snapshot_engine = SnapshotEngine(
        destination=config.backup_destination,
        exclude_patterns=config.exclude_patterns,
    )
    
    # Find the snapshot
    snapshot_path = snapshot_engine.get_snapshot_by_timestamp(args.snapshot)
    if snapshot_path is None:
        print(f"Snapshot not found: {args.snapshot}", file=sys.stderr)
        return EXIT_GENERAL_ERROR
    
    # Perform restore
    destination = args.destination
    success = snapshot_engine.restore(
        snapshot=snapshot_path,
        source_path=args.path,
        destination=destination,
        source_directories=config.source_directories,
    )
    
    if success:
        dest_str = str(destination) if destination else "original location"
        print(f"Restored '{args.path}' from {args.snapshot} to {dest_str}")
        return EXIT_SUCCESS
    else:
        print(f"Failed to restore '{args.path}' from {args.snapshot}", file=sys.stderr)
        return EXIT_GENERAL_ERROR


def cmd_diff(args: argparse.Namespace) -> int:
    """
    Execute the 'diff' command - show changes since snapshot.
    """
    config = load_config(args.config, args.verbose)
    if config is None:
        return EXIT_CONFIG_ERROR
    
    snapshot_engine = SnapshotEngine(
        destination=config.backup_destination,
        exclude_patterns=config.exclude_patterns,
    )
    
    # Find the snapshot
    snapshot_path = snapshot_engine.get_snapshot_by_timestamp(args.snapshot)
    if snapshot_path is None:
        print(f"Snapshot not found: {args.snapshot}", file=sys.stderr)
        return EXIT_GENERAL_ERROR
    
    # Get diff
    diff_result = snapshot_engine.diff(
        snapshot=snapshot_path,
        source_directories=config.source_directories,
        source_path=args.path,
    )
    
    # Display results
    if not any([diff_result["added"], diff_result["modified"], diff_result["deleted"]]):
        print("No changes detected.")
        return EXIT_SUCCESS
    
    if diff_result["added"]:
        print(f"Added ({len(diff_result['added'])}):")
        for path in diff_result["added"][:20]:  # Limit output
            print(f"  + {path}")
        if len(diff_result["added"]) > 20:
            print(f"  ... and {len(diff_result['added']) - 20} more")
    
    if diff_result["modified"]:
        print(f"Modified ({len(diff_result['modified'])}):")
        for path in diff_result["modified"][:20]:
            print(f"  ~ {path}")
        if len(diff_result["modified"]) > 20:
            print(f"  ... and {len(diff_result['modified']) - 20} more")
    
    if diff_result["deleted"]:
        print(f"Deleted ({len(diff_result['deleted'])}):")
        for path in diff_result["deleted"][:20]:
            print(f"  - {path}")
        if len(diff_result["deleted"]) > 20:
            print(f"  ... and {len(diff_result['deleted']) - 20} more")
    
    return EXIT_SUCCESS


def cmd_search(args: argparse.Namespace) -> int:
    """
    Execute the 'search' command - search files in snapshots.
    """
    config = load_config(args.config, args.verbose)
    if config is None:
        return EXIT_CONFIG_ERROR
    
    snapshot_engine = SnapshotEngine(
        destination=config.backup_destination,
        exclude_patterns=config.exclude_patterns,
    )
    
    # Find specific snapshot if provided
    snapshot_path = None
    if args.snapshot:
        snapshot_path = snapshot_engine.get_snapshot_by_timestamp(args.snapshot)
        if snapshot_path is None:
            print(f"Snapshot not found: {args.snapshot}", file=sys.stderr)
            return EXIT_GENERAL_ERROR
    
    # Search
    results = snapshot_engine.search(
        pattern=args.pattern,
        snapshot=snapshot_path,
    )
    
    if not results:
        print(f"No files matching '{args.pattern}' found.")
        return EXIT_SUCCESS
    
    print(f"Found {len(results)} match(es):")
    print(f"{'Snapshot':<22} {'Size':>12} {'Path'}")
    print("-" * 70)
    
    for match in results[:50]:  # Limit output
        size_str = _format_size(match["size"])
        print(f"{match['snapshot']:<22} {size_str:>12} {match['path']}")
    
    if len(results) > 50:
        print(f"... and {len(results) - 50} more matches")
    
    return EXIT_SUCCESS


def cmd_install(args: argparse.Namespace) -> int:
    """
    Execute the 'install' command - install scheduler.
    
    Requirements: 9.5
    """
    config = load_config(args.config, args.verbose)
    if config is None:
        return EXIT_CONFIG_ERROR
    
    scheduler_type = SchedulerType(config.scheduler.type)
    scheduler = Scheduler(
        scheduler_type=scheduler_type,
        interval_seconds=config.scheduler.interval_seconds,
        log_file=config.logging.log_file,
        error_log_file=config.logging.error_log_file,
    )
    
    try:
        scheduler.install()
        interval_str = _format_interval(config.scheduler.interval_seconds)
        print(f"Scheduler installed ({config.scheduler.type})")
        print(f"Backups will run every {interval_str}")
        return EXIT_SUCCESS
    except SchedulerError as e:
        print(f"Failed to install scheduler: {e}", file=sys.stderr)
        return EXIT_GENERAL_ERROR


def cmd_uninstall(args: argparse.Namespace) -> int:
    """
    Execute the 'uninstall' command - remove scheduler.
    
    Requirements: 9.6
    """
    config = load_config(args.config, args.verbose)
    if config is None:
        return EXIT_CONFIG_ERROR
    
    scheduler_type = SchedulerType(config.scheduler.type)
    scheduler = Scheduler(
        scheduler_type=scheduler_type,
        interval_seconds=config.scheduler.interval_seconds,
    )
    
    try:
        if not scheduler.is_installed():
            print("Scheduler is not installed.")
            return EXIT_SUCCESS
        
        scheduler.uninstall()
        print("Scheduler removed.")
        return EXIT_SUCCESS
    except SchedulerError as e:
        print(f"Failed to remove scheduler: {e}", file=sys.stderr)
        return EXIT_GENERAL_ERROR


def cmd_init(args: argparse.Namespace) -> int:
    """
    Execute the 'init' command - create default config.
    
    Requirements: 9.7
    """
    config_path = args.config or DEFAULT_CONFIG_PATH
    
    if config_path.exists() and not args.force:
        print(f"Config file already exists: {config_path}", file=sys.stderr)
        print("Use --force to overwrite.", file=sys.stderr)
        return EXIT_GENERAL_ERROR
    
    # Create parent directories
    config_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Write default config
    default_config = create_default_config()
    config_path.write_text(default_config)
    
    print(f"Created default config: {config_path}")
    print("Edit this file to configure your backup settings.")
    
    return EXIT_SUCCESS


def cmd_verify(args: argparse.Namespace) -> int:
    """
    Execute the 'verify' command - verify snapshot integrity.
    
    Requirements: 7.3
    """
    config = load_config(args.config, args.verbose)
    if config is None:
        return EXIT_CONFIG_ERROR
    
    snapshot_engine = SnapshotEngine(
        destination=config.backup_destination,
        exclude_patterns=config.exclude_patterns,
    )
    
    # Find the snapshot
    snapshot_path = snapshot_engine.get_snapshot_by_timestamp(args.snapshot)
    if snapshot_path is None:
        print(f"Snapshot not found: {args.snapshot}", file=sys.stderr)
        return EXIT_GENERAL_ERROR
    
    # Verify the snapshot
    verifier = IntegrityVerifier()
    result = verifier.verify_snapshot(
        snapshot_path=snapshot_path,
        pattern=args.pattern,
    )
    
    if args.json:
        # JSON output
        output = {
            "success": result.success,
            "snapshot": args.snapshot,
            "files_verified": result.files_verified,
            "files_failed": result.files_failed,
            "missing_files": result.missing_files,
            "corrupted_files": result.corrupted_files,
            "errors": result.errors,
        }
        print(json.dumps(output, indent=2))
    else:
        # Human-readable output
        print(f"Verification of snapshot: {args.snapshot}")
        print("=" * 50)
        
        if result.success:
            print(f"Status: PASSED")
            print(f"Files verified: {result.files_verified}")
        else:
            print(f"Status: FAILED")
            print(f"Files verified: {result.files_verified}")
            print(f"Files failed: {result.files_failed}")
            
            if result.missing_files:
                print(f"\nMissing files ({len(result.missing_files)}):")
                for path in result.missing_files[:20]:
                    print(f"  - {path}")
                if len(result.missing_files) > 20:
                    print(f"  ... and {len(result.missing_files) - 20} more")
            
            if result.corrupted_files:
                print(f"\nCorrupted files ({len(result.corrupted_files)}):")
                for path in result.corrupted_files[:20]:
                    print(f"  ! {path}")
                if len(result.corrupted_files) > 20:
                    print(f"  ... and {len(result.corrupted_files) - 20} more")
            
            if result.errors:
                print(f"\nErrors ({len(result.errors)}):")
                for error in result.errors[:10]:
                    print(f"  * {error}")
                if len(result.errors) > 10:
                    print(f"  ... and {len(result.errors) - 10} more")
    
    return EXIT_SUCCESS if result.success else EXIT_GENERAL_ERROR


def cmd_mcp_server(args: argparse.Namespace) -> int:
    """
    Execute the 'mcp-server' command - start MCP server.
    
    Requirements: 10.9, 10.10
    """
    from devbackup.mcp_server import run_server
    
    try:
        run_server(config_path=args.config)
        return EXIT_SUCCESS
    except KeyboardInterrupt:
        return EXIT_SUCCESS
    except Exception as e:
        print(f"MCP server error: {e}", file=sys.stderr)
        return EXIT_GENERAL_ERROR


def cmd_health(args: argparse.Namespace) -> int:
    """
    Execute the 'health' command - check health of backup snapshots.
    
    Requirements: 12.1
    """
    config = load_config(args.config, args.verbose)
    if config is None:
        return EXIT_CONFIG_ERROR
    
    verifier = IntegrityVerifier()
    health_checker = HealthChecker(
        destination=config.backup_destination,
        verifier=verifier,
    )
    
    result = health_checker.check_all(min_age_days=args.min_age_days)
    
    if args.json:
        # JSON output
        output = {
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
        print(json.dumps(output, indent=2))
    else:
        # Human-readable output
        print("Backup Health Check")
        print("=" * 50)
        
        if result.total_snapshots == 0:
            print("No snapshots found.")
            return EXIT_SUCCESS
        
        print(f"Total snapshots: {result.total_snapshots}")
        print(f"Healthy: {result.healthy_snapshots}")
        print(f"Unhealthy: {result.unhealthy_snapshots}")
        print()
        
        # Show details for unhealthy snapshots
        unhealthy = [s for s in result.snapshots if not _is_snapshot_healthy(s)]
        if unhealthy:
            print("Unhealthy Snapshots:")
            print("-" * 50)
            for snap in unhealthy:
                print(f"\n{snap.snapshot_name}:")
                if not snap.readable:
                    print("  - Not readable")
                if snap.has_manifest and not snap.manifest_valid:
                    print("  - Manifest validation failed")
                if snap.corrupted_files:
                    print(f"  - Corrupted files: {len(snap.corrupted_files)}")
                    for f in snap.corrupted_files[:5]:
                        print(f"      ! {f}")
                    if len(snap.corrupted_files) > 5:
                        print(f"      ... and {len(snap.corrupted_files) - 5} more")
                if snap.missing_files:
                    print(f"  - Missing files: {len(snap.missing_files)}")
                    for f in snap.missing_files[:5]:
                        print(f"      - {f}")
                    if len(snap.missing_files) > 5:
                        print(f"      ... and {len(snap.missing_files) - 5} more")
                if snap.error:
                    print(f"  - Error: {snap.error}")
        
        if result.errors:
            print("\nErrors:")
            for error in result.errors:
                print(f"  * {error}")
    
    return EXIT_SUCCESS if result.unhealthy_snapshots == 0 else EXIT_GENERAL_ERROR


def cmd_register_cursor(args: argparse.Namespace) -> int:
    """
    Execute the 'register-cursor' command - register with Cursor IDE.
    """
    from devbackup.cursor_integration import CursorIntegration
    
    integration = CursorIntegration()
    
    if args.status:
        # Show status
        status = integration.get_config_status()
        if status["is_registered"]:
            print("devbackup is registered with Cursor")
            print(f"  Config file: {status['config_path']}")
        else:
            print("devbackup is not registered with Cursor")
            if status["config_exists"]:
                print(f"  Config file exists: {status['config_path']}")
            else:
                print("  No Cursor config file found")
        return EXIT_SUCCESS
    
    if args.unregister:
        # Unregister
        result = integration.unregister()
        print(result.message)
        return EXIT_SUCCESS if result.success else EXIT_GENERAL_ERROR
    
    # Register
    result = integration.auto_register()
    print(result.message)
    
    if result.success and not result.already_registered:
        print("\nRestart Cursor to activate the integration.")
        print("Then you can say things like:")
        print('  "Back up my projects"')
        print('  "What\'s my backup status?"')
        print('  "Restore app.py from yesterday"')
    
    return EXIT_SUCCESS if result.success else EXIT_GENERAL_ERROR


def cmd_menubar(args: argparse.Namespace) -> int:
    """
    Execute the 'menubar' command - launch menu bar app.
    """
    from devbackup.menubar_app import main as menubar_main
    
    print("Starting DevBackup menu bar app...")
    try:
        menubar_main()
        return EXIT_SUCCESS
    except KeyboardInterrupt:
        return EXIT_SUCCESS
    except Exception as e:
        print(f"Menu bar app error: {e}", file=sys.stderr)
        return EXIT_GENERAL_ERROR


def _is_snapshot_healthy(health) -> bool:
    """Determine if a snapshot is healthy."""
    if not health.readable:
        return False
    if health.has_manifest and not health.manifest_valid:
        return False
    if health.corrupted_files or health.missing_files:
        return False
    return True


def _format_size(size_bytes: int) -> str:
    """Format size in bytes to human-readable string."""
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} KB"
    elif size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):.1f} MB"
    else:
        return f"{size_bytes / (1024 * 1024 * 1024):.1f} GB"


def _format_interval(seconds: int) -> str:
    """Format interval in seconds to human-readable string."""
    if seconds < 60:
        return f"{seconds} seconds"
    elif seconds < 3600:
        minutes = seconds // 60
        return f"{minutes} minute{'s' if minutes != 1 else ''}"
    elif seconds < 86400:
        hours = seconds // 3600
        return f"{hours} hour{'s' if hours != 1 else ''}"
    else:
        days = seconds // 86400
        return f"{days} day{'s' if days != 1 else ''}"


def main(argv: list = None) -> int:
    """
    Main CLI entry point.
    
    Args:
        argv: Command line arguments (defaults to sys.argv[1:])
    
    Returns:
        Exit code (0 for success, non-zero for failure)
    
    Requirements: 9.8
    """
    parser = create_parser()
    args = parser.parse_args(argv)
    
    # If no command specified, show help
    if args.command is None:
        parser.print_help()
        return EXIT_SUCCESS
    
    # Dispatch to command handler
    try:
        if args.command == 'run':
            return cmd_run(args)
        elif args.command == 'status':
            return cmd_status(args)
        elif args.command == 'list':
            return cmd_list(args)
        elif args.command == 'restore':
            return cmd_restore(args)
        elif args.command == 'diff':
            return cmd_diff(args)
        elif args.command == 'search':
            return cmd_search(args)
        elif args.command == 'install':
            return cmd_install(args)
        elif args.command == 'uninstall':
            return cmd_uninstall(args)
        elif args.command == 'init':
            return cmd_init(args)
        elif args.command == 'verify':
            return cmd_verify(args)
        elif args.command == 'mcp-server':
            return cmd_mcp_server(args)
        elif args.command == 'health':
            return cmd_health(args)
        elif args.command == 'register-cursor':
            return cmd_register_cursor(args)
        elif args.command == 'menubar':
            return cmd_menubar(args)
        else:
            print(f"Unknown command: {args.command}", file=sys.stderr)
            return EXIT_GENERAL_ERROR
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        return 130  # Standard exit code for SIGINT
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return EXIT_GENERAL_ERROR


if __name__ == "__main__":
    sys.exit(main())
