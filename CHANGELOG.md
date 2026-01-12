# Changelog

All notable changes to DevBackup will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2025-01-11

### Added

#### Core Features
- Incremental backup engine using rsync with hard links
- Timestamped snapshots in `YYYY-MM-DD-HHMMSS` format
- Configurable retention policies (hourly/daily/weekly)
- SHA-256 integrity verification with manifests
- Automatic retry with exponential backoff for transient failures
- Disk space validation before backup
- Lock management to prevent concurrent backups
- Signal handling for graceful shutdown (SIGTERM, SIGINT)

#### Menu Bar App
- Native macOS menu bar application using rumps
- Real-time backup progress display (file count, elapsed time)
- Toast notifications on backup completion with sound
- "Back Up Now" for manual backup trigger
- "Browse Backups" to open backup folder in Finder
- Preferences dialog for source/destination configuration
- "Start at Login" auto-start via LaunchAgent
- Status icons: ✓ (protected), ⟳ (backing up), ! (attention needed)

#### CLI Commands
- `devbackup run` - Run backup immediately
- `devbackup status` - Show backup status
- `devbackup list` - List all snapshots
- `devbackup restore` - Restore files from snapshot
- `devbackup diff` - Show changes since snapshot
- `devbackup search` - Search files across snapshots
- `devbackup verify` - Verify snapshot integrity
- `devbackup health` - Check health of all snapshots
- `devbackup install/uninstall` - Manage automatic scheduling
- `devbackup menubar` - Start menu bar app
- `devbackup init` - Initialize configuration

#### AI Integration
- MCP (Model Context Protocol) server for Cursor
- Zero-config setup via natural language
- Auto-discovery of projects and backup destinations
- Plain language status explanations
- Natural language file search and restore
- `devbackup register-cursor` for auto-registration

#### Smart Features
- Auto-discovery of development projects (package.json, pyproject.toml, etc.)
- Auto-discovery of backup destinations (external drives, iCloud)
- Battery-aware scheduling (skips when battery < 20%)
- Developer-focused exclude patterns (node_modules, .git, build, etc.)
- Backup queue for offline destinations

### Technical Details
- Python 3.11+ required
- macOS 12+ supported
- Uses system rsync for reliability
- TOML configuration format
- Comprehensive test suite with property-based tests

---

## Version History

- **0.1.0** - Initial release with full feature set
