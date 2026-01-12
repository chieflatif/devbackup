# DevBackup

**Never lose your code again.**

DevBackup is an automatic backup tool for developers on macOS. It runs quietly in your menu bar, creating incremental snapshots of your projects every hour. When disaster strikes—accidental deletion, bad merge, corrupted file—your code is right there waiting.

## The Problem

You're deep in a coding session. You refactor something, delete some files, maybe force-push to the wrong branch. Hours later, you realize you need that code back. Git can't help (you already committed over it). Time Machine is backing up your whole system and that file you need is from 3 hours ago.

## The Solution

DevBackup creates **timestamped snapshots** of just your project folders, every hour. Each snapshot is a complete copy of your files, but thanks to hard links, unchanged files don't take extra disk space. A project with 10 snapshots might only use 10% more space than a single copy.

**What makes it different:**
- **Developer-focused** — Auto-excludes `node_modules`, `.git`, `build`, `__pycache__` and other junk
- **Space-efficient** — Uses rsync hard links, so 10 backups ≠ 10x the space
- **Runs in your menu bar** — See backup status at a glance, trigger manual backups with one click
- **Shows you what changed** — "3 files changed / 2,895 total" so you know it's actually working
- **Battery-aware** — Won't drain your laptop when you're running low
- **Works with Cursor AI** — Manage backups through natural language conversation

## Quick Look

Click the menu bar icon to see your backup status:

```
🟢 Ready to back up
✓ Last: 2 hours ago • 3 changed / 2,895 total
─────────────────────────────────────────────
Back Up Now
Browse Backups
─────────────────────────────────────────────
Preferences...
☑ Start at Login
─────────────────────────────────────────────
Quit
```

When a backup completes, you get a notification:
> **✅ Backup Complete**  
> 3 files changed • 2 sec  
> 2,895 total files protected

## Installation

```bash
pip install devbackup
```

Requires macOS 12+ and Python 3.11+.

## Getting Started

### 1. Start the menu bar app

```bash
devbackup menubar
```

### 2. Configure your backups

Click the DB icon → **Preferences...**

- Pick the folders you want to back up (your project directories)
- Pick where to store backups (external drive, iCloud, or local folder)

### 3. That's it

Backups run automatically every hour. Enable "Start at Login" to have DevBackup always running.

## How It Works

DevBackup uses `rsync` with the `--link-dest` flag to create incremental backups:

1. **First backup**: Full copy of all your files
2. **Every backup after**: Only changed files are copied; unchanged files are hard-linked to the previous snapshot

This means:
- Each snapshot looks like a complete backup (you can browse it in Finder)
- But unchanged files share disk space across snapshots
- A 1GB project with 20 snapshots might only use 1.2GB total

Your backups are stored as simple folders with timestamps:
```
/Volumes/BackupDrive/DevBackups/
├── 2025-01-11-100000/    ← Complete snapshot
├── 2025-01-11-110000/    ← Only changed files copied
├── 2025-01-11-120000/    ← Only changed files copied
└── 2025-01-11-130000/    ← Only changed files copied
```

## Restoring Files

### From Finder
Just open your backup folder and copy files back. Each snapshot is a regular folder.

### From CLI
```bash
devbackup restore 2025-01-11-100000 path/to/file.py
```

### From Cursor AI
> "Restore the config.py file from yesterday"

## Features

### Menu Bar App
- Real-time backup progress
- Shows files changed vs total files
- Toast notifications with sound on completion
- One-click manual backup
- Auto-start at login

### Smart Defaults
- Auto-excludes: `node_modules/`, `.git/`, `build/`, `dist/`, `__pycache__/`, `.next/`, `target/`, `vendor/`
- Retention policy: 24 hourly + 7 daily + 4 weekly snapshots
- Battery threshold: Skips backup below 20% battery

### CLI Commands
```bash
devbackup run              # Run backup now
devbackup status           # Show backup status  
devbackup list             # List all snapshots
devbackup restore          # Restore files
devbackup diff             # Show what changed
devbackup search "*.py"    # Find files across snapshots
devbackup verify           # Check backup integrity
```

### Cursor AI Integration
Add to `.cursor/mcp.json`:
```json
{
  "mcpServers": {
    "devbackup": {
      "command": "devbackup",
      "args": ["mcp-server"]
    }
  }
}
```

Then just ask:
- "Back up my projects"
- "What's my backup status?"
- "Restore app.py from yesterday"
- "Find the config file I edited last week"

## Configuration

Config file: `~/.config/devbackup/config.toml`

```toml
[main]
backup_destination = "/Volumes/BackupDrive/DevBackups"
source_directories = [
    "~/Projects",
    "~/Code"
]

[retention]
hourly = 24
daily = 7
weekly = 4

[scheduler]
interval_seconds = 3600  # 1 hour
```

## Requirements

- macOS 12 or later
- Python 3.11+
- rsync (included with macOS)

## Contributing

Contributions welcome! See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

MIT License - see [LICENSE](LICENSE).

---

**DevBackup** — Your code, always safe.
