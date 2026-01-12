# Contributing to DevBackup

Thank you for your interest in contributing to DevBackup! This document provides guidelines and information for contributors.

## Code of Conduct

Please be respectful and constructive in all interactions. We're all here to make a useful tool for developers.

## How to Contribute

### Reporting Bugs

1. Check if the bug has already been reported in [Issues](https://github.com/yourusername/devbackup/issues)
2. If not, create a new issue with:
   - Clear, descriptive title
   - Steps to reproduce
   - Expected vs actual behavior
   - macOS version and Python version
   - Relevant log output (`~/.local/log/devbackup.log`)

### Suggesting Features

1. Check existing issues and discussions for similar ideas
2. Create a new issue with the "enhancement" label
3. Describe the use case and proposed solution

### Pull Requests

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes
4. Run tests (`pytest`)
5. Commit with clear messages (`git commit -m 'Add amazing feature'`)
6. Push to your fork (`git push origin feature/amazing-feature`)
7. Open a Pull Request

## Development Setup

### Prerequisites

- macOS 12+
- Python 3.11+
- Git

### Setup

```bash
# Clone your fork
git clone https://github.com/YOUR_USERNAME/devbackup.git
cd devbackup

# Create virtual environment
python -m venv .venv
source .venv/bin/activate

# Install with dev dependencies
pip install -e ".[dev]"

# Run tests to verify setup
pytest
```

### Running Tests

```bash
# All tests
pytest

# With coverage report
pytest --cov=devbackup --cov-report=html

# Specific test file
pytest tests/test_backup.py

# Property-based tests (may take longer)
pytest tests/test_*_properties.py -v
```

### Code Style

We use:
- **ruff** for linting and formatting
- **mypy** for type checking

```bash
# Check linting
ruff check devbackup/

# Format code
ruff format devbackup/

# Type check
mypy devbackup/
```

## Project Structure

```
devbackup/
├── devbackup/           # Main package
│   ├── __init__.py
│   ├── backup.py        # Backup orchestration
│   ├── cli.py           # CLI commands
│   ├── config.py        # Configuration parsing
│   ├── menubar_app.py   # macOS menu bar app
│   ├── mcp_server.py    # Cursor MCP server
│   ├── snapshot.py      # rsync snapshot engine
│   ├── retention.py     # Retention policies
│   ├── verify.py        # Integrity verification
│   └── ...
├── tests/               # Test files
│   ├── test_*.py        # Unit tests
│   └── test_*_properties.py  # Property-based tests
├── docs/                # Documentation
├── pyproject.toml       # Project configuration
└── README.md
```

## Architecture Overview

### Core Components

1. **SnapshotEngine** (`snapshot.py`)
   - Creates incremental backups using rsync
   - Manages hard links via `--link-dest`
   - Handles timestamp collision detection

2. **RetentionManager** (`retention.py`)
   - Applies hourly/daily/weekly retention policies
   - Safely removes old snapshots

3. **BackupOrchestrator** (`backup.py`)
   - Coordinates the full backup process
   - Handles locking, validation, error recovery

4. **MenuBarApp** (`menubar_app.py`)
   - Native macOS status bar integration
   - Progress display and notifications

5. **MCPServer** (`mcp_server.py`)
   - Model Context Protocol for AI integration
   - Natural language backup management

### Key Design Decisions

- **rsync with hard links**: Space-efficient incremental backups
- **Atomic operations**: In-progress snapshots renamed on completion
- **Graceful degradation**: Works without daemon, external drives, etc.
- **Battery awareness**: Skips backups on low battery

## Testing Guidelines

### Unit Tests

- Test individual functions and classes
- Mock external dependencies (filesystem, subprocess)
- Fast execution (< 1 second per test)

### Property-Based Tests

- Use Hypothesis for property-based testing
- Test invariants that should always hold
- Example: "Retention never deletes the most recent snapshot"

### Integration Tests

- Test full backup/restore workflows
- Use temporary directories
- Clean up after tests

## Documentation

- Update README.md for user-facing changes
- Update docs/USER_GUIDE.md for usage changes
- Add docstrings to new functions/classes
- Include type hints

## Release Process

1. Update version in `pyproject.toml`
2. Update CHANGELOG.md
3. Create git tag (`git tag v0.2.0`)
4. Push tag (`git push origin v0.2.0`)
5. GitHub Actions builds and publishes to PyPI

## Questions?

- Open a [Discussion](https://github.com/yourusername/devbackup/discussions)
- Check existing issues and PRs

Thank you for contributing! 🎉
