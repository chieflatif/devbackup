#!/bin/bash
#
# DevBackup Installer for macOS
# Double-click this file to install DevBackup
#

set -e

echo "╔════════════════════════════════════════════╗"
echo "║         DevBackup Installer                ║"
echo "║     Never lose your code again.            ║"
echo "╚════════════════════════════════════════════╝"
echo ""

# Check macOS version
if [[ "$(uname)" != "Darwin" ]]; then
    echo "❌ This installer is for macOS only."
    exit 1
fi

echo "📦 Installing DevBackup..."
echo ""

# Check for Python 3.11+
PYTHON=""
for py in python3.13 python3.12 python3.11 python3; do
    if command -v "$py" &> /dev/null; then
        version=$("$py" -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')" 2>/dev/null)
        major=$(echo "$version" | cut -d. -f1)
        minor=$(echo "$version" | cut -d. -f2)
        if [[ "$major" -ge 3 && "$minor" -ge 11 ]]; then
            PYTHON="$py"
            break
        fi
    fi
done

if [[ -z "$PYTHON" ]]; then
    echo "❌ Python 3.11 or later is required."
    echo ""
    echo "Install Python from: https://www.python.org/downloads/"
    echo "Or with Homebrew: brew install python@3.12"
    echo ""
    read -p "Press Enter to exit..."
    exit 1
fi

echo "✓ Found Python: $PYTHON ($version)"

# Create virtual environment in user's home
INSTALL_DIR="$HOME/.devbackup"
VENV_DIR="$INSTALL_DIR/venv"

echo "📁 Installing to: $INSTALL_DIR"

mkdir -p "$INSTALL_DIR"

# Create virtual environment
echo "🔧 Creating virtual environment..."
"$PYTHON" -m venv "$VENV_DIR"

# Install devbackup
echo "📥 Downloading and installing DevBackup..."
"$VENV_DIR/bin/pip" install --upgrade pip -q
"$VENV_DIR/bin/pip" install git+https://github.com/chieflatif/devbackup.git -q

echo "✓ DevBackup installed"

# Create the .app bundle
APP_DIR="/Applications/DevBackup.app"
echo "🖥️  Creating application..."

mkdir -p "$APP_DIR/Contents/MacOS"
mkdir -p "$APP_DIR/Contents/Resources"

# Create Info.plist
cat > "$APP_DIR/Contents/Info.plist" << 'PLIST'
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>CFBundleExecutable</key>
    <string>DevBackup</string>
    <key>CFBundleIdentifier</key>
    <string>com.devbackup.menubar</string>
    <key>CFBundleName</key>
    <string>DevBackup</string>
    <key>CFBundleDisplayName</key>
    <string>DevBackup</string>
    <key>CFBundleVersion</key>
    <string>0.1.0</string>
    <key>CFBundleShortVersionString</key>
    <string>0.1.0</string>
    <key>CFBundlePackageType</key>
    <string>APPL</string>
    <key>LSMinimumSystemVersion</key>
    <string>12.0</string>
    <key>LSUIElement</key>
    <true/>
    <key>NSHighResolutionCapable</key>
    <true/>
</dict>
</plist>
PLIST

# Create launcher script
cat > "$APP_DIR/Contents/MacOS/DevBackup" << LAUNCHER
#!/bin/bash
exec "$VENV_DIR/bin/devbackup" menubar
LAUNCHER

chmod +x "$APP_DIR/Contents/MacOS/DevBackup"

echo "✓ Application created"

# Create default config if it doesn't exist
CONFIG_DIR="$HOME/.config/devbackup"
CONFIG_FILE="$CONFIG_DIR/config.toml"

if [[ ! -f "$CONFIG_FILE" ]]; then
    echo "📝 Creating default configuration..."
    "$VENV_DIR/bin/devbackup" init
    echo "✓ Configuration created at: $CONFIG_FILE"
fi

# Add CLI to PATH by creating a symlink
echo "🔗 Adding 'devbackup' command to PATH..."
mkdir -p "$HOME/.local/bin"
ln -sf "$VENV_DIR/bin/devbackup" "$HOME/.local/bin/devbackup"

# Check if ~/.local/bin is in PATH
if [[ ":$PATH:" != *":$HOME/.local/bin:"* ]]; then
    echo ""
    echo "💡 Add this to your shell profile (~/.zshrc or ~/.bashrc):"
    echo "   export PATH=\"\$HOME/.local/bin:\$PATH\""
fi

echo ""
echo "╔════════════════════════════════════════════╗"
echo "║         ✅ Installation Complete!          ║"
echo "╚════════════════════════════════════════════╝"
echo ""
echo "📍 DevBackup.app is now in your Applications folder"
echo ""
echo "Next steps:"
echo "  1. Open DevBackup from Applications (or Spotlight)"
echo "  2. Click the DB icon in your menu bar"
echo "  3. Go to Preferences to set up your backup folders"
echo ""
echo "Or configure from terminal:"
echo "  devbackup init      # Create/edit config"
echo "  devbackup run       # Run a backup"
echo "  devbackup status    # Check status"
echo ""

# Ask to launch
read -p "🚀 Launch DevBackup now? [Y/n] " -n 1 -r
echo ""
if [[ $REPLY =~ ^[Yy]$ ]] || [[ -z $REPLY ]]; then
    open "/Applications/DevBackup.app"
    echo "✓ DevBackup is now running in your menu bar (look for 'DB')"
fi

echo ""
echo "Thanks for installing DevBackup! 🎉"
echo ""
read -p "Press Enter to close this window..."
