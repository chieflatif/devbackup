# devbackup User Guide

A friendly guide to keeping your work safe with automatic backups.

## What is devbackup?

devbackup automatically saves copies of your project files so you never lose your work. Think of it like a safety net - if you accidentally delete something or want to go back to an earlier version, your files are there waiting for you.

## Getting Started

### The Easy Way (Recommended)

If you use Cursor, just open it and say:

> "Back up my projects"

That's it! The AI assistant will:
1. Find your project folders automatically
2. Find a good place to store backups (like an external drive)
3. Set everything up for you

### What You'll See

When you first ask to back up your projects, you'll see something like:

> "I found 3 projects on your computer:
> - MyWebsite (about 120 MB)
> - PythonScripts (about 5 MB)  
> - MobileApp (about 450 MB)
>
> I can save backups to your 'Backup Drive' which has plenty of space (500 GB free).
>
> Should I set this up?"

Just say "yes" and you're protected!

## Starting DevBackup

### From the Menu Bar

The easiest way to use DevBackup is through the menu bar app:

1. Open Terminal and run: `devbackup menubar`
2. Or, if you've enabled "Start at Login", it starts automatically

### Making it Start Automatically

To have DevBackup start every time you log in:
1. Click the "DB" icon in your menu bar
2. Check "Start at Login"

Now DevBackup will always be there, keeping your files safe.

## Checking Your Backups

### "Are my files safe?"

Ask Cursor:
> "What's my backup status?"

You'll get a friendly answer like:
> "Your projects are safely backed up. The last backup was 2 hours ago, and the next one will run in about an hour."

### Menu Bar Icon

Look for "DB" or a status icon in your menu bar (top right of your screen):

| What you see | What it means |
|--------------|---------------|
| ✓ | Everything's good - your files are backed up |
| ↻ | A backup is happening right now |
| ! | Setup needed or something needs attention |
| ? | Checking status... |
| DB | Ready and waiting |

Click the icon to see:
- Current status (e.g., "🟢 Protected" or "⚠️ Setup required")
- **Back Up Now** - Run a backup immediately
- **Browse Backups** - Open your backup folder in Finder
- **Preferences...** - Set up or change your backup settings
- **Start at Login** - Make DevBackup start automatically when you log in
- **Quit** - Close the menu bar app

### First-Time Setup

When you first launch DevBackup:
1. You'll see "!" in the menu bar
2. Click it and select "Preferences..."
3. A setup wizard will guide you:
   - First, pick the folders you want to back up
   - Then, pick where to store your backups
4. Click OK to confirm, and you're done!

Your backups will now run automatically every hour.

## Getting Files Back

### "I deleted something by accident!"

Just tell Cursor what you need:
> "I accidentally deleted app.py, can you get it back?"

The AI will find it and ask:
> "I found app.py in your backups from 2 hours ago. Want me to restore it?"

Say yes, and your file is back!

### "I want an older version"

If you made changes you want to undo:
> "Undo my changes to config.json"

Or find a specific version:
> "Show me the version of main.py from yesterday"

### Where Restored Files Go

By default, restored files go to a "Recovered Files" folder on your Desktop. This way, you can check them before replacing your current files.

## Common Questions

### How often are backups made?

By default, every hour. You can change this if you want more or less frequent backups.

### How much space do backups use?

Less than you'd think! devbackup is smart - it only saves files that have actually changed. If a file hasn't changed since the last backup, it doesn't take up extra space.

### What if my backup drive isn't connected?

devbackup will wait patiently. When you reconnect your drive, it will automatically catch up on any missed backups.

### What if my laptop battery is low?

devbackup won't run backups when your battery is below 20% (unless you're plugged in). This helps preserve your battery life.

### What files are backed up?

Your project files - code, documents, configurations. devbackup automatically skips things that don't need backing up, like:
- Downloaded packages (node_modules, etc.)
- Build outputs
- Temporary files

### Can I back up to iCloud?

Yes! devbackup can find and use your iCloud Drive as a backup destination.

## Talking to devbackup

Here are some things you can say to Cursor:

| What to say | What happens |
|-------------|--------------|
| "Back up my projects" | Runs a backup (or sets up if first time) |
| "Are my files backed up?" | Shows your backup status |
| "Restore [filename]" | Gets a file back from backup |
| "Find [filename] from [time]" | Searches for a specific file |
| "Undo changes to [file]" | Restores the previous version |
| "When was my last backup?" | Shows the last backup time |
| "How much space are backups using?" | Shows storage info |

## Troubleshooting

### "Backup drive not found"

Your external drive might not be connected. Plug it in and try again.

### "Backup is taking a long time"

The first backup takes longer because it's copying everything. After that, backups are much faster because only changed files are copied.

### "I can't find my restored file"

Check the "Recovered Files" folder on your Desktop. That's where restored files go by default.

### Need More Help?

Click "Get Help" in the menu bar app, or ask Cursor:
> "Help me with my backups"

## Tips for Best Results

1. **Use an external drive** - Keeps your backups safe even if something happens to your computer
2. **Keep your backup drive connected** - Or at least connect it regularly so backups can run
3. **Don't worry about it** - Once set up, devbackup works automatically in the background

---

*devbackup - Your work, always safe.*
