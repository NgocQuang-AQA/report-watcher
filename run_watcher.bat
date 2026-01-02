@echo off
set CONFIG_FILE=config.windows.json
cd /d "%~dp0"
start "" /B .\.venv\Scripts\pythonw.exe watcher.py
exit