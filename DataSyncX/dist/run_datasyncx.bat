@echo off
cd /d "%~dp0"
echo Starting DataSyncX at %date% %time% >> logs\scheduler.log
"new_sync.exe" >> logs\scheduler.log 2>&1
echo Finished at %date% %time% >> logs\scheduler.log
echo -------------------------------------------- >> logs\scheduler.log