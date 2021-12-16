@echo off
taskkill /f /fi "USERNAME eq pc" /im "Platinum.Crontab.exe"
echo %DATE:~0,4%-%DATE:~5,2%-%DATE:~8,2% %TIME:~0,2%:%TIME:~3,2%:%TIME:~6,2% - Restart Crontab ...  >> server.log

cd %~dp0
cd ../Release
timeout /t 60
start "" Platinum.Crontab.exe