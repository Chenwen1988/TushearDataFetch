@echo off
:taskkill /f /fi "USERNAME eq pc" /im "Platinum.Crontab.exe"
echo %DATE:~0,4%-%DATE:~5,2%-%DATE:~8,2% %TIME:~0,2%:%TIME:~3,2%:%TIME:~6,2% - Restart ScheduleJobs ...  >> server.log

echo %cd% ... >> server.log
cd ../..

call C:\Users\pc\anaconda3\Scripts\activate.bat

start python RetrieveStockData.py