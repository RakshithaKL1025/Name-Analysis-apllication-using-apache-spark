@echo off
REM Batch file to run the name counter script with proper environment setup

REM Set environment variables for PySpark on Windows
set HADOOP_HOME=C:\hadoop
set JAVA_HOME="C:\Program Files\Java\jdk-11.0.2"

echo Running Name Counter Script...
echo ==============================

python simple_name_counter.py

echo ==============================
echo Script execution completed.
pause