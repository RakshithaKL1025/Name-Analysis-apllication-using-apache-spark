@echo off
REM Batch file to start the Name Letter Counter web application

echo Starting Name Letter Counter Web Application...
echo =============================================

REM Check if required packages are installed
echo Checking for required packages...
python -c "import flask" >nul 2>&1
if %errorlevel% neq 0 (
    echo Installing Flask...
    pip install flask
)

python -c "import pyspark" >nul 2>&1
if %errorlevel% neq 0 (
    echo Installing PySpark...
    pip install pyspark
)

echo Starting web server...
echo The application will be available at http://localhost:5000
echo Press Ctrl+C to stop the server

python web_app.py

pause