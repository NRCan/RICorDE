:: script to activate project pyqgis environment 
:: not to be confused with %ACTIVATE_VENV_BAT%, the virtual environment activation script

echo off
:: set the caller paths

call %~dp0../env/settings.bat

:: activate the system pyqgis environment
call "%PYQGIS_ENV_BAT%"
ECHO python environment activated from %PYQGIS_ENV_BAT%

:: activate virtual environment
call "%ACTIVATE_VENV_BAT%"

:: Set python paths
SET PYTHONPATH=%PYTHONPATH%;%SRC_DIR%

 