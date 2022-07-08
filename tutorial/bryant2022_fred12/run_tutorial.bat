REM =============
REM VARIABLES
REM =============
set PARAM_FP=C:\LS\09_REPOS\03_TOOLS\RICorDE\tutorial\bryant2022_fred12\RICorDE_bryant2022.ini
set ROOT_DIR=C:\LS\10_IO\ricorde\bryant2022\

REM =============
REM SETUP
REM =============
REM Activate pyqgis environment and 
call "C:\Users\cefect\.venv\QGIS 3.22.8\ricorde\python-qgis-ltr_3228_activate.bat"

REM add the project to the python path
set PYTHONPATH=C:\LS\09_REPOS\03_TOOLS\RICorDE;%PYTHONPATH%

REM set the path to RICorDE's main caller
set MAIN=C:\LS\09_REPOS\03_TOOLS\RICorDE\main.py

REM =============
REM EXECUTE 
REM =============
ECHO Executing RICorDE
python %MAIN% -rd %ROOT_DIR% %PARAM_FP% -t r1 -name tutorial1

pause