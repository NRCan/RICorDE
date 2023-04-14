# Contributing Guidelines

First off, thanks for considering to contribute to this project!

These are mostly guidelines, not rules. Use your best judgment, and feel free to propose changes to this document in a pull request.


## Code Style

Make sure your code *roughly* follows [PEP-8](https://www.python.org/dev/peps/pep-0008/) and keeps things consistent with the rest of the code:

- docstrings: [pandas-style](https://pandas.pydata.org/docs/development/contributing_docstring.html) is used to write technical documentation.
- formatting: [black](https://black.readthedocs.io/) is used to automatically format the code without debate.
- sorted imports: [isort](https://pycqa.github.io/isort/) is used to sort imports
- static analisis: [flake8](https://flake8.pycqa.org/en/latest/) is used to catch some dizziness and keep the source code healthy.


## Development environment

We usually develop to target the QGIS LTR. The program itself does not (normally) require any additional dependencies. 
However, development requires some (e.g., pytest_qgis for testing). 

### Building dev environment

To isolate this development environment from your main pyqgis build,  it's best to use a virtual environment.. which can be tricky to set up.
The batch script `./dev/pyqgis_venv_build.bat` has been provided to do this on Windows which requires the following steps: 
    1) create a batch script to initialize your system's pyqgis environment (if you haven't already done so). 
    2) create a `./env/settings.bat` to set your environment variables (see example below)
    3) call `./dev/pyqgis_venv_build.bat`, changing the value to 'true' when prompted. this should create a python virtual environment in `./env/` and install the additional dependencies. 
    
### Activating dev environment
The batch script `./env/activate_py.bat` should activate the development environment (if the above is configured correctly). 
This is useful for running tests from command line. 
Note the amendments to PYTHONPATH

### Testing the environment
A simple way to test if the dependencies are installed is to import them within python:
```
python
>>> import qgis.core
>>> import pytest
>>> import pytest_qgis
```
if you encounter any errors, your environment is not set up correctly.

### Example settings.bat (for Windows)
```
:: development environment variables and batch scripts

:: project name
set PROJ_NAME=ricorde

:: system pyqgis environment config file (should call c:\OSGeo4W\bin\o4w_env.bat at a minimum)
set PYQGIS_ENV_BAT=L:\09_REPOS\01_COMMON\Qall\bin\setup_pyqgis_ltr.bat

:: set the target directory for the environment
SET VDIR=%~dp0\%PROJ_NAME%

:: requirements file
SET REQ_FP=%~dp0requirements.txt

:: set the venv activation script
SET ACTIVATE_VENV_BAT=%VDIR%\Scripts\activate.bat

:: set the python project activate script
SET ACTIVATE_BAT=%~dp0\activate_py.bat

:: project source
SET SRC_DIR=%~dp0.. 

ECHO project %PROJ_NAME% settings set
```
## Tests
see `./tests2/CONTRIBUTING.md`
