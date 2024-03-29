# RICorDE
RICorDE produces gridded water depth estimates from flood inundation data by incorporating a HAND sub-model and cost distancing algorithms to extrapolate edge values into the inundated region. 

![img](/img/cover.png)

RICorDE is an open source software originally released under the Government of Canada’s Open Government License. RICorDE was co-developed by Bryant, McGrath and Boudreault. Full description of the methodology can be found here: Bryant, S., McGrath, H., and Boudreault, M.: Gridded flood depth estimates from satellite-derived inundations, Nat. Hazards Earth Syst. Sci., 22, 1437–1450, https://doi.org/10.5194/nhess-22-1437-2022, 2022.

To read more about the algorithm, and its performance and applications, see [Bryant 2022](https://nhess.copernicus.org/articles/22/1437/2022/nhess-22-1437-2022.html).

To see what's new, check out the [CHANGELOG.md](CHANGELOG.md)

## Installation
tldr: clone, create your own definitions.py file, run from pyqgis command line

### Prerequisites
RICorDE is a standalone application built on open source QGIS python bindings and WhiteboxTools.  Before using, ensure the following are installed:

- [QGIS 3.22.8](https://download.qgis.org/downloads/) 
- [WhiteboxTools Open Core v2.1.0](https://www.whiteboxgeo.com/download-whiteboxtools/). (this should install a _whitebox_tools.exe_ file you'll reference in ./definitions.py) 

RICorDE must be run within a working pyqgis environment. To test if your environment is working, try the following:
```
~python
>>>import qgis.core
>>>import processing
```
If any of these result in an error, your environment is not set up correctly. 

The tutorials assume your pyqgis environment is activated by calling `./dev/activate_py.bat` within windows cmd. This is not strictly necessary, but makes scripting model runs much easier. See `./CONTRIBUTING.md` for some advice on setting up such a batch script and a virtual pyqgis environment. 

### Setup
1. clone this repo `git clone https://github.com/NRCan/RICorDE.git`
2. create a `./definitions.py` file as shown below modified to your system
3. add the source folder to your PYTHONPATH (see `./dev/activate_py.bat`)

Now you're ready to activate your python environment and use the Command Line Interface (see below)!

see `./tests/README.md` for testing

example `./definitions.py`:
```
import os
#location of whitebox executable
"""
Change this to match your whitebox exe path
"""
whitebox_exe_d = {'v2.1.0':r'C:\LS\06_SOFT\whitebox\v2.1.0\whitebox_tools.exe'}

#maximum processors to use
max_procs = 4 

#location of source code
proj_dir = os.path.dirname(os.path.abspath(__file__))

#path to python logging config file
logcfg_file=os.path.join(proj_dir, 'logger.conf')

#root directory for building default directories in
root_dir=r'C:\LS\10_IO\ricorde'
```

## Use
RICorDE routines can be run in sequence using the command-line parsing in [main.py](main.py) or by calling the session methods in [ricorde/scripts.py](ricorde/scripts.py) directly in custom python scripts.

### Command Line Interface (CLI)
An end-to-end run of the RICorDE algorithm is provided through the CLI by specifying a parameter.ini file:

`~python main.py path/to/parameter.ini`

Additional arguments can be passed to control RICorDE's file behaviour and some defaults. Information on these controls can be obtained through the help command:
```
~python main.py -h


usage: RICorDE [-h] [-exit_summary] [-compress {hiT,hi,med,none}]
               [-root_dir ROOT_DIR] [-out_dir OUT_DIR] [-temp_dir TEMP_DIR]
               [-tag TAG] [-write] [-prec PREC] [-overwrite]
               [-relative]
               param_fp

Compute a depths grid from flood inundation and DEM

positional arguments:
  param_fp              filepath to parameter .txt file (see documentation for
                        format)

optional arguments:
  -h, --help            show this help message and exit
  -compress {hiT,hi,med,none}, -c {hiT,hi,med,none}
                        set the default raster compression level
  -root_dir ROOT_DIR, -rd ROOT_DIR
                        Base directory of the project. Used for generating
                        default directories. Defaults to value in definitions
  -tag TAG, -t TAG      tag for the run

```


#### Parameter.ini file

A RICorDE parameter file is a [pythonic ini file](https://docs.python.org/3/library/configparser.html#supported-ini-file-structure) where the input data and algorithm parameters are specified. The parameter file expects 20 sections, each of which corresponds to an intermediate or end result file, except for the [session] section where the primary inputs are specified. Here's the first 10 rows from [RICorDE_params_default.ini](RICorDE_params_default.ini) 
```
[session]
name=project1       #project name
aoi_fp =            #optional area of interest polygon filepath
dem_fp=             #dem rlay filepath
pwb_fp=             #permanent water body filepath (raster or polygon)
inun_fp=            #inundation filepath (raster or polygon)
crsid=EPSG:4326     #CRSID

[dem]
#resolution =10             #optional resolution for resampling the DEM
```
Characters following '#' are ignored. 

To prepare a RICorDE run, first copy the provided [RICorDE_params_default.ini](RICorDE_params_default.ini) into your working directory, then edit as needed, before saving and executing using main.py (see above).

#### Typical CLI use

Once your parameter.ini file is prepared and you've decided on your run arguments, prepare your python call as shown above. For example:

`python main.py -rd path/to/my/work -t r0 path/to/parameter.ini`

then execute this in your pyqgis environment. On Windows, this is typically accomplished via a batch script which performs the environment setup then makes the RICorDE call. An example of such a batch script is provided in the [tutorials folder](tutorial\bryant2022_fred12\run_tutorial.bat) (be sure to edit this with your own paths). Once you're confident the run is configured correctly, python's '-O' flag can be passed to remove some checks. 

### Custom Scripting

For more flexibility, RICorDE methods can be called in custom python scripts by referencing the session methods in [ricorde/scripts.py](ricorde/scripts.py) directly. The function [_run_from_params_](ricorde/runrs.py) provides a nice example (this is the default behaviour of the CLI call) and calls the following hi-level function sequence: 
```
run_dataPrep() #Clean and load inputs into memory.
run_HAND() #Build the HAND raster from the DEM using whiteboxtools
run_imax() #Perform the Phase 1: Inundation Correction
run_HANDgrid() #Perform PHASE2: Compute Rolling HAND grid
run_wslRoll() #Perform PHASE3: Rolling WSL grid
run_depths() #PHASE4: Resultant Depths computation
```
When developing your custom script, parameters from the parameter.ini file should be passed to the session as a dictionary in the _bk_lib_ key word argument (these can be loaded from the _load_params_ function if you'd like to still use the parameter.ini file). 

A typical end-to-end RICorDE run generates ~20 intermediate result data files. This architecture is useful for debugging, resuming a partial run, or for quickly replacing the results of some internal sub-routine (e.g., supplying your own HAND layer rather than using the one computed by RICorDE/WBT). Filepaths to an alternate intermediate (pre-compiled) result data file can be passed to a 'Session' object (e.g., via 'run_from_params') with the 'compiled_fp_d' key. For example:
```
    run_from_params(
        param_fp='path_to_parameters',

        #use this hand raster file instead
        compiled_fp_d = {'hand_fp':'path_to_layer'},
        )
```
This will tell RICorDE to use the raster specified rather than call the building function (`build_hand` in this case). Most the building and (pre-compiled) loading functions are specified [here](https://github.com/cefect/RICorDE/blob/8579036e0747c4d97c391968772832f73d564741/ricorde/scripts.py#L66). Using this feature to supply an intermediate data file computed outside of RICorDE is not recommended as this may cause downstream issues in the computation pipeline. 

## Tutorial

A pre-configured run of the Fredericton 2018 flood is provided in the [tutorials](tutorials\bryant2022_fred12) folder. See [Bryant 2022](https://nhess.copernicus.org/articles/22/1437/2022/nhess-22-1437-2022.html) for data sources. Edit the batch file to reflect your own paths. A short video showing the execution of this tutorial can be found [here](https://youtu.be/BzAeMpCo23c).

## Logging

For each run, RICorDE creates four log files [in a directory]:

- __root.log__: This is the debug log where all messages from all runs are stored [root_dir]. 
- __rootwarn.log__: Same as root.log, but only warnings and errors are stored [root_dir].
- __Qproj.log__: Same as root.log, but only QGIS feedback messages are stored [root_dir].
- __tag_datetime.log__: This log stores all messages for an individual run and is used to document the results [out_dir]


Good luck! and thank you for  your interest in the project. We'd love to hear from you, so please post an issue with suggestions or questions.
