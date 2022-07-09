# RICorDE
RICorDE produces gridded water depth estimates by incorporating a HAND sub-model and cost distancing algorithms to extrapolate edge values into the inundated region. 

![img](/img/cover.png)

To read more about the algorithm, its performance and applications, see [Bryant 2022](https://nhess.copernicus.org/articles/22/1437/2022/nhess-22-1437-2022.html).

## Installation
RICorDE is a standalone application built on QGIS python bindings.  Before using, ensure the following are installed:

- [QGIS 3.22.8](https://download.qgis.org/downloads/) (see the [requirements.txt](requirements.txt) for dependencies usually shipped with QGIS)
- [Whitebox.exe](https://github.com/jblindsay/whitebox-tools) (v1.4.0 and v2.0.0) (specify the exe locations in definitions.py)

RICorDE must be run within a working pyqgis development environment. To test if your environment is working, try the following:

    ~python
    >>>import qgis.core
    >>>import processing

If any of these result in an error, your environment is not set up correctly. 

## Use
RICorDE routines can be run in sequence using the command-line parsing in [main.py](main.py) or by calling the session methods in [ricorde/scripts.py](ricorde/scripts.py) directly in custom python scripts.

### Command Line Interface (CLI)
An end-to-end run of the RICorDE algorithm is provided through the CLI by passing a parameter.ini file.

`~python main.py path/to/parameter.ini`

Additional arguments can be passed to control RICorDE's file behaviour and some defaults. Information on these controls can be obtained through the help command:

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
      -exit_summary, -exs   flag to disable writing the exit summary
      -compress {hiT,hi,med,none}, -c {hiT,hi,med,none}
                            set the default raster compression level
      -root_dir ROOT_DIR, -rd ROOT_DIR
                            Base directory of the project. Used for generating
                            default directories. Defaults to value in definitions
      -out_dir OUT_DIR, -od OUT_DIR
                            Directory used for outputs. Defaults to a sub-
                            directory within root_dir
      -temp_dir TEMP_DIR    Directory for temporary outputs (i.e., cache).
                            Defaults to a sub-directory of out_dir.
      -tag TAG, -t TAG      tag for the run
      -write, -w            flag to disable output writing
 
      -prec PREC            Default float precision
      -overwrite            Disable overwriting files as the default behaviour when
                            attempting to overwrite a file
      -relative             Default behaviour of filepaths (relative vs. absolute)
 


#### Parameter.ini file

The parameter file is a [pythonic ini files](https://docs.python.org/3/library/configparser.html#supported-ini-file-structure) where the input data and algorithm parameters are specified. The parameter file expects 20 sections, each of which corresponds to an intermediate or end result file, except for the [session] section where the primary inputs are specified. Here's the first 10 rows from [RICorDE_params_default.ini](RICorDE_params_default.ini) 

    [session]
    name=project1       #project name
    aoi_fp =            #optional area of interest polygon filepath
    dem_fp=             #dem rlay filepath
    pwb_fp=             #permanent water body filepath (raster or polygon)
    inun_fp=            #inundation filepath (raster or polygon)
    crsid=EPSG:4326     #CRSID

    [dem]
    #resolution =10             #optional resolution for resampling the DEM

Characters following '#' are ignored. 

To prepare a RICorDE run, first copy the provided [RICorDE_params_default.ini](RICorDE_params_default.ini) into your working directory, then edit as needed, before saving and executing using main.py (see above).

#### Typical CLI use

Once your parameter.ini file is prepared and you've decided on your run arguments, prepare your python call as shown above:

`python RICorDE/main.py -rd path/to/my/work -t r0 path/to/parameter.init`

then execute this in your pyqgis environment. On Windows, this is typically accomplished via a batch script which performs the environment setup then makes the RICorDE call. An example of such a batch script is provided in the [tutorial folder](tutorial\bryant2022_fred12\run_tutorial.bat).

### Custom Scripting

For more flexibility, RICorDE methods can be called in custom scripts by referencing the session methods in [ricorde/scripts.py](ricorde/scripts.py). The function [_run_from_params_](ricorde/runrs.py) provides a nice example (this is the default behaviour of the CLI call) and calls the following hi-level function sequence: 

    run_dataPrep() #Clean and load inputs into memory.
    run_HAND() #Build the HAND raster from the DEM using whiteboxtools
    run_imax() #Perform the Phase 1: Inundation Correction
    run_HANDgrid() #Perform PHASE2: Compute Rolling HAND grid
    run_wslRoll() #Perform PHASE3: Rolling WSL grid
    run_depths() #PHASE4: Resultant Depths computation
    
## Tutorial

A pre-configured run of the Fredericton 2018 flood is provided in the [tutorials](tutorial\bryant2022_fred12) folder. See [Bryant 2022](https://nhess.copernicus.org/articles/22/1437/2022/nhess-22-1437-2022.html) for data sources.