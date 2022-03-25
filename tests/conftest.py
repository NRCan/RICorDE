'''
Created on Feb. 21, 2022

@author: cefect

copied from 2112_Agg
'''
import os, shutil
import pytest
import numpy as np
from qgis.core import QgsCoordinateReferenceSystem
from ricorde.ses import Session
    
 
#===============================================================================
# function.fixtures-------
#===============================================================================
@pytest.fixture(scope='function')
def proj_d(request): #retrieve test dataset
    name = request.param
    #get data dir
    base_dir = r'C:\LS\09_REPOS\03_TOOLS\RICorDE\tests\data'
    
    #get filenames
    rproj_lib = {
        'fred01':{
            'aoi_fp':'aoi01T_fred_20220325.geojson',
            'dem_fp':'dem_fred_aoi01T_1x1_0325.tif',
            'fic_fp':'inun_fred_aoi01T_0325.geojson',
            'nhn_fp':'pwater_fred_aoi01T_0325.geojson',     
            'crsid':'EPSG:3979', 
            'name':name       
                
                },
    
        }
    
    rproj_d = rproj_lib[name].copy()
    
    proj_d = dict()
    
    for k,v in rproj_d.items():
        if k.endswith('_fp'):
            fp = os.path.join(base_dir,name, v)
            assert os.path.exists(fp), 'got bad fp on \'%s.%s\'\n    %s'%(name, k, fp)
            proj_d[k] = fp
        else:
            proj_d[k]=v
    
    
    
    return proj_d
    
 


@pytest.fixture(scope='function')
#@pytest.mark.parametrize('proj_d',['fred01'], indirect=False) 
def session(tmp_path,
            #wrk_base_dir=None, 
            proj_d, #scope=function
            base_dir, write,logger, feedback,# (scope=session)
 
                    ):
 
    np.random.seed(100)
    
    #configure output
    out_dir=tmp_path
    if write:
        #retrieves a directory specific to the test... useful for writing compiled true data
        out_dir = os.path.join(base_dir, os.path.basename(tmp_path)) 
        
    
    
    with Session(aoi_fp=proj_d['aoi_fp'], 
                 name='test', #probably a better way to propagate through this key
                 fp_d={k:v for k,v in proj_d.items() if k in ['dem_fp', 'fic_fp', 'nhn_fp']}, 
                 crs=QgsCoordinateReferenceSystem(proj_d['crsid']),
                 out_dir=out_dir, temp_dir=os.path.join(tmp_path, 'temp'),
                 compress='none',  
                 logger=logger, feedback=feedback,
                 #work_dir=work_dir, use the default
                   overwrite=True) as ses:
        
        #assert len(ses.data_d)==0
        yield ses
 
#===============================================================================
# session.fixtures----------
#===============================================================================
@pytest.fixture(scope='session')
def write():
    write=True
    if write:
        print('WARNING!!! runnig in write mode')
    return write

@pytest.fixture(scope='session')
def logger():
    out_dir = r'C:\LS\10_OUT\2112_Agg\outs\tests'
    if not os.path.exists(out_dir): os.makedirs(out_dir)
    os.chdir(out_dir) #set this to the working directory
    print('working directory set to \"%s\''%os.getcwd())

    from hp.logr import BuildLogr
    lwrkr = BuildLogr()
    return lwrkr.logger

@pytest.fixture(scope='session')
def feedback(logger):
    from hp.Q import MyFeedBackQ
    return MyFeedBackQ(logger=logger)
 

@pytest.fixture(scope='session')
def base_dir():
    
    #'C:\\LS\\09_REPOS\\03_TOOLS\\RICorDE\\tests\\data\\compiled'
    base_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data', 'compiled')
 
    assert os.path.exists(base_dir)
    return base_dir



@pytest.fixture
def true_dir(write, tmp_path, base_dir):
    true_dir = os.path.join(base_dir, os.path.basename(tmp_path))
    if write:
        if os.path.exists(true_dir): 
            shutil.rmtree(true_dir)
            os.makedirs(true_dir) #add back an empty folder
            
    return true_dir
    
#===============================================================================
# helper funcs-------
#===============================================================================
def search_fp(dirpath, ext, pattern): #get a matching file with extension and beginning
    assert os.path.exists(dirpath), 'searchpath does not exist: %s'%dirpath
    fns = [e for e in os.listdir(dirpath) if e.endswith(ext)]
    
    result= None
    for fn in fns:
        if pattern in fn:
            result = os.path.join(dirpath, fn)
            break
        
    if result is None:
        raise IOError('failed to find a match for \'%s\' in %s'%(pattern, dirpath))
    
    assert os.path.exists(result), result
        
        
    return result
