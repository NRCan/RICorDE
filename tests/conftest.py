'''
Created on Feb. 21, 2022

@author: cefect

copied from 2112_Agg
'''
import os, shutil
import pytest
import numpy as np
from numpy.testing import assert_equal
import pandas as pd
from pandas.testing import assert_frame_equal, assert_series_equal, assert_index_equal
idx = pd.IndexSlice

from qgis.core import QgsCoordinateReferenceSystem, QgsVectorLayer, QgsWkbTypes, QgsRasterLayer
import processing
from ricorde.scripts import Session
from hp.Q import vlay_get_fdf
from hp.gdal import getRasterStatistics, rlay_to_array, getRasterMetadata
    
rproj_lib = {
        'fred01':{ #raster inputs (a bit faster)
            'aoi_fp':'aoi01T_fred_20220325.geojson',
            'dem_fp':'test_tag_0326_dem.tif', #pre-cleaned to 2x2
            'inun_fp':'test_tag_0326_inun_rlay.tif',
            'pwb_fp':'test_tag_0326_pwb_rlay.tif',     
            'crsid':'EPSG:3979', 
       
                
                },
        'fred02':{ #vector layers
            'aoi_fp':'aoi01T_fred_20220325.geojson',
            'dem_fp':'dem_fred_aoi01T_2x2_0325.tif',
            'inun_fp':'inun_fred_aoi01T_0325.geojson',
            'pwb_fp':'pwater_fred_aoi01T_0325.geojson',     
            'crsid':'EPSG:3979', 
    
                
                },
        }

@pytest.fixture(scope='session')
def write():
    #===========================================================================
    # write key
    #===========================================================================
    write=False
    #===========================================================================
    # write key
    #===========================================================================
    
    if write:
        print('WARNING!!! runnig in write mode')
    return write

#===============================================================================
# function.fixtures-------
#===============================================================================
@pytest.fixture(scope='function')
def proj_d(request): #retrieve test dataset
    
    return get_proj_d(request.param)

def get_proj_d(name):
    base_dir = r'C:\LS\09_REPOS\03_TOOLS\RICorDE\tests\data'
    rproj_d = rproj_lib[name].copy()
    rproj_d['name'] = name
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
def test_name(request):
    return request.node.name.replace('[','_').replace(']', '_')
 
 


@pytest.fixture(scope='function')
#@pytest.mark.parametrize('proj_d',['fred01'], indirect=False) 
def session(tmp_path,
            root_dir, 
            proj_d, #scope=function
            base_dir, 
            write,logger, feedback,# (scope=session)
            test_name,
 
                    ):
 
    np.random.seed(100)
    
    #configure output
    out_dir=tmp_path
    if write:
        #retrieves a directory specific to the test... useful for writing compiled true data
        """this is dying after the yield statement for some reason...."""
        out_dir = os.path.join(base_dir, os.path.basename(tmp_path))
        #out_dir = os.path.join(r'C:\LS\09_REPOS\03_TOOLS\RICorDE\tests\data\compiled', test_name)
        #out_dir = r'C:\LS\10_OUT\ricorde\tests\try1' 
        
    
    
    with Session( 
                 name='test', #probably a better way to propagate through this key
                 #fp_d={k:v for k,v in proj_d.items() if k in ['dem_fp', 'fic_fp', 'nhn_fp']}, 
                 
                 #aoi_fp=proj_d['aoi_fp'],
                 
                 
                 
                 out_dir=out_dir, 
                 temp_dir=os.path.join(tmp_path, 'temp'),
                 root_dir=root_dir, #testing default is the same as the session default for now
                 
                 compress='none',  
                 crs=QgsCoordinateReferenceSystem(proj_d['crsid']),
                 
                 logger=logger, feedback=feedback,
                 
                   overwrite=True,
                   write=write, #avoid writing prep layers
                   exit_summary=False,
                   compiled_fp_d=dict(), #I guess my tests are writing to the class... no tthe instance
                   **{k:v for k,v in proj_d.items() if k in ['dem_fp', 'inun_fp', 'pwb_fp', 'aoi_fp']}, #extract from the proj_d
                   ) as ses:
        
        assert len(ses.data_d)==0
        assert len(ses.compiled_fp_d)==0
        yield ses
 
 
#===============================================================================
# session.fixtures----------
#===============================================================================
@pytest.fixture(scope='session')
def root_dir():
    from definitions import root_dir
    return root_dir



@pytest.fixture(scope='session')
def logger(root_dir):

    os.chdir(root_dir) #set this to the working directory
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
            os.makedirs(os.path.join(true_dir, 'working')) #and the working folder
            
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


def retrieve_data(dkey, fp, ses): #load some compiled result off the session (using the dkey)
    assert dkey in ses.data_retrieve_hndls
    hndl_d = ses.data_retrieve_hndls[dkey]
    assert 'compiled' in hndl_d, '%s has no compliled handles'%dkey
    
    return hndl_d['compiled'](fp=fp, dkey=dkey)

def compare_layers(lay_test, lay_true, #two containers of layers
                   test_data=True, #check vlay attributes
                   test_spatial=True,
                   ignore_fid=True,  #whether to ignore the native ordering of the vlay
                   wrkr=None,
                   ):
    
 
        
    dptest, dptrue = lay_test.dataProvider(), lay_true.dataProvider()
    
    assert type(lay_test)==type(lay_true)
    
    #=======================================================================
    # vectorlayer checks
    #=======================================================================
    if isinstance(lay_test, QgsVectorLayer):
        assert dptest.featureCount()==dptrue.featureCount()
        assert lay_test.wkbType() == dptrue.wkbType()
        
        #data checks
        if test_data:
            true_df, test_df = vlay_get_fdf(lay_true), vlay_get_fdf(lay_test)
            
            if ignore_fid:
                true_df = true_df.sort_values(true_df.columns[0],  ignore_index=True) #sort by first column and reset index
                test_df = test_df.sort_values(test_df.columns[0],  ignore_index=True)
            
            
            assert_frame_equal(true_df, test_df,check_names=False)
            
    elif isinstance(lay_test, QgsRasterLayer):
        
        #compare spatial meta
        if not wrkr is None and test_spatial:
            matching, msg = wrkr.rlay_check_match(lay_test, lay_true)
            if not matching:
                raise AssertionError('test layer (%s) spatial meta does not match \n    %s'%(lay_test.name(), msg))
        
        #compare stats
        testStats_d = rasterstats(lay_test) #getRasterStatistics(lay_test.source())
        trueStats_d = rasterstats(lay_true) # getRasterStatistics(lay_true.source())
        
        compare_dicts(testStats_d, trueStats_d, index_l=['MAX', 'MEAN', 'MIN', 'RANGE', 'SUM'])
 
        
        """
        rasterstats(lay_test)
        """
        #getRasterMetadata(lay_test.source())
        
        if test_data:
            ar_test = rlay_to_array(lay_test.source())
            ar_true = rlay_to_array(lay_true.source())
            
            assert_equal(ar_test, ar_true)
            
def compare_dicts(dtest, dtrue, index_l = None,
                 ):
        df1 = pd.DataFrame.from_dict({'true':dtrue, 'test':dtest})
        
        if index_l is None:
            index_l = df1.index.tolist()
        
        df2 = df1.loc[index_l,: ].round(3)
        
        bx = ~df2['test'].eq(other=df2['true'], axis=0)
        if bx.any():
            raise AssertionError('%i/%i raster stats failed to match\n%s'%(bx.sum(), len(bx), df2.loc[bx,:]))
    
            
def rasterstats(rlay): 
      
    ins_d = { 'BAND' : 1, 
             'INPUT' : rlay,
              'OUTPUT_HTML_FILE' : 'TEMPORARY_OUTPUT' }
 
    return processing.run('native:rasterlayerstatistics', ins_d )   
            
            
            
            
            
            
            
            
