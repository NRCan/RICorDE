'''Unit tests on runner functions'''
import pytest, copy, os

from ricorde.runrs import load_params, run_from_params

#===============================================================================
# fixtures
#===============================================================================
@pytest.fixture(scope='function')
def param_fp(proj_dir, request):
    param_fp = os.path.join(proj_dir, request.param)
    assert os.path.exists(param_fp)
    return param_fp

#===============================================================================
# tests
#===============================================================================
@pytest.mark.dev 
@pytest.mark.parametrize('param_fp', ['params_default.txt'], indirect=True)
def test_load_params(param_fp):
    load_params(param_fp)
    

@pytest.mark.parametrize('param_fp', ['params_default.txt'], indirect=True)
@pytest.mark.parametrize('proj_d',['fred01'], indirect=True) #using the faster setup files
def test_run_from_params(proj_d, param_fp, tmp_path, 
                         qgis_app, qgis_processing, logger, feedback,
                         write):
    
    #load the parameters
    param_lib = load_params(param_fp)
    
    #overwrite paramter file with test values
    param_lib['session'] = proj_d#{k:v for k,v in proj_d.items() if k in param_lib['input_layers'].keys()}
    
    #execute
    run_from_params(param_lib=param_lib,tag='test', root_dir=tmp_path,
                    qgis_app=qgis_app,qgis_processing=qgis_processing, 
                    logger=logger, feedback=feedback, 
                    compress='none', temp_dir=os.path.join(tmp_path, 'temp'),
                    exit_summary=False, write=True, overwrite=True) 
    
 
