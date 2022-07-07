'''Unit tests on runner functions'''
import pytest, copy, os, configparser

from ricorde.runrs import load_params, run_from_params
from main import parse_args

#===============================================================================
# fixtures
#===============================================================================
@pytest.fixture(scope='function')
def param_fp(proj_dir, proj_d, request, tmp_path):
    sectName='session'
    fileName = request.param
    param_fp = os.path.join(proj_dir, fileName)
    assert os.path.exists(param_fp)
    
    #add the project info
    parser=configparser.ConfigParser(inline_comment_prefixes='#')
    parser.read(param_fp)
    
    
    parser.remove_section(sectName)
    parser.add_section(sectName)
    
    for k,v in proj_d.items():
        parser[sectName][k]=v
        
    #write
    ofp = os.path.join(tmp_path, fileName)
    with open(ofp, 'w') as file:
        parser.write(file)
    return ofp

#===============================================================================
# tests
#===============================================================================

@pytest.mark.parametrize('proj_d',['fred01'], indirect=True) #using the faster setup files
@pytest.mark.parametrize('param_fp', ['params_default.txt'], indirect=True)
def test_load_params(param_fp):
    load_params(param_fp)
    
@pytest.mark.dev 
@pytest.mark.parametrize('param_fp', ['params_default.txt'], indirect=True)
@pytest.mark.parametrize('proj_d',['fred01'], indirect=True) #using the faster setup files
def test_run_from_params(param_fp, tmp_path, 
                         qgis_app, qgis_processing, logger, feedback,
                         write):
    
    #load the parameters
    param_lib = load_params(param_fp)
 
    #execute
    run_from_params(param_lib=param_lib,tag='test', root_dir=tmp_path,
                    qgis_app=qgis_app,qgis_processing=qgis_processing, 
                    logger=logger, feedback=feedback, 
                    compress='none', temp_dir=os.path.join(tmp_path, 'temp'),
                    exit_summary=False, write=True, overwrite=True) 
    
def test_parse_args():
    parse_args([])
    
 
