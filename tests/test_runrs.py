'''Unit tests on runner functions'''

from main import parse_args, run_from_args, get_dict_str
from ricorde.runrs import load_params, run_from_params, get_parser
import pytest, copy, os, configparser


#===============================================================================
# fixtures
#===============================================================================
@pytest.fixture(scope='function')
def param_fp(proj_dir, proj_d, tmp_path):
    
    #check the proj_d
    assert 'pwb_fp' in proj_d
    
    sectName='session'
    fileName = 'test_params.ini'
    param_fp = os.path.join(proj_dir, 'tests', fileName) #just using one file now
    parser=get_parser(param_fp)
    
    parser.remove_section(sectName)
    parser.add_section(sectName)
    
    for k,v in proj_d.items():
        parser[sectName][k]=v
        
    #write
    ofp = os.path.join(tmp_path, fileName)
    with open(ofp, 'w') as file:
        parser.write(file)
    return ofp


@pytest.fixture(scope='function')                  
def args(param_fp, exit_summary, compress):
 
    #===========================================================================
    # #positional
    #===========================================================================
    args = [param_fp]
 
    #===========================================================================
    # kwargs
    #===========================================================================
    for param, (flag, val) in {
        exit_summary:('-exs', None),
        compress:('-compress', compress),
        #root_dir:('-root_dir', root_dir),
        }.items():
        if not param is None:
            args.append(flag)
            
            if not val is None:
                args.append(val)
                
    #===========================================================================
    # #special params for testing
    #===========================================================================
 
    return args
 
#===============================================================================
# unit tests
#===============================================================================


@pytest.mark.parametrize('proj_d',['fred01'], indirect=True) #using the faster setup files
#@pytest.mark.parametrize('param_fp', ['RICorDE_params_default.ini'], indirect=True)
def test_load_params(param_fp):
    result = load_params(param_fp)
    
    assert isinstance(result, dict)
    assert 'session' in result
    

#@pytest.mark.parametrize('param_fp', ['RICorDE_params_default.ini'], indirect=True)
@pytest.mark.parametrize('proj_d',['fred01'], indirect=True) #using the faster setup files
@pytest.mark.parametrize('exit_summary',[None, True]) 
@pytest.mark.parametrize('compress',[None, 'hiT', 'hi', 'med', 'none'])
#@pytest.mark.parametrize('root_dir',[None, os.getcwd()])                   
def test_parse_args(args):
    """test lots of parameter combinations...gets pretty redundant"""
    parsed_kwargs = parse_args(args)
    assert isinstance(parsed_kwargs, dict)


#===============================================================================
# integration tests
#===============================================================================
@pytest.mark.dev 
#@pytest.mark.parametrize('param_fp', ['RICorDE_params_default.ini'], indirect=True)
@pytest.mark.parametrize('proj_d',['fred01', 'fred02', 'fred03'], indirect=True) #using the faster setup files
def test_run_from_params(param_fp, 
                         tmp_path, qgis_app, qgis_processing, logger, feedback,
                         write):
    """see test_run_parsed for slightly more comprehensive test"""
    
    #load the parameters
    param_lib = load_params(param_fp)
 
    #execute
    run_from_params(param_lib=param_lib,tag='test', root_dir=tmp_path,
                    qgis_app=qgis_app,qgis_processing=qgis_processing, 
                    logger=logger, feedback=feedback, 
                    compress='none', temp_dir=os.path.join(tmp_path, 'temp'),
                    exit_summary=False, write=True, overwrite=True) 
    


 
    

#@pytest.mark.parametrize('param_fp', ['RICorDE_params_default.ini'], indirect=True)
@pytest.mark.parametrize('proj_d',['fred01'], indirect=True) #using the faster setup files
@pytest.mark.parametrize('exit_summary',[True]) 
@pytest.mark.parametrize('compress',['none'])                
def test_run_parsed(args,  
                    tmp_path,qgis_app, qgis_processing, logger, feedback):
    """full integration test on different arg combinations"""
    
    #===========================================================================
    # add special args via parser
    #===========================================================================
    args = args + [
        '-root_dir', str(tmp_path), '-w', '-t', 'test', '-overwrite'
        ]
    
    result = run_from_args(args, 
                  qgis_app=qgis_app,qgis_processing=qgis_processing, 
                    logger=logger, feedback=feedback, 
                    )
