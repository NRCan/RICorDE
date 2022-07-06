'''
Functions for running RICorDE workflows
'''
import os
import configparser
import pprint
from ricorde.scripts import Session, QgsCoordinateReferenceSystem
from definitions import proj_dir
from hp.basic import get_dict_str


def run_from_params(
                #parameter inputs and file
                param_fp=os.path.join(proj_dir, 'params_default.txt'),
                param_lib=None,
                
                **kwargs):
    """execute a RICorDE workflow from a parameter file
    
    Parameters
    ----------
    param_fp : str
        Filepath to parameter file. Defaults to the params_default.txt found in the project
    param_lib : dict, optional
        Override loading from file and use the passed parameters (mainly for testing)
        
    Returns
    ----------
    str
        Directory where outputs are written
    dict
        Keyed output filepaths
        
    Notes
    -----------
    parameters passed in the 'session' section will be passed as kwargs to the  __init__ cascade.
    Other sections will be passed in 'bk_lib'
    
    """
    #load from teh parameter file
    if param_lib is None:
        param_lib = load_params(param_fp)
    
    #extract special parameters
    session_kwargs = param_lib.pop('session')
    print('running w/ \n%s\n\n'%get_dict_str(session_kwargs))

    #initilze the calculation session using these parameters
    with Session(            
                  
            bk_lib = param_lib,            
            #session_kwargs
            #crsid=QgsCoordinateReferenceSystem(crsid),
            #aoi_fp=aoi_fp, dem_fp=dem_fp, inun_fp=inun_fp, pwb_fp=pwb_fp, #filepaths for this project
            **{**session_kwargs, **kwargs}
            ) as wrkr:
        wrkr.logger.info('\n\n start calc sequence\n\n')
        wrkr.run_dataPrep()
        wrkr.run_HAND()
        wrkr.run_imax()
        wrkr.run_HANDgrid()
        wrkr.run_wslRoll()
        wrkr.run_depths()
        
        outputs = wrkr.out_dir, wrkr.ofp_d
        
    return outputs


def load_params(param_fp,
                config_params=Session.config_params,
                ):
    """
    Load RICorDE run parameters from a config file
    
    Parametersd
    ----------
    param_fp : str
        Filepath to parameter file
    config_params : dict, default Session.config_params
        parameters for the configarser {sectionName:{varName:(mandatory_flag, ConfigParser get method)}}
        defaults to Session
 
 
    """
    
    #===========================================================================
    # init the parser
    #===========================================================================
    assert os.path.exists(param_fp), param_fp
    
    parser=configparser.ConfigParser(inline_comment_prefixes='#')
    parser.read(param_fp)
    
    #===========================================================================
    # check parameter file
    #===========================================================================
    for sectName, sect in parser.items():
        if sectName =='DEFAULT': 
            continue
        assert sectName in config_params, 'got unrecognized section name \'%s\''%sectName
        for varName, var in sect.items():
            assert varName in config_params[sectName], 'got unrecognized variable: \'%s.%s\''%(sectName, varName)
            print('%s.%s'%(sectName, varName))
    
    #===========================================================================
    # load usin the parameters
    #===========================================================================
    cnt = 0
    res_lib = {k:dict() for k in config_params.keys()}  #results congainer
    for sectName, d in config_params.items():
        assert sectName in parser.sections(), '%s missing from parameter file'%sectName
        for varName, (required, method) in d.items():
            #print('%s.%s: required=%s, method=%s'%(sectName, varName, required, method))
            
            #check if the parameter is in the file
            if varName in parser[sectName]:
                f = getattr(parser, method)
                res_lib[sectName][varName] = f(sectName, varName)
                cnt+=1
            else:
                assert not required, '%s.%s is required and not found in the parameter file'%(sectName, varName)
            
    print('retrieved %i parameters from file\n%s'%(cnt, get_dict_str(res_lib)))
    
    return res_lib
    
 
 



def runr(
        tag = 'r1',
        name = 'idai',
        crsid = 'EPSG:32737',
        aoi_fp= r'C:\LS\02_WORK\NRC\2202_TC\04_CALC\aoi\aoi04_0326.gpkg',
        dem_fp = r'C:\LS\10_OUT\2202_TC\ins\dem\merit_0304\MERIT_merge_0304_90x90_aoi04.tif',            
        inun_fp = r'C:\LS\02_WORK\NRC\2202_TC\06_DATA\aer\220307\aer_afed_hilo_acc_3s_20190301-20190331_v05r01_0326_xfed.tif',
        pwb_fp = r'C:\LS\02_WORK\NRC\2202_TC\06_DATA\JRC\JRC_extent_merge_0326_aoi05_clean.tif', #native resolution
 
        compress='med',
        
        #run_dataPrep
        pwb_resampling='Maximum',
        
        #build_b1Bounds: hand value stats for bouding beach1 samples
       qhigh=0.8, cap=6.0,  #uppers               
       qlow=0.2, floor=1.0, #lowers
        
        #build_inun1
        buff_dist=0, #pwb has lots of noise
        
       
        
        #build_beach2
        b2_method='polygons', b2_spacing=90*4, b2_write=True,
        
        #build_hgInterp
        hgi_minPoints=3, searchRad=90*12, hgi_resolution=90*6,
        
        #build_hgSmooth
        hval_precision=0.5,   max_iter=5,
        
        #build_depths
        d_compress='med', 
        
        **kwargs):
    
 
    
    with Session(name=name, tag=tag,
                 root_dir=r'C:\LS\10_OUT\2202_TC',
                 compress=compress,  
                 crs=QgsCoordinateReferenceSystem(crsid),
                   overwrite=True,
                   bk_lib = {
                       'pwb_rlay':dict(resampling=pwb_resampling),
                       'b1Bounds':dict(qhigh=qhigh, cap=cap, qlow=qlow, floor=floor),
                       'inun1':dict(buff_dist=buff_dist),
                       'beach2':dict(method=b2_method, spacing=b2_spacing, write_plotData=b2_write),
                       'hgInterp':dict(pts_cnt=hgi_minPoints, radius=searchRad, resolution=hgi_resolution),
                       'hgSmooth':dict(max_iter=max_iter, precision=hval_precision),
                       'depths':dict(compress=d_compress),

                       },
                   aoi_fp=aoi_fp, dem_fp=dem_fp, inun_fp=inun_fp, pwb_fp=pwb_fp, #filepaths for this project
                   **kwargs) as wrkr:
        
 

        #=======================================================================
        # wrkr.run_dataPrep()
        # wrkr.run_HAND()
        # wrkr.run_imax()
        # wrkr.run_HANDgrid()
        # wrkr.run_wslRoll()
        #=======================================================================
        wrkr.run_depths()
        
        out_dir = wrkr.out_dir
        
    return out_dir

if __name__ == "__main__":
    run_from_params()
