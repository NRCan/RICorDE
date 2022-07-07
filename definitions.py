'''
Created on Mar. 25, 2022

@author: cefect
'''
import os
proj_dir = os.path.dirname(os.path.abspath(__file__))

logcfg_file=os.path.join(proj_dir, 'logger.conf')

root_dir=r'C:\LS\10_OUT\ricorde'

config_params = { #{sectionName:{varName:(mandatory_flag, ConfigParser get method)}}
     'session':{
        'aoi_fp':(False, 'get'),
        'dem_fp':(True, 'get'),
        'pwb_fp':(True, 'get'),
        'inun_fp':(True, 'get'),
        'crsid':(True, 'get'),'name':(True, 'get'),
        },
                         
    'dem':{
        'resolution':(False, 'getint'),
        },
    'pwb_rlay':{
        'resampling':(False, 'get'),
        },
    'inun_rlay':{
        'resampling':(False, 'get'),
        },
    
    'dem_hyd':{
        
        },
    'HAND':{
        
        },
    'HAND_mask':{
        
        },
    'inun1':{
        
        },
    'beach1':{
        
        },
    'b1Bounds':{
        
        },
    'inunHmax':{
        
        },
    'inun2':{
        
        },
    'beach2':{
        
        },
    'hgInterp':{
        
        },
    'hgRaw':{
        
        },
    'hgSmooth':{
        
        },
    'hInunSet':{
        
        },
    'hWslSet':{
        
        },
    'wslMosaic':{
        
        },
    'depths':{
        
        },
    
    }