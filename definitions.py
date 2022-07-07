'''Application wide defaults'''
import os

 
#location of whitebox executable
whitebox_exe_d = {
        'v1.4.0':r'C:\LS\06_SOFT\whitebox\v1.4.0\whitebox_tools.exe',
        'v2.0.0':r'C:\LS\06_SOFT\whitebox\v2.0.0\whitebox_tools.exe',
        }

#maximum processors to use
max_procs = 4 

#location of source code
proj_dir = os.path.dirname(os.path.abspath(__file__))

#path to python logging config file
logcfg_file=os.path.join(proj_dir, 'logger.conf')

#root directory for building default directories in
root_dir=r'C:\LS\10_OUT\ricorde'

#parameters for parsing the parameter file
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
        'dist':(False, 'getint'),
        },
    'HAND':{},
    'HAND_mask':{},
    'inun1':{
        'buff_dist':(False, 'getint')
        },
    'beach1':{},
    'b1Bounds':{
        'qhigh':(False, 'getfloat'),
        'cap':(False, 'getfloat'),
        'qlow':(False, 'getfloat'),
        'floor':(False, 'getfloat'),
        },
    'inunHmax':{
        'hval':(False, 'getfloat'),
        },
    'inun2':{},
    'beach2':{
        'method':(False, 'get'),
        },
    'hgInterp':{
        'resolution':(False, 'getint'),
        'distP':(False, 'getfloat'),
        'pts_cnt':(False, 'getint'),
        'radius':(False, 'getfloat'),
        },
    'hgRaw':{},
    'hgSmooth':{
        'resolution':(False, 'getint'),
        'max_grade':(False, 'getfloat'),
        'neighborhood_size':(False, 'getint'),
        'range_thresh':(False, 'getfloat'),
        'max_iter':(False, 'getint'),
        'precision':(False, 'getfloat'),
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