'''
Created on Jun 24, 2017

@author: cef

Library of windows file/directory common operations
'''


# Import Python LIbraries 
import os, time, shutil, logging,  re, copy

import urllib.request as request
from contextlib import closing


from datetime import datetime




"""
throws: ModuleNotFoundError: No module named '_tkinter'

2019 12 11: played with this a bit.
seems to only throw during debugging
"""


from hp.exceptions import Error



mod_logger = logging.getLogger(__name__)
"""NOTE ON LOGGING
In general, logging should go to the 'main' logger called above
WARNING: this requires the main logger be initialized prior to loading this script
see main.py for details

 """          

mod_logger.debug('hp.basic initialized')
 

def get_temp_dir(temp_dir_sfx = r'py\temp'):
    
    from pathlib import Path
    homedir = str(Path.home())
    temp_dir = os.path.join(homedir, temp_dir_sfx)
    if not os.path.exists(temp_dir):
        os.makedirs(temp_dir)
        
    return temp_dir


def force_open_dir(folder_path_raw, logger=mod_logger): #force explorer to open a folder
    logger = logger.getChild('force_open_dir')
    
    if not os.path.exists(folder_path_raw):
        logger.error('passed directory does not exist: \n    %s'%folder_path_raw)
        return False
        
    import subprocess
    
    #===========================================================================
    # convert directory to raw string literal for windows
    #===========================================================================
    try:
        #convert forward to backslashes
        folder_path=  folder_path_raw.replace('/', '\\')
    except:
        logger.error('failed during string conversion')
        return False
    
    try:

        args = r'explorer "' + str(folder_path) + '"'
        subprocess.Popen(args) #spawn process in explorer
        'this doesnt seem to be working'
        logger.info('forced open folder: \n    %s'%folder_path)
        return True
    except:
        logger.error('unable to open directory: \n %s'%dir)
        return False
    
def copy_file(filetocopy_path,  #copy file to a directory
              dest_base_dir, 
              overwrite = True, 
              new_fn = None,
              sfx = None,
              logger=mod_logger): 
    """
    #===========================================================================
    # INPUTS
    #===========================================================================
    
    """
    #===========================================================================
    # setups and defaults
    #===========================================================================
    logger = logger.getChild('copy_file')
    tail, filename = os.path.split(filetocopy_path)
    if new_fn is None: 
        new_fn = filename
    
    
    #===========================================================================
    # get new filepath
    #===========================================================================
    if not sfx is None:
        nfn_clean, ext = os.path.splitext(new_fn)
        
        if ext == '':
            raise Error('missing the extension')
        
        new_fn = '%s%s%s'%(nfn_clean, sfx, ext)
    
    
    dest_file_path = os.path.join(dest_base_dir, new_fn)
    
    #===========================================================================
    # checks and sub folder creation
    #===========================================================================
    if not os.path.exists(filetocopy_path):
        logger.error('passed file does not exist: \"%s\''%filetocopy_path)
        raise IOError
    
    if os.path.exists(dest_file_path):
        logger.warning('destination file already exists. no copy made. \n    %s'%dest_file_path)
        return False
    
    if not os.path.exists(dest_base_dir): #check if the base directory exists
        logger.warning('passed base directory does not exist. creating:\n    %s'%dest_base_dir)
        os.makedirs(dest_base_dir) #create the folder

    try:    
        shutil.copyfile(filetocopy_path,dest_file_path)
    except:
        logger.error('failed to copy \'%s\' to \'%s\''%(filetocopy_path,dest_file_path))
        raise IOError
    
    logger.debug('copied file to \n    %s'%dest_file_path)
    
    return True

def build_out_dir( #build an output folder with some logic and unique checking
                    work_dir, #parent directory to create the working folder in
                    basename='out', #base name for the outs folder
                    logger=mod_logger):
    
    logger = logger.getChild('build_out_dir')
    
    #precheck
    if not os.path.exists(work_dir): 
        raise IOError('passed work_dir does not exists: \'%s\''%work_dir)
    
    #===========================================================================
    # loop and find unique name
    #===========================================================================
    for ind in range(0,100,1):
        tnow = datetime.now().strftime('%Y%m%d%H%M%S') #convenience time string
    
        out_dir = os.path.join(work_dir, basename+'_'+ tnow)
    
    
        if os.path.exists(out_dir): #check to see if it exists
            logger.warning('time generated output folder already exists at: %s'%out_dir)
            time.sleep(1)
            continue #wait a second and try anoth reloop
        else:
            break
        
    #===========================================================================
    # wrau pup
    #===========================================================================
    if out_dir is None: raise IOError #loop didnt find a good path
        
    os.makedirs(out_dir) #create the folder
    logger.info('out_dir built: %s'%out_dir)
    
    return out_dir

def copy_to_temp( #copy the file to a temporary directory
        fpath, 
        temp_dir = None,
        override = True, 
        logger = mod_logger):
    
    #===========================================================================
    # defaults
    #===========================================================================
    log = logger.getChild('copy_to_temp')
    if temp_dir is None: temp_dir = get_temp_dir()
    
    #===========================================================================
    # make the copy
    #===========================================================================
    if not fpath.endswith('.shp'):
        _, ogfn = os.path.split(fpath)
        dest_fp = os.path.join(temp_dir, '_temp_'+ogfn)
        return shutil.copy(fpath, dest_fp)
    
    else:
        #=======================================================================
        # special batch copy for shapefiles
        #=======================================================================
        shp_ext = ('cpg', 'dbf', 'prj', 'qpj', 'shx','shp')
        log.debug('shape file detected. copying %i sibling files'%len(shp_ext))
        
        root_fpath = fpath[:-4] #strip the extension
        
        #loop through each extnesion
        cnt = 0
        for ext in shp_ext:
            ofpath = root_fpath + '.'+ext
            
            #make sure the file we are copying exists
            if not os.path.exists(ofpath):
                log.error('could not find file: %s. skipping'%ofpath)
                continue #not all shape file sets have the same extensions
            
            #get the new file name
            _, ofname = os.path.split(ofpath)
            dest_fp = os.path.join(temp_dir, '_temp_'+ofname)
            
            #make the copy
            res = shutil.copy(ofpath, dest_fp)
            cnt+=1
            
        log.debug('finisehd making %i copies'%cnt)
        
        if not res.endswith('.shp'):
            raise IOError
        
        return res  #should return the sh ape files
    
    
def delete_dir(dirpath): #remove directory AND contents
    assert os.path.exists(dirpath)
    
    #collect all the files
    fps = set()
    for dir_i, _, fns in os.walk(dirpath):
        fps.update([os.path.join(dir_i, e) for e in fns])
        
    assert len(fps)<1000, 'safety check... %i files requested for removal in \n    %s'%(
        len(fps), dirpath)
    #remove all these files
    for fp in fps:
        try:
            os.remove(fp)
            #print('deleted %s'%fp)
        except Exception as e:
            #print('failed to remove %s \n    %s'%(fp, e))
            pass
        
    #remove the driector yu
    try:
        os.rmdir(dirpath)
        #print('deleted %s'%dirpath)
    except Exception as e:
        pass
        #print('failed to remove directory %s /n    %s'%( dirpath, e))
    
    
    



def get_valid_filename(s):
    s = str(s).strip().replace(' ', '_')
    s = re.sub(r'(?u)[^-\w.]', '', s)
    s = re.sub(':','-', s)
    return s

def url_retrieve(
        url, #url of file to download
        ofp=None, #output directory
        use_cache=False, #if the file is already there.. just use it
        overwrite=False,
        logger=mod_logger,
        ):
    log=logger.getChild('url_retrieve')
    #===========================================================================
    # #get output filepath
    #===========================================================================
    if ofp is None: 
        
        out_dir = get_temp_dir()
        
        ofp = os.path.join(out_dir, os.path.basename(url))
        
    #===========================================================================
    # check existance and chace
    #===========================================================================
    if os.path.exists(ofp):
        if use_cache:
            log.info('using cache: %s'%ofp)
            return ofp
        
        assert overwrite, 'file exists: \n    %s'%ofp
        os.remove(ofp)
        
    #ensure base directory exists
    if not os.path.exists(os.path.dirname(ofp)):os.makedirs(os.path.dirname(ofp))
    
    #===========================================================================
    # #download and copy over
    #===========================================================================
    log.debug('downloading from \n    %s'%url)
    
    try:
        with closing(request.urlopen(url.lower())) as r:
            with open(ofp, 'wb') as f:
                shutil.copyfileobj(r, f)
    except Exception as e:
        raise Error('failed to DL from %s w/ \n    %s'%(url, e))
            
    log.info('downloaded from %s'%url)
            
    return ofp
    
    
if __name__ == '__main__':
    
   

    
    print('finished %s'%__name__)
    
    
    
    

 