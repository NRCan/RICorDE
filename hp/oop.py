'''
Created on Mar 10, 2019

@author: cef

object oriented programming

'''


import os, sys, datetime, gc, copy, pickle

from hp.dirz import delete_dir

from hp.exceptions import Error
from qgis.core import QgsMapLayer

 

#===============================================================================
# functions------------------------------------------------------------------- 
#===============================================================================

class Basic(object): #simple base class
    
    
    
    def __init__(self, 
                 
                 #directories
                 root_dir       = None,
                 out_dir        = None,
                 temp_dir       = None,
                 
                 #labelling
                 mod_name       = 'RICorDE',
                 name           = 'SessionName', 
                 tag            = '',
                
                #logging control 
                logger         = None,
                logcfg_file    = None,
                 
                 
                 #general parameters
                 prec           = 2,
                 overwrite      = False, #file overwriting control
                 relative       = False, #specify whether 
                 
                 #inheritancee
                 inher_d        = {}, #container of inheritance pars
                 session        = None,
                 ):
        """
        Initialize a generic class object.
    
        Provides common methods and parameters for object based programming.
    
        Parameters
        ----------
        root_dir: str, optional
            Base directory of the project. Used for generating default directories.            
        out_dir : str, optional
            Directory used for outputs            
        temp_dir: str, optional
            Directory for temporary outputs
        mod_name: str, default 'RICorDE'
            Base name for all labels and directories,
        name: str, default 'SessionName'
            Name of a sub-class. (is this used?)
        tag: str, default ''
            Label for a specific run or version.
        logger: logging.RootLogger, optional
            Logging worker.
        logcfg_file: str, optional
            Filepath of a python logging configuration file
        prec: int, default 2
            Default float precision for this object.
        overwrite: bool, default False
            Default behavior when attempting to overwrite a file for this object
        relative: bool, default False
            Default behavior of filepaths (relative vs. absolute) for this object
        inher_d: dict, default {}
            Container of inheritance parameters {attribute name: object}
        session: scripts.Session, optional
            Reference to parent session
        
        """
        
        #=======================================================================
        # attachments
        #=======================================================================
        
        self.today_str = datetime.datetime.today().strftime('%Y%m%d')
        self.mod_name = mod_name
        self.name = name
        self.tag = tag
        self.prec=prec
        self.overwrite=overwrite
        self.relative=relative

        self.trash_fps = list() #container for files to delete on exit
        
        #setup inheritance handles
        self.inher_d = {**inher_d, #add all thosefrom parents 
                        **{'Basic':[ #add the basic
                            'root_dir', 'mod_name', 'tag', 'overwrite']}, 
                        }
        self.session=session
        
        #=======================================================================
        # root directory
        #=======================================================================
        if root_dir is None:
            from definitions import root_dir
        
        assert os.path.exists(root_dir), root_dir
        if not os.getcwd() == root_dir:
            os.chdir(root_dir) #set this as the working directory (mostly used by the logger)
            print('set  directory to %s'%root_dir)
            
        self.root_dir=root_dir
        #=======================================================================
        # output directory
        #=======================================================================
        if out_dir is None:
            if not tag == '':
                out_dir = os.path.join(root_dir, 'outs', tag, self.today_str)
            else:
                out_dir = os.path.join(root_dir, 'outs', self.today_str)
            
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)
            
        self.out_dir = out_dir
        
        #=======================================================================
        # #temporary directory
        #=======================================================================
        """not removing this automatically"""
        if temp_dir is None:
 
            temp_dir = os.path.join(self.out_dir, 'temp_%s_%s'%(
                self.__class__.__name__, datetime.datetime.now().strftime('%M%S')))
            
            if os.path.exists(temp_dir):
                delete_dir(temp_dir)
 
        if not os.path.exists(temp_dir):
            os.makedirs(temp_dir)
            
        self.temp_dir = temp_dir
        
        #=======================================================================
        # #setup the logger
        #=======================================================================
        if logger is None:
 
            from hp.logr import BuildLogr

            lwrkr = BuildLogr(logcfg_file = logcfg_file)
            logger=lwrkr.logger
            lwrkr.duplicate(self.out_dir, 
                        basenm='%s_%s'%(tag, datetime.datetime.today().strftime('%m%d.%H.%M')))

        self.logger=logger
            
        
        
        self.logger.debug('finished Basic.__init__')
        
    def _install_info(self,
                         log = None): #print version info
        if log is None: log = self.logger
        
        #verison info
        
        log.info('main python version: \n    %s'%sys.version)
        import numpy as np
        log.info('numpy version: %s'%np.__version__)
        import pandas as pd
        log.info('pandas version: %s'%(pd.__version__))
        
        #directory info
        log.info('os.getcwd: %s'%os.getcwd())
        
        log.info('exe: %s'%sys.executable)

        #systenm paths
        log.info('system paths')
        for k in sys.path: 
            log.info('    %s'%k)
            
    def inherit(self,#inherit the passed parameters from the passed parent
                session=None,
                inher_d = None, #attribute names to inherit from session
                logger=None,
                ):
        
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        log = logger.getChild('%s.inherit'%self.__class__.__name__)
        if session is None: session=self.session
        if inher_d is None: inher_d = self.inher_d
        
        if session is None:
            log.warning('no session! skipping')
            return {}
        else:
            self.session = session #set
        
        #=======================================================================
        # execute
        #=======================================================================
        log.debug('\'%s\' inheriting %i groups from \'%s\''%(
            self.__class__.__name__, len(inher_d), session.__class__.__name__))
        
        d = dict() #container for reporting
        cnt = 0
        for k,v in inher_d.items():
            d[k] = dict()
            assert isinstance(v, list)
            for attn in v:
                val = getattr(session, attn) #retrieve
                setattr(self, attn, val) #set
                
                d[k][attn] = val
                cnt+=1
                
            log.debug('set \'%s\' \n    %s'%(k, d[k]))
            
        log.info('inherited %i attributes from \'%s\''%(cnt, session.__class__.__name__) )
        return d
    
    def __enter__(self):
        return self
    
    def __exit__(self, #destructor
             *args,**kwargs):
        
        #print('opp.__exit__ on \'%s\''%self.__class__.__name__)
        #clear all my attriburtes
        for k in copy.copy(list(self.__dict__.keys())):
            if not k=='trash_fps':
                del self.__dict__[k]
        
        #gc.collect()
        #=======================================================================
        # #remove temporary files
        #=======================================================================
        """this fails pretty often... python doesnt seem to want to let go"""
        for fp in self.trash_fps:
            if not os.path.exists(fp): continue #ddeleted already
            try:
                if os.path.isdir(fp):
                    delete_dir(fp)
                else:
                    os.remove(fp)
                #print('    deleted %s'%fp)
            except Exception as e:
                pass
                #print('failed to delete \n    %s \n    %s'%(fp, e))
        
        
class Session(Basic): #analysis with flexible loading of intermediate results
    """typically we only instance this once
        but tests will instance multiple times
        so beware of setting containers here"""

    
    
    
    def __init__(self, 
                 bk_lib=dict(),         #kwargs for builder calls {dkey:kwargs}
                 compiled_fp_d = dict(), #container for compiled (intermediate) results {dkey:filepath}
                 data_retrieve_hndls=None, #data retrival handles
                             #default handles for building data sets {dkey: {'compiled':callable, 'build':callable}}
                            #all callables are of the form func(**kwargs)
                            #see self._retrieve2()
                            
                wrk_dir=None, #output for working/intermediate files
                write=True,

                **kwargs):
        
        assert isinstance(data_retrieve_hndls, dict), 'must past data retrival handles'
        
        
        super().__init__(**kwargs)
        
        self.data_d = dict() #datafiles loaded this session
    
        self.ofp_d = dict() #output filepaths generated this session
        
        
        #=======================================================================
        # retrival handles---------
        #=======================================================================
                    
            
        self.data_retrieve_hndls=data_retrieve_hndls
        
        #check keys
        keys = self.data_retrieve_hndls.keys()
        if len(keys)>0:
            l = set(bk_lib.keys()).difference(keys)
            assert len(l)==0, 'keymismatch on bk_lib \n    %s'%l
            
            l = set(compiled_fp_d.keys()).difference(keys)
            assert len(l)==0, 'keymismatch on compiled_fp_d \n    %s'%l
            
            
        #attach    
        self.bk_lib=bk_lib
        self.compiled_fp_d = compiled_fp_d
        self.write=write
        
        
        #start meta
        self.dk_meta_d = {k:dict() for k in keys}
 
 
        #=======================================================================
        # defaults
        #=======================================================================
        if wrk_dir is None:
            wrk_dir = os.path.join(self.out_dir, 'working')
        
        if not os.path.exists(wrk_dir):
            os.makedirs(wrk_dir)
            
        self.wrk_dir = wrk_dir
        
        
    def retrieve(self, #flexible 3 source data retrival
                 dkey,
                 *args,
                 logger=None,
                 **kwargs
                 ):
        
        if logger is None: logger=self.logger
        log = logger.getChild('retrieve')
        

        start = datetime.datetime.now()
        #=======================================================================
        # 1.alredy loaded
        #=======================================================================
        """
        self.data_d.keys()
        self.mstore
        """
        if dkey in self.data_d:
            #layers
            if isinstance(self.data_d[dkey], QgsMapLayer):
                #todo: check its in the store
                return self.data_d[dkey]
            
            try:
                return copy.deepcopy(self.data_d[dkey])
            except Exception as e:
                log.warning('failed to get a copy of \"%s\' w/ \n    %s'%(dkey, e))
                return self.data_d[dkey]
            
        
        #=======================================================================
        # retrieve handles
        #=======================================================================
        log.info('loading %s'%dkey)
                
        assert dkey in self.data_retrieve_hndls, dkey
        
        hndl_d = self.data_retrieve_hndls[dkey]
        meta_d = dict()
        #=======================================================================
        # 2.compiled provided
        #=======================================================================
 
        if dkey in self.compiled_fp_d and 'compiled' in hndl_d:
            data = hndl_d['compiled'](fp=self.compiled_fp_d[dkey], dkey=dkey)
            method='loaded pre-compiled from %s'%self.compiled_fp_d[dkey]
        #=======================================================================
        # 3.build from scratch
        #=======================================================================
        else:
            assert 'build' in hndl_d, 'no build handles for %s'%dkey
            
            #retrieve builder kwargs
            if dkey in self.bk_lib:
                bkwargs=self.bk_lib[dkey].copy()
                bkwargs.update(kwargs) #function kwargs take precident
                kwargs = bkwargs
                """
                clearer to the user
                also gives us more control for calls within calls
                """

            data = hndl_d['build'](*args, dkey=dkey, **kwargs)
            
            method='built w/ %s'%kwargs
            
        #=======================================================================
        # store
        #=======================================================================
        assert data is not None, '\'%s\' got None'%dkey
        
        if isinstance(data, QgsMapLayer):
            self.mstore.addMapLayer(data)
            
            meta_d.update({'layname':data.name(), 'source':data.source()})
            
            
        else:
            assert hasattr(data, '__len__'), '\'%s\' failed to retrieve some data'%dkey
        self.data_d[dkey] = data
        
        #=======================================================================
        # meta
        #=======================================================================
        tdelta = round((datetime.datetime.now() - start).total_seconds(), 1)
        meta_d.update({
            'tdelta (secs)':tdelta, 'dtype':type(data), 'method':method})
        
        
            
        self.dk_meta_d[dkey].update(meta_d)
        #=======================================================================
        # wrap
        #=======================================================================
        log.info('finished on \'%s\' w/   dtype=%s'%(dkey,  type(data)))
        
        return data
    

    
    def load_pick(self,
                  fp=None, 
                  dkey=None,
                  ):
        
        assert os.path.exists(fp), 'bad fp for \'%s\' \n    %s'%(dkey, fp)
        
        with open(fp, 'rb') as f:
            data = pickle.load(f)
            
        return data
    
    def write_pick(self, 
                   data, 
                   out_fp,
                   overwrite=None,
                   protocol = 3, # added in Python 3.0. It has explicit support for bytes
                   logger=None):
        
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        log=logger.getChild('write_pick')
        if overwrite is None: overwrite=self.overwrite
        
        #=======================================================================
        # checks
        #=======================================================================
        
        if os.path.exists(out_fp):
            assert overwrite, out_fp
            
        assert out_fp.endswith('.pickle')
            
        log.debug('writing to %s'%out_fp)
        
        with open(out_fp,  'wb') as f:
            pickle.dump(data, f, protocol)
        
        log.info('wrote %i to %s'%(len(data), out_fp))
            
        
        return out_fp
        
    def _get_meta(self, #get a dictoinary of metadat for this model
                 ):
        
        d = super()._get_meta()
        
        if len(self.data_d)>0:
            d['data_d.keys()'] = list(self.data_d.keys())
            
        if len(self.ofp_d)>0:
            d['ofp_d.keys()'] = list(self.ofp_d.keys())
            
        if len(self.compiled_fp_d)>0:
            d['compiled_fp_d.keys()'] = list(self.compiled_fp_d.keys())
            
        if len(self.bk_lib)>0:
            d['bk_lib'] = copy.deepcopy(self.bk_lib)
            
        return d
    
    def __exit__(self, #destructor
                 *args, **kwargs):
        
        print('oop.Session.__exit__ (%s)'%self.__class__.__name__)
        
        #=======================================================================
        # log major containers
        #=======================================================================
        if len(self.data_d)>0:
            print('    data_d.keys(): %s'%(list(self.data_d.keys())))
            self.data_d = dict() #not necessiary any more
        
        if len(self.ofp_d)>0:
            print('    ofp_d (%i):'%len(self.ofp_d))
            for k,v in self.ofp_d.items():
                print('        \'%s\':r\'%s\','%(k,v))
            print('\n')
            self.ofp_d = dict()
              
              
        
        
        super().__exit__(*args, **kwargs)
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    