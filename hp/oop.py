'''
Created on Mar 10, 2019

@author: cef

object oriented programming

'''


import os, sys, datetime, gc, copy

from hp.dirz import delete_dir

from hp.exceptions import Error


 

#===============================================================================
# functions------------------------------------------------------------------- 
#===============================================================================

class Basic(object): #simple base class
    
    
    
    def __init__(self, 
                 logger         = None,
                 out_dir        = None,
                 work_dir       = r'C:\LS\03_TOOLS\misc',
                 mod_name       = 'Simp',
                 name           = 'SessionName', 
                 tag            = '',
                 prec           = 2,
                 overwrite      = False, #file overwriting control
                 
                 #inheritancee
                 inher_d        = {}, #container of inheritance pars
                 session        = None,
                 ):
        
        #=======================================================================
        # attachments
        #=======================================================================
        
        self.today_str = datetime.datetime.today().strftime('%Y%m%d')
        self.work_dir = work_dir
        self.mod_name = mod_name
        self.tag = tag
        self.prec=prec
        self.overwrite=overwrite
        self.name = name
        self.trash_fps = list() #container for files to delete on exit
        
        #setup inheritance handles
        self.inher_d = {**inher_d, #add all thosefrom parents 
                        **{'Basic':[ #add the basic
                            'work_dir', 'mod_name', 'tag', 'overwrite']}, 
                        }
        self.session=session
        
        #=======================================================================
        # working directory
        #=======================================================================
        """needed by logger"""
        os.chdir(work_dir) #set this to the working directory
        print('working directory set to \"%s\''%os.getcwd())
            
        #=======================================================================
        # output directory
        #=======================================================================
        if out_dir is None:
            if not tag == '':
                out_dir = os.path.join(work_dir, 'outs', tag, self.today_str)
            else:
                out_dir = os.path.join(work_dir, 'outs', self.today_str)
            
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)
            
        self.out_dir = out_dir
        
        #=======================================================================
        # #setup the logger
        #=======================================================================
        if logger is None:
            from hp.logr import BuildLogr
            lwrkr = BuildLogr()
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
        
        
        
        
                
        
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    