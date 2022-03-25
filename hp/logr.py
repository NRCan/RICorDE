'''
Created on Mar. 26, 2020

@author: cefect

usually best to call this before any standard imports
    some modules have auto loggers to the root loger
    calling 'logging.getLogger()' after these configure will erase these
'''
import os, logging, logging.config, pprint




class BuildLogr(object): #simple class to build a logger
    
    def __init__(self,

            logcfg_file =None,
            ):
        """
        creates a log file (according to the logger.conf parameters) in the passed working directory
        """

        #===============================================================================
        # FILE SETUP
        #===============================================================================
        if logcfg_file is None:
            #todo: check if there is a definitions file
            from definitions import logcfg_file #import from the definitions file
 
        assert os.path.exists(logcfg_file), 'No logger Config File found at: \n   %s'%logcfg_file
        assert logcfg_file.endswith('.conf')
        #===========================================================================
        # build logger
        #===========================================================================
        
        logger = logging.getLogger() #get the root logger
        
        #load the configuration file
        """
        relative to os.getcwd()
        usually creates 2 fileloggers (in wd) and 1 console logger 
        """
        logging.config.fileConfig(logcfg_file, 
 
                                  #disable_existing_loggers=True,
                                  ) 
        
        logger.info('root logger initiated and configured from file: %s'%(logcfg_file))
        
 
 
        
        self.logger = logger
        self.log_handlers()
        
    def log_handlers(self, #convenience to readout handler info
                     logger=None):
        if logger is None:
            logger=self.logger
            
        #=======================================================================
        # #collect handler info
        #=======================================================================
        res_lib = dict()
        for handler in logger.handlers:
            
            htype = type(handler).__name__
            
            d = {'htype':htype}
            
            if 'FileHandler' in htype:
                d['filename'] = handler.baseFilename
            
            
            
            res_lib[handler.get_name()] = d
            
        #=======================================================================
        # #log
        #=======================================================================
        #get fancy string
        txt = pprint.pformat(res_lib, width=30, indent=0, compact=True, sort_dicts =False)
        
        for c in ['{', '}']: 
            txt = txt.replace(c, '') #clear some unwanted characters..
        
        logger.info('logger configured w/ %i handlers\n%s'%(len(res_lib), txt))
        
        return res_lib
        
 
                
 
        
    def duplicate(self, #duplicate the root logger to a diretory
                  out_dir, #directory to place the new logger
                  basenm = 'duplicate', #basename for the new logger file
                  level = logging.DEBUG,
                  ):
        
        #===============================================================================
        # # Load duplicate log file
        #===============================================================================
        assert os.path.exists(out_dir)
        logger_file_path = os.path.join(out_dir, '%s.log'%basenm)
        
        #build the handler
        formatter = logging.Formatter('%(asctime)s.%(levelname)s.%(name)s:  %(message)s')        
        handler = logging.FileHandler(logger_file_path) #Create a file handler at the passed filename 
        handler.setFormatter(formatter) #attach teh formater object
        handler.setLevel(level) #set the level of the handler
        
        self.logger.addHandler(handler) #attach teh handler to the logger
        
        self.logger.info('duplicate logger \'level = %i\' built: \n    %s'%(
            level, logger_file_path))
        
def get_new_file_logger(
        name,
        level=logging.DEBUG,
        fp=None, #file location to log to
        ):
    
    #===========================================================================
    # configure the logger
    #===========================================================================
    logger = logging.getLogger(name)
    
    logger.setLevel(level)
    
    #===========================================================================
    # configure the handler
    #===========================================================================
    assert fp.endswith('.log')
    
    formatter = logging.Formatter('%(asctime)s.%(levelname)s:  %(message)s')        
    handler = logging.FileHandler(fp, mode='w') #Create a file handler at the passed filename 
    handler.setFormatter(formatter) #attach teh formater object
    handler.setLevel(level) #set the level of the handler
    
    logger.addHandler(handler) #attach teh handler to the logger
    
    logger.info('built new file logger \'%s\' here \n    %s'%(name, fp))
    
    return logger
    
    
    
    
    
    
    
    
    
    
    
    
    