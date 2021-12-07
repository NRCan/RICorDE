'''
Created on Mar. 26, 2020

@author: cefect

usually best to call this before any standard imports
    some modules have auto loggers to the root loger
    calling 'logging.getLogger()' after these configure will erase these
'''
import os, logging, logging.config




class BuildLogr(object): #simple class to build a logger
    
    def __init__(self,

            logcfg_file =r'C:\LS\09_REPOS\01_COMMON\coms\logger.conf',
            ):
        """
        creates a log file (according to the logger.conf parameters) in the passed working directory
        """

        #===============================================================================
        # FILE SETUP
        #===============================================================================
        #=======================================================================
        # assert os.path.exists(work_dir), work_dir
        # os.chdir(work_dir) #set this to the working directory
        # print('working directory set to \"%s\''%os.getcwd())
        # 
        #=======================================================================
        """"
        spent 30mins trying to pass the log file location explciitly
            cant get dynamic logger setup to work well with the config file
            for this behavior, best to configure with a script
            for now... just changing the directory 
        """
        assert os.path.exists(logcfg_file), 'No logger Config File found at: \n   %s'%logcfg_file
        assert logcfg_file.endswith('.conf')
        #===========================================================================
        # build logger
        #===========================================================================
        
        logger = logging.getLogger() #get the root logger
        logging.config.fileConfig(logcfg_file,
 
                                  #disable_existing_loggers=True,
                                  ) #load the configuration file
        'usually adds a log file to the working directory/_outs/root.log'
        logger.info('root logger initiated and configured from file: %s'%(logcfg_file))
        
 
        
        self.logger = logger
        
        
        
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
    
    
    
    
    
    
    
    
    
    
    
    
    