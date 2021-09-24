'''
Created on Jul. 15, 2021

@author: cefect

gdal/ogr helpers

2021-07-24
    was getting some strange phantom crashing
    reverted and seems to be working again
'''


#===============================================================================
# imports----------
#===============================================================================
import time, sys, os, logging, copy

from osgeo import ogr, gdal_array, gdal

import numpy as np


from qgis.core import QgsVectorLayer, QgsMapLayerStore

from hp.exceptions import Error


mod_logger = logging.getLogger(__name__)

#===============================================================================
# classes------
#===============================================================================

class GeoDataBase(object): #wrapper for GDB functions
    
    def __init__(self,
                 fp, #filepath to gdb
                 name=None,
                 logger=mod_logger,
                 ):
        #=======================================================================
        # defaults
        #=======================================================================
        #logger setup
        self.logger = logger.getChild(os.path.basename(fp))
        if name is None: name = os.path.basename(fp)
        self.name=name
        self.fp = fp
        self.mstore = QgsMapLayerStore()
        #=======================================================================
        # #driver and data
        #=======================================================================
        self.driver = ogr.GetDriverByName("OpenFileGDB")
        
        self.data = self.driver.Open(fp, 0)
        
        #get layer info
        self.layerNames_l = [l.GetName() for l in self.data]
        
        logger.info('loaded GDB \'%s\' w/ %i layers \n    %s'%(
            name,len(self.layerNames_l), self.layerNames_l))
        
    def GetLayerByName(self, layerName, 
                       mstore=True, #whether to add the layer to the mstore (and kill on close)
                       ): #return a pyqgis layer
        
        assert layerName in self.layerNames_l, 'passed layerName not found \'%s\''%layerName
        
        #=======================================================================
        # load the layer
        #=======================================================================
        uri = "{0}|layername={1}".format(self.fp, layerName)
        
        vlay = QgsVectorLayer(uri,layerName,'ogr')
        
        #===========================================================================
        # checks
        #===========================================================================
        if not isinstance(vlay, QgsVectorLayer): 
            raise IOError
        
        #check if this is valid
        if not vlay.isValid():
            raise Error('loaded vlay \'%s\' is not valid. \n \n did you initilize?'%vlay.name())
        
        #check if it has geometry
        if vlay.wkbType() == 100:
            raise Error('loaded vlay has NoGeometry')
        
        #check coordinate system
        if not vlay.crs().isValid():
            raise Error('bad crs')
        
        if vlay.crs().authid() == '':
            print('\'%s\' has a bad crs'%layerName)
            
        #=======================================================================
        # wrap
        #=======================================================================
        
        if mstore: self.mstore.addMapLayer(vlay)
        
        self.logger.debug("loading with mstore=%s \n    %s"%(mstore, uri))
        
        return vlay
        
    def __enter__(self,):
        return self
    
    def __exit__(self, #destructor
                 *args,**kwargs):
        

        self.logger.debug('closing layerGDB')
        
        self.data.Release()
        
        self.mstore.removeAllMapLayers()
        
        #super().__exit__(*args,**kwargs) #initilzie teh baseclass
        

#===============================================================================
# functions------
#===============================================================================
def get_layer_gdb_dir( #extract a specific layer from all gdbs in a directory
                 gdb_dir,
                 layerName='NHN_HD_WATERBODY_2', #layername to extract from GDBs
                 logger=mod_logger,
                 ):
    
    #=======================================================================
    # defaults
    #=======================================================================
    log=logger.getChild('get_layer_gdb_dir')
    
    
    #=======================================================================
    # #get filepaths
    #=======================================================================
    fp_l = [os.path.join(gdb_dir, e) for e in os.listdir(gdb_dir) if e.endswith('.gdb')]
    
    log.info('pulling from %i gdbs found in \n    %s'%(len(fp_l), gdb_dir))
    
    #=======================================================================
    # load and save each layer
    #=======================================================================
    d = dict()
    for fp in fp_l:

        """need to query layers in the gdb.. then extract a specific layer"""
         
        
        with GeoDataBase(fp, logger=log) as db:
            assert not db.name in d, db.name
            d[copy.copy(db.name)] = db.GetLayerByName(layerName, mstore=False)
        
 
            
    
    log.info('loaded %i layer \'%s\' from GDBs'%(len(d), layerName))
    
    return d

def get_nodata_val(rlay_fp):
    assert os.path.exists(rlay_fp)
    ds = gdal.Open(rlay_fp)
    band = ds.GetRasterBand(1)
    return band.GetNoDataValue()
    
    


def rlay_to_array(rlay_fp, dtype=np.dtype('float32')):
    #get raw data
    ds = gdal.Open(rlay_fp)
    band = ds.GetRasterBand(1)
    
    
    ar_raw = np.array(band.ReadAsArray(), dtype=dtype)
    
    #remove nodata values
    ndval = band.GetNoDataValue()
    
    ar_raw[ar_raw==ndval]=np.nan
    
    return ar_raw

    
    
    
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            