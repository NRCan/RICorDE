'''
Created on Mar. 27, 2021

@author: cefect

HAND raster calculations
'''
#===============================================================================
# imports-----------
#===============================================================================
import os, datetime
 




#from hp.exceptions import Error
#from hp.dirz import force_open_dir
 
#from hp.Q import Qproj, QgsRasterLayer, QgsCoordinateReferenceSystem, QgsMapLayerStore #only for Qgis sessions

from hp.whitebox import Whitebox
 
from ricorde.tcoms import TComs

"""doesn't seem to be working... need to stick with QGIS algos
import whitebox
from WBT.whitebox_tools import WhiteboxTools"""


#===============================================================================
# vars
#===============================================================================



#===============================================================================
# CLASSES----------
#===============================================================================

        
        
class HANDses(TComs):
 
        
    def __init__(self,tag='HAND',**kwargs):
        

        
        super().__init__(tag=tag,**kwargs)  # initilzie teh baseclass
        
 
        
    def load_dem(self, fp):
        self.dem_fp = fp
        self.dem_rlay = self.rlay_load(fp, set_proj_crs=True)
        

        
    def hydro_correct(self, #hydro correct the DEM
                   dem_fp = '',
                   dist=100, #breach distance
                   logger=None,
                   ofp=None,
                   ):
        
 
        if logger is None: logger=self.logger
        
        #check compression
        assert self.getRasterCompression(dem_fp) is None, 'dem has some compression: %s'%dem_fp
        
        ofp = Whitebox(out_dir=self.out_dir, logger=logger
                 ).breachDepressionsLeastCost(dem_fp, dist=dist, ofp=ofp)
                 
        assert self.getRasterCompression(ofp) is None, 'result has some compression: %s'%dem_fp
        
        """
        ofp=r'C:\LS\10_OUT\202103_InsCrve\outs\DR\20220114\wrk\filldep.tif'
        
        """
        
        return ofp
    
    def hand(self,
                 dem_fp, #filepath to hydrocorrected dem... must be uncompressed!
                 stream_fp,
                 ofp=None,
                 logger=None,
                 ):
 
        if logger is None: logger=self.logger
        if ofp is None:
            ofp = os.path.join(self.temp_dir, '%s_%s_HAND_%s.tif'%(
                self.name, self.tag,  datetime.datetime.now().strftime('%m%d')))
        
        if os.path.exists(ofp): 
            assert self.overwrite
            os.remove(ofp)
            
        
        assert self.getRasterCompression(dem_fp) is None, 'dem has some compression: %s'%dem_fp
        #assert self.getRasterCompression(stream_fp) is None, 'streams have some compression: %s'%stream_fp

        
        return Whitebox(out_dir=self.out_dir, logger=logger
                 ).elevationAboveStream(dem_fp, stream_fp, out_fp=ofp)
                 
    def run(self,
            dem_fp='',
            stream_fp='',
            compress=None,
            logger=None,
            ofp = None,
            ):
        
        """
        TODO: add check on streams
        """
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        log=logger.getChild('r')
        if compress is None:
            compress=self.compress
            
        #filepaths
        if ofp is None:
            ofp = os.path.join(self.out_dir, self.layName_pfx+'_HAND.tif')
            
        if os.path.exists(ofp):
            assert self.overwrite
            os.remove(ofp)
            

        
        #=======================================================================
        # algo
        #=======================================================================
        #pit remove
        dem_hyd_fp = self.hydro_correct(dem_fp=dem_fp, logger=log)

        #get HAND
        if not compress=='none': hand1_fp = None
        else: hand1_fp = ofp
        hand1_fp = self.hand(dem_hyd_fp, stream_fp, ofp=hand1_fp, logger=log)
        

        #=======================================================================
        # compression
        #=======================================================================
        if not compress=='none':
            self.trash_fps.append(hand1_fp)
            
            ofp = self.warpreproject(hand1_fp, compression=compress, nodata_val=-9999,
                                     output=ofp)
        else:
            ofp = hand1_fp
            

            
            #=======================================================================
        # wrap
        #=======================================================================
 
        log.debug('finished on %s'%ofp)
            
        return ofp


        

        

        
        



#===============================================================================
# FUNCTIONS-----------------
#===============================================================================
from memory_profiler import profile
#@profile(precision=2)
def get_HAND(
        
        dem_fp = r'C:\LS\03_TOOLS\_jobs\202103_InsCrve\outs\HRDEM\20210716\HRDEM_0711_filld_cmp.tif',
        
        #small one for testing
        #dem_fp = r'C:\LS\03_TOOLS\_jobs\202103_InsCrve\outs\HAND\HRDEM_cilp2.tif',
        
        
        stream_fp=r'C:\LS\03_TOOLS\_jobs\202103_InsCrve\outs\depWf\20210715b\NHN_HD_WATERBODY_CMM1_0715_clean.gpkg',
        name='CMM'
        ):
    """1.23 min. 200Mb"""
    
    with HANDses(name=name, overwrite=True) as wrkr:

        ofp = wrkr.run(dem_fp, stream_fp)

    
 
    
    return ofp
    






if __name__ =="__main__": 
    start =  datetime.datetime.now()
    print('start at %s'%start)


    get_HAND()

    
    

    
    tdelta = datetime.datetime.now() - start
    print('finished in %s'%tdelta)
