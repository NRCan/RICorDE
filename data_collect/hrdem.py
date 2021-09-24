'''
Created on Mar. 27, 2021

@author: cefect

downloading and cleaning HRDEM mosaic data

TODO: explore downloading raw HRDEM datasets from the FTP to improve performance
    2021-07-15: see email to Charles
    could just download a random data tile
'''
#===============================================================================
# imports-----------
#===============================================================================
import os, datetime, urllib
import pandas as pd
import numpy as np




from hp.exceptions import Error
from hp.dirz import force_open_dir, url_retrieve
 
from hp.Q import Qproj, QgsRasterLayer, QgsCoordinateReferenceSystem, QgsMapLayerStore,\
    QgsDataSourceUri, QgsVectorLayer, QgsWkbTypes
 
from hp.whitebox import Whitebox

from t1.tcoms import TComs

#===============================================================================
# vars
#===============================================================================



#===============================================================================
# CLASSES----------
#===============================================================================

        
        
class HRDEMses(TComs):
    """
    #===========================================================================
    # CRS
    #===========================================================================
    HRDEM wcs layer should always be 3979
        accepting aois with different CRS, (adopting these for the qproj)
        so we can clip and reproject the HRDEM layer
    
    """
    
    
    wcs_rlay = None

    trash_fps = [] #container of fps to delete during wrap up
        
    def __init__(self,
 
                  tag='HRDEM',
                  #compres='med',
                  #using hi compression 
                  #work_dir = r'C:\LS\03_TOOLS\_jobs\202103_InsCrve',
                  #session=None, #parent session for child mode
                 **kwargs):
        
        
        
        super().__init__(
                        tag=tag,
                        # work_dir=work_dir,
                         **kwargs)  # initilzie teh baseclass
        
    def get_hrdem(self, #main run sequence for retriving HRDEM mosaic data
                  aoi_fp=None,
                  resolution=10,
                  logger=None,
                  compress=None, 
                  ofp=None,
                  ):
        start =  datetime.datetime.now()
        
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        log= logger.getChild('get_hrdem')
        
        if aoi_fp is None: aoi_fp=self.aoi_fp
        if compress is  None: compress=self.compress
        #=======================================================================
        # download the raw tif
        #=======================================================================
        fp_key = 'dem_raw_fp'
        if not fp_key in self.fp_d:
            try:
                
                raw_fp = self.retrieve_url(aoi_fp=aoi_fp, resolution=resolution, logger=log)
            except Exception as e:
                log.warning('retrieve_url failed w/\n    %s'%e)
                raw_fp = self.retrieve_qgs(aoi_fp=aoi_fp, resolution=resolution, logger=log)
                
            self.ofp_d[fp_key] = raw_fp
        else:
            raw_fp = self.fp_d[fp_key]
        #=======================================================================
        # convert
        #=======================================================================
        
        ofp = self.clean_and_clip(dem_raw_fp = raw_fp, aoi_fp=aoi_fp, logger=log, ofp=ofp,
                                  resolution=resolution)
        

        
        #=======================================================================
        # wrap
        #=======================================================================
        
        tdelta = datetime.datetime.now() - start        
        runtime = round(tdelta.total_seconds()/60.0, 3)
        log.info('retrieved HRDEM mosaic in %.2f mins at \n    %s'%(runtime, ofp))
        
        return ofp, runtime
        
 
    def retrieve_url(self, #get a wcs raster clip from an aoi using url downloads
                     aoi_fp='',
                     authid='EPSG:3979',
                     resolution=10,
                     url=r'https://datacube-stage.services.geo.ca/ows/elevation?service=WCS',
                     logger=None,
                     ofp=None,
                     ):
        
        """
        this is faster than using qgs, but:
        the datacube-stage url is only working during business hours?"""
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        log= logger.getChild('dl')
        crs = QgsCoordinateReferenceSystem(authid)
        
        assert resolution>=1
        #=======================================================================
        # aoi
        #=======================================================================
        if isinstance(aoi_fp, str):
            aoi_vlay = self.vlay_load(aoi_fp, logger=log)
            
        assert isinstance(aoi_vlay, QgsVectorLayer)
        assert 'Polygon' in QgsWkbTypes().displayString(aoi_vlay.wkbType())
        assert aoi_vlay.dataProvider().featureCount()==1
        assert aoi_vlay.crs()==self.qproj.crs() 
        """
        here we work within the typical HRDEM crs
            see clean_and_clip() for reprojecting to the project crs
                which should match the aoi
        """
        
        #reprojec
        if not aoi_vlay.crs()==crs:
            aoi_vlay = self.reproject(aoi_vlay, crsOut=crs, logger=log)
            
        #get bounding box
        rect = aoi_vlay.extent()
        

        if ofp is None: 
            ofp=os.path.join(self.temp_dir, 'HRDEM_wcs_%s_%s.tif'%(
                aoi_vlay.name()[:5],
                self.today_str))
        #=======================================================================
        # build uri
        #=======================================================================
        
        bbox_str = '%.3f,%.3f,%.3f,%.3f'%(
            rect.xMinimum(), rect.yMinimum(), rect.xMaximum(), rect.yMaximum())
        
 
        GridBaseCRS = 'urn:ogc:def:crs:EPSG:'+crs.authid().replace('EPSG','')
        
        pars_d = {
            '':url,
            'version':'1.1.1',
            'request':'GetCoverage',
            'identifier':'dtm',
            'format':'image/geotiff',
            'BoundingBox':bbox_str+','+GridBaseCRS,
            'GridBaseCRS':GridBaseCRS,
            'GridOffsets':'%.1f,-%.1f'%(resolution, resolution)
            }

 
        
        uri = urllib.parse.unquote(urllib.parse.urlencode(pars_d, encoding='utf-8'))[1:]
 
        log.info('got \n    %s'%uri)
        
        #=======================================================================
        # download the file
        #=======================================================================
        url_retrieve(uri, ofp=ofp, overwrite=self.overwrite, logger=log)
 
        
        return ofp


    def retrieve_qgs(self, #get a local copy using QGS objects

                   url='cache=PreferCache&crs=EPSG:3979&dpiMode=7&format=image/geotiff&identifier=dtm&url=https://datacube.services.geo.ca/ows/elevation',
                   aoi_fp='',
                   ofp=None,
                   resolution='raw',
                                      logger=None,
                   ): #load the wcs layher
        
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        log= logger.getChild('retrieve_qgs')
        
        if ofp is None:
            ofp = os.path.join(self.temp_dir, self.layName_pfx + '_hrdem_raw.tif')
            
        if os.path.exists(ofp):
            assert self.overwrite
            os.remove(ofp)
        
        
        #=======================================================================
        # load the wcs layer
        #=======================================================================
        wcs_rlay = QgsRasterLayer(url, 'rlay', 'wcs')
        
        assert wcs_rlay.isValid(), "Layer failed to load!"
        assert isinstance(wcs_rlay, QgsRasterLayer), 'failed to get a QgsRasterLayer'
        
        """always working in the crs with HRDEM"""
        assert wcs_rlay.crs() == QgsCoordinateReferenceSystem('EPSG:3979')
        
        wcs_rlay.setName('HRDEM_wcs')
        
        self.mstore.addMapLayer(wcs_rlay)
        
 
        
        #=======================================================================
        # buidl
        #=======================================================================

        log.info('building from \'%s\''%wcs_rlay.name())
        
        #=======================================================================
        # handle crs
        #=======================================================================
        aoi_vlay_raw = self.vlay_load(aoi_fp, logger=log)
        """not forcing match with qproj... just working within EPSG3979"""
        if not wcs_rlay.crs() == aoi_vlay_raw.crs():
            aoi_vlay = self.reproject(aoi_vlay_raw, crsOut = wcs_rlay.crs())
            self.mstore.addMapLayer(aoi_vlay)
        else:
            aoi_vlay = aoi_vlay_raw
            
        
        #=======================================================================
        # precheck
        #=======================================================================
        """need the crs to line up for setting the export extents"""
        assert wcs_rlay.crs() == aoi_vlay.crs()
        #=======================================================================
        # make a raw local copy
        #=======================================================================
        """this can be super slow... can't get the feedback object to work"""
        self.overwrite=True
        log.debug('creating a local raster at %s \n'%aoi_vlay.extent())
        
        ofp = self.rlay_write(wcs_rlay, 
                               ofp=ofp,
                               extent=aoi_vlay.extent(), 
                               resolution=resolution,
                               opts=[], #deleting this anyways.. just do uncompressed
                               logger=log, 
                               )
 
        #=======================================================================
        # wrap
        #=======================================================================
 
        
        return ofp
        
 
        
 
        
    def clean_and_clip(self,
                       dem_raw_fp='',
                       aoi_fp='',
                  resolution=None, #want the reprojected to match this resolution also
                   fillNoData=200, #pixel distance to fill (pass 0 for no filling)
                   logger=None,
                   ofp=None,
                   ):
        
        #=======================================================================
        # defaults
        #=======================================================================
  
        
        if logger is None: logger=self.logger
        log= logger.getChild('clean_and_clip')
        
        assert os.path.exists(dem_raw_fp)
 
        #=======================================================================
        # filepaths
        #=======================================================================
        if ofp is None:
            ofp = os.path.join(self.out_dir, 
               'hrdem_%s_%s_%02d_fild.tif'%(
                   self.name, datetime.datetime.now().strftime('%m%d'), resolution))
        
        if os.path.exists(ofp):
            assert self.overwrite
            os.remove(ofp)
            
        #=======================================================================
        # get crs
        #=======================================================================
        #=======================================================================
        # aoi_vlay = self.vlay_load(aoi_fp, logger=log)
        # self.mstore.addMapLayer(aoi_vlay)
        #=======================================================================
        #aoi_fp = self.reproject(aoi_vlay_raw, crsOut = wcs_rlay.crs())['OUTPUT']
            
        #=======================================================================
        # reproject
        #=======================================================================
        dem0_fp = self.warpreproject(dem_raw_fp, 
                 resolution=resolution,
                 crsOut=self.qproj.crs(),
                   #compression=compress,
                   nodata_val=-9999,
                   #output=ofp,
                   logger=log
                   )
                
        #=======================================================================
        # extrapolate
        #=======================================================================
        if fillNoData>0:

            log.info('filling NoData w/ dist=%i'%fillNoData)
            #ofp2 = self.extrapNoData(ofp1, dist=fillNoData, logger=log)['OUTPUT']
            
            """switched to whitebox (ignores edges)"""
            dem1_fp = Whitebox(out_dir=self.temp_dir, logger=log).fillMissingData(
                dem0_fp, dist=fillNoData)
            
 
 
        else:
            dem1_fp = dem0_fp
        

        
        #=======================================================================
        # clip and compress
        #=======================================================================
        log.debug('clipping and compressing raster')


        _ = self.cliprasterwithpolygon(dem1_fp,
                                       aoi_fp, 
                               #outResolution=outResolution,
                               output=ofp, 
                               #crsOut=aoi_vlay.crs(), #reproject in aoi's native crs
                                logger=log, 
 
                                dataType=6,
                                options=self.compress
                                )

        
        log.info('finished w/ clipped HRDEM (%s) \n    %s'%(self.qproj.crs().authid(),
                                                            ofp))
        
        #=======================================================================
        # wrap
        #=======================================================================

        return ofp
    

        


#===============================================================================
# FUNCTIONS-----------------
#===============================================================================
from memory_profiler import profile
@profile(precision=2)
def get_clip(
        resolution=4,
        #aoi_fp=r'C:\LS\02_WORK\02_Mscripts\InsuranceCurves\04_CALC\CMM\aoi\aoi02_CMM_20210711.gpkg',#37mins
        
        #EPSG:3936
        #aoi_fp=r'C:\LS\02_WORK\02_Mscripts\InsuranceCurves\04_CALC\CMM\aoi\test_aoi.gpkg', #1min 180Mb
        
        #small lower left test aoi
        aoi_fp=r'C:\LS\02_WORK\02_Mscripts\InsuranceCurves\04_CALC\CMM\aoi\aoi_t2_CMM_20210716.gpkg',
        #aoi_fp=r'C:\LS\02_WORK\02_Mscripts\InsuranceCurves\04_CALC\CMM\aoi\aoi_t1_CMM_20210716.gpkg',
        
        name='CMM'
        ):
    
    
    with HRDEMses(name=name, compress='med', overwrite=True, aoi_set_proj_crs=True, aoi_fp=aoi_fp) as wrkr:
    
        ofp = wrkr.get_hrdem(resolution=resolution)
        
 
    
    return ofp

def dl(
        aoi_fp=r'C:\LS\02_WORK\02_Mscripts\InsuranceCurves\04_CALC\CMM\aoi\aoi_t2_CMM_20210716.gpkg',
        name='CMM'
        ):
    
    
    with HRDEMses(name=name, compress='med') as wrkr:
    
        wrkr.retrieve_url(aoi_fp)
 

if __name__ =="__main__": 
    start =  datetime.datetime.now()
    print('start at %s'%start)


    get_clip()
    #dl()

    
    

    
    tdelta = datetime.datetime.now() - start
    print('finished in %s'%tdelta)
