'''
Created on Mar. 27, 2021

@author: cefect

workflows for deriving gridded depth estimates from inundation polygons and DEMs

 
 

#===============================================================================
# passing layers vs. filepaths
#===============================================================================
some algos natively input/output QgsLayers and others filepaths
    filepaths are easier to de-bug (can open in QGIS)
        lower memory requirements
    QgsLayers are easier to clean and code (can extract info)
    
PROCEDURE GUIDE
    passing inputs between functions (excluding coms and _helpers): 
        filepaths
    layers within a function
        QgsLayers or filepaths
        
        
#===============================================================================
# TODO
#===============================================================================
switch to default input as raster

change variable names to be more geneirc (less Canadian)

paralleleize expensive loops

add some better logic checks (%inundation)

create a master parameter list 
    explan function/roll of each parameter
    which functions use the parameter
    default value

better organize outputs
    temps should be a single folder
    intermediaries a second
    only the main depths output lands at the top level

add tests


'''
#===============================================================================
# imports-----------
#===============================================================================
import os, datetime, copy, shutil, gc, pickle
import pandas as pd
import numpy as np
 
start =  datetime.datetime.now()

 
import processing

from hp.exceptions import Error, assert_func
from hp.dirz import force_open_dir

 
#from hp.plot import Plotr #only needed for plotting sessions
from hp.Q import Qproj, QgsCoordinateReferenceSystem, QgsMapLayerStore, \
    QgsRasterLayer, QgsWkbTypes, vlay_get_fdf, QgsVectorLayer, vlay_get_fdata, \
    vlay_get_geo, QgsMapLayer, view
    
from hp.oop import Session as baseSession
     
from ricorde.tcoms import TComs
from hp.gdal import get_nodata_val, rlay_to_array
from hp.whitebox import Whitebox


#===============================================================================
# CLASSES----------
#===============================================================================
        
        
class Session(TComs, baseSession):
    """
    session for RICorDE calcs
        for downloading data, see data_collect
    
    """
    
 
    afp_d = {}
    #special inheritance parameters for this session
    childI_d = {'Session':['aoi_vlay', 'name', 'layName_pfx', 'fp_d', 'dem_psize',
                           'hval_prec', ]}
    
    smry_d = dict() #container of frames summarizing some calcs
    meta_d = dict() #1d summary data (goes on the first page of the smry_d)_
    
    def __init__(self, 
                 tag='tag',
                 aoi_fp = None, #optional area of interest polygon filepath
                 dem_fp=None, #dem rlay filepath
                 pwb_fp=None, #permanent water body filepath (raster or polygon)
                 inun_fp=None, #inundation filepath (raster or polygon)
             
                 exit_summary=True,
                 
                 **kwargs):
        
        #=======================================================================
        # #retrieval handles----------
        #=======================================================================
 
        data_retrieve_hndls = {
            'dem':{
                'compiled':lambda **kwargs:self.load_dem(**kwargs), #only rasters
                'build':lambda **kwargs:self.build_dem(**kwargs),
                },
            'pwb_rlay':{ #permanent waterbodies (raster)
                'compiled':lambda **kwargs:self.rlay_load(**kwargs), #only rasters
                'build': lambda **kwargs:self.build_rlay(pwb_fp, **kwargs), #rasters or gpkg
                },
            'inun_rlay':{ #flood inundation observation
                'compiled':lambda **kwargs:self.rlay_load(**kwargs), #only rasters
                'build': lambda **kwargs:self.build_rlay(inun_fp, **kwargs), #rasters or gpkg
                },
            'dem_hyd':{
                'compiled':lambda **kwargs:self.rlay_load(**kwargs),
                'build': lambda **kwargs:self.build_dem_hyd(**kwargs),
                },
            'HAND':{
                'compiled':lambda **kwargs:self.rlay_load(**kwargs),
                'build': lambda **kwargs:self.build_hand(**kwargs),
                },
            'HAND_mask':{
                'compiled':lambda **kwargs:self.rlay_load(**kwargs),
                'build':lambda **kwargs:self.build_hand_mask(**kwargs),
                },
            'inun1':{
                'compiled':lambda **kwargs:self.rlay_load(**kwargs),
                'build':lambda **kwargs:self.build_inun1(**kwargs),
                },
            'beach1':{
                'compiled':lambda **kwargs:self.rlay_load(**kwargs),
                'build':lambda **kwargs:self.build_beach1(**kwargs),
                },
            'b1Bounds':{
                'compiled':lambda **kwargs:self.load_pick(**kwargs),
                'build':lambda **kwargs:self.build_b1Bounds(**kwargs),
                },
            'inunHmax':{
                'compiled':lambda **kwargs:self.rlay_load(**kwargs),
                'build':lambda **kwargs:self.build_hmax(**kwargs),
                },
            'inun2':{
                'compiled':lambda **kwargs:self.rlay_load(**kwargs),
                'build':lambda **kwargs:self.build_inun2(**kwargs),
                },
            'beach2':{
                'compiled':lambda **kwargs:self.vlay_load(**kwargs),
                'build':lambda **kwargs:self.build_beach2(**kwargs),
                },
            'hgRaw':{
                'compiled':lambda **kwargs:self.rlay_load(**kwargs),
                'build':lambda **kwargs:self.build_hgRaw(**kwargs),
                },
            'hgSmooth':{
                'compiled':lambda **kwargs:self.rlay_load(**kwargs),
                'build':lambda **kwargs:self.build_hgSmooth(**kwargs),
                },
            'hInunSet':{
                'compiled':lambda **kwargs:self.load_pick(**kwargs),
                'build':lambda **kwargs:self.build_hiSet(**kwargs),
                },
            'hWslSet':{
                'compiled':lambda **kwargs:self.load_pick(**kwargs),
                'build':lambda **kwargs:self.build_hwslSet(**kwargs),
                },
            'wslMosaic':{
                'compiled':lambda **kwargs:self.rlay_load(**kwargs),
                'build':lambda **kwargs:self.build_wsl(**kwargs),
                },
            'depths':{
                'compiled':lambda **kwargs:self.rlay_load(**kwargs),
                'build':lambda **kwargs:self.build_depths(**kwargs),
                },
             
            }
        
        #attach inputs
        self.dem_fp, self.pwb_fp, self.inun_fp = dem_fp, pwb_fp, inun_fp
        self.exit_summary=exit_summary 
            
        
        super().__init__(tag=tag, 
                         data_retrieve_hndls=data_retrieve_hndls,
                         #prec=prec,
                         **kwargs)
        
 
        #special aoi
        """for the top-level session... letting the aoi set the projection"""
        if not aoi_fp is None:
            #get the aoi
            aoi_vlay = self.load_aoi(aoi_fp, reproj=True)
            self.aoi_fp = aoi_fp
        """
        self.out_dir
        """
 
    

    #===========================================================================
    # PHASE0: Data Prep---------
    #===========================================================================

        
    def run_dataPrep(self, #clean and load inputs into memory
                     ):
        #=======================================================================
        # defaults
        #=======================================================================
        log = self.logger.getChild('rDataPrep')

        start =  datetime.datetime.now()
        self.clear_all() #release everything from memory and reset the data containers
        #=======================================================================
        # execute
        #=======================================================================
        
        self.retrieve('dem')
 
        self.retrieve('pwb_rlay')
 
        self.retrieve('inun_rlay')
        
        #=======================================================================
        # wrap
        #=======================================================================
        self.mstore_log(logger=self.logger.getChild('rDataPrep'))
        
        assert len(self.mstore.mapLayers())==3, 'count mismatch in mstore'
        
        tdelta = datetime.datetime.now() - start
        
        log.info('finished in %s'%tdelta)
        
 

    def build_dem(self, #checks and reprojection on the DEM
                  dem_fp=None,
                  
                  #parameters
                  resolution=None, #optional resolution for resampling the DEM
                  aoi_vlay=None,
                  
                  
                  #gen
                  dkey=None,write=None,overwrite=None,
                  ):
        """
        user can pass a pixel size, or we reproject to the nearest integer
        see also self.build_rlay()
        """
        #=======================================================================
        # defaults
        #=======================================================================
        log = self.logger.getChild('build_dem')
        if write is None: write=self.write
        if overwrite is None: overwrite=self.overwrite
        assert dkey =='dem'
        if dem_fp is None: dem_fp=self.dem_fp
        if aoi_vlay is None: aoi_vlay=self.aoi_vlay
        
        if not resolution is  None:
            assert isinstance(resolution, int), 'got bad pixel size request on the dem (%s)'%resolution
            
        layname, ofp = self.get_outpars(dkey, write)
        
        mstore=QgsMapLayerStore()
        
        #=======================================================================
        # load
        #=======================================================================
        rlay_raw= self.rlay_load(dem_fp, logger=log)
        
        #=======================================================================
        # warp----------
        #=======================================================================
        #=======================================================================
        # config resolution
        #=======================================================================
        psize_raw = self.rlay_get_resolution(rlay_raw)
        
        #use passed pixel size
        if not resolution is None: #use passed
            assert resolution>=psize_raw, 'only downsampling allowed'
 
        #use native
        else:
            resolution=int(round(psize_raw, 0))
            
        resample=False
        if not psize_raw == resolution:
            resample=True
        
        meta_d = {'raw_fp':dem_fp, 'resolution':resolution, 'psize_raw':psize_raw}
        
        #=======================================================================
        # execute
        #=======================================================================
        
        rlay1, d = self.rlay_warp(rlay_raw, ref_lay=None, aoi_vlay=aoi_vlay, decompres=True,
                               resample=resample, resolution=resolution, resampling='Average', ofp=ofp,
                               logger=log)
            
 
        
        meta_d.update(d)

        #=======================================================================
        # #use loader to attach common parameters
        #=======================================================================
        """for consistency between compiled loads and builds"""
        self.load_dem(rlay=rlay1, logger=log, dkey=dkey)
        #=======================================================================
        # wrap
        #=======================================================================
        if self.exit_summary: self.smry_d[dkey] = pd.Series(meta_d).to_frame()
        if write: self.ofp_d[dkey] = ofp
        mstore.removeAllMapLayers()
 
        
        return rlay1
    
    def load_dem(self,
                 fp=None,
                 rlay=None, 
                 logger=None,
                 dkey=None,
                 write=None,
                 ):
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        log = logger.getChild('load_dem')
        if write is None: write=self.write
        assert dkey=='dem'
 
        #=======================================================================
        # load
        #=======================================================================
        if rlay is None:
            if fp is None:
                fp = self.dem_fp
            
            rlay = self.rlay_load(fp, logger=log)
            
        assert isinstance(rlay, QgsRasterLayer), type(rlay)
        #=======================================================================
        # attach parameters
        #=======================================================================
        dem_psize = self.rlay_get_resolution(rlay)
        
        assert round(dem_psize, 0)==dem_psize, 'got bad resolution on dem: %s'%dem_psize
        
        self.dem_psize = int(dem_psize)
        
        log.info('loaded %s w/ dem_psize=%.2f'%(rlay.name(), dem_psize))
        
        return rlay 
 
        
        
                  
    


    
    

    def build_rlay(self, #build raster from some water polygon
                        fp,
                        dkey=None,
                        write=None,
                        ref_lay=None,
                        aoi_vlay=None,
                        
                        resampling='Maximum', #resampling method
                        
                        clean_inun_kwargs={},
                        ):
        """
        see also self.build_dem()
        """
 
        #=======================================================================
        # defaults
        #=======================================================================
        log = self.logger.getChild('build_rlay.%s'%dkey)
        if write is None: write=self.write
        assert dkey in ['pwb_rlay', 'inun_rlay']
        
        assert not fp is None, dkey
        assert os.path.exists(fp), fp
        if aoi_vlay is None: aoi_vlay=self.aoi_vlay
        #load the reference layer
        if ref_lay is None:
            ref_lay = self.retrieve('dem', logger=log)
            
        dem_psize = self.dem_psize
        
        mstore = QgsMapLayerStore()
        
        #output
        layname, ofp = self.get_outpars(dkey, write)

        meta_d = {'dkey':dkey,'raw_fp':fp,'ref_lay':ref_lay.source(),'aoi_vlay':aoi_vlay}
        #=======================================================================
        # raster-------
        #=======================================================================
        if fp.endswith('tif'):
            rlay_fp = fp
        
        #=======================================================================
        # polygon--------
        #=======================================================================
        else:
 
            vlay_raw = self.vlay_load(fp, logger=log)
            #===================================================================
            # #pre-clip
            #===================================================================
 
            if aoi_vlay is None:
                """TODO: build polygon aoi from ref layer extents
                check if extents match"""
                raise IOError('not implemented')
            else:
                clip_vlay = aoi_vlay
            
            if not clip_vlay is None:
                mstore.addMapLayer(vlay_raw)
                vlay1 = self.slice_aoi(vlay_raw, aoi_vlay=aoi_vlay, logger=log,
                        method='clip', #avoidds breaking any geometry (we clip below anyway)... no... leaves too much
                                        )
            else:
                vlay1=vlay_raw
 
                
            #===================================================================
            # #cleaning
            #===================================================================
            vlay2_fp, d = self.clean_inun_vlay(vlay1, logger=log,
                                 output=os.path.join(self.temp_dir, '%s_clean_inun.gpkg'%(vlay_raw.name())),
                                                  **clean_inun_kwargs)
            
            meta_d.update(d)
 
 
            #===================================================================
            # #build the raster 
            #===================================================================
            rlay_fp = self.rasterize_inun(vlay2_fp, logger=log, ref_lay=ref_lay,
                                          ofp=os.path.join(self.temp_dir, '%s_clean_inun.tif'%(vlay_raw.name()))
                                          )
            
            log.debug('\'%s\' saved to \n    %s'%(dkey, rlay_fp))
            
            

        #=======================================================================
        # warp-----
        #=======================================================================
        
        rlay1, d = self.rlay_warp(rlay_fp, ref_lay=ref_lay, aoi_vlay=aoi_vlay, decompres=False,
                                     resampling=resampling, logger=log, ofp=ofp)
        
        
        meta_d.update(d)
        
        #=======================================================================
        # checks
        #=======================================================================
        assert_func(lambda:self.rlay_check_match(rlay1, ref_lay, logger=log))
        
        assert_func(lambda:  self.mask_check(rlay1), msg=dkey)
        
 
        if dkey == 'pwb_rlay':
            assert_func(lambda: self.check_pwb(rlay1))
        
        #=======================================================================
        # wrap
        #=======================================================================
        if self.exit_summary:
            assert not dkey in self.smry_d
            self.smry_d[dkey] = pd.Series(meta_d).to_frame()
        
 
        log.info('finished on %s'%rlay1.name())
        mstore.removeAllMapLayers()
        
        if write: self.ofp_d[dkey] = ofp
        
        return rlay1
    """
    self.mstore_log()
    """
    def rlay_warp(self,  #special implementation of gdalwarp processing tools
                  input_raster, #filepath or layer
                   ref_lay=None,
                   aoi_vlay=None,
                   
                   clip=None, reproj=None, resample=None,decompres=None,
                   
                   #parameters
                   resampling='Maximum', #resampling method
                   compress=None,
                   resolution=None,
                   
                  
                  logger=None, ofp=None,
                  ):
        
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        log=logger.getChild('rlay_warp')
        
        
        #=======================================================================
        # retrieve
        #=======================================================================
        rlay_raw = self.get_layer(input_raster, logger=log)
        
        if ofp is None: ofp=os.path.join(self.temp_dir, '%s_warp.tif'%rlay_raw.name())
        
 
            
        if aoi_vlay is None: aoi_vlay=self.aoi_vlay
        
        """I guess were allowing this...
        assert ref_lay.extent()==aoi_vlay.extent()"""
        
        mstore = QgsMapLayerStore()
        #=======================================================================
        # parameters
        #=======================================================================
        if clip is None:
            if aoi_vlay is None:
                """get the ndb from the dem"""
                raise Error('not implemnted')
            else:
                clip = True
             
        if reproj is None:   
            reproj = False
            if not rlay_raw.crs() == self.qproj.crs():
                reproj = True
                
        if decompres is None:
            decompres=False
            if not self.getRasterCompression(rlay_raw.source()) is None:
                decompres = True
            
        
        if decompres:
            if compress is None: 
                compress='none'
                
        if compress is None: compress=self.compress
            
        meta_d = {'crs':rlay_raw.crs().authid, 'compress':compress}
        #=======================================================================
        # clip or reproject----
        #=======================================================================
        """because we want to specify custom resampling... doing this in two stages"""
        if clip or reproj or decompres:
            #===================================================================
            # defaults
            #===================================================================
            msg = 'warping (%s) ' % (rlay_raw.name())
            if clip:
                msg = msg + ' +clipping extents to %s' % aoi_vlay.name()
            if reproj:
                msg = msg + ' +reproj to %s' % self.qproj.crs().authid()
                
            if decompres:
                msg = msg + ' +decompress'
                
            log.info(msg)
            mstore.addMapLayer(rlay_raw)
            #===================================================================
            # clip and reproject
            #===================================================================
            """custom cliprasterwithpolygon"""
            ins_d = {'ALPHA_BAND':False, 
                'CROP_TO_CUTLINE':clip, 
                'DATA_TYPE':6, #float32
                'EXTRA':'', 
                'INPUT':rlay_raw, 
                'MASK':aoi_vlay, 
                'MULTITHREADING':True, 
                'NODATA':-9999, 
                'OPTIONS':self.compress_d[compress], #no compression
                'OUTPUT':'TEMPORARY_OUTPUT', 
                'KEEP_RESOLUTION':False, #will ignore x and y res
                'SET_RESOLUTION':False, 
                'X_RESOLUTION':None, 
                'Y_RESOLUTION':None, 
                'SOURCE_CRS':None, 
                'TARGET_CRS':self.qproj.crs()}
            algo_nm = 'gdal:cliprasterbymasklayer'
            log.debug('executing \'%s\' with ins_d: \n    %s \n\n' % (algo_nm, ins_d))
            res_d = processing.run(algo_nm, ins_d, feedback=self.feedback)
            log.debug('finished w/ \n    %s' % res_d)
            if not os.path.exists(res_d['OUTPUT']):
                """failing intermittently"""
                raise Error('failed to get a result')
            rlay2 = res_d['OUTPUT']
        else:
            rlay2 = rlay_raw
        
        rlay2 = self.get_layer(rlay2)
        #=======================================================================
        # resample-----
        #=======================================================================

            
            
        reso_raw = self.rlay_get_resolution(rlay2)
        
        if resolution is None:
            assert not ref_lay is None
            resolution = self.rlay_get_resolution(ref_lay)
            
        if resample is None:
            #===================================================================
            # get parameters
            #===================================================================

            meta_d.update({'reso_raw':reso_raw, 'reso_ref':resolution})
 
            resample = False
            if not resolution == reso_raw:
                resample = True
                
            """seems to trip often because the aoi extents dont match the dem?"""
            if not ref_lay is None:
                if not rlay2.extent()==ref_lay.extent():
                    resample=True
                
        #===================================================================
        # resample
        #===================================================================
        if resample:
            log.info('resampling from %.4f to %.2f w/ %s'%(reso_raw, resolution, resampling))
            
            if not ref_lay is None:
                extents=ref_lay.extent()
            else:
                extents=None
            
            rlay3 = self.warpreproject(rlay2, resolution=int(resolution), compress=compress, 
                resampling=resampling, logger=log, extents=extents,
                output=ofp)
            if isinstance(rlay2, QgsMapLayer):mstore.addMapLayer(rlay2)
        else:
            rlay3 = self.get_layer(rlay2, logger=log)
            self.rlay_write(rlay3, ofp=ofp, logger=log)
            
        #=======================================================================
        # checks
        #=======================================================================
 
        
        return self.get_layer(rlay3, logger=log), meta_d
    #===========================================================================
    # PHASE0: Build HAND---------
    #===========================================================================
    def run_HAND(self,
                 logger=None,
                 ):
        """TODO: release dem_hyd?"""
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        log=logger.getChild('rHand')
        start =  datetime.datetime.now()
        self.clear_all() #release everything from memory and reset the data containers
        #=======================================================================
        # run
        #=======================================================================
        dem = self.retrieve('dem', logger=log) #just for checking
        
        #hydrocorrected DEM
        dem_hyd = self.retrieve('dem_hyd', logger=log)
        
        #get the hand layer
        hand_rlay = self.retrieve('HAND', logger=log)  
        
        #nodata boundary of hand layer (polygon)
        hand_mask = self.retrieve('HAND_mask', logger=log) 
        
        #=======================================================================
        # wrap
        #=======================================================================
        assert_func(lambda:  self.rlay_check_match(hand_rlay,dem, logger=log))
        
        tdelta = datetime.datetime.now() - start
        
        log.info('finished in %s'%tdelta)
        
        return
    
    def build_dem_hyd(self, #hydraulically corrected DEM
                      dem_rlay=None,
                      
                      #parameters
                      dist=None, #Maximum search distance for breach paths in cells
                      
                      #generals
                      dkey=None, logger=None,write=None,
                      ):
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        if write is None: write=self.write
        log=logger.getChild('build_dem_hyd')
        assert dkey=='dem_hyd'
        

        
        if dem_rlay is None:
            dem_rlay = self.retrieve('dem')
            
        if dist is None:
            #2km or 100 cells
            dist = min(int(2000/self.dem_psize), 100)
        
        assert isinstance(dist, int)
 
        
        #output
        layname = '%s_%s'%(self.layName_pfx, dkey) 
        if write:
            ofp = os.path.join(self.wrk_dir, layname+'.tif')
            self.ofp_d[dkey] = ofp
        else:
            ofp=os.path.join(self.temp_dir, layname+'.tif')
            
        if os.path.exists(ofp):
            assert self.overwrite
            os.remove(ofp)
            
        #=======================================================================
        # precheck
        #=======================================================================
        #check compression
        assert self.getRasterCompression(dem_rlay.source()) is None, 'dem has some compression: %s'%dem_rlay.name()
        
        #=======================================================================
        # execute
        #=======================================================================
        ofp = Whitebox(out_dir=self.temp_dir, logger=logger
                 ).breachDepressionsLeastCost(dem_rlay.source(), dist=dist, ofp=ofp)
                 
        #=======================================================================
        # wrap
        #=======================================================================
        assert self.getRasterCompression(ofp) is None, 'result has some compression: %s'%ofp
        
        rlay = self.rlay_load(ofp, logger=log)
        
        assert_func(lambda:  self.rlay_check_match(rlay, dem_rlay, logger=log), msg=dkey)
        
        if self.exit_summary:
 
            self.smry_d[dkey] = pd.Series({'search_dist':dist}).to_frame()
        
        log.info('finished on %s'%rlay.name())
 
        
        return rlay
            
            
     
    def build_hand(self, #load or build the HAND layer
                   dkey=None,
                   
                   demH_rlay=None, #hydro corrected DEM
                   pwb_rlay=None,
                   
                   write=None,
                   logger=None,

                 ):

        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        log=logger.getChild('build_hand')
        assert dkey == 'HAND'
        #if dem_fp is None: dem_fp=self.dem_fp
        if write is None: write=self.write
        
        layname, ofp = self.get_outpars(dkey, write)
        #=======================================================================
        # retrieve
        #=======================================================================
        if pwb_rlay is None:
            pwb_rlay = self.retrieve('pwb_rlay')
             

        if demH_rlay is None:
            demH_rlay = self.retrieve('dem_hyd')

        
        pwb_fp = pwb_rlay.source()
        dem_fp = demH_rlay.source() 
        
        log.info('on %s'%{'pwb_fp':pwb_fp, 'dem_fp':dem_fp})
        
        #=======================================================================
        # precheck
        #=======================================================================
        
        assert self.getRasterCompression(dem_fp) is None, 'dem has some compression: %s'%dem_fp
        
        #=======================================================================
        # execute
        #=======================================================================
        Whitebox(out_dir=self.out_dir, logger=logger
                 ).elevationAboveStream(dem_fp, pwb_fp, out_fp=ofp)
        
        
            
        #=======================================================================
        # wrap 
        #=======================================================================
        rlay = self.rlay_load(ofp, logger=log)
        
        assert_func(lambda:  self.rlay_check_match(rlay,demH_rlay, logger=log))
        
        if write:self.ofp_d[dkey] = ofp
            
 
        return rlay
    
    
    def build_hand_mask(self, #get the no-data boundary of the HAND rlay (as a vector)
                dkey=None,
                  hand_rlay=None,
                  #stream_fp='',
                  logger=None,
                  write=None,
                  ):
        
        """
        TODO: try and simplify this
        """
        #=======================================================================
        # defautls
        #=======================================================================
        if logger is None: logger=self.logger
        log=logger.getChild('build_hand_mask')
        if write is None: write=self.write
        
        assert dkey=='HAND_mask'
        
        if hand_rlay is None:
            hand_rlay = self.retrieve('HAND')
 
        #===================================================================
        # #setup
        #===================================================================
        log.info('building \'%s\' on \'%s\''%(dkey, hand_rlay.name()))
        
 
        if write:
            ofp = os.path.join(self.wrk_dir, '%s_%s.tif'%(self.layName_pfx, dkey))
        else:
            ofp = os.path.join(self.temp_dir, '%s_%s.tif'%(self.layName_pfx, dkey))
            
 
        
        
        #=======================================================================
        # #convert raster to binary
        #=======================================================================
        """NOTE: HAND layers have zero values on the streams
            using zero_shift"""
        self.mask_build(hand_rlay, zero_shift=True, logger=log, 
                                   #ofp=os.path.join(self.temp_dir, 'hand_mask1.tif'),
                                   ofp = ofp
                                   )
 
        rlay = self.rlay_load(ofp, logger=log)
        
 
        #=======================================================================
        # check
        #=======================================================================
        assert_func(lambda:  self.rlay_check_match(rlay,hand_rlay, logger=log))
        
        assert_func(lambda:  self.mask_check(rlay))
            
 
        #=======================================================================
        # wrap
        #=======================================================================
 
        #log.info('got \'%s\': \n    %s'%(fp_key, ofp))
        if write:self.ofp_d[dkey] = ofp
        
        return rlay
    
    
    #===========================================================================
    # PHASE1: Inundation Correction---------
    #===========================================================================
    def run_imax(self,
 
                 ):
        #=======================================================================
        # defaults
        #=======================================================================
        logger=self.logger
        log=logger.getChild('rImax')
        start =  datetime.datetime.now()
        
        self.clear_all() #release everything from memory and reset the data containers
        
        dem_rlay = self.retrieve('dem', logger=log) #just for checking
 
        #=======================================================================
        # add minimum water bodies to FiC inundation
        #=======================================================================
 
        
        inun1_rlay = self.retrieve('inun1')
        
        #=======================================================================
        # get hydrauilc maximum
        #=======================================================================
        #get initial HAND beach values
        beach1_rlay=self.retrieve('beach1')
 
        #get beach bounds
        beach1_bounds = self.retrieve('b1Bounds')
        
        #get hydrauilc maximum
        inun_hmax = self.retrieve('inunHmax')
        

        #=======================================================================
        # reduce inun by the hydrauilc maximum
        #=======================================================================
        #clip inun1 by hydrauilc  maximum (raster)
        inun2_rlay = self.retrieve('inun2') 
        

        #=======================================================================
        # wrap
        #=======================================================================
        assert_func(lambda:  self.rlay_check_match(inun2_rlay,dem_rlay, logger=log))
        
        tdelta = datetime.datetime.now() - start
        
        log.info('finished in %s'%tdelta)
        
        return tdelta
    
    
        

    def build_inun1(self, #merge NHN and FiC and crop to DEM extents
            
            
            #layer inputs
            pwb_rlay = None,
            inun_rlay=None,
            HAND_mask=None,
 
              
              
              #parameters
              buff_dist=None, #buffer to apply to perm water
 
              
              #misc
              logger=None,
              write=None, dkey = None,
              ):
 
        """
        currently setup for inun1 as a vectorlayer
            see note on hand_inun.Session.get_edge_samples()
            
        just spent a few hours re-coding the inun1 so it returns a raster
            I prefer working with rasters here
            
        Id like to see some real data before deciding how to proceed 
            
        """
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        log=logger.getChild('b.inun1')
        assert dkey == 'inun1'
        if write is None: write=self.write
        
        layname, ofp = self.get_outpars(dkey, write)
        
        #buff_dist
        if not isinstance(buff_dist, int):
            assert buff_dist is None
            if self.dem_psize is None:
                self.retrieve('dem')
            assert isinstance(self.dem_psize, int), 'got bad type on dem_psize: %s'%type(self.dem_psize)
            buff_dist = self.dem_psize*2
        #=======================================================================
        # retrieve
        #=======================================================================
        if pwb_rlay is None:
            pwb_rlay = self.retrieve('pwb_rlay')
        
        if inun_rlay is None:
            inun_rlay = self.retrieve('inun_rlay')
            
        if HAND_mask is None:
            HAND_mask=self.retrieve('HAND_mask')
            
        #=======================================================================
        # build
        #=======================================================================
 
        log.info('building \'%s\' from \n    %s'%(dkey,{'pwb_rlay':pwb_rlay.name(), 'inun_rlay':inun_rlay.name()}))
        
        assert_func(lambda:  self.rlay_check_match(pwb_rlay,inun_rlay, logger=log))
        assert_func(lambda:  self.rlay_check_match(pwb_rlay,HAND_mask, logger=log))
            
 
        #===================================================================
        # buffer
        #===================================================================
        """nice to add a tiny buffer to the waterbodies to ensure no zero hand values
        can be dangerous when the pwb has lots of noise"""

        #raw buffer (buffered cells have value=2)
        if buff_dist>0:
            #grass buffer
            pwb_buff1_fp = self.rBuffer(pwb_rlay, logger=log, dist=buff_dist,
                                        output = os.path.join(self.temp_dir, '%s_buff1.tif'%pwb_rlay.name()))
            
            #get back on same extents
            pwb_buff2_fp = self.warpreproject(pwb_buff1_fp, extents=pwb_rlay.extent(), logger=log)
 
            
            assert_func(lambda:  self.rlay_check_match(pwb_buff2_fp,pwb_rlay, logger=log))
            
            #convert to a mask again
            pwb_buff3_fp = self.mask_build(pwb_buff2_fp, logger=log)
            
            assert_func(lambda:  self.rlay_check_match(pwb_buff3_fp,pwb_rlay, logger=log))
        else:
            pwb_buff3_fp = pwb_rlay
        
 
        
        #=======================================================================
        # merge inundation and pwb
        #=======================================================================
        inun1_1_fp = self.mask_combine([pwb_buff3_fp, inun_rlay], logger=log,
                                     ofp = os.path.join(self.temp_dir, 'inun1_1.tif'))
        
        
        #===================================================================
        # crop to HAND extents
        #===================================================================
        self.mask_apply(inun1_1_fp, HAND_mask, logger=log, ofp=ofp)
 
        rlay = self.rlay_load(ofp, logger=log)
        
        #=======================================================================
        # check
        #=======================================================================
        assert_func(lambda:  self.rlay_check_match(rlay,HAND_mask, logger=log))
        
        assert_func(lambda:  self.mask_check(rlay))
        
        #===================================================================
        # #wrap
        #===================================================================
 
        self.ofp_d[dkey] = ofp
        
        if self.exit_summary:
 
            self.smry_d[dkey] = pd.Series({'buff_dist':buff_dist}).to_frame()
        
        
 
        log.info('for \'%s\' built: \n    %s'%(dkey, ofp))

        return rlay
    
    def build_beach1(self, #raster of beach values (on HAND layer)
            #inputs
            hand_rlay=None,
            #handM_rlay=None,
            inun1_rlay=None,
            
    
                
              #generals
              dkey=None,
              logger=None,write=None,
             ):
        """
        (rToSamp_fp=hand_fp, 
        inun_fp=inun1_fp,
        #                                 ndb_fp=ndb_fp, sample_spacing=sample_spacing)
        """
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        if write is None: write=self.write
        log=logger.getChild('b.%s'%dkey)
        assert dkey=='beach1'
 
        layname, ofp = self.get_outpars(dkey, write)
        

            
        #=======================================================================
        # retrieve
        #=======================================================================
        if hand_rlay is None:
            hand_rlay=self.retrieve('HAND')
 
        if inun1_rlay is None:
            inun1_rlay=self.retrieve('inun1')
            
 

        #=======================================================================
        # execute
        #=======================================================================
        self.get_beach_rlay(
            inun_rlay=inun1_rlay, base_rlay=hand_rlay, logger=log, ofp=ofp)
            
            
 
        #=======================================================================
        # wrap
        #=======================================================================
        rlay = self.rlay_load(ofp, logger=log)
        
        assert_func(lambda:  self.rlay_check_match(rlay,hand_rlay, logger=log))
        
        
        if write:
            self.ofp_d[dkey] = ofp
 
        
        return rlay
    
    def get_beach_rlay(self, #raster along edge of inundation where values match some base layer
                       inun_rlay=None,
                       base_rlay=None,
                        logger=None,
                        ofp=None,
                        ):
        """
        see hand_inun.Session.get_edge_samples() for vector based implementation
        """
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
 
        log=logger.getChild('get_beach_rlay')
        
        assert isinstance(base_rlay, QgsRasterLayer)
        
        log.info('w/ inun \'%s\' and base \'%s\''%(inun_rlay.name(), base_rlay.name()))
        
        pixel_size = self.rlay_get_resolution(inun_rlay)
        
        #=======================================================================
        # build sampling mask
        #=======================================================================
        #buffer 1 pixel
        buff_fp = self.rBuffer(inun_rlay, logger=log, dist=pixel_size)
        
        #reproject get back on same extents
        buff_f2 = self.warpreproject(buff_fp, extents=inun_rlay.extent(), logger=log)
        
        #retrieve buffered pixels
        mask1_fp = self.mask_build(buff_f2, logger=log, thresh=1.5, thresh_type='lower')
        
        #=======================================================================
        # sample the base
        #=======================================================================
        samp_fp = self.mask_apply(base_rlay, mask1_fp,logger=log, ofp=ofp) 
        
        log.debug('finsihed on %s'%samp_fp)
        
        return samp_fp
        
            
    def build_b1Bounds(self,
               beach1_rlay = None,

                          
                #hand value stats for bouding beach1 samples
               qhigh=0.9, cap=7.0,  #uppers               
               qlow=0.1, floor=0.5, #lowers
               
               #gen
              dkey=None, logger=None,write=None,
               ):
        
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        if write is None: write=self.write
        log=logger.getChild('b.%s'%dkey)
        

 
        assert dkey=='b1Bounds'
        
 
        layname, ofp = self.get_outpars(dkey, write)
        
        if beach1_rlay is None:
            beach1_rlay = self.retrieve('beach1')
        
        #=======================================================================
        # #get bounds
        #=======================================================================
        hv_d, meta_d = self.get_sample_bounds(beach1_rlay, qhigh=qhigh,cap=cap,
                                           qlow=qlow, floor=floor, logger=log)
        
        #=======================================================================
        # wrap
        #=======================================================================
 
        if self.exit_summary:
 
            self.smry_d[dkey] = pd.Series(meta_d).to_frame()
        
 
        
 
        log.info('for \'%s\' built: \n    %s'%(dkey, hv_d))
        if write:
            self.ofp_d[dkey] = self.write_pick(hv_d.copy(), 
                                   os.path.join(self.wrk_dir, '%s_%s.pickle'%(self.layName_pfx, dkey)), 
                                               logger=log)

        return hv_d
        
        
        
    
    def get_sample_bounds(self, #get the min/max HAND values to use (from sample stats)
                          beach_lay,
                          
                          #data parameters
                          qhigh=None, 
                          qlow=None,
                          
                          cap=None, #maxiomum hval to allow (overwrite quartile
                          floor = None, #0.5, #minimum
                          
                          coln=None,
                          logger=None,
                          prec=3,
                          ):
        """
        calculating these bounds each run
        """
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        log=logger.getChild('get_sample_bounds')


        if coln is None: coln = self.smpl_fieldName
        
        log.debug('on %s'% beach_lay.name())
        
        res_d = dict()
        
        #=======================================================================
        # points
        #=======================================================================
        if isinstance(beach_lay, QgsVectorLayer):
            """leaving for backwards compatability"""
            #vlay_raw = self.vlay_load(pts_fp, logger=log)
            
            #mstore.addMapLayer(vlay_raw)
            
            df_raw = vlay_get_fdf(beach_lay, logger=log)
            assert 'float' in df_raw[coln].dtype.name
            
            sraw = df_raw[coln].round(prec).copy()
            
        #=======================================================================
        # raster
        #=======================================================================
        elif isinstance(beach_lay, QgsRasterLayer):
            ar = rlay_to_array(beach_lay.source())
            sraw = pd.Series(ar.reshape((1, -1))[0], dtype=float).dropna().round(3).reset_index(drop=True)
 
            
            
        else:
            raise IOError('bad type: %s'%type(beach_lay))
 
        #===================================================================
        # upper bound
        #===================================================================
        if not qhigh is None:
            qh = sraw.quantile(q=qhigh)
            if qh > cap:
                log.warning('q%.2f (%.2f) exceeds cap (%.2f).. using cap'%(
                    qhigh, qh, cap))
                hv_max = cap
            else:
                hv_max=qh
            
            res_d['qhi'] = round(hv_max, 3)
        #=======================================================================
        # lower bound
        #=======================================================================
        if not qlow is None:
            ql = sraw.quantile(q=qlow)
            if ql < floor:
                log.warning('q%.2f (%.2f) is lower than floor (%.2f).. using floor'%(
                    qlow, ql, floor))
                hv_min = floor
            else:
                hv_min=ql
                
            res_d['qlo'] = round(hv_min, 3)
            
        #=======================================================================
        # wrap
        #=======================================================================
        log.info('got %s'%res_d)
        
 
            
        meta_d = {**{'qhigh':qhigh, 'qlow':qlow, 'cap':cap, 'floor':floor}, **res_d}
        
        return res_d, meta_d
    
    

    def build_hmax(self, #get the hydrauilc maximum inundation from sampled HAND values
                   
                   hand_rlay=None,
                   hval=None,
 
               
               #gen
              dkey=None, logger=None,write=None,
               ):
        
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        if write is None: write=self.write
        log=logger.getChild('b.%s'%dkey)
        

 
        assert dkey=='inunHmax'
        
 
        layname, ofp = self.get_outpars(dkey, write)
        
        #=======================================================================
        # retrieve
        #=======================================================================
 
        if hand_rlay is None:
            hand_rlay=self.retrieve('HAND')
            
        if hval is None:
            bounds=self.retrieve('b1Bounds')
            hval = bounds['qhi']

        
        #=======================================================================
        # get inundation
        #=======================================================================
 
        self.get_hand_inun(hand_rlay,hval, logger=log, ofp=ofp)
            

        #=======================================================================
        # wrap
        #=======================================================================
        rlay = self.rlay_load(ofp, logger=log)
        
        assert_func(lambda:  self.rlay_check_match(rlay,hand_rlay, logger=log))
        
        
        if self.exit_summary:
 
            self.smry_d[dkey] = pd.Series({'hval':hval}).to_frame()
        
        if write:self.ofp_d[dkey]=ofp
        
 
        log.info('for \'%s\' built: \n    %s'%(dkey, ofp))

        return rlay
    

    def build_inun2(self, #merge inun_2 with the max
                    inun1_rlay=None,
                    inun_hmax=None,
                    
                  
               #gen
              dkey=None, logger=None,write=None,
                  ):
 
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        if write is None: write=self.write
        log=logger.getChild('b.%s'%dkey)
        

 
        assert dkey=='inun2'
        
 
        layname, ofp = self.get_outpars(dkey, write)
 
        #=======================================================================
        # retrieve
        #=======================================================================
        if inun_hmax is None:
            inun_hmax=self.retrieve('inunHmax')
            
        if inun1_rlay is None:
            inun1_rlay = self.retrieve('inun1')
 
 
        log.info('maxFiltering \'%s\' with \'%s\''%(
            inun1_rlay.name(),
            inun_hmax.name()))
        
 
 
        
        #===================================================================
        # apply fillter
        #===================================================================
        self.inun_max_filter(inun_hmax.source(), inun1_rlay.source(),
                             ofp=ofp,logger=log)
            
 
        #=======================================================================
        # wrap
        #=======================================================================
        rlay = self.rlay_load(ofp, logger=log)
        
        assert_func(lambda:  self.rlay_check_match(rlay,inun1_rlay, logger=log))
 
        
        if write:self.ofp_d[dkey]=ofp
        
 
        log.info('for \'%s\' built: \n    %s'%(dkey, ofp))

        return rlay
    
    def xxxbuild_inun2_vlay(self,
                         inun2r_fp='',
                         clean_kwargs={}, 
                         logger=None,
                         ):
        
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        log=logger.getChild('b.inun2')

        fp_key = 'inun2_fp'
 
        
        #=======================================================================
        # build
        #=======================================================================
        if not fp_key in self.fp_d:
            #===================================================================
            # setup
            #===================================================================
            log.info('polygonize %s'%os.path.join(inun2r_fp))
 
            #filepaths
 
            ofp = os.path.join(self.out_dir, self.layName_pfx+'_inun2.gpkg')
            if os.path.exists(ofp): 
                assert self.overwrite
                os.remove(ofp)
            

            #===================================================================
            # convert to vlay
            #===================================================================
            """point sampler is set up for polygons...
                    would improve performance to re-do point sampler for all raster"""

            
            inun3_raw_vlay_fp = self.polygonizeGDAL(inun2r_fp,  logger=log)
            self.trash_fps.append(inun3_raw_vlay_fp)
 
            
            #===================================================================
            # clean
            #===================================================================
 
            self.clean_inun_vlay(inun3_raw_vlay_fp, output=ofp, logger=log,**clean_kwargs)
            
            #===================================================================
            # wrap
            #===================================================================
            self.ofp_d[fp_key]= ofp
            
        else:
            ofp = self.fp_d[fp_key]

        
        #=======================================================================
        # wrap
        #=======================================================================
         

        
        log.info('got  \'%s\' \n    %s'%(
               fp_key, ofp))
        
        return ofp
    
    #===========================================================================
    # PHASE2: Rolling HAND grid----------
    #===========================================================================
    
    def run_HANDgrid(self, #get mosaic of depths (from HAND values)

                  logger=None,
                  ):


        
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        log=logger.getChild('rHg')
        start =  datetime.datetime.now()
 
        self.clear_all() #release everything from memory and reset the data containers
 
        
        #=======================================================================
        # execute
        #=======================================================================
        dem_rlay = self.retrieve('dem', logger=log) #just for chcecking

        beach2_rlay = self.retrieve('beach2', logger=log)
        
        hgRaw_rlay = self.retrieve('hgRaw', logger=log)
        
        
        """
        a raster of smoothed HAND values
            this approximates the event HAND with rolling values
        
        """
        hvgrid = self.retrieve('hgSmooth', logger=log)
        
        #=======================================================================
        # wrap
        #=======================================================================
        
        assert_func(lambda:  self.rlay_check_match(hvgrid,dem_rlay, logger=log))
        
        tdelta = datetime.datetime.now() - start
        
        log.info('finished in %s'%tdelta)
        
        return 
 
        

    
        
    def build_beach2(self, #beach values (on inun2 w/ some refinement)
             
             #datalayesr
             hand_rlay=None,
             inun2_rlay=None,
             
             #parameters
             bounds=None,  # hi/low quartiles from beach1
             
               #gen
               write_csv=False,
              dkey=None, logger=None,write=None,
                  ):
 
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        if write is None: write=self.write
        log=logger.getChild('b.%s'%dkey)
        
 
 
        assert dkey=='beach2'
        
 
        layname, ofp = self.get_outpars(dkey, write, ext='.gpkg')
 
        #=======================================================================
        # retrieve
        #=======================================================================
        if bounds is None: 
            bounds = self.retrieve('b1Bounds')
            
        if hand_rlay is None:
            hand_rlay=self.retrieve('HAND')
 
        if inun2_rlay is None:
            inun2_rlay=self.retrieve('inun2')
            
        mstore=QgsMapLayerStore()
        
        
        #=======================================================================
        # get raw samples
        #=======================================================================
        samp_raw_fp = self.get_beach_rlay(
            inun_rlay=inun2_rlay, base_rlay=hand_rlay, logger=log)
        
        #convert to points
        fieldName='hvals'
        samp_raw_pts = self.pixelstopoints(samp_raw_fp, logger=log, fieldName=fieldName)
        
        mstore.addMapLayer(samp_raw_pts)
        samp_raw_pts.setName('%s_samp_raw'%dkey)
        #=======================================================================
        # cap samples
        #=======================================================================
        samp_cap_vlay, df, meta_d = self.get_capped_pts(samp_raw_pts, 
                                   logger=log, fieldName=fieldName,
                            vmin=bounds['qlo'], vmax=bounds['qhi'])
        #=======================================================================
        # wrap
        #=======================================================================
        
        
        
        if write:
            self.ofp_d[dkey] = self.vlay_write(samp_cap_vlay,ofp,  logger=log)
            
        if write_csv:
            """for plotting later"""
            ofp = os.path.join(self.out_dir, '%s_%s_hvals.csv'%(self.layName_pfx, dkey))
            df.to_csv(ofp)
            log.info('wrote %s to %s'%(str(df.shape), ofp))
            
            
        if self.exit_summary:
            self.smry_d[dkey] = pd.Series(meta_d).to_frame()
 
 
        return samp_cap_vlay
 
 
        
    def get_capped_pts(self, #force lower/upper bounds on some points
                    vlay_raw, #points vector layer
                    fieldName='hvals', #field name on vlay with sample values
                    
                    #parameters
                    vmin=None, #minimum HAND value to allow
                    vmax=None,
                    
                    
                    prec=3,
                    #plot=None,
                    logger=None,
                    ):
        
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        log=logger.getChild('get_capped_pts')
        
        assert isinstance(vmin, float)
        assert isinstance(vmax, float)
        #===================================================================
        # setup
        #===================================================================
        log.info('forcing bounds (%.2f and %.2f) on  %s'%(
            vmin, vmax, vlay_raw.name()))
 
        #===================================================================
        # get values
        #===================================================================
        sraw = pd.Series(vlay_get_fdata(vlay_raw, fieldName), dtype=float, name=fieldName).round(prec)
 
        assert sraw.notna().all()
 
        sclean = sraw.copy()
        #===================================================================
        # force new lower bounds
        #===================================================================
        bx = sraw<vmin
        sclean.loc[bx] = vmin
        
        #===================================================================
        # force upper bounds
        #===================================================================
        bx_up = sraw>vmax
        sclean.loc[bx_up] = vmax
        
        log.info('set %i / %i (of %i) min/max vals %.2f / %.2f'%(
            bx.sum(), bx_up.sum(), len(bx), vmin, vmax))
        
        #===================================================================
        # build result
        #===================================================================
        if (not bx_up.any()) and (not bx.any()):
            res_vlay = vlay_raw
            log.warning('set zero caps... returning raw')
        else:
        
            geo_d= vlay_get_geo(vlay_raw)
            
            res_vlay = self.vlay_new_df(sclean.to_frame(), geo_d=geo_d, logger=log)
        
        #=======================================================================
        # wrap
        #=======================================================================
        meta_d = {'max_cap_cnt':bx_up.sum(), 
                       'min_floor_cnt':bx.sum(),
                       'total':len(bx)}
        
        df = pd.concat({'raw':sraw, 'capped':sclean}, axis=1)
        
        
        return res_vlay, df, meta_d
    
    def build_hgRaw(self, #interpolate and grow the beach 2 values
             
             #datalayesr
             beach2_vlay=None,
             dem_rlay=None, #for reference
             inun2_rlay=None,
             fieldName=None, #field name with sample values
             
             #parameters (interploate)
             distP=2.0, #distance coeffiocient#I think this is unitless

             pts_cnt = 10, #number of points to include in seawrches
             radius=None, #Search Radius in map units
 
             
               #gen
              dkey=None, logger=None,write=None,
                  ):
        """
        should this be split?
        
        2022-03-28:
            changed to use wbt
            changed to always match dem resolution
        """
 
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        if write is None: write=self.write
        log=logger.getChild('b.%s'%dkey)
 
        assert dkey=='hgRaw'
 
        layname, ofp = self.get_outpars(dkey, write)
        
 
        #=======================================================================
        # retrieve
        #=======================================================================
        if dem_rlay is None:
            dem_rlay = self.retrieve('dem')
            
        if beach2_vlay is None:
            beach2_vlay=self.retrieve('beach2')
            
        if inun2_rlay is None:
            """basically a mask of the beach2_vlay points"""
            inun2_rlay=self.retrieve('inun2')
            
        #=======================================================================
        # parameters 2
        #=======================================================================
        if fieldName is None:
            fnl = [f.name() for f  in beach2_vlay.fields() if not f.name()=='fid']
            assert len(fnl)==1
            fieldName = fnl[0]
        
 
        
        if radius is None:
            radius=self.dem_psize*4
            
        meta_d = {'distP':distP, 'pts_cnt':pts_cnt, 'radius':radius}
        #=======================================================================
        # #build interpolated surface from edge points-----
        #=======================================================================
        log.info('IDW Interpolating HAND values from \'%s\' (%i) w/ \n    %s'%(
                        beach2_vlay.name(), beach2_vlay.dataProvider().featureCount(), meta_d))
        
        #===================================================================
        # get interpolated raster-----
        #===================================================================
        #=======================================================================
        # native
        #=======================================================================
        """couldnt figure out how to configure the input field
        #===================================================================
        # interp_rlay = self.idwinterpolation(pts_vlay, coln, resolution, distP=distP, 
        #                                     logger=log)
        #==================================================================="""
        #=======================================================================
        # whitebox
        #=======================================================================
        #convert to shapefile
        shp_fp = self.vlay_write(beach2_vlay, os.path.join(self.temp_dir, '%s.shp'%beach2_vlay.name()), driverName='ESRI Shapefile')
        
        #run tool
        interp_raw_fp = Whitebox(logger=logger, version='v2.0.0', #1.4 wont cap processors
                               ).IdwInterpolation(shp_fp, fieldName,
                                weight=distP, 
                                radius=radius,
                                min_points=pts_cnt,
                                #cell_size=resolution,
                                ref_lay_fp=dem_rlay.source(),
                                out_fp=os.path.join(self.temp_dir, 'wbt_IdwInterpolation_%s.tif'%dkey))
                               
 
        """GRASS.. a bit slow
        #=======================================================================
        # interp_raw_fp = self.vSurfIdw(beach2_vlay, fieldName, distP=distP,
        #               pts_cnt=pts_cnt, cell_size=resolution, extents=dem_rlay.extent(),
        #               logger=log, 
        #               output=os.path.join(self.temp_dir, 'vsurfidw.tif'),
        #               )
        #======================================================================="""
        assert os.path.exists(interp_raw_fp)
        
 
        
        #=======================================================================
        # #re-interpolate interior regions-----
        #=======================================================================
        self.wsl_extrap_wbt(interp_raw_fp,inun2_rlay.source(),  logger=log, ofp=ofp)
 
        #=======================================================================
        # wrap
        #=======================================================================
        rlay = self.rlay_load(ofp, logger=log)
        
        assert_func(lambda:  self.rlay_check_match(rlay,dem_rlay, logger=log))
        
        
        if write:
            self.ofp_d[dkey] = ofp
 
        if self.exit_summary:
            self.smry_d[dkey] = pd.Series(meta_d).to_frame()
 
 
        return rlay
        
 
        
    def build_hgSmooth(self, #smoth the rolling hand grid (low-pass and downsample)
             
             #datalayesr
             hgRaw_vlay=None,
             
             #parameters
              resolution=None, #resolution for rNeigbour averaging. not output
             
             range_thresh=None, #maximum range (between HAND cell values) to allow
                #None: calc from max_slope and resolution
             max_grade = 0.1, #maximum hand value grade to allow 
             
             neighborhood_size = 7,
             
             max_iter=20, #maximum number of smoothing iterations to allow
             precision=0.2,  #prevision of resulting HAND values (value to round to nearest multiple of)
             
             
               #gen
              dkey=None, logger=None,write=None, debug=False,
                  ):
        """
        this is super nasty... must be a nice pre-built low-pass filter out there
        """
 
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        if write is None: write=self.write
        log=logger.getChild('b.%s'%dkey)
 
        assert dkey=='hgSmooth'
        
        layname, ofp = self.get_outpars(dkey, write)
        meta_d = dict()
        
 
        #=======================================================================
        # retrieve
        #=======================================================================
        if hgRaw_vlay is None:
            hgRaw_vlay=self.retrieve('hgRaw')
        
        #=======================================================================
        # parameters 2
        #=======================================================================
 
        if resolution is None: 
            """not much benefit to downsampling actually
                    just makes the smoothing iterations faster"""
            resolution = int(self.rlay_get_resolution(hgRaw_vlay)*3)
            
        if range_thresh is  None:
            """capped at 2.0 for low resolution runs""" 
            range_thresh = round(min(max_grade*resolution, 2.0),2)
            
            
 
            
        log.info('applying low-pass filter and downsampling (%.2f) from %s'%(
            resolution, hgRaw_vlay.name()))
 
        
        #=======================================================================
        # run smoothing
        #=======================================================================
        rlay_smooth, d, smry_df = self.rlay_smooth(hgRaw_vlay,
            neighborhood_size=neighborhood_size, resolution=resolution,
            max_iter=max_iter, range_thresh=range_thresh, 
            debug=debug,logger=log)

        meta_d.update(d) 
        
        #=======================================================================
        # round values
        #=======================================================================
        rlay_mround_fp = self.rlay_mround(rlay_smooth,  logger=log, multiple=precision,
                                          output=ofp)['OUTPUT']
        rlay = self.rlay_load(rlay_mround_fp, logger=log)
        #=======================================================================
        # wrap
        #=======================================================================

        if write:
            self.ofp_d[dkey] = rlay_mround_fp
 
        if self.exit_summary:
            self.smry_d[dkey] = pd.Series(meta_d).to_frame()
            self.smry_d['%s_smoothing'%dkey] = smry_df
 
 
        return rlay
    
    def rlay_smooth(self,
                    rlay_raw,
                    neighborhood_size=None,
                    circular_neighborhood=True,
                    resolution=None, #for rNeighbors
                    
                    max_iter=10, 
                    range_thresh=None,
                    
 
                    #gen
                    logger=None, debug=False, ofp=None,
                    ):
        #=======================================================================
        # ddefaults
        #=======================================================================
        if logger is None: logger=self.logger
        log = logger.getChild('rsmth')
        meta_d={'smooth_resolution':resolution, 'smooth_range_thresh':range_thresh, 'max_iter':max_iter}
        log.info('on \'%s\'  %s\n    %s'%(rlay_raw.name(), self.rlay_get_props(rlay_raw), meta_d))
        
 
        #===================================================================
        # smooth initial
        #===================================================================
        smooth_rlay_fp1 = self.rNeighbors(rlay_raw,
                        neighborhood_size=neighborhood_size, 
                        circular_neighborhood=circular_neighborhood,
                        cell_size=resolution,
                        #output=ofp, 
                        logger=log)
        """this has a new extents"""
        
        assert os.path.exists(smooth_rlay_fp1)
        #===================================================================
        # get mask
        #===================================================================
        """
        getting a new mask from teh smoothed as this has grown outward
        """
        mask_fp = self.mask_build(smooth_rlay_fp1, logger=log,
                        ofp=os.path.join(self.temp_dir, 'rsmooth_mask.tif'))
        

        #===================================================================
        # smooth loop-----
        #===================================================================
        #===================================================================
        # setup
        #===================================================================
        rlay_fp_i=smooth_rlay_fp1
        rval = 99.0 #dummy starter value
        rvals_d = dict()
        
        #directories
        temp_dir = os.path.join(self.temp_dir, 'smoothing')
        if not os.path.exists(temp_dir): os.makedirs(temp_dir)
        
        #===================================================================
        # loopit
        #===================================================================
        fail_cnt = 0 #number of failures to smoothen
        for i in range(0,max_iter):
 
            #===============================================================
            # #check range and smoth
            #===============================================================
            try:
                check, rlay_fp_i, rvali, fail_pct = self._smooth_iter(rlay_fp_i, 
                                                           range_thresh=range_thresh,
                                                           mask=mask_fp,
                                                           logger=log.getChild(str(i)),
                                                           out_dir=temp_dir,
                                                           sfx='%03d'%i,
                                                           debug=debug)
            except Exception as e:
                log.warning('_smooth_iter %i failed w/ \n    %s'%(i, e))
                fail_cnt=10
                rvali=0

            #===============================================================
            # #check progress  
            #===============================================================
            if not rvali< rval:
                """this can trip early on
                TODO: more sophisticated trip"""
                fail_cnt+=1
                log.warning('(%i/%i) failed to reduce the range (%.2f>%.2f). fail_cnt=%i'%(
                    i,max_iter-1, rvali, rval, fail_cnt))
                
            else:
                fail_cnt=0 #reset counter
            #===============================================================
            # #wrap
            #===============================================================
            rval = rvali
            rvals_d[i] = {
                'rval':round(rvali,3), 'check':check,'fail_pct':fail_pct,'fail_cnt':fail_cnt, 
                'fp':rlay_fp_i} 
            
            if check:
                break
            
            #execsesive faiolure check
            if fail_cnt>3:
                log.warning('excessive concurrent failures...breaking')
                break
        #===================================================================
        # #wrap
        #===================================================================
        meta_d.update({'_smooth_iters':i, 'smooth_rval':round(rvali, 3)})
        
        #meta frame summarizing each iteration. rval=maximum range value
        df = pd.DataFrame.from_dict(rvals_d, orient='index')

        #sucess
        if check:
            log.info('achieved desired smoothness in %i iters \n    %s'%(i,
                                                  df['rval'].to_dict()))
        
        #failure
        else:
            # retrieve minimum
            """pulling the best we did"""
            imin = df['rval'].idxmin()
            rlay_fp_i = df.loc[imin, 'fp']
            
            
            log.warning('FAILED smoothness in %i (%.2f>%.2f). taking i=%i\n    %s'%(
                i,rvali, range_thresh, imin, df['rval'].to_dict()))
        
        """
        view(df)
        """
        #===================================================================
        # post
        #===================================================================
        assert os.path.exists(rlay_fp_i), 'failed to get a result: %s'%rlay_fp_i
        #repeoject to extents and original resolution
        ofp = self.warpreproject(rlay_fp_i, 
                                 output=ofp,
                                 extents=rlay_raw.extent(), 
                                 resolution = int(self.rlay_get_resolution(rlay_raw)),
                                  logger=log)

 
     
        res_rlay = self.rlay_load(ofp, logger=log)
        assert_func(lambda:  self.rlay_check_match(res_rlay,rlay_raw, logger=log))
 
        #===================================================================
        # build animations
        #===================================================================
        if debug:
            from hp.animation import capture_images
            capture_images(
                os.path.join(self.out_dir,   self.layName_pfx+'_shvals_avg.gif'),
                os.path.join(temp_dir, 'avg')
                )
            
            capture_images(
                os.path.join(self.out_dir,   self.layName_pfx+'_shvals_range.gif'),
                os.path.join(temp_dir, 'range')
                )
            
        return res_rlay, meta_d, df
 
    def _smooth_iter(self,  #check if range threshold is satisifed... or smooth 
                    rlay_fp, 
                    range_thresh=1.0,
                    neighborhood_size=3,
                    #circular_neighborhood=True,
                    mask=None,
                    sfx='',
                    out_dir=None,
                    logger=None,
                    debug=False,
                    ):
        """
        spent a few hours on this
            theres probably a nicer pre-buit algo I should be using
            
        most parameter configurations return the smoothest result on iter=2
        """
        
        #=======================================================================
        # defaults
        #=======================================================================
        
        if logger is None: logger=self.logger
        log=logger.getChild('iter')
        log.debug('on %s'%os.path.join(rlay_fp))
        
        #setup directories
        if out_dir is None: out_dir=self.temp_dir
        
        if not os.path.exists(os.path.join(out_dir, 'range')):
            os.makedirs(os.path.join(out_dir, 'range'))
            
        if not os.path.exists(os.path.join(out_dir, 'avg')):
            os.makedirs(os.path.join(out_dir, 'avg'))
            
        if not os.path.exists(os.path.join(out_dir, 'mask')):
            os.makedirs(os.path.join(out_dir, 'mask'))
            
        #=======================================================================
        # apply mask
        #=======================================================================
        
        rlay_maskd_fp = self.mask_apply(rlay_fp, mask, logger=log)
        assert os.path.exists(rlay_maskd_fp)
        #=======================================================================
        # get max mrange
        #=======================================================================
        range_fp = self.rNeighbors(rlay_maskd_fp,
                            neighborhood_size=neighborhood_size, 
                            circular_neighborhood=False,
                            method='range',
                            #mask=mask_range, #moved to a hard mask
                            #output=os.path.join(out_dir, 'range','%s_range.tif'%sfx), #not working
                            #logger=log,
                            #feedback='none',
                            )
        #copy over
        assert os.path.exists(range_fp)
        if debug:
            shutil.copyfile(range_fp,os.path.join(out_dir, 'range','%s_range.tif'%sfx))
        
        #get the statistics
        stats_d = self.rasterlayerstatistics(range_fp)
        rval = stats_d['MAX']        
        """what if we target the average instead?"""
        #=======================================================================
        # check critiera
        #=======================================================================
        if rval<=range_thresh:
            log.debug('maximum range (%.2f) < %.2f'%(rval, range_thresh))
            
            
            return True, rlay_maskd_fp, rval, 0
        
        #=======================================================================
        # build mask of values failing criteria
        #=======================================================================
        range_mask = self.mask_build(range_fp, 
                     thresh=(range_thresh*0.75), #values exceeding this are smoothed below
                        #smoothing a bit beyond those failing seems to improve the overall
                                     thresh_type='lower', logger=log,
                                     ofp=os.path.join(out_dir, 'mask', '%s_range_mask.tif'%sfx))
        
        #get the fail count
        fail_cnt = self.rasterlayerstatistics(range_mask)['SUM']
        cell_cnt = self.rasterlayerstatistics(rlay_maskd_fp)['SUM']
        pct = (fail_cnt/float(cell_cnt))*100.0
        
        assert cell_cnt>1
        #=======================================================================
        # apply smoothing to these
        #=======================================================================
        log.info('max range (%.2f) exceeds threshold (%.2f) on %.2f pct... smoothing'%(
            rval, range_thresh,pct))
        assert os.path.exists(rlay_maskd_fp)
        assert os.path.exists(range_mask)
        
        """"
        #=======================================================================
        # #performance tests
        #=======================================================================
        neighborhood_size=7, circular_neighborhood=TRUE, mask=range_mask, resolution=15
            range=2.96 (i=2)
        
        neighborhood_size=5, circular_neighborhood=TRUE, mask=range_mask, resolution=15
            range=2.96 (i=2)
            
        neighborhood_size=5, circular_neighborhood=False, mask=range_mask, resolution=15
            range=2.96 (i=2)
            
        neighborhood_size=5, circular_neighborhood=False, mask=range_mask, resolution=15, method='median'
            range=3.137 (i=1)
            
        neighborhood_size=5, circular_neighborhood=False, mask=range_mask*1.1, resolution=15
            range=2.96 (i=2)
            
        neighborhood_size=5, circular_neighborhood=False, mask=None, resolution=15
            range=1.95 (i=19)
            
        neighborhood_size=5, circular_neighborhood=False, mask=range_mask+0.5, resolution=15
            range=2.96 (i=2), pct=1.62
            
        neighborhood_size=5, circular_neighborhood=False, mask=range_mask/2.0, resolution=15
            range=2.96 (i=1), pct=1.62
            
        neighborhood_size=5, circular_neighborhood=False, mask=range_mask*0.25, resolution=15
            range=2.22(i=19), pct=22
        
        """
        smooth_fp = self.rNeighbors(rlay_maskd_fp,
                            neighborhood_size=neighborhood_size+2, #must be odd number 
                            circular_neighborhood=True,
                            method='average',
                            mask=range_mask, #only smooth those failing the threshold
                            #output=os.path.join(out_dir,'avg','%s_avg.tif'%sfx),
                            #logger=log
                            #feedback='none',
                            )
        assert os.path.exists(smooth_fp)
        """good to have a different name for iterations"""
        
        ofp = os.path.join(os.path.dirname(smooth_fp), '%s_avg.tif'%sfx)
        os.rename(smooth_fp,ofp)
        #copy over
        if debug:
            shutil.copyfile(ofp,os.path.join(out_dir,'avg','%s_avg.tif'%sfx))
        
        return False, ofp, rval, fail_cnt
    

    #===========================================================================
    # PHASE3: Rolling WSL grid-----------
    #===========================================================================
    

    
    def run_wslRoll(self,
 
                    ):
        
        #=======================================================================
        # defaults    
        #=======================================================================
        logger=self.logger
        log=logger.getChild('rWSL')
        start =  datetime.datetime.now()
 
        self.clear_all() #release everything from memory and reset the data containers
        
        #=======================================================================
        # get rolling WSL
        #=======================================================================
        
        
        #build a HAND inundation for each value on the hvgrid
        hinun_pick = self.retrieve('hInunSet', logger=log)
        

        #buidl the HAND WSL set
        """convert each inundation layer into a WSL"""
        hwsl_pick = self.retrieve('hWslSet')
        
        
        
        
 
        
        #mask and mosaic to get event wsl
        """using the approriate mask derived from teh hvgrid
            mosaic togehter the corresponding HAND wsl rasters
            extents here should match the hvgrid"""
            
        wsl_rlay = self.retrieve('wslMosaic')
            
        #=======================================================================
        # wrap
        #=======================================================================
        dem_rlay = self.retrieve('HAND', logger=log) #just for checking
        
        assert dem_rlay.extent()==wsl_rlay.extent()
        
        tdelta = datetime.datetime.now() - start
        
        log.info('finished in %s'%tdelta)
        
        return 
    

    
    def build_hiSet(self, #get HAND derived inundations
                #input layers
                hgSmooth_rlay=None,
                hand_rlay=None,
                
                #parameters
                resolution=None, #resolution for inundation rasters
                    #same resolution is used for hWslSet and wslMosaic
                    #allowsing downsampling here
                
               #gen
              dkey=None, logger=None,write=None, debug=False, 
              compress=None, #could result in large memory usage
                  ):
 
        """
        TODO: performance improvements
            reduce resolution?
            different raster format? netCDF
        """
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        if write is None: write=self.write

        log=logger.getChild('b.%s'%dkey)
 
        assert dkey=='hInunSet'
        
        layname, ofp = self.get_outpars(dkey, write, ext='.pickle')
        meta_d = dict()
        
 
        #=======================================================================
        # setup
        #=======================================================================
    
        #directory
        if write:
            out_dir = os.path.join(self.wrk_dir, dkey)
        else:
            out_dir=os.path.join(self.temp_dir, dkey)
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)
        
        mstore=QgsMapLayerStore()
        
        
        #=======================================================================
        # retrieve
        #=======================================================================
        if hgSmooth_rlay is None:
            hgSmooth_rlay=self.retrieve('hgSmooth')
        if hand_rlay is None:
            hand_rlay=self.retrieve('HAND')
            
        
        log.info('on %s:  %s'%(hgSmooth_rlay.name(),self.rlay_get_props(hgSmooth_rlay)))
        
        #=======================================================================
        # downsample the hand layer
        #=======================================================================
        hres = self.rlay_get_resolution(hand_rlay)

        if resolution is None:
            resolution=hres
 
        #reproject with new resolution
        if not hres == resolution:
            log.info('downsampling \'%s\' from %.2f to %.2f'%(
                hand_rlay.name(), hres,  resolution))
             
            hand1_rlay = self.warpreproject(hand_rlay, resolution=resolution,
                                           logger=log)
            
            mstore.addMapLayer(hand1_rlay)
            assert hand1_rlay.extent()==hand_rlay.extent()
        else:
            hand1_rlay = hand_rlay
            
 
        #get total grid size
        hand_cell_cnt = self.rlay_get_cellCnt(hand1_rlay)
        
        #=======================================================================
        # get grid values
        #=======================================================================
        """use the native to avoid new values
        rlay = self.roundraster(hvgrid_fp, logger=log, prec=hval_prec)"""
        
        uq_vals = self.rlay_uq_vals(hgSmooth_rlay, prec=1)
        

        #=======================================================================
        # get inundation rasters
        #=======================================================================
        log.info('building %i HAND inundation rasters (%.2f to %.2f) reso=%.1f'%(
            len(uq_vals), min(uq_vals), max(uq_vals), resolution))
        res_d = dict()
        
        """TODO: paralleleize this"""
        for i, hval in enumerate(uq_vals):
            log.debug('(%i/%i) getting hinun for %.2f'%(i+1, len(uq_vals), hval))
            
            #get this hand inundation
 
            rlay_fp = self.get_hand_inun(hand1_rlay, hval, logger=log,
                               ofp = os.path.join(out_dir, '%03d_hinun_%03d.tif'%(i, hval*100)),
                               compress=compress
                               )
            
            stats_d = self.rasterlayerstatistics(rlay_fp, logger=log)
            res_d[i] = {**{'hval':hval,'fp':rlay_fp,
                                      'flooded_pct':(stats_d['SUM']/float(hand_cell_cnt))*100,
                                       'error':np.nan},
                                        **stats_d, }
            
            log.info('(%i/%i) got hinun for hval=%.2f w/ %.2f pct flooded'%(
                i+1, len(uq_vals), hval, res_d[i]['flooded_pct']))
            
        #=======================================================================
        # check
        #=======================================================================
        inun_rlay_i = self.rlay_load(rlay_fp, logger=log, mstore=mstore)
        
        assert inun_rlay_i.extent()==hand_rlay.extent(), 'resulting inundation extents do not match'
 
            
        #===================================================================
        # build animations
        #===================================================================
        if debug:
            from hp.animation import capture_images
            capture_images(
                os.path.join(self.out_dir, self.layName_pfx+'_hand_inuns.gif'),
                out_dir
                )
 
        #=======================================================================
        # wrap
        #=======================================================================
        meta_d.update({'uq_vals_cnt':len(uq_vals), 'hinun_set_resol':resolution,
                     'hvgrid_uq_vals':copy.copy(uq_vals)})
        
        df = pd.DataFrame.from_dict(res_d, orient='index')

        res2_d = df.set_index('hval')['fp'].to_dict()

        if write:
            self.ofp_d[dkey] = self.write_pick(res2_d, ofp, logger=log)
 
            
 
        if self.exit_summary:
            self.smry_d[dkey] = pd.Series(meta_d).to_frame()
            self.smry_d['%s_stats'%dkey] = df
 
        mstore.removeAllMapLayers()
        return res2_d
    
 
    
    def build_hwslSet(self, #get set of HAND derived wsls (from hand inundations)
                #input layers
                hi_fp_d=None,
                dem_rlay=None,
                
                #parameters
                max_fail_cnt=5, #maximum number of wsl failures to allow
               #gen
              dkey=None, logger=None,write=None,  
              compress=None, #could result in large memory usage
                  ):
        """resolution is taken from hInunSet layers"""
 
 
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        if write is None: write=self.write

        log=logger.getChild('b.%s'%dkey)
 
        assert dkey=='hWslSet'
        
        layname, ofp = self.get_outpars(dkey, write, ext='.pickle')
        meta_d = dict()
        
        #=======================================================================
        # setup
        #=======================================================================
        #directory
        if write:
            out_dir = os.path.join(self.wrk_dir, dkey)
        else:
            out_dir=os.path.join(self.temp_dir, dkey)
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)
        
        mstore = QgsMapLayerStore()
        
        #=======================================================================
        # retrieve
        #=======================================================================
        """NOTE: resolutions are allowed to not match (see build_hiSet())"""
        if hi_fp_d is None:
            hi_fp_d = self.retrieve('hInunSet')
        if dem_rlay is None:
            dem_rlay=self.retrieve('dem')

 
        #check these
        for hval, hi_fp_i in hi_fp_d.items():
            assert os.path.exists(hi_fp_i), 'hval %.2f bad fp in pickel:\n    %s'%(hval, hi_fp_i)
            assert QgsRasterLayer.isValidRasterFileName(hi_fp_i),  \
                'hval %.2f bad fp in pickel:\n    %s'%(hval, hi_fp_i)
            

        #=======================================================================
        # get matching DEM
        #=======================================================================
        ref_lay = self.rlay_load(hi_fp_i, mstore=mstore, logger=log)
        matching, msg = self.rlay_check_match(ref_lay, dem_rlay)
 
 

        #reproject with new resolution
        if not matching:
            """not tested
            this should only affect resolution... no extents or crs"""
            log.warning('warping DEM to match:\n%s'%msg)
            dem1_rlay = self.rlay_warp(dem_rlay, ref_lay=ref_lay, logger=log)
            
        else:
            dem1_rlay = dem_rlay

        assert self.rlay_check_match(ref_lay, dem1_rlay, logger=log), 'HANDinun resolution does not match dem'
        mstore.removeAllMapLayers()
        
        #=======================================================================
        # get water level rasters----
        #=======================================================================
        log.info('building %i wsl rasters on \'%s\''%(
            len(hi_fp_d), dem1_rlay.name()))
        
        res_d = dict()
        fail_cnt = 0
        for i, (hval, fp) in enumerate(hi_fp_d.items()):
            log.info('(%i/%i) hval=%.2f on %s'%(
                i,len(hi_fp_d)-1,hval, os.path.basename(fp)))
            
            try:
            
                #extrapolate in
                wsl_fp = self.wsl_extrap_wbt(dem1_rlay.source(), fp, logger=log.getChild(str(i)),
                            ofp = os.path.join(out_dir, '%03d_hwsl_%03d.tif'%(i, hval*100.0)), #result layer
                            out_dir=os.path.join(self.temp_dir, dkey, str(i)), #dumping iter layers
                            compress=compress,)
                
                #smooth
                """would result in some negative depths?
                    moved to the wsl mosaic"""
 
                
                
                #get the stats
                stats_d = self.rasterlayerstatistics(wsl_fp, logger=log)
                res_d[i] = {**stats_d, **{'hval':hval,'inun_fp':fp,'fp':wsl_fp, 'error':np.nan}}
            except Exception as e:
                """
                letting the calc proceed
                    normally rGrowDistance fails on fringe hvals
                    otherwise... the depth raster will look spotty
                """
                log.warning('failed to get wsl on %i hval=%.2f w/ \n    %s'%(i, hval, e))
                res_d[i] = {'hval':hval,'inun_fp':fp, 'error':e}
                
                #check we aren't continuously failing
                fail_cnt +=1
                if fail_cnt>max_fail_cnt:
                    raise Error('failed to get wsl too many times')
            

        #===================================================================
        # build animations
        #===================================================================
        """not showing up in the gifs for some reason"""

 
        if len(res_d)==0:
            raise Error('failed to generate any WSLs')
 
 
        #=======================================================================
        # output
        #=======================================================================
        meta_d.update({'fail_cnt':fail_cnt})
        
        df = pd.DataFrame.from_dict(res_d, orient='index')

        #write the reuslts pickel
        """only taking those w/ successfulr asters"""
        res2_d = df[df['error'].isna()].set_index('hval', drop=True)['fp'].to_dict()

        if write:
            self.ofp_d[dkey] = self.write_pick(res2_d, ofp, logger=log)
 
            
 
        if self.exit_summary:
            self.smry_d[dkey] = pd.Series(meta_d).to_frame()
            self.smry_d['%s_stats'%dkey] = df
 
        mstore.removeAllMapLayers()
        return res2_d
    
    def build_wsl(self, #get set of HAND derived wsls (from hand inundations)

                    
                #input layers
                hwsl_fp_d=None,
                hgSmooth_rlay=None,
 
                
                #parameters
                hvgrid_uq_vals=None,
 
               #gen
              dkey=None, logger=None,write=None,  
              compress=None, #could result in large memory usage
                  ):
        """resolution is taken from hInunSet layers"""
 
 
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        if write is None: write=self.write

        log=logger.getChild('b.%s'%dkey)
 
        assert dkey=='wslMosaic'
        
        layname, ofp = self.get_outpars(dkey, write, ext='.tif')
        meta_d = dict()
        
        
        if hvgrid_uq_vals is None:
            #????
            if 'hvgrid_uq_vals' in self.meta_d: 
                hvgrid_uq_vals=self.meta_d['hvgrid_uq_vals']
                
        temp_dir = os.path.join(self.temp_dir, dkey)
        if not os.path.exists(temp_dir):os.makedirs(temp_dir)
                
        mstore=QgsMapLayerStore()
        #=======================================================================
        # retrieve
        #=======================================================================
        if hgSmooth_rlay is None:
            hgSmooth_rlay=self.retrieve('hgSmooth')
            
        if hwsl_fp_d is None:
            hwsl_fp_d = self.retrieve('hWslSet')
        
            
        #sort it
        hwsl_fp_d = dict(sorted(hwsl_fp_d.copy().items()))
 
        #check these
        for hval, hwsl_fp in hwsl_fp_d.items():
            assert os.path.exists(hwsl_fp), 'hval %.2f bad fp in pickel:\n    %s'%(hval, hwsl_fp)
            assert QgsRasterLayer.isValidRasterFileName(hwsl_fp),  \
                'hval %.2f bad fp in pickel:\n    %s'%(hval, hwsl_fp)
                
        resolution = self.rlay_get_resolution(hwsl_fp)
        #=======================================================================
        # round the hv grid
        #=======================================================================
        """
        NO! multiple roundings might cause issues.. just use the raw and check it
        
        grid precision needs to match the hvals for the mask production
        """
                
        #=======================================================================
        # check hand grid values
        #=======================================================================
        #get the values
        if hvgrid_uq_vals is None:
            """usually set by run_hinunSet()"""
            hvgrid_uq_vals = self.rlay_uq_vals(hgSmooth_rlay, prec=1)
                
        #check against the pickel
        miss_l = set(hwsl_fp_d.keys()).symmetric_difference(hvgrid_uq_vals)
        assert len(miss_l)==0, '%i value mismatch between hwsl_pick  and hvgrid (%s) \n    %s'%(
            len(miss_l),  hgSmooth_rlay.name(), miss_l)
        
        """
        ar = rlay_to_array(hvgrid_fp)
        view(pd.Series(ar.reshape(1, ar.size).tolist()[0]).dropna().value_counts())
        """
 
        #=======================================================================
        # loop and mask
        #=======================================================================
        log.info('masking %i'%len(hwsl_fp_d))
        res_d = dict()
        first=True
        mask_j_fp, hval_j=None, 0
        
        for i, (hval, wsl_fp) in enumerate(hwsl_fp_d.items()): #order matters now
            log.debug('    (%i/%i) hval=%.2f on %s'%(
                i, len(hwsl_fp_d)-1, hval, os.path.basename(wsl_fp)))
            
            #check montonoticy
            assert hval>hval_j
            hval_j=hval
            
            assert os.path.exists(wsl_fp), 'bad wsl_fp on %i'%(i, wsl_fp)
            #===================================================================
            # #get donut mask for this hval
            #===================================================================
            #mask those less than the hval (threshold mask)
            mask_i_fp = self.mask_build(hgSmooth_rlay, logger=log,
                                      thresh=hval, thresh_type='upper',                                      
                          ofp=os.path.join(temp_dir, 'mask','mask_i_%03d_%03d.tif'%(i, hval*100))
                          )
            
            #take this for the first
            if first:
                mask_fp = mask_i_fp
                first=False
                
            #remove cells previously  masked (to get donut maskk)
            else:
                mask_fp = self.mask_apply(
                    mask_i_fp, #everything less than the current hval  (big wsl)
                    mask_j_fp, #everything less than the previous hval (small wsl)
                    invert_mask=True,   #take out small wsl from big
                    logger=log,
                          ofp=os.path.join(temp_dir, 'mask','mask_dnt_%03d_%03d.tif'%(i, hval*100))
                                          )
            mask_j_fp = mask_i_fp #set the previous threshold mask
            
            
            #get mask stats
            assert os.path.exists(mask_fp)
            cell_cnt = self.rasterlayerstatistics(mask_fp, logger=log)['SUM']
            
            d={'hval':hval, 'mask_cell_cnt':cell_cnt,'wsl_fp':wsl_fp,'mask_fp':mask_fp,
               'error':np.nan}
            
            log.info('    (%i/%i) hval=%.2f on %s got %i wet cells'%(
                i, len(hwsl_fp_d)-1, hval, os.path.basename(wsl_fp), cell_cnt))
            #===================================================================
            # check cell co unt
            #===================================================================
            if not cell_cnt>0:
                """this shouldnt trip any more
                if it does... need to switch to mask_build with a range"""
                log.error('identified no hval=%.2f cells'%hval)
                d['error'] = 'no wet cells'
                
            #===================================================================
            # apply the donut mask
            #===================================================================
            else:
 
                wsli_fp = self.mask_apply(wsl_fp, mask_fp, logger=log,
                                  ofp=os.path.join(temp_dir, 'wsl_maskd_%03d_%03d.tif'%(i, hval*100)),
                                  allow_empty=True, #whether to allow an empty wsl. can happen for small masks
                                  )
                
                stats_d = self.rasterlayerstatistics(wsli_fp, logger=log, allow_empty=True)
                
                assert os.path.exists(wsli_fp)                
                d = {**d, **{ 'wsl_maskd_fp':wsli_fp},**stats_d}
 


            #wrap
            res_d[i] = d
        
        #=======================================================================
        # get valids
        #=======================================================================
        """some hval grid cells wind up not having any valid wsl values"""
        df = pd.DataFrame.from_dict(res_d, orient='index')
        df['valid']= np.logical_and(
            df['mask_cell_cnt']>0, #seems like a decent flag
            df['SUM']>0)
        
        fp_d = df.loc[df['valid'], 'wsl_maskd_fp'].to_dict()
        

        if not len(fp_d)>0:
            with pd.option_context('display.max_rows', None, 
                           'display.max_columns', None,
                           'display.width',1000):
                log.debug(df)
            raise Error('failed to get any valid masked HAND wsls for mosaicing... see logger')
 
        #=======================================================================
        # merge masked
        #=======================================================================
        
        log.info('merging %i (of %i) rasters w/ valid wsls'%(len(fp_d), len(df)))
 
        
        wsl1_fp = self.mergeraster(list(fp_d.values()), compression='none',
                         logger=log,
                         output=os.path.join(temp_dir, 'wsl_merged.tif'))
        
        #=======================================================================
        # apply a filter
        #=======================================================================
        """as we want to preserve heterogeneity in teh WSL raster... this is just a single filter"""
        wsl2_fp = self.rNeighbors(wsl1_fp,
                        neighborhood_size=5, 
                        circular_neighborhood=True,
                        #$cell_size=resolution, #use the input
                        #output=ofp, 
                        logger=log)
        
        assert os.path.exists(wsl2_fp)
        
        #reproject
        wsl3_fp = self.warpreproject(wsl2_fp, resolution=int(resolution), compress=compress, extents=hgSmooth_rlay.extent(), logger=log,
                           output=ofp)
        
        rlay = self.rlay_load(wsl3_fp, logger=log)
        
        #=======================================================================
        # check
        #=======================================================================
        assert hgSmooth_rlay.extent()==rlay.extent(), 'extents dont match'
        assert_func(lambda:  self.rlay_check_match(hwsl_fp, rlay, logger=log), msg='%s does not match hWslSet'%dkey)
        #=======================================================================
        # output
        #=======================================================================
        log.info('finished on %s'%rlay.name())
        if write:self.ofp_d[dkey] = rlay.source()
 
            
 
        if self.exit_summary:
            self.smry_d[dkey] = pd.Series(meta_d, dtype=float).to_frame()
            self.smry_d['%s_masking'%dkey] = df
 
        mstore.removeAllMapLayers()
        return rlay
    
    #===========================================================================
    # PHASE4: Depth-----------
    #===========================================================================
    def run_depth(self,
 
                    ):
        
        #=======================================================================
        # defaults    
        #=======================================================================
        logger=self.logger
        log=logger.getChild('rDep')
        self.clear_all() #release everything from memory and reset the data containers
        #=======================================================================
        # #get depths
        #=======================================================================
        dep_rlay = self.retrieve('depths')
        
        return
        dep_fp = self.build_depths(wslM_fp=wslM_fp,dem_fp=dem_fp,inun2r_fp=inun2r_fp,
                                   logger=log)
         

        #=======================================================================
        # wrap
        #=======================================================================
        dem_rlay = self.retrieve('HAND', logger=log) #just for checking
        
        assert dem_rlay.extent()==wsl_rlay.extent()
        
        tdelta = datetime.datetime.now() - start
        
        log.info('finished in %s'%tdelta)
        
        return 
    
    def build_depths(self,
                 #input layers
                 wslM_rlay=None,
                 dem_rlay=None,
                 inun2_rlay=None,
                 
                 precision=1, #rounding to apply for delta calc
 
               #gen
              dkey=None, logger=None,write=None,  
              compress=None, #could result in large memory usage
                  ):
        """resolution is taken from hInunSet layers"""
 
 
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        if write is None: write=self.write

        log=logger.getChild('b.%s'%dkey)
 
        assert dkey=='depths'
        
        layname, ofp = self.get_outpars(dkey, write, ext='.tif')
        meta_d = dict()
        
 
                
        temp_dir = os.path.join(self.temp_dir, dkey)
        if not os.path.exists(temp_dir):os.makedirs(temp_dir)
                
        mstore=QgsMapLayerStore()
        
                    
        if compress is None:
            compress=self.compress
        
 
        
        #=======================================================================
        # retrieve
        #=======================================================================
        if wslM_rlay is None:
            wslM_rlay = self.retrieve('wslMosaic')
            
        if dem_rlay is None:
            dem_rlay = self.retrieve('dem')
            
        if inun2_rlay is None:
            inun2_rlay = self.retrieve('inun2')

        log.info('on \'%s\' and \'%s\' with %s'%(
                 wslM_rlay.name(), dem_rlay.name(), self.rlay_get_props(wslM_rlay)))
 
        
        """no... resolution will match the wsl mosaic (which inherited resolution from hInunSet
        if the user wants another projection... they can do this on their own
        #=======================================================================
        # fix resolution
        #======================================================================="""
        
        #=======================================================================
        # check
        #=======================================================================
        """resolution doesnt have to match here"""
        assert wslM_rlay.extent()==dem_rlay.extent()
        

        assert_func(lambda:self.rlay_check_match(inun2_rlay, dem_rlay, logger=log))
        #=======================================================================
        # get depths
        #=======================================================================
        dep1_fp = self.get_delta(wslM_rlay, dem_rlay, logger=log)
        
        """best to force some rounding before the zero value filtering"""
        dep1b_fp = self.roundraster(dep1_fp, prec=precision, logger=log)
        
        dep2_fp = self.get_positives(dep1b_fp, logger=log)
        
 
        #=======================================================================
        # mask to only those within hydrauilc maximum (and handle compression)
        #=======================================================================
 
        dep3_fp = self.mask_apply(dep2_fp, inun2_rlay, logger=log, ofp=ofp,
                                  allow_approximate=True, 
                                  )
                
        #=======================================================================
        # summary
        #=======================================================================
        rlay = self.rlay_load(dep3_fp, logger=log)
        
        stats_d = self.rasterlayerstatistics(rlay)
        stats_d2 = {'range':stats_d['RANGE'], 'min':stats_d['MIN'], 'max':stats_d['MAX']}
        stats_d2 = {k:round(v,2) for k,v in stats_d2.items()}
        
        cell_cnt = self.rlay_get_cellCnt(dep3_fp)
        
        meta_d = {**{'resolution':self.rlay_get_resolution(rlay), 'wet_cells':cell_cnt}, **stats_d2}
        
        #=======================================================================
        # wrap
        #=======================================================================
        log.info('finished on %s \n    %s'%(rlay.name(), meta_d))
        if write:self.ofp_d[dkey] = rlay.source()
 
            
 
        if self.exit_summary:
            self.smry_d[dkey] = pd.Series(meta_d, dtype=float).to_frame()
 
 
        mstore.removeAllMapLayers()
        return rlay
        
    #===============================================================================
    # CHECKS-----
    #===============================================================================
    def check_pwb(self, #coverage checks against the NHN water bodies
                      rlay=None,
                      aoi_vlay=None,
                      min_ratio=0.001, #minimum stream_area/aoi_area
                      logger=None):
        
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        log=logger.getChild('check_pwb')
        
        if rlay is None:
            rlay = self.retrieve('pwb_rlay')
            
        if aoi_vlay is None:
            aoi_vlay=self.aoi_vlay
        
 
        log.debug('on %s'%rlay.name())
        assert rlay.crs() == self.qproj.crs()
        
        #=======================================================================
        # get areas
        #=======================================================================
        """streams is already clipped to aoi"""
        streams_area = self.mask_get_area(rlay)
        aoi_area = self.vlay_poly_tarea(aoi_vlay)
        
        #=======================================================================
        # check threshold
        #=======================================================================
        ratio = streams_area/aoi_area
        log.debug('coverage = %.2f'%ratio)
        if ratio<min_ratio:
            return False, 'perm water body (%s) coverage  less than min (%.3f<%.3f)'%(
                rlay.name(), ratio, min_ratio)
        else:
            return True, ''

 
    #===========================================================================
    # helpers--------
    #===========================================================================
    def clear_all(self): #clear all the loaded data
        self.data_d = dict()
        self.mstore.removeAllMapLayers()
        self.compiled_fp_d.update(self.ofp_d) #copy everything over to compile
        gc.collect()
        
    def get_outpars(self, dkey, write, ext='.tif'
                    ):
                #output
        layname = '%s_%s'%(self.layName_pfx, dkey) 
        if write:
            ofp = os.path.join(self.wrk_dir, layname+ext)
            
        else:
            ofp=os.path.join(self.temp_dir, layname+ext)
            
        if os.path.exists(ofp):
            assert self.overwrite
            os.remove(ofp)
            
        return layname, ofp
    def get_delta(self, #subtract two rasters
                  top_fp,
                  bot_fp,
                  logger=None,
                  layname=None,
                  **kwargs):
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is  None: logger=self.logger
        log=logger.getChild('get_delta')
        
        if layname is None: layname = 'delta'
        
        rcentry_d = {k: self._rCalcEntry(obj) for k, obj in {'top':top_fp, 'bot':bot_fp}.items()}
        
        formula = '\"{0}\"-\"{1}\"'.format(rcentry_d['top'].ref, rcentry_d['bot'].ref)
        
        return self.rcalc1(rcentry_d['top'].raster, formula,
                          list(rcentry_d.values()),
                          logger=log,layname=layname,
                          **kwargs)
        
    def get_positives(self, #get positive values
                  rlay,
                  zero_val=0.001, #often nice to mask out TRUE zeros
                  logger=None,
                  layname=None,
                  **kwargs):
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is  None: logger=self.logger
        log=logger.getChild('get_positives')
        
        if layname is None: layname = 'posi'
        
        rcent = self._rCalcEntry(rlay)
 
        
        formula = '\"{0}\"/(\"{0}\">{1:.5f})'.format(rcent.ref, zero_val)
        
        return self.rcalc1(rcent.raster, formula,
                          [rcent],
                          logger=log,layname=layname,
                          **kwargs)
        
    def set_layer_stats(self, #push all layer statistics to the summary
                        d=None,
                        logger=None,
                        ):
        """
        called on exit
        """
        from pathlib import Path
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        log=logger.getChild('set_layer_stats')
        if d is None: 
            d={**self.fp_d, **self.ofp_d.copy()}
        
        #=======================================================================
        # collect stats
        #=======================================================================
        res_d=dict()
        for i, (fp_key, fp) in enumerate(d.items()):
            try:
                res_d[fp_key] = {
                                    'i':i,
                                    'ext':os.path.splitext(fp)[1],
                                    'filename':os.path.basename(fp), 
                                 'size (kb)':round(Path(fp).stat().st_size*.001, 1),
                                 'fp':fp}
                
                #========================================== =========================
                # rasters
                #===================================================================
                if fp.endswith('.tif'):
                    res_d[fp_key].update(self.rasterlayerstatistics(fp))
                    rlay = self.rlay_load(fp, logger=log)
                    self.mstore.addMapLayer(rlay)
                    res_d[fp_key].update({
                        'layname':rlay.name(),
                        'crs':rlay.crs().authid(),
                        'width':rlay.width(),
                        'height':rlay.height(),
                        'pixel_size':'%.2f, %.2f'%(rlay.rasterUnitsPerPixelY(), rlay.rasterUnitsPerPixelX()),
                        'providerType':rlay.providerType(),
                        'nodata':get_nodata_val(fp),
                        
                        })
                    
                #===================================================================
                # vectors
                #===================================================================
                elif fp.endswith('gpkg'):
                    vlay = self.vlay_load(fp, logger=log)
                    self.mstore.addMapLayer(vlay)
                    dp = vlay.dataProvider()
                    res_d[fp_key].update(
                        {'fcnt':dp.featureCount(),
                         'layname':vlay.name(),
                         'wkbType':QgsWkbTypes().displayString(vlay.wkbType()),
                         'crs':vlay.crs().authid(),
                         })
                    
                    #Polytons
                    if 'Polygon' in QgsWkbTypes().displayString(vlay.wkbType()):
                        res_d[fp_key]['area'] = self.vlay_poly_tarea(vlay)
                       
                        
                        
                    
                else:
                    pass
            except Exception as e:
                log.warning('failed to get stats on %s w/ \n    %s'%(fp, e))
                
        #=======================================================================
        # append info
        #=======================================================================
        df = pd.DataFrame.from_dict(res_d, orient='index')
        
        self.smry_d = {**{'layerSummary':df}, **self.smry_d}
                
        
            
        
 
    def __exit__(self, #destructor
                 *args,**kwargs):
        self.logger.debug('__exit__ \n \n \n \n')
        
        if self.exit_summary:
            self._log_datafiles()
            
            
            #=======================================================================
            # layerSummary
            #=======================================================================
            
            self.set_layer_stats()
            
            #=======================================================================
            # summary tab
            #=======================================================================
    
            for attn in self.childI_d['Session']:
                self.meta_d[attn] = getattr(self, attn)
                
            tdelta = datetime.datetime.now() - start
            runtime = tdelta.total_seconds()/60.0
            #self.meta_d.update(self.ofp_d) #this is on the layerSummary now
            
            self.meta_d = {**{'now':datetime.datetime.now(), 'runtime (mins)':runtime}, **self.meta_d}
            
            #===================================================================
            # assembel summary sheets
            #===================================================================
            #merge w/ retrieve data
            for k, sub_d in self.dk_meta_d.items():
                if len(sub_d)==0:continue
                retrieve_df = pd.Series(sub_d).to_frame()
                if not k in self.smry_d:
                    self.smry_d[k] = retrieve_df
                else:
                    self.smry_d[k] = self.smry_d[k].reset_index().append(
                            retrieve_df.reset_index(), ignore_index=True).set_index('index')
                            
            
            #dkey summary
            
            
            
            self.smry_d = {**{'_smry':pd.Series(self.meta_d, name='val').to_frame(),
                              '_smry.dkey':pd.DataFrame.from_dict(self.dk_meta_d).T},
                            **self.smry_d}
            
            #=======================================================================
            # write the summary xlsx
            #=======================================================================
    
            #get the filepath
            ofp = os.path.join(self.out_dir, self.layName_pfx+'_calc_smry_%s.xls'%(
                datetime.datetime.now().strftime('%H%M%S')))
            if os.path.exists(ofp):
                assert self.overwrite
                os.remove(ofp)
        
            #write
            try:
                with pd.ExcelWriter(ofp) as writer:
                    for tabnm, df in self.smry_d.items():
                        df.to_excel(writer, sheet_name=tabnm, index=True, header=True)
                        
                print('wrote %i summary sheets to \n    %s'%(len(self.smry_d), ofp))
                    
            except Exception as e:
                print('failed to write summaries w/ \n    %s'%e)
        
            #=======================================================================
            # wrap
            #=======================================================================
            self.logger.info('finished in %.2f mins'%(runtime))
        super().__exit__(*args,**kwargs) #initilzie teh baseclass
        


