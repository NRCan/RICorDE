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
import os, datetime, copy, shutil
import pandas as pd
 
start =  datetime.datetime.now()

 
import processing

from hp.exceptions import Error, assert_func
from hp.dirz import force_open_dir
 
#from hp.plot import Plotr #only needed for plotting sessions
from hp.Q import Qproj, QgsCoordinateReferenceSystem, QgsMapLayerStore, \
    QgsRasterLayer, QgsWkbTypes, vlay_get_fdf
    
from hp.oop import Session as baseSession
     
from ricorde.tcoms import TComs
from hp.gdal import get_nodata_val 


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
            'isamp1':{
                'build':lambda **kwargs:self.build_samples1(**kwargs),
                }
             
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
 
    

    

        
    def run_dataPrep(self, #clean and load data into memory
                     ):
        
        self.retrieve('dem')
 
        self.retrieve('pwb_rlay')
 
        self.retrieve('inun_rlay')
        
        self.mstore_log(logger=self.logger.getChild('rDataPrep'))
        
        

        



    def build_dem(self, #checks and reprojection on the DEM
                  dem_fp=None,
                  
                  #parameters
                  dem_psize=None,
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
        
        if not dem_psize is  None:
            assert isinstance(dem_psize, int), 'got bad pixel size request on the dem (%s)'%dem_psize
            
 
        mstore=QgsMapLayerStore()
        
        #=======================================================================
        # load
        #=======================================================================
        rlay_raw= self.rlay_load(dem_fp, logger=log)
        
        #=======================================================================
        # confi parameters----------
        #=======================================================================
        #=======================================================================
        # config resolution
        #=======================================================================
        psize_raw = self.rlay_get_resolution(rlay_raw)
        resample=False
        
        #use passed pixel size
        if not dem_psize is None: #use passed
            assert dem_psize>=psize_raw
            if not psize_raw==dem_psize:
                resample=True
        
        #use native
        else:
            dem_psize=int(round(psize_raw, 0))
            if not round(psize_raw, 0) == psize_raw:
                resample=True
        
        meta_d = {'raw_fp':dem_fp, 'dem_psize':dem_psize, 'psize_raw':psize_raw}
            
 
        #=======================================================================
        # clip
        #=======================================================================
        clip=False
        if not aoi_vlay is None:
            if not aoi_vlay.extent()==rlay_raw.extent():
                clip=True
                
        #=======================================================================
        # reproject
        #=====================================================================
        reproj=False
        if not rlay_raw.crs()==self.qproj.crs():
            reproj=True
            
        #=======================================================================
        # compression
        #=======================================================================
        decompres=False
        if not self.getRasterCompression(rlay_raw.source()) is None:
            decompres = True
            
        meta_d.update({'clip':clip, 'resample':resample, 'reproj':reproj, 'decompres':decompres})
        #=======================================================================
        # warp-----
        #=======================================================================
        if resample or clip or reproj or decompres:
            #===================================================================
            # defaults
            #===================================================================
            msg = 'warping DEM (%s) w/ resol=%.4f'%(rlay_raw.name(), psize_raw)
            if clip:
                msg = msg + ' +clipping extents to %s'%aoi_vlay.name()
            if resample:
                msg = msg + ' +resampling to %.2f'%dem_psize
            if reproj:
                msg = msg + ' +reproj to %s'%self.qproj.crs().authid()
            if decompres:
                msg = msg + ' +decompress'
            log.info(msg)
 
            mstore.addMapLayer(rlay_raw)
            
            if write:
                ofp = os.path.join(self.wrk_dir, '%s_%ix%i_%s.tif'%(self.layName_pfx,int(dem_psize), int(dem_psize), dkey))
                
            else:
                ofp=os.path.join(self.temp_dir, '%s_%s.tif'%(self.layName_pfx, dkey))
                
            if os.path.exists(ofp):
                assert overwrite
                os.remove(ofp)
            
            #===================================================================
            # warp
            #===================================================================
            """custom cliprasterwithpolygon"""
            ins_d = {   'ALPHA_BAND' : False,
                    'CROP_TO_CUTLINE' : clip,
                    'DATA_TYPE' : 6, #float32
                    'EXTRA' : '',
                    'INPUT' : rlay_raw,
                    
                    'MASK' : aoi_vlay,
                    'MULTITHREADING' : True,
                    'NODATA' : -9999,
                    'OPTIONS' : '', #no compression
                    'OUTPUT' : ofp,
                    
                    'KEEP_RESOLUTION' : not resample,  #will ignore x and y res
                    'SET_RESOLUTION' : resample,
                    'X_RESOLUTION' : dem_psize,
                    'Y_RESOLUTION' : dem_psize,
                    
                    'SOURCE_CRS' : None,
                    'TARGET_CRS' : self.qproj.crs(),

                     }
                    
            algo_nm = 'gdal:cliprasterbymasklayer'
            log.debug('executing \'%s\' with ins_d: \n    %s \n\n'%(algo_nm, ins_d))
        
            res_d = processing.run(algo_nm, ins_d, feedback=self.feedback)
            
            log.debug('finished w/ \n    %s'%res_d)
            
            if not os.path.exists(res_d['OUTPUT']):
                """failing intermittently"""
                raise Error('failed to get a result')
 
            
            #===================================================================
            # wrap
            #===================================================================
            self.ofp_d[dkey] = ofp
            rlay = self.rlay_load(ofp, logger=log)
            
            log.info('finished building \'%s\' w/ \n    %s'%(dkey, ofp))


            
        else:
            rlay=rlay_raw

        #use loader to attach common parameters
        """for consistency between compiled loads and builds"""
        self.load_dem(rlay=rlay, logger=log, dkey=dkey)
        #=======================================================================
        # wrap
        #=======================================================================
        self.smry_d[dkey] = pd.Series(meta_d).to_frame()
        mstore.removeAllMapLayers()
 
 
 
        
        return rlay
    
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
        
        assert round(dem_psize, 0)==dem_psize
        
        self.dem_psize = int(dem_psize)
        
        log.info('loaded %s w/ dem_psize=%.2f'%(rlay.name(), dem_psize))
        
        return rlay 
 
        
        
                  
    
    def build_rlay(self, #build raster from some water polygon
                        fp,
                        dkey=None,
                        write=None,
                        ref_lay=None,
                        aoi_vlay=None,
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
        
        mstore = QgsMapLayerStore()
        
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
        # load the layer
        #=======================================================================
        rlay= self.rlay_load(rlay_fp, logger=log)
        
        #=======================================================================
        # clip
        #=======================================================================
        if not aoi_vlay is None:
            rlay1 = self.slice_aoi(rlay, aoi_vlay=aoi_vlay, logger=log, output=ofp)
            mstore.addMapLayer(rlay)
        else:
            shutil.copyfile(rlay.source(),ofp)
            rlay1=rlay
 
 
        #=======================================================================
        # checks
        #=======================================================================
        assert os.path.exists(ofp)
        assert_func(lambda:  self.rlay_check_match(rlay1, ref_lay, logger=log), msg=dkey)
        
        assert_func(lambda:  self.mask_check(rlay1), msg=dkey)
        
 
        if dkey == 'pwb_rlay':
            assert_func(lambda: self.check_pwb(rlay1))
        
        #=======================================================================
        # wrap
        #=======================================================================
        assert not dkey in self.smry_d
        self.smry_d[dkey] = pd.Series(meta_d).to_frame()
        
        rlay1.setName(layname)
        log.info('finished on %s'%rlay1.name())
        mstore.removeAllMapLayers()
        
        return rlay1
    """
    self.mstore_log()
    """
    
    #===========================================================================
    # PHASE1: Inundation Correction---------
    #===========================================================================
    def run_imax(self,
                  #input data
                     #==========================================================
                     # dem_fp=None,
                     # nhn_fp=None,
                     # fic_fp=None,
                     #==========================================================
                     
                     #get_edge_samples
                     sample_spacing=None, #HAND sample point spacing. None=dem*5
                     
                     #get_sample_bounds
                     qhigh=0.75, #quartile defining the maximum inundation HAND value
                     qlow=0.25,
 
                      logger=None,
                 ):
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        log=logger.getChild('rImax')
        start =  datetime.datetime.now()
        
        #ofp_d_old = copy.copy(self.afp_d)
        
 
        #=======================================================================
        # #get the HAND layer
        #=======================================================================
 
        #get the hand layer
        hand_rlay = self.retrieve('HAND', logger=log)        
 
         
        #=======================================================================
        # add minimum water bodies to FiC inundation
        #=======================================================================
        

        #nodata boundary of hand layer (polygon)
        hand_mask = self.retrieve('HAND_mask')
 
        
        inun1_rlay = self.retrieve('inun1')
        #=======================================================================
        # get hydrauilc maximum
        #=======================================================================
        return
        isamp1_vlay=self.retrieve('isamp1')
        
        #get initial edge samples
        #=======================================================================
        # smpls1_fp = self.build_samples1(rToSamp_fp=hand_fp, inun_fp=inun1_fp,
        #                                 ndb_fp=ndb_fp, sample_spacing=sample_spacing)
        #=======================================================================
        
        #get bounds
        hv_max, hv_min = self.get_sample_bounds(smpls1_fp, qhigh=qhigh, qlow=qlow,logger=log)
        
        #get hydrauilc maximum
        inun_hmax_fp = self.build_hmax(hand_fp=hand_fp,hval=hv_max,logger=log)
        
        #=======================================================================
        # reduce inun by the hydrauilc maximum
        #=======================================================================
        #clip inun1 by hydrauilc  maximum (raster) 
        inun2r_fp = self.build_inun2(inun1_fp, inun_hmax_fp, logger=log)
        
        #vector polygons
        inun2_fp = self.build_inun2_vlay(inun2r_fp, logger=log)
        
        #=======================================================================
        # wrap
        #=======================================================================
        
        #get just those datalayers built by this function
        ofp_d = {k:v for k,v in self.ofp_d.items() if not k in ofp_d_old.keys()}
        log.info('built %i datalayers'%len(ofp_d))
        
        
        self._log_datafiles(d=ofp_d)
        
        self.afp_d = {**self.fp_d, **self.ofp_d} #fp_d overwritten by ofp_d
        
        return datetime.datetime.now() - start
    
    
        
    def build_hand(self, #load or build the HAND layer
                   dkey=None,
                   
                   dem_rlay=None,
                   pwb_rlay=None,
                   
                   write=None,
                   logger=None,
                  **kwargs
                 ):
        """
        TODO: metadata?
        """
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        log=logger.getChild('build_hand')
        assert dkey == 'HAND'
        #if dem_fp is None: dem_fp=self.dem_fp
        if write is None: write=self.write
        #=======================================================================
        # retrieve
        #=======================================================================
        if pwb_rlay is None:
            pwb_rlay = self.retrieve('pwb_rlay')
        pwb_rlay_fp = pwb_rlay.source()
 
        
        if dem_rlay is None:
            dem_rlay = self.retrieve('dem')


        dem_fp = dem_rlay.source() 
        
        log.info('on %s'%{'pwb_rlay_fp':pwb_rlay_fp, 'dem_fp':dem_fp})
        #=======================================================================
        # build sub session
        #=======================================================================
        from ricorde.hand import HANDses
        
        """using a pretty complicated inheritance parameter passing
        seems to be working... but I wouldnt do this again
        see oop.Session.inherit
        """
        
        if write:
            ofp = os.path.join(self.wrk_dir, '%s_%s.tif'%(self.layName_pfx, dkey))
        else:
            """warning: the subsession will delete its temp_dir"""
            ofp = os.path.join(self.temp_dir, '%s_%s.tif'%(self.layName_pfx, dkey))
 
        
        with HANDses(session=self, logger=logger,  inher_d=self.childI_d,  feedback=self.feedback,
                     temp_dir = os.path.join(self.temp_dir, 'HANDses'), 
                     write=write, #otherwise
                     ) as wrkr:
 
            """passing all filepathss for a clean kill"""
            fp = wrkr.run(dem_fp=dem_fp, pwb_fp = pwb_rlay_fp, 
                          ofp=ofp, #easier to control at this level
                          **kwargs)
            """
            wrkr.temp_dir
            """
            
            wrkr.logger.debug('finished')
            
        #=======================================================================
        # check 
        #=======================================================================
        rlay = self.rlay_load(fp, logger=log)
        
        assert_func(lambda:  self.rlay_check_match(rlay,dem_rlay, logger=log))
        
        if write:self.ofp_d[dkey] = fp
            
 
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
    
    
    def build_inun1(self, #merge NHN and FiC and crop to DEM extents
            
            
            #layer inputs
            pwb_rlay = None,
            inun_rlay=None,
            HAND_mask=None,
 
              
              
              #parameters
              buff_dist=None, #buffer to apply to nhn
 
              
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
        
        

        
        #output
        if write:
            ofp = os.path.join(self.wrk_dir, '%s_%s.tif'%(self.layName_pfx, dkey))
        else:
            ofp = os.path.join(self.temp_dir, '%s_%s.tif'%(self.layName_pfx, dkey))
         
        if os.path.exists(ofp):
            assert self.overwrite
            os.remove(ofp)
        
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

            
 
        #===================================================================
        # buffer
        #===================================================================
        """nice to add a tiny buffer to the waterbodies to ensure no zero hand values"""

        #raw buffer (buffered cells have value=2)
        pwb_buff1_fp = self.rBuffer(pwb_rlay, logger=log, dist=buff_dist,
                                    output = os.path.join(self.temp_dir, '%s_buff1.tif'%pwb_rlay.name()))
        
        assert_func(lambda:  self.rlay_check_match(pwb_buff1_fp,HAND_mask, logger=log))
        
        #convert to a mask again
        pwb_buff2_fp = self.mask_build(pwb_buff1_fp, logger=log)
        
 
        
        #=======================================================================
        # merge inundation and pwb
        #=======================================================================
        inun1_1_fp = self.mask_combine([pwb_buff2_fp, inun_rlay], logger=log,
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
        
        
 
        log.info('for \'%s\' built: \n    %s'%(dkey, ofp))

        return rlay
    
    def build_samples1(self, #sapmle the HAND layer using inun1 edges
            #inputs
            hand_rlay=None,
            handM_rlay=None,
            inun1_rlay=None,
            
            
            #parameters
            sample_spacing=None,       
                
              #generals
              dkey=None,
              logger=None,write=None,
              **kwargs):
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
        assert dkey=='isamp1'
 
 
        
        if sample_spacing is None:
            sample_spacing = self.dem_psize*5
            
        #=======================================================================
        # retrieve
        #=======================================================================
        if hand_rlay is None:
            hand_rlay=self.retrieve('HAND')
            
        rToSamp_fp = hand_rlay.source()

            
        if handM_rlay is None:
            handM_rlay = self.retrieve('HAND_mask')

        if inun1_rlay is None:
            inun1_rlay=self.retreive('inun1')
            
        
        
        #=======================================================================
        # build a subsession
        #=======================================================================
 
            
        log.info('building \'%s\' w/ %s %s'%(dkey,  kwargs))
        
        from ricorde.hand_inun import HIses
        
        #filepaths
        if write:
            ofp = os.path.join(self.wrk_dir, '%s_%s.tif'%(self.layName_pfx, dkey))
        else:
            """warning: the subsession will delete its temp_dir"""
            ofp = os.path.join(self.temp_dir, '%s_%s.tif'%(self.layName_pfx, dkey))
        
        #init
        with HIses(session=self, logger=logger, inher_d=self.childI_d,
                   temp_dir = os.path.join(self.temp_dir, 'HANDses'), 
                   write=write) as wrkr:
        
            ofp, meta_d = wrkr.get_edge_samples(
                rToSamp_fp=rToSamp_fp,
                 **kwargs)
            
        
        
        
        self.ofp_d[dkey] = ofp
        self.meta_d.update({dkey:meta_d})
        
        assert self.smpl_fieldName == meta_d['smpl_fieldName']
 
            

            
        #=======================================================================
        # wrap
        #=======================================================================
        log.info('got %s \n    %s'%(fp_key, ofp))
        
        return ofp
            
    def get_sample_bounds(self, #get the min/max HAND values to use (from sample stats)
                          pts_fp,
                          
                          #data parameters
                          qhigh=0.75, 
                          qlow=0.25,
                          
                          cap=7.0, #maxiomum hval to allow (overwrite quartile
                          floor = 0.5, #minimum
                          
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
        
        log.debug('on w/ qhigh=%.2f, qlow=%.2f from %s'%(
            qhigh, qlow, os.path.basename(pts_fp)))
        
        #=======================================================================
        # get stats----
        #=======================================================================
        mstore= QgsMapLayerStore()
        #=======================================================================
        # load the layer
        #=======================================================================
        vlay_raw = self.vlay_load(pts_fp, logger=log)
        mstore.addMapLayer(vlay_raw)
        
        #===================================================================
        # get values
        #===================================================================
        df_raw = vlay_get_fdf(vlay_raw, logger=log)
        assert 'float' in df_raw[coln].dtype.name
        
        sraw = df_raw[coln].round(prec).copy()
        
        
        #===================================================================
        # upper bound
        #===================================================================
        qh = sraw.quantile(q=qhigh)
        if qh > cap:
            log.warning('q%.2f (%.2f) exceeds cap (%.2f).. using cap'%(
                qhigh, qh, cap))
            hv_max = cap
        else:
            hv_max=qh
            
        #=======================================================================
        # lower bound
        #=======================================================================
        ql = sraw.quantile(q=qlow)
        if ql < floor:
            log.warning('q%.2f (%.2f) is lower than floor (%.2f).. using floor'%(
                qlow, ql, floor))
            hv_min = floor
        else:
            hv_min=ql
            
        #=======================================================================
        # wrap
        #=======================================================================
        log.info('got hv_max=%.2f, hv_min=%.2f'%(hv_max, hv_min))
        
        self.hv_max=round(hv_max, 3)
        self.hv_min=round(hv_min, 3)
            
        self.meta_d.update({'get_sample_bounds':{'hv_max':self.hv_max, 'hv_min':self.hv_min}})
        
        return self.hv_max, self.hv_min
    
        
    def build_hmax(self, #get the hydrauilc maximum inundation from sampled HAND values
                hand_fp='',
                hval=None,
              logger=None,
              **kwargs):
        
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        log=logger.getChild('b.hmax')
 
        fp_key = 'hInun_max_fp'
        
        if hval is None: hval=self.hv_max #from get_sample_bounds()
        
        #=======================================================================
        # build
        #=======================================================================
        if not fp_key in self.fp_d:
            log.info('building \'%s\' w/ %s %s'%(fp_key, hand_fp, kwargs))
            
            ofp = os.path.join(self.out_dir, self.layName_pfx+'_hrun_imax_%03d.tif'%(hval*100))
            
            
            from ricorde.tcoms import TComs as SubSession
            
            with SubSession(session=self, logger=logger, inher_d=self.childI_d,
                            fp_d=self.fp_d) as wrkr:
            
                wrkr.get_hand_inun(hand_fp, hval,ofp=ofp, **kwargs)
                

                
            self.ofp_d[fp_key] = ofp
            #self.meta_d.update(meta_d)

        #=======================================================================
        # load
        #=======================================================================
        else:
            ofp = self.fp_d[fp_key]
                
        #=======================================================================
        # wrap
        #=======================================================================
        log.info('got %s \n    %s'%(fp_key, ofp))
        
        return ofp
    
    def build_inun2(self, #merge inun_2 with the max
                  inun1_fp,
                  hInun_max_fp,
                  
                  logger=None,
                  ):
 
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        log=logger.getChild('b.inun2r')

        fp_key = 'inun2r_fp'
 
        
        #=======================================================================
        # build
        #=======================================================================
        if not fp_key in self.fp_d:
            #===================================================================
            # setup
            #===================================================================
            log.info('maxFiltering \'%s\' with \'%s\''%(
                os.path.basename(inun1_fp),
                os.path.basename(hInun_max_fp)))
 
            #filepaths
 
            ofp = os.path.join(self.out_dir, self.layName_pfx+'_inun2r.tif')
            if os.path.exists(ofp): 
                assert self.overwrite
                os.remove(ofp)
            
            #===================================================================
            # rasterize inundation polygon
            #===================================================================

            inun2_rlay_fp = self.rasterize_inun(inun1_fp, 
                            ref_lay=hInun_max_fp,
                            ofp=os.path.join(self.temp_dir, '%s_rasterized.tif'%fp_key), #get a filepath
                                                 logger=log)
 
            #===================================================================
            # apply fillter
            #===================================================================
            self.inun_max_filter(inun2_rlay_fp, hInun_max_fp, 
                            ofp=ofp,logger=log)
            
 
            
            #===================================================================
            # wrap
            #===================================================================
            self.ofp_d[fp_key]= ofp
            
        else:
            ofp = self.fp_d[fp_key]

        
        #=======================================================================
        # wrap
        #=======================================================================
 
        
        log.info('got rectified hydrauilc maximum inundation  \'%s\' \n    %s'%(
               fp_key, ofp))
        
        return ofp
    
    def build_inun2_vlay(self,
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
    # PHASE2: Rolling HAND----------
    #===========================================================================
    
    def run_hdep_mosaic(self, #get mosaic of depths (from HAND values)
                  inun2_fp=None,
                  hand_fp=None,
                   ndb_fp=None, #nodata boundary polygon
                   inun2r_fp=None,
                   dem_fp=None,
                   
                   hval_prec=0.1,# (vertical) precision of hvals to discretize
                   
                   #cap_samples() bounds
                   hv_min=None, hv_max=None,
                   
                  logger=None,
                  ):
        

        

        
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        log=logger.getChild('rHdMo')
        start =  datetime.datetime.now()
        
        ofp_d_old = copy.copy(self.afp_d)
        #=======================================================================
        # datafile ssetup
        #=======================================================================
 
        
        if inun2_fp is None: inun2_fp=self.afp_d['inun2_fp']
        if hand_fp is None: hand_fp=self.afp_d['hand_fp']
        if ndb_fp is None: ndb_fp=self.afp_d['ndb_fp']
        if inun2r_fp is  None: inun2r_fp=self.afp_d['inun2r_fp']
        if dem_fp is None: dem_fp=self.afp_d['dem_fp']
        
        
        if hv_min is None: hv_min=self.hv_min
        if hv_max is None: hv_max=self.hv_max
 
 
        self.mstore.removeAllMapLayers() 
        #=======================================================================
        # get rolling hand values
        #=======================================================================
        """
        a raster of smoothed HAND values
            this approximates the event HAND with rolling values
        
        """
        hvgrid_fp = self.build_hvgrid(inun2_fp=inun2_fp, inun2r_fp=inun2r_fp,
                                      hand_fp=hand_fp,ndb_fp=ndb_fp,
                                      sample_spacing=None, #use default. #None=dem_psize x 5
                                      max_grade = 0.1, #maximum hand value grade to allow 
                                      hval_prec=hval_prec,
                                      hv_min=hv_min, hv_max=hv_max, #value caps
                                      logger=log)
        
        
        #=======================================================================
        # get rolling WSL
        #=======================================================================
        #build a HAND inundation for each value on the hvgrid
        hinun_pick = self.build_hiSet(hvgrid_fp=hvgrid_fp, hand_fp=hand_fp, logger=log,
                                      hval_prec=hval_prec,
                                      )
        
        #buidl the HAND WSL set
        """convert each of the above into depth rasters"""
        hwsl_pick = self.build_hwslSet(hinun_pick=hinun_pick, dem_fp=dem_fp, logger=log)
        
        #mask and mosaic to get event wsl
        """using the approriate mask derived from teh hvgrid
            mosaic togehter the corresponding HAND wsl rasters
            extents here should match the hvgrid"""
        wslM_fp = self.build_wsl(hwsl_pick=hwsl_pick, hvgrid_fp=hvgrid_fp, logger=log)
        
        #=======================================================================
        # #get depths
        #=======================================================================
        dep_fp = self.build_depths(wslM_fp=wslM_fp,dem_fp=dem_fp,inun2r_fp=inun2r_fp,
                                   logger=log)
         

        #=======================================================================
        # wrap
        #=======================================================================
        
        #get just those datalayers built by this function
        ofp_d = {k:v for k,v in self.ofp_d.items() if not k in ofp_d_old.keys()}
        log.info('built %i datalayers'%len(ofp_d))
        
        
        self._log_datafiles(d=ofp_d, log=log)
        
        return datetime.datetime.now() - start
    
        
    def build_hvgrid(self, #get gridded HAND values from some indundation
                *args,
              logger=None,
              **kwargs):
        
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        log=logger.getChild('b.hvgrid')
        
 
        fp_key = 'hvgrid_fp'
        
        #=======================================================================
        # build
        #=======================================================================
        if not fp_key in self.fp_d:
            log.info('building \'%s\' w/ %s %s'%(fp_key, args, kwargs))
            from ricorde.hand_inun import HIses as SubSession
            
            with SubSession(session=self, logger=logger, inher_d=self.childI_d,
                            fp_d=self.fp_d) as wrkr:
            
                ofp_d, meta_d = wrkr.run_hvgrid(*args, fp_key=fp_key, **kwargs)
                
            self.ofp_d.update(ofp_d)
            self.meta_d.update(meta_d) #this function is too complex to report as a block (should promote)
            ofp = ofp_d[fp_key] #pull out the focal dataset for consistency
        #=======================================================================
        # load
        #=======================================================================
        else:
            ofp = self.fp_d[fp_key]
                
        #=======================================================================
        # wrap
        #=======================================================================
        log.info('got %s \n    %s'%(fp_key, ofp))
        
        return ofp
    
    def build_hiSet(self, #get HAND derived inundations
                    *args,
              logger=None,
 
              **kwargs):
        
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        log=logger.getChild('b.hiset')
        
        fp_key = 'hinun_pick'
 
        #=======================================================================
        # build
        #=======================================================================
        if not fp_key in self.fp_d:
            log.info('building HAND inundation set \n \n')
            
            from ricorde.hand_inun import HIses as SubSession
            
            with SubSession(session=self, logger=logger, inher_d=self.childI_d,
                            fp_d=self.fp_d) as wrkr:
            
                ofp, meta_d = wrkr.run_hinunSet(*args, **kwargs)
            
            
            self.ofp_d[fp_key] = ofp
            self.meta_d.update({'run_hinunSet':meta_d})
        
        else:
            ofp = self.fp_d[fp_key]

                
        #=======================================================================
        # wrap
        #=======================================================================
        log.info('got \'%s\' \n    %s'%(fp_key, ofp))
        
        return ofp
    
    def build_hwslSet(self, #get set of HAND derived wsls (from hand inundations)
                    *args,
              logger=None,
              **kwargs):
        
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        log=logger.getChild('b.hwset')
        
        fp_key = 'hwsl_pick'
        
        #=======================================================================
        # build
        #=======================================================================
        if not fp_key in self.fp_d:
            log.info('building HAND wsl set')
            
            from ricorde.hand_inun import HIses as SubSession
            
            with SubSession(session=self, logger=log, inher_d=self.childI_d,
                            fp_d=self.fp_d) as wrkr:
            
                ofp = wrkr.run_hwslSet(*args, **kwargs)
            
            
            self.ofp_d[fp_key] = ofp
        
        else:
            ofp = self.fp_d[fp_key]

                
        #=======================================================================
        # wrap
        #=======================================================================
        log.info('got %s \n    %s'%(fp_key, ofp))
        
        return ofp
    
    def build_wsl(self, #get set of HAND derived wsls (from hand inundations)
                    *args,
                    hvgrid_uq_vals=None,
              logger=None,
              **kwargs):
        
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        log=logger.getChild('b.wsl')
        
        #pull from run_hinunSet

                
            
        
        fp_key = 'wslM_fp'
        
        #=======================================================================
        # build
        #=======================================================================
        if not fp_key in self.fp_d:
            log.info('building wsl mosaic')
            
            if hvgrid_uq_vals is None:
                if 'hvgrid_uq_vals' in self.meta_d: 
                    hvgrid_uq_vals=self.meta_d['hvgrid_uq_vals']
            
            
            
            
            from ricorde.hand_inun import HIses as SubSession
            
            with SubSession(session=self, logger=logger, inher_d=self.childI_d,
                            fp_d=self.fp_d) as wrkr:
            
                ofp = wrkr.run_wsl_mosaic(*args, 
                                          hvgrid_uq_vals=hvgrid_uq_vals,
                                          **kwargs)
 
            self.ofp_d[fp_key] = ofp
        else:
            ofp = self.fp_d[fp_key]

                
        #=======================================================================
        # wrap
        #=======================================================================
        log.info('got %s \n    %s'%(fp_key, ofp))
        
        return ofp
    
    def build_depths(self,
                     wslM_fp='',
                     dem_fp='',
                     inun2r_fp='', #hydro corrected inundation
                     resolution=None, #depth resolution. None=match dem
                     compress=None,
                     prec=3,
                     logger=None,
                     ):
        
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        log=logger.getChild('b.dep')
        if prec is None: prec=self.prec
        
        ofp = os.path.join(self.out_dir, self.layName_pfx + '_dep.tif')
        
        if os.path.exists(ofp):
            assert self.overwrite
            os.remove(ofp)
        
        if resolution is None:
            resolution = int(self.get_resolution(dem_fp))
            
        if compress is None:
            compress=self.compress
        log.info('on \'%s\' and \'%s\' with resolution=%.2f'%(
                 os.path.basename(wslM_fp), os.path.basename(dem_fp), resolution))
        #=======================================================================
        # precheck
        #=======================================================================
        assert os.path.exists(wslM_fp), wslM_fp
        assert os.path.exists(dem_fp), dem_fp
        
        #=======================================================================
        # fix resolution
        #=======================================================================
        resl_raw =  self.get_resolution(wslM_fp)
        
        if not resl_raw==float(resolution):
        
            wsl1_fp = self.warpreproject(wslM_fp, resolution=resolution,
                                         logger=log)
            
            """
            
            self.get_resolution(wsl1_fp)
            """
        
        else:
            
            wsl1_fp=wslM_fp
        #=======================================================================
        # get depths
        #=======================================================================
        dep1_fp = self.get_delta(wsl1_fp, dem_fp, logger=log)
        
        """best to force some rounding before the zero value filtering"""
        dep1b_fp = self.roundraster(dep1_fp, prec=prec, logger=log)
        
        dep2_fp = self.get_positives(dep1b_fp, logger=log)
        
        
        
        
        #=======================================================================
        # mask to only those within hydrauilc maximum (and handle compression)
        #=======================================================================
        if not compress=='none':
            dep3a_fp = self.mask_apply(dep2_fp, inun2r_fp, logger=log)
            
            dep3_fp = self.warpreproject(dep3a_fp, compression=compress, nodata_val=-9999,
                                         output=ofp, logger=log)
            
        else:
            dep3_fp = self.mask_apply(dep2_fp, inun2r_fp, logger=log, ofp=ofp)
                
        #=======================================================================
        # summary
        #=======================================================================
        stats_d = self.rasterlayerstatistics(dep3_fp)
        stats_d2 = {'range':stats_d['RANGE'], 'min':stats_d['MIN'], 'max':stats_d['MAX']}
        stats_d2 = {k:round(v,2) for k,v in stats_d2.items()}
        
        cell_cnt = self.rlay_get_cellCnt(dep3_fp)
        
        self.meta_d['build_depths'] = {**{'resolution':resolution, 'wet_cells':cell_cnt}, **stats_d2}
        #=======================================================================
        # wrap
        #=======================================================================
        
        self.ofp_d['dep_fp'] = dep3_fp
        
        
        log.info('finished on\n    %s'%dep3_fp)
        
        return dep3_fp
        
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
                if QgsRasterLayer.isValidRasterFileName(fp):
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
            
            
            self.smry_d = {**{'_smry':pd.Series(self.meta_d, name='val').to_frame()},
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
        


