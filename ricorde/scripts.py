'''Primary session object with algo methods'''
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

 
from hp.Q import Qproj, QgsCoordinateReferenceSystem, QgsMapLayerStore, \
    QgsRasterLayer, QgsWkbTypes, vlay_get_fdf, QgsVectorLayer, vlay_get_fdata, \
    vlay_get_geo, QgsMapLayer, view
    
from hp.oop import Session as baseSession
     
from ricorde.tcoms import TComs
from hp.gdal import get_nodata_val, rlay_to_array
from hp.whitebox import Whitebox
 


config_params = { #{sectionName:{varName:(mandatory_flag, ConfigParser get method)}}
     'session':{
        'aoi_fp':(False, 'get'),
        'dem_fp':(True, 'get'),
        'pwb_fp':(True, 'get'),
        'inun_fp':(True, 'get'),
        'crsid':(True, 'get'),'name':(True, 'get'),
        },
                         
    'dem':{
        'resolution':(False, 'getint'),
        },
    'pwb_rlay':{
        'resampling':(False, 'get'),
        },
    'inun_rlay':{
        'resampling':(False, 'get'),
        },
    
    'dem_hyd':{
        'dist':(False, 'getint'),
        },
    'HAND':{},
    'HAND_mask':{},
    'inun1':{
        'buff_dist':(False, 'getint')
        },
    'beach1':{},
    'b1Bounds':{
        'qhigh':(False, 'getfloat'),
        'cap':(False, 'getfloat'),
        'qlow':(False, 'getfloat'),
        'floor':(False, 'getfloat'),
        },
    'inunHmax':{
        'hval':(False, 'getfloat'),
        },
    'inun2':{},
    'beach2':{
        'method':(False, 'get'),
        },
    'hgInterp':{
        'resolution':(False, 'getint'),
        'distP':(False, 'getfloat'),
        'pts_cnt':(False, 'getint'),
        'radius':(False, 'getfloat'),
        },
    'hgRaw':{},
    'hgSmooth':{
        'resolution':(False, 'getint'),
        'max_grade':(False, 'getfloat'),
        'neighborhood_size':(False, 'getint'),
        'range_thresh':(False, 'getfloat'),
        'max_iter':(False, 'getint'),
        'precision':(False, 'getfloat'),
        },
    'hInunSet':{
        'animate':(False, 'getboolean'),
        },
    'hWslSet':{
        'max_fail_cnt':(False, 'getint'),
        },
    'wslMosaic':{},
    'depths':{
        'precision':(False, 'getint'),
        },
    
    }


#===============================================================================
# CLASSES----------
#===============================================================================
        
        
class Session(TComs, baseSession):
    """Calc session and methods for RICorDE"""
    
 
    afp_d = {}
    #special inheritance parameters for this session
    childI_d = {'Session':['aoi_vlay', 'name', 'layName_pfx', 'fp_d', 'dem_psize',
                           'hval_prec', ]}
    
    smry_d = dict() #container of frames summarizing some calcs
    meta_d = dict() #1d summary data (goes on the first page of the smry_d)_
    

    
    def __init__(self, 
 
                 #special filepath parmaeters (passable here or in 
                 aoi_fp = None, #optional area of interest polygon filepath
                 dem_fp=None, #dem rlay filepath
                 pwb_fp=None, #permanent water body filepath (raster or polygon)
                 inun_fp=None, #inundation filepath (raster or polygon)
             
                 exit_summary=True,
                 data_retrieve_hndls = None,
                 **kwargs):
        
        
        #=======================================================================
        # #retrieval handles----------
        #=======================================================================
        if data_retrieve_hndls is None: data_retrieve_hndls=dict()
        data_retrieve_hndls.update({
            'dem':{
                'compiled':lambda **kwargs:self.load_dem(**kwargs), #only rasters
                'build':lambda **kwargs:self.build_dem(dem_fp, **kwargs),
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
            'hgInterp':{
                'compiled':lambda **kwargs:self.rlay_load(**kwargs),
                'build':lambda **kwargs:self.build_hgInterp(**kwargs),
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
             
            })
        
        #attach inputs
        self.dem_fp, self.pwb_fp, self.inun_fp = dem_fp, pwb_fp, inun_fp
        self.exit_summary=exit_summary 
        self.config_params = config_params
 
        super().__init__(data_retrieve_hndls=data_retrieve_hndls, 
                         **kwargs)
        
 
        #special aoi
        """for the top-level session... letting the aoi set the projection"""
        if not aoi_fp is None:
            #get the aoi
            aoi_vlay = self.load_aoi(aoi_fp, reproj=True)
            #self.aoi_fp = aoi_fp
            
        else:
            v1 = self.layerextent(inun_fp)
            self.aoi_vlay = self.deletecolumn(v1, [f.name() for f in v1.fields()]) 
            self._check_aoi(self.aoi_vlay)
 

    #===========================================================================
    # PHASE0: Data Prep---------
    #===========================================================================
 
    def run_dataPrep(self,):
        """
        Clean and load inputs into memory.
        """
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
        self._log_datafiles()

    def build_dem(self, #checks and reprojection on the DEM
                  dem_fp, 
                  
                  #parameters
                  resolution=None, #optional resolution for resampling the DEM
                  aoi_vlay=None,
                  
                  #gen
                  dkey=None,write=None,overwrite=None,
                  ):
        """Load a DEM raster, apply any transformations, and extract parameters
        
        Notes
        ----------
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
        #if dem_fp is None: dem_fp=self.dem_fp
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
        
        rlay1, d = self.rlay_warp(rlay_raw, ref_lay=None, aoi_vlay=aoi_vlay, 
                                  decompres=True,
                               resample=resample, resolution=resolution, resampling='Average', ofp=ofp,
                               compress='none',
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
        
        if self.exit_summary: 
            self.smry_d[dkey] = pd.Series(meta_d).to_frame()
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
            assert isinstance(fp, str), type(fp)
            rlay = self.rlay_load(fp, logger=log)
            
        #=======================================================================
        # check
        #=======================================================================
        assert self.getRasterCompression(rlay.source()) is None, 'dem has some compression: %s'%rlay.name()
        assert isinstance(rlay, QgsRasterLayer), type(rlay)
        #=======================================================================
        # attach parameters
        #=======================================================================
        dem_psize = self.rlay_get_resolution(rlay)
        
        assert round(dem_psize, 0)==dem_psize, 'got bad resolution on dem: %s'%dem_psize
        
        self.dem_psize = int(dem_psize)
        
        log.info('loaded %s w/ dem_psize=%.2f'%(rlay.name(), dem_psize))
        
        return rlay 

    def build_rlay(self, 
                        fp,
                        
                        ref_lay=None,
                        aoi_vlay=None,
                        
                        resampling='Maximum',  
                        
                        clean_inun_kwargs={},
                        dkey=None,write=None,
                        ):
        """
        Build inundation raster from inundation data
        
        Used by 'pwb_rlay' and 'inun_rlay'
        
        Parameters
        ----------
        fp : str
            Filepath to raw inundation layer.
            
        resampling : str, default 'Maximum'
            gdalwarp resampling method (for polygon inputs)
        
            
        Returns
        ----------
        QgsRasterLayer
            Binary inundation raster layer.
            
        Notes
        ----------
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
        # convert to a binary mask
        #=======================================================================
        """downsampling only works on zero-type inundation layers"""
        assert_func(lambda:  self.mask_check(rlay_fp,nullType='native'), msg=dkey)
        
        rlay2 = self.fillnodata(rlay_fp, fval=0, logger=log)
        
        assert_func(lambda:  self.mask_check(rlay2,nullType='zeros'), msg=dkey)
        
        #=======================================================================
        # warp-----
        #=======================================================================
        
        rlay3, d = self.rlay_warp(rlay2, ref_lay=ref_lay, aoi_vlay=aoi_vlay, decompres=False,
                                     resampling=resampling, logger=log, 
                                     #ofp=ofp,
                                     )
 
        meta_d.update(d)
        
        #convert badztck
        rlay4_fp = self.mask_build(rlay3, logger=log, ofp=ofp)
        rlay4 = self.rlay_load(rlay4_fp, logger=log)
        #=======================================================================
        # checks
        #=======================================================================
        assert_func(lambda:  self.mask_check(rlay4, nullType='native'), msg=dkey)
        
        assert_func(lambda:self.rlay_check_match(rlay4, ref_lay, logger=log))
        
        
        
 
        if dkey == 'pwb_rlay':
            assert_func(lambda: self.check_pwb(rlay4))
        
        #=======================================================================
        # wrap
        #=======================================================================
        if self.exit_summary:
            assert not dkey in self.smry_d
            self.smry_d[dkey] = pd.Series(meta_d).to_frame()
        
 
        log.info('finished on %s'%rlay4.name())
        mstore.removeAllMapLayers()
        
        if write: self.ofp_d[dkey] = ofp
        
        return rlay4
 
    def rlay_warp(self,  #
                  input_raster, #filepath or layer
                   ref_lay=None,
                   aoi_vlay=None,
                   
                   clip=None, reproj=None, resample=None,decompres=None,
                   
                   #parameters
                   resampling='Maximum', #resampling method
                   compress=None,
                   resolution=None,
                  
                  logger=None, ofp=None,out_dir=None,
                  ):
        """special implementation of gdalwarp processing tools"""
        
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        log=logger.getChild('rlay_warp')
        
        #=======================================================================
        # retrieve
        #=======================================================================
        rlay_raw = self.get_layer(input_raster, logger=log)
        if out_dir is None: out_dir=self.temp_dir
        if ofp is None: 
            ofp=os.path.join(out_dir, '%s_warp.tif'%rlay_raw.name())
            
        if aoi_vlay is None: aoi_vlay=self.aoi_vlay
 
        mstore = QgsMapLayerStore()
        #=======================================================================
        # parameters
        #=======================================================================
        if clip is None:
            if aoi_vlay is None:
                clip =False
                #raise Error('not implemnted')
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
                
            #===================================================================
            # build dummy aoi
            #===================================================================
            """probably a better way to do this..."""
            if aoi_vlay is None:
                aoi_vlay = self.layerextent(rlay_raw)
                
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
                'OPTIONS':self.compress_d[compress],  
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
        mstore.addMapLayer(rlay2)
        if resample:
            log.info('resampling from %.4f to %.2f w/ %s'%(reso_raw, resolution, resampling))
            
            if not ref_lay is None:
                extents=ref_lay.extent()
            else:
                extents=None
            
            _ = self.warpreproject(rlay2, resolution=int(resolution), compress=compress, 
                resampling=resampling, logger=log, extents=extents,
                output=ofp)
            
        else:
            _ = self.rlay_write(rlay2, ofp=ofp, logger=log, compress=compress)
            """reloading here.. but this gives a consistent source"""
            
        rlay3 = self.rlay_load(ofp, logger=log)  
        #=======================================================================
        # checks
        #=======================================================================
        assert rlay3.source()==ofp
 
        if decompres:
            assert self.getRasterCompression(rlay3.source()) is None, 'failed to decompress'
        
        return rlay3, meta_d
    #===========================================================================
    # PHASE0: Build HAND---------
    #===========================================================================
    def run_HAND(self,logger=None,):
        """Build the HAND raster from the DEM using whiteboxtools"""
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
        if 'HAND' in self.compiled_fp_d:
            kwargs = dict()
        else:
            kwargs=dict(write_dir=self.out_dir)
            
        hand_rlay = self.retrieve('HAND', logger=log, **kwargs)  
        
        #nodata boundary of hand layer (polygon)
        hand_mask = self.retrieve('HAND_mask', logger=log) 
        
        #=======================================================================
        # wrap
        #=======================================================================
        assert_func(lambda:  self.rlay_check_match(hand_rlay,dem, logger=log))
        
        tdelta = datetime.datetime.now() - start
        
        log.info('finished in %s'%tdelta)
        self._log_datafiles()
        
        return
    
    def build_dem_hyd(self,  
                      dem_rlay=None,
                      
                      #parameters
                      dist=None,  
                      
                      #generals
                      dkey=None, logger=None,write=None,
                      ):
        """Build the hydraulically corrected DEM needed by the HAND builder
        
        Parameters
        ----------
        dist : int, optional
            Maximum search distance for breach paths in cells
            for WhiteBox.breachDepressionsLeastCost tool
            Defaults to min(int(2000/self.dem_psize), 100)
            
        Returns
        ----------
        QgsRasterLayer
            DEM hydraulically corrected with breachDepressionsLeastCost
        """
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
        """v.2+ is returning a compressed result regardless of the flag"""
        #assert self.getRasterCompression(ofp) is None, 'result has some compression: %s'%ofp
        
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
                   
                   #coms
                   write=None,logger=None,write_dir=None,

                 ):
        """
        Build the Height Above Nearest Drainage (HAND) layer
        
        Uses Whitebox.elevationAboveStream
        """

        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        log=logger.getChild('build_hand')
        assert dkey == 'HAND'
        #if dem_fp is None: dem_fp=self.dem_fp
        if write is None: write=self.write
        
        layname, ofp = self.get_outpars(dkey, write, write_dir=write_dir)
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
        """relaxing
        assert self.getRasterCompression(dem_fp) is None, 'dem has some compression: %s'%dem_fp"""
        
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
    
    def build_hand_mask(self,  
                dkey=None,
                  hand_rlay=None, 
                  logger=None,
                  write=None,
                  ):
        
        """Build the no-data boundary of the HAND rlay 
         
        Returns
        --------
        QgsRasterLayer
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
    def run_imax(self,):
        """Perform the Phase 1: Inundation Correction."""
        #=======================================================================
        # defaults
        #=======================================================================
        logger=self.logger
        log=logger.getChild('rImax')
        start =  datetime.datetime.now()
        
        self.clear_all() #release everything from memory and reset the data containers
        
        """
        self.write
        self.ofp_d
        self.compiled_fp_d
        """
        
        dem_rlay = self.retrieve('dem', logger=log) #just for checking
 
        #=======================================================================
        # add minimum water bodies to FiC inundation
        #=======================================================================
 
        
        inun1_rlay = self.retrieve('inun1', logger=log)
        
        #=======================================================================
        # get hydrauilc maximum
        #=======================================================================
        #get initial HAND beach values
        beach1_rlay=self.retrieve('beach1', logger=log)
 
        #get beach bounds
        beach1_bounds = self.retrieve('b1Bounds', logger=log)
        
        #get hydrauilc maximum
        inun_hmax = self.retrieve('inunHmax', logger=log)
        

        #=======================================================================
        # reduce inun by the hydrauilc maximum
        #=======================================================================
        #clip inun1 by hydrauilc  maximum (raster)
                #get the hand layer
        if 'inun2' in self.compiled_fp_d:
            kwargs = dict()
        else:
            kwargs=dict(write_dir=self.out_dir)
        inun2_rlay = self.retrieve('inun2',  logger=log, **kwargs) 
        

        #=======================================================================
        # wrap
        #=======================================================================
        assert_func(lambda:  self.rlay_check_match(inun2_rlay,dem_rlay, logger=log))
        
        tdelta = datetime.datetime.now() - start
        
        log.info('finished in %s'%tdelta)
        self._log_datafiles()
        return tdelta

    def build_inun1(self,             
            #layer inputs
            pwb_rlay = None,inun_rlay=None,HAND_mask=None,
 
              #parameters
              buff_dist=None, 
              
              #misc
              logger=None,write=None, dkey = None,):        
        """
        Merge layers to create the hydro corrected inundation           
        
        
        Parameters
        ----------
        pwb_rlay : QgsRasterLayer
            Permanent water bodies raster layer
        inun_rlay : QgsRasterLayer
            Primary input inundation raster layer (uncorrected)
        HAND_mask: QgsRasterLayer
            HAND layer extents
        buff_dist: int, optional
            buffer (in pixels) to apply to pwb_rlay. Defaults to pixel size of DEM*2.
            
        Returns
        ----------
        QgsRasterLayer
            Hydro-corrected inundation raster (inun1)
        """
 
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
    
    def build_beach1(self, 
            hand_rlay=None,
            inun1_rlay=None,
                
              #generals
              dkey=None,logger=None,write=None,
             ):
        """Build raster of HAND beach (shoreline) values"""
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
        """
        Compute the upper and lower bounds of the HAND beach values
        
        Parameters
        -----------
        qhigh : float, default 0.9
            Quartile to use for upper bound
        cap : float, default 7.0
            Maximum value to allow for upper bound
        qlow : float, default 0.1
            Quartile to use for lower bound
        floor : float, default 0.5
            Minimum value to allow for lower bound
        
        Returns
        -------
        dict
            Upper and lower quartiles of HAND beach values 
            {'qhi': 7.0, 'qlo': 2.225}
            
        """
        
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

    def build_hmax(self, #
                   
                   hand_rlay=None,hval=None,               
               #gen
              dkey=None, logger=None,write=None,
               ):
        """
        Build the hydraulic maximum inundation from sampled HAND values
        
        Parameters
        ----------
        hval : float, optional
            HAND value for computing the maximum inundation.
            Defaults to b1Bounds['qhi']
        
        """
        
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
              dkey=None, logger=None,write=None,write_dir=None,
                  ):
        """Filter hydraulically corrected inundations with maximum"""
 
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        if write is None: write=self.write
        log=logger.getChild('b.%s'%dkey)
 
        assert dkey=='inun2'
 
        layname, ofp = self.get_outpars(dkey, write, write_dir=write_dir)
 
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
    
    #===========================================================================
    # PHASE2: Compute Rolling HAND grid---------------
    #===========================================================================
    def run_HANDgrid(self, #
                  logger=None,
                  ):
        """Perform PHASE2: Compute Rolling HAND grid"""
 
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
        
        hgInterp_rlay = self.retrieve('hgInterp', logger=log)
        
        hgRaw_rlay = self.retrieve('hgRaw', logger=log)
        
        
        """
        a raster of smoothed HAND values
            this approximates the event HAND with rolling values
        
        """
        if 'hgSmooth' in self.compiled_fp_d:
            kwargs = dict()
        else:
            kwargs=dict(write_dir=self.out_dir)
        hvgrid = self.retrieve('hgSmooth', logger=log, **kwargs)
        
        #=======================================================================
        # wrap
        #=======================================================================
        
        assert_func(lambda:  self.rlay_check_match(hvgrid,dem_rlay, logger=log))
        
        tdelta = datetime.datetime.now() - start
        
        log.info('finished in %s'%tdelta)
        self._log_datafiles()    
        
    def build_beach2(self, #beach values (on inun2 w/ some refinement) pts vlay
             
             #datalayesr
             hand_rlay=None,
             inun2_rlay=None,
             
             #parameters
             method='pixels', #
             bounds=None,  
             fieldName='hvals',
             
             #parameters (polygon)
             spacing=None, #sample resolution
             dist=None, #distance from boundary to exclude
             
               #gen
               write_plotData=None,
              dkey=None, logger=None,write=None,
                  ):
        """
        Build the HAND beacu values from inun2
        
        Parameters
        -----------
        method : str, default 'pixels'
            method for extracting beach points from the inundation raster
            pixels: use donut raster then convert pixels to points
            polygons: convert inundation to a polygon then build edge points
            
        bounds : dict, optional
            upper and lower HAND values for filtering.
            Defaults to b1Bounds
        
        Returns
        --------
        QgsVectorLayer
            Points with sampled and filtered HAND beach values
 
        """
 
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        if write is None: write=self.write
        if write_plotData is None: write_plotData=write
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
        
        meta_d = dict()
        
        #=======================================================================
        # parameter defaults 2
        #=======================================================================
        resolution = int(self.rlay_get_resolution(hand_rlay))
        if spacing is None:
            spacing = resolution*4
            
        if dist is None:
            dist = resolution*2
        #=======================================================================
        # get raw samples----
        #=======================================================================
        #=======================================================================
        # pixel based from donut raster
        #=======================================================================
        if method=='pixels':
            #raster along edge of inundation where values match some base layer
            samp_raw_fp = self.get_beach_rlay(
                inun_rlay=inun2_rlay, base_rlay=hand_rlay, logger=log)
            
            #convert to points            
            samp_raw_pts = self.pixelstopoints(samp_raw_fp, logger=log, fieldName=fieldName)
        
        #=======================================================================
        # polygon based
        #=======================================================================
        elif method=='polygons':
            #vectorize inundation
            inun2_vlay_fp, d = self.get_inun_vlay(inun2_rlay, logger=log)
            meta_d.update(d)
            
            #setup
            temp_dir = os.path.join(self.temp_dir, dkey)
            if not os.path.exists(temp_dir): os.makedirs(temp_dir)
            
            #get these points )w/ some edge filtering
            samp_raw_pts, d = self.get_beach_pts_poly(inun2_vlay_fp, 
                  base_rlay=hand_rlay,logger=log, spacing=spacing, dist=dist, fieldName=fieldName,
                out_dir=temp_dir,)
        
            meta_d.update(d)
            
            samp_raw_pts = self.get_layer(samp_raw_pts)
            
        else:
            raise KeyError('unrecognized method')
        
        mstore.addMapLayer(samp_raw_pts)
        samp_raw_pts.setName('%s_samp_raw'%dkey)
        #=======================================================================
        # cap samples
        #=======================================================================
        samp_cap_vlay, df, d = self.get_capped_pts(samp_raw_pts, 
                                   logger=log, fieldName=fieldName,
                            vmin=bounds['qlo'], vmax=bounds['qhi'])
        meta_d.update(d)
        #=======================================================================
        # wrap
        #=======================================================================
        samp_cap_vlay.setName(layname)
        log.info('finished on %s'%samp_cap_vlay.name())
        
        if write:
            self.ofp_d[dkey] = self.vlay_write(samp_cap_vlay,ofp,  logger=log)
            log.info('wrote %s to \n    %s'%(dkey, ofp))
            
        if write_plotData:
            """for plotting later"""

            self.write_pick({'data':df, 'b1Bounds':bounds}, 
                            out_fp=os.path.join(self.out_dir, '%s_%s_hvals.pickle'%(self.layName_pfx, dkey)),
                             logger=log)
            #log.info('wrote %s to %s'%(str(df.shape), ofp))
            
        if self.exit_summary:
            self.smry_d[dkey] = pd.Series(meta_d).to_frame()
 
        return samp_cap_vlay
    
    def get_inun_vlay(self, #retrieve an inundation polygon from an inundatino raster
                      rlay_raw,
                      
                      #clean_inun_vlay parameters
                          simp_dist=None,
                        hole_size=None,
                        island_size=None,
 
                        
                      logger=None):
        
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        log=logger.getChild('get_inun_vlay')
        
        log.info('on %s'%rlay_raw.name())
        
        assert_func(lambda:  self.mask_check(rlay_raw, nullType='native'))
        
        resolution = self.rlay_get_resolution(rlay_raw)
        #=======================================================================
        # #vectorize inundation
        #=======================================================================
        vlay1_fp = self.polygonizeGDAL(rlay_raw, logger=log)
        
        #=======================================================================
        # clean
        #=======================================================================
        
        vlay2_fp, meta_d = self.clean_inun_vlay(vlay1_fp, logger=log,
                                 output=os.path.join(self.temp_dir, '%s_clean_inun.gpkg'%(rlay_raw.name())),
                                  simp_dist=simp_dist,hole_size=hole_size,island_size=island_size,
                                  dem_psize=int(resolution), #used for setting defaults
                                  )
        
        #=======================================================================
        # wrap
        #=======================================================================
        log.debug('finished on %s'%vlay2_fp)
        return vlay2_fp, meta_d
 
        
    
    def get_beach_pts_poly(self, #extract beach points from a polygon
                             vlay_raw,
                             base_rlay=None, #masking layer for edge filtering
                             fieldName=None,
                             
                             #parameters
                             spacing=None, #sample resolution
                             dist=None, #distance from boundary to exclude
                             
                             out_dir=None,
                             ofp=None,
                             logger=None):
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        log=logger.getChild('get_beach_pts_poly')
        
        mstore=QgsMapLayerStore()
        meta_d={'spacing':spacing, 'dist':dist}
        
        vlay_raw = self.get_layer(vlay_raw, mstore=mstore)
        assert isinstance(vlay_raw, QgsVectorLayer)
        
        if out_dir is None:
            out_dir=self.out_dir
        
        
        if ofp is None:
            ofp = os.path.join(out_dir, '%s_beachPtsRaw.gpkg'%vlay_raw.name())
            
        log.debug('on %s'%vlay_raw)
        #===================================================================
        # points along edge
        #===================================================================

        pts_vlay1 = self.pointsalonglines(vlay_raw, logger=log, spacing=spacing)
        """
        view(pts_vlay_raw)
        view(pts_vlay1)
        """
        meta_d['cnt_raw'] = pts_vlay1.dataProvider().featureCount()
        #===================================================================
        # #fix fid
        #===================================================================
        #remove all the fields

        pts_vlay2 = self.deletecolumn(pts_vlay1, 
                                      [f.name() for f in pts_vlay1.fields()], 
                                      output=os.path.join(out_dir, '01_pointsalonglines.gpkg'),
                                      logger=log)
        
        
        #===================================================================
        # #filter by raster edge
        #===================================================================
        #retrieve a mask for filtering
        """easier to just rebuild here"""
        mask_rlay = self.mask_build(base_rlay, logger=log, zero_shift=True)
        
        pts_clean_vlay_fp, fcnt = self.filter_edge_pts(pts_vlay2, mask_rlay, logger=log, dist=dist,
                                               ofp=os.path.join(out_dir, '02_filter_edge_pts.gpkg'),
                                        )
        
        #=======================================================================
        # sample raster
        #=======================================================================
        sample_vlay1 = self.rastersampling(pts_clean_vlay_fp, base_rlay, logger=log)
        mstore.addMapLayer(sample_vlay1)
        """
        base_rlay.source()
        view(sample_vlay2)
        sample_vlay2.source()
        """
        
        sample_vlay2 = self.renameField(sample_vlay1, 'sample_1', fieldName,
                                        output=os.path.join(out_dir, '03_rastersamples.gpkg')
                                        )
        
        #check
        
        #=======================================================================
        # wrap
        #=======================================================================
        
        
        meta_d['cnt_edgeFilter'] = fcnt
        
        mstore.removeAllMapLayers()
        
        log.debug('finished on %s'%sample_vlay2)
        
        return sample_vlay2, meta_d
        
    def filter_edge_pts(self, #filter points where close to a rlay's no-data boundary
                        pts_vlay,
                        mask_rlay,
                        dist=None, #distance from boundary to exclude
                        ofp=None, #optional outpath
                        logger=None,
                        ):
        """todo: incorpoarte this into get_beach_pts_poly"""
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        log=logger.getChild('filter_edge_pts')
        
        mstore=QgsMapLayerStore()
        pts_vlay = self.get_layer(pts_vlay, mstore=mstore)
        if ofp is None:
            ofp = os.path.join(self.temp_dir, '%s_edgePtsFiltered.gpkg'%pts_vlay.name())
        
        #=======================================================================
        # build selection donut
        #=======================================================================
        #polygonize buffer
        mask_vlay_fp = self.polygonizeGDAL(mask_rlay, logger=log)
        
        
        #outer buffer of no-data poly
        nd_big_vlay = self.buffer(mask_vlay_fp, dist=dist, dissolve=True, logger=log)
        
        #inner buffer of no-data poly
        nd_sml_vlay = self.buffer(mask_vlay_fp, dist=-dist, dissolve=True, logger=log)
        
        #outer-inner donut no-data poly
        nd_donut_vlay = self.symmetricaldifference(nd_big_vlay, nd_sml_vlay, logger=log)
        
        lays = [nd_big_vlay, nd_sml_vlay, nd_donut_vlay]
        
        #=======================================================================
        # #apply fiolter
        #=======================================================================
        #select points intersecting donut
 
        lays.append(pts_vlay)
        
        self.selectbylocation(pts_vlay, nd_donut_vlay, allow_none=False, logger=log)
        
        """
        view(pts_vlay)
        """
        
        #invert selection
        pts_vlay.invertSelection()
        
        #exctract remaining points
        pts_clean_vlay_fp = self.saveselectedfeatures(pts_vlay, logger=log,
                          output=ofp)
        
        #=======================================================================
        # wrap
        #=======================================================================
        mstore.addMapLayers(lays)
        

        fcnt = pts_vlay.selectedFeatureCount()
        log.info('selected %i (of %i) pts by raster no-data extent'%(
            fcnt, pts_vlay.dataProvider().featureCount()))
        
        mstore.removeAllMapLayers()
        
        return pts_clean_vlay_fp, fcnt
 
        
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
    
    def build_hgInterp(self, #
             
             #datalayesr
             beach2_vlay=None,
             dem_rlay=None, #for reference

             fieldName=None, #field name with sample values
             
             #parameters
             resolution = None,distP=2.0, pts_cnt = 5, radius=None,  
             max_procs=4,
             
               #gen
              dkey=None, logger=None,write=None,
                  ):
        """
        Build an interpolated surface from  beach 2 point HAND values
        
        Parameters
        -----------
        resolution: int, optional
            base resolution for output.
            Defaults to dem pixel size x 2
            
        distP: float, default 2.0
            distance coefficient for whitebox.IdwInterpolation (unitless?)
            
        pts_cnt: int, default 5
            number of points to include in search for whitebox.IdwInterpolation(min_point)
            
        radius: float, optional
            Search Radius in map units (larger is faster) for whitebox.IdwInterpolation
            Defaults to resolution*6
            
        Returns
        --------
        QgsRasterLayer
            Interpolated beach HAND values
        
        """
        """
        
        2022-03-28:
            changed to use wbt
 
            
        because we're using rasters instead of polys there are way more points to interpolate
            and this is much slower
            added a new polygonize method to beach2
 
            
 
        divide layer?
            wbt already parallelizes... so not sure this would be worth it
            
        resolution
            since we've already sampled, and were dealing with HAND values
            there isn't much of an accuracy loss in lowering the resolution
            the wbt interp function seems to hang for large jobs
            
        TODO: test all the idw algorhithims again. give the user options
 
        """
 
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        if write is None: write=self.write
        log=logger.getChild('b.%s'%dkey)
 
        assert dkey=='hgInterp'
 
        layname, ofp = self.get_outpars(dkey, write)
 
        #=======================================================================
        # retrieve
        #=======================================================================
        if dem_rlay is None:
            dem_rlay = self.retrieve('dem')
            
        if beach2_vlay is None:
            beach2_vlay=self.retrieve('beach2')
            
        if resolution is None:
            resolution=int(self.rlay_get_resolution(dem_rlay))*3
        assert isinstance(resolution, int)
        #=======================================================================
        # parameters 2
        #=======================================================================
        if fieldName is None:
            fnl = [f.name() for f  in beach2_vlay.fields() if not f.name()=='fid']
            assert len(fnl)==1
            fieldName = fnl[0]
        
        if radius is None:
            radius=resolution*6
            
        meta_d = {'distP':distP, 'pts_cnt':pts_cnt, 'radius':radius, 'resolution':resolution}
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
        """still very slow... may be the ref_lay"""
        interp1_fp = Whitebox(logger=logger, 
                              #version='v2.0.0', #1.4 wont cap processors
                                 max_procs=max_procs, 
                               ).IdwInterpolation(shp_fp, fieldName,
                                weight=distP, 
                                radius=radius,
                                min_points=pts_cnt,
                                cell_size=resolution,
                                #ref_lay_fp=dem_rlay.source(),
                                out_fp=os.path.join(self.temp_dir, 'wbt_IdwInterpolation_raw_%s.tif'%dkey))
 
        """GRASS.. a bit slow
        #=======================================================================
        # interp_raw_fp = self.vSurfIdw(beach2_vlay, fieldName, distP=distP,
        #               pts_cnt=pts_cnt, cell_size=resolution, extents=dem_rlay.extent(),
        #               logger=log, 
        #               output=os.path.join(self.temp_dir, 'vsurfidw.tif'),
        #               )
        #======================================================================="""
        assert os.path.exists(interp1_fp)
        
        #reproject
        interp2_fp = self.warpreproject(interp1_fp, resolution=int(resolution), 
                                     logger=log, extents=dem_rlay.extent(), 
                                     crsOut=self.qproj.crs(), crsIn=self.qproj.crs(),
                                     output=ofp)
 
        #=======================================================================
        # wrap
        #=======================================================================
        rlay = self.rlay_load(interp2_fp, logger=log)
        
        """not forcing a match any more
        assert_func(lambda:  self.rlay_check_match(rlay,dem_rlay, logger=log))"""
 
        if write:
            self.ofp_d[dkey] = ofp
 
        if self.exit_summary:
            self.smry_d[dkey] = pd.Series(meta_d).to_frame()
 
        return rlay
 
    def build_hgRaw(self,
                #input layers
                hgInterp_rlay=None,inun2_rlay=None,
                
               #gen
              dkey=None, logger=None,write=None,
                  ):
        """Grow the interpolated HAND values onto the interior"""
    
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        if write is None: write=self.write
        log=logger.getChild('b.%s'%dkey)
 
        assert dkey=='hgRaw'
 
        layname, ofp = self.get_outpars(dkey, write)
        
        if inun2_rlay is None:
            inun2_rlay=self.retrieve('inun2')
            
        if hgInterp_rlay is None:
            hgInterp_rlay=self.retrieve('hgInterp')
      
        meta_d = dict()
        mstore = QgsMapLayerStore()
        #=======================================================================
        # resolution match
        #=======================================================================
        """
        hgInterp is often a lower resolution than the inundation
            but we want to preserve inundation resolution for the grow
            TODO: investigate relaxing this 
            
            NOTE: hgSmooth allows a second coarsening
        """
        inun2_res = self.rlay_get_resolution(inun2_rlay)
        hgInterp_res = self.rlay_get_resolution(hgInterp_rlay)
        
        assert inun2_res<=hgInterp_res, 'hgInterp must have a lower (coarser) resolution than the inundation'
        
        if not inun2_res==hgInterp_res:
            log.info('resolution mismatch... reprojecting \'%s\' (%i to %i)'%(
                hgInterp_rlay.name(), hgInterp_res, inun2_res))
            
            hgInterp2_rlay_fp = self.warpreproject(hgInterp_rlay, extents=inun2_rlay.extent(), 
                                                   resolution=int(inun2_res), logger=log)
 
        else:
            hgInterp2_rlay_fp = hgInterp_rlay.source()
        
        #=======================================================================
        # #re-interpolate interior regions-----
        #=======================================================================
        self.wsl_extrap_wbt(hgInterp2_rlay_fp,inun2_rlay.source(),  logger=log, ofp=ofp)
 
        #=======================================================================
        # wrap
        #=======================================================================
        rlay = self.rlay_load(ofp, logger=log)
        
        if write:
            self.ofp_d[dkey] = ofp
 
        if self.exit_summary:
            self.smry_d[dkey] = pd.Series(meta_d).to_frame()
            
        mstore.removeAllMapLayers()
 
        return rlay
        
    def build_hgSmooth(self, #
             
             #datalayesr
             hgRaw_vlay=None,
             
             #parameters
              resolution=None,  
               max_grade = 0.1,  
               neighborhood_size = 7,
              range_thresh=None,  
             max_iter=5,
             precision=0.2,  
             
               #gen
              dkey=None, logger=None,write=None, debug=False,write_dir=None,
                  ):
        """
        Smooth the rolling HAND grid using grass7:r.neighbors
        
        Parameters
        ----------
        resolution: int, optional
            Resolution for rNeigbour averaging. not output.
            Defaults to input raster resolution *3
            
        max_grade: float, default 0.1
            maximum hand value grade to allow 
            
        neighborhood_size: int, default 7
            neighbourhood size for grass7:r.neighbors
            
        range_thresh: float, optional
            maximum range (between HAND cell values) to allow. should no exceed 2.0.
            Defaults to min(max_grade*resolution, 2.0),2)
            NOTE: this is also used to spatially select where the smoothing applies,
            so it will change the result. lower values mean smoother. 
            
        max_iter: int, default 5
            maximum number of smoothing iterations to allow
            
        precision: float, default 0.2
            precision of resulting HAND values (value to round to nearest multiple of)
        
        
        """
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
        
        layname, ofp = self.get_outpars(dkey, write, write_dir=write_dir)
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
                    just makes the smoothing iterations faster
                    NOTE: we also allow the hgInterp to be downsampled (but this is reverted in hgRaw
                    """
            resolution = int(self.rlay_get_resolution(hgRaw_vlay)*3)
            
        if range_thresh is  None:
            """capped at 2.0 for low resolution runs""" 
            range_thresh = round(min(max_grade*resolution, 2.0),2)
            
        assert range_thresh<=2.0
            
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
                    
                    max_iter=None, 
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
        assert isinstance(resolution, int)
        
        assert isinstance(range_thresh, float)
        assert isinstance(max_iter, int)
 
        #===================================================================
        # smooth initial
        #===================================================================
        smooth_rlay_fp1 = self.rNeighbors(rlay_raw,
                        neighborhood_size=neighborhood_size, 
                        circular_neighborhood=circular_neighborhood,
                        cell_size=resolution,
                        #output=ofp, 
                        logger=log)
        
        
        assert os.path.exists(smooth_rlay_fp1)
        
        #reproject
        """this has a new extents and a sloppy resolution
            cleaning resolution and setting back to og extent
            self.rlay_get_resolution(smooth_rlay_fp2)
            """
        smooth_rlay_fp2 =  self.warpreproject(smooth_rlay_fp1, 
                           resolution=resolution, 
                           extents=rlay_raw.extent(), logger=log, output=ofp)
        #===================================================================
        # get mask
        #===================================================================
        """
        getting a new mask from teh smoothed as this has grown outward
        """
        mask_fp = self.mask_build(smooth_rlay_fp2, logger=log,
                        ofp=os.path.join(self.temp_dir, 'rsmooth_mask.tif'))
        
        """NO! allowing downsampling
        assert_func(lambda:  self.rlay_check_match(mask_fp,rlay_raw, logger=log))"""
        #===================================================================
        # smooth loop-----
        #===================================================================
        #===================================================================
        # setup
        #===================================================================
        rlay_fp_i=smooth_rlay_fp2
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
            assert_func(lambda:  self.rlay_check_match(rlay_fp_i,mask_fp, logger=log))
            
            try:
                check, rlay_fp_i, rvali, fail_pct = self._smooth_iter(rlay_fp_i, 
                                                           range_thresh=range_thresh,
                                                           mask=mask_fp,
                                                           logger=log.getChild(str(i)),
                                                           out_dir=temp_dir,sfx='%03d'%i,debug=debug,
                                                           )
            except Exception as e:
                """why do we allow this to fail???"""
                if i==0: 
                    raise Error('_smooth_iter failed on first iteration w/\n    %s'%e)
                log.warning('_smooth_iter %i failed w/ \n    %s'%(i, e))
                fail_cnt=10 #????
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
            
            
            log.warning('range exceeds thresh after %i (%.2f>%.2f). taking i=%i\n    %s'%(
                i,rvali, range_thresh, imin, df['rval'].to_dict()))
        
        """
        view(df)
        """
        #===================================================================
        # post
        #===================================================================
        assert os.path.exists(rlay_fp_i), 'failed to get a result: %s'%rlay_fp_i
        #repeoject to extents and original resolution
        """TODO: consider relaxing this... doing lots of downsampling already"""
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
                    range_thresh=None,
                    neighborhood_size=3, #hardcoded
                    #circular_neighborhood=True,
                    mask=None,
                    sfx='',
                    out_dir=None,logger=None,debug=False,
                    ):
        """
        spent (more than) a few hours on this
            theres probably a nicer pre-buit algo I should be using
            
        most parameter configurations return the smoothest result on iter=2
        """
        
        #=======================================================================
        # defaults
        #=======================================================================
        assert isinstance(range_thresh, float)
        assert isinstance(neighborhood_size, int)
        
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
            
        mstore=QgsMapLayerStore()
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
            log.info('maximum range (%.2f) < %.2f achieved'%(rval, range_thresh))
            
            
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
        
        #=======================================================================
        # reproject
        #=======================================================================
        """this probably makes things slower... but cleaner to force all this to match"""
        base_rlay = self.get_layer(rlay_fp, mstore=mstore, logger=log)
        
        
        ofp = os.path.join(os.path.dirname(smooth_fp), '%s_avg.tif'%sfx)
        #os.rename(smooth_fp,ofp)
        
        """
        self.rlay_check_match(smooth_fp,base_rlay, logger=log)
        self.rlay_get_props(smooth_fp)
        """
        
        self.warpreproject(smooth_fp, 
                           resolution=int(self.rlay_get_resolution(base_rlay)), 
                           extents=base_rlay.extent(), logger=log, output=ofp)
        
        
        assert_func(lambda:  self.rlay_check_match(ofp,base_rlay, logger=log))
        #=======================================================================
        # wrap
        #=======================================================================
        #copy over
        if debug:
            shutil.copyfile(ofp,os.path.join(out_dir,'avg','%s_avg.tif'%sfx))
        
        mstore.removeAllMapLayers()
        return False, ofp, rval, fail_cnt
    

    #===========================================================================
    # PHASE3: Rolling WSL grid-----------
    #=========================================================================== 
    def run_wslRoll(self,):
        """Perform PHASE3: Rolling WSL grid"""
        
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
        hwsl_pick = self.retrieve('hWslSet', logger=log)
        
 
        #mask and mosaic to get event wsl
        """using the approriate mask derived from teh hvgrid
            mosaic togehter the corresponding HAND wsl rasters
            extents here should match the hvgrid"""
            
        wsl_rlay = self.retrieve('wslMosaic', logger=log, write_dir=self.out_dir)
            
        #=======================================================================
        # wrap
        #=======================================================================
        dem_rlay = self.retrieve('HAND', logger=log) #just for checking
        
        assert dem_rlay.extent()==wsl_rlay.extent()
        
        tdelta = datetime.datetime.now() - start
        
        log.info('finished in %s'%tdelta)
        self._log_datafiles()
 
 
    def build_hiSet(self,  
                hgSmooth_rlay=None,hand_rlay=None,
                
                #parameters
                resolution=None,
                
                #output control
                animate=False,
                                
               #gen
              dkey=None, logger=None,write=None, compress=None,
              relative=None,  
                  ): 
        """
        Build set of HAND derived inundation from hgSmooth
        
        Builds one inundation raster for each value found in 
        hgSmooth using the pre-calculated HAND layer
        
        
        Parameters
        ----------
        hgSmooth_rlay: QgsRasterLayer
            Best estimate of HAND value for flooding in each pixel.        
        hand_rlay: QgsRasterLayer
            HAND layer from which to build inundations.            
        resolution: int, optional
            Resolution to use when computing each inundation raster.
            Defaults to resolution of hand_rlay.
        relative: bool, optional
            Filepath behavior (defaults to self).
        animate: bool, default False
            Flag to create animations of outputs. 
            
        Returns
        ----------
        hInunSet: dict
            Filepaths of each inundation raster created {hval:fp}
 
        
        """
        
        """
        TODO: performance improvements
            parallelize
 
 
            different raster format? netCDF
        """
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        if write is None: write=self.write
        if relative is None: relative=self.relative

        log=logger.getChild('b.%s'%dkey)
 
        assert dkey=='hInunSet'
        
        
        meta_d = dict()
        
        
        #=======================================================================
        # setup
        #=======================================================================
        layname, ofp = self.get_outpars(dkey, write, ext='.pickle')
        #directory
        if write:
            out_dir = os.path.join(self.wrk_dir, dkey)
        else:
            out_dir=os.path.join(self.temp_dir, dkey)
            
        temp_dir = os.path.join(self.temp_dir, dkey)
        
        if not os.path.exists(out_dir):os.makedirs(out_dir)
        if not os.path.exists(temp_dir):os.makedirs(temp_dir)
        
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
            raise Error('not sure about htis')
            #===================================================================
            # log.info('downsampling \'%s\' from %.2f to %.2f'%(
            #     hand_rlay.name(), hres,  resolution))
            #  
            # hand1_rlay = self.warpreproject(hand_rlay, resolution=resolution,
            #                                logger=log)
            # 
            # mstore.addMapLayer(hand1_rlay)
            # assert hand1_rlay.extent()==hand_rlay.extent()
            #===================================================================
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
    
        if __debug__: #quick check on the last layer
            inun_rlay_i = self.rlay_load(rlay_fp, logger=log, mstore=mstore)
            
            assert inun_rlay_i.extent()==hand_rlay.extent(), 'resulting inundation extents do not match'
 
            
        #===================================================================
        # build animations
        #===================================================================
        if animate:
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
        
        mstore.removeAllMapLayers()
        #reshape metadata
        df = pd.DataFrame.from_dict(res_d, orient='index')
        
        if self.exit_summary:
            self.smry_d[dkey] = pd.Series(meta_d).to_frame()
            self.smry_d['%s_stats'%dkey] = df       
        
        #=======================================================================
        # write pickle of filepaths
        #=======================================================================
        res_d2 = df.set_index('hval')['fp'].to_dict()


        if write:
            self.ofp_d[dkey] = self.write_pick(res_d2, ofp, logger=log, relative=relative)
 
 
        return res_d2
    
 
    
    def build_hwslSet(self, #
                #input layers
                hi_fp_d=None,
                dem_rlay=None,
                
                #parameters
                max_fail_cnt=5, #
               #gen
              dkey=None, logger=None,write=None,  
              compress=None,relative=None
                  ):
        """
        Builds one WSL raster for each inundation layer using a grow routine.        
        
        Parameters
        ----------
        hi_fp_d: dict, optional
            hInunSet. Filepaths of inundation rasters {hval:fp}.
            Defaults to retrieve.       
        dem_rlay: QgsRasterLayer, optional
            DEM raster.
            Defaults to retrieve.
        max_fail_cnt: int, default 5
            Maximum number of wsl failures to allow 
        relative: bool, optional
            Filepath behavior (defaults to self).
 
            
        Returns
        ----------
        hWslSet: dict
            HandValue per filepath of WSL raster {hval:fp}
            
        Notes
        ----------
        resolution is taken from hInunSet layers
        
        """

 
 
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
        temp_dir = os.path.join(self.temp_dir, dkey)
        if not os.path.exists(out_dir):os.makedirs(out_dir)
        if not os.path.exists(temp_dir):os.makedirs(temp_dir)
        
        mstore = QgsMapLayerStore()
        
        #=======================================================================
        # retrieve
        #=======================================================================
        """NOTE: resolutions are allowed to not match (see build_hiSet())"""
        if hi_fp_d is None:
            hi_fp_d = self.retrieve('hInunSet', relative=relative)
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
            dem1_rlay = self.rlay_warp(dem_rlay, ref_lay=ref_lay, logger=log, out_dir=temp_dir)
            
        else:
            dem1_rlay = dem_rlay

        assert_func(lambda:  self.rlay_check_match(ref_lay, dem1_rlay, logger=log), 
                    msg='HANDinun resolution does not match dem')
 
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
                            out_dir=os.path.join(temp_dir, dkey, str(i)), #dumping iter layers
                            compress=compress,)
                
                #smooth
                """would result in some negative depths? moved to the wsl mosaic"""
 
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
            
        if len(res_d)==0:
            raise Error('failed to generate any WSLs')
        #===================================================================
        # build animations
        #===================================================================
        """not showing up in the gifs for some reason"""
 
        #=======================================================================
        # wrap
        #=======================================================================
        meta_d.update({'fail_cnt':fail_cnt})
        
        df = pd.DataFrame.from_dict(res_d, orient='index')
        
        if self.exit_summary:
            self.smry_d[dkey] = pd.Series(meta_d).to_frame()
            self.smry_d['%s_stats'%dkey] = df
 
        mstore.removeAllMapLayers()

        #=======================================================================
        # write pickle of filepaths
        #=======================================================================
        """only taking those w/ successfulr asters"""
        res2_d = df[df['error'].isna()].set_index('hval', drop=True)['fp'].to_dict()

        if write:
            self.ofp_d[dkey] = self.write_pick(res2_d, ofp, logger=log, relative=relative)
 
        return res2_d
    
    def build_wsl(self,
                #input layers
                hwsl_fp_d=None,hgSmooth_rlay=None, 
                
                #parameters
                hvgrid_uq_vals=None,
 
               #gen
              dkey=None, logger=None,write=None,  write_dir=None,compress=None,
              relative=True):
        """
        Construct a WSL mosiac by filling with lookup HAND values.
        
        Using the HAND values grid representing the flood (hgSmooth), select
        the approriate WSL value for each pixel by referencing the corresponding
        WSL raster from hWslSet.
        
        
        Parameters
        ----------
        hwsl_fp_d: dict, optional
            hWslSet. Filepaths of WSL rasters {hval:fp}.
            Defaults to retrieve.       
        hgSmooth_rlay: QgsRasterLayer, optional
            hgSmooth. Best estimate of HAND value for flooding in each pixel.
            Defaults to retrieve.
        hvgrid_uq_vals: dict, optional
            Container of unique values found on hgSmooth. Defaults to re-calculating. 
 
            
        Returns
        ----------
        wslMosaic: QgsRasterLayer
            WSL raster mosaiked from underlying HAND values
            
        Notes
        ----------
 
        
        """
 
 
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        if write is None: write=self.write

        log=logger.getChild('b.%s'%dkey)
 
        assert dkey=='wslMosaic'
        
        layname, ofp = self.get_outpars(dkey, write, ext='.tif', write_dir=write_dir)
        meta_d = dict()
        
        #retrieve from meta if we already have this
        if hvgrid_uq_vals is None:
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
            hwsl_fp_d = self.retrieve('hWslSet', relative=relative)
        
            
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
                          ofp=os.path.join(temp_dir, 'mask','mask_i_%03d_%03d.tif'%(i, hval*100)),
                          out_dir=temp_dir,  
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
                    logger=log, out_dir=temp_dir,temp_dir=temp_dir,
                    ofp=os.path.join(temp_dir, 'mask','mask_dnt_%03d_%03d.tif'%(i, hval*100)),
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
                                  out_dir=temp_dir, temp_dir=temp_dir
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
    # PHASE4: Final Depth-----------
    #===========================================================================
    def run_depths(self,):
        """PHASE4: Resultant Depths computation"""
        
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
 
        #=======================================================================
        # wrap
        #=======================================================================
        dem_rlay = self.retrieve('HAND', logger=log) #just for checking
        
        assert dem_rlay.extent()==dep_rlay.extent()
        
        tdelta = datetime.datetime.now() - start
        
        log.info('finished in %s'%tdelta)
        #self._log_datafiles()   no... this will happen on exit
 
    
    def build_depths(self,
                 wslM_rlay=None,dem_rlay=None,inun2_rlay=None,precision=1, 
 
               #gen
              dkey=None, logger=None,write=None,  write_dir=None,
              compress=None, ):
        """
        Construct the depths from DEM and WSL mosaic
        
        
        Parameters
        ----------
        wslM_rlay: QgsRasterLayer, optional
            wslMosaic. WSL raster. defaults to retrieve.        
        dem_rlay: QgsRasterLayer, optional
            dem. Elevations. Defaults to retrieve.           
        inun2_rlay: QgsRasterLayer, optional
            inun2. Maximum inundation. Used to mask results. Defaults to retrieve.  
        precision: int, default 1
            Rounding to apply for delta calculation. 
            
        Returns
        ----------
        QgsRasterLayer
            Computed raster of depths values
 
        Notes
        ----------
        output properties (e.g., resolution) will match the wslM_rlay.
        defaults to writing in the 'out_dir' (not the working)
        """
        """
        
        TODO: fix the nodata type
        """
 
 
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        if write is None: write=self.write

        log=logger.getChild('b.%s'%dkey)
 
        assert dkey=='depths'
        if write_dir is None: write_dir=self.out_dir
        layname, ofp = self.get_outpars(dkey, write, ext='.tif', write_dir=write_dir)
        
      
        temp_dir = os.path.join(self.temp_dir, dkey)
        if not os.path.exists(temp_dir):os.makedirs(temp_dir)
                
        mstore=QgsMapLayerStore()
        
        if compress is None:
            compress=self.compress
 
        meta_d = {'precision': precision, 'compress':compress}
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
        # mask to only those within hydrauilc maximum 
        #======================================================================= 
        dep3_fp = self.mask_apply(dep2_fp, inun2_rlay, logger=log,  
                                  allow_approximate=True, #really?
                                  )
        
        #=======================================================================
        # clean up layer
        #=======================================================================
        dep4_fp = self.warpreproject(dep3_fp, compress=compress, output=ofp, logger=log)
                
        #=======================================================================
        # summary
        #=======================================================================
        rlay = self.rlay_load(dep4_fp, logger=log)
        
        stats_d = self.rasterlayerstatistics(rlay)
        stats_d2 = {'range':stats_d['RANGE'], 'min':stats_d['MIN'], 'max':stats_d['MAX']}
        stats_d2 = {k:round(v,2) for k,v in stats_d2.items()}
        
        cell_cnt = self.rlay_get_cellCnt(dep3_fp)
        
        meta_d.update({**{'resolution':self.rlay_get_resolution(rlay), 'wet_cells':cell_cnt}, **stats_d2})
        
        #=======================================================================
        # wrap
        #=======================================================================
        log.info('finished on %s \n    %s'%(rlay.name(), meta_d))
        if write:self.ofp_d[dkey] = rlay.source()            
 
        if self.exit_summary:
            self.smry_d[dkey] = pd.Series(meta_d, dtype=str).to_frame()
 
 
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
        
    def get_outpars(self, dkey, write, ext='.tif', write_dir=None,
                    ):
        
        layname = '%s_%s'%(self.layName_pfx, dkey) 
        if write:
            if write_dir is None:
                write_dir=self.wrk_dir
            
            ofp = os.path.join(write_dir, layname+ext)
            
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
            #self._log_datafiles()
            
            
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
        


