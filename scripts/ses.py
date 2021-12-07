'''
Created on Mar. 27, 2021

@author: cefect

workflows for deriving gridded depth estimates from inundation polygons and DEMs

setup to execute 1 workflow at a time (output 1 depth raster)
    see validate.py for calculating the performance of these outputs
    
2021-07-20: revised to gridded-hand values with mosaiced DEPTHS
    see branch '20210720_hva_grid'


#===============================================================================
# passing layers vs. filepaths
#===============================================================================
some algos natively input/output QgsLayers and others filepaths
    filepaths are easier to de-bug (can open in QGIS)
    QgsLayers are easier to clean and code (can extract info)
    
PROCEDURE GUIDE
    passing inputs between functions (excluding coms and _helpers): 
        filepaths
    layers within a function
        QgsLayers or filepaths
    


'''
#===============================================================================
# imports-----------
#===============================================================================
import os, datetime, copy
import pandas as pd
 
start =  datetime.datetime.now()

 


from hp.exceptions import Error
from hp.dirz import force_open_dir
 

from hp.Q import Qproj, QgsCoordinateReferenceSystem, QgsMapLayerStore, \
    QgsRasterLayer, QgsWkbTypes, vlay_get_fdf
     
from scripts.tcoms import TComs
from hp.gdal import get_nodata_val

#===============================================================================
# CLASSES----------
#===============================================================================
        
        
class Session(TComs):
    
 
    
    #special inheritance parameters for this session
    childI_d = {'Session':['aoi_vlay', 'out_dir', 'name', 'layName_pfx', 'fp_d', 'dem_psize',
                           'hval_prec', 'temp_dir', 'init_plt_d', 'plot']}
    
    smry_d = dict() #container of frames summarizing some calcs
    meta_d = dict() #1d summary data (goes on the first page of the smry_d)_
    
    def __init__(self, 
                 tag='DR',
                 aoi_fp = None,
                 figsize     = (6.5, 6.5), 
                 **kwargs):
        

            
            
        
        super().__init__(tag=tag,figsize=figsize, **kwargs)
        
 
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
    # Download and Pre-Processing----------
    #===========================================================================
    
    def run_get_data(self, #common data retrival workflow
                      
        #HRDEM
        resolution=None,
        #buildingFootPrint kwargs
        #prov='quebec',
        
        #FiC kwargs
           min_dt=datetime.datetime.strptime('2017-05-05', '%Y-%m-%d'),
           max_dt=datetime.datetime.strptime('2017-05-14', '%Y-%m-%d'),
           
        #NHN kwargs
        waterTypes = ['Watercourse'],
        
        #general kwargs
        aoi_fp=None,
        logger=None
        ):
        """
        These are all (mostly) independent data downloading/pre-processing
            loading/building further down the datastream should be elsewhere
        
        TODO: paralleleize these calls"""
        
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        log=logger.getChild('rgd')
    
        if aoi_fp is None: aoi_fp=self.aoi_fp
        assert os.path.exists(aoi_fp), 'got invalid aoi_fp: \'%s\''%aoi_fp

        #=======================================================================
        # FloodsInCanada
        #=======================================================================
        self.load_fic(logger=log, min_dt=min_dt, max_dt=max_dt)
        

        #=======================================================================
        # NHN
        #=======================================================================
        self.load_nhn(waterTypes=waterTypes, aoi_fp=aoi_fp, logger=log)
        
        
        #=======================================================================
        # HRDEM
        #=======================================================================
        self.load_hrdem(logger=log, aoi_fp=aoi_fp,resolution=resolution)
        """TODO: refine AOI to zones with HRDEM coverage"""

        
        #===========================================================================
        # building footprints
        #===========================================================================
        #self.load_bfp(prov=prov, logger=log)
 
 
        #=======================================================================
        # wrap
        #=======================================================================
        #promote meta
        self._promote_meta('01run_get_data')
        
        
        
        d = self.ofp_d
        log.info('%i data files built \n    %s'%(
            len(d), list(d.keys())))
 
 
        
        self.afp_d = {**self.fp_d, **self.ofp_d} #fp_d overwritten by ofp_d

 
        return self.afp_d
    


    
    def load_bfp(self, #building footprints
                prov='quebec',
                logger=None):
        
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        log=logger.getChild('load_bfp')
        
        #=======================================================================
        # build from scratch
        #=======================================================================
        if not 'bfp_fp' in self.fp_d:
            log.info('bulilding bfps')
            
            from data_collect.microsoftBldgFt import mbfpSession
            
            with mbfpSession(session=self, logger=logger, inher_d=self.childI_d) as wrkr:
                # get filepaths
                fp_d = wrkr.get_microsoft_fps()
            
                # load and slice
                assert prov in fp_d, prov
                fp =  wrkr.get_mbfp_pts(fp_d[prov], aoi_vlay=self.aoi_vlay, logger=log)
            
            self.ofp_d['bfp_fp'] = fp #add to outputs container

        #=======================================================================
        # load pre-built
        #=======================================================================
        else:
            log.info('passed sliced bfps... loading')
            fp = self.fp_d['bfp_fp']
            
        
            
        #=======================================================================
        # #checks
        #=======================================================================
        
        vlay = self.vlay_load(fp, logger=log)
        
        assert vlay.isValid()
        assert vlay.wkbType()==1
        assert vlay.dataProvider().featureCount()>0
        assert vlay.crs()==self.qproj.crs(), 'crs mismatch'

        #=======================================================================
        # wrap
        #=======================================================================
        self.mstore.addMapLayer(vlay)
        self.mstore.removeMapLayers([vlay])
        
 
        return fp
    
    
    
    
    def load_fic(self, #intellighent loading of FloodsInCanada
                 logger=None,
                 **kwargs):
        
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        log=logger.getChild('load_fic')
        
        #=======================================================================
        # build from scratch
        #=======================================================================
        if not 'fic_fp' in self.fp_d:
            log.info('building fic polys')
            
            from data_collect.fic_composite import ficSession
            
            with ficSession(session=self, logger=logger, inher_d=self.childI_d) as wrkr:
            
                #temporal and spaital selection
                wrkr.load_db(logger=log)
    
                fp, meta_d = wrkr.get_fic_polys(logger=log, reproject=True, **kwargs)
                
                self.meta_d.update({'load_fic':meta_d})


            #add and load
            self.ofp_d['fic_fp'] = fp

        #=======================================================================
        # load pre-built
        #=======================================================================
        else:
            log.info('passed pre-built fics... loading')
            fp = self.fp_d['fic_fp']
        
        
            
        #=======================================================================
        # #checks
        #=======================================================================
        vlay = self.vlay_load(fp, logger=log)
        assert vlay.isValid()
        assert vlay.wkbType()==6,'expected \'MultiPolygon\' on \'%s\' got: %s'%(vlay.name(), QgsWkbTypes().displayString(vlay.wkbType()))
        assert vlay.dataProvider().featureCount()>0
        assert vlay.crs()==self.qproj.crs(), 'crs mismatch'
        
        
        #=======================================================================
        # wrap
        #=======================================================================
        self.mstore.addMapLayer(vlay)
        self.mstore.removeMapLayers([vlay])
 
        
        return fp
    
    def load_hrdem(self, 
                   aoi_fp='',
                   logger=None,
                   resolution=2,
                 **kwargs):
        
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        log=logger.getChild('load_hrdem')
        fp_key = 'dem_fp'
        meta_d = dict()
        
        assert isinstance(resolution, int), 'user specifed bad resolution: \'%s\' (%s)'%(resolution, type(resolution))
        #=======================================================================
        # build from scratch
        #=======================================================================
        if not fp_key in self.fp_d:
            log.info('\'%s\' downloading and clipping HRDEM'%fp_key)
            
            from data_collect.hrdem import HRDEMses
            
            with HRDEMses(session=self, logger=logger, inher_d=self.childI_d, fp_d=self.fp_d,
                          aoi_fp=aoi_fp) as wrkr:
            
                ofp, runtime = wrkr.get_hrdem(resolution=resolution, **kwargs)
                meta_d['runtime (mins)'] = runtime

            #add and load
            self.ofp_d[fp_key] = ofp
            
        else:
            
            ofp = self.fp_d[fp_key]
            log.info('using dem from file: %s'%ofp)
        
        
        #=======================================================================
        # checks
        #=======================================================================
        rlay = self.rlay_load(ofp, logger=log)
        assert rlay.crs() == self.qproj.crs(), 'passed dem \'%s\' doesnt match  the proj crs'%(rlay.name())
        
        #resolution
        raw_res = self.get_resolution(rlay)
        assert raw_res == float(resolution), 'resolution failed to match (%s vs %s)'%(raw_res, resolution)
 
 
        
        self.dem_psize = round(rlay.rasterUnitsPerPixelY(),2)

        meta_d.update({'real_pixel_count':self.rlay_get_cellCnt(ofp), 'pixel_size':self.dem_psize})
        self.meta_d.update({'load_hrdem':meta_d})
        #=======================================================================
        # wrap
        #=======================================================================
        self.mstore.addMapLayer(rlay)
        self.mstore.removeMapLayers([rlay])
 
        log.info('loaded %s'%ofp)
        
        return ofp
    
    def load_nhn(self,
                 logger=None,
                 aoi_fp='',
                 **kwargs):
            
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        log=logger.getChild('load_nhn')
        fp_key = 'nhn_fp'
        
        log.info('getting %s'%fp_key)
        #=======================================================================
        # build from scratch
        #=======================================================================
        if not fp_key in self.fp_d:
            
            
            from data_collect.nhn import NHNses as SubSession
            
            with SubSession(session=self, logger=log, 
                            fp_d=self.fp_d, #letting these passs
                            inher_d=self.childI_d) as wrkr:
            
                ofp_d, meta_d = wrkr.run(**kwargs)
                
                """want to check before we store to ofp_d"""
                self.check_streams(ofp_d[fp_key],aoi_fp, logger=log)
                
                #update containers
                self.meta_d.update({'load_nhn':meta_d})
                self.ofp_d.update(ofp_d)
            
            ofp = self.ofp_d[fp_key]
        else:

            ofp = self.fp_d[fp_key]
            
            self.check_streams(ofp,aoi_fp, logger=log)
            
 
        #=======================================================================
        # wrap
        #=======================================================================

 
        
        log.info('for \'%s\' got \n %s'%(fp_key, ofp))
        
        return ofp
    
    def check_streams(self, #coverage checks against the NHN water bodies
                      streams_fp,
                      aoi_fp,
                      min_ratio=0.001, #minimum stream_area/aoi_area
                      logger=None):
        
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        log=logger.getChild('check_strams')
        
        #=======================================================================
        # layer checks
        #=======================================================================
        vlay = self.vlay_load(streams_fp, logger=log)
        assert vlay.crs() == self.qproj.crs()
        log.debug('on %s'%streams_fp)
        #=======================================================================
        # get areas
        #=======================================================================
        """streams is already clipped to aoi"""
        streams_area = self.vlay_poly_tarea(streams_fp)
        aoi_area = self.vlay_poly_tarea(aoi_fp)
        
        #=======================================================================
        # check threshold
        #=======================================================================
        ratio = streams_area/aoi_area
        
        if ratio<min_ratio:
            raise Error('streams (%s) coverage  less than min (%.3f<%.3f)'%(
                os.path.basename(streams_fp), ratio, min_ratio))
        else:
            log.debug('coverage = %.2f'%ratio)

        #=======================================================================
        # wrap
        #=======================================================================
        self.mstore.addMapLayer(vlay)
        self.mstore.removeMapLayers([vlay])
        
        log.debug('finished')
        return
    

        
        
    #===========================================================================
    # Inundation Hydro Correction---------
    #===========================================================================

    def run_imax(self, #get gridded hand values
                  #input data
                     dem_fp=None,
                     nhn_fp=None,
                     fic_fp=None,
                     
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
        
        ofp_d_old = copy.copy(self.afp_d)
        
        if sample_spacing is None:
            sample_spacing = self.dem_psize*5
        
        #=======================================================================
        # datafile ssetup
        #=======================================================================
        
        fp_d = self.afp_d #mash pre-built and newly built datasets
        
        if dem_fp is None: dem_fp=fp_d['dem_fp']
        if nhn_fp is None: nhn_fp=fp_d['nhn_fp']
        if fic_fp is  None: fic_fp=fp_d['fic_fp']
        
 
        
        """clear everything from run_get_data"""
        self.mstore.removeAllMapLayers() 
        #=======================================================================
        # #get the HAND layer
        #=======================================================================
        #rasterize NHN polys
        nhn_rlay_fp = self.rasterize_inun(nhn_fp, ref_lay=dem_fp,
                                          fp_key='nhn_rlay_fp', logger=log)
        
        #get the hand layer        
        hand_fp = self.build_hand(dem_fp=dem_fp, stream_fp=nhn_rlay_fp, logger=log)
        
        #=======================================================================
        # add minimum water bodies to FiC inundation
        #=======================================================================
        #nodata boundary of hand layer (polygon)
        ndb_fp = self.build_nd_bndry(hand_fp=hand_fp, logger=log)
        
        #merge, crop, and clean
        inun1_fp = self.build_inun1(fic_fp=fic_fp,nhn_fp=nhn_fp,ndb_fp=ndb_fp,
              logger=log)
        
        
        #=======================================================================
        # get hydrauilc maximum
        #=======================================================================
        #get initial edge samples
        smpls1_fp = self.build_samples1(rToSamp_fp=hand_fp, inun_fp=inun1_fp,
                                        ndb_fp=ndb_fp, sample_spacing=sample_spacing)
        
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
        #promote meta
        self._promote_meta('02run_imax')
 
        
        #get just those datalayers built by this function
        ofp_d = {k:v for k,v in self.ofp_d.items() if not k in ofp_d_old.keys()}
        log.info('built %i datalayers'%len(ofp_d))
        
        
        self._log_datafiles(d=ofp_d)
        
        self.afp_d = {**self.fp_d, **self.ofp_d} #fp_d overwritten by ofp_d
        
        return
        

        
    def build_hand(self, #load or build the HAND layer
                   *args,
                   logger=None,
                  **kwargs
                 ):
        
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        log=logger.getChild('b.hand')
        
        fp_key = 'hand_fp'
        

        #=======================================================================
        # build
        #=======================================================================
        if not fp_key in self.fp_d:
            from scripts.hand import HANDses as SubSession
            
            with SubSession(session=self, logger=logger, inher_d=self.childI_d) as wrkr:
                """passing all filepathss for a clean kill"""
                fp = wrkr.run(*args, **kwargs) 
            
            self.ofp_d[fp_key] = fp
            log.info('built hand layer at %s'%fp)
            
        #=======================================================================
        # retrieve
        #=======================================================================
        else:
            
            fp = self.fp_d[fp_key]
            log.info('loading HAND layer from %s'%fp)
            
 
        
        return fp
    
    
    def build_nd_bndry(self, #get the no-data boundary of the HAND-ray (as a vector)
                  hand_fp='',
                  #stream_fp='',
                  logger=None,
                  ):
        
        """
        TODO: try and simplify this
        """
        #=======================================================================
        # defautls
        #=======================================================================
        if logger is None: logger=self.logger
        log=logger.getChild('build_nd_bndry')
        
 
        fp_key = 'ndb_fp'
        
        #=======================================================================
        # build
        #=======================================================================
        if not fp_key in self.fp_d:
            #===================================================================
            # #setup
            #===================================================================
            log.info('building noDataBoundary for \'%s\''%os.path.basename(hand_fp))
            
            ofp = os.path.join(self.out_dir, self.layName_pfx+'_ndb.gpkg')
            if os.path.exists(ofp):
                assert self.overwrite
                os.remove(ofp)
                
            assert os.path.exists(self.temp_dir)
            #===================================================================
            # merge back in streams
            #===================================================================
            #rlay0 = self.mergeraster([hand_fp, stream_fp], logger=log)
            
            
            #convert raster to binary
            """NOTE: HAND layers have zero values on the streams
                so the binary polygon has multiple features still"""
            rlay1_fp = self.mask_build(hand_fp, zero_shift=True, logger=log, 
                                       ofp=os.path.join(self.temp_dir, 'hand_mask1.tif'))
            
 
            #raster to no-data edge polygon
            log.debug('polygonizing masked hand')
            nd_vlay_fp1 = self.polygonizeGDAL(rlay1_fp, logger=log,
                                      output=os.path.join(self.temp_dir, 'hand_mask1.gpkg'),
                                      )
            
            assert os.path.exists(nd_vlay_fp1), 'failed to polygonize hand mask'
            
            #clean/dissolve
            """need to drop DN field"""
            nd_vlay1= self.deletecolumn(nd_vlay_fp1, ['DN'], logger=log)
            
            #fix geometry
            nd_vlay2_fp = self.fixgeo(nd_vlay1, logger=log, 
                                      output=os.path.join(self.temp_dir, 'nd_vlay2_fixgeo.gpkg'))
             

            """these seem to still be neeeded"""
            #delete all the holes
            nd_vlay4 = self.deleteholes(nd_vlay2_fp, hole_area=0, logger=log,
                                        output=os.path.join(self.temp_dir, 'nd_vlay4_deleteholes.gpkg'))
            
            #fix geometry
            """needed a second time for some polys"""
            nd_vlay5 = self.fixgeo(nd_vlay4, logger=log, 
                                      output=os.path.join(self.temp_dir, 'nd_vlay5_fixgeo.gpkg'))
            
            #dissolve
            _ = self.dissolve(nd_vlay5, output=ofp, logger=log)
            
            #wrap
            self.mstore.addMapLayers([nd_vlay1])
            self.mstore.removeMapLayers([nd_vlay1])
            
            self.ofp_d[fp_key]= ofp
            
            
        else:
            ofp = self.fp_d[fp_key]
            
 
        #=======================================================================
        # wrap
        #=======================================================================
 
        log.info('got \'%s\': \n    %s'%(fp_key, ofp))
        
        return ofp
    
    
    def build_inun1(self, #merge NHN and FiC and crop to DEM extents
              fic_fp='',
              nhn_fp='',
              ndb_fp='', #no data boundary of hand layer (as a vector polygon)
              hole_size=None, #size of hole to delete
              buff_dist=None, #buffer to apply to nhn
              simp_dist=None,
              logger=None,
              ):
        """
        consider making this a separate worker class
        """
        
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        log=logger.getChild('b.inun1')
        
 
        fp_key = 'inun1_fp'
        
        
        
        #=======================================================================
        # build
        #=======================================================================
        if not fp_key in self.fp_d:
            log.info('building \'%s\' from %s'%(fp_key, os.path.basename(fic_fp)))
            ofp = os.path.join(self.out_dir, self.layName_pfx+'_inun1.gpkg')
            
            if os.path.exists(ofp):
                assert self.overwrite
                os.remove(ofp)
                
            mstore = QgsMapLayerStore()
            #===================================================================
            # buffer
            #===================================================================
            """nice to add a tiny buffer to the waterbodies to ensure no zero hand values"""
            if buff_dist is  None:
                buff_dist = self.dem_psize*2
                
 
            
            vbuff_fp = self.buffer(nhn_fp, dist=buff_dist, logger=log, dissolve=True,
                                output=os.path.join(self.temp_dir, '%s_buffer'%os.path.basename(nhn_fp)))
 
            #=======================================================================
            # merge the layers
            #=======================================================================
            vlay1_fp = self.mergevectorlayers([fic_fp, vbuff_fp], logger=log,
                                           output=os.path.join(self.temp_dir, 'inun1_merge.gpkg'))
 
            
            #===================================================================
            # crop to dem extents
            #===================================================================
            vlay2 = self.clip(vlay1_fp, ndb_fp, logger=log)
            
            #===================================================================
            # clean
            #===================================================================
 
            #clean
            self.clean_inun_vlay(vlay2, output=ofp, logger=log, mstore=mstore,
                                 simp_dist=simp_dist, hole_size=hole_size)
 
            #===================================================================
            # #wrap
            #===================================================================
            mstore.removeAllMapLayers()
            self.ofp_d[fp_key] = ofp
 
            
        else:
            
            ofp = self.fp_d[fp_key]
            log.info('retrieving from %s'%ofp)
        

 
        #=======================================================================
        # wrap
        #=======================================================================
        log.info('got \'%s\': \n    %s'%(fp_key, ofp))

        return ofp
    
    def build_samples1(self, #get the hydrauilc maximum inundation from sampled HAND values
                *args,
              logger=None,
              **kwargs):
        
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        log=logger.getChild('b.hmax')
 
        fp_key = 'smpls1_fp'
        
        #=======================================================================
        # build
        #=======================================================================
        if not fp_key in self.fp_d:
            
            log.info('building \'%s\' w/ %s %s'%(fp_key, args, kwargs))
            from scripts.hand_inun import HIses as SubSession
            
            with SubSession(session=self, logger=logger, inher_d=self.childI_d,
                            fp_d=self.fp_d) as wrkr:
            
                ofp, meta_d = wrkr.get_edge_samples(*args,fp_key=fp_key, **kwargs)
                
            self.ofp_d[fp_key] = ofp
            self.meta_d.update({'build_samples1':meta_d})
            
            assert self.smpl_fieldName == meta_d['smpl_fieldName']
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
            
    def get_sample_bounds(self, #get the min/max HAND values to use (from sample stats)
                          pts_fp,
                          
                          #data parameters
                          qhigh=0.75, 
                          qlow=0.25, #used to set the lower threshold on cap_samples
                          
                          cap=7.0, #maxiomum hval to allow (overwrite quartile
                          floor = 0.5, #minimum
                          
                          drop_zeros=True,
                          
                          coln=None,
                          plot=None,
                          logger=None,
                          prec=3,
                          ):
        """
        calculating these bounds each run
        
        TODO: 
            merge w/ hand_inun cap_samples
        """
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        if plot is None:
            plot=self.plot
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
        
        if drop_zeros:
            s1 = sraw[sraw>0.1]
        else:
            s1 = sraw
        #===================================================================
        # upper bound
        #===================================================================
        qh = s1.quantile(q=qhigh)
        if qh > cap:
            log.warning('q%.2f (%.2f) exceeds cap (%.2f).. using cap'%(
                qhigh, qh, cap))
            hv_max = cap
        else:
            hv_max=qh
            
        #=======================================================================
        # lower bound
        #=======================================================================
        ql = s1.quantile(q=qlow)
        if ql < floor:
            log.warning('q%.2f (%.2f) is lower than floor (%.2f).. using floor'%(
                qlow, ql, floor))
            hv_min = floor
            
            use_floor=True
        else:
            hv_min=ql
            use_floor=False
            
        #=======================================================================
        # wrap
        #=======================================================================
        if plot:
            plot_fp = self.plot_hand_vals(sraw, xval_lines_d={'max (q=%.2f)'%qhigh:hv_max,
                                                              'min (q=%.2f, use_floor=%s)'%(qlow, use_floor):hv_min}, 
                                title='get_sample_bounds',
                                label=os.path.basename(pts_fp),logger=log)
        else:
            plot_fp=None
            
        log.info('got hv_max=%.2f, hv_min=%.2f'%(hv_max, hv_min))
        
        self.hv_max=round(hv_max, 3)
        self.hv_min=round(hv_min, 3)
            
        self.meta_d.update({'get_sample_bounds':{'hv_max':self.hv_max, 'hv_min':self.hv_min, 'plot_fp':plot_fp}})
        
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
            
            
            from scripts.tcoms import TComs as SubSession
            
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
    # Rolling HAND depths----------
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
        #promote meta
        self._promote_meta('03run_hdep_mosaic')
 
        
        #get just those datalayers built by this function
        ofp_d = {k:v for k,v in self.ofp_d.items() if not k in ofp_d_old.keys()}
        log.info('built %i datalayers'%len(ofp_d))
        
        
        self._log_datafiles(d=ofp_d, log=log)
        
        return
    
        
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
            from scripts.hand_inun import HIses as SubSession
            
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
            
            from scripts.hand_inun import HIses as SubSession
            
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
            
            from scripts.hand_inun import HIses as SubSession
            
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
            
            #retrieve unique values from meta?
            if hvgrid_uq_vals is None:
                if 'hvgrid_uq_vals' in self.meta_d: 
                    hvgrid_uq_vals=self.meta_d['hvgrid_uq_vals']
            
            
            
            
            from scripts.hand_inun import HIses as SubSession
            
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
        
    
    #===========================================================================
    # misc-----------
    #===========================================================================
    
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
        
    def _promote_meta(self,
                      tabnm):
        
        #add some commons
        tdelta = datetime.datetime.now() - start
        runtime = tdelta.total_seconds()/60.0
        #self.meta_d.update(self.ofp_d) #this is on the layerSummary now
        
        self.meta_d = {**{'now':datetime.datetime.now(), 'runtime (mins)':runtime}, **self.meta_d}
        
        self.smry_d[tabnm] = pd.Series(self.meta_d, name='val').to_frame()
        
        #reset the metad
        self.meta_d = dict()
                
            
            
        
 
    def __exit__(self, #destructor
                 *args,**kwargs):
        
        self._log_datafiles()
        
        
        #=======================================================================
        # layerSummary
        #=======================================================================
        
        self.set_layer_stats()
        
        #=======================================================================
        # summary tab
        #=======================================================================
        """if the script completed, meta_d should be empty"""
        #add inheritance variables
        for attn in self.childI_d['Session']:
            self.meta_d[attn] = getattr(self, attn)
            
        tdelta = datetime.datetime.now() - start
        runtime = tdelta.total_seconds()/60.0
        #self.meta_d.update(self.ofp_d) #this is on the layerSummary now
        
        self.meta_d = {**{'now':datetime.datetime.now(), 'runtime (mins)':runtime}, **self.meta_d}
        

        self.smry_d = {**{'_smry':pd.Series(self.meta_d, name='val').to_frame()},
                        **self.smry_d}
        
        #check it
        for k,v in self.smry_d.items():
            if not isinstance(v, pd.DataFrame):
                raise Error('got bad type on \'%s\': %s'%(k, type(v)))
        
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
        


