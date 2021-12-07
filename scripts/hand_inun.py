'''
Created on Jul. 16, 2021

@author: cefect

getting inundation polygons from HAND rastsers (and raw inundation)

2021-07-18: see '0718' branch for vector grid based workflow
    revised to use IDW interpolation and a low-pass filter
'''

#===============================================================================
# imports-----------
#===============================================================================
import os, datetime, shutil, pickle, copy
 
import pandas as pd
import numpy as np



from hp.exceptions import Error
from hp.dirz import force_open_dir
 
from hp.Q import Qproj, QgsVectorLayer, view, vlay_get_fdf, QgsMapLayerStore, \
    vlay_get_fdata, vlay_get_geo, QgsCoordinateReferenceSystem, QgsRasterLayer
    


from qgis.analysis import QgsRasterCalculatorEntry
from hp.gdal import rlay_to_array

from scripts.tcoms import TComs



from hp.animation import capture_images


class HIses(TComs): #get inundation raters from HAND and raw polygonss
    
    #hvgrid_uq_vals=None #NO! cant pass between instances
    ofp_d = dict() #container for built data file paths
    
    #field name expectations on the gstats_vlay
    gstats_fns = ['fid', 'grid', 'count', 'min', 'max', 'mean', 'stddev', 'q1', 'q3']
    
    attn_h = 'hval'
    
    inun3_mask_fp=None
    
    def __init__(self,
                 tag='HANDin',
                 
                 compress='med',
                **kwargs):
        
        super().__init__(tag=tag, compress=compress,
                         **kwargs)  # initilzie teh baseclass
        
        #=======================================================================
        # attach
        #=======================================================================
        
        
        

 
    
    #===========================================================================
    # RUNERS-----------
    #===========================================================================

        
    def run_hvgrid(self, #get gridded rolling hand values
                   inun2_fp='', #inundation (filtered bby h ydrauilc maximum)
                   inun2r_fp='',#same in raster form
                   hand_fp='',
                   ndb_fp='', #nodata boundary polygon of hand layer
                   
                    #pars: get_smpl_pts
                    sample_spacing=None,  #spacing between sampling points
                        #None=dem_psize x 5
                        #generally finer resolution than needed by run_hmax
                        
                    #cap_sample_vals()
                    hv_min=None,
                    hv_max=None, 
                    
                    #pars: get_interp
                   distP= None, #distance coeefificent
                   interp_resolution=None, #resolution of interpolated raster. 
                        #None= sample_spacing x 2
                   
                   #pars: smooth_hvals
                     range_thresh=None, #maximum range (between HAND cell values) to allow
                        #None: calc from max_slope and resolution
                     max_grade = 0.05, #maximum hand value grade to allow 
                     hval_prec=0.1,
 
                   fp_key = 'hvgrid_fp', #becuase weve done a nasty nesting
                   logger=None,
                      ):
        """TODO: promote this method to main"""
        
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        log=logger.getChild('rhvgrid')
        
        if sample_spacing is None:
            sample_spacing = self.dem_psize*5
        
        meta_d = {}
        
        log.info('start w/ %s'%meta_d)
        #=======================================================================
        # sample HAND values at edges (on refined)
        #=======================================================================
        smpls_fp, d = self.get_edge_samples(rToSamp_fp=hand_fp, ndb_fp=ndb_fp, inun_fp=inun2_fp,
                                         sample_spacing=sample_spacing, key_sfx='2', logger=log)
        
        meta_d.update({'get_edge_samples2':d})
        
        
        #cap low-ballers
        smplsC_fp, d = self.cap_samples(smpls_fp=smpls_fp, vmin=hv_min, vmax=hv_max, logger=log)
        
        meta_d.update({'cap_samples':d})
        #=======================================================================
        # smothed and gridded rolling HAND values 
        #=======================================================================
        #build interpolated surface from edge points
        interp_rlay_fp, d = self.get_interp(smplsC_fp, hand_fp=hand_fp, logger=log,
                                     distP=distP, resolution=interp_resolution)
        meta_d.update({'get_interp':d})
        
        #re-interpolate interior regions
        interp2_rlay_fp, d = self.interp_interior(interp_rlay_fp,inun2r_fp=inun2r_fp, logger=log)
        meta_d.update({'interp_interior':d})
        
        #low-pass and downsample
        hvgrid_fp, d = self.smooth_hvals(interp2_rlay_fp, logger=log,
                                      range_thresh=range_thresh,max_grade=max_grade,
                                      hval_prec=hval_prec,fp_key=fp_key
                                      )
        
        meta_d.update({'smooth_hvals':d})
        #=======================================================================
        # wrap
        #=======================================================================
        log.info('finished w/ %s'%hvgrid_fp)
        
        #want to retun all the sub-layers
        return self.ofp_d, meta_d
    



    def run_hinunSet(self,
             hvgrid_fp='',
              hand_fp='',
              hval_prec=1, #precision of hvals to discretize
              resolution=None, #resolution for inundation rasters
              logger=None,
              debug=False,
              ):
        
        """
        TODO: performance improvements
            reduce resolution?
            temporal raster?
        """
        
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        log=logger.getChild('r.hinun')
        
        ofp = os.path.join(self.out_dir, 'hinun_set.pickle')
        
        self.hval_prec=hval_prec #used by run_wsl_mosaic()
        
        
        #=======================================================================
        # setup
        #=======================================================================
    
        #directory
        out_dir = os.path.join(self.out_dir, 'hinun_set')
        if not os.path.exists(out_dir):os.makedirs(out_dir)

        log.debug('on %s w/ hval_prec=%.2f'%(os.path.basename(hvgrid_fp), hval_prec))
        #=======================================================================
        # downsample the hand layer
        #=======================================================================
        hres = self.get_resolution(hand_fp)

        if resolution is None:
            resolution=hres
 
        #reproject with new resolution
        if not hres == resolution:
            log.info('downsampling \'%s\' from %.2f to %.2f'%(
                os.path.basename(hand_fp), hres,  resolution))
             
            hand1_fp = self.warpreproject(hand_fp, resolution=resolution,
                                           logger=log)
             
        else:
            hand1_fp = hand_fp
            
        #get total grid size
        hand_cell_cnt = self.rlay_get_cellCnt(hand1_fp)
        #=======================================================================
        # get grid values
        #=======================================================================
        """use the native to avoid new values
        rlay = self.roundraster(hvgrid_fp, logger=log, prec=hval_prec)"""
        
        uq_vals = self.rlay_uq_vals(hvgrid_fp, prec=1)
        

        #=======================================================================
        # get inundation rasters
        #=======================================================================
        log.info('building %i HAND inundation rasters (%.2f to %.2f) reso=%.1f'%(
            len(uq_vals), min(uq_vals), max(uq_vals), resolution))
        res_d = dict()
        
        
        for i, hval in enumerate(uq_vals):
            log.debug('(%i/%i) getting hinun for %.2f'%(i+1, len(uq_vals), hval))
            #get this hand inundation
            
            rlay_fp = self.get_hand_inun(hand1_fp, hval, logger=log,
                               ofp = os.path.join(out_dir, '%03d_hinun_%03d.tif'%(i, hval*100))
                               )
            
            stats_d = self.rasterlayerstatistics(rlay_fp, logger=log)
            res_d[i] = {**{'hval':hval,'fp':rlay_fp,
                                      'flooded_pct':(stats_d['SUM']/float(hand_cell_cnt))*100,
                                       'error':np.nan},
                                        **stats_d, }
            
            log.info('(%i/%i) got hinun for hval=%.2f w/ %.2f pct flooded'%(
                i+1, len(uq_vals), hval, res_d[i]['flooded_pct']))
            
        #===================================================================
        # build animations
        #===================================================================
        if debug:
            capture_images(
                os.path.join(self.out_dir, self.layName_pfx+'_hand_inuns.gif'),
                out_dir
                )
 
        #=======================================================================
        # wrap
        #=======================================================================
        df = pd.DataFrame.from_dict(res_d, orient='index')
        self.session.smry_d['run_hinunSet'] = df.copy()
        
        #write the reuslts pickel
 
        with open(ofp, 'wb') as f:
            # Pickle the 'data' dictionary using the highest protocol available.
            pickle.dump(df.set_index('hval')['fp'].to_dict(), f, pickle.HIGHEST_PROTOCOL)
        
        log.debug('finished writing %i to \n    %s'%(len(res_d), ofp))
    
        return ofp, {'uq_vals_cnt':len(uq_vals), 'hinun_set_resol':resolution,
                     'hvgrid_uq_vals':copy.copy(uq_vals)}
            

    def run_hwslSet(self,
             hinun_pick='',
              dem_fp='',
              logger=None,
 
              ):
        
        """
 
        """
        
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        log=logger.getChild('r.hwsl')
        
        ofp = os.path.join(self.out_dir, 'hwsl_set.pickle')
        
        #=======================================================================
        # setup
        #=======================================================================
        out_dir = os.path.join(self.out_dir, 'hwsl_set')
        if not os.path.exists(out_dir):os.makedirs(out_dir)
        
        mstore = QgsMapLayerStore()

        #=======================================================================
        # #get hand inundation filepaths
        #=======================================================================
        #pull the filepaths dictionary from the pickel
        with open(hinun_pick, 'rb') as f:
            hi_fp_d = pickle.load(f)
 
        #check these
        for hval, fp in hi_fp_d.items():
            assert os.path.exists(fp), 'hval %.2f bad fp in pickel:\n    %s'%(hval, fp)
            assert QgsRasterLayer.isValidRasterFileName(fp),  \
                'hval %.2f bad fp in pickel:\n    %s'%(hval, fp)
            

        #=======================================================================
        # downsampling DEM
        #=======================================================================
        hres = self.get_resolution(fp)
        dres = self.get_resolution(dem_fp)
 

        #reproject with new resolution
        if not dres ==hres:
            log.info('resampling \'%s\' from %.2f to %.2f'%(
                os.path.join(dem_fp), dres,  hres))
            
            dem1_fp = self.warpreproject(dem_fp, resolution=hres,
                                         output=os.path.join(self.temp_dir, os.path.basename(dem_fp)),
                                           logger=log)
            
        else:
            dem1_fp = dem_fp

        assert self.rlay_check_match(fp, dem1_fp, logger=log), 'HANDinun resolution does not match dem'
        mstore.removeAllMapLayers()
        #=======================================================================
        # get water level rasters
        #=======================================================================
        log.info('building %i wsl rasters on \'%s\' resl=%.1f'%(
            len(hi_fp_d), os.path.basename(dem_fp), hres))
        res_d = dict()
        fail_cnt = 0
        for i, (hval, fp) in enumerate(hi_fp_d.items()):
            log.info('(%i/%i) hval=%.2f on %s w/ resolution = %.2f'%(
                i,len(hi_fp_d)-1,hval, os.path.basename(fp), hres))
            
            try:
            
                #extrapolate in
                wsl_raw_fp = self.wsl_extrap_wbt(dem1_fp, fp, logger=log.getChild(str(i)),
                            ofp = os.path.join(out_dir, '%03d_hwsl_%03d.tif'%(i, hval*100.0)), #result layer
                            out_dir=os.path.join(self.temp_dir, 'hwsl_set', str(i)), #dumping iter layers
                            )
                
                #smooth
                """would result in some negative depths?
                    moved to the wsl mosaic"""
    
                wsl_fp = wsl_raw_fp
                
                
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
                if fail_cnt>5:
                    raise Error('failed to get wsl too many times')
            

        #===================================================================
        # build animations
        #===================================================================
        """not showing up in the gifs for some reason"""

        #=======================================================================
        # output
        #=======================================================================
        if len(res_d)>0:
            #summary data
            df = pd.DataFrame.from_dict(res_d, orient='index')
            self.session.smry_d['run_hwslSet'] = df.copy()
                
            #write the reuslts pickel
            """only taking those w/ successfulr asters"""
            res_d = df[df['error'].isna()].set_index('hval', drop=True)['fp'].to_dict()
            
            with open(ofp, 'wb') as f:
                # Pickle the 'data' dictionary using the highest protocol available.
                pickle.dump(res_d, f, pickle.HIGHEST_PROTOCOL)
        else:
            log.error('failed to get any results')
        
        #=======================================================================
        # wrap
        #=======================================================================
        log.debug('finished writing %i to \n    %s'%(len(res_d), ofp))
    
        return ofp
    
    
    def run_wsl_mosaic(self, #mosaic together wsl sets using hvgrid
             hwsl_pick='',
             hvgrid_fp='', #grid showing where each hval should apply
             hvgrid_uq_vals=None,
 
              logger=None,
 
              ):
        """
        resolution: using wahtever is on each hwsl raster
        """
   
        
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        log=logger.getChild('r.wslM')
        
        """no.. we launch a new instance
        if hvgrid_uq_vals is None: hvgrid_uq_vals=self.hvgrid_uq_vals"""
        
        ofp = os.path.join(self.out_dir, self.layName_pfx + '_wslM.tif')
        #=======================================================================
        # setup
        #=======================================================================
        mstore = QgsMapLayerStore()
        
        temp_dir = os.path.join(self.temp_dir, 'wslM')
        if not os.path.exists(temp_dir):
            os.makedirs(temp_dir)
            os.makedirs(os.path.join(temp_dir, 'mask'))
        #=======================================================================
        # #get hand wsl filepaths
        #=======================================================================
        #pull the filepaths dictionary from the pickel
        with open(hwsl_pick, 'rb') as f:
            hwsl_fp_d_raw = pickle.load(f)
            
        #sort it
        hwsl_fp_d = dict(sorted(hwsl_fp_d_raw.copy().items()))
 
        #check these
        for hval, fp in hwsl_fp_d.items():
            assert os.path.exists(fp), 'hval %.2f bad fp in pickel:\n    %s'%(hval, fp)
            assert QgsRasterLayer.isValidRasterFileName(fp),  \
                'hval %.2f bad fp in pickel:\n    %s'%(hval, fp)
                
        #=======================================================================
        # round the hv grid
        #=======================================================================
        """
        NO! multiple roundings might cause issues.. just use the raw and check it
        
        grid precision needs to match the hvals for the mask production
            couldnt find a good way to extract this
            doing a check against values found on the raw hvgrid and those passed in the pickel
        #get the precision from the hvals
 
        
        hvg_fp = self.roundraster(hvgrid_fp, logger=log, prec=self.hval_prec)"""
                
        #=======================================================================
        # check hand grid values
        #=======================================================================
        #get the values
        if hvgrid_uq_vals is None:
            """usually set by run_hinunSet()"""
            hvgrid_uq_vals = self.rlay_uq_vals(hvgrid_fp, prec=1)
                
        #check against the pickel
        miss_l = set(hwsl_fp_d.keys()).symmetric_difference(hvgrid_uq_vals)
        assert len(miss_l)==0, '%i value mismatch between hwsl_pick (%s) and hvgrid (%s) \n    %s'%(
            len(miss_l), os.path.basename(hwsl_pick), os.path.basename(hvgrid_fp), miss_l)
        
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
            #===================================================================
            # #get donut mask for this hval
            #===================================================================
            #mask those less than the hval (threshold mask)
            mask_i_fp = self.mask_build(hvgrid_fp, logger=log,
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
            cell_cnt = self.rasterlayerstatistics(mask_fp, logger=log)['SUM']
            
            d={'hval':hval, 'mask_cell_cnt':cell_cnt,'wsl_fp':wsl_fp,'mask_fp':mask_fp,
               'error':np.nan}
            log.info('    (%i/%i) hval=%.2f on %s got %i wet cells'%(
                i, len(hwsl_fp_d)-1, hval, os.path.basename(wsl_fp), cell_cnt))
            #===================================================================
            # check
            #===================================================================
            if cell_cnt>0:
                #apply to the wsl
                wsli_fp = self.mask_apply(wsl_fp, mask_fp, logger=log,
                                          ofp=os.path.join(temp_dir, 'wsl_maskd_%03d_%03d.tif'%(
                                              i, hval*100)))
                
                stats_d = self.rasterlayerstatistics(wsli_fp, logger=log)
                
                assert os.path.exists(wsli_fp)
                
                d = {**d, **{ 'wsl_maskd_fp':wsli_fp},**stats_d}
            else:
                """this shouldnt trip any more
                if it does... need to switch to mask_build with a range"""
                log.error('identified no hval=%.2f cells'%hval)
                d['error'] = 'no wet cells'

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
        
        """
        view(df)
        """
        self.session.smry_d['run_wsl_mosaic'] = df
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
                        output=ofp, 
                        logger=log)
        
        assert os.path.exists(wsl2_fp)
        #=======================================================================
        # wrap
        #=======================================================================
        mstore.removeAllMapLayers()
        
        log.info('finished w/ %s'%wsl2_fp)
        
        return wsl2_fp
            
        
 
    
    #===========================================================================
    # sub-routines-------
    #===========================================================================
    
    def inun_clipd(self, #rectify the inundation layer extents
                     nd_vlay=None,
                     inun_vlay = None,
                     logger=None,
                     ):
        
        #=======================================================================
        # default
        #=======================================================================
        if logger is None: logger=self.logger
        log=logger.getChild('inun_clipd')
        

        
        fp_key = 'inun2_fp'
        
        #=======================================================================
        # build
        #=======================================================================
        if not fp_key in self.fp_d:
            
            #===================================================================
            # set defaults
            #===================================================================
            if inun_vlay is None: inun_vlay=self.inun_vlay
            if nd_vlay is None: nd_vlay=self.nd_vlay #nodata boundary of HAND layer
        
            #=======================================================================
            # build and intersect
            #=======================================================================
            #get intersection
            ofp = os.path.join(self.out_dir, '%s_rect.gpkg'%inun_vlay.name())
            _ = self.intersection(nd_vlay, inun_vlay, output=ofp, logger=log)
            
            log.info('built rectified inundation \'%s\' at \n    %s'%(
                inun_vlay.name(), ofp))
        else:
            
            ofp = self.fp_d[fp_key]
            log.info('loading rectified inundation from %s'%ofp)
            
                    
        inun_vlay1 = self.vlay_load(ofp, logger=log)
        #=======================================================================
        # wrap
        #=======================================================================
        assert isinstance(inun_vlay1, QgsVectorLayer)
        self.inun_vlay = inun_vlay1
        self.ofp_d[fp_key]= ofp
        
        return inun_vlay1
        

        
    def get_smpl_pts(self, #build sampling points from inundation polygon
                     inun_fp='',
                     ndb_fp='', #nodata boundary polygon
                     fp_key = 'smpts1_fp',
                     spacing=200, #sample resolution
                     logger=None,
                     
                     ):
        
        #===================================================================
        # defaults
        #===================================================================
        if logger is None: logger=self.logger
        log=logger.getChild('get_smpl_pts')
        
        """no good way to get this back from a created points layer"""
        self.sample_spacing = spacing #used for defaults by get_interp()
 
        meta_d=dict()
        #=======================================================================
        # build
        #=======================================================================
        if not fp_key in self.fp_d:
            
            #===================================================================
            # setup
            #===================================================================
            ofp = os.path.join(self.out_dir, self.layName_pfx+'_%s.gpkg'%fp_key.replace('_fp', ''))
            if os.path.exists(ofp):
                assert self.overwrite
                os.remove(ofp)
                
            log.debug('building \'%s\' on %s'%(fp_key, os.path.basename(inun_fp)))
            #===================================================================
            # points along edge
            #===================================================================
            assert os.path.exists(inun_fp), inun_fp
            pts_vlay_raw = self.pointsalonglines(inun_fp, logger=log, spacing=spacing)
            """
            view(pts_vlay_raw)
            view(pts_vlay1)
            """
            #===================================================================
            # #fix fid
            #===================================================================
            #remove all the fields
            ofp1 = os.path.join(self.temp_dir, self.layName_pfx+'_%s_raw.gpkg'%fp_key.replace('_fp', ''))
            _ = self.deletecolumn(pts_vlay_raw, 
                                          [f.name() for f in pts_vlay_raw.fields()], 
                                          output=ofp1,
                                          logger=log)
            
            #pts_vlay1 = self.vlay_load(ofp, logger=log)
 
            #===================================================================
            # #filter by raster edge
            #===================================================================
            _, fcnt = self.filter_edge_pts(pts_fp=ofp1, ndb_fp=ndb_fp, logger=log,
                                            ofp=ofp)
 
            meta_d['final_cnt'] = fcnt
            #=======================================================================
            # wrap
            #=======================================================================
 
            self.mstore.addMapLayers([pts_vlay_raw])
            self.mstore.removeMapLayers([pts_vlay_raw])
            
            self.ofp_d[fp_key]= ofp
        #=======================================================================
        # load
        #=======================================================================
        else:
            ofp = self.fp_d[fp_key]
            
 
            
        #=======================================================================
        # wrap
        #=======================================================================
        log.info('got \'%s\': \n    %s'%(fp_key, ofp))
        
        return ofp, meta_d
        
    def filter_edge_pts(self, #filter points where close to a rlay's no-data boundary
                        pts_fp='',
                        ndb_fp='',
                        dist=10, #distance from boundary to exclude
                        ofp=None, #optional outpath
                        logger=None,
                        ):
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        log=logger.getChild('filter_edge_pts')
        
 
        if ofp is None:
            ofp = os.path.join(self.temp_dir, 
                  '%s_filtered.gpkg'%(os.path.basename(pts_fp).replace('.gpkg','')))
        
        #=======================================================================
        # build selection donut
        #=======================================================================
        #outer buffer of no-data poly
        nd_big_vlay = self.buffer(ndb_fp, dist=dist, dissolve=True, logger=log)
        
        #inner buffer of no-data poly
        nd_sml_vlay = self.buffer(ndb_fp, dist=-dist, dissolve=True, logger=log)
        
        #outer-inner donut no-data poly
        nd_donut_vlay = self.symmetricaldifference(nd_big_vlay, nd_sml_vlay, logger=log)
        
        lays = [nd_big_vlay, nd_sml_vlay, nd_donut_vlay]
        
        #=======================================================================
        # #apply fiolter
        #=======================================================================
        #select points intersecting donut
        vlay_raw = self.vlay_load(pts_fp, logger=log)
        lays.append(vlay_raw)
        
        self.selectbylocation(vlay_raw, nd_donut_vlay, allow_none=False, logger=log)
        
        """
        view(pts_vlay)
        """
        
        #invert selection
        vlay_raw.invertSelection()
        
        #exctract remaining points
        vlay_fp = self.saveselectedfeatures(vlay_raw, logger=log,
                          output=ofp)
        
        #=======================================================================
        # wrap
        #=======================================================================
        self.mstore.addMapLayers(lays)
        

        fcnt = vlay_raw.selectedFeatureCount()
        log.info('selected %i (of %i) pts by raster no-data extent'%(
            fcnt, vlay_raw.dataProvider().featureCount()))
        
        self.mstore.removeMapLayers(lays)
        
        return vlay_fp, fcnt
    
    def get_edge_samples(self,
                         rToSamp_fp='', #raster layer to sample
                         inun_fp='', #inundation (filtered bby h ydrauilc maximum)
                         ndb_fp='', #nodata boundary polygon of hand layer
                         key_sfx='', 
                         fp_key=None,
                         sample_spacing=None,  #spacing between sampling points
                         plot=True,
                         logger=None,
                         ):
        """
        OG/current setup is to extract points on the edge of a polygon
            then use those points to sample the underlying raster
            
            
            this isnt a very precise treatment of edges 
            inconsistent sampling of inside vs. outside cells
        
        fwdet instead extracts a contour, then rasterizes it
            this also isnt very precise
            
        TODO: explore more precise methods for sampling edge cells of some underling dem using a mask
            polygonize then offsets based on cell size?
            
        Regardless, some unexpected extremes will show up in the edge values
            becuase of the vertical/value variance in the dem
            i.e. some min/max filtering will always be required
        """
        
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        log = self.logger.getChild('edge_samps')
        
        if fp_key is None: 
            fp_key='smpls%s_fp'%key_sfx
        
        log.debug('on %s'%os.path.basename(rToSamp_fp))
        meta_d = {'smpl_fieldName':self.smpl_fieldName, 'sample_spacing':sample_spacing}
        #=======================================================================
        # get the sampling points
        #=======================================================================
        
        smpts_fp, d = self.get_smpl_pts(inun_fp=inun_fp,
                                          ndb_fp=ndb_fp,
                                          fp_key = 'smpts%s_fp'%key_sfx,
                                          spacing=sample_spacing, #just getting the q3 
                                          logger=log)
        
        meta_d.update({'get_smpl_pts':d})
        
        #=======================================================================
        # #sample the raster
        #=======================================================================
        smpls_fp = self.get_samples(smpts_fp=smpts_fp, hand_fp=rToSamp_fp, logger=log,
                                     fp_key = fp_key)
        
        #=======================================================================
        # get stats
        #=======================================================================
        if plot:
            self.plot_samples(smpls_fp)
        
        
        return smpls_fp, meta_d
    
    def plot_samples(self, #helper for plotting the histogram of the samples
                     fp,
                     logger=None,
                     ):
        
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        log=logger.getChild('plot_samples')
        

    def get_samples(self,
                    smpts_fp='', 
                    hand_fp='',
                    fp_key = 'smpls1_fp',
                    logger=None,
                    ):
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        log=logger.getChild('get_samples')
 
        mstore = QgsMapLayerStore()
 
        #=======================================================================
        # build
        #=======================================================================
        if not fp_key in self.fp_d:
            #===================================================================
            # setup
            #===================================================================
            log.info('building \'%s\' from %s'%(fp_key, os.path.basename(smpts_fp)))
                    
            ofp = os.path.join(self.out_dir, self.layName_pfx+'_%s.gpkg'%fp_key.replace('_fp', ''))
        
            if os.path.exists(ofp): assert self.overwrite
            
            #=======================================================================
            # sample
            #=======================================================================
            pts_vlay = self.vlay_load(smpts_fp, logger=log)
            mstore.addMapLayer(pts_vlay)
            
            
            smpl_vlay = self.rastersampling(pts_vlay, hand_fp, logger=log,
                                       pfx='hand_',
                                #output=ofp,
                                )
            
            self.createspatialindex(smpl_vlay)
            #===================================================================
            # get the new field name
            #===================================================================
            old_nms = [f.name() for f in pts_vlay.fields()]
            new_nms = [f.name() for f in smpl_vlay.fields()]
            
            d_nms = list(set(new_nms).difference(old_nms))
            
            assert len(d_nms)==1
            
            #check against expectation
            fnm =  d_nms[0]
            if not fnm==self.smpl_fieldName: log.warning('sample field name mismatch \'%s\''%fnm)
            
            #set
            self.smpl_fieldName =fnm
            
            #===================================================================
            # write
            #===================================================================
            self.vlay_write(smpl_vlay, ofp, logger=log)
            
            #===================================================================
            # get stats
            #===================================================================
            df = vlay_get_fdf(smpl_vlay)
            self.session.smry_d['%s_vals'%fp_key.replace('_fp', '')] = df
            
            #===================================================================
            # wrap
            #===================================================================
            self.ofp_d[fp_key]= ofp
        else:   
            ofp = self.fp_d[fp_key]
            smpl_vlay = self.vlay_load(ofp, logger=log)

 
            
        #=======================================================================
        # check
        #=======================================================================
        assert self.smpl_fieldName in [f.name() for f in smpl_vlay.fields()], \
            '\'%s\' missing field \'%s\''%(smpl_vlay.name(), self.smpl_fieldName)
        
        #=======================================================================
        # warp
        #=======================================================================`
        mstore.addMapLayer(smpl_vlay)
        mstore.removeAllMapLayers()
        
        
        log.info('got \'%s\'  at \n    %s'%(
            fp_key,   ofp))
        
        return ofp
    
    
    def cap_samples(self, #force lower/upper bounds on some points
                    smpls_fp='', #points vector layer
                    vmin=0.0, #minimum HAND value to allow
                    vmax=100.0,
                    prec=3,
                    plot=None,
                    logger=None,
                    ):
        """
        numeric upper and lower bound forcing
            no adjustment of location as the lower bound inundation can not be hydraulically determined
        
        min/max typically calculated by get_sample_bounds() using quartiles of the initial sample
        
        see note on get_edge_samples()
        
        TODO:
            merge w/ Session.get_sample_bounds
        """
        
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        log=logger.getChild('cap_samples')
        if plot is None:
            plot=self.plot
        
        fp_key = 'smpls2C_fp'
        
        meta_d=dict()
        #=======================================================================
        # build
        #=======================================================================
        if not fp_key in self.fp_d:
            #===================================================================
            # setup
            #===================================================================
            log.info('forcing bounds (%.2f and %.2f) on  %s'%(
                vmin, vmax, os.path.basename(smpls_fp)))
            
            coln = self.smpl_fieldName
            
            ofp = os.path.join(self.out_dir, self.layName_pfx+'_smplsC.gpkg')
        
            if os.path.exists(ofp): 
                assert self.overwrite
            mstore= QgsMapLayerStore()
            #=======================================================================
            # load the layer
            #=======================================================================
            vlay_raw = self.vlay_load(smpls_fp, logger=log)
            mstore.addMapLayer(vlay_raw)
            
            #===================================================================
            # get values
            #===================================================================
            df_raw = vlay_get_fdf(vlay_raw, logger=log)
            assert 'float' in df_raw[coln].dtype.name
            
            sraw = df_raw[coln].round(prec)
            
            df = df_raw.copy()
            #===================================================================
            # force new lower bounds
            #===================================================================
            bx = sraw<vmin
            df.loc[bx, coln] = vmin
            
            #===================================================================
            # force upper bounds
            #===================================================================
            bx_up = sraw>vmax
            df.loc[bx_up, coln] = vmax
            
            log.info('set %i / %i (of %i) min/max vals %.2f / %.2f'%(
                bx.sum(), bx_up.sum(), len(bx), vmin, vmax))
            
            #===================================================================
            # build result
            #===================================================================
            geo_d= vlay_get_geo(vlay_raw, logger=log)
            
            res_vlay = self.vlay_new_df(df, geo_d=geo_d, logger=log)
            
            self.vlay_write(res_vlay,ofp,  logger=log)
            
            #===================================================================
            # plot
            #===================================================================
            if plot:
                self.plot_hand_vals(sraw, 
                                    title='cap_samples',
                            xval_lines_d={'max':vmax,'min':vmin}, 
                                label=os.path.basename(smpls_fp),logger=log)
                
            
            #===================================================================
            # meta
            #===================================================================
            meta_d.update({'max_cap_cnt':bx_up.sum(), 
                           'min_floor_cnt':bx.sum(),
                           'total':len(bx)})
            
            self.ofp_d[fp_key] = ofp
        #=======================================================================
        # load
        #=======================================================================
        else:
            ofp = self.fp_d[fp_key]

        #=======================================================================
        # wrap
        #=======================================================================
        log.info('capped minimums \'%s\' \n    %s'%(
               fp_key, ofp))
        
        return ofp, {'cap_samples':meta_d}

    
    def get_interp(self,
                       smpl_fp, #raw HAND value points
                       hand_fp='', #hand raster (for extents)
                       distP= None, #distance coeefificent
                       resolution=None, #resolution of interpolated raster. 
                        #None=2 x sample_spacing
 
                    logger=None,
                    ):

        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        log=logger.getChild('get_interp')
        
        fp_key = 'interp_fp'
        #=======================================================================
        # build
        #=======================================================================
        if not fp_key in self.fp_d:
            #===================================================================
            # setup
            #===================================================================
            ofp = os.path.join(self.out_dir, self.layName_pfx+'_interp.tif')
        
            if os.path.exists(ofp): 
                assert self.overwrite
                os.remove(ofp)
            
            #load samples
            smpl_vlay = self.vlay_load(smpl_fp, logger=log)
            mstore = QgsMapLayerStore()
            mstore.addMapLayer(smpl_vlay)
            
            #===================================================================
            # get default parameters
            #===================================================================
            #distance coeffiocient
            if distP is None:
                #distP = float(self.sample_spacing) #sems decent
                distP=2.0 #I think this is unitless
            assert isinstance(distP, float)
            
            #pixel size
            """nice to preserve resolution until the downsampling
                    NO! too slow"""
            if resolution is None:
                resolution = int(self.sample_spacing)*2
                
            #extents
            rlay = self.rlay_load(hand_fp, logger=log)
            mstore.addMapLayer(rlay)
                
                
            log.info('IDW Interpolating HAND values from \'%s\' (%i)\n '%(
                        smpl_vlay.name(), smpl_vlay.dataProvider().featureCount()) +\
                        '    distP=%.2f, resolution=%i'%(distP, resolution))
 
            #===================================================================
            # get interpolated raster
            #===================================================================
            """couldnt figure out how to configure the input field"""
            #===================================================================
            # interp_rlay = self.idwinterpolation(pts_vlay, coln, resolution, distP=distP, 
            #                                     logger=log)
            #===================================================================
            """tried a bit to get this to work... could be worth more effort as its probably faster"""
            #===================================================================
            # interp_rlay = Whitebox(out_dir=self.out_dir, logger=logger
            #      ).IdwInterpolation(smpl_vlay_fp, self.smpl_fieldName,
            #                         weight=distP, cell_size=resolution,
            #                         logger=log, out_fp=ofp)
            #===================================================================
            """GRASS.. a bit slow"""
            interp_raw_fp = self.vSurfIdw(smpl_vlay, self.smpl_fieldName, distP=distP,
                          pts_cnt=10, cell_size=resolution, extents=rlay.extent(),
                          logger=log, 
                          output=os.path.join(self.temp_dir, 'vsurfidw.tif'),
                          )
            assert os.path.exists(interp_raw_fp)
            #===================================================================
            # fix resolution
            #===================================================================
            """v.surf.idw isnt generating the correct resolution
            ores = self.get_resolution(interp_raw_fp)"""
            log.info('warpreproject resolution=%.4f on  %s'%(resolution, interp_raw_fp))
            self.warpreproject(interp_raw_fp, 
                               resolution=resolution, nodata_val=-9999,
                               output=ofp, logger=log)
            
            
            ores = self.get_resolution(ofp)
            assert ores  == resolution
            
            meta_d = {'distP':distP, 'interp_resolution':resolution}
            
            self.ofp_d[fp_key]= ofp
        else:
            ofp = self.fp_d[fp_key]
            meta_d=dict()
 
        #=======================================================================
        # wrap
        #=======================================================================
        
        log.info('got \'%s\' at \n    %s'%(fp_key, ofp))
        
 
        return ofp, meta_d
    
    def interp_interior(self, #interpolate edge values onto interior
                        rlay_fp, #raster with initial interpolation
                        inun2r_fp='', #maximum inundation raster
                      logger=None,
                      ):
        """
        the IDW interpolation doesnt give very good values in the interior
        """
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        log=logger.getChild('interp_interior')

        fp_key = 'interp2_fp'
 
            
        #=======================================================================
        # build
        #=======================================================================
        if not fp_key in self.fp_d:
            log.info('\'%s\' building interior interpolation on %s'%(fp_key, os.path.basename(rlay_fp)))
            
            #setup paths
            ofp = os.path.join(self.out_dir, self.layName_pfx+'_interp2.tif')
            if os.path.exists(ofp): assert self.overwrite
            assert os.path.exists(inun2r_fp)
            #===================================================================
            # extraploate into interior
            #===================================================================
            self.wsl_extrap_wbt(rlay_fp,inun2r_fp,  logger=log, ofp=ofp)
            

            self.ofp_d[fp_key]= ofp
        else:
            ofp = self.fp_d[fp_key]
 
        #=======================================================================
        # get meta
        #=======================================================================
        stats_d = self.rasterlayerstatistics(ofp)
        stats_d2 = {'range':stats_d['RANGE'], 'min':stats_d['MIN'], 'max':stats_d['MAX']}
        stats_d2 = {k:round(v,2) for k,v in stats_d2.items()}
        #=======================================================================
        # wrap
        #=======================================================================
        
        log.info('got \'%s\' at \n    %s'%(fp_key, ofp))
 
        return ofp, {'interp2':stats_d2}
        
    
    def smooth_hvals(self,
                     interp_rlay_fp, #raw HAND value points
 
                     resolution=None,
                     
                     range_thresh=None, #maximum range (between HAND cell values) to allow
                        #None: calc from max_slope and resolution
                     max_grade = 0.05, #maximum hand value grade to allow 
                     
                     max_iter=20, #maximum number of smoothing iterations to allow
                     hval_prec=0.2, #reserved for mround
                     debug=False,
                    logger=None,
                    fp_key = 'shvals_fp',
                    ):
        """
 

        """
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        log=logger.getChild('smooth_hvals')
        
        #assert hval_prec==1, 'other precisions not implemmented'
 
        
 
        meta_d={'max_grade':max_grade, 'max_iter':max_iter}
        #=======================================================================
        # build
        #=======================================================================
        if not fp_key in self.fp_d:
            #===================================================================
            # setup
            #===================================================================
            if resolution is None: 
                """not much benefit to downsampling actually
                        just makes the smoothing iterations faster"""
                resolution = int(self.sample_spacing*3)
                
            if range_thresh is  None:
                """capped at 2.0 for low resolution runs""" 
                range_thresh = min(max_grade*resolution, 2.0)
                
                
            meta_d.update({'smooth_resolution':resolution, 'smooth_range_thresh':range_thresh})
                
            log.info('applying low-pass filter and downsampling (%.2f) from %s'%(
                resolution, os.path.basename(interp_rlay_fp)))
                
            #filepath
            ofp = os.path.join(self.out_dir, self.layName_pfx+'_hvgrid.tif')
        
            if os.path.exists(ofp):
                os.remove(ofp) 
                assert self.overwrite
            
            
            #===================================================================
            # smooth initial
            #===================================================================
            smooth_rlay_fp1 = self.rNeighbors(interp_rlay_fp,
                            neighborhood_size=7, 
                            circular_neighborhood=True,
                            cell_size=resolution,
                            #output=ofp, 
                            logger=log)
            
            assert os.path.exists(smooth_rlay_fp1)
            #===================================================================
            # get mask
            #===================================================================
            """
            getting a new mask from teh smoothed as this has grown outward
            """
            mask_fp = self.mask_build(smooth_rlay_fp1, logger=log,
                            ofp=os.path.join(self.temp_dir, 'inun31_mask.tif'))
            

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
                    check, rlay_fp_i, rvali, fail_pct = self.smooth_iter(rlay_fp_i, 
                                                               range_thresh=range_thresh,
                                                               mask=mask_fp,
                                                               logger=log.getChild(str(i)),
                                                               out_dir=temp_dir,
                                                               sfx='%03d'%i,
                                                               debug=debug)
                except Exception as e:
                    log.warning('smooth_iter %i failed w/ \n    %s'%(i, e))
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
            meta_d.update({'smooth_iters':i, 'smooth_rval':round(rvali, 3)})
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
            
            self.session.smry_d['smooth_hvals'] = df.copy() #add tot he summary sheet
            #===================================================================
            # copy to result path
            #===================================================================
            """TODO: replace with something that can mround"""
            #self.roundraster(rlay_fp_i, prec=1, logger=log, output=ofp)
            self.rlay_mround(rlay_fp_i, output=ofp, logger=log, multiple=hval_prec)

            #===================================================================
            # build animations
            #===================================================================
            if debug:
                capture_images(
                    os.path.join(self.out_dir, self.layName_pfx+'_shvals_avg.gif'),
                    os.path.join(temp_dir, 'avg')
                    )
                
                capture_images(
                    os.path.join(self.out_dir, self.layName_pfx+'_shvals_range.gif'),
                    os.path.join(temp_dir, 'range')
                    )
                
            #===================================================================
            # reapply mask
            #===================================================================
            """moved inside loop
            self.mask_apply(rlay_fp_i, mask_fp, ofp=ofp, logger=log)"""
            self.ofp_d[fp_key]= ofp
        else:
            ofp = self.fp_d[fp_key]
            
        #=======================================================================
        # get meta
        #=======================================================================
        stats_d = self.rasterlayerstatistics(ofp)
        stats_d2 = {'end_range':stats_d['RANGE'], 'end_min':stats_d['MIN'], 'end_max':stats_d['MAX']}
        stats_d2 = {k:round(v,2) for k,v in stats_d2.items()}
        meta_d.update(stats_d2)
        #=======================================================================
        # wrap
        #=======================================================================
        
        log.info('got \'%s\' at \n    %s'%(fp_key, ofp))
 
        return ofp, meta_d
    
    def smooth_iter(self,  #check if range threshold is satisifed... or smooth 
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
    



    def __exit__(self, #destructor
                 *args,**kwargs):
        
        s0=''
        for k,v in self.ofp_d.items(): 
            s0 = s0+'\n    \'%s\':r\'%s\','%(k,  v)
        
        #print('exit w/ %s'%s0)
        
        super().__exit__(*args,**kwargs) #initilzie teh baseclass

        
        
        
        
        
        


def build_hmax(
        name='CMM2',
        crsid = 'EPSG:2950',
        #sample_resolution=100,
        
        #30secs
        inun_fp = r'C:\LS\03_TOOLS\_jobs\202103_InsCrve\outs\depWf\20210719a\CMMt1_depWf_0719_inun1.gpkg', #rawish inundation
        hand_fp = r'C:\LS\03_TOOLS\_jobs\202103_InsCrve\outs\depWf\20210716t1\CMMt1_depWf_HAND_0716.tif',
        ndb_fp = r'C:\LS\03_TOOLS\_jobs\202103_InsCrve\outs\depWf\20210720\CMMt1_depWf_0720_ndb.gpkg',
        
        fp_d = {
 

            },

        out_dir=None,
        #out_dir = r'C:\LS\03_TOOLS\_jobs\202103_InsCrve\outs\HANDin\20210719b',
        ):
    
    with HIses(name=name, overwrite=True, fp_d=fp_d, out_dir=out_dir,
               crs=QgsCoordinateReferenceSystem(crsid),
               ) as wrkr:

        wrkr.run_hmax(hand_fp=hand_fp,inun1_fp=inun_fp,ndb_fp=ndb_fp
                 #sample_resolution=sample_resolution,
                 )
        
        wrkr._log_datafiles()
    
def build_hvgrid(
        name='CMM2',
        crsid = 'EPSG:2950',
        #sample_resolution=100,
        
        #30secs
        inun2_fp = r'C:\LS\03_TOOLS\_jobs\202103_InsCrve\outs\depWf\20210720\CMMt1_depWf_0720_inun2.gpkg', #rawish inundation
        inun2r_fp = r'C:\LS\03_TOOLS\_jobs\202103_InsCrve\outs\depWf\20210720\CMMt1_depWf_0720_inun2r.tif',
        hand_fp=r'C:\LS\03_TOOLS\_jobs\202103_InsCrve\outs\depWf\20210716t1\CMMt1_depWf_HAND_0716.tif',
        ndb_fp = r'C:\LS\03_TOOLS\_jobs\202103_InsCrve\outs\depWf\20210720\CMMt1_depWf_0720_ndb.gpkg',
        
        fp_d = {
                 'smpts2_fp':r'C:\LS\03_TOOLS\_jobs\202103_InsCrve\outs\depWf\20210720\CMMt1_depWf_0720_smpts2.gpkg',
                 'smpls2_fp':r'C:\LS\03_TOOLS\_jobs\202103_InsCrve\outs\depWf\20210720\CMMt1_depWf_0720_smpls2.gpkg',
                 'interp_fp':r'C:\LS\03_TOOLS\_jobs\202103_InsCrve\outs\depWf\20210720\CMMt1_depWf_0720_interp.tif',
                 'interp2_fp':r'C:\LS\03_TOOLS\_jobs\202103_InsCrve\outs\depWf\20210720\CMMt1_depWf_0720_interp2.tif',
            },

        out_dir=None,
        #out_dir = r'C:\LS\03_TOOLS\_jobs\202103_InsCrve\outs\HANDin\20210719b',
        ):
    
    with HIses(name=name, overwrite=True, fp_d=fp_d, out_dir=out_dir,
               crs=QgsCoordinateReferenceSystem(crsid),
               ) as wrkr:

        wrkr.run_hvgrid(inun2_fp=inun2_fp, inun2r_fp=inun2r_fp,
                                      hand_fp=hand_fp,ndb_fp=ndb_fp,
                 )
        
        wrkr._log_datafiles()
        
        
        
        
if __name__ =="__main__": 
    start =  datetime.datetime.now()
    print('start at %s'%start)


    #build_hmax()
    
    build_hvgrid()

    
    

    
    tdelta = datetime.datetime.now() - start
    print('finished in %s'%tdelta)