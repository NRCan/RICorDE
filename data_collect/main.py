'''
Created on Mar. 25, 2022

@author: cefect
'''
import os, datetime, copy
start =  datetime.datetime.now()

from hp.oop import Session as baseSession
     
from ricorde.tcoms import TComs

class Session(TComs, baseSession):
    
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
        d = self.ofp_d
        log.info('%i data files built \n    %s'%(
            len(d), list(d.keys())))
 
        
        #self._log_datafiles(log)
        
        """
        self.ofp_d['dem_fp']='test'
        """
        
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
                vlay_db = wrkr.load_db(logger=log)
    
                raw_fp, d1 = wrkr.get_fromDB(vlay_db, logger=log,  **kwargs)
                
                fp, d2 =wrkr.clean_fic(raw_fp, reproject=True)
                
                self.meta_d.update({'get_fromDB':{**d1, **d2}})


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
        assert vlay.wkbType()==6
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
        """
        TODO: need to separate HRDEM (canada) from passing a dem_fp
        """
        
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
        raw_res = self.rlay_get_resolution(rlay)
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