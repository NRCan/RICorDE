'''
Created on Jul. 17, 2021

@author: cefect

common methods for the depths workflow 1
'''

#===============================================================================
# imports
#===============================================================================
import os, datetime, copy
import processing

from hp.Q import Qproj, QgsCoordinateReferenceSystem, QgsMapLayerStore, QgsRasterLayer


from hp.exceptions import Error
from hp.dirz import delete_dir

from hp.whitebox import Whitebox





class TComs(Qproj):
    
    dem_psize=1.0 #pixel size of dem
    hval_prec=1
    
    #adding placeholders for runs w/o aois
    aoi_vlay=None
    aoi_fp=None
    
    def __init__(self, 
                 smpl_fieldName='hand_1',
                 layName_pfx=None,
                 fp_d = {},
                 work_dir = r'C:\LS\03_TOOLS\RICorDE',
             **kwargs):
        
        super().__init__(work_dir=work_dir,**kwargs)
        
        self.smpl_fieldName=smpl_fieldName
        
        if layName_pfx is None:
            layName_pfx = '%s_%s_%s'%(self.name, self.tag,  datetime.datetime.now().strftime('%m%d'))
        self.layName_pfx = layName_pfx
        
        #data containers
        self.fp_d = fp_d
        self.ofp_d = dict()
        
        #=======================================================================
        # #temporary directory
        #=======================================================================
        self.temp_dir = os.path.join(self.out_dir, 'temp_%s_%s'%(
            self.__class__.__name__, datetime.datetime.now().strftime('%M%S')))
        if os.path.exists(self.temp_dir):
            delete_dir(self.temp_dir)

        if not os.path.exists(self.temp_dir):os.makedirs(self.temp_dir)
        
        if len(fp_d)>0:
            self.logger.info('init w/ %i preloaded datasets: \n    %s'%(
                len(fp_d), list(fp_d.keys())))
         
    #===========================================================================
    # common sub-routines--------
    #===========================================================================
    def rasterize_inun(self, #rasterize an inundation layer (raster props from a reference layer)
                      rlay_fp,

                      ref_lay=None, #layer to use for reference
                      #compress=None, hard coded medium compression
                      
                    fp_key=None, #key for checking if the layer is already built
                      ofp=None,
                      logger=None,
                      ):
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        log=logger.getChild('rasterize_inun')
        
        #if compress is None: compress=self.compress
        #=======================================================================
        # check build status
        #=======================================================================
        build=True
        if not fp_key is None:
            if fp_key in self.fp_d:
                build=False
                
        #=======================================================================
        # build
        #=======================================================================
        if build:
            
 
            mstore = QgsMapLayerStore()
            if isinstance(ref_lay, str):
                ref_lay=self.rlay_load(ref_lay, logger=log)
                mstore.addMapLayer(ref_lay)
            
            assert ref_lay.crs() == self.qproj.crs()
            assert os.path.exists(rlay_fp)
            assert os.path.exists(self.out_dir)
            #=======================================================================
            # #rasterize
            #=======================================================================
            if ofp is None:
                """special default"""
                ofp = os.path.join(self.out_dir, os.path.splitext(os.path.basename(rlay_fp))[0]+'.tif')
            
            
            #get reference values
            rect = ref_lay.extent()
            extent = '%s,%s,%s,%s'%(rect.xMinimum(), rect.xMaximum(), rect.yMinimum(), rect.yMaximum())+ \
                    ' [%s]'%ref_lay.crs().authid()
                    
            
            #build pars
            """want to match the dem exactly... so using a special implementation"""
            pars_d = { 'BURN' : 1, 'NODATA' : 0,
             'DATA_TYPE' : 5, 'UNITS' : 0, #pixels         
             'EXTRA' : '', 'FIELD' : '', 'INVERT' : False, 
             #'HEIGHT' : 14152, 'WIDTH' : 21807,
             'HEIGHT' : ref_lay.height(), 'WIDTH' : ref_lay.width(), 'EXTENT' : extent, 
             'INIT' : None, 
             'INPUT' : rlay_fp,  'OUTPUT' : ofp, 
             'OPTIONS' : 'COMPRESS=LZW', #lite compression for WhiteBoxx 
    
               }
            
            #execute
            algo_nm = 'gdal:rasterize'
            log.debug('%s w/ \n    %s'%(algo_nm, pars_d))
            ofp = processing.run(algo_nm, pars_d, feedback=self.feedback)['OUTPUT']
        
            #=======================================================================
            # wrap
            #=======================================================================
            mstore.removeAllMapLayers()
        
        #=======================================================================
        # load
        #=======================================================================
        else:
            ofp = self.fp_d[fp_key]
            
        if not fp_key is None:
            log.info('got \"%s\': \n    %s'%(fp_key, ofp))
        else:
            log.debug('got %s'%ofp)
            
        
        return ofp
    
    def clean_inun_vlay(self, #typical operations for cleaning an inundation polygon
                        vlay_raw,
                        output='TEMPORARY_OUTPUT',
                        simp_dist=None,
                        hole_size=None,
                        island_size=None,
                        logger=None, mstore=None,
                        ):
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        log=logger.getChild('clean_iv')
 
        if mstore is None:
            clear_mstore=True
            mstore = QgsMapLayerStore()
        else:
            clear_mstore=False
            
        #parameter defaults
        assert isinstance(self.dem_psize, float)            
        if simp_dist is None: simp_dist = self.dem_psize
        if hole_size is None: hole_size = (self.dem_psize*5)**2.0
        if island_size is  None: island_size=hole_size
        
        log.info('cleaning \'%s\' w/ simp_dist=%s, hole_size=%s'%(
            vlay_raw, simp_dist, hole_size))
        
        #=======================================================================
        # fix geo
        #=======================================================================
        vlay1 = self.fixgeo(vlay_raw, logger=log)
        mstore.addMapLayer(vlay1)
        #=======================================================================
        # dissovlve
        #=======================================================================

        vlay2= self.dissolve(vlay1, logger=log)
        mstore.addMapLayer(vlay2)
        
        #===================================================================
        # remove fields
        #===================================================================
        vlay3 = self.deletecolumn(vlay2, [f.name() for f in vlay2.fields()], logger=log)
        mstore.addMapLayer(vlay3)
        
        #===================================================================
        # simplify 
        #===================================================================
        """best not to have more complexity than the dem resolution"""
        if not simp_dist =='none':
            vlay3simp = self.simplifygeometries(vlay3, simp_dist=simp_dist, logger=log)
            mstore.addMapLayer(vlay3simp)
        else:
            vlay3simp = vlay3
        #===================================================================
        # delete holes
        #===================================================================
        """because of the overlap... often some small artificact holes remain
        but we still want to preserve islands"""

        if not hole_size =='none':
            vlay4 = self.deleteholes(vlay3simp, hole_area=hole_size, logger=log)
            mstore.addMapLayer(vlay4)
        else:
            vlay4=vlay3simp
        
        #=======================================================================
        # break to singleparts
        #=======================================================================
        
        vlay5 = self.multiparttosingleparts(vlay4, logger=log)
        
        
        #=======================================================================
        # remvoe small feats
        #=======================================================================
        if not island_size=='none':
            res = self.extractbyexpression(vlay5, '$area>%i'%island_size, output=output, logger=log)
        else:
            raise Error('dome')
        
        #=======================================================================
        # wrap
        #=======================================================================
        if clear_mstore: mstore.removeAllMapLayers()
        
        log.debug('finished w/ %s'%res)
        return res
        
    def inun_max_filter(self, #return inundatio matching BOTH
                        inun_fp, inun_max_fp, 
                        logger=None, 
                        compress=None, #force default
                        **kwargs):
        
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        log=logger.getChild('inun_max_filter')
        
 
        #===================================================================
        # build the raster calc entries
        #===================================================================
 
        rcentry_d = {k:self._rCalcEntry(obj) for k,obj in {'raw':inun_fp,'max':inun_max_fp}.items()}
        
        #===================================================================
        # build the formula
        #===================================================================
        f1 = '({0} AND {1})'.format(rcentry_d['raw'].ref, rcentry_d['max'].ref)
        
        #=======================================================================
        # execute
        #=======================================================================
        
        return self.rcalc1(rcentry_d['raw'].raster, 
                          #'%s/%s'%(f1, f1),
                          f1, #this seems to work...
                          list(rcentry_d.values()),
                          layname=self.layName_pfx + '_inun3',
                          logger=log,
                          compress=compress,
                          **kwargs)
        

    
    def wsl_extrap_grass(self, #carve our inundation and extrapolate with edges
                           dem_fp,
                           inun_fp, #rlay
                           
                           ofp=None,
                           out_dir=None,
                           logger=None,
                           ):
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        log=logger.getChild('wsl_ex_in')
        start =  datetime.datetime.now()
        
        if out_dir is None:
            out_dir = self.temp_dir
        if not os.path.exists(out_dir):os.makedirs(out_dir)
        
        log.debug('on \'%s\' w/ \'%s\''%(
            os.path.basename(dem_fp), os.path.basename(inun_fp)))
        #===================================================================
        # mask out interior
        #===================================================================
        rlay1_fp = self.mask_apply(dem_fp, inun_fp, 
                                         invert_mask=True,
                                         logger=log,
                     ofp=os.path.join(out_dir, 'mask.tif'))
 
        #=======================================================================
        # grow into void
        #=======================================================================
        d = self.rGrowDistance(rlay1_fp, logger=log,
                                  output=os.path.join(out_dir, 'rGrow.tif')
                                  )
        
        rlay2_fp = d['value']
        
        assert os.path.exists(rlay2_fp),'rGrowDistance failed \n    input: %s\n    %s'%(
            rlay1_fp, d)

        #=======================================================================
        # clear exterior
        #=======================================================================
        if ofp is None:
            ofp=os.path.join(out_dir, 'wsl_result.tif')
            
        rlay3_fp = self.mask_apply(rlay2_fp, inun_fp, ofp=ofp,
                                         invert_mask=False,
                                         logger=log)
        
        #=======================================================================
        # wrap
        #=======================================================================
        self.trash_fps = self.trash_fps+[rlay1_fp, rlay2_fp]
        
        log.info('finished in %s w/ \n    %s'%(
            datetime.datetime.now() - start, rlay3_fp))
        
        return rlay3_fp
    
    def wsl_extrap_wbt(self, #carve our inundation and extrapolate with edges. using WBT
                           dem_fp, #terrain to be clipped and extrapolted against
                           inun_fp, #rlay of inundation
                           
                           ofp=None,
                           out_dir=None, #usually this is a temp dir passed b y controller
                           logger=None,
                           ):
        #=======================================================================
        # defaults
        #=======================================================================
        start =  datetime.datetime.now()
        if logger is None: logger=self.logger
        log=logger.getChild('wsl_extrap_wbt')
        
        if out_dir is None:
            out_dir = self.temp_dir
        if not os.path.exists(out_dir):os.makedirs(out_dir)
        
        log.debug('on \'%s\' w/ \'%s\''%(
            os.path.basename(dem_fp), os.path.basename(inun_fp)))
        #===================================================================
        # mask out interior
        #===================================================================
        rlay1_fp = self.mask_apply(dem_fp, inun_fp, 
                                         invert_mask=True,
                                         logger=log,
                     ofp=os.path.join(out_dir, 'dem_maskd.tif'))
 
        #=======================================================================
        # grow into void
        #=======================================================================
        #get neutral cost surface
        cost_fp = self.createconstantrasterlayer(rlay1_fp, burn_val=1, logger=log,
                                                output=os.path.join(out_dir, 'cost_neutral.tif'))
        
        #get the backlink raster
        log.debug('getting the backlink raster')
        
        cd_fp, blink_fp = Whitebox(out_dir=out_dir, logger=log
                 ).costDistance(source_fp=rlay1_fp, cost_fp=cost_fp)
                 
        
        #allocate the costs
        log.debug('getting the costAllocation raster')
        rlay2_fp = Whitebox(out_dir=out_dir, logger=log
                 ).costAllocation(source_fp=rlay1_fp, blink_fp=blink_fp)
                 
                 
        assert os.path.exists(rlay2_fp),'costAllocation failed \n    input: %s'%(
            rlay1_fp)

        #=======================================================================
        # clear exterior
        #=======================================================================
        if ofp is None:
            ofp=os.path.join(out_dir, 'wsl_result.tif')
            
        rlay3_fp = self.mask_apply(rlay2_fp, inun_fp, ofp=ofp,
                                         invert_mask=False,
                                         logger=log)
        
        #=======================================================================
        # wrap
        #=======================================================================
        self.trash_fps = self.trash_fps+[rlay1_fp, rlay2_fp]
        
        log.debug('finished in %s w/ \n    %s'%(
            datetime.datetime.now() - start, rlay3_fp))
        
        return rlay3_fp
    
    def get_hand_inun(self, #get an inundation raster from a hand rasterand value
                      hand_rlay, 
                      hval,
                      compress=None, #use Session compression
                      **kwargs):
        
        
        
        #=======================================================================
        # build calculator constructors
        #=======================================================================

        rcentry=self._rCalcEntry(hand_rlay)
        #=======================================================================
        # build formula
        #=======================================================================
        f1 = '({}<%.3f)'%hval
        f2 = '%s/%s'%(f1, f1)
        formula = f2.format(rcentry.ref, rcentry.ref)
        
        return self.rcalc1(hand_rlay, formula, 
                           [rcentry] , 
                           compress=compress, 
                           **kwargs)
        

    #===========================================================================
    # HELPERS----
    #===========================================================================
    def createconstantrasterlayer(self,
            rlay_fp,
            burn_val=1, #value to burn
 
 
            logger=None,
            output='TEMPORARY_OUTPUT',
            ):
        """
        replacement for native algo not relying on resolution
        """
        if output=='TEMPORARY_OUTPUT':
            out_fp = None
        else:
            out_fp=output
        
        assert isinstance(rlay_fp, str)
 
        
        _ =  Whitebox(out_dir=self.out_dir, logger=logger
                 ).NewRasterFromBase(rlay_fp, value=burn_val, out_fp=out_fp)
                 
        assert self.rlay_check_match(rlay_fp, out_fp, logger=logger), 'new layer failed to match'
        
        return out_fp
        
        
        
 

            
                    
                    
        
        
    def _log_datafiles(self, 
                       log=None,
                       d = None,
                       ):
        if log is None: log=self.logger
        if d is None:
        
            #print each datafile
            d = copy.copy(self.fp_d) #start with the passed
            d.update(self.ofp_d) #add the loaded
        
        

        s0=''
        for k,v in d.items():
            s0 = s0+'\n    \'%s\':r\'%s\','%(k,  v)

                
        log.info(s0)
            
            
    
    def __exit__(self, #destructor
                 *args,**kwargs):
        
        delete_dir(self.temp_dir)
        
        super().__exit__(*args,**kwargs) #initilzie teh baseclass
    
#===============================================================================
# FUNCTIONS---------
#===============================================================================
from memory_profiler import profile
@profile(precision=2)
def test(
        dem_fp=r'C:\LS\03_TOOLS\_jobs\202103_InsCrve\outs\cost\CMM2_ins\HAND_0722.tif',
        inunr_rp=r'C:\LS\03_TOOLS\_jobs\202103_InsCrve\outs\cost\CMM2_ins\inun2r.tif',
        name='wsl_test',
        out_dir=r'C:\LS\03_TOOLS\_jobs\202103_InsCrve\outs\cost\0723',
        crsid='EPSG:2950',
        ):
    
    #===========================================================================
    # test wbt
    #===========================================================================
    with TComs(name=name, overwrite=True, out_dir=os.path.join(out_dir, 'wbt'),
               crs=QgsCoordinateReferenceSystem(crsid)) as wrkr:

 
        wbt_ofp = wrkr.wsl_extrap_wbt(dem_fp, inunr_rp, out_dir=out_dir)
        
    #===========================================================================
    # test grass
    #===========================================================================
    with TComs(name=name, overwrite=True, out_dir=os.path.join(out_dir, 'grass'),
               crs=QgsCoordinateReferenceSystem(crsid)) as wrkr:
        
        r_ofp = wrkr.wsl_extraploate_in(dem_fp, inunr_rp, out_dir=out_dir)

    
 
    
    return 

    
if __name__ =="__main__": 
    
    start =  datetime.datetime.now()
    print('start at %s'%start)
    
    test()

    
    tdelta = datetime.datetime.now() - start
    print('finished in %s'%tdelta)