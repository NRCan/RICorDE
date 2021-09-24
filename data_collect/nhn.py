'''
Created on Mar. 27, 2021

@author: cefect

dowwnload National Hydrauilc Network data from an AOI
'''
#===============================================================================
# imports-----------
#===============================================================================
import os, datetime, copy
import zipfile

 

from hp.exceptions import Error
from hp.dirz import force_open_dir, url_retrieve
 
from hp.Q import Qproj, vlay_get_fdata, QgsVectorLayer #only for Qgis sessions

from hp.gdal import GeoDataBase, get_layer_gdb_dir

 



#===============================================================================
# vars
#===============================================================================



#===============================================================================
# CLASSES----------
#===============================================================================

        
class NHNses(Qproj):
    
    ##layer and field name with NHN index info
    index_fp = r'C:\LS\03_TOOLS\_jobs\202103_InsCrve\_ins\NHN_INDEX_20_0714.gpkg'
 
    ftp_base_url = r'https://ftp.maps.canada.ca/pub/nrcan_rncan/vector/geobase_nhn_rhn/gdb_en'
    
    ofp_d=dict()
    
    waterType_d = {
        'Reservoir':5,
        'Unknown':-1,
        'Lake':4,
        'Watercourse':6,
        }
    
    def __init__(self, 

                 tag='NHN',
                 work_dir = os.path.dirname(os.path.dirname(__file__)),
                 aoi_fp=None,
                 fp_d={}, #reserved
                 **kwargs):
        
        
        
        super().__init__(work_dir=work_dir, #out_dir=os.path.join(work_dir, 'out'),
                         tag=tag,
                          **kwargs)
        self.fp_d=fp_d
        if not aoi_fp is None:
            self.load_aoi(aoi_fp, set_proj_crs=True)
        
        
    def get_FDA(self, #identify the FDA(s) of interest 
                index_fp = None, 
                i1fn='WSCMDA', #field with lvl1 NHN index
                i2fn = 'DATASETNAM', #lvl 2 index
                aoi_vlay=None,
                logger=None,
                ):
        #=======================================================================
        # defaults
        #=======================================================================
        if aoi_vlay is None: aoi_vlay=self.aoi_vlay
        if logger is None: logger=self.logger
        if index_fp is None: index_fp=self.index_fp
        log=logger.getChild('get_FDA')
        
        #=======================================================================
        # load index
        #=======================================================================
        ind_vlay = self.vlay_load(index_fp, reproj=False, logger=log)
        self.mstore.addMapLayer(ind_vlay)
        
        #=======================================================================
        # id intersecting feats
        #=======================================================================
        self.selectbylocation(ind_vlay, aoi_vlay, allow_none=False, logger=log)
        
        
        log.info('got %i NHN basins intersecting aoi \'%s\''%(
            ind_vlay.selectedFeatureCount(), aoi_vlay.name()))
        
        #=======================================================================
        # #pull out the indexs
        #=======================================================================
        i1_d = vlay_get_fdata(ind_vlay, i1fn, selected=True)
        i2_d = vlay_get_fdata(ind_vlay, i2fn, selected=True)
        
        #zip with FDA as key and DA as value
        i_d = dict(zip(i2_d.values(), i1_d.values()))
        
        
        log.debug('got %s'%i_d)
        
        return i_d
    
    def dl_zip(self, #download the gdb zip file from the ftp via the fda
                 indx_d, #{lvl2 keys: lvl1 key} NHN lookup
                 base_dir = None,
                 logger=None,
                 ):
        """
        todo: use local file if found
        """
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        log=logger.getChild('dl_zip')
        if base_dir is None: 
            base_dir = os.path.join(self.temp_dir,  'zips')
        
        log.info('attempting to download %i NHN gdbs'%len(indx_d))
        
        #=======================================================================
        # download each
        #=======================================================================
        res_d = dict()
        
        for k1, k0 in indx_d.items():
            log.debug('%s:%s'%(k1, k0))
            
            url = self.ftp_base_url + '/%s/nhn_rhn_%s_gdb_en.zip'%(k0, k1)
            
            res_d[k1] = url_retrieve(url.lower(), logger=log.getChild(k1), overwrite=self.overwrite,
                                     use_cache=True, #no-need to re download
                                     ofp = os.path.join(base_dir,os.path.basename(url)))
            
        #=======================================================================
        # wrap
        #=======================================================================
        log.info('finished DL %i zips to %s'%(len(res_d), self.out_dir))
 
        
        return res_d
        
 
        
        
    
    
    def extract_zip(self, #extract the contents of the zip files into a single folder
                fp_d,
                ofp= None,
                logger=None,
                ):
        
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        log=logger.getChild('extract')
        
        if ofp is None: 
            ofp = os.path.join(self.temp_dir, 'gdbs')
        #=======================================================================
        # loop through and unzip each
        #=======================================================================
        for k, fp in fp_d.items():
            with zipfile.ZipFile(fp, 'r') as zip_ref:
                zip_ref.extractall(ofp)
                
        #=======================================================================
        # wrap
        #=======================================================================
        log.info('extracted %i zip files to %s'%(len(fp_d), ofp))
 
        
        return ofp
    
    def build_gdb_union(self, #extract a specific layer from all gdbs in a directory and merge
                     gdb_dir,
                     layerName='NHN_HD_WATERBODY_2', #layername to extract from GDBs
                     logger=None,
                     ofp=None,
                     ):
        
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        log = logger.getChild('build_gdb_union')

        #filepaths
        if ofp is None:
            ofp = os.path.join(self.out_dir, 
                       '%s_%s_%s_raw.gpkg'%(
                            layerName[:-2], self.name, datetime.datetime.now().strftime('%m%d')))
        
        if os.path.exists(ofp): 
            assert self.overwrite, ofp
            os.remove(ofp)
        
        
        #=======================================================================
        # #load all the layers from the GDB
        #=======================================================================
        lay_d = get_layer_gdb_dir(gdb_dir, layerName=layerName, logger=logger)
        
        
        
        #=======================================================================
        # #merge
        #=======================================================================

        
        """this transforms to the project crs"""
        _ = self.mergevectorlayers(list(lay_d.values()), logger=logger, output=ofp)
        
        #=======================================================================
        # wrap
        #=======================================================================
        self.mstore.addMapLayers(list(lay_d.values()))
        
        log.info('pulled and merged %i layers and wrote to \n    %s'%(
            len(lay_d), ofp))
        
 


        return ofp
    
    def clean(self, #clean waterbody polygons
              nhn_raw_fp, 
              min_area = 1000.0, #minimum area for threshold filter
              waterTypes = ['Watercourse'], #watertype field values to select
                #attributes of relevance probably depend on the area of interest
              waterType_fn = 'waterDefinition', #field with watertype values
              aoi_vlay=None,
              logger=None):
        
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        log = logger.getChild('clean')
        if aoi_vlay is None: aoi_vlay=self.aoi_vlay
        layerName = os.path.splitext(os.path.basename(nhn_raw_fp))[0]
        
        

        
        #=======================================================================
        # get features of interest
        #=======================================================================
#===============================================================================
#         """consider swwappingt o 'waterDefinition' field (numeric')"""
#         #build expression string
#         for i, e in enumerate(waterTypes):
#             assert len(e)>0
#             if i==0:
#                 s = '\'{}\''.format(e)
#             else:
#                 s = s+',\'{}\''.format(e)
#                 
# 
#         
#         exp_str = '"{}" in ({})'.format(waterType_fn, s)
#         
#         
#         res_d = self.extractbyexpression(nhn_raw_fp, exp_str, logger=log, 
#                                          output=os.path.join(self.temp_dir, 'nhn_type.gpkg'),
#                                                              fail_output='TEMPORARY_OUTPUT')
#===============================================================================
        #selection loop
        vlay_raw = self.vlay_load(nhn_raw_fp, logger=log)
        sel_d=dict()
        cnt=0
        for waterType in waterTypes:
            assert waterType in self.waterType_d, 'missing \'%s\''%waterType
            self.selectbyattribute(vlay_raw, waterType_fn, self.waterType_d[waterType])
            sel_d[waterType] = vlay_raw.selectedFeatureCount()-cnt
            cnt=vlay_raw.selectedFeatureCount()
            
        #save these
        assert vlay_raw.selectedFeatureCount()>0
        vlay0_fp = self.saveselectedfeatures(vlay_raw, 
                         output=os.path.join(self.temp_dir, 'nhn_selectedFeats.gpkg'))
        
        fcnt1 = vlay_raw.selectedFeatureCount()
        fcnt0 = vlay_raw.dataProvider().featureCount()
        
        log.info('pulled %i (of %i) feats with \'%s\'=\n    %s'%(
            fcnt1, fcnt1+fcnt0, waterType_fn, sel_d))
        
 
        
        #=======================================================================
        # fix geometry
        #=======================================================================
        vlay1 = self.fixgeo(vlay0_fp, logger=log)
        self.mstore.addMapLayer(vlay1)
        
        #=======================================================================
        # clip to aoi
        #=======================================================================
        """after fixgeo?"""
        vlay1b = self.clip(vlay1, aoi_vlay, logger=log,
                           output=os.path.join(self.temp_dir, 'nhn_clipd.gpkg'))
        #self.mstore.addMapLayer(vlay1b)
        
        #=======================================================================
        # remove tinys
        #=======================================================================
        """consider merging with attribute selection"""
        vlay2 = self.extractbyexpression(vlay1b, '$area>%.2f'%min_area, logger=log)['OUTPUT']
        fcnt2 = vlay2.dataProvider().featureCount()
        
        log.info('dropped %i (of %i) features with area < %.2f'%(
            fcnt1-fcnt2, fcnt1, min_area))
        
        self.mstore.addMapLayer(vlay2)
        
        
        #=======================================================================
        # dissolve
        #=======================================================================
        vlay3 = self.dissolve(vlay2, fields=[waterType_fn], logger=log)
        self.mstore.addMapLayer(vlay3)
        
        #=======================================================================
        #explode to singleparts
        #=======================================================================
        
        ofp = os.path.join(self.out_dir,layerName.replace('raw', 'clean')+'.gpkg')
        
        if os.path.exists(ofp): 
            assert self.overwrite, ofp
            os.remove(ofp)

        self.multiparttosingleparts(vlay3, output=ofp, logger=log)

        #=======================================================================
        # wrap
        #=======================================================================
        meta_d = {'NHN_waterTypes':waterTypes,
                  'NHN_fcnt':fcnt2
                  }
        log.info('finished cleaning. output to \n    %s'%ofp)
        
        
        return ofp, meta_d
    
    def run(self, #convience bundling of typical workflow
            waterTypes = ['Watercourse'],
            logger=None,
            ):
        
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        log = logger.getChild('r')
        
        #=======================================================================
        # get the raw data
        #=======================================================================
        fp_key = 'nhn_raw_fp'
        
        if not fp_key in self.fp_d:
            #id the basins intersecting the aoi
            indx_l = self.get_FDA(logger=log)
            
            #download each zip file
            zip_fp_d = self.dl_zip(indx_l, logger=log)
            
            #pull out of the zips
            gdb_fp = self.extract_zip(zip_fp_d, logger=log)
            
            #merge together
            fp_raw = self.build_gdb_union(gdb_fp, logger=log)
            
            
            self.ofp_d[fp_key] = fp_raw
        else:
            fp_raw = self.fp_d[fp_key]

        #=======================================================================
        # #clean the result
        #=======================================================================
        fp, meta_d= self.clean(fp_raw,
                       waterTypes=waterTypes, logger=log)
        
        #=======================================================================
        # wrap
        #=======================================================================
        self.ofp_d['nhn_fp'] = fp
        
        log.info('finished w/ \n    %s'%self.ofp_d)
        
        return self.ofp_d, meta_d
            

        
    
        
 

                       
        


def CMM(): #extract FiC polygons for Montreal
    
    
    with NHNses(
        
        work_dir = r'C:\LS\03_TOOLS\_jobs\202103_InsCrve',
        aoi_fp=r'C:\LS\02_WORK\02_Mscripts\InsuranceCurves\04_CALC\CMM\aoi\aoi02_CMM_20210711.gpkg',
        
        #big square
        #aoi_fp = r'C:\LS\02_WORK\02_Mscripts\InsuranceCurves\04_CALC\CMM\aoi\aoi01_CMM_20210711.gpkg',

        name='CMM',
        overwrite=True,
        ) as wrkr:
    
        fp = wrkr.run(waterTypes = ['Watercourse', 'Reservoir'])
        

    
    
    return fp






















if __name__ =="__main__": 
    
    start =  datetime.datetime.now()
    print('start at %s'%start)
    
    CMM()

    
    tdelta = datetime.datetime.now() - start
    print('finished in %s'%tdelta)
