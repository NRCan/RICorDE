'''
Created on Mar. 27, 2021

@author: cefect

generate inundation rasters from FloodsInCanada
    inputs:
        date(s)
        location
        
FiC raw data is provided on an FTP
    so I'm just using my local copies
    TODO: connect w/ web servers
    
    
TODO: build workflow to identify peak flow day from:
    location
    year
    HYDAT
'''
#===============================================================================
# imports-----------
#===============================================================================
import os, datetime, zipfile
import pandas as pd
import numpy as np

from PyQt5.QtCore import QDate

 

from hp.exceptions import Error
from hp.dirz import force_open_dir
from hp.oop import Basic
from hp.plot import Plotr #only needed for plotting sessions
from hp.Q import Qproj, QgsDateRange, QgsExpression, processing, QgsMapLayerStore,\
    QgsCoordinateReferenceSystem #only for Qgis sessions
 


#===============================================================================
# vars
#===============================================================================



#===============================================================================
# CLASSES----------
#===============================================================================
        
        
class ficSession(Qproj):
    
    def __init__(self, 
                 #location of FiC polygon database
                 fic_lib_fp = r'C:\LS\05_DATA\Canada\GOC\NRCan\FloodsInCanada\archive\gdb\20210707\EGS_Flood_Product_Archive.gdb',

                 tag='fic',
                 #work_dir = os.path.dirname(os.path.dirname(__file__)),
                 **kwargs):
        
        self.fic_lib_fp=fic_lib_fp
        
        super().__init__(
                        #work_dir=work_dir, #out_dir=os.path.join(work_dir, 'out'),
                         tag=tag,
                          **kwargs)
        
 
        
        
    def load_db(self, #loadthe FiC polygon database
            fp=None,
            date_fieldNm = 'date_utc',
            logger=None,
            ):
        #===================================================================
        # defaults
        #===================================================================
        if logger is None: logger=self.logger
        log = logger.getChild('load_db')
        
        if fp is None: fp = self.fic_lib_fp
        
        assert os.path.exists(fp), 'bad FiC fp:\n    %s'%fp
        
 
        
        #===================================================================
        # load
        #===================================================================
        log.info('loading FiC database from \n    %s'%os.path.basename(fp))
        
        
        vlay = self.vlay_load(fp, addSpatialIndex=False, logger=log, dropZ=True)
        
        #=======================================================================
        # check
        #=======================================================================
        
        
        assert vlay.wkbType() == 6
        
        assert date_fieldNm in [f.name() for f in vlay.fields()], 'missing date field'
        
        #get index
        findx = vlay.dataProvider().fieldNameIndex(date_fieldNm)
        
        dfield = vlay.fields().at(findx)
        
        assert dfield.type() == 16, 'got bad type on field \'%s\': %s'%(date_fieldNm, dfield.typeName())
        
        #=======================================================================
        # #store
        #=======================================================================

        self.fic_findx = findx #store the date field index
        self.date_fieldNm = date_fieldNm
        #self.fic_vdb = 
        self.mstore.addMapLayer(vlay)
        
        return vlay
        
    def get_fromDB(self, #extract fic polygons from date and aoi
                   vlay_db,
                   min_dt=datetime.datetime.strptime('2017-05-05', '%Y-%m-%d'),
                   max_dt=datetime.datetime.strptime('2017-05-07', '%Y-%m-%d'),
                   #
                   
                   logger=None,
                   ):
        
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger = self.logger
        
        
        log = logger.getChild('get_fromDB')
        
        tdelta = max_dt-min_dt
        
        
        
        log.info('selecting from %s to %s (%i days)'%(
            min_dt.strftime('%Y-%m-%d'),
            max_dt.strftime('%Y-%m-%d'),
            tdelta.days
            ))
        
        #=======================================================================
        # build temporal expression
        #=======================================================================
        """not sure this is working well for dense dates"""
        exp_str = '\"%s\" > to_datetime(\'%s\')'%(self.date_fieldNm, min_dt.strftime('%Y-%m-%d %H:%M:%S')) +\
                    ' AND ' +\
                    '\"%s\" < to_datetime(\'%s\')'%(self.date_fieldNm, max_dt.strftime('%Y-%m-%d %H:%M:%S'))
                    
        exp = QgsExpression(exp_str)
 
        
        assert not exp.hasParserError()
        assert not exp.hasEvalError()
        
        #=======================================================================
        # select using date expression
        #=======================================================================
        algo_nm = 'qgis:selectbyexpression'
        ins_d = { 'EXPRESSION' : exp_str,
                  'INPUT' : vlay_db, 
                 'METHOD' : 0, #new selection
                  }
        res_d = processing.run(algo_nm, ins_d, feedback=self.feedback, context=self.context)
        
        assert vlay_db.selectedFeatureCount() >0, 'failed to find any features within the date range\n    %s to %s'%(min_dt, max_dt)
        log.info("found %i features within the date range"%vlay_db.selectedFeatureCount())
        
        #=======================================================================
        # #save selected features
        #=======================================================================
        
        ofp = os.path.join(self.out_dir, 'FiC_%s_%s-%s_%s_raw.gpkg'%(self.name,

                    min_dt.strftime('%Y%m%d'), max_dt.strftime('%Y%m%d'),
                    datetime.datetime.now().strftime('%m%d%H')
                    ))
                
                
        self.saveselectedfeatures(vlay_db, output=ofp, logger=log)
        
        #=======================================================================
        # wrap
        #=======================================================================
        log.info('wrote fic_raw to %s'%ofp)
 
        return ofp, {'fic_date_cnt': vlay_db.selectedFeatureCount(),'min_dt':min_dt, 'max_dt':max_dt}
        
        
        
        
    def clean_fic(self,
                  fic_raw_fp, #filepath to raw fic polys (single obesrvation)
                    #can be a zip file (containing shp), shp, or gpkg
                  reproject=True,
                  aoi_vlay = None,
                  logger=None,
                  ):
        
        #=======================================================================
        # defaults
        #=======================================================================
        mstore=QgsMapLayerStore() #build a new store
        if aoi_vlay is None: aoi_vlay = self.aoi_vlay
        if logger is None: logger = self.logger
        
        log = logger.getChild('clean_fic')
        
        #=======================================================================
        # handle zips
        #=======================================================================
        if fic_raw_fp.endswith('.zip'):
            fic_raw_fp = self.get_from_zip(fic_raw_fp, logger=log)

        #=======================================================================
        # load it
        #=======================================================================
        vlay_raw = self.vlay_load(fic_raw_fp, reproj=reproject, logger=log, dropZ=True)
        mstore.addMapLayer(vlay_raw)
        #=======================================================================
        # spatial selection
        #=======================================================================
        vlay0 = self.selectbylocation(vlay_raw, aoi_vlay,method='new', result_type='layer')
        
        log.info('found %i feats within date range AND aoi'%(
            vlay0.dataProvider().featureCount()))
        
        mstore.addMapLayer(vlay0)
        #=======================================================================
        # pre- clean
        #=======================================================================
        #=======================================================================
        # vlay0 = processing.run('native:dropmzvalues', 
        #                {'INPUT':vlay00, 'OUTPUT':'TEMPORARY_OUTPUT', 'DROP_Z_VALUES':True},  
        #                feedback=self.feedback, context=self.context)['OUTPUT']
        #                
        # mstore.addMapLayer(vlay0)
        #=======================================================================
        
        #fix geo                                                     
        vlay1 = processing.run('native:fixgeometries', {'INPUT':vlay0, 'OUTPUT':'TEMPORARY_OUTPUT'},  
                               feedback=self.feedback)['OUTPUT']
        
        mstore.addMapLayer(vlay1)
        #=======================================================================
        # clip
        #=======================================================================
        vlay2 = processing.run('native:clip', { 'INPUT' : vlay1,
                                               'OUTPUT' : 'TEMPORARY_OUTPUT','OVERLAY' : self.aoi_vlay}, 
                       feedback=self.feedback)['OUTPUT']
        mstore.addMapLayer(vlay2)
        
        
        #=======================================================================
        # #setup outputs
        #=======================================================================
        ofn = ('%s_%i'%(vlay_raw.name(), vlay_raw.dataProvider().featureCount())).replace('_raw', '')
        ofp = os.path.join(self.out_dir,ofn+'.gpkg')
        if os.path.exists(ofp):
            log.warning('output file exists and overwwrite=%s\n    %s'%(self.overwrite, ofp))
            assert self.overwrite
            
            
        #=======================================================================
        # clean
        #=======================================================================
        self._clean(vlay2, mstore, ofp, log, reproject=reproject)
        
        
        #=======================================================================
        # wrap
        #=======================================================================
        meta_d = { 
                  'fic_cnt':vlay2.dataProvider().featureCount(),
 
                  }
        log.info('finished extracting clipped polys to \n    %s'%ofp)
        mstore.removeAllMapLayers()
        
        return ofp, meta_d
    
    def get_from_zip(self, #pull the shapefile out of standard zip files provided by FiC
                     zip_fp,
                     logger=None,
                     ):
        
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger = self.logger
        
        log = logger.getChild('get_from_zip')
        
        #=======================================================================
        # pull out files
        #=======================================================================
        temp_dir = os.path.join(self.temp_dir, os.path.basename(zip_fp).replace('.zip',''))
        with zipfile.ZipFile(zip_fp, 'r') as zip_ref:
            zip_ref.extractall(temp_dir)
    
        log.info('extracted %i files from zip to \n    %s'%(len(os.listdir(temp_dir)), temp_dir))
        
        #=======================================================================
        # select shapefile
        #=======================================================================
        
        fn_match = [e for e in os.listdir(temp_dir) if e.endswith('.shp') and e.startswith('FloodExtentPolygon')]

        assert len(fn_match)==1, 'failed to match.. got %i \n    %s'%(len(fn_match), fn_match)
        
        return os.path.join(temp_dir, fn_match[0])
    

    def _clean(self,
               vlay_raw,
               mstore,
               ofp,
               log,
               reproject=True,
               ):
        #=======================================================================
        # clean
        #=======================================================================
        log.debug('native:dropmzvalues')
        #drop MZ
        vlay = processing.run('native:dropmzvalues', 
                       {'INPUT':vlay_raw, 'OUTPUT':'TEMPORARY_OUTPUT', 'DROP_Z_VALUES':True},  
                       feedback=self.feedback, context=self.context)['OUTPUT']
                       
        mstore.addMapLayer(vlay)
        
        #fix geo                     
        log.debug('native:fixgeometries')                                
        vlay1 = processing.run('native:fixgeometries', {'INPUT':vlay, 'OUTPUT':'TEMPORARY_OUTPUT'},  
                               feedback=self.feedback)['OUTPUT']
        
        mstore.addMapLayer(vlay1)

        
        #=======================================================================
        # dissolve
        #=======================================================================
        
        """simpler to dissolve all runs.. even if theres only 1 feat"""

        #=======================================================================
        # if reproject:
        #     output = 'TEMPORARY_OUTPUT'
        # else:
        #     output = ofp
        #=======================================================================
        output = ofp
        #run
        log.debug('native:dissolve')
        res1 = processing.run('native:dissolve', { 'INPUT' : vlay1,'OUTPUT' : output,'FIELD' : []}, 
                   feedback=self.feedback)['OUTPUT']
        
        #=======================================================================
        # reproject
        #=======================================================================
        #=======================================================================
        # if reproject:
        #     log.debug('reproject')
        #     mstore.addMapLayer(res1)
        #         #reproejct
        #     res_d = self.reproject(res1, output=ofp, logger=log, selected_only=False)
        #     
        #     return ofp
        #=======================================================================
        return res1
            
        


    
    def merge_fics(self, #helper func to merge to FiC files
                   fp_l,
                  reproject=True,
 
                   logger=None,
                   ofp=None,
                   ):
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger = self.logger
 
        
        log = logger.getChild('merge_fics')
        mstore=QgsMapLayerStore() #build a new store
        
        log.info('on %i polys'%len(fp_l))
        
        if ofp is None: ofp = os.path.join(self.out_dir, 'FiC_merge_%ix.gpkg'%len(fp_l))
        
        #=======================================================================
        # check
        #=======================================================================
        for fp in fp_l: assert os.path.exists(fp), 'bad fp: %s'%fp
        #=======================================================================
        # merge
        #=======================================================================
        if len(fp_l)>1:
            log.debug('merging \n    %s'%fp_l)
            vlay1 = processing.run('native:mergevectorlayers', {'CRS':None,'LAYERS':fp_l, 'OUTPUT':'TEMPORARY_OUTPUT'},  
                                   feedback=self.feedback)['OUTPUT']
            
            mstore.addMapLayer(vlay1)
        else:
            vlay1 = fp_l[0]
            
        #=======================================================================
        # clean
        #=======================================================================
        self._clean(vlay1, mstore, ofp, log, reproject=reproject)
        
        log.info('finished on \n     %s'%ofp)
        return ofp
    
    def get_aoi(self, #build an AOi from a cleaned FiC Poly
                fp,
                clip_fp=None, #alternate polygon to clip with
                #buffer=100, holesize=10000, simplify=10,
                prec=100,
                logger=None,):
        """
        just using bounding boxes
        """
        
        #=======================================================================
        # defautls
        #=======================================================================
        if logger is None: logger=self.logger
        log = logger.getChild('get_aoi')
        ofp = os.path.join(self.out_dir, '%s_%s_aoi.gpkg'%(self.name, os.path.splitext(os.path.basename(fp))[0]))
        

        log.info('on %s'%fp)
        #=======================================================================
        # algos
        #=======================================================================
 
        if clip_fp is None:
            output=ofp
        else:
            output='TEMPORARY_OUTPUT'
        
        res1 = processing.run('native:boundingboxes', { 'INPUT' : fp,'OUTPUT' : output,
                                                       'ROUND_TO' : prec}, 
            feedback=self.feedback)['OUTPUT']
        
        
        #clip #just overlap with HRDEM
        if not clip_fp is None:
            res2 = processing.run('native:clip', { 'INPUT' : res1, 
                                      'OUTPUT' : ofp, 
                                      'OVERLAY' : clip_fp}, 
                                                    feedback=self.feedback)['OUTPUT']
        
        #=======================================================================
        # wrap
        #=======================================================================
        log.info('converted FiC to aoi \n    %s'%ofp)
        return ofp
        
        

   
        
                       
        


def run(
        aoi_fp=r'C:\LS\02_WORK\02_Mscripts\InsuranceCurves\04_CALC\CMM\aoi\aoi02_CMM_20210711.gpkg',
        crsID_default = 'EPSG:2950',
        name='CMM',
        crsid='EPSG:3979',
        
        #time window
       min_dt=datetime.datetime.strptime('2017-05-05', '%Y-%m-%d'),
       max_dt=datetime.datetime.strptime('2017-05-14', '%Y-%m-%d'),
        
        ): #extract FiC polygons for Montreal
    
    with ficSession(
        aoi_fp=aoi_fp,
        crsID_default = crsID_default, crs = QgsCoordinateReferenceSystem(crsid),
        name=name,
        ) as wrkr:
    
        wrkr.load_db()
        
        #3 polys
        wrkr.get_fromDB(min_dt=min_dt,max_dt=max_dt, reproject=True)
        #max poly
        
        out_dir = wrkr.out_dir
        
    return out_dir


def CMM2():
    
    return run(
        aoi_fp=r'C:\LS\02_WORK\02_Mscripts\InsuranceCurves\04_CALC\CMM\aoi\aoi02_CMM_20210711.gpkg',
        crsID_default = 'EPSG:2950',
        name='CMM',
        
       #flood week (3 captures)
       min_dt=datetime.datetime.strptime('2017-05-05', '%Y-%m-%d'),
       max_dt=datetime.datetime.strptime('2017-05-14', '%Y-%m-%d'),
        
        )



def Fred12():
    return run(
        name='Fred12',
        aoi_fp=r'C:\LS\02_WORK\02_Mscripts\InsuranceCurves\04_CALC\Fred\aoi\aoi12_fred_0722.gpkg',
        crsid='EPSG:3979',
        
        #FiC  period of interest
        min_dt=datetime.datetime.strptime('2018-04-02', '%Y-%m-%d'),
        max_dt=datetime.datetime.strptime('2018-06-04', '%Y-%m-%d'),
        
        )
if __name__ =="__main__": 
    
    start =  datetime.datetime.now()
    print('start at %s'%start)
    
    #CMM2()
    out_dir = Fred12()

    
    #===========================================================================
    # wrap
    #===========================================================================
    force_open_dir(out_dir)
    tdelta = datetime.datetime.now() - start
    print('finished in %s'%tdelta)
