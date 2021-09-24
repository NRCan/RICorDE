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
import os, datetime
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
                 fic_lib_fp = r'C:\LS\05_DATA\Canada\GOC\NRCan\FloodsInCanada\EGS_Flood_Product_Archive.gdb',

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
        
        assert dfield.type() == 16, 'got bad type on field \'%s\: %s'%(date_fieldNm, dfield.typeName())
        
        #=======================================================================
        # #store
        #=======================================================================

        self.fic_findx = findx #store the date field index
        self.date_fieldNm = date_fieldNm
        self.fic_vdb = vlay
        self.mstore.addMapLayer(vlay)
        
    def get_fic_polys(self, #extract fic polygons from date and aoi
                   min_dt=datetime.datetime.strptime('2017-05-05', '%Y-%m-%d'),
                   max_dt=datetime.datetime.strptime('2017-05-07', '%Y-%m-%d'),
                   reproject=False,
                   aoi_vlay = None,
                   logger=None,
                   ):
        
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger = self.logger
        if aoi_vlay is None: aoi_vlay = self.aoi_vlay
        
        log = logger.getChild('get_fic_polys')
        
        tdelta = max_dt-min_dt
        
        mstore=QgsMapLayerStore() #build a new store
        
        log.info('selecting from %s to %s (%i days)'%(
            min_dt.strftime('%Y-%m-%d'),
            max_dt.strftime('%Y-%m-%d'),
            tdelta.days
            ))
        
        #=======================================================================
        # build temporal expression
        #=======================================================================
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
                  'INPUT' : self.fic_vdb, 
                 'METHOD' : 0, #new selection
                  }
        res_d = processing.run(algo_nm, ins_d, feedback=self.feedback, context=self.context)
        
        assert self.fic_vdb.selectedFeatureCount() >0, 'failed to find any features within the date range'
        log.info("found %i features within the date range"%self.fic_vdb.selectedFeatureCount())
        
        
        #=======================================================================
        # spatial selection
        #=======================================================================
        vlay_raw = self.selectbylocation(self.fic_vdb, aoi_vlay,
                                          method='subselection', result_type='layer')
        
        log.info('found %i feats within date range AND aoi'%(
            vlay_raw.dataProvider().featureCount()))

        #=======================================================================
        # clean
        #=======================================================================
        vlay = processing.run('native:dropmzvalues', 
                       {'INPUT':vlay_raw, 'OUTPUT':'TEMPORARY_OUTPUT', 'DROP_Z_VALUES':True},  
                       feedback=self.feedback, context=self.context)['OUTPUT']
                       
        mstore.addMapLayer(vlay)
        #fix geo
        #=======================================================================
        # ofp1 = os.path.join(self.out_dir, 'FiC_%s_%ix_%s-%s.gpkg'%(self.name,
        #                          vlay.dataProvider().featureCount(),
        #                         min_dt.strftime('%Y%m%d'), max_dt.strftime('%Y%m%d')))
        #=======================================================================
                                      
                                                     
        vlay1 = processing.run('native:fixgeometries', {'INPUT':vlay, 'OUTPUT':'TEMPORARY_OUTPUT'},  
                               feedback=self.feedback)['OUTPUT']
        
        mstore.addMapLayer(vlay1)
        #=======================================================================
        # clip
        #=======================================================================
        #=======================================================================
        # ofp2 = os.path.join(self.out_dir, 'FiC_%s_%ix_%s-%s_aoi.gpkg'%(self.name,
        #                          vlay.dataProvider().featureCount(),
        #                         min_dt.strftime('%Y%m%d'), max_dt.strftime('%Y%m%d')))
        #=======================================================================
        
        vlay2 = processing.run('native:clip', { 'INPUT' : vlay1,'OUTPUT' : 'TEMPORARY_OUTPUT','OVERLAY' : self.aoi_vlay}, 
                       feedback=self.feedback)['OUTPUT']
        mstore.addMapLayer(vlay2)
        
        #=======================================================================
        # dissolve
        #=======================================================================
        
        """simpler to dissolve all runs.. even if theres only 1 feat"""
        
        
        #setup outputs
        ofp = os.path.join(self.out_dir, 'FiC_%s_%ix_%s-%s_%s.gpkg'%(self.name,
                     vlay.dataProvider().featureCount(),
                    min_dt.strftime('%Y%m%d'), max_dt.strftime('%Y%m%d'),
                    datetime.datetime.now().strftime('%m%d%H')
                    ))
        if os.path.exists(ofp):
            log.warning('output file exists and overwwrite=%s\n    %s'%(self.overwrite, ofp))
            assert self.overwrite
        
        
        if reproject:
            output = 'TEMPORARY_OUTPUT'
        else:
            output = ofp

        #run
        res1 = processing.run('native:dissolve', { 'INPUT' : vlay2,'OUTPUT' : output,'FIELD' : []}, 
                   feedback=self.feedback)['OUTPUT']
        
        #=======================================================================
        # reproject
        #=======================================================================
        if reproject:
            mstore.addMapLayer(res1)
                #reproejct
            res_d = self.reproject(res1, output=ofp, logger=log, selected_only=False)

        #=======================================================================
        # wrap
        #=======================================================================
        meta_d = {'fic_date_cnt': self.fic_vdb.selectedFeatureCount(),
                  'fic_cnt':vlay_raw.dataProvider().featureCount(),
                  'min_dt':min_dt, 'max_dt':max_dt
                  }
        log.info('finished extracting clipped polys to \n    %s'%ofp)
        mstore.removeAllMapLayers()
        
        return ofp, meta_d
    

        
                       
        


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
        wrkr.get_fic_polys(min_dt=min_dt,max_dt=max_dt, reproject=True)
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
