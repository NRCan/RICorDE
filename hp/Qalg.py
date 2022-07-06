'''Methods for QGIS processing'''

#===============================================================================
# # standard imports -----------------------------------------------------------
#===============================================================================
import time, sys, os

#===============================================================================
# QGJIS imports
#===============================================================================
import processing  
from qgis.core import *
from qgis.analysis import QgsNativeAlgorithms
import numpy as np

#===============================================================================
# custom  imports
#===============================================================================
from hp.exceptions import Error

#===============================================================================
# classes
#===============================================================================


class QAlgos(object):
    """
    common methods for applying algorthhims
    
    made a separate class just for organization
    """
    
    #projection operations
    """theres probably a nice way to get this from the users profile"""
    proj_d = {#{from:{to:operation}}
        'EPSG:4326':{
            'EPSG:3979':'+proj=pipeline +step +proj=unitconvert +xy_in=deg +xy_out=rad +step +proj=lcc +lat_0=49 +lon_0=-95 +lat_1=49 +lat_2=77 +x_0=0 +y_0=0 +ellps=GRS80',
            'EPSG:3857':'+proj=pipeline +step +proj=unitconvert +xy_in=deg +xy_out=rad +step +proj=webmerc +lat_0=0 +lon_0=0 +x_0=0 +y_0=0 +ellps=WGS84',
            'EPSG:2950':'+proj=pipeline +step +proj=unitconvert +xy_in=deg +xy_out=rad +step +proj=push +v_3 +step +proj=cart +ellps=WGS84 +step +inv +proj=helmert +x=-0.991 +y=1.9072 +z=0.5129 +rx=-0.0257899075194932 +ry=-0.0096500989602704 +rz=-0.0116599432323421 +s=0 +convention=coordinate_frame +step +inv +proj=cart +ellps=GRS80 +step +proj=pop +v_3 +step +proj=tmerc +lat_0=0 +lon_0=-73.5 +k=0.9999 +x_0=304800 +y_0=0 +ellps=GRS80',
            },
        'EPSG:3979':{
            'EPSG:3857':'+proj=pipeline +step +inv +proj=lcc +lat_0=49 +lon_0=-95 +lat_1=49 +lat_2=77 +x_0=0 +y_0=0 +ellps=GRS80 +step +proj=webmerc +lat_0=0 +lon_0=0 +x_0=0 +y_0=0 +ellps=WGS84',
            'EPSG:2950':'+proj=pipeline +step +inv +proj=lcc +lat_0=49 +lon_0=-95 +lat_1=49 +lat_2=77 +x_0=0 +y_0=0 +ellps=GRS80 +step +proj=tmerc +lat_0=0 +lon_0=-73.5 +k=0.9999 +x_0=304800 +y_0=0 +ellps=GRS80',
            'EPSG:3978':'+proj=noop'
            },
        'EPSG:3402':{
            'EPSG:3857':'+proj=pipeline +step +inv +proj=tmerc +lat_0=0 +lon_0=-115 +k=0.9992 +x_0=500000 +y_0=0 +ellps=GRS80 +step +proj=webmerc +lat_0=0 +lon_0=0 +x_0=0 +y_0=0 +ellps=WGS84'
            },
        'EPSG:3857':{
            'EPSG:2950':'+proj=pipeline +step +inv +proj=webmerc +lat_0=0 +lon_0=0 +x_0=0 +y_0=0 +ellps=WGS84 +step +proj=push +v_3 +step +proj=cart +ellps=WGS84 +step +inv +proj=helmert +x=-0.991 +y=1.9072 +z=0.5129 +rx=-0.0257899075194932 +ry=-0.0096500989602704 +rz=-0.0116599432323421 +s=0 +convention=coordinate_frame +step +inv +proj=cart +ellps=GRS80 +step +proj=pop +v_3 +step +proj=tmerc +lat_0=0 +lon_0=-73.5 +k=0.9999 +x_0=304800 +y_0=0 +ellps=GRS80',
            'EPSG:3979':'+proj=pipeline +step +inv +proj=webmerc +lat_0=0 +lon_0=0 +x_0=0 +y_0=0 +ellps=WGS84 +step +proj=lcc +lat_0=49 +lon_0=-95 +lat_1=49 +lat_2=77 +x_0=0 +y_0=0 +ellps=GRS80',
            },
        'EPSG:2950':{
            'EPSG:3979':'+proj=pipeline +step +inv +proj=tmerc +lat_0=0 +lon_0=-73.5 +k=0.9999 +x_0=304800 +y_0=0 +ellps=GRS80 +step +proj=lcc +lat_0=49 +lon_0=-95 +lat_1=49 +lat_2=77 +x_0=0 +y_0=0 +ellps=GRS80',
            },
        'EPSG:3978':{
            'EPSG:3979':'+proj=noop'
            }

        }
    
    #WARNING: some processing providers dont play well with high compression 
        #e.g. Whitebox doesnt recognize 'PREDICTOR' compression
    compress_d =  {
        'hiT':'COMPRESS=LERC_DEFLATE|PREDICTOR=2|ZLEVEL=9|MAX_Z_ERRROR=0.001', #nice for terrain
        'hi':'COMPRESS=DEFLATE|PREDICTOR=2|ZLEVEL=9',#Q default hi
        'med':'COMPRESS=LZW',
        'none':None        
        }

    #===========================================================================
    # input converters
    #===========================================================================
    #spatial relation predicates
    predicate_d = {'intersects':0,'contains':1,'equals':2,'touches':3,'overlaps':4,'within':5, 'crosses':6}
    
    #statistical summaries
    """2021-07-17: built from qgis:joinbylocationsummary"""
    summaries_d = {'count': 0, 'unique': 1, 'min': 2, 'max': 3, 'range': 4, 'sum': 5,
                    'mean': 6, 'median': 7, 'stddev': 8, 'minority': 9, 'majority': 10,
                    'q1': 11, 'q3': 12, 'iqr': 13, 'empty': 14, 'filled': 15, 'min_length': 16, 'max_length': 17, 'mean_length': 18}
    
    raster_dtype_d={'Float32':5}
    
    selectionMeth_d =  {'new':0, 'add':1, 'subselection':2, }
    
    def __init__(self, 
                 inher_d = {},
                 **kwargs):
        
        super().__init__(  #initilzie teh baseclassass
            inher_d = {**inher_d,
                **{'QAlgos':['context']}},
                        **kwargs) 
        
    def _init_algos(self,
                    context=None,
                    invalidGeometry=QgsFeatureRequest.GeometrySkipInvalid,
                        #GeometryNoCheck
                        #GeometryAbortOnInvalid
                        
                    ): #initiilize processing and add providers
        """
        crashing without raising an Exception
        """
    
        log = self.logger.getChild('_init_algos')
        
        if not isinstance(self.qap, QgsApplication):
            raise Error('qgis has not been properly initlized yet')
        
        #=======================================================================
        # build default co ntext
        #=======================================================================
        if context is None:

            context=QgsProcessingContext()
            context.setInvalidGeometryCheck(invalidGeometry)
            
        self.context=context
        
        #=======================================================================
        # init p[rocessing]
        #=======================================================================
        from processing.core.Processing import Processing
    
        Processing.initialize()  
    
        QgsApplication.processingRegistry().addProvider(QgsNativeAlgorithms())
        #QgsApplication.processingRegistry().addProvider(WbtProvider())
        
        #=======================================================================
        # #log all the agos
        # for alg in QgsApplication.processingRegistry().algorithms():
        #     log.debug("{}:{} --> {}".format(alg.provider().name(), alg.name(), alg.displayName()))
        #=======================================================================
        
        assert not self.feedback is None, 'instance needs a feedback method for algos to work'
        
        log.info('processing initilzied w/ feedback: \'%s\''%(type(self.feedback).__name__))

        return True
    
    #===========================================================================
    # NATIVE---------
    #===========================================================================
   
    def reproject(self,
                  vlay,
                  output='TEMPORARY_OUTPUT',
                  crsOut=None,
                  logger=None,
                  #layname=None,
                  selected_only=False,
                  ):
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        log=logger.getChild('reproject')
        #if layname is None: layname=vlay.name()
        if crsOut is None: crsOut=self.qproj.crs()
        
        #=======================================================================
        # get operation
        #=======================================================================
        inid = vlay.crs().authid()
        outid = crsOut.authid()
        
        assert inid in self.proj_d, 'missing requested source crs: %s'%inid
        
        assert outid in self.proj_d[inid], 'missing requested op: %s to %s'%(inid, outid)
        
        #selection handling
        if selected_only:
            """not working well"""
            input_obj = self._get_sel_obj(vlay)
        else:
            input_obj = vlay
 
        #=======================================================================
        # execute
        #=======================================================================
        res_d = processing.run('native:reprojectlayer', 
                           { 'INPUT' : input_obj,
                             'OPERATION' : self.proj_d[inid][outid], 
                             'OUTPUT' : output,
                             'TARGET_CRS' : crsOut},  
                           feedback=self.feedback, context=self.context)

        log.debug('finished  w/ %s'%res_d)
        return res_d['OUTPUT']
    
    def layerextent(self,
                    vlay,
                    output='TEMPORARY_OUTPUT',
                    precision=10, 
                    ):
        
        algo_nm = 'native:polygonfromlayerextent'
        
        ins_d = { 'INPUT' : vlay,'OUTPUT' : output, 'ROUND_TO' : precision }
        
        res_d = processing.run(algo_nm, ins_d, feedback=self.feedback)
        
        return res_d['OUTPUT']
    
    def selectbylocation(self, #select features (from main laye) by geoemtric relation with comp_vlay
                vlay, #vlay to select features from
                comp_vlay, #vlay to compare 
                
                result_type = 'select',
                
                method= 'new',  #Modify current selection by
                pred_l = ['intersect'],  #list of geometry predicate names
                
                selected_only = False, #selected features only on the comp_vlay
                
                #expectations
                allow_none = True,
                
                logger = None,):
        
        #=======================================================================
        # setups and defaults
        #=======================================================================
        if logger is None: logger=self.logger    
        algo_nm = 'native:selectbylocation'   
        log = logger.getChild('selectbylocation')
        
        #===========================================================================
        # #set parameter translation dictoinaries
        #===========================================================================
            
        pred_d = {
                'are within':6,
                'intersect':0,
                'overlap':5,
                  }
        
        #predicate (name to value)
        pred_l = [pred_d[pred_nm] for pred_nm in pred_l]
        
        if selected_only:
            intersect = self._get_sel_obj(comp_vlay)
        else:
            intersect = comp_vlay
    
        #=======================================================================
        # setup
        #=======================================================================
        ins_d = { 
            'INPUT' : vlay, 
            'INTERSECT' : intersect, 
            'METHOD' : self.selectionMeth_d[method], 
            'PREDICATE' : pred_l }
        
        log.debug('executing \'%s\' on \'%s\' with: \n     %s'
            %(algo_nm, vlay.name(), ins_d))
            
        #===========================================================================
        # #execute
        #===========================================================================
        _ = processing.run(algo_nm, ins_d,  feedback=self.feedback, context=self.context)
        
        #=======================================================================
        # check
        #=======================================================================
        fcnt = vlay.selectedFeatureCount()
        
        if fcnt == 0:
            msg = 'No features selected!'
            if allow_none:
                log.warning(msg)
            else:
                raise Error(msg)
            
        #=======================================================================
        # wrap
        #=======================================================================
        log.debug('selected %i (of %i) features from %s'
            %(vlay.selectedFeatureCount(),vlay.dataProvider().featureCount(), vlay.name()))
        
        return self._get_sel_res(vlay, result_type=result_type, logger=log, allow_none=allow_none)

    def dissolve(self, #select features (from main laye) by geoemtric relation with comp_vlay
                vlay, #vlay to select features from
                fields = [], 
                output='TEMPORARY_OUTPUT',
                selected_only = False, #selected features only on the comp_vlay

                logger = None,

                ):
        
        #=======================================================================
        # setups and defaults
        #=======================================================================
        if logger is None: logger=self.logger    
        algo_nm = 'native:dissolve'   
        log = logger.getChild('dissolve')
        
        if selected_only:
            alg_input = self._get_sel_obj(vlay)
        else:
            alg_input = vlay
    
        #=======================================================================
        # setup
        #=======================================================================
        ins_d = { 'FIELD' : fields, 
                 'INPUT' : alg_input,
                 'OUTPUT' : output,
                 }
        
        log.debug('executing \'%s\' on \'%s\' with: \n     %s'
            %(algo_nm, vlay, ins_d))
            
        #===========================================================================
        # #execute
        #===========================================================================
        res_d = processing.run(algo_nm, ins_d,  feedback=self.feedback, context=self.context)

        return res_d['OUTPUT']
    
    def fixgeo(self, 
                vlay, #vlay to select features from

                output='TEMPORARY_OUTPUT',
                selected_only = False, #selected features only on the comp_vlay

                logger = None,

                ):
        
        #=======================================================================
        # setups and defaults
        #=======================================================================
        if logger is None: logger=self.logger    
        algo_nm = 'native:fixgeometries'   
        log = logger.getChild('fixgeo')
        
        if selected_only:
            alg_input = self._get_sel_obj(vlay)
        else:
            alg_input = vlay
    
        #=======================================================================
        # setup
        #=======================================================================
        ins_d = { 
                'INPUT' : alg_input,
                 'OUTPUT' : output,
                 }
        
        log.debug('executing \'%s\' on \'%s\' with: \n     %s'
            %(algo_nm, vlay, ins_d))
            
        #===========================================================================
        # #execute
        #===========================================================================
        res_d = processing.run(algo_nm, ins_d,  feedback=self.feedback)

        return res_d['OUTPUT']
    
    def centroids(self, 
                vlay, #vlay to select features from

                output='TEMPORARY_OUTPUT',
                selected_only = False, #selected features only on the comp_vlay

                logger = None,

                ):
        
        #=======================================================================
        # setups and defaults
        #=======================================================================
        if logger is None: logger=self.logger    
        algo_nm = 'native:centroids'   
        log = logger.getChild('centroids')
        
        if selected_only:
            alg_input = self._get_sel_obj(vlay)
        else:
            alg_input = vlay
    
        #=======================================================================
        # setup
        #=======================================================================
        ins_d = { 'ALL_PARTS' : False, 
                 'INPUT' : alg_input,
                  'OUTPUT' : output}
        
        log.debug('executing \'%s\' on \'%s\' with: \n     %s'
            %(algo_nm, vlay.name(), ins_d))
            
        #===========================================================================
        # #execute
        #===========================================================================
        res_d = processing.run(algo_nm, ins_d,  feedback=self.feedback)

        return res_d['OUTPUT']
    
    def pointonsurf(self, 
                vlay, #vlay to select features from

                output='TEMPORARY_OUTPUT',
                selected_only = False, #selected features only on the comp_vlay
                logger=None,

                ):
        
        #=======================================================================
        # setups and defaults
        #=======================================================================
        if logger is None: logger=self.logger    
        algo_nm = 'native:pointonsurface'   
        log = logger.getChild('pointonsurf')
        
        if selected_only:
            alg_input = self._get_sel_obj(vlay)
        else:
            alg_input = vlay
    
        #=======================================================================
        # setup
        #=======================================================================
        ins_d = { 'ALL_PARTS' : False, 
                 'INPUT' : alg_input,
                  'OUTPUT' : output}
        
        log.debug('executing \'%s\' on \'%s\' with: \n     %s'
            %(algo_nm, vlay.name(), ins_d))
            
        #===========================================================================
        # #execute
        #===========================================================================
        res_d = processing.run(algo_nm, ins_d,  feedback=self.feedback, context=self.context)

        return res_d['OUTPUT']
    
    def rastersampling(self, 
                vlay, #vlay with sampling features
                rlay, #raster to sample
                pfx='sample_',

                output='TEMPORARY_OUTPUT',
                selected_only = False, #selected features only on the comp_vlay
                logger=None,

                ):
        
        #=======================================================================
        # setups and defaults
        #=======================================================================
        if logger is None: logger=self.logger    
        algo_nm = 'native:rastersampling'   
        log = logger.getChild('rastersampling')
        
        if selected_only:
            alg_input = self._get_sel_obj(vlay)
        else:
            alg_input = vlay
    
        #=======================================================================
        # setup
        #=======================================================================
        ins_d = { 'COLUMN_PREFIX' : pfx, 
                 'INPUT' : alg_input,
                 'OUTPUT' : output, 
                 'RASTERCOPY' : rlay }
        
        log.debug('executing \'%s\' on \'%s\' with: \n     %s'
            %(algo_nm, vlay, ins_d))
            
        #===========================================================================
        # #execute
        #===========================================================================
        res_d = processing.run(algo_nm, ins_d,  feedback=self.feedback, context=self.context)

        return res_d['OUTPUT']
    
    def saveselectedfeatures(self,#generate a memory layer from the current selection
                             vlay,
                             logger=None,
                             allow_none = False,
                             output='TEMPORARY_OUTPUT',
                             ): 
        """
        TODO: add these intermediate layers to the store
        """
        
        #===========================================================================
        # setups and defaults
        #===========================================================================
        if logger is None: 
            
            logger = self.logger
            """to avoid pushing lots of messages"""
            feedback = QgsProcessingFeedback()
        else:
            feedback=self.feedback
        log = logger.getChild('saveselectedfeatures')
        algo_nm = 'native:saveselectedfeatures'
              
        #=======================================================================
        # precheck
        #=======================================================================
        fcnt = vlay.selectedFeatureCount()
        if fcnt == 0:
            msg = 'No features selected!'
            if allow_none:
                log.warning(msg)
                return None
            else:
                raise Error(msg)
        
        log.debug('on \'%s\' with %i feats selected'%(
            vlay.name(), vlay.selectedFeatureCount()))
        #=======================================================================
        # # build inputs
        #=======================================================================
        ins_d = {'INPUT' : vlay,
                 'OUTPUT' : output}
        
        log.debug('\'native:saveselectedfeatures\'  with: \n   %s'
            %(ins_d))
        
        #execute
        res_d = processing.run(algo_nm, ins_d,  feedback=feedback)

        return res_d['OUTPUT']
    
    def fillnodata(self,
                rlay,
                fval = 0, #value to fill nodata w/
                output='TEMPORARY_OUTPUT',
 
                logger=None,

                ):
        
        #=======================================================================
        # setups and defaults
        #=======================================================================
        if logger is None: logger=self.logger    
        algo_nm = 'native:fillnodata'   
        log = logger.getChild('fillnodata')
    
        #=======================================================================
        # setup
        #=======================================================================
        ins_d = { 'BAND' : 1, 
                 'FILL_VALUE' : fval,
                  'INPUT' : rlay,
                  'OUTPUT' : output}
        
        log.debug('executing \'%s\' on \'%s\' with: \n     %s'
            %(algo_nm, rlay, ins_d))
            
        #===========================================================================
        # #execute
        #===========================================================================
        res_d = processing.run(algo_nm, ins_d,  feedback=self.feedback, context=self.context)
        
        """returns a filepathf or some reason"""
        return res_d['OUTPUT']
    
    def mergevectorlayers(self,
                vlay_l,
                crs=None,
                output='TEMPORARY_OUTPUT',
                logger=None,

                ):
        
        #=======================================================================
        # setups and defaults
        #=======================================================================
        if logger is None: logger=self.logger    
        algo_nm = 'native:mergevectorlayers'  
        log = logger.getChild('mergevectorlayers')
 
        if crs is None: crs = self.qproj.crs()
        
        #=======================================================================
        # precheck
        #=======================================================================
        assert isinstance(vlay_l, list)
        #=======================================================================
        # setup
        #=======================================================================
        ins_d = { 'CRS' : crs, 'LAYERS' :vlay_l,'OUTPUT' : output }
        
        log.debug('executing \'%s\' with: \n     %s'
            %(algo_nm,  ins_d))
            
        #===========================================================================
        # #execute
        #===========================================================================
        res_d = processing.run(algo_nm, ins_d,  feedback=self.feedback, context=self.context)

        return res_d['OUTPUT']
    
    def extractbyexpression(self,
                vlay,
                exp_str, #expression string to apply
                output='TEMPORARY_OUTPUT',
                fail_output = None, #how/if to output those failing the expression
                logger=None,

                ):
        
        #=======================================================================
        # setups and defaults
        #=======================================================================
        if logger is None: logger=self.logger    
        algo_nm = 'native:extractbyexpression'
        log = logger.getChild('extractbyexpression')
 
        #=======================================================================
        # setup
        #=======================================================================
        ins_d =    { 'EXPRESSION' : exp_str, 'INPUT' : vlay, 'OUTPUT' : output,
                    'FAIL_OUTPUT' : fail_output}
        
        log.debug('executing \'%s\' with: \n     %s'%(algo_nm,  ins_d))
 
        res_d = processing.run(algo_nm, ins_d,  feedback=self.feedback, context=self.context)

        return res_d #fail_output can be useful
    
    def multiparttosingleparts(self,
            vlay,
            output='TEMPORARY_OUTPUT',
            logger=None,
            ):
        
        #=======================================================================
        # setups and defaults
        #=======================================================================
        if logger is None: logger=self.logger    
        algo_nm = 'native:multiparttosingleparts'
        log = logger.getChild('multiparttosingleparts')
 
        ins_d =    {'INPUT' : vlay, 'OUTPUT' : output}
        
        log.debug('executing \'%s\' with: \n     %s'%(algo_nm,  ins_d))
 
        res_d = processing.run(algo_nm, ins_d,  feedback=self.feedback, context=self.context)

        return res_d['OUTPUT']
    
    def clip(self,
            vlay,
            vlay_top,
            output='TEMPORARY_OUTPUT',
            logger=None,
            ):
        
        #=======================================================================
        # setups and defaults
        #=======================================================================
        if logger is None: logger=self.logger    
        algo_nm = 'native:clip'
        log = logger.getChild('clip')
 
        ins_d =    {'INPUT' : vlay, 'OUTPUT' : output, 'OVERLAY':vlay_top}
        
        log.debug('executing \'%s\' with: \n     %s'%(algo_nm,  ins_d))
 
        res_d = processing.run(algo_nm, ins_d,  feedback=self.feedback, context=self.context)

        return res_d['OUTPUT']
    
    def pointsalonglines(self,
            vlay,
            spacing=100,
            output='TEMPORARY_OUTPUT',
            logger=None,
            ):
        
        assert isinstance(spacing, int)
        #=======================================================================
        # setups and defaults
        #=======================================================================
        if logger is None: logger=self.logger    
        algo_nm = 'native:pointsalonglines'
        log = logger.getChild('pointsalonglines')
 
        ins_d =    {'INPUT' : vlay, 'OUTPUT' : output,         
                    'DISTANCE' : spacing, 'END_OFFSET' : 0, 'START_OFFSET' : 0}
        
        log.debug('executing \'%s\' with: \n     %s'%(algo_nm,  ins_d))
 
        res_d = processing.run(algo_nm, ins_d,  feedback=self.feedback, context=self.context)

        return res_d['OUTPUT']
    
    def buffer(self,
            vlay,
            dist=10,
            dissolve=True,
            output='TEMPORARY_OUTPUT',
            logger=None,
            ):
        
        #=======================================================================
        # setups and defaults
        #=======================================================================
        assert isinstance(dist, int)
        if logger is None: logger=self.logger    
        algo_nm = 'native:buffer'
        log = logger.getChild('buffer')
 
        ins_d =    {'INPUT' : vlay, 'OUTPUT' : output,         
                    'DISSOLVE' : dissolve, 'DISTANCE' : dist, 'END_CAP_STYLE' : 0, 
                    'JOIN_STYLE' : 0, 'MITER_LIMIT' : 2, 'SEGMENTS' : 5 
                    }
        
        self.feedback.log('executing \'%s\' with: \n     %s'%(algo_nm,  ins_d))
 
        res_d = processing.run(algo_nm, ins_d,  feedback=self.feedback, context=self.context)

        return res_d['OUTPUT']
    
    def symmetricaldifference(self,
            vlay,
            vlay2,
            output='TEMPORARY_OUTPUT',
            logger=None,
            ):
        
        #=======================================================================
        # setups and defaults
        #=======================================================================
        if logger is None: logger=self.logger    
        algo_nm = 'native:symmetricaldifference'
        log = logger.getChild('symmetricaldifference')
 
        ins_d =    {'INPUT' : vlay, 'OUTPUT' : output,         
                    'OVERLAY' : vlay2, 'OVERLAY_FIELDS_PREFIX':'',
                    }
        
        log.debug('executing \'%s\' with: \n     %s'%(algo_nm,  ins_d))
 
        res_d = processing.run(algo_nm, ins_d,  feedback=self.feedback, context=self.context)

        return res_d['OUTPUT']
    
    def intersection(self,
            vlay,
            vlay2,
            output='TEMPORARY_OUTPUT',
            logger=None,
            ):
        
        #=======================================================================
        # setups and defaults
        #=======================================================================
        if logger is None: logger=self.logger    
        algo_nm = 'native:intersection'
        log = logger.getChild('intersection')
 
        ins_d =    {'INPUT' : vlay, 'OUTPUT' : output,         
                    'OVERLAY' : vlay2, 'OVERLAY_FIELDS_PREFIX':'',
                    'INPUT_FIELDS' : [],'OVERLAY_FIELDS' : []
                    }
        
        log.debug('executing \'%s\' with: \n     %s'%(algo_nm,  ins_d))
 
        res_d = processing.run(algo_nm, ins_d,  feedback=self.feedback, context=self.context)

        return res_d['OUTPUT']
    
    def creategrid(self,
            extent_layer, #layer with extents to use  to populate grid
            spacing=1000, #grid spacing
            crs=None,
            
            output='TEMPORARY_OUTPUT',
            logger=None,
            ):
        
        #=======================================================================
        # setups and defaults
        #=======================================================================
        if logger is None: logger=self.logger    
        algo_nm = 'native:creategrid'
        log = logger.getChild('creategrid')
        
        if crs is None: crs=self.qproj.crs()
        
        assert extent_layer.crs() == crs, 'crs mismatch'
 
        ins_d =    { 'CRS' :crs,
                     'EXTENT' : extent_layer.extent(),
                      'HOVERLAY' : 0, 'VOVERLAY' : 0,
                     'HSPACING' : spacing, 'VSPACING' : spacing,
                      'TYPE' : 2, #rectangle
                       'OUTPUT' : output}
        
        log.debug('executing \'%s\' with: \n     %s'%(algo_nm,  ins_d))
 
        res_d = processing.run(algo_nm, ins_d,  feedback=self.feedback, context=self.context)

        return res_d['OUTPUT']
 
    def renameField(self,
            vlay,
            old_fn, new_fn,
            
            output='TEMPORARY_OUTPUT',
            logger=None,
            ):
        
        #=======================================================================
        # setups and defaults
        #=======================================================================
        if logger is None: logger=self.logger    
        algo_nm = 'native:renametablefield'
        log = logger.getChild('renameField')
 
        ins_d = { 'FIELD' : old_fn, 'NEW_NAME' : new_fn,
                  'INPUT' : vlay, 'OUTPUT' : output}
        
        log.debug('executing \'%s\' with: \n     %s'%(algo_nm,  ins_d))
 
        res_d = processing.run(algo_nm, ins_d,  feedback=self.feedback, context=self.context)

        return res_d['OUTPUT']
    
    def deleteholes(self,
            vlay,
            hole_area=0, #0= delete all holes
            output='TEMPORARY_OUTPUT',
            logger=None,
            ):
        
        #=======================================================================
        # setups and defaults
        #=======================================================================
        if logger is None: logger=self.logger    
        algo_nm = 'native:deleteholes'
        log = logger.getChild('deleteholes')

        ins_d = { 'MIN_AREA':hole_area,
                  'INPUT' : vlay, 'OUTPUT' : output}
        
        log.debug('executing \'%s\' with: \n     %s'%(algo_nm,  ins_d))
 
        res_d = processing.run(algo_nm, ins_d,  feedback=self.feedback, context=self.context)
        
        return res_d['OUTPUT']
    
    def simplifygeometries(self,
            vlay,
            simp_dist=1,
            output='TEMPORARY_OUTPUT',
            logger=None,
            ):
        
        #=======================================================================
        # setups and defaults
        #=======================================================================
        if logger is None: logger=self.logger    
        algo_nm = 'native:simplifygeometries'
        log = logger.getChild('simplifygeometries')

        ins_d = { 'METHOD':0, #douglas pecker
                 'TOLERANCE':simp_dist,
                  'INPUT' : vlay, 'OUTPUT' : output}
        
        log.debug('executing \'%s\' with: \n     %s'%(algo_nm,  ins_d))
 
        res_d = processing.run(algo_nm, ins_d,  feedback=self.feedback, context=self.context)
        
        return res_d['OUTPUT']
    
    def rasterlayerstatistics(self,
            rlay,
 
            logger=None,feedback='none',
            allow_empty=False, 
            ):
        
        #=======================================================================
        # setups and defaults
        #=======================================================================
        if logger is None: logger=self.logger    
        algo_nm = 'native:rasterlayerstatistics'
        #
        
        if feedback =='none':
            feedback=None
        elif feedback is None: 
            feedback=self.feedback

        ins_d = { 'BAND' : 1, 
                 'INPUT' : rlay,
                  'OUTPUT_HTML_FILE' : 'TEMPORARY_OUTPUT' }
        
        #log.debug('executing \'%s\' with: \n     %s'%(algo_nm,  ins_d))
 
        res_d = processing.run(algo_nm, ins_d,  feedback=feedback, context=self.context)
        
        #check result
        if np.isnan(res_d['MEAN']):
            
            msg = 'layer has no cells with value: %s'%rlay
            
            if not allow_empty:
                raise Error(msg)
            logger.getChild('rasterlayerstatistics').error(msg)
      
        return res_d 
    
    def roundraster(self,
            rlay,
            prec=3,
            logger=None,
            output='TEMPORARY_OUTPUT',
            ):
        
        #=======================================================================
        # setups and defaults
        #=======================================================================
        if logger is None: logger=self.logger    
        algo_nm = 'native:roundrastervalues'
        #log = logger.getChild('simplifygeometries')
        assert isinstance(prec, int)
 
        feedback=self.feedback

        ins_d = { 'BAND' : 1, 'BASE_N' : 10, 'DECIMAL_PLACES' : prec,
                  'INPUT' :rlay, 'OUTPUT' : output,
                   'ROUNDING_DIRECTION' : 1, #round to nearest
                    }
        
        #log.debug('executing \'%s\' with: \n     %s'%(algo_nm,  ins_d))
 
        res_d = processing.run(algo_nm, ins_d,  feedback=feedback, context=self.context)
        
        return res_d['OUTPUT']
    
    def createconstantrasterlayer(self,
            extent_layer,
            burn_val=1, #value to burn
            resolution=None, #output resolution. None=take from extent_layer
            dtype='Float32',
            logger=None,
            output='TEMPORARY_OUTPUT',
            ):
        
        #=======================================================================
        # setups and defaults
        #=======================================================================
        if logger is None: logger=self.logger    
        algo_nm = 'native:createconstantrasterlayer'
        log = logger.getChild('createconstantrasterlayer')
        
        mstore=QgsMapLayerStore()
        
        if isinstance(extent_layer, str):
            extent_layer = self.rlay_load(extent_layer, logger=log)
            mstore.addMapLayer(extent_layer)
        
        if resolution is None:
            resolution = self.rlay_get_resolution(extent_layer)
            
        """because this algo is resolution based, can result in a row/column mismatch"""
        assert round(resolution, 2)==resolution, 'got overly precise resolution: %s'%str(resolution)
 
        feedback=self.feedback

        ins_d = { 'EXTENT' : extent_layer.extent(),
                  'NUMBER' : burn_val,
                   'OUTPUT' : output,
                   'OUTPUT_TYPE' : self.raster_dtype_d[dtype],
                    'PIXEL_SIZE' : resolution,
                  'TARGET_CRS' : extent_layer.crs() }
        
        log.debug('executing \'%s\' with: \n     %s'%(algo_nm,  ins_d))
 
        res_d = processing.run(algo_nm, ins_d,  feedback=feedback, context=self.context)
        
        assert self.rlay_check_match(extent_layer, res_d['OUTPUT'], logger=log), 'result failed to match'
        
        mstore.removeAllMapLayers()
        return res_d['OUTPUT']

    def pixelstopoints(self,
            rlay,
            fieldName='VALUE',
 
            logger=None,
            output='TEMPORARY_OUTPUT',
            ):
        
        #=======================================================================
        # setups and defaults
        #=======================================================================
        if logger is None: logger=self.logger    
        algo_nm = 'native:pixelstopoints'
        log = logger.getChild('pixelstopoints')
 
        feedback=self.feedback

        ins_d = { 'FIELD_NAME' : fieldName, 'INPUT_RASTER' : rlay,
                  'OUTPUT' : output, 'RASTER_BAND' : 1 }
        
        log.debug('executing \'%s\' with: \n     %s'%(algo_nm,  ins_d))
 
        res_d = processing.run(algo_nm, ins_d,  feedback=feedback, context=self.context)
 
        return res_d['OUTPUT']
    #===========================================================================
    # QGIS--------
    #===========================================================================
    
    def createspatialindex(self,
                     in_vlay,
                     logger = None,
                     ):

        #=======================================================================
        # presets
        #=======================================================================
        algo_nm = 'qgis:createspatialindex'
        if logger is None: logger=self.logger
        #log = logger.getChild('createspatialindex')

        #=======================================================================
        # assemble pars
        #=======================================================================
        #assemble pars
        ins_d = { 'INPUT' : in_vlay }
        
        #log.debug('executing \'%s\' with ins_d: \n    %s'%(algo_nm, ins_d))
        
        res_d = processing.run(algo_nm, ins_d, feedback=self.feedback)
        
        return
    
    def joinattributesbylocation(self, 
         vlay, #base layer
         jvlay, #layer to join
         jvlay_fnl=[], #join layer field name list
         predicate='intersects', 
         prefix='',
         method=0, #join type
             # 0: Create separate feature for each matching feature (one-to-many)
             #1: Take attributes of the first matching feature only (one-to-one)
             #2: Take attributes of the feature with largest overlap only (one-to-one)
        output='TEMPORARY_OUTPUT',
             
        logger=None,
                                 ):
        """
        also see canflood.hlpr.Q for more sophisticated version
        
        dropped all the data checks and warnings here
        """
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        log = logger.getChild('joinattributesbylocation')
        
        algo_nm = 'qgis:joinattributesbylocation'
        
        #=======================================================================
        # assemble parameters
        #=======================================================================
        assert predicate in self.predicate_d, 'unrecognized predicarte: %s' %predicate
        
        pars_d = { 'DISCARD_NONMATCHING' : False, 
                  'INPUT' : vlay, 
                  'JOIN' : jvlay, 
                  'JOIN_FIELDS' : jvlay_fnl, 
                  'METHOD' : method, 
                  'NON_MATCHING' : 'TEMPORARY_OUTPUT', 
                  'OUTPUT' : output, 
                  'PREDICATE' : [self.predicate_d[predicate]], #only accepting single predicate
                  'PREFIX' : prefix }

        #=======================================================================
        # execute
        #=======================================================================
        log.debug('%s w/ \n%s'%(algo_nm, pars_d))
        res_d = processing.run(algo_nm, pars_d, feedback=self.feedback)
        
        """just leaving the output as is
        #retriieve results
        if os.path.exists(output):
            res_vlay = self.vlay_load(output)
        else:
            res_vlay = res_d[output]
            
        assert isinstance(res_vlay, QgsVectorLayer)
        """
            
        result = res_d['OUTPUT']
        
        join_cnt  = res_d['JOINED_COUNT']
        
        vlay_nomatch = res_d['NON_MATCHING'] #Unjoinable features from first layer
        
        #=======================================================================
        # warp
        #=======================================================================
        ofcnt = vlay.dataProvider().featureCount()
        jfcnt = jvlay.dataProvider().featureCount()
        miss_cnt = ofcnt-join_cnt
        
        if not miss_cnt>=0:
            log.warning('got negative miss_cnt: %i'%miss_cnt)
            """this can happen when a base feature intersects multiple join features for method=0"""
        
        log.info('finished joining \'%s\' (%i feats) to \'%s\' (%i feats)\n    %i hits and %i misses'%(
            vlay.name(), ofcnt, jvlay.name(), jfcnt, join_cnt, miss_cnt))
        
        return result, miss_cnt
    
    def joinbylocationsummary(self,
            vlay, #layer to add stats to
             join_vlay, #layer to extract stats from
             jlay_fieldn_l, #list of field names to extract from the join_vlay
             selected_only=False, #limit to selected only on the main
             jvlay_selected_only = False, #only consider selected features on the join layer

             predicate_l = ['intersects'],#list of geometric serach predicates
             smry_l = ['sum'], #data summaries to apply
             discard_nomatch = False, #Discard records which could not be joined
                 #TRUE: two resulting layers have no features in common
                #FALSE: in layer retains all non matchers, out layer only has the non-matchers?
 
             output='TEMPORARY_OUTPUT',
             logger=None,
                                 
                     ):
        """
        WARNING: This ressets the fids
          """
        
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        log = logger.getChild('joinbylocationsummary')
        algo_nm = 'qgis:joinbylocationsummary'
 
        #=======================================================================
        # presets
        #=======================================================================
        if isinstance(jlay_fieldn_l, set):
            jlay_fieldn_l = list(jlay_fieldn_l)
            
        #convert predicate to code
        pred_code_l = [self.predicate_d[pred_name] for pred_name in predicate_l]
            
        #convert summaries to code
        sum_code_l = [self.summaries_d[smry_str] for smry_str in smry_l]
        
        #=======================================================================
        # prechecks
        #=======================================================================
        if not isinstance(jlay_fieldn_l, list):
            raise Error('expected a list')
        
        #check requested join fields
        fn_l = [f.name() for f in join_vlay.fields()]
        s = set(jlay_fieldn_l).difference(fn_l)
        assert len(s)==0, 'requested join fields not on layer: %s'%s
        
        #check crs
        assert join_vlay.crs().authid() == vlay.crs().authid()
                
        #=======================================================================
        # set selection
        #=======================================================================
        if selected_only:
            main_input = self._get_sel_obj(vlay)
        else:
            main_input=vlay

        if jvlay_selected_only:
            join_input = self._get_sel_obj(join_vlay)
        else:
            join_input = join_vlay

        #=======================================================================
        # #assemble pars
        #=======================================================================
        ins_d = { 'DISCARD_NONMATCHING' : discard_nomatch,
                  'INPUT' : main_input,
                   'JOIN' : join_input,
                   'JOIN_FIELDS' : jlay_fieldn_l,
                  'OUTPUT' : output, 
                  'PREDICATE' : pred_code_l, 
                  'SUMMARIES' : sum_code_l,
                   }
        
        log.debug('executing \'%s\' with ins_d: \n    %s'%(algo_nm, ins_d))
 
        res_d = processing.run(algo_nm, ins_d, feedback=self.feedback, context=self.context)
 
        return res_d
    
    def deletecolumn(self, #Drop Field(s)
                     vlay,
                     fields_l, #field names to drop
                     selected_only=False, #limit to selected only on the main
                     output='TEMPORARY_OUTPUT',
                     logger = None,
                     ):

        #=======================================================================
        # presets
        #=======================================================================
        algo_nm = 'qgis:deletecolumn'
        if logger is None: logger=self.logger
        log = logger.getChild('deletecolumn')
        
        assert isinstance(fields_l, list)
        #=======================================================================
        # assemble pars
        #=======================================================================
        if selected_only:
            main_input = self._get_sel_obj(vlay)
        else:
            main_input=vlay
        
        #assemble pars
        ins_d = { 'COLUMN' : fields_l, 
                 'INPUT' : main_input,
                    'OUTPUT' : output }
        
        log.debug('executing \'%s\' with ins_d: \n    %s'%(algo_nm, ins_d))
        
        res_d = processing.run(algo_nm, ins_d, feedback=self.feedback)
        
        return res_d['OUTPUT']

    def distancematrix(self, # table containing a distance matrix, with distances between all the points in a points layer.
                     vlay,
                     ncnt=4, #number of neighroubs to include
                     selected_only=False, #limit to selected only on the main
                     output='TEMPORARY_OUTPUT',
                     logger = None,
                     ):

        #=======================================================================
        # presets
        #=======================================================================
        algo_nm = 'qgis:distancematrix'
        if logger is None: logger=self.logger
        log = logger.getChild('distancematrix')
 
        #=======================================================================
        # assemble pars
        #=======================================================================
        if selected_only:
            main_input = self._get_sel_obj(vlay)
        else:
            main_input=vlay
        
        #assemble pars
        ins_d = { 'INPUT' : main_input,
                  'INPUT_FIELD' : 'fid', 
                  'MATRIX_TYPE' : 1, #standard distance matrix
                   'NEAREST_POINTS' : 8,
                   'OUTPUT' : 'TEMPORARY_OUTPUT',
                    'TARGET' : vlay,
                  'TARGET_FIELD' : 'fid' }
        
        #log.debug('executing \'%s\' with ins_d: \n    %s'%(algo_nm, ins_d))
        
        res_d = processing.run(algo_nm, ins_d, feedback=self.feedback)
        
        return res_d['OUTPUT']
    
    def idwinterpolation(self, # table containing a distance matrix, with distances between all the points in a points layer.
                     pts_vlay,
                     fieldn, #field name with data 
                     pixel_size,
                     distP=2, #number of neighroubs to include
 
                     extent_layer=None, #layer to pull raster extents from
                        #None: use pts_vlay
                    
                     selected_only=False, #limit to selected only on the main
                     output='TEMPORARY_OUTPUT',
                     logger = None,
                     ):

        #=======================================================================
        # presets
        #=======================================================================
        algo_nm = 'qgis:idwinterpolation'
        if logger is None: logger=self.logger
        log = logger.getChild('idwinterpolation')

        raise Error('cant figure out how to configure INTERPOLATION_DATA')
 
        #=======================================================================
        # assemble pars
        #=======================================================================
        if selected_only:
            main_input = self._get_sel_obj(pts_vlay)
        else:
            main_input=pts_vlay
            
        if extent_layer is None:
            extent_layer=pts_vlay
        
        #assemble pars
        ins_d = { 'DISTANCE_COEFFICIENT' : distP,
                  'EXTENT' : extent_layer.extent(),
                   'INTERPOLATION_DATA' : main_input,
                   'OUTPUT' : output, 
                  'PIXEL_SIZE' : pixel_size }
        
        #log.debug('executing \'%s\' with ins_d: \n    %s'%(algo_nm, ins_d))
        
        res_d = processing.run(algo_nm, ins_d, feedback=self.feedback)
        
        return res_d['OUTPUT']
    
    def addgeometry(self, # table containing a distance matrix, with distances between all the points in a points layer.
                     vlay,
 
                     output='TEMPORARY_OUTPUT',
                     logger = None,
                     ):

        #=======================================================================
        # presets
        #=======================================================================
        algo_nm = 'qgis:exportaddgeometrycolumns'
        if logger is None: logger=self.logger
        log = logger.getChild('addgeometry')
 
        #=======================================================================
        # assemble pars
        #=======================================================================

        #assemble pars
        ins_d = { 'CALC_METHOD' : 0, #use layer crs
                  'INPUT' : vlay, 
                 'OUTPUT' : output }
        
        #log.debug('executing \'%s\' with ins_d: \n    %s'%(algo_nm, ins_d))
        
        res_d = processing.run(algo_nm, ins_d, feedback=self.feedback)
        
        return res_d['OUTPUT']
    
    def selectbyattribute(self, # table containing a distance matrix, with distances between all the points in a points layer.
                     vlay,
                     fieldName,
                     value,
                     method='add',

                     logger = None,
                     ):

        #=======================================================================
        # presets
        #=======================================================================
        algo_nm = 'qgis:selectbyattribute'
        if logger is None: logger=self.logger
        log = logger.getChild('selectbyattribute')

        assert isinstance(vlay, QgsVectorLayer)
        #=======================================================================
        # assemble pars
        #=======================================================================

        #assemble pars
        ins_d = { 'FIELD' : fieldName, 
                    'INPUT' : vlay, 
                    'METHOD' : self.selectionMeth_d[method], 
                    'OPERATOR' : 0,#equals to
                     'VALUE' : str(value) }
        
        #log.debug('executing \'%s\' with ins_d: \n    %s'%(algo_nm, ins_d))
        
        res_d = processing.run(algo_nm, ins_d, feedback=self.feedback)
        
        return res_d['OUTPUT']
    
    #===========================================================================
    # GDAL---------
    #===========================================================================
    
    def cliprasterwithpolygon(self,
              rlay_raw,
              poly_vlay,
 
              output = 'TEMPORARY_OUTPUT',
              #result = 'layer', #type fo result to provide
                #layer: default, returns a raster layuer
                #fp: #returns the filepath result
              outResolution = None, #resultion for output. None = use input
              crsOut = None,
              options = [],
              dataType=0, # 0: Use Input Layer Data Type
                #6: Float32.
                #TODO: replace w/ raster_dtype_d
              logger = None,
                              ):
        """
        clipping a raster layer with a polygon mask using gdalwarp
        """
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: 
            
            logger = self.logger
            """to avoid pushing lots of messages"""
            feedback = QgsProcessingFeedback()
        else:
            feedback=self.feedback
        log = logger.getChild('cliprasterwithpolygon')
            
        algo_nm = 'gdal:cliprasterbymasklayer'

        #=======================================================================
        # precheck
        #=======================================================================
        assert isinstance(rlay_raw, QgsRasterLayer)
        assert isinstance(poly_vlay, QgsVectorLayer)
        assert 'Poly' in QgsWkbTypes().displayString(poly_vlay.wkbType())
         
        assert rlay_raw.crs() == poly_vlay.crs()
        
        #=======================================================================
        # cleanup outputs
        #=======================================================================
        if os.path.exists(output):
            assert self.overwrite
            os.remove(output) #gdal requires the file to be onge
            
        #=======================================================================
        # resolution
        #=======================================================================
        if not outResolution is None:
            assert isinstance(outResolution, int)
            setResolution = True
            
            log.debug('setting output resolution to %i'%outResolution)
        else:
            setResolution = False
            
        #=======================================================================
        # run algo        
        #=======================================================================
        
        ins_d = {   'ALPHA_BAND' : False,
                    'CROP_TO_CUTLINE' : True,
                    'DATA_TYPE' : dataType,
                    'EXTRA' : '',
                    'INPUT' : rlay_raw,
                    'KEEP_RESOLUTION' : True, 
                    'MASK' : poly_vlay,
                    'MULTITHREADING' : True,
                    'NODATA' : -9999,
                    'OPTIONS' : options,
                    'OUTPUT' : output,
                    'SET_RESOLUTION' : setResolution,
                    'SOURCE_CRS' : None,
                    'TARGET_CRS' : crsOut,
                    'X_RESOLUTION' : outResolution,
                    'Y_RESOLUTION' : outResolution,
                     }
        
        log.debug('executing \'%s\' with ins_d: \n    %s \n\n'%(algo_nm, ins_d))
        
        res_d = processing.run(algo_nm, ins_d, feedback=feedback)
        
        log.debug('finished w/ \n    %s'%res_d)
        
        if not os.path.exists(res_d['OUTPUT']):
            """failing intermittently"""
            raise Error('failed to get a result')
        
        #=======================================================================
        # get the result
        #=======================================================================
        return res_d
    
    def extrapNoData(self,
                     rlay,
                     dist, #maximum pixes to search for interpolation values
                     iterations=0,
                     output='TEMPORARY_OUTPUT',
                     options='',
                     logger=None,
                     ):
        if logger is None: logger=self.logger
        log = logger.getChild('extrapNoData')
        
        algo_nm = 'gdal:fillnodata'
        
        ins_d = { 'BAND' : 1, 
                 'DISTANCE' : dist, 'EXTRA' : '',
          'INPUT' : rlay,
           'ITERATIONS' : iterations, 
           'MASK_LAYER' : None, 'NO_MASK' : False,
          'OPTIONS' : options, 
          'OUTPUT' : output }
 
        log.info('dist=%.2f on %s'%(dist, rlay))
        return processing.run(algo_nm, ins_d, feedback=self.feedback, context=self.context)
        
    def warpreproject(self, #repojrect a raster
                              rlay_raw,
                              
                              crsOut = None, #crs to re-project to
                              crsIn=None, #needed for some rasters
                              resolution=None,
                              compress = None,
                              nodata_val=-9999,
                              resampling='Nearest neighbour', #resampling method
                              extents=None,
 
                              output = 'TEMPORARY_OUTPUT', #always writes to file
                              logger = None,
                              ):
        """
                        bbox_str = '%.3f, %.3f, %.3f, %.3f [%s]'%(
                    rect.xMinimum(), rect.xMaximum(), rect.yMinimum(), rect.yMaximum(),
                    self.aoi_vlay.crs().authid())
        """
        
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger = self.logger
        log = logger.getChild('warpreproject')
        if compress is None: compress=self.compress
        if output is None: output='TEMPORARY_OUTPUT'
        
        resamp_d = {0:'Nearest neighbour',
                    1:'Bilinear',
                    2:'Cubic',
                    3:'Cubic spline',
                    4:'Lanczos windowed sinc',
                    5:'Average',
                    6:'Mode',
                    7:'Maximum',
                    8:'Minimum',
                    9:'Median',
                    10:'First quartile',
                    11:'Third quartile'}
            
        algo_nm = 'gdal:warpreproject'
            
        #=======================================================================
        # if crsOut is None: 
        #     crsOut = self.crs #just take the project's
        #=======================================================================
        #=======================================================================
        # precheck
        #=======================================================================
        """the algo accepts 'None'... but not sure why we'd want to do this"""
        if isinstance(rlay_raw, str):
            assert os.path.exists(rlay_raw), 'requested file does not exist: \n    %s'%rlay_raw
            assert QgsRasterLayer.isValidRasterFileName(rlay_raw),  \
                'requested file is not a valid raster file type: %s'%rlay_raw
        else:
            
            #assert isinstance(crsOut, QgsCoordinateReferenceSystem), 'bad crs type'
            assert isinstance(rlay_raw, QgsRasterLayer)
     
            #assert rlay_raw.crs() != crsOut, 'layer already on this CRS!'
            
        if os.path.exists(output):
            os.remove(output)
            
        if not resolution is None:
            assert isinstance(resolution, int), 'got bad type in resolution: %s'%type(resolution)
        #=======================================================================
        # run algo        
        #=======================================================================
        opts = self.compress_d[compress]
        if opts is None: opts = ''
        
        ins_d =  {
             'DATA_TYPE' : 0,#use input
             'EXTRA' : '',
             'INPUT' : rlay_raw,
             'MULTITHREADING' : False,
             'NODATA' : nodata_val,
             'OPTIONS' : opts,
             'OUTPUT' : output,
             'RESAMPLING' : {v:k for k,v in resamp_d.items()}[resampling],
             'SOURCE_CRS' : crsIn,
             'TARGET_CRS' : crsOut,
             'TARGET_EXTENT' : extents,
             'TARGET_EXTENT_CRS' : None,
             'TARGET_RESOLUTION' : resolution,
          }
 
        log.debug('executing \'%s\' with ins_d: \n    %s \n\n'%(algo_nm, ins_d))
 
        res_d = processing.run(algo_nm, ins_d, feedback=self.feedback)
        
        if not os.path.exists(res_d['OUTPUT']):
            """failing intermittently"""
            raise Error('failed to get a result')
        
        log.debug('finished w/ %s'%res_d)
          
        return res_d['OUTPUT']
    
    def mergeraster(self, #merge a set of raster layers
                  rlays_l,
                  crsOut = None, #crs to re-project to
                  layname = None,
                  compression = 'hiT',
                  output = 'TEMPORARY_OUTPUT',
                  logger = None,
                              ):

        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger = self.logger
        log = logger.getChild('mergeraster')
        
        if layname is None:
            layname = 'merge'
            
        algo_nm = 'gdal:merge'
            
        if crsOut is None: crsOut = self.qproj.crs() #just take the project's
        #=======================================================================
        # precheck
        #=======================================================================
        """the algo accepts 'None'... but not sure why we'd want to do this"""
        assert isinstance(crsOut, QgsCoordinateReferenceSystem), 'bad crs type'
        assert isinstance(rlays_l, list)
        assert (output == 'TEMPORARY_OUTPUT') or (output.endswith('.tif')) 
 
        first, bc = True, None
        for r in rlays_l: 
            if not os.path.exists(r):
                assert isinstance(r, QgsRasterLayer)
                assert r.crs() != crsOut, 'layer already on this CRS!'
                
                if first:
                    first = False
                else:
                    assert r.bandCount() == bc
                bc = r.bandCount()
        
        #=======================================================================
        # execute
        #=======================================================================
        ins_d = { 'DATA_TYPE' : 5, 
                 'EXTRA' : '',
                  'INPUT' : rlays_l, 
                  #'NODATA_INPUT' : -9999, 
                  'NODATA_OUTPUT' : -9999,
                   'OPTIONS' : self.compress_d[compression],
                  'OUTPUT' : output, 
                  'PCT' : False, 'SEPARATE' : False }
        
        log.debug('executing \'%s\' with ins_d: \n    %s \n\n'%(algo_nm, ins_d))
        
        res_d = processing.run(algo_nm, ins_d, feedback=self.feedback, context=self.context)
        
        #=======================================================================
        # wrap
        #=======================================================================
        log.debug('finished w/ \n    %s'%res_d)
        
        if not os.path.exists(res_d['OUTPUT']):
            """failing intermittently"""
            raise Error('failed to get a result')
        
        if output == 'TEMPORARY_OUTPUT':
            res_rlay = QgsRasterLayer(res_d['OUTPUT'], layname)

            assert isinstance(res_rlay, QgsRasterLayer), 'got bad type: %s'%type(res_rlay)
            assert res_rlay.isValid()
            assert bc==res_rlay.bandCount(), 'band count mismatch'
       
            res_rlay.setName(layname) #reset the name
               
            log.debug('finished w/ %s'%res_rlay.name())
        else:
            res_rlay = res_d['OUTPUT']
          
        return res_rlay
    
    def rasterize_value(self, #build a rastser with a fixed value from a polygon
                bval, #fixed value to burn,
                poly_vlay, #polygon layer with geometry
                resolution=10,
                output = 'TEMPORARY_OUTPUT',
                result = 'layer', #type fo result to provide
                #layer: default, returns a raster layuer
                #fp: #returns the filepath result
                layname=None,
                logger=None,
                  ):
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        log = logger.getChild('rasterize')
        if layname is None: layname = '%s_%.2f'%(poly_vlay.name(), bval)
        algo_nm = 'gdal:rasterize'
        
        """
        extents =  QgsRectangle(-127.6, 44.1, -106.5, 54.1)
        """
        #=======================================================================
        # get extents
        #=======================================================================
        rect = poly_vlay.extent()
        
        extent = '%s,%s,%s,%s'%(rect.xMinimum(), rect.xMaximum(), rect.yMinimum(), rect.yMaximum())+ \
                ' [%s]'%poly_vlay.crs().authid()
        
        #=======================================================================
        # build pars
        #=======================================================================
        pars_d = { 'BURN' : bval, #fixed value to burn
                  'EXTENT' : extent,
                   #'EXTENT' : '1221974.375000000,1224554.125000000,466981.406300000,469354.031300000 [EPSG:3005]',
                    'EXTRA' : '', 'FIELD' : '', 
                    'HEIGHT' : resolution, 
                    'WIDTH' : resolution, 
                    'UNITS' : 1,  #Georeferenced units 
                    'INIT' : None, #Pre-initialize the output image with value
                     
                      'INVERT' : False,
                   'NODATA' : -9999, 'DATA_TYPE' : 5,'OPTIONS' : '',
                   'INPUT' : poly_vlay, 'OUTPUT' : output,
                     
                      }
        
        log.debug('%s w/ \n    %s'%(algo_nm, pars_d))
        res_d = processing.run(algo_nm, pars_d, feedback=self.feedback)
        
        #laod teh rlay
    
        return self._get_rlay_res(res_d, result, layname=layname)
    
    def rastercalculatorGDAL(self, #build a rastser with a fixed value from a polygon
                rlayA, #fixed value to burn,
 
                formula,
                compression = 'none',
                output = 'TEMPORARY_OUTPUT',
                dtype='Float32', #Float32. TODO: replace w/ raster_dtype_d
                logger=None,
                  ):
        """
        TODO: more sophisticated handling of inputs
            take a list of rasters from theu ser
            assign each one in t he list to the letters
            assign thecorresponding band values (assume 1 if not sdpecified)
        """
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        log = logger.getChild('rastercalculator')

        algo_nm = 'gdal:rastercalculator'
        
        ins_d = { 'BAND_A' : 1, 'BAND_B' : None, 'BAND_C' : None,
                  'BAND_D' : None, 'BAND_E' : None, 'BAND_F' : None,
                  'EXTRA' : '',
                   'FORMULA' : formula,
                   'INPUT_A' : rlayA,
                    'INPUT_B' : None, 'INPUT_C' : None, 'INPUT_D' : None, 'INPUT_E' : None,'INPUT_F' : None,
                     'NO_DATA' : -9999, 
                  'OPTIONS' : self.compress_d[compression], 
                  'OUTPUT' : output, 
                  'RTYPE' : self.raster_dtype_d[dtype], #int32
                   }
        
        log.debug('executing \'%s\' with ins_d: \n    %s \n\n'%(algo_nm, ins_d))
        
        res_d = processing.run(algo_nm, ins_d, feedback=self.feedback, context=self.context)
        
        return res_d
    
    def polygonizeGDAL(self,
                       rlay,
                       output = 'TEMPORARY_OUTPUT',
                       logger=None,
                       ):
 
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        log = logger.getChild('polygonize')

        algo_nm = 'gdal:polygonize'
        
        ins_d = { 'BAND' : 1, 'EIGHT_CONNECTEDNESS' : False, 
                 'EXTRA' : '', 'FIELD' : 'DN', 
                 'INPUT' : rlay, 
                 'OUTPUT' : output }
        
        log.debug('executing \'%s\' with ins_d: \n    %s \n\n'%(algo_nm, ins_d))
        
        res_d = processing.run(algo_nm, ins_d, feedback=self.feedback, context=self.context)
        
        return res_d['OUTPUT']
    
    def convertformat(self,
                      input,
                      output = 'TEMPORARY_OUTPUT', #defaults to a gpkg
                      ):
        algo_nm = 'gdal:convertformat'
        ins_d = { 'INPUT' : input, 'OPTIONS' : '', 'OUTPUT' : output}
        
        res_d = processing.run(algo_nm, ins_d, feedback=self.feedback, context=self.context)
        
        return res_d['OUTPUT']
    
    #===========================================================================
    # GRASS--------
    #===========================================================================
    def vSurfIdw(self,
                       pts_vlay,
                       fieldName,
                       distP=2, #distance coefficienct
                       pts_cnt = 50, #number of points to include in seawrches
                       cell_size=10,
                       extents=None,
                       output = 'TEMPORARY_OUTPUT',
                       logger=None,
                       ):
        """NOTE: this isnt matching the specified cellsize exactly
        
        need to use gdalwarp"""
 
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        log = logger.getChild('vSurfIdw')

        algo_nm = 'grass7:v.surf.idw'
        
        if isinstance(pts_vlay, QgsVectorLayer):
            assert fieldName in [f.name() for f  in pts_vlay.fields()]
        #=======================================================================
        # pars
        #=======================================================================
        if extents is None:
            extents = pts_vlay.extent()
        
        ins_d = { '-n' : False, 
                 'GRASS_MIN_AREA_PARAMETER' : 0.0001,
                  'GRASS_RASTER_FORMAT_META' : '', 'GRASS_RASTER_FORMAT_OPT' : '',
                   'GRASS_REGION_CELLSIZE_PARAMETER' : cell_size,
                    'GRASS_REGION_PARAMETER' : extents,
                     'GRASS_SNAP_TOLERANCE_PARAMETER' : -1,
                      'column' : fieldName,
                       'input' : pts_vlay,
                  'npoints' : pts_cnt, 'power' :distP, 
                   'output' : output,}
        
        log.debug('executing \'%s\' with ins_d: \n    %s \n\n'%(algo_nm, ins_d))
        
        res_d = processing.run(algo_nm, ins_d, feedback=self.feedback, context=self.context)
        
        assert os.path.exists(res_d['output'])
        
        return res_d['output']
    
    def rNeighbors(self,
                       rlay,
                       neighborhood_size=3, 
                       circular_neighborhood=True,
                       cell_size=0, #0= take in put?
                       method='average',
                       mask=None,
                       output = 'TEMPORARY_OUTPUT',
                       logger=None,
                       feedback=None,
                       ):
        
        """
        only temporary output seems to be working
        """
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        log = logger.getChild('rNeighbors')

        algo_nm = 'grass7:r.neighbors'
        
        if feedback =='none':
            feedback=None
        elif feedback is None: 
            feedback=self.feedback
 
        #=======================================================================
        # pars
        #=======================================================================
        
        ins_d = { '-a' : True, #dont align with input 
                 '-c' : circular_neighborhood,
                  'GRASS_RASTER_FORMAT_META' : '', 'GRASS_RASTER_FORMAT_OPT' : '',
                   'GRASS_REGION_CELLSIZE_PARAMETER' : cell_size, 
                   'GRASS_REGION_PARAMETER' : None, 
                   'gauss' : None,
                    'input' : rlay,
                     'method' : {'average':0, 'median':1,'range':5}[method], #average
                     'output' : output, 
                     'quantile' : '', 
                     'selection' : mask, 
                     'size' : neighborhood_size,
                      'weight' : '' }
        
        log.debug('executing \'%s\' with ins_d: \n    %s \n\n'%(algo_nm, ins_d))
        
        """this prints some things to std.out regardless"""
        res_d = processing.run(algo_nm, ins_d, feedback=feedback, context=self.context)
        
        return res_d['output']
    
    def rGrowDistance(self,
                       rlay,
 
                       output = 'TEMPORARY_OUTPUT',
                       logger=None,
                       ):
 
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        log = logger.getChild('rGrowDistance')

        algo_nm = 'grass7:r.grow.distance'
 
        #=======================================================================
        # pars
        #=======================================================================
        
        ins_d = { '-' : False, '-m' : False, 'GRASS_RASTER_FORMAT_META' : '', 'GRASS_RASTER_FORMAT_OPT' : '', 
                 'GRASS_REGION_CELLSIZE_PARAMETER' : 0, #inherit?
                  'GRASS_REGION_PARAMETER' : None,
                  'distance' : 'TEMPORARY_OUTPUT',
                   'input' : rlay, 
                 'metric' : 0, #euclidan
                 'value' : output }
        
        log.debug('executing \'%s\' with ins_d: \n    %s \n\n'%(algo_nm, ins_d))
        
        res_d = processing.run(algo_nm, ins_d, feedback=self.feedback, context=self.context)
        
        return res_d

    def rBuffer(self, #buffer a raster
                       rlay,
                       dist=10,
                       output = 'TEMPORARY_OUTPUT',
                       logger=None,
                       ):
        """
        for buffered mask layers... the buffered cells have a value of 2
        WARNING: this changes the extents
        """
 
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        log = logger.getChild('rBuffer')

        algo_nm = 'grass7:r.buffer'
 
        #=======================================================================
        # pars
        #=======================================================================
        
        ins_d = { '-z' : False, #ignore zeros instead of nulls
                  'GRASS_RASTER_FORMAT_META' : '', 'GRASS_RASTER_FORMAT_OPT' : '', 'GRASS_REGION_CELLSIZE_PARAMETER' : 0, 'GRASS_REGION_PARAMETER' : None, 
                  'distances' : str(int(dist)), 
                  'input' : rlay, 
                 'output' : output, 
                 'units' : 0, #meteres
                  }
        
        log.debug('executing \'%s\' with ins_d: \n    %s \n\n'%(algo_nm, ins_d))
        
        res_d = processing.run(algo_nm, ins_d, feedback=self.feedback, context=self.context)
        
        return res_d['output']

    #===========================================================================
    # WHITEBOX------
    #===========================================================================
    def BreachDepressionsLeastCost(self,
                                   rlay,
                                   dist=100,
                                   output='TEMPORARY_OUTPUT',
                                   ):
        raise Error('cant get the whitebox provider to work')
        
        ins_d = { 'dem' : rlay,
                  'dist' : dist, 
                  'fill' : True, 'flat_increment' : None, 'max_cost' : None, 
                  'min_dist' : True,
                   'output' : output }
        
        algo_nm='wbt:BreachDepressionsLeastCost'
        
        res_d = processing.run(algo_nm, ins_d, feedback=self.feedback)
        
        return res_d
    #===========================================================================
    # helpers-------
    #===========================================================================
    
    def _get_sel_obj(self, vlay): #get the processing object for algos with selections
        
        log = self.logger.getChild('_get_sel_obj')
        
        assert isinstance(vlay, QgsVectorLayer)
        if vlay.selectedFeatureCount() == 0:
            raise Error('Nothing selected on \'%s\'. exepects some pre selection'%(vlay.name()))
        
        #=======================================================================
        # """consider moving this elsewhere"""
        # #handle project layer store
        # if QgsProject.instance().mapLayer(vlay.id()) is None:
        #     #layer not on project yet. add it
        #     if QgsProject.instance().addMapLayer(vlay, False) is None:
        #         raise Error('failed to add map layer \'%s\''%vlay.name())
        #=======================================================================
        
        #handle project layer store
        if self.qproj.mapLayer(vlay.id()) is None:
            #layer not on project yet. add it
            if self.qproj.addMapLayer(vlay, False) is None:
                raise Error('failed to add map layer \'%s\''%vlay.name())
       
        log.debug('based on %i selected features from \'%s\''%(len(vlay.selectedFeatureIds()), vlay.name()))
        
        return QgsProcessingFeatureSourceDefinition(source=vlay.id(), 
                                                    selectedFeaturesOnly=True, 
                                                    featureLimit=-1, 
                                                    geometryCheck=QgsFeatureRequest.GeometryAbortOnInvalid)
 
        #return QgsProcessingFeatureSourceDefinition(vlay.id(), True)
    
    def _get_sel_res(self, #handler for returning selection like results
                        vlay, #result layer (with selection on it
                         result_type='select',
                         
                         #expectiions
                         allow_none = False,
                         logger=None
                         ):
        
        #=======================================================================
        # setup
        #=======================================================================
        if logger is None: logger = self.logger
        log = logger.getChild('_get_sel_res')
        #=======================================================================
        # precheck
        #=======================================================================
        if vlay.selectedFeatureCount() == 0:
            if not allow_none:
                raise Error('nothing selected')
            
            return None
        
        #log.debug('user specified \'%s\' for result_type'%result_type)
        #=======================================================================
        # by handles
        #=======================================================================
        if result_type == 'select':
            #log.debug('user specified \'select\', doing nothing with %i selected'%vlay.selectedFeatureCount())
            
            result = None
            
        elif result_type == 'fids':
            
            result = vlay.selectedFeatureIds() #get teh selected feature ids
            
        elif result_type == 'feats':
            
            result =  {feat.id(): feat for feat in vlay.getSelectedFeatures()}
            
        elif result_type == 'layer':
            
            result = self.saveselectedfeatures(vlay, logger=log)
            
        else: 
            raise Error('unexpected result_type kwarg')
            
        return result
    
    def _get_rlay_res(self, res_d, result, layname=None):
        
        if result == 'layer':
            res_rlay = QgsRasterLayer(res_d['OUTPUT'], layname)
    
            #=======================================================================
            # #post check
            #=======================================================================
            assert isinstance(res_rlay, QgsRasterLayer), 'got bad type: %s'%type(res_rlay)
            assert res_rlay.isValid()
       
            res_rlay.setName(layname) #reset the name
          
            return res_rlay
        elif result == 'fp':
            return res_d['OUTPUT']
        else:
            raise Error('unrecognzied result kwarg: %s'%result)
    
