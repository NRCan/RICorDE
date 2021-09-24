'''
Created on Oct. 8, 2020

@author: cefect
'''



#===============================================================================
# # standard imports -----------------------------------------------------------
#===============================================================================
import time, sys, os, logging, datetime, inspect, gc, shutil

import numpy as np
import pandas as pd

#===============================================================================
# import QGIS librarires
#===============================================================================
from qgis.core import *
from qgis.gui import QgisInterface
from hp.gdal import get_nodata_val

from PyQt5.QtCore import QVariant, QMetaType 
import processing  


from qgis.analysis import QgsNativeAlgorithms, QgsRasterCalculatorEntry, QgsRasterCalculator



#whitebox
#from processing_wbt.wbtprovider import WbtProvider 


"""throws depceciationWarning"""


#===============================================================================
# custom imports
#===============================================================================

from hp.exceptions import Error

from hp.oop import Basic
from hp.dirz import get_valid_filename
from hp.Qalg import QAlgos
from hp.logr import get_new_file_logger

from hp.gdal import rlay_to_array
#===============================================================================
# logging
#===============================================================================
mod_logger = logging.getLogger(__name__)


#==============================================================================
# globals
#==============================================================================
fieldn_max_d = {'SpatiaLite':50, 'ESRI Shapefile':10, 'Memory storage':50, 'GPKG':50}

npc_pytype_d = {'?':bool,
                'b':int,
                'd':float,
                'e':float,
                'f':float,
                'q':int,
                'h':int,
                'l':int,
                'i':int,
                'g':float,
                'U':str,
                'B':int,
                'L':int,
                'Q':int,
                'H':int,
                'I':int, 
                'O':str, #this is the catchall 'object'
                }

type_qvar_py_d = {10:str, 2:int, 135:float, 6:float, 4:int, 1:bool, 16:datetime.datetime, 12:str} #QVariant.types to pythonic types

#parameters for lots of statistic algos
stat_pars_d = {'First': 0, 'Last': 1, 'Count': 2, 'Sum': 3, 'Mean': 4, 'Median': 5,
                'St dev (pop)': 6, 'Minimum': 7, 'Maximum': 8, 'Range': 9, 'Minority': 10,
                 'Majority': 11, 'Variety': 12, 'Q1': 13, 'Q3': 14, 'IQR': 15}



#===============================================================================
# workers
#===============================================================================

class Qproj(QAlgos, Basic):
    """
    common methods for Qgis projects
    """
    
    crsID_default = 'EPSG:4326'
    
    driverName = 'SpatiaLite' #default data creation driver type
    out_dName = driverName #default output driver/file type
    
    
    
    def __init__(self, 
                 feedback=None, 
                 crs = None,
                 crsID_default = None,
                 
                 #aois
                 aoi_fp = None,
                 aoi_set_proj_crs=False, #force hte project crs from the aoi
                 aoi_vlay = None,
                 
                 compress='med', #raster compression default
                 
                 #inheritance
                 session=None, #parent session for child mode
                 inher_d = {},
                 **kwargs):

        
        mod_logger.debug('Qproj super')
        
        super().__init__(
            inher_d = {**inher_d,
                **{'Qproj':['qap', 'qproj', 'vlay_drivers', 'feedback']}},
            
            **kwargs) #initilzie teh baseclass
        
        #=======================================================================
        # attach
        #=======================================================================
        self.compress=compress
        
        #=======================================================================
        # setup qgis
        #=======================================================================
        if feedback is None:
            """
            self.logger.info('test')
            """
            #build a separate logger to capture algorhtihim feedback
            qlogger= get_new_file_logger('Qproj',
                fp=os.path.join(self.work_dir, 'Qproj.log'))
 
            feedback = MyFeedBackQ(logger=qlogger)
            
        self.feedback = feedback
        
        if aoi_set_proj_crs:
            assert crs is None
        
        if not crsID_default is None: 
            self.crsID_default=crsID_default
            
        #standalone
        if session is None:
            self._init_qgis(crs=crs)
            
            self._init_algos()
            
            self._set_vdrivers()
            
        #child mode
        else:
            self.inherit(session=session)
            """having a separate mstore is one of the main reasons to use children"""
            self.mstore = QgsMapLayerStore() #build a new map store 
        
        
        if not self.proj_checks():
            raise Error('failed checks')
        """
        self.tag
        """
        #=======================================================================
        # aois
        #=======================================================================
        if not aoi_fp is None:
            self.aoi_fp=aoi_fp
            self.load_aoi(aoi_fp, set_proj_crs=aoi_set_proj_crs)
        
        if not aoi_vlay is None:
            assert aoi_fp is None, 'cant pass a layer and a filepath'
            self._check_aoi(aoi_vlay)
            self.aoi_vlay= aoi_vlay
        
        self.logger.info('Qproj __INIT__ finished w/ crs \'%s\''%self.qproj.crs().authid())
        
        
        return
    
    
    def _init_qgis(self, #instantiate qgis
                   crs=None,
                  gui = False): 
        """
        WARNING: need to hold this app somewhere. call in the module you're working in (scripts)
        
        """
        log = self.logger.getChild('_init_qgis')
        
        
        #=======================================================================
        # init the application
        #=======================================================================
        
        try:
            
            QgsApplication.setPrefixPath(r'C:/OSGeo4W64/apps/qgis-ltr', True)
            
            app = QgsApplication([], gui)

            app.initQgis()


            
        
        except:
            raise Error('QGIS failed to initiate')
        
        #=======================================================================
        # store the references
        #=======================================================================
        self.qap = app
        self.qproj = QgsProject.instance()
        self.mstore = QgsMapLayerStore() #build a new map store
        
        #=======================================================================
        # crs
        #=======================================================================
        if crs is None: 
            crs = QgsCoordinateReferenceSystem(self.crsID_default)
            
            
        assert isinstance(crs, QgsCoordinateReferenceSystem), 'bad crs type'
        assert crs.isValid()
            
        self.qproj.setCrs(crs)
        
        log.info('set project crs to %s'%self.qproj.crs().authid())
        

    
    

    def _set_vdrivers(self):
        
        #build vector drivers list by extension
        """couldnt find a good built-in to link extensions with drivers"""
        vlay_drivers = {'SpatiaLite':'sqlite', 'OGR':'shp'}
        
        
        #vlay_drivers = {'sqlite':'SpatiaLite', 'shp':'OGR','csv':'delimitedtext'}
        
        for ext in QgsVectorFileWriter.supportedFormatExtensions():
            dname = QgsVectorFileWriter.driverForExtension(ext)
            
            if not dname in vlay_drivers.keys():
            
                vlay_drivers[dname] = ext
            
        #add in missing/duplicated
        for vdriver in QgsVectorFileWriter.ogrDriverList():
            if not vdriver.driverName in vlay_drivers.keys():
                vlay_drivers[vdriver.driverName] ='?'
                
        self.vlay_drivers = vlay_drivers
        
        self.logger.debug('built driver:extensions dict: \n    %s'%vlay_drivers)
        
        return
        

           
    def proj_checks(self):
        log = self.logger.getChild('proj_checks')
        
        if not self.driverName in self.vlay_drivers:
            raise Error('unrecognized driver name')
        
        if not self.out_dName in self.vlay_drivers:
            raise Error('unrecognized driver name')
 
        assert not self.feedback is None
        
 
        log.debug('project passed all checks')
        
        return True
    
    #===========================================================================
    # READ/WRITE-----
    #===========================================================================
    
    def vlay_write(self, #write  a VectorLayer
        vlay, out_fp, 

        driverName='GPKG',
        fileEncoding = "CP1250", 
        opts = QgsVectorFileWriter.SaveVectorOptions(), #empty options object
        overwrite=None,
        logger=mod_logger):
        """
        help(QgsVectorFileWriter.SaveVectorOptions)
        QgsVectorFileWriter.SaveVectorOptions.driverName='GPKG'
        
        
        opt2 = QgsVectorFileWriter.BoolOption(QgsVectorFileWriter.CreateOrOverwriteFile)
        
        help(QgsVectorFileWriter)

        """

        #==========================================================================
        # defaults
        #==========================================================================
        log = logger.getChild('vlay_write')
        if overwrite is None: overwrite=self.overwrite
 
        #===========================================================================
        # assemble options
        #===========================================================================
        opts.driverName = driverName
        opts.fileEncoding = fileEncoding

        #===========================================================================
        # checks
        #===========================================================================
        #file extension
        fhead, ext = os.path.splitext(out_fp)
        
        if not 'gpkg' in ext:
            raise Error('unexpected extension: %s'%ext)
        
        if os.path.exists(out_fp):
            msg = 'requested file path already exists!. overwrite=%s \n    %s'%(
                overwrite, out_fp)
            if overwrite:
                log.warning(msg)
                os.remove(out_fp) #workaround... should be away to overwrite with the QgsVectorFileWriter
            else:
                raise Error(msg)
            
        
        if vlay.dataProvider().featureCount() == 0:
            raise Error('\'%s\' has no features!'%(
                vlay.name()))
            
        if not vlay.isValid():
            Error('passed invalid layer')

        #=======================================================================
        # write
        #=======================================================================
        
        error = QgsVectorFileWriter.writeAsVectorFormatV2(
                vlay, out_fp, 
                QgsCoordinateTransformContext(),
                opts,
                )

        #=======================================================================
        # wrap and check
        #=======================================================================
          
        if error[0] == QgsVectorFileWriter.NoError:
            log.debug('layer \' %s \' written to: \n     %s'%(vlay.name(),out_fp))
            return out_fp
         
        raise Error('FAILURE on writing layer \' %s \'  with code:\n    %s \n    %s'%(vlay.name(),error, out_fp))
        
    
    def vlay_load(self,
                  file_path,

                  providerLib='ogr',
                  addSpatialIndex=True,
                  dropZ=False,
                  reproj=False, #whether to reproject hte layer to match the project
                  
                  set_proj_crs = False, #set the project crs from this layer

                  logger = None,
                  ):
        
        #=======================================================================
        # setup
        #=======================================================================
        if logger is None: logger=self.logger
        log = logger.getChild('vlay_load')
        
        
        #file
        assert os.path.exists(file_path), file_path
        fname, ext = os.path.splitext(os.path.split(file_path)[1])
        assert not ext in ['tif'], 'passed invalid filetype: %s'%ext
        log.debug('on %s'%file_path)
        mstore = QgsMapLayerStore() 
        #===========================================================================
        # load the layer--------------
        #===========================================================================

        vlay_raw = QgsVectorLayer(file_path,fname,providerLib)
  
        #===========================================================================
        # checks
        #===========================================================================
        if not isinstance(vlay_raw, QgsVectorLayer): 
            raise IOError
        
        #check if this is valid
        if not vlay_raw.isValid():
            raise Error('loaded vlay \'%s\' is not valid. \n \n did you initilize?'%vlay_raw.name())
        
        #check if it has geometry
        if vlay_raw.wkbType() == 100:
            raise Error('loaded vlay has NoGeometry')
        
        #check coordinate system
        if not vlay_raw.crs().isValid():
            raise Error('bad crs')
        
        if vlay_raw.crs().authid() == '':
            log.warning('bad crs')
 
        #=======================================================================
        # clean------
        #=======================================================================
        #spatial index
        if addSpatialIndex and (not vlay_raw.hasSpatialIndex()==QgsFeatureSource.SpatialIndexPresent):
            self.createspatialindex(vlay_raw, logger=log)
            


        #=======================================================================
        # #zvalues
        #=======================================================================
 
        
        if dropZ and vlay_raw.wkbType()>=1000:
            vlay1 = processing.run('native:dropmzvalues', 
                                   {'INPUT':vlay_raw, 'OUTPUT':'TEMPORARY_OUTPUT', 'DROP_Z_VALUES':True},  
                                   #feedback=self.feedback, 
                                   context=self.context)['OUTPUT']
            mstore.addMapLayer(vlay_raw)
        else:
            vlay1 = vlay_raw
            
        #=======================================================================
        # #CRS
        #=======================================================================
        if not vlay1.crs() == self.qproj.crs():
            log.warning('\'%s\' crs: \'%s\' doesnt match project: %s. reproj=%s. set_proj_crs=%s'%(
                vlay_raw.name(), vlay_raw.crs().authid(), self.qproj.crs().authid(), reproj, set_proj_crs))
            
            if reproj:
                vlay2 = self.reproject(vlay1, logger=log)['OUTPUT']
                mstore.addMapLayer(vlay1)

                
            elif set_proj_crs:
                self.qproj.setCrs(vlay1.crs())
                vlay2 = vlay1
                
            else:
                vlay2 = vlay1
                
        else:
            vlay2 = vlay1
 
                
        vlay2.setName(vlay_raw.name())
 
        #=======================================================================
        # wrap
        #=======================================================================
        #add to project
        'needed for some algorhithims?. moved to algos'
        #vlay = self.qproj.addMapLayer(vlay, False)
        

        dp = vlay2.dataProvider()
                
        log.debug('loaded \'%s\' as \'%s\' \'%s\'  with %i feats crs: \'%s\' from file: \n     %s'
                    %(vlay2.name(), dp.storageType(), 
                      QgsWkbTypes().displayString(vlay2.wkbType()), 
                      dp.featureCount(), 
                      vlay2.crs().authid(),
                      file_path))
        
        mstore.removeAllMapLayers()
        
        return vlay2
    
    def rlay_write(self, #make a local copy of the passed raster layer
                   rlayer, #raster layer to make a local copy of
                   extent = 'layer', #write extent control
                        #'layer': use the current extent (default)
                        #'mapCanvas': use the current map Canvas
                        #QgsRectangle: use passed extents
                   
                   
                   resolution = 'raw', #resolution for output
                   nodata=-9999,
                   
                   opts = ["COMPRESS=LZW"], #QgsRasterFileWriter.setCreateOptions
                   
                   out_dir = None, #directory for puts
                   newLayerName = None,
                   ofp=None,
                   
                   logger=None,
                   ):
        """
        taken from CanFlood.hlpr.Q
        because  processing tools only work on local copies
            otherwise.. better to use gdalwarp or some other algo
        
        #=======================================================================
        # coordinate transformation
        #=======================================================================
        NO CONVERSION HERE!
        can't get native API to work. use gdal_warp instead
        """
        
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        
        if newLayerName is None: newLayerName = rlayer.name()
        
        log = logger.getChild('write_rlay')
        #=======================================================================
        # filepaths
        #=======================================================================
        if ofp is None:
            if out_dir is None: out_dir = self.out_dir
            assert os.path.exists(out_dir)
            
            newFn = get_valid_filename('%s.tif'%newLayerName) #clean it
        
            ofp = os.path.join(out_dir, newFn)

        
        
        
        log.debug('on \'%s\' w/ \n    crs:%s \n    extents:%s\n    xUnits:%.4f'%(
            rlayer.name(), rlayer.crs(), rlayer.extent(), rlayer.rasterUnitsPerPixelX()))
        
        #=======================================================================
        # precheck
        #=======================================================================
        assert isinstance(rlayer, QgsRasterLayer)
        
        
        if os.path.exists(ofp):
            msg = 'requested file already exists! and overwrite=%s \n    %s'%(
                self.overwrite, ofp)
            if self.overwrite:
                log.warning(msg)
            else:
                raise Error(msg)
            
        #=======================================================================
        # coordinate transformation
        #=======================================================================
        """see note"""
        transformContext = self.qproj.transformContext()
            
        #=======================================================================
        # extents
        #=======================================================================
        if extent == 'layer':
            extent = rlayer.extent()
            
        elif extent=='mapCanvas':
            assert isinstance(self.iface, QgisInterface), 'bad key for StandAlone?'
            
            #get the extent, transformed to the current CRS
            extent =  QgsCoordinateTransform(
                self.qproj.crs(), 
                rlayer.crs(), 
                transformContext
                    ).transformBoundingBox(self.iface.mapCanvas().extent())
                
        assert isinstance(extent, QgsRectangle), 'expected extent=QgsRectangle. got \"%s\''%extent
        
        #expect the requested extent to be LESS THAN what we have in the raw raster
        assert rlayer.extent().width()>=extent.width(), 'passed extents too wide'
        assert rlayer.extent().height()>=extent.height(), 'passed extents too tall'
        #=======================================================================
        # resolution
        #=======================================================================
        #use the resolution of the raw file
        if resolution == 'raw':
            """this respects the calculated extents"""
            
            nRows = int(extent.height()/rlayer.rasterUnitsPerPixelY())
            nCols = int(extent.width()/rlayer.rasterUnitsPerPixelX())
            

            
        elif isinstance(resolution, int):
            nRows = int(extent.height()/resolution)
            nCols = int(extent.width()/resolution)
        else:
            """dont think theres any decent API support for the GUI behavior"""
            raise Error('not implemented')

        #=======================================================================
        # extract info from layer
        #=======================================================================
        """consider loading the layer and duplicating the renderer?
        renderer = rlayer.renderer()"""
        provider = rlayer.dataProvider()
        
        
        #build projector
        projector = QgsRasterProjector()
        #projector.setCrs(provider.crs(), provider.crs())
        

        #build and configure pipe
        pipe = QgsRasterPipe()
        if not pipe.set(provider.clone()): #Insert a new known interface in default place
            raise Error("Cannot set pipe provider")
             
        if not pipe.insert(2, projector): #insert interface at specified index and connect
            raise Error("Cannot set pipe projector")
 
        
        #=======================================================================
        # #build file writer
        #=======================================================================

        file_writer = QgsRasterFileWriter(ofp)
        #file_writer.Mode(1) #???
        
        if not opts is None:
            file_writer.setCreateOptions(opts)
        
        log.info('writing to file w/ \n    %s'%(
            {'nCols':nCols, 'nRows':nRows, 'extent':extent, 'crs':rlayer.crs()}))
        
        #execute write
        error = file_writer.writeRaster( pipe, nCols, nRows, extent, rlayer.crs(), transformContext,
                                         feedback=RasterFeedback(log), #not passing any feedback
                                         )
        
        log.info('wrote to file \n    %s'%ofp)
        
        if not error == QgsRasterFileWriter.NoError:
            raise Error(error)
        
        #=======================================================================
        # handle nodata
        #=======================================================================
        """cant figure out how to specify nodata value"""
        nodata_val_native = get_nodata_val(ofp)
        if not nodata_val_native==nodata:
            ofp1 = self.warpreproject(ofp, nodata_val=nodata, logger=log)
            
            #move file over
            os.remove(ofp)
            shutil.copy2(ofp1,ofp)
            
        
        #=======================================================================
        # wrap
        #=======================================================================
        assert os.path.exists(ofp)
        
        assert QgsRasterLayer.isValidRasterFileName(ofp),  \
            'requested file is not a valid raster file type: %s'%ofp
        
        
        return ofp
    
    def rlay_load(self, fp,  #load a raster layer and apply an aoi clip
                  aoi_vlay = None,
                  logger=None,
                  
                  #crs handling
                  set_proj_crs = False, #set the project crs from this layer
                  reproj=False,
                  **clipKwargs):
        
        #=======================================================================
        # defautls
        #=======================================================================
        if logger is None: logger = self.logger
        log = logger.getChild('rlay_load')
        mstore = QgsMapLayerStore() #build a new store
        
        assert os.path.exists(fp), 'requested file does not exist: \n    %s'%fp
        assert QgsRasterLayer.isValidRasterFileName(fp),  \
            'requested file is not a valid raster file type: %s'%fp
        
        basefn = os.path.splitext(os.path.split(fp)[1])[0]
    
        log.debug("QgsRasterLayer(%s, %s)"%(fp, basefn))
        #=======================================================================
        # load
        #=======================================================================
        rlay_raw = QgsRasterLayer(fp, basefn)
        
        #===========================================================================
        # check
        #===========================================================================
        
        assert isinstance(rlay_raw, QgsRasterLayer), 'failed to get a QgsRasterLayer'
        assert rlay_raw.isValid(), "Layer failed to load!"
        log.debug('loaded \'%s\' from \n    %s'%(rlay_raw.name(), fp))
        
        #=======================================================================
        # #CRS
        #=======================================================================
        if not rlay_raw.crs() == self.qproj.crs():
            log.warning('\'%s\'  match fail (%s v %s) \n    reproj=%s set_proj_crs=%s'%(
                rlay_raw.name(), rlay_raw.crs().authid(), self.qproj.crs().authid(), reproj, set_proj_crs))
            
            if reproj:
                raise Error('not implemented')

                mstore.addMapLayer(rlay_raw)

                
            elif set_proj_crs:
                self.qproj.setCrs(rlay_raw.crs())
                rlay1 = rlay_raw
                
            else:
                rlay1 = rlay_raw
                
        else:
            rlay1 = rlay_raw
        
        #=======================================================================
        # aoi
        #=======================================================================

        if not aoi_vlay is None:

            log.debug('clipping with %s'%aoi_vlay.name())

            rlay2 = self.cliprasterwithpolygon(rlay1,aoi_vlay, 
                               logger=log, layname=rlay1.name(), **clipKwargs)
            
            #remove the raw
            
            mstore.addMapLayer(rlay1) #add the layers to the store
            
        else:
            rlay2 = rlay1

        #=======================================================================
        # wrap
        #=======================================================================
        mstore.removeAllMapLayers() #remove all the layers
            
        
        return rlay2
    #===========================================================================
    # AOI methods--------
    #===========================================================================
    def load_aoi(self,
                 aoi_fp, **kwargs):
        
        log= self.logger.getChild('load_aoi')
        vlay = self.vlay_load(aoi_fp, **kwargs)
        self._check_aoi(vlay)
        self.aoi_vlay = vlay
        
        log.info('loaded aoi \'%s\''%vlay.name())

        
        return vlay
    
    def slice_aoi(self, vlay,
                  aoi_vlay = None,
                  sfx = 'aoi',
                  logger=None):
        """
        also see misc.aoi_slicing
        """
        
        #=======================================================================
        # defaults
        #=======================================================================
        if aoi_vlay is None: aoi_vlay = self.aoi_vlay
        if logger is None: logger=self.logger
        log = logger.getChild('slice_aoi')
        
        #=======================================================================
        # prechecks
        #=======================================================================
        self._check_aoi(aoi_vlay)
        
        
        #=======================================================================
        # slice by aoi
        #=======================================================================

        vlay.removeSelection()
        log.info('slicing finv \'%s\' and %i feats w/ aoi \'%s\''%(
            vlay.name(),vlay.dataProvider().featureCount(), aoi_vlay.name()))
        

        
        res_vlay =  self.selectbylocation(vlay, aoi_vlay, result_type='layer', logger=log)
        
        #=======================================================================
        # wrap
        #=======================================================================
        vlay.removeSelection()
        #got some selection
        if isinstance(res_vlay, QgsVectorLayer):
            res_vlay.setName('%s_%s'%(vlay.name(), sfx))
            
        
        
            
        return res_vlay
    
    def _check_aoi(self, #special c hecks for AOI layers
                  vlay, 
                  logger=None):
        
        assert isinstance(vlay, QgsVectorLayer)
        assert 'Polygon' in QgsWkbTypes().displayString(vlay.wkbType())
        assert vlay.dataProvider().featureCount()==1
        assert vlay.crs() == self.qproj.crs(), 'aoi CRS (%s) does not match project (%s)'%(vlay.crs(), self.qproj.crs())
        
        return 
    
    #===========================================================================
    # VLAY methods-------------
    #===========================================================================
    def vlay_new_df(self, #build a vlay from a df
            df_raw,
            
            geo_d = None, #container of geometry objects {fid: QgsGeometry}

            crs=None,
            gkey = None, #data field linking with geo_d (if None.. uses df index)
            
            layname='df',
            
            index = False, #whether to include the index as a field
            logger=None, 

            ):
        """
        performance enhancement over vlay_new_df
            simpler, clearer
            although less versatile
        """
        #=======================================================================
        # setup
        #=======================================================================
        if crs is None: crs = self.qproj.crs()
        if logger is None: logger = self.logger
            
        log = logger.getChild('vlay_new_df')
        
        
        #=======================================================================
        # index fix
        #=======================================================================
        df = df_raw.copy()
        
        if index:
            if not df.index.name is None:
                coln = df.index.name
                df.index.name = None
            else:
                coln = 'index'
                
            df[coln] = df.index
            
        #=======================================================================
        # precheck
        #=======================================================================
        
        
        #make sure none of hte field names execeed the driver limitations
        max_len = fieldn_max_d[self.driverName]
        
        #check lengths
        boolcol = df_raw.columns.str.len() >= max_len
        
        if np.any(boolcol):
            log.warning('passed %i columns which exeed the max length=%i for driver \'%s\'.. truncating: \n    %s'%(
                boolcol.sum(), max_len, self.driverName, df_raw.columns[boolcol].tolist()))
            
            
            df.columns = df.columns.str.slice(start=0, stop=max_len-1)

        
        #make sure the columns are unique
        assert df.columns.is_unique
        
        #check the geometry
        if not geo_d is None:
            assert isinstance(geo_d, dict)
            if not gkey is None:
                assert gkey in df_raw.columns
        
                #assert 'int' in df_raw[gkey].dtype.name
                
                #check gkey match
                l = set(df_raw[gkey].drop_duplicates()).difference(geo_d.keys())
                assert len(l)==0, 'missing %i \'%s\' keys in geo_d: %s'%(len(l), gkey, l)
                
            #against index
            else:
                
                #check gkey match
                l = set(df_raw.index).difference(geo_d.keys())
                assert len(l)==0, 'missing %i (of %i) fid keys in geo_d: %s'%(len(l), len(df_raw), l)

        #===========================================================================
        # assemble the fields
        #===========================================================================
        #column name and python type
        fields_d = {coln:np_to_pytype(col.dtype) for coln, col in df.items()}
        
        #fields container
        qfields = fields_build_new(fields_d = fields_d, logger=log)
        
        #=======================================================================
        # assemble the features
        #=======================================================================
        #convert form of data
        
        feats_d = dict()
        for fid, row in df.iterrows():
    
            feat = QgsFeature(qfields, fid) 
            
            #loop and add data
            for fieldn, value in row.items():
    
                #skip null values
                if pd.isnull(value): continue
                
                #get the index for this field
                findx = feat.fieldNameIndex(fieldn) 
                
                #get the qfield
                qfield = feat.fields().at(findx)
                
                #make the type match
                ndata = qtype_to_pytype(value, qfield.type(), logger=log)
                
                #set the attribute
                if not feat.setAttribute(findx, ndata):
                    raise Error('failed to setAttribute')
                
            #setgeometry
            if not geo_d is None:
                if gkey is None:
                    gobj = geo_d[fid]
                else:
                    gobj = geo_d[row[gkey]]
                
                feat.setGeometry(gobj)
            
            #stor eit
            feats_d[fid]=feat
        
        log.debug('built %i \'%s\'  features'%(
            len(feats_d),
            QgsWkbTypes.geometryDisplayString(feat.geometry().type()),
            ))
        
        
        #=======================================================================
        # get the geo type
        #=======================================================================\
        if not geo_d is None:
            gtype = QgsWkbTypes().displayString(next(iter(geo_d.values())).wkbType())
        else:
            gtype='None'

            
            
        #===========================================================================
        # buidl the new layer
        #===========================================================================
        vlay = vlay_new_mlay(gtype,
                             crs, 
                             layname,
                             qfields,
                             list(feats_d.values()),
                             logger=log,
                             )
        self.createspatialindex(vlay, logger=log)
        #=======================================================================
        # post check
        #=======================================================================
        if not geo_d is None:
            if vlay.wkbType() == 100:
                raise Error('constructed layer has NoGeometry')

        return vlay
    
    def vlay_rename_fields(self,
        vlay_raw,
        rnm_d, #field name conversions to apply {old FieldName:newFieldName}
        #output='TEMPORARY_OUTPUT', #NO! need to load a layer
        logger=None,
 

        ):
        """
        for single field renames, better to use 'native:renametablefield'
        """
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=mod_logger
        log=logger.getChild('vlay_rename_fields')
        
        #=======================================================================
        # #get a working layer
        #=======================================================================
        vlay_raw.selectAll()
        vlay = processing.run('native:saveselectedfeatures', 
                {'INPUT' : vlay_raw, 'OUTPUT' : 'TEMPORARY_OUTPUT'}, 
                #feedback=self.feedback,
                )['OUTPUT']
        
        #=======================================================================
        # check and convert to index
        #=======================================================================
        #get fieldname index conversion for layer
        fni_d = {f.name():vlay.dataProvider().fieldNameIndex(f.name()) for f in vlay.fields()}
        
        #check it
        for k in rnm_d.keys():
            assert k in fni_d.keys(), 'requested field \'%s\' not on layer'%k
        
        #re-index rename request
        fiRn_d = {fni_d[k]:v for k,v in rnm_d.items()}
    
        #=======================================================================
        # #apply renames
        #=======================================================================
        if not vlay.dataProvider().renameAttributes(fiRn_d):
            raise Error('failed to rename')
        vlay.updateFields()
        
        #=======================================================================
        # #check it
        #=======================================================================
        fn_l = [f.name() for f in vlay.fields()]
        s = set(rnm_d.values()).difference(fn_l)
        assert len(s)==0, 'failed to rename %i fields: %s'%(len(s), s)
        
        #=======================================================================
        # wrap
        #=======================================================================
        vlay.setName(vlay_raw.name())
        
        log.debug('applied renames to \'%s\' \n    %s'%(vlay.name(), rnm_d))
        
        
        return vlay
    
    def vlay_poly_tarea(self,#get the total area of polygons within the layer
        vlay_raw,
        logger=None):
    
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        log=logger.getChild('vlay_poly_tarea')
        mstore = QgsMapLayerStore()
        

        #=======================================================================
        # add the geometry
        #=======================================================================
        vlayg = self.addgeometry(vlay_raw, logger=log)
        mstore.addMapLayer(vlayg)
        
        #=======================================================================
        # pull the values
        #=======================================================================
        area_d = vlay_get_fdata(vlayg, 'area')
        mstore.removeAllMapLayers()
        
        return pd.Series(area_d).sum()
    
    #===========================================================================
    # RLAYs--------
    #===========================================================================
    def rcalc1(self, #simple raster calculations with a single raster
               ref_lay,
               formula, #string formatted formula
               rasterEntries, #list of QgsRasterCalculatorEntry
               ofp=None,
               out_dir=None,
               layname='result',
               logger=None,
               clear_all=False, #clear all rasters from memory
               compress='none', #optional compression. #usually we are deleting calc results
               ):
        """
        see __rCalcEntry
        
        memory handling:
            would be easier to make a standalone worker
        """
        
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        log=logger.getChild('rcalc1')
        mstore = QgsMapLayerStore()
        if compress is None: compress=self.compress
        #=======================================================================
        # output file
        #=======================================================================
        if ofp is None:
            if out_dir is None: out_dir=self.temp_dir
    
            if not os.path.exists(out_dir):
                os.makedirs(out_dir)
            
            ofp = os.path.join(out_dir,layname+'.tif' )
            
        if os.path.exists(ofp): 
            assert self.overwrite
            
            try:
                os.remove(ofp)
            except Exception as e:
                raise Error('failed to clear existing file.. unable to write \n    %s \n    %s'%(
                    ofp, e))
                
        assert ofp.endswith('.tif')
        
        #set based on whether we  want to applpy some post compression
        if compress == 'none':
            ofp1=ofp
        else:
            ofp1 = os.path.join(self.temp_dir,layname+'_raw.tif')
        #=======================================================================
        # assemble parameters
        #=======================================================================
        if isinstance(ref_lay, str):
            rlay = self.rlay_load(ref_lay, logger=log)
            mstore.addMapLayer(rlay)
        else:
            rlay = ref_lay
        
        outputExtent  = rlay.extent()
        outputFormat = 'GTiff'
        nOutputColumns = rlay.width()
        nOutputRows = rlay.height()
 
        crsTrnsf = QgsCoordinateTransformContext()
        #=======================================================================
        # execute
        #=======================================================================
        log.debug('on %s'%formula)
 
        rcalc = QgsRasterCalculator(formula, ofp1, 
                                    outputFormat, 
                                    outputExtent,
                                    self.qproj.crs(),
                            nOutputColumns, nOutputRows, rasterEntries,crsTrnsf)
        
        result = rcalc.processCalculation(feedback=self.feedback)
        
        #=======================================================================
        # check    
        #=======================================================================
        if not result == 0:
            raise Error('formula=%s failed w/ \n    %s'%(formula, rcalc.lastError()))
        
        
        
        log.debug('saved result to: \n    %s'%ofp1)
        
        #=======================================================================
        # compression
        #=======================================================================
        if not compress == 'none':
            assert not ofp1==ofp
            res = self.warpreproject(ofp1, compression=compress, output=ofp, logger=log)
            assert ofp==res
            
        assert os.path.exists(ofp)
        #=======================================================================
        # wrap
        #=======================================================================

        if clear_all:
            log.debug('clearing layers')
            for rcentry in rasterEntries:
                mstore.addMapLayer(rcentry.raster)
        mstore.removeAllMapLayers()
        
        log.debug('finished')
        return ofp
    
    def mask_build(self, #get a mask from a raster with data
                   rlay,
                   logger=None,
                   layname=None,
                   zero_shift=False, #necessary for preserving zero values
                   thresh=None, #optional threshold value with which to build raster
                   thresh_type='lower', #specify whether threshold is a lower or an upper bound
                   rval=None, #make a mask from a specific value
                   ofp=None, **kwargs):
        
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        log=logger.getChild('mask_invert')
        
        rcentry = self._rCalcEntry(rlay)

        if layname is None: layname='%s_mask'%rcentry.raster.name()
        
        log.debug('on %s w/ zero_shift=%s, thresh=%s'%(
            rlay, zero_shift, thresh))
        #=======================================================================
        # build formula--
        #=======================================================================
        if rval is None:
            #=======================================================================
            # #from teh data as is
            #=======================================================================
            if thresh is None:

                if zero_shift:
                    """
                    without this.. zero values will show up as null on the mask
                    """
                    f1 = '(abs(\"{0}\")+999)'.format(rcentry.ref)
                    formula = f1 + '/' + f1
                else:
                
                    formula = '\"{0}\"/\"{0}\"'.format(rcentry.ref)
            #=======================================================================
            # #apply a threshold to the data
            #=======================================================================

            else:
                """
                WARNING: strange treatment of values right around some specified threshold (e.g., rlay<1.01). 
                I think the data has more precision than what is shown (even with using the raster rounder)
                """
                
                if thresh_type=='lower': # val=1 where greater than the lower bound
                    thresh_i = thresh-.001
                    f1 = '(\"{0}\">={1:.3f})'.format(rcentry.ref, thresh_i)
                    
                elif thresh_type=='upper':
                    thresh_i = thresh+.001
                    f1 = '(\"{0}\"<={1:.3f})'.format(rcentry.ref, thresh_i)
                else:
                    raise Error('bad thresh_type: %s'%thresh_type)
                formula = f1 + '/' + f1
        
        #=======================================================================
        # fixed values
        #=======================================================================
        else:
            """note... this is sensitivite to precision"""
            f1 = '(\"{0}\"={1:.1f})'.format(rcentry.ref, rval)
            formula = f1 + '/' + f1
 
        #=======================================================================
        # get
        #=======================================================================
        return self.rcalc1(rlay, formula, [rcentry], logger=log,ofp=ofp,
                           layname=layname, **kwargs)
 
     
    def mask_invert(self, #take a mask layer, and invert it
                    rlay,
                    logger=None,
                    ofp=None,
                    include_nulls=False, #whether to treat nulls as postiive in the result
                    layname='mask_invert',
                    **kwargs):
        """ surprised there isnt a builtin wayu to do this
        """
        
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        log=logger.getChild('mask_invert')
 
        if include_nulls: 
            raise Error('not implememnted')
        #=======================================================================
        # add 2 to nodata
        #=======================================================================
        rlay1_fp= self.fillnodata(rlay, fval=2, logger=log)
        
        #=======================================================================
        # subtract 1 from all
        #=======================================================================
        """were assuming the layer is already a mask"""
        
        rcentry = self._rCalcEntry(rlay1_fp)
        
        formula = '({0}-1)/({0}-1)'.format(rcentry.ref)
        
        """
        rcentry.raster
        """
        
        return self.rcalc1(rlay1_fp, formula, [rcentry], logger=log,ofp=ofp,
                           layname=layname, **kwargs)
        

    def mask_apply(self, #apply a mask to a la yer
                   rlay, #layer to mask
                   mask_rlay, #mask raseter
                        #1=dont mask; 0 or nan = mask 
                   invert_mask=False,
                   layname=None,
                   logger=None,
                   **kwargs):
        """

        mask=1: result = rlay
        mask=0: result = nan
        mask=nan: result=nan
        rlay=nan: result=nan
        """
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        log=logger.getChild('mask_apply')

        #=======================================================================
        # handle inverstion
        #=======================================================================
        
        if invert_mask:
 
            mask_rlay1 = self.mask_invert(mask_rlay, logger=log,
                          ofp=os.path.join(self.temp_dir, 
                           '%s_invert.tif'%os.path.basename(mask_rlay).replace('.tif', '')),
                          )
        else:
            mask_rlay1 = mask_rlay
        #===================================================================
        # build the raster calc entries
        #===================================================================
        log.debug('building entries')

 
        rlay_obj_d = {'raw':rlay, 'mask':mask_rlay1}
        rcentry_d = dict()
        
        for k,obj in rlay_obj_d.items():
            rcentry_d[k] = self._rCalcEntry(obj, logger=log)
 
        
        
        if layname is None: 
            layname = (rcentry_d['raw'].raster.name() + '%s_maskd'%(
                rcentry_d['raw'].raster.name())) 
        #===================================================================
        # build the formula
        #===================================================================

        formula = '(\"{0}\"/\"{1}\")'.format(rcentry_d['raw'].ref, rcentry_d['mask'].ref)
 
        #=======================================================================
        # execute
        #=======================================================================
        
        ofp = self.rcalc1(rcentry_d['raw'].raster, formula,
                          list(rcentry_d.values()),
                          layname=layname,
                          logger=log,**kwargs)
        
        log.debug('finished w/ %s'%ofp)
        
 
        
        return ofp
    
    def get_resolution(self,  
                       rlay):
        #setup
        mstore=QgsMapLayerStore()
        
        if isinstance(rlay, str):
            rlay = self.rlay_load(rlay)
            mstore.addMapLayer(rlay)
            
        assert isinstance(rlay, QgsRasterLayer)
        
        
        
        #get the resolution
        res = (rlay.rasterUnitsPerPixelY() + rlay.rasterUnitsPerPixelX())*0.5
        
        #wrap
        mstore.removeAllMapLayers()
        
        return res
    
    def rlay_get_cellCnt(self,
                         rlay,
                         exclude_nulls=True,
                         log=None,
                         ):
        """surprised there is no builtin"""
        
        #=======================================================================
        # defaults
        #=======================================================================
        if log is None: log=self.logger.getChild('rlay_get_cellCnt')
        #setup

        mstore=QgsMapLayerStore()
        
        if isinstance(rlay, str):
 
            rlay = self.rlay_load(rlay, logger=log)
            mstore.addMapLayer(rlay)
 

        assert isinstance(rlay, QgsRasterLayer)
        
        if exclude_nulls:
            
            mask = self.mask_build(rlay, zero_shift=True, logger=log)
            res= self.rasterlayerstatistics(mask)['SUM']

        else:
        
            res = rlay.width()*rlay.height()
        mstore.removeAllMapLayers()
        
        log.debug('got %i'%res)
        return int(res)
        
    def rlay_uq_vals(self, rlay,
                     prec=None,
                     log=None,
                     ):
        if log is None: log=self.logger.getChild('rlay_uq_vals')
        log.debug('on %s'%rlay)
        #pull array
        ar = rlay_to_array(rlay)
        """
        this is giving some floats that don't quite match what is shown in QGIS
        """
        #collapse into series
        ser_raw = pd.Series(ar.reshape(1, ar.size).tolist()[0]).dropna()
        
        if not prec is None:
            ser= ser_raw.round(prec)
        else:
            ser=ser_raw
 
        return np.unique(ser)
    
    def rlay_mround(self, #rounda  raster to some multiple
                    rlay,
                    multiple=0.2, #value to round to nearest multiple of
                    **kwargs):
        
        return self.rastercalculatorGDAL(
            rlay,
            '{0:.2f}*numpy.round(A/{0:.2f})'.format(multiple),
            **kwargs)
    #===========================================================================
    # HELPERS---------
    #===========================================================================
 
     
    def _rCalcEntry(self, #helper for raster calculations 
                         rlay_obj, bandNumber=1,
 
                          **kwargs):
        #=======================================================================
        # load the object
        #=======================================================================
        
        if isinstance(rlay_obj, str):
            assert os.path.exists(rlay_obj), rlay_obj
            rlay = QgsRasterLayer(rlay_obj, os.path.basename(rlay_obj).replace('.tif', ''))
            self.mstore.addMapLayer(rlay)
 
        else:
            rlay = rlay_obj
 
        #=======================================================================
        # check
        #=======================================================================
        assert isinstance(rlay, QgsRasterLayer)
        assert rlay.crs()==self.qproj.crs(), 'bad crs on \'%s\' (%s)'%(rlay.name(),rlay.crs().authid())
        
        #=======================================================================
        # build the entry
        #=======================================================================
        rcentry = QgsRasterCalculatorEntry()
        rcentry.raster =rlay #not accesesible for some reason
        rcentry.ref = '%s@%i'%(rlay.name(), bandNumber)
        rcentry.bandNumber=bandNumber
        
        return rcentry
    
    def _install_info(self, log=None, **kwargs):
        if log is None: log = self.logger
        
        log.info(u'QGIS version: %s, release: %s'%(
                Qgis.QGIS_VERSION.encode('utf-8'), Qgis.QGIS_RELEASE_NAME.encode('utf-8')))
        
        #=======================================================================
        # pyqt
        #=======================================================================
        from PyQt5 import Qt
        
        vers = ['%s = %s' % (k,v) for k,v in vars(Qt).items() if k.lower().find('version') >= 0 and not inspect.isbuiltin(v)]
        log.info('\n    '.join(sorted(vers)))
        
        
        super()._install_info(**kwargs) #initilzie teh baseclass
 
 
    def __exit__(self, #destructor
                 *args,**kwargs):
        
        #clear your map store
        self.mstore.removeAllMapLayers()
        #print('clearing mstore')
        
        super().__exit__(*args,**kwargs) #initilzie teh baseclass
        
    


    
    

    
class RasterFeedback(QgsRasterBlockFeedback):
    prog=0
    
    def __init__(self,log):
        
        self.logger=log
        
        super().__init__()
        
        
    def onNewData(self):
        print('onNewData')
        
    def setProgress(self, newProg):
        self.prog+=newProg
        print(self.prog)
 
class MyFeedBackQ(QgsProcessingFeedback):
    """
    wrapper for easier reporting and extended progress
    
    2021-07-20: logging to a separate file
        couldnt figure out how to query the sender's info
    
    Dialogs:
        built by QprojPlug.qproj_setup()
    
    Qworkers:
        built by Qcoms.__init__()
    
    """
    
    def __init__(self,
                 logger=mod_logger):
        
        self.logger=logger.getChild('FeedBack')
        
        super().__init__()

    def setProgressText(self, text):
        self.logger.debug(text)

    def pushInfo(self, info):
        self.logger.debug(info)

    def pushCommandInfo(self, info):
        self.logger.debug(info)

    def pushDebugInfo(self, info):
        self.logger.debug(info)

    def pushConsoleInfo(self, info):
        self.logger.debug(info)
        
    def pushVersionInfo(self, info):
        self.logger.debug(info)
    
    def pushWarning(self, info):
        self.logger.debug(info)

    def reportError(self, error, fatalError=False):
        """
        lots of useless junk
        """
        self.logger.debug(error)
        
    def log(self, msg):
        self.logger.debug(msg)
        
    
    def upd_prog(self, #advanced progress handling
             prog_raw, #pass None to reset
             method='raw', #whether to append value to the progress
             ): 
            
        #=======================================================================
        # defaults
        #=======================================================================
        #get the current progress
        progress = self.progress() 
    
        #===================================================================
        # prechecks
        #===================================================================
        #make sure we have some slots connected
        """not sure how to do this"""
        
        #=======================================================================
        # reseting
        #=======================================================================
        if prog_raw is None:
            """
            would be nice to reset the progressBar.. .but that would be complicated
            """
            self.setProgress(0)
            return
        
        #=======================================================================
        # setting
        #=======================================================================
        if method=='append':
            prog = min(progress + prog_raw, 100)
        elif method=='raw':
            prog = prog_raw
        elif method == 'portion':
            rem_prog = 100-progress
            prog = progress + rem_prog*(prog_raw/100)
            
        assert prog<=100
        
        #===================================================================
        # emit signalling
        #===================================================================
        self.setProgress(prog)
        

#===============================================================================
# standalone funcs--------
#===============================================================================

def vlay_get_fdf( #pull all the feature data and place into a df
                    vlay,
                    fmt='df', #result fomrat key. 
                        #dict: {fid:{fieldname:value}}
                        #df: index=fids, columns=fieldnames
                    
                    #limiters
                    request = None, #request to pull data. for more customized requestes.
                    fieldn_l = None, #or field name list. for generic requests
                    
                    #modifiers
                    reindex = None, #optinal field name to reindex df by
                    selected=False, #for only get selected features
                    
                    #expectations
                    expect_all_real = False, #whether to expect all real results
                    allow_none = False,
                    
                    #db_f = False,
                    logger=mod_logger,
                    feedback=MyFeedBackQ()):
    """
    performance improvement
    
    Warning: requests with getFeatures arent working as expected for memory layers
    
    this could be combined with vlay_get_feats()
    also see vlay_get_fdata() (for a single column)
    
    CanFlood.hlpr.Q
    
    
    RETURNS
    a dictionary in the Qgis attribute dictionary format:
        key: generally feat.id()
        value: a dictionary of {field name: attribute value}

    """
    #===========================================================================
    # setups and defaults
    #===========================================================================
    log = logger.getChild('vlay_get_fdf')
    
    assert isinstance(vlay, QgsVectorLayer)
    all_fnl = [fieldn.name() for fieldn in vlay.fields().toList()]
    
    if fieldn_l is None: #use all the fields
        fieldn_l = all_fnl
        
   
    #field name check
    assert isinstance(fieldn_l, list)
    miss_l = set(fieldn_l).difference(all_fnl)
    assert len(miss_l)==0, '\'%s\' missing %i requested fields: %s'%(vlay.name(), len(miss_l), miss_l)
  

    
    if allow_none:
        if expect_all_real:
            raise Error('cant allow none and expect all reals')
        
    
    #===========================================================================
    # prechecks
    #===========================================================================
    if not reindex is None:
        if not reindex in fieldn_l:
            raise Error('requested reindexer \'%s\' is not a field name'%reindex)
    
    if not vlay.dataProvider().featureCount()>0:
        raise Error('no features!')

    if len(fieldn_l) == 0:
        raise Error('no fields!')
    
    if fmt=='dict' and not (len(fieldn_l)==len(all_fnl)):
        raise Error('dict results dont respect field slicing')
    
    assert hasattr(feedback, 'setProgress')
    
    
    #===========================================================================
    # build the request
    #===========================================================================
    feedback.setProgress(2)
    if request is None:
        """WARNING: this doesnt seem to be slicing the fields.
        see Alg().deletecolumns()
            but this will re-key things
        
        request = QgsFeatureRequest().setSubsetOfAttributes(fieldn_l,vlay.fields())"""

        request = QgsFeatureRequest()
        
    #get selected only
    if selected: 
        request.setFilterFids(vlay.selectedFeatureIds())
        assert vlay.selectedFeatureCount()>0, 'passed selected=True but nothing is selected!'
        
    #never want geometry   
    request = request.setFlags(QgsFeatureRequest.NoGeometry) 
               

    log.debug('extracting data from \'%s\' on fields: %s'%(vlay.name(), fieldn_l))
    #===========================================================================
    # loop through each feature and extract the data
    #===========================================================================
    
    fid_attvs = dict() #{fid : {fieldn:value}}
    fcnt = vlay.dataProvider().featureCount()

    for indxr, feat in enumerate(vlay.getFeatures(request)):
        
        #zip values
        fid_attvs[feat.id()] = feat.attributes()
        
        feedback.setProgress((indxr/fcnt)*90)


    #===========================================================================
    # post checks
    #===========================================================================
    if not len(fid_attvs) == vlay.dataProvider().featureCount():
        log.debug('data result length does not match feature count')

        if not request.filterType()==3: #check if a filter fids was passed
            """todo: add check to see if the fiter request length matches tresult"""
            raise Error('no filter and data length mismatch')
        
    #check the field lengthes
    if not len(all_fnl) == len(feat.attributes()):
        raise Error('field length mismatch')

    #empty check 1
    if len(fid_attvs) == 0:
        log.warning('failed to get any data on layer \'%s\' with request'%vlay.name())
        if not allow_none:
            raise Error('no data found!')
        else:
            if fmt == 'dict': 
                return dict()
            elif  fmt == 'df':
                return pd.DataFrame()
            else:
                raise Error('unexpected fmt type')
            
    
    #===========================================================================
    # result formatting
    #===========================================================================
    log.debug('got %i data elements for \'%s\''%(
        len(fid_attvs), vlay.name()))
    
    if fmt == 'dict':
        
        return fid_attvs
    elif fmt=='df':
        
        #build the dict
        
        df_raw = pd.DataFrame.from_dict(fid_attvs, orient='index', columns=all_fnl)
        
        
        #handle column slicing and Qnulls
        """if the requester worked... we probably  wouldnt have to do this"""
        df = df_raw.loc[:, tuple(fieldn_l)].replace(NULL, np.nan)
        
        feedback.setProgress(95)
        
        if isinstance(reindex, str):
            """
            reindex='zid'
            view(df)
            """
            #try and add the index (fids) as a data column
            try:
                df = df.join(pd.Series(df.index,index=df.index, name='fid'))
            except:
                log.debug('failed to preserve the fids.. column already there?')
            
            #re-index by the passed key... should copy the fids over to 'index
            df = df.set_index(reindex, drop=True)
            
            log.debug('reindexed data by \'%s\''%reindex)
            
        return df
    
    else:
        raise Error('unrecognized fmt kwarg')
    
    
def vlay_get_fdata(vlay, 
                   fieldn,  #get data from a field
                   request=None,
                   selected=False,
                   ):
    
    #===========================================================================
    # precheck
    #===========================================================================
    fnl = [f.name() for f in vlay.fields().toList()]
    assert fieldn in fnl, 'requested field \'%s\' not on layer'%fieldn
    
    #===========================================================================
    # build request
    #===========================================================================
    if request is None:
        request = QgsFeatureRequest()
        
    request = request.setFlags(QgsFeatureRequest.NoGeometry)
    request = request.setSubsetOfAttributes([fieldn],vlay.fields())
    
    
    
    
    if selected:
        """
        todo: check if there is already a fid filter placed on the reuqester
        """
 
        sfids = vlay.selectedFeatureIds()
        
        request = request.setFilterFids(sfids)
        
    return {f.id():f.attribute(fieldn) for f in vlay.getFeatures(request)}
    

def vlay_get_geo( #get geometry dict from layer
        vlay,
        request = None, #additional requester (limiting fids). fieldn still required. additional flags added
        selected = False,
        logger=mod_logger,
        ):
    
    log = logger.getChild('vlay_get_geo')
    
    #===========================================================================
    # build the request
    #===========================================================================
    
    if request is None:
        request = QgsFeatureRequest()
    request = request.setNoAttributes() #dont get any attributes
    
    
    if selected:
        """
        todo: check if there is already a fid filter placed on the reuqester
        """
        log.debug('limiting data pull to %i selected features on \'%s\''%(
            vlay.selectedFeatureCount(), vlay.name()))
        
        sfids = vlay.selectedFeatureIds()
        
        request = request.setFilterFids(sfids)
        
        
    #===========================================================================
    # loop through and collect hte data
    #===========================================================================

    d = {f.id():f.geometry() for f in vlay.getFeatures(request)}
    
    log.debug('retrieved %i attributes from features on \'%s\''%(
        len(d), vlay.name()))
    
    #===========================================================================
    # checks
    #===========================================================================
    assert len(d)>0
    
    #===========================================================================
    # wrap
    #===========================================================================
    return d

def vlay_new_mlay(#create a new mlay
                      gtype, #"Point", "LineString", "Polygon", "MultiPoint", "MultiLineString", or "MultiPolygon".
                      crs,
                      layname,
                      qfields,
                      feats_l,

                      logger=mod_logger,
                      ):
        #=======================================================================
        # defaults
        #=======================================================================
        log = logger.getChild('vlay_new_mlay')

        #=======================================================================
        # prechecks
        #=======================================================================
        if not isinstance(layname, str):
            raise Error('expected a string for layname, isntead got %s'%type(layname))
        
        if gtype=='None':
            log.warning('constructing mlay w/ \'None\' type')
        #=======================================================================
        # assemble into new layer
        #=======================================================================
        #initilzie the layer
        EPSG_code=int(crs.authid().split(":")[1]) #get teh coordinate reference system of input_layer
        uri = gtype+'?crs=epsg:'+str(EPSG_code)+'&index=yes'
        
        vlaym = QgsVectorLayer(uri, layname, "memory")
        
        # add fields
        if not vlaym.dataProvider().addAttributes(qfields):
            raise Error('failed to add fields')
        
        vlaym.updateFields()
        
        #add feats
        if not vlaym.dataProvider().addFeatures(feats_l):
            raise Error('failed to addFeatures')
        
        vlaym.updateExtents()
        

        
        #=======================================================================
        # checks
        #=======================================================================
        if vlaym.wkbType() == 100:
            msg = 'constructed layer \'%s\' has NoGeometry'%vlaym.name()
            if gtype == 'None':
                log.debug(msg)
            else:
                raise Error(msg)

        
        log.debug('constructed \'%s\''%vlaym.name())
        return vlaym
    

    
    
    
    
    
    
def field_new(fname, 
              pytype=str, 
              driverName = 'SpatiaLite', #desired driver (to check for field name length limitations)
              fname_trunc = True, #whether to truncate field names tha texceed the limit
              logger=mod_logger): #build a QgsField
    
    #===========================================================================
    # precheck
    #===========================================================================
    if not isinstance(fname, str):
        raise IOError('expected string for fname')
    
    #vector layer field name lim itation
    max_len = fieldn_max_d[driverName]
    """
    fname = 'somereallylongname'
    """
    if len(fname) >max_len:
        log = logger.getChild('field_new')
        log.warning('got %i (>%i)characters for passed field name \'%s\'. truncating'%(len(fname), max_len, fname))
        
        if fname_trunc:
            fname = fname[:max_len]
        else:
            raise Error('field name too long')
        
    
    qtype = ptype_to_qtype(pytype)
    
    """
    #check this type
    QMetaType.typeName(QgsField(fname, qtype).type())
    
    QVariant.String
    QVariant.Int
    
     QMetaType.typeName(new_qfield.type())
    
    """
    #new_qfield = QgsField(fname, qtype)
    new_qfield = QgsField(fname, qtype, typeName=QMetaType.typeName(QgsField(fname, qtype).type()))
    
    return new_qfield

def fields_build_new( #build qfields from different data containers
                    samp_d = None, #sample data from which to build qfields {fname: value}
                    fields_d = None, #direct data from which to build qfields {fname: pytype}
                    fields_l = None, #list of QgsField objects
                    logger=mod_logger):

    log = logger.getChild('fields_build_new')
    #===========================================================================
    # buidl the fields_d
    #===========================================================================
    if (fields_d is None) and (fields_l is None): #only if we have nothign better to start with
        if samp_d is None: 
            log.error('got no data to build fields on!')
            raise IOError
        
        fields_l = []
        for fname, value in samp_d.items():
            if pd.isnull(value):
                log.error('for field \'%s\' got null value')
                raise IOError
            
            elif inspect.isclass(value):
                raise IOError
            
            fields_l.append(field_new(fname, pytype=type(value)))
            
        log.debug('built %i fields from sample data'%len(fields_l))
        
    
    
    #===========================================================================
    # buidl the fields set
    #===========================================================================
    elif fields_l is None:
        fields_l = []
        for fname, ftype in fields_d.items():
            fields_l.append(field_new(fname, pytype=ftype))
            
        log.debug('built %i fields from explicit name/type'%len(fields_l))
            
        #check it 
        if not len(fields_l) == len(fields_d):
            raise Error('length mismatch')
            
    elif fields_d is None: #check we have the other
        raise IOError
    
    
    
            
    #===========================================================================
    # build the Qfields
    #===========================================================================
    
    Qfields = QgsFields()
    
    fail_msg_d = dict()
    
    for indx, field in enumerate(fields_l): 
        if not Qfields.append(field):
            fail_msg_d[indx] = ('%i failed to append field \'%s\''%(indx, field.name()), field)

    #report
    if len(fail_msg_d)>0:
        for indx, (msg, field) in fail_msg_d.items():
            log.error(msg)
            
        raise Error('failed to write %i fields'%len(fail_msg_d))
    
    """
    field.name()
    field.constraints().constraintDescription()
    field.length()
    
    """
    
    
    #check it 
    if not len(Qfields) == len(fields_l):
        raise Error('length mismatch')


    return Qfields





def view(#view the vector data (or just a df) as a html frame
        obj, logger=mod_logger,
        #**gfd_kwargs, #kwaqrgs to pass to vlay_get_fdatas() 'doesnt work well with the requester'
        ):
    
    if isinstance(obj, pd.DataFrame) or isinstance(obj, pd.Series):
        df = obj
    elif isinstance(obj, QgsVectorLayer):
        """this will index the viewed frame by fid"""
        df = vlay_get_fdf(obj)
    else:
        raise Error('got unexpected object type: %s'%type(obj))
    
    from hp.pd import view_web_df
    view_web_df(df)
    
    logger.info('viewer closed')
    
    return

#==============================================================================
# type conversions----------------
#==============================================================================

def np_to_pytype(npdobj, logger=mod_logger):
    
    if not isinstance(npdobj, np.dtype):
        raise Error('not passed a numpy type')
    
    try:
        return npc_pytype_d[npdobj.char]

    except Exception as e:
        log = logger.getChild('np_to_pytype')
        
        if not npdobj.char in npc_pytype_d.keys():
            log.error('passed npdtype \'%s\' not found in the conversion dictionary'%npdobj.name)
            
        raise Error('failed oto convert w/ \n    %s'%e)
    

def qtype_to_pytype( #convert object to the pythonic type taht matches the passed qtype code
        obj, 
        qtype_code, #qtupe code (qfield.type())
        logger=mod_logger): 
    
    if is_qtype_match(obj, qtype_code): #no conversion needed
        return obj 
    
    
    #===========================================================================
    # shortcut for nulls
    #===========================================================================
    if qisnull(obj):
        return None

        
    
        
    
    
    #get pythonic type for this code
    py_type = type_qvar_py_d[qtype_code]
    
    try:
        return py_type(obj)
    except:
        #datetime
        if qtype_code == 16:
            return obj.toPyDateTime()
        
        
        log = logger.getChild('qtype_to_pytype')
        if obj is None:
            log.error('got NONE type')
            
        elif isinstance(obj, QVariant):
            log.error('got a Qvariant object')
            
        else:
            log.error('unable to map object \'%s\' of type \'%s\' to type \'%s\''
                      %(obj, type(obj), py_type))
            
            
            """
            QMetaType.typeName(obj)
            """
        raise IOError
    
def ptype_to_qtype(py_type, logger=mod_logger): #get the qtype corresponding to the passed pytype
    """useful for buildign Qt objects
    
    really, this is a reverse 
    
    py_type=str
    
    """
    if not inspect.isclass(py_type):
        logger.error('got unexpected type \'%s\''%type(py_type))
        raise Error('bad type')
    
    #build a QVariant object from this python type class, then return its type
    try:
        qv = QVariant(py_type())
    except:
        logger.error('failed to build QVariant from \'%s\''%type(py_type))
        raise IOError
    
    """
    #get the type
    QMetaType.typeName(qv.type())
    """
    
    
    return qv.type()

def rlay_to_npArray(lyr, dtype=np.dtype(float)): #Input: QgsRasterLayer
    """silently crashing
    
    see hp.gdal.rlay_to_array
    
    """
    assert isinstance(lyr, QgsRasterLayer)
 
    provider= lyr.dataProvider()
    block = provider.block(1,lyr.extent(),lyr.width(),lyr.height())
    
    
    l = list()
    for j in range(lyr.height()):
        l.append([block.value(i,j) for i in range(lyr.width())])

 
    
 
    return np.array(l, dtype=dtype)

 
        
 

#==============================================================================
# type checks-----------------
#==============================================================================

def qisnull(obj):
    if obj is None:
        return True
    
    if isinstance(obj, QVariant):
        if obj.isNull():
            return True
        else:
            return False
        
    
    if pd.isnull(obj):
        return True
    else:
        return False
    
def is_qtype_match(obj, qtype_code, logger=mod_logger): #check if the object matches the qtype code
    log = logger.getChild('is_qtype_match')
    
    #get pythonic type for this code
    try:
        py_type = type_qvar_py_d[qtype_code]
    except:

        if not qtype_code in type_qvar_py_d.keys():
            log.error('passed qtype_code \'%s\' not in dict from \'%s\''%(qtype_code, type(obj)))
            raise IOError
    
    if not isinstance(obj, py_type):
        #log.debug('passed object of type \'%s\' does not match Qvariant.type \'%s\''%(type(obj), QMetaType.typeName(qtype_code)))
        return False
    else:
        return True

def test_install(): #test your qgis install
    
    
    
    proj = Qproj()
    proj._install_info()
    
    
    
    
    
if __name__ == '__main__':

    test_install()
    

    
    print('finished')
    
    
    
    
    
    
    
    