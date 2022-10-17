'''
Factory methods for Whitebox tools

Notes
---------------
    would be nicer to use the project python libraries
        but these require tkinter
    could be nicer to use the QGIS processing algos
        but I can't get these to setup
'''

import subprocess, os, logging

from hp.dirz import get_temp_dir
from hp.gdal import get_nodata_val
from definitions import whitebox_exe_d, max_procs
#===============================================================================
# classes------
#===============================================================================


class Whitebox(object):
    
    def __init__(self,
                 out_dir=None,
                 logger=None,
                 overwrite=True,
                 version='v2.1.0',
 
                 max_procs=max_procs, 
                 ):
        """
        worker for whitebox_tools.exe
        
        Parameters
        -----------
        version: str
            which file path to use from definitions.py. 
            because some tools are broken on newer versions
        
        """
        
        self.exe_d=whitebox_exe_d
        if out_dir is None: out_dir = get_temp_dir()
        if logger is None:
            logger = logging.getLogger(__name__)
        self.out_dir=out_dir
        self.logger=logger.getChild('wbt')
        self.overwrite =overwrite
        self.exe_fp=self.exe_d[version]
        self.max_procs=max_procs

        assert os.path.exists(self.exe_fp), 'bad exe: \n    %s'%self.exe_fp
        
    def breachDepressionsLeastCost(self,
                                   dem_fp, #file path to fill. MUST BE UNCOMPRESSED!
                                   dist=100, #(Maximum search distance for breach paths in cells) pixel distance to fill
                                   ofp = None, #outpath
                                   logger=None,
        
                                   ):
        """can be very slow"""
        #=======================================================================
        # defaults
        #=======================================================================
        tool_nm = 'BreachDepressionsLeastCost'
        if logger is None: logger=self.logger
        log=logger.getChild(tool_nm)
        
        if ofp is None: 
            ofp = os.path.join(self.out_dir, os.path.splitext(os.path.basename(dem_fp))[0]+'_hyd.tif')
            
        if os.path.exists(ofp):
            assert self.overwrite
            os.remove(ofp)
        #=======================================================================
        # configure        
        #=======================================================================
        args = [self.exe_fp,
                '--run={}'.format(tool_nm),
                '--dem=\'{}\''.format(dem_fp),
                '--dist=%i'%dist,
                '--min_dist=\'True\'', #Optional flag indicating whether to minimize breach distances
                '--fill=\'True\'', # fill any remaining unbreached depressions
                #'--compress_raster=\'False\'',
                '--output=\'{}\''.format(ofp),

                '-v'
                ]
        
        log.info('executing \'%s\' on \'%s\''%(tool_nm, os.path.basename(dem_fp)))
        #log.debug(args)
        #subprocess.Popen(args)
        #=======================================================================
        # execute
        #=======================================================================
        self.__run__(args) #execute
        #=======================================================================
        # result = subprocess.run(args, #spawn process in explorer
        #                         capture_output=True,text=True,
        #                         #stderr=sys.STDOUT, stdout=PIPE,
        #                         ) 
        #=======================================================================
        
        return ofp
    
    def elevationAboveStream(self,
                             dem_fp,
                             streams_fp,
                             out_fp=None,
                                  logger=None):

        #=======================================================================
        # defaults
        #=======================================================================
        tool_nm = 'ElevationAboveStream'
        if logger is None: logger=self.logger
        log=logger.getChild(tool_nm)
        
        if out_fp is None: 
            out_fp = os.path.join(self.out_dir, os.path.splitext(os.path.basename(dem_fp))[0]+'_HAND.tif')
        
        assert out_fp.endswith('.tif')
 
        #=======================================================================
        # setup
        #=======================================================================
        args = [self.exe_fp,'-v','--run={}'.format(tool_nm),'--output={}'.format(out_fp),
                '--dem={}'.format(dem_fp),
                '--streams={}'.format(streams_fp),
                ]
        
        #=======================================================================
        # execute
        #=======================================================================
        log.info('executing \'%s\' on \'%s\''%(tool_nm, os.path.basename(dem_fp)))
        self.__run__(args) #execute
        
        return out_fp
    
    def fillMissingData(self,
                        rlay_fp,
                        dist=11, #pixel length to infil
                        logger=None, out_fp=None,
                        ):

        #=======================================================================
        # defaults
        #=======================================================================
        tool_nm = 'FillMissingData'
        if logger is None: logger=self.logger
        log=logger.getChild(tool_nm)
        
        if out_fp is None: 
            out_fp = os.path.join(self.out_dir, os.path.splitext(os.path.basename(rlay_fp))[0]+'_fild.tif')
        
        #=======================================================================
        # checks
        #=======================================================================
        if os.path.exists(out_fp):
            assert self.overwrite
            os.remove(out_fp)
        assert out_fp.endswith('.tif')
        
        nan_val = get_nodata_val(rlay_fp)
        assert nan_val==-9999,'got unsupported nodata val'
 
        #=======================================================================
        # setup
        #=======================================================================
        args = [self.exe_fp,'-v','--run={}'.format(tool_nm),'--output={}'.format(out_fp),
                '--input={}'.format(rlay_fp),
                '--filter=%i'%dist,
                '--weight=2.0',
                '--no_edges=\'True\'',
                ]
        
        #=======================================================================
        # execute
        #=======================================================================
        log.info('executing \'%s\' on \'%s\''%(tool_nm, os.path.basename(rlay_fp)))
        self.__run__(args) #execute
        
        return out_fp
    
    def NewRasterFromBase(self,
                        rlay_fp,
                        value=1.0, #constant value to burn
                        data_type='float',
                        logger=None, out_fp=None,
                        ):

        #=======================================================================
        # defaults
        #=======================================================================
        tool_nm = 'NewRasterFromBase'
        if logger is None: logger=self.logger
        log=logger.getChild(tool_nm)
        
        if out_fp is None: 
            out_fp = os.path.join(self.out_dir, os.path.splitext(os.path.basename(rlay_fp))[0]+'_burn.tif')
        
        #=======================================================================
        # checks
        #=======================================================================
        assert os.path.exists(rlay_fp), rlay_fp
        if os.path.exists(out_fp):
            assert self.overwrite
            os.remove(out_fp)
        assert out_fp.endswith('.tif')
        
        """seems to be working
        nan_val = get_nodata_val(rlay_fp)
        assert nan_val==-9999,'got unsupported nodata val: \'%s\''%nan_val"""
 
        #=======================================================================
        # setup
        #=======================================================================
        args = [self.exe_fp,'-v','--run={}'.format(tool_nm),'--output={}'.format(out_fp),
                '--input={}'.format(rlay_fp),
                '--value={}'.format(value),
                '--data_type={}'.format(data_type),
                ]
        
        #=======================================================================
        # execute
        #=======================================================================
        log.debug('executing \'%s\' on \'%s\''%(tool_nm, os.path.basename(rlay_fp)))
        self.__run__(args) #execute
        
        return out_fp
    
    def IdwInterpolation(self,
                        vlay_pts_fp, fieldn, 
                        
                        #parameters
                        weight=2, #IDW weight value
                        radius=4.0, #Search Radius in map units
                        min_points=3, #Minimum number of points
                        
                        #output data props
                        cell_size=None, #resolution of output
                        ref_lay_fp=None, #optional reference layer (if no cell_size is specifed)
                        
                        #gen 
                        logger=None, out_fp=None,
                        ):

        #=======================================================================
        # defaults
        #=======================================================================
        tool_nm = 'IdwInterpolation'
        if logger is None: logger=self.logger
        log=logger.getChild(tool_nm)
        
        if out_fp is None: 
            out_fp = os.path.join(self.out_dir, os.path.splitext(os.path.basename(vlay_pts_fp))[0]+'_idw.tif')
        
        assert out_fp.endswith('.tif')
        
        assert os.path.exists(vlay_pts_fp)
        
        assert vlay_pts_fp.endswith('.shp'), 'only shapefiles allowed'

        #=======================================================================
        # setup
        #=======================================================================
        args = [self.exe_fp,'-v','--run={}'.format(tool_nm),'--output={}'.format(out_fp),
                '-i={}'.format(vlay_pts_fp),
                '--field=%s'%fieldn,
                '--weight=%.2f'%weight,
                '--radius=%.2f'%radius,
                '--min_points=%i'%min_points,
                '--max_procs=%i'%self.max_procs,
                ]
        
        if ref_lay_fp is None:
            assert isinstance(cell_size, int)
            args.append('--cell_size=%i'%cell_size)
        else:
            assert cell_size is None
            assert os.path.exists(ref_lay_fp)
            assert ref_lay_fp.endswith('.tif')
            args.append('--base=%s'%ref_lay_fp)
        
        #=======================================================================
        # execute
        #=======================================================================
        log.info('executing \'%s\' on \'%s\''%(tool_nm, os.path.basename(vlay_pts_fp)))
        self.__run__(args) #execute
        
        return out_fp
    
    def costDistance(self, #cost-distance or least-cost pathway analyses
                       source_fp='', #source raster null=no source
                        # (e.g., clipped DEM). 
                       cost_fp='', #cost raster. nulls will be null in output. 
                            #(e.g. mask of all 1s for neutral cost)
                        logger=None, ofp=None,
                     
                     ):
        
        #=======================================================================
        # defaults
        #=======================================================================
        tool_nm='CostDistance'
        if logger is None: logger=self.logger
        log=logger.getChild(tool_nm)
        
        #filepathjs
        if ofp is None:
            ofp = os.path.join(self.out_dir, 
                               os.path.splitext(os.path.basename(source_fp))[0]+'_costAccum.tif')
        
        if os.path.exists(ofp):
            assert self.overwrite
            os.remove(ofp)
            
        #filepath for backlink result
        ofp2 = os.path.join(self.out_dir, 
                               os.path.splitext(os.path.basename(source_fp))[0]+'_costBlink.tif')
            
        #=======================================================================
        # setup
        #=======================================================================
        args = [self.exe_fp,'-v','--run={}'.format(tool_nm),
                '--out_accum={}'.format(ofp),
                '--out_backlink={}'.format(ofp2),
                '--source={}'.format(source_fp),
                '--cost={}'.format(cost_fp),
                ]
        
        #=======================================================================
        # execute
        #=======================================================================
        log.debug('executing \'%s\' on \'%s\''%(tool_nm, os.path.basename(source_fp)))
        self.__run__(args) #execute
        
        return ofp, ofp2
    
    def costAllocation(self,
                       source_fp='', #source raster null=no source
                        # (e.g., clipped DEM). 
                        blink_fp='', #backlink raster (generally from  costDistance())
                        logger=None, ofp=None,
                        ):

        #=======================================================================
        # defaults
        #=======================================================================
        tool_nm='CostAllocation'
        if logger is None: logger=self.logger
        log=logger.getChild(tool_nm)
        
        #filepathjs
        if ofp is None:
            ofp = os.path.join(self.out_dir, 
                               os.path.splitext(os.path.basename(source_fp))[0]+'_costAlloc.tif')
        
        if os.path.exists(ofp):
            assert self.overwrite
            os.remove(ofp)

        #=======================================================================
        # setup
        #=======================================================================
        args = [self.exe_fp,'-v','--run={}'.format(tool_nm),
                '--backlink={}'.format(blink_fp),
                '--output={}'.format(ofp),
                '--source={}'.format(source_fp),
                ]
        
        #=======================================================================
        # execute
        #=======================================================================
        log.debug('executing \'%s\' on \'%s\''%(tool_nm, os.path.basename(source_fp)))
        self.__run__(args) #execute
        
        return ofp
        
    def __run__(self, args, logger=None):
        """I think this only returns info upon completion?
        check the verbose flag also"""
        #=======================================================================
        # setup
        #=======================================================================
        if logger is None: logger=self.logger
        log = logger.getChild('r')
        log.debug('executing w/ arge \n    %s'%args)
        
        #=======================================================================
        # execute
        #=======================================================================
        result = subprocess.run(args,capture_output=True,text=True,)
        
        #=======================================================================
        # #handle result
        #=======================================================================

        log.debug('finished w/ returncode=%i \n    %s'%(result.returncode, result.stdout))
        
        if not result.returncode==0: 
            self.logger.error('failed w/ \n    %s'%result.stderr)
            
        result.check_returncode()
            
        return result


if __name__ == '__main__':
    dem_fp = r'C:\LS\03_TOOLS\_jobs\202103_InsCrve\outs\HAND\HRDEM_cilp2.tif'
    result = Whitebox().breachDepressionsLeastCost(dem_fp)
    
    print('finished')

