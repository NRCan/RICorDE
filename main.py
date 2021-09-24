'''
Created on Mar. 27, 2021

@author: cefect

workflows for deriving gridded depth estimates from inundation polygons and DEMs

setup to execute 1 workflow at a time (output 1 depth raster)
    see validate.py for calculating the performance of these outputs
    




'''
#===============================================================================
# imports-----------
#===============================================================================
import os, datetime, copy
 

start =  datetime.datetime.now()
print('start at %s'%start)
 
 
from scripts.ses import Session, QgsCoordinateReferenceSystem, force_open_dir


#from memory_profiler import profile
#@profile(precision=1)
def run(#main runner
        
        #=======================================================================
        # medium right test aoi
        #=======================================================================
        name='CMMt1',
         aoi_fp=r'C:\LS\02_WORK\02_Mscripts\InsuranceCurves\04_CALC\CMM\aoi\aoi_t1_CMM_20210716.gpkg',
         fp_d = {                  
            },
        
        #=======================================================================
        # common pars
        #=======================================================================
        #prov='quebec',
        
        
        
        #FiC  period of interest
        min_dt=datetime.datetime.strptime('2017-05-07', '%Y-%m-%d'),
        max_dt=datetime.datetime.strptime('2017-05-10', '%Y-%m-%d'),
       
        #NHN kwargs
        waterTypes = ['Watercourse', 'Reservoir'],
        
        #general kwargs
        crsid = 'EPSG:2950',
        dem_resolution=2,
        hval_prec=0.2,
        out_dir=None,
        

        ):
    """
    aoi02_CMM_20210711: 3Gb and 40mins
    """
    

    # setup
    with Session(aoi_fp=aoi_fp, name=name, fp_d=fp_d, crs=QgsCoordinateReferenceSystem(crsid),
                 out_dir=out_dir,compress='med',
                   overwrite=True) as wrkr:

        # load
        ofp_d = wrkr.run_get_data(min_dt=min_dt, max_dt=max_dt, waterTypes=waterTypes,
                                   resolution=dem_resolution,
                                   #prov=prov,
                                   )
        
        #adjust for hydrauilc maximum
        wrkr.run_imax()
        
        #get depths mosaic
        wrkr.run_hdep_mosaic(hval_prec=hval_prec)
        
        out_dir = wrkr.out_dir
        
    return out_dir
        
    
        

def Fred12():
    
    return run(
        name='Fred12',
        aoi_fp=r'C:\LS\02_WORK\02_Mscripts\InsuranceCurves\04_CALC\Fred\aoi\aoi12_fred_0722.gpkg',
        crsid='EPSG:3979',
        
        fp_d={
            'dem_fp':r'C:\LS\03_TOOLS\_jobs\202103_InsCrve\outs\depWf\fred01_0723\hrdem_Fred01_0722_05_fild.tif',
            'fic_fp':r'C:\LS\03_TOOLS\_jobs\202103_InsCrve\outs\depWf\fred01_0723\FiC_Fred01_1x_20180502-20180504_072223.gpkg',
            'nhn_raw_fp':r'C:\LS\03_TOOLS\_jobs\202103_InsCrve\outs\depWf\fred01_0723\NHN_HD_WATERBODY_Fred01_0723_raw.gpkg',
            'nhn_fp':r'C:\LS\03_TOOLS\_jobs\202103_InsCrve\outs\depWf\fred01_0723\NHN_HD_WATERBODY_Fred01_0723_clean.gpkg',
            'hand_fp':r'C:\LS\03_TOOLS\_jobs\202103_InsCrve\outs\depWf\fred01_0723\Fred01_DR_0723_HAND.tif',
            'ndb_fp':r'C:\LS\03_TOOLS\_jobs\202103_InsCrve\outs\depWf\fred01_0723\Fred01_DR_0723_ndb.gpkg',
            'inun1_fp':r'C:\LS\03_TOOLS\_jobs\202103_InsCrve\outs\DR\Fred12_0724\Fred01_DR_0724_inun1.gpkg',
            'smpls1_fp':r'C:\LS\03_TOOLS\_jobs\202103_InsCrve\outs\DR\Fred12_0724\Fred01_DR_0724_smpls1.gpkg',
            'hInun_max_fp':r'C:\LS\03_TOOLS\_jobs\202103_InsCrve\outs\DR\Fred12_0724\Fred01_DR_0724_hrun_imax_700.tif',
            'inun2r_fp':r'C:\LS\03_TOOLS\_jobs\202103_InsCrve\outs\DR\Fred12_0724\Fred01_DR_0724_inun2r.tif',
            'inun2_fp':r'C:\LS\03_TOOLS\_jobs\202103_InsCrve\outs\DR\Fred12_0724\Fred01_DR_0724_inun2.gpkg',
            'hvgrid_fp':r'C:\LS\03_TOOLS\_jobs\202103_InsCrve\outs\DR\Fred12_0724\Fred01_DR_0724_hvgrid.tif',
            'hinun_pick':r'C:\LS\03_TOOLS\_jobs\202103_InsCrve\outs\DR\Fred12_0724\hinun_set.pickle',
            'hwsl_pick':r'C:\LS\03_TOOLS\_jobs\202103_InsCrve\outs\DR\Fred12_0724\hwsl_set.pickle',
            'wslM_fp':r'C:\LS\03_TOOLS\_jobs\202103_InsCrve\outs\DR\Fred12_0724\Fred01_DR_0724_wslM.tif',

            },
        out_dir=r'C:\LS\03_TOOLS\_jobs\202103_InsCrve\outs\DR\Fred12_0724',
        
        
        #FiC  period of interest
        min_dt=datetime.datetime.strptime('2018-05-02', '%Y-%m-%d'),
        max_dt=datetime.datetime.strptime('2018-05-04', '%Y-%m-%d'),
        
        #NHN
        waterTypes=['Unknown','Watercourse'],
        
        #gen
        
        dem_resolution=5,
        hval_prec=0.2,
        
        
        )





if __name__ =="__main__": 
    
 
    #od = CMM2()
    od = Fred12()
    
    #od = Fred12()
    
    
    #===========================================================================
    # wrap
    #===========================================================================
    force_open_dir(od)
    tdelta = datetime.datetime.now() - start
    print('finished in %s'%tdelta)
