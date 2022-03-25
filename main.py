'''
Created on Mar. 27, 2021

@author: cefect

workflows for deriving gridded depth estimates from inundation polygons and DEMs

setup to execute 1 workflow at a time (output 1 depth raster)
    see validate.py for calculating the performance of these outputs
    

TODO:
    merge main runner from InsCrve
    switch to data file request/call dictionary
        add metadata per-datafile for this
        add function kwargs
        
    clean up the creation/use of temporary folders
        
    collapse all necessary hp and tcom ricorde into a single hp.py
    re-org folders
        RICorDE
            main #top-level callers
            can_data
            hand #pre-process DEM
            imax #hydrauilc maximum
            mosaic
            hp #all common helpers
            
    parallelize


'''
#===============================================================================
# imports-----------
#===============================================================================
import os, datetime, copy
 

start =  datetime.datetime.now()
print('start at %s'%start)
 
 
from ricorde.ses import Session, QgsCoordinateReferenceSystem, force_open_dir


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
