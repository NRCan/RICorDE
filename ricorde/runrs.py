'''
Functions for running RICorDE workflows
'''
from ricorde.scripts import Session, QgsCoordinateReferenceSystem


 

def runr(
        tag = 'r1',
        name = 'idai',
        crsid = 'EPSG:32737',
        aoi_fp= r'C:\LS\02_WORK\NRC\2202_TC\04_CALC\aoi\aoi04_0326.gpkg',
        dem_fp = r'C:\LS\10_OUT\2202_TC\ins\dem\merit_0304\MERIT_merge_0304_90x90_aoi04.tif',            
        inun_fp = r'C:\LS\02_WORK\NRC\2202_TC\06_DATA\aer\220307\aer_afed_hilo_acc_3s_20190301-20190331_v05r01_0326_xfed.tif',
        pwb_fp = r'C:\LS\02_WORK\NRC\2202_TC\06_DATA\JRC\JRC_extent_merge_0326_aoi05_clean.tif', #native resolution
 
        compress='med',
        
        #run_dataPrep
        pwb_resampling='Maximum',
        
        #build_b1Bounds: hand value stats for bouding beach1 samples
       qhigh=0.8, cap=6.0,  #uppers               
       qlow=0.2, floor=1.0, #lowers
        
        #build_inun1
        buff_dist=0, #pwb has lots of noise
        
       
        
        #build_beach2
        b2_method='polygons', b2_spacing=90*4, b2_write=True,
        
        #build_hgInterp
        hgi_minPoints=3, searchRad=90*12, hgi_resolution=90*6,
        
        #build_hgSmooth
        hval_precision=0.5,   max_iter=5,
        
        #build_depths
        d_compress='med', 
        
        **kwargs):
    
 
    
    with Session(name=name, tag=tag,
                 root_dir=r'C:\LS\10_OUT\2202_TC',
                 compress=compress,  
                 crs=QgsCoordinateReferenceSystem(crsid),
                   overwrite=True,
                   bk_lib = {
                       'pwb_rlay':dict(resampling=pwb_resampling),
                       'b1Bounds':dict(qhigh=qhigh, cap=cap, qlow=qlow, floor=floor),
                       'inun1':dict(buff_dist=buff_dist),
                       'beach2':dict(method=b2_method, spacing=b2_spacing, write_plotData=b2_write),
                       'hgInterp':dict(pts_cnt=hgi_minPoints, radius=searchRad, resolution=hgi_resolution),
                       'hgSmooth':dict(max_iter=max_iter, precision=hval_precision),
                       'depths':dict(compress=d_compress),

                       },
                   aoi_fp=aoi_fp, dem_fp=dem_fp, inun_fp=inun_fp, pwb_fp=pwb_fp, #filepaths for this project
                   **kwargs) as wrkr:
        
 

        #=======================================================================
        # wrkr.run_dataPrep()
        # wrkr.run_HAND()
        # wrkr.run_imax()
        # wrkr.run_HANDgrid()
        # wrkr.run_wslRoll()
        #=======================================================================
        wrkr.run_depths()
        
        out_dir = wrkr.out_dir
        
    return out_dir

if __name__ == "__main__":
    pass