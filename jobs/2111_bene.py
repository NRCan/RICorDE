#===============================================================================
# '''
# Created on Sep. 24, 2021
# 
# @author: cefect
# 
# RICorDE runner for
#     C:\LS\02_WORK\NRC\202111_Benedikt
#===============================================================================


import datetime, os, copy

start =  datetime.datetime.now()
print('start at %s'%start)


 
from scripts.ses import Session as Session
from scripts.ses import force_open_dir, QgsCoordinateReferenceSystem

work_dir= r'C:\LS\03_TOOLS\RICorDE'


def run(#main runner
        name='2111_Bene',
         aoi_fp=None,
         #fic_fps=[], #set of fics
         fp_d = {},
 
        
        #general kwargs
        crsid = 'EPSG:32737',
        dem_resolution=100,
        hval_prec=0.4,
        out_dir=None,
        

        ):
    """
    aoi02_CMM_20210711: 3Gb and 40mins
    """
    

    #=======================================================================
    # prelim FiC work
    #=======================================================================
    """this analysis is keyed by FiC filename"""
    #===========================================================================
    # from data_collect.fic_composite import ficSession 
    # 
    # with ficSession(out_dir=out_dir, name=name, crs=QgsCoordinateReferenceSystem(crsid),
    #                 work_dir=work_dir,
    #                 ) as wrkr:
    #     #FiC composite
    #     if not 'fic_fp' in fp_d:
    #         fic_fp = wrkr.merge_fics(fic_fps, reproject=True)
    #         fp_d['fic_fp'] = fic_fp
    #     else:
    #         fic_fp = fp_d['fic_fp']
    #         
    #     #aoi
    #     if aoi_fp is None:
    #         #get from FiC
    #         aoi_fp = wrkr.get_aoi(fic_fp, clip_fp=hrdem_cov_fp)
    #===========================================================================
            

    #===========================================================================
    # RICorDE
    #===========================================================================
    with Session(aoi_fp=aoi_fp, name=name, fp_d=fp_d, crs=QgsCoordinateReferenceSystem(crsid),
                 out_dir=out_dir,compress='med', work_dir=work_dir,
                   overwrite=True) as wrkr:
        
        wrkr.load_hrdem(resolution=dem_resolution)
        wrkr.afp_d = copy.copy(fp_d)
        # load
 
        
        #adjust for hydrauilc maximum
        wrkr.run_imax()
        
        #get depths mosaic
        wrkr.run_hdep_mosaic(hval_prec=hval_prec)
        
        out_dir = wrkr.out_dir
        
    return out_dir

def mozam():
    
    return run(
         name='mozam',
         
         aoi_fp = r'C:\LS\02_WORK\NRC\202111_Benedikt\aoi01.gpkg',#if None: built from Fic composite
         #======================================================================
         # fic_fps = [
         #     r'C:\LS\02_WORK\NRCan\202112_RICorDE\06_DATA\FiC\20211201\prod\FiC_20211201_aoi02_1202.gpkg',
         #     ],
         #======================================================================
 
          fp_d = {
              'fic_fp':r'C:\LS\02_WORK\NRC\202111_Benedikt\data\DFO_clean_1124_aoi01.gpkg',
              'dem_fp': r'C:\LS\02_WORK\NRC\202111_Benedikt\data\MERIT_DEM_aoi01_1202.tif',    
              
              #manipulated into polygons (needed by build_inun1)
              'nhn_fp':r'C:\LS\02_WORK\NRC\202111_Benedikt\data\JRC_water_extent_1202_aoi01.gpkg',
              
              #thisis the raw from JRC
              'nhn_rlay_fp':r'C:\LS\02_WORK\NRC\202111_Benedikt\data\JRC_water_extent_1202_aoi01_100.tif',
              
              
              'hand_fp':r'C:\LS\03_TOOLS\RICorDE\outs\2111_bene\1202\mozam_DR_1202_HAND.tif',
            'ndb_fp':r'C:\LS\03_TOOLS\RICorDE\outs\2111_bene\1202\mozam_DR_1202_ndb.gpkg',
            'inun1_fp':r'C:\LS\03_TOOLS\RICorDE\outs\2111_bene\1202\mozam_DR_1202_inun1.gpkg',
            'smpls1_fp':r'C:\LS\03_TOOLS\RICorDE\outs\2111_bene\1202\mozam_DR_1202_smpls1.gpkg',
            'hInun_max_fp':r'C:\LS\03_TOOLS\RICorDE\outs\2111_bene\1202\mozam_DR_1202_hrun_imax_700.tif',
            'inun2r_fp':r'C:\LS\03_TOOLS\RICorDE\outs\2111_bene\1202\mozam_DR_1202_inun2r.tif',
            'inun2_fp':r'C:\LS\03_TOOLS\RICorDE\outs\2111_bene\1202\mozam_DR_1202_inun2.gpkg',
            'smpts2_fp':r'C:\LS\03_TOOLS\RICorDE\outs\2111_bene\1202\mozam_DR_1202_smpts2.gpkg',
            'smpls2_fp':r'C:\LS\03_TOOLS\RICorDE\outs\2111_bene\1202\mozam_DR_1202_smpls2.gpkg',
            'smpls2C_fp':r'C:\LS\03_TOOLS\RICorDE\outs\2111_bene\1202\mozam_DR_1202_smplsC.gpkg',


              },
          

        #general kwargs
 
        dem_resolution=100,
        hval_prec=0.4,
        
          
        out_dir=r'C:\LS\03_TOOLS\RICorDE\outs\2111_bene\1202',
        )
 

if __name__ =="__main__": 
    
 
    #od = King2019()
    #CO2019()
    #LSJ2019n()
    
    #GM2017()
    mozam()
    
 
    
    #===========================================================================
    # wrap
    #===========================================================================
 
    tdelta = datetime.datetime.now() - start
    print('finished in %s'%tdelta)
    