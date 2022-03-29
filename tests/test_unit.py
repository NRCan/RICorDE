'''
Created on Mar. 25, 2022

@author: cefect

tests for phase 1: inun max
'''

import pytest, copy, os
from tests.conftest import search_fp, retrieve_data, compare_layers, compare_dicts



@pytest.mark.parametrize('proj_d',['fred02'], indirect=True) #feeds through the session (see conftest.py) 
@pytest.mark.parametrize('resolution',[None, 6]) #feeds through the session (see conftest.py) 
def test_01dem(session, true_dir, resolution):
    
    dkey = 'dem'
    test_rlay = session.retrieve(dkey, resolution=resolution)
    
    assert isinstance(session.dem_psize, int), 'bad type on dem_psize: %s'%type(session.dem_psize)
    
    layer_post(dkey, true_dir, session, test_rlay, test_data=False, test_spatial=(resolution is None))




@pytest.mark.parametrize('proj_d',['fred02'], indirect=True) #feeds through the session (see conftest.py) 
@pytest.mark.parametrize('dem',[r'test_01dem_None_fred02_0\working\test_tag_0328_dem.tif'] ) #from test_pwb
def test_02pwb(session, true_dir, dem, write, base_dir):
    dkey = 'pwb_rlay'
    water_rlay_tests(dkey, session, true_dir, dem, write, base_dir)
    

@pytest.mark.parametrize('proj_d',['fred01','fred02', 'fred03'], indirect=True) #raster and polygon inundations
@pytest.mark.parametrize('dem',[r'test_01dem_None_fred02_0\working\test_tag_0328_dem.tif'] ) #from test_pwb
def test_03inun(session, true_dir, dem, write, base_dir):
    dkey = 'inun_rlay'
    water_rlay_tests(dkey, session, true_dir, dem, write, base_dir)
 

@pytest.mark.parametrize('dem_fp',[r'test_01dem_None_fred02_0\working\test_tag_0328_dem.tif'] ) #from test_pwb
@pytest.mark.parametrize('proj_d',['fred01'], indirect=True) #using the faster setup files
def test_04demHyd(session, true_dir, write, base_dir, dem_fp):
    
    #set the compiled references
    session.compiled_fp_d = {
        'dem':os.path.join(base_dir, dem_fp),
        }
    
    
    dkey = 'dem_hyd'
    test_rlay = session.retrieve(dkey, write=write )

    layer_post(dkey, true_dir, session, test_rlay, test_data=False)


@pytest.mark.parametrize('pwb_rlay',[r'test_02pwb_test_01dem_None_fre0\working\test_tag_0328_pwb_rlay.tif'] ) #from test_pwb
@pytest.mark.parametrize('dem_hyd',[r'test_04demHyd_fred01_test_01de0\working\test_tag_0328_dem_hyd.tif'] ) #from test_pwb
@pytest.mark.parametrize('proj_d',['fred01'], indirect=True) #using the faster setup files
def test_04hand(session, true_dir, pwb_rlay, write, base_dir, dem_hyd):
    
    #set the compiled references
    session.compiled_fp_d = {
        'pwb_rlay':os.path.join(base_dir, pwb_rlay),
        'dem_hyd':os.path.join(base_dir, dem_hyd),
        }
    
    
    dkey = 'HAND'
    test_rlay = session.retrieve(dkey, write=write )

    layer_post(dkey, true_dir, session, test_rlay, test_data=False)
    
    

@pytest.mark.parametrize('hand_fp',[r'test_04hand_fred01_test_04demH0\working\test_tag_0328_HAND.tif'] ) #from test_hand
@pytest.mark.parametrize('proj_d',['fred01'], indirect=True) #feeds through the session (see conftest.py) 
def test_05handMask(session, true_dir, hand_fp, write, base_dir):
    
    #set the compiled reference
    session.compiled_fp_d['HAND'] = os.path.join(base_dir, hand_fp)
    
    dkey = 'HAND_mask'
    test_rlay = session.retrieve(dkey, write=write)

    layer_post(dkey, true_dir, session, test_rlay, test_data=False)
    


@pytest.mark.parametrize('buff_dist',[10] ) #othwerwise the dem needs to be loaded
@pytest.mark.parametrize('handM_fp',[r'test_05handMask_fred01_test_040\working\test_tag_0328_HAND_mask.tif'] ) #from test_hand_mask
@pytest.mark.parametrize('pwb_fp',[r'test_02pwb_test_01dem_None_fre0\working\test_tag_0328_pwb_rlay.tif'] ) #from test_pwb
@pytest.mark.parametrize('inun_fp',[r'test_03inun_test_01dem_None_fr1\working\test_tag_0328_inun_rlay.tif'] )  
@pytest.mark.parametrize('proj_d',['fred01'], indirect=True) #feeds through the session (see conftest.py) 
def test_06inun1(session, true_dir, handM_fp, write, base_dir, buff_dist, pwb_fp, inun_fp):
    
    #set the compiled references
    session.compiled_fp_d = {
        'HAND_mask':os.path.join(base_dir, handM_fp),
        'pwb_rlay':os.path.join(base_dir, pwb_fp),
        'inun_rlay':os.path.join(base_dir, inun_fp),
        }
    
    dkey = 'inun1'
    test_rlay = session.retrieve(dkey, write=write, buff_dist=buff_dist)

    layer_post(dkey, true_dir, session, test_rlay, test_data=False)
    

@pytest.mark.parametrize('hand_fp',[r'test_04hand_fred01_test_04demH0\working\test_tag_0328_HAND.tif'] ) #from test_hand
@pytest.mark.parametrize('inun1',[r'test_06inun1_fred01_test_03inu0\working\test_tag_0328_inun1.tif'] )  
@pytest.mark.parametrize('proj_d',['fred01'], indirect=True) #feeds through the session (see conftest.py) 
def test_07beach1(session, true_dir, write, base_dir, inun1, hand_fp):
    
    #set the compiled references
    session.compiled_fp_d={
        'inun1':os.path.join(base_dir, inun1),
        'HAND':os.path.join(base_dir, hand_fp),
        }
    
    dkey = 'beach1'
    test_rlay = session.retrieve(dkey, write=write)

    layer_post(dkey, true_dir, session, test_rlay, test_data=False)
    

@pytest.mark.parametrize('beach1',[r'test_07beach1_fred01_test_06in0\working\test_tag_0328_beach1.tif'] )  
@pytest.mark.parametrize('proj_d',['fred01'], indirect=True) #feeds through the session (see conftest.py) 
def test_08b1Bounds(session, true_dir, write, base_dir, beach1):
    
    #set the compiled references
    session.compiled_fp_d={
        'beach1':os.path.join(base_dir, beach1),

        }
    
    dkey = 'b1Bounds'
    test_bnds = session.retrieve(dkey, write=write)
    
    #===========================================================================
    # load true
    #===========================================================================
    true_fp = search_fp(os.path.join(true_dir, 'working'), '.pickle', dkey) #find the data file.
    assert os.path.exists(true_fp), 'failed to find match for %s'%dkey
    
    true_bnds = retrieve_data(dkey, true_fp, session)
    
    #===========================================================================
    # check
    #===========================================================================
    compare_dicts(test_bnds, true_bnds)

 
    

@pytest.mark.parametrize('hand_fp',[r'test_04hand_fred01_test_04demH0\working\test_tag_0328_HAND.tif'] ) #from test_hand
@pytest.mark.parametrize('beach1',[r'test_07beach1_fred01_test_06in0\working\test_tag_0328_beach1.tif'] )  
@pytest.mark.parametrize('proj_d',['fred01'], indirect=True) #feeds through the session (see conftest.py) 
def test_08inunHmax(session, true_dir, write, base_dir, beach1, hand_fp):
    
    #set the compiled references
    session.compiled_fp_d={
        'beach1':os.path.join(base_dir, beach1),
        'HAND':os.path.join(base_dir, hand_fp),
        }
    
    dkey = 'inunHmax'
    test_rlay = session.retrieve(dkey, write=write)

    layer_post(dkey, true_dir, session, test_rlay, test_data=False)
    

@pytest.mark.parametrize('inunHmax',[r'test_08inunHmax_fred01_test_070\working\test_tag_0328_inunHmax.tif'] ) 
@pytest.mark.parametrize('inun1',[r'test_06inun1_fred01_test_03inu0\working\test_tag_0328_inun1.tif'] )   
@pytest.mark.parametrize('proj_d',['fred01'], indirect=True) #feeds through the session (see conftest.py) 
def test_09inun2(session, true_dir, write, base_dir, inunHmax, inun1):
    
    #set the compiled references
    session.compiled_fp_d={
        'inunHmax':os.path.join(base_dir, inunHmax),
        'inun1':os.path.join(base_dir, inun1),
        }
    
    dkey = 'inun2'
    test_rlay = session.retrieve(dkey, write=write)

    layer_post(dkey, true_dir, session, test_rlay, test_data=False)
    

@pytest.mark.parametrize('b1Bounds',[r'test_08b1Bounds_fred01_test_070\working\test_tag_0328_b1Bounds.pickle'] ) #from test_hand
@pytest.mark.parametrize('HAND',[r'test_04hand_fred01_test_04demH0\working\test_tag_0328_HAND.tif'] ) #from test_hand
@pytest.mark.parametrize('inun2',[r'test_09inun2_fred01_test_06inu0\working\test_tag_0328_inun2.tif'] )   
@pytest.mark.parametrize('proj_d',['fred01'], indirect=True) #feeds through the session (see conftest.py) 
def test_10beach2(session, true_dir, write, base_dir, HAND, inun2, b1Bounds):
    """todo.. output this as a geojson"""
    
    #set the compiled references
    session.compiled_fp_d={
        'HAND':os.path.join(base_dir, HAND),
        'inun2':os.path.join(base_dir, inun2),
        'b1Bounds':os.path.join(base_dir, b1Bounds),
        }
    
    dkey = 'beach2'
    test_rlay = session.retrieve(dkey, write=write)

    layer_post(dkey, true_dir, session, test_rlay, test_data=False, ext='.gpkg')
    

@pytest.mark.parametrize('radius',[30] )   #speed things up
@pytest.mark.parametrize('pts_cnt',[3] )   #speed things up
@pytest.mark.parametrize('beach2',[r'test_10beach2_fred01_test_09in0\working\test_tag_0328_beach2.gpkg'] ) #from test_hand
@pytest.mark.parametrize('dem',[r'test_01dem_None_fred02_0\working\test_tag_0328_dem.tif'] ) #from test_pwb
@pytest.mark.parametrize('inun2',[r'test_09inun2_fred01_test_06inu0\working\test_tag_0328_inun2.tif'] )   
@pytest.mark.parametrize('proj_d',['fred01'], indirect=True) #feeds through the session (see conftest.py) 
def test_11hgRaw(session, true_dir, write, base_dir, beach2, dem, inun2, pts_cnt, radius):
    """3 parameters were not really testing here"""
    
    #set the compiled references
    session.compiled_fp_d={
        'beach2':os.path.join(base_dir, beach2),
        'dem':os.path.join(base_dir, dem),
        'inun2':os.path.join(base_dir, inun2),
        }
    
    dkey = 'hgRaw'
    test_rlay = session.retrieve(dkey, write=write, pts_cnt=pts_cnt, radius=radius)

    layer_post(dkey, true_dir, session, test_rlay, test_data=False, test_spatial=True)
    
@pytest.mark.dev
@pytest.mark.parametrize('resolution',[8] ) #too slow otherwise
@pytest.mark.parametrize('range_thresh',[2.7] ) #too slow otherwise
@pytest.mark.parametrize('hgRaw',[r'test_11hgRaw_fred01_test_09inu0\working\test_tag_0328_hgRaw.tif'] ) #from test_hand
@pytest.mark.parametrize('proj_d',['fred01'], indirect=True) #feeds through the session (see conftest.py) 
def test_11hgSmooth(session, true_dir, write, base_dir, hgRaw, resolution, range_thresh):
     
    #set the compiled references
    session.compiled_fp_d={
        'hgRaw':os.path.join(base_dir, hgRaw),
  
        }
     
    dkey = 'hgSmooth'
    test_rlay = session.retrieve(dkey, write=write, resolution=resolution, max_iter=5, range_thresh=range_thresh)
 
    layer_post(dkey, true_dir, session, test_rlay, test_data=False)
#===============================================================================
# commons--------
#===============================================================================
def water_rlay_tests(dkey, session, true_dir, dem, write, base_dir):  #common test for inun and pwb
    #set the compiled references
    session.compiled_fp_d.update({
        'dem':os.path.join(base_dir, dem),
        })
    
    
    test_rlay = session.retrieve(dkey, write=write)

    layer_post(dkey, true_dir, session, test_rlay, test_data=False)
    

def layer_post( #post test commons for layer types
        dkey, true_dir, session, test_rlay, ext='.tif', **kwargs):
    #===========================================================================
    # load true
    #===========================================================================
    true_fp = search_fp(os.path.join(true_dir, 'working'), ext, dkey) #find the data file.
    assert os.path.exists(true_fp), 'failed to find match for %s'%dkey
    
    true_rlay = retrieve_data(dkey, true_fp, session)
    
    #===========================================================================
    # compare
    #===========================================================================
    compare_layers(test_rlay, true_rlay, wrkr=session, **kwargs)