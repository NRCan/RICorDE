'''
Created on Mar. 25, 2022

@author: cefect

tests for phase 1: inun max
'''

import pytest, copy, os
from tests.conftest import search_fp, retrieve_data, compare_layers



@pytest.mark.parametrize('proj_d',['fred02'], indirect=True) #feeds through the session (see conftest.py) 
@pytest.mark.parametrize('dem_psize',[None, 6]) #feeds through the session (see conftest.py) 
def test_01dem(session, true_dir, dem_psize):
    
    dkey = 'dem'
    test_rlay = session.retrieve(dkey, dem_psize=dem_psize)
    
    assert isinstance(session.dem_psize, int), 'bad type on dem_psize: %s'%type(session.dem_psize)
    
    rlay_compare(dkey, true_dir, session, test_rlay, test_data=False)




@pytest.mark.parametrize('proj_d',['fred02'], indirect=True) #feeds through the session (see conftest.py) 
@pytest.mark.parametrize('dem',[r'test_01dem_None_fred02_0\working\test_tag_0326_2x2_dem.tif'] ) #from test_pwb
def test_02pwb(session, true_dir, dem, write, base_dir):
    dkey = 'pwb_rlay'
    water_rlay_tests(dkey, session, true_dir, dem, write, base_dir)
    
 
@pytest.mark.parametrize('proj_d',['fred02'], indirect=True) #feeds through the session (see conftest.py) 
@pytest.mark.parametrize('dem',[r'test_01dem_None_fred02_0\working\test_tag_0326_2x2_dem.tif'] ) #from test_pwb
def test_03inun(session, true_dir, dem, write, base_dir):
    dkey = 'inun_rlay'
    water_rlay_tests(dkey, session, true_dir, dem, write, base_dir)
 

    

@pytest.mark.dev
@pytest.mark.parametrize('pwb_rlay',[r'test_02pwb_test_01dem_None_fre0\working\test_tag_0326_pwb_rlay.tif'] ) #from test_pwb
@pytest.mark.parametrize('proj_d',['fred01'], indirect=True) #using the faster setup files
def test_04hand(session, true_dir, pwb_rlay, write, base_dir):
    
    #set the compiled references
    session.compiled_fp_d.update({
        'pwb_rlay':os.path.join(base_dir, pwb_rlay),
        })
    
    
    dkey = 'HAND'
    test_rlay = session.retrieve(dkey, write=write )

    rlay_compare(dkey, true_dir, session, test_rlay, test_data=False)
    
    

@pytest.mark.parametrize('hand_fp',[r'test_04hand_fred01_test_02pwb_0\working\test_tag_0326_HAND.tif'] ) #from test_hand
@pytest.mark.parametrize('proj_d',['fred01'], indirect=True) #feeds through the session (see conftest.py) 
def test_05handMask(session, true_dir, hand_fp, write, base_dir):
    
    #set the compiled reference
    session.compiled_fp_d['HAND'] = os.path.join(base_dir, hand_fp)
    
    dkey = 'HAND_mask'
    test_rlay = session.retrieve(dkey, write=write)

    rlay_compare(dkey, true_dir, session, test_rlay, test_data=False)
    

 
@pytest.mark.parametrize('buff_dist',[10] ) #othwerwise the dem needs to be loaded
@pytest.mark.parametrize('handM_fp',[r'test_05handMask_fred01_test_040\working\test_tag_0326_HAND_mask.tif'] ) #from test_hand_mask
@pytest.mark.parametrize('proj_d',['fred01'], indirect=True) #feeds through the session (see conftest.py) 
def test_06inun1(session, true_dir, handM_fp, write, base_dir, buff_dist):
    
    #set the compiled references
    session.compiled_fp_d.update({
        'HAND_mask':os.path.join(base_dir, handM_fp)
        })
    
    dkey = 'inun1'
    test_rlay = session.retrieve(dkey, write=write, buff_dist=buff_dist)

    rlay_compare(dkey, true_dir, session, test_rlay, test_data=False)
    

@pytest.mark.parametrize('sample_spacing',[10] ) #othwerwise the dem needs to be loaded
@pytest.mark.parametrize('handM_fp',[r'test_05handMask_fred01_test_040\working\test_tag_0326_HAND_mask.tif'] ) #from test_hand_mask
@pytest.mark.parametrize('proj_d',['fred01'], indirect=True) #feeds through the session (see conftest.py) 
def test_07isamp1(session, true_dir, handM_fp, write, base_dir, sample_spacing):
    
    #set the compiled references
    session.compiled_fp_d.update({
        'HAND_mask':os.path.join(base_dir, handM_fp)
        })
    
    dkey = 'inun1'
    test_rlay = session.retrieve(dkey, write=write, buff_dist=buff_dist)

    rlay_compare(dkey, true_dir, session, test_rlay, test_data=False)
    
#===============================================================================
# commons--------
#===============================================================================
def water_rlay_tests(dkey, session, true_dir, dem, write, base_dir):  #common test for inun and pwb
    #set the compiled references
    session.compiled_fp_d.update({
        'dem':os.path.join(base_dir, dem),
        })
    
    
    test_rlay = session.retrieve(dkey, write=write)

    rlay_compare(dkey, true_dir, session, test_rlay, test_data=False)
    

def rlay_compare(dkey, true_dir, session, test_rlay, test_data=False):
    #===========================================================================
    # load true
    #===========================================================================
    true_fp = search_fp(os.path.join(true_dir, 'working'), '.tif', dkey) #find the data file.
    assert os.path.exists(true_fp), 'failed to find match for %s'%dkey
    
    true_rlay = retrieve_data(dkey, true_fp, session)
    
    #===========================================================================
    # compare
    #===========================================================================
    compare_layers(test_rlay, true_rlay, test_data=test_data)