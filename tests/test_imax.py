'''
Created on Mar. 25, 2022

@author: cefect

tests for phase 1: inun max
'''

import pytest, copy, os
from tests.conftest import search_fp, retrieve_data, compare_layers



@pytest.mark.parametrize('proj_d',['fred01'], indirect=True) #feeds through the session (see conftest.py) 
def test_pwb(session, true_dir):
    
    dkey = 'pwb_rlay'
    test_rlay = session.retrieve(dkey)

    #===========================================================================
    # load true
    #===========================================================================
    true_fp = search_fp(os.path.join(true_dir, 'working'), '.tif', dkey) #find the data file.
    assert os.path.exists(true_fp), 'failed to find match for %s'%dkey
    
    true_rlay = retrieve_data(dkey, true_fp, session)
    
    #===========================================================================
    # compare
    #===========================================================================
    compare_layers(test_rlay, true_rlay, test_data=True)
    

@pytest.mark.dev
@pytest.mark.parametrize('pwb_rlay',[r'test_pwb_fred01_0\working\test_tag_0325_pwb_rlay.tif'] ) #from test_pwb
@pytest.mark.parametrize('proj_d',['fred01'], indirect=True) #feeds through the session (see conftest.py) 
def test_hand(session, true_dir, pwb_rlay, write, base_dir):
    
    dkey = 'hand_rlay'
    test_rlay = session.retrieve(dkey, write=write, 
                                 pwb_rlay_fp=os.path.join(base_dir, pwb_rlay))

    #===========================================================================
    # load true
    #===========================================================================
    true_fp = search_fp(os.path.join(true_dir, 'working'), '.tif', dkey) #find the data file.
    assert os.path.exists(true_fp), 'failed to find match for %s'%dkey
    
    true_rlay = retrieve_data(dkey, true_fp, session)
    
    #===========================================================================
    # compare
    #===========================================================================
    compare_layers(test_rlay, true_rlay, test_data=True)