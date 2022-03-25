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
    