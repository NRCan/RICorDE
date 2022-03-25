'''
Created on Mar. 25, 2022

@author: cefect

integration tests
'''
import pytest, copy

#===============================================================================
# from ricorde.ses import Session as Session
# from ricorde.ses import force_open_dir, QgsCoordinateReferenceSystem
#===============================================================================

@pytest.mark.parametrize('hval_prec',[0.4]) #feeds through the session (see conftest.py) 
@pytest.mark.parametrize('proj_d',['fred01'], indirect=True) #feeds through the session (see conftest.py) 
def test_one(session, hval_prec):
    
    session.afp_d = copy.copy(session.fp_d)
    #adjust for hydrauilc maximum
    session.run_imax()
    
    #get depths mosaic
    session.run_hdep_mosaic(hval_prec=hval_prec)