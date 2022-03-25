'''
Created on Mar. 25, 2022

@author: cefect

integration tests
'''
import pytest, copy, os
from tests.conftest import search_fp, retrieve_data
#===============================================================================
# from ricorde.ses import Session as Session
# from ricorde.ses import force_open_dir, QgsCoordinateReferenceSystem
#===============================================================================

@pytest.mark.parametrize('hval_prec',[0.4]) #feeds through the session (see conftest.py) 
@pytest.mark.parametrize('proj_d',['fred01'], indirect=True) #feeds through the session (see conftest.py) 
def test_integ_full(session, hval_prec, true_dir):
    
    session.afp_d = copy.copy(session.fp_d)
    #adjust for hydrauilc maximum
    session.run_imax()
    
    
    
    #get depths mosaic
    session.run_hdep_mosaic(hval_prec=hval_prec)
    
    for dkey, fp in session.afp_d.items():
        if dkey in session.fp_d: continue #ignore loaded data
        #=======================================================================
        # retrieve trues
        #=======================================================================
        #get string components from reslt
        fn = os.path.basename(fp)        
        ext = os.path.splitext(fp)[1]        
        search_str = os.path.splitext(fn)[0].replace(session.layName_pfx, '')
        
        
        true_fp = search_fp(true_dir, ext, search_str) #find the data file.
        assert os.path.exists(true_fp), 'failed to find match for %s'%dkey
        #true = retrieve_data(dkey, true_fp, session)
        