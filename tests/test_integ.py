'''
Created on Mar. 25, 2022

@author: cefect

run the full algorhithim (no checks)
    redundant w/ test_runrs (less comprehensive)
'''
import pytest, copy, os 


#@pytest.mark.parametrize('hval_prec',[0.4]) #feeds through the session (see conftest.py) 
@pytest.mark.parametrize('proj_d',['fred01'], indirect=True) #feeds through the session (see conftest.py) 
def test_integ_full(session,true_dir):
    session.write=True #must write for handovers to work
    #===========================================================================
    # algorhithim phases
    #===========================================================================
    session.run_dataPrep()
    
    session.run_imax()
    
    session.run_HANDgrid()
    
    session.run_wslRoll()
    
    session.run_depths()
    
    #===========================================================================
    # compare
    #===========================================================================
    """not implemented
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
        #true = retrieve_data(dkey, true_fp, session)"""
        
