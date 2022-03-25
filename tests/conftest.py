'''
Created on Feb. 21, 2022

@author: cefect

copied from 2112_Agg
'''
import os, shutil
import pytest

    
#===============================================================================
# fixture-----
#===============================================================================
 

@pytest.fixture(scope='session')
def logger():
    out_dir = r'C:\LS\10_OUT\2112_Agg\outs\tests'
    if not os.path.exists(out_dir): os.makedirs(out_dir)
    os.chdir(out_dir) #set this to the working directory
    print('working directory set to \"%s\''%os.getcwd())

    from hp.logr import BuildLogr
    lwrkr = BuildLogr()
    return lwrkr.logger

@pytest.fixture(scope='session')
def feedback(logger):
    from hp.Q import MyFeedBackQ
    return MyFeedBackQ(logger=logger)
    
 
    



@pytest.fixture(scope='session')
def base_dir():
    base_dir = r'C:\LS\09_REPOS\02_JOBS\2112_Agg\cef\tests\hyd\data\compiled'
    assert os.path.exists(base_dir)
    return base_dir



@pytest.fixture
def true_dir(write, tmp_path, base_dir):
    true_dir = os.path.join(base_dir, os.path.basename(tmp_path))
    if write:
        if os.path.exists(true_dir): 
            shutil.rmtree(true_dir)
            os.makedirs(true_dir) #add back an empty folder
            
    return true_dir
    
    
