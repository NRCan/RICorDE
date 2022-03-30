'''
Created on Mar. 30, 2022

@author: cefect

simple analysis/plotting tools
'''
import os, datetime, copy
start =  datetime.datetime.now()
print('start at %s'%start)

from analysis.scripts import Session

def runr(
        name='someName',
        tag='r1A',
        b2_fp=None,
        **kwargs):
   
    with Session(name=name, tag=tag,
                 root_dir=r'C:\LS\10_OUT\2202_TC',
 
 
                   **kwargs) as wrkr:
        
        if not b2_fp is None:
            wrkr.plot_beach_pts_capped(
                data_fp=b2_fp)
            
        out_dir = wrkr.out_dir
        
    return out_dir

 

def dev():
    
    return runr(
        tag='devA',
        b2_fp = r'C:\LS\10_OUT\2202_TC\outs\dev\20220330\idai_dev_0330_beach2_hvals.csv',
        )


if __name__ =="__main__": 
    
 
    output = dev()
    #output= r1()
    
 
    
    
    #===========================================================================
    # wrap
    #===========================================================================
    #force_open_dir(od)
    tdelta = datetime.datetime.now() - start
    print('finished in %s\n    %s'%(tdelta, output))