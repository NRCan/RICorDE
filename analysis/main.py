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
        name='Idai',
        tag='r1A',
        compiled_fp_d=dict(),
        **kwargs):
   
    with Session(name=name, tag=tag,
                 root_dir=r'C:\LS\10_OUT\2202_TC',
                 compiled_fp_d=compiled_fp_d,
 
                   **kwargs) as wrkr:
        
 
        wrkr.plot_beach_pts_capped()
            
        out_dir = wrkr.out_dir
        
    return out_dir

 

def dev():
    
    return runr(
        tag='devA',
        compiled_fp_d = {
            'b2Plotting': r'C:\\LS\\10_OUT\\2202_TC\\outs\\dev\\20220330\\idai_dev_0330_beach2_hvals.pickle',
            
            },
 
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