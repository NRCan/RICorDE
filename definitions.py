'''Application wide defaults'''
import os

 
#location of whitebox executable
"""
Change this to match your whitebox exe path
"""
whitebox_exe_d = {

        'v2.1.0':r'C:\LS\06_SOFT\whitebox\v2.1.0\whitebox_tools.exe'
        }

for k,v in whitebox_exe_d.items():
    assert os.path.exists(v), 'got bad whitebox path for %s: \n    %s'%(k,v)

#maximum processors to use
max_procs = 4 

#location of source code
proj_dir = os.path.dirname(os.path.abspath(__file__))

#path to python logging config file
logcfg_file=os.path.join(proj_dir, 'logger.conf')

#root directory for building default directories in
root_dir=r'C:\LS\10_IO\ricorde'
 
