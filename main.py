'''
Running RICorDE from command line    



''' 
import sys, argparse
from ricorde.runrs import run_from_params
from hp.basic import get_dict_str
 

if __name__ == "__main__":
    """getting some 'application path not initlizlized?"""
    print(sys.argv)
    #===========================================================================
    # setup argument parser 
    #===========================================================================
    parser = argparse.ArgumentParser(prog='RICorDE',description='Compute a depths grid from flood inundation and DEM')
    #===========================================================================
    # #add arguments
    #===========================================================================
    parser.add_argument("param_fp",help='parameter file path') #positional 
 
    #scripts.Session
    parser.add_argument("-exit_summary", help='flag to disable writing the exit summary', action='store_true')#defaults to False
    
    #hp.Q.Qproj
    parser.add_argument("-compress",'-c', help='set the default raster compression level', 
                        choices=['hiT', 'hi', 'med', 'none'], default='med')
    
    
    #hp.oop.Basic
    parser.add_argument("-root_dir",'-rd', help='Base directory of the project. Used for generating default directories. Defaults to value in definitions', 
                        default=None)  
    parser.add_argument("-out_dir",'-od', 
                        help='Directory used for outputs. Defaults to a sub-directory within root_dir', 
                        default=None) 
    parser.add_argument("-temp_dir", 
                        help='Directory for temporary outputs (i.e., cache). Defaults to a sub-directory of out_dir.', 
                        default=None) 
    parser.add_argument("-tag",'-t', help='tag for the run', default='r0') 
    parser.add_argument("-write",'-w', help='flag to write outputs', action='store_true')#defaults to False
    parser.add_argument("-name",'-n', help='project name', default='proj1')
    parser.add_argument("-prec", help='Default float precision', default=None, type=int)
    parser.add_argument("-overwrite", help='Default behavior when attempting to overwrite a file', action='store_false')
    parser.add_argument("-relative", help='Default behavior of filepaths (relative vs. absolute)', action='store_true')
    
    args = parser.parse_args()
    kwargs = vars(args)
    print('parser got these kwargs: \n    %s'%get_dict_str(kwargs)) #print all the parsed arguments in dictionary form
    
 
    print('\n\nSTART \n\n\n\n')
    #run_from_params(**kwargs)
