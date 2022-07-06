'''
Running RICorDE from command line    

TODO:
    need a parameter file
 
    switch to data file request/call dictionary
        add metadata per-datafile for this
        add function kwargs
        
    clean up the creation/use of temporary folders
        
    collapse all necessary hp and tcom ricorde into a single hp.py
    re-org folders
        RICorDE
            main #top-level callers
            can_data
            hand #pre-process DEM
            imax #hydrauilc maximum
            mosaic
            hp #all common helpers
            
    parallelize


''' 
import sys, argparse
from ricorde.runrs import runr
 

if __name__ == "__main__":
    print(sys.argv)
    #===========================================================================
    # setup argument parser 
    #===========================================================================
    parser = argparse.ArgumentParser(prog='RICorDE',description='execute RICorDE')
    #add arguments
 
    parser.add_argument("-tag",'-t', help='tag for the run') #this defaults to None if not passed
    parser.add_argument("-write",'-w', help='flag to write outputs', action='store_true')#defaults to False
    
    parser.add_argument("-cap", help='cap to apply to beach values', type=float, default=6.0) #this defaults to None if not passed
    parser.add_argument("-floor", help='cap to apply to beach values', type=float, default=1.5) #this defaults to None if not passed
    parser.add_argument("-dev",'-d', help='flag for dev runs', action='store_true')
    parser.add_argument("-b2_method",  help='method for beach2 construction', default='pixels')
    parser.add_argument("-hgi_resolution",  help='resolution for hgInterp', default=90*4, type=int)
    parser.add_argument("-hgi_minPoints",  help='minimum points count (pts_cnt) for hgInterp', default=5, type=int)
    parser.add_argument("-hval_precision",  help='precision for HAND values in hgSmooth', default=0.2, type=float)
    
    args = parser.parse_args()
    kwargs = vars(args)
    print('parser got these kwargs: \n    %s'%kwargs) #print all the parsed arguments in dictionary form
    
     
    dev = kwargs.pop('dev')
    print('\n\nSTART (dev=%s) \n\n\n\n'%dev)
