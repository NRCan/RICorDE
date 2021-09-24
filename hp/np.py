'''
Created on Mar 5, 2019

@author: cef

np.version.version

py2.7
'''
#===============================================================================
# # imports --------------------------------------------------------------------
#===============================================================================
import numpy as np
import pandas as pd

import os, logging, shutil, random, re, copy


from hp.exceptions import Error


#===============================================================================
# # # logger config ------------------------------------------------------------
#===============================================================================
if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG) #lower the logging level for debug runs
    
mod_logger = logging.getLogger(__name__)
mod_logger.debug('initialized')







    
    
#===============================================================================
# IN/OUT --------------------------------------------------------------------
#===============================================================================

def write_to_csv(filepath,
                 data,
                 logger=mod_logger):
    
    log = logger.getChild('write_to_csv')
    
    if isinstance(data, dict):
        raise IOError
        data_raw = copy.copy(data)
        data = np.array(list(data_raw.items()))
        
        names = ['id','data']
        formats = ['f8','f8']
        dtype = dict(names = names, formats=formats)
        
        array = np.fromiter(data_raw.items(),dtype=dtype, count=len(data_raw))
        
    
    np.savetxt(filepath, data, delimiter=',')
    
    log.info('saved data to filepath: \n       %s'%filepath)

def csv_to_dict( #load two columns from a csv to a dictionary
                fpath,  #filepath to csv
                kind = 0, #key indexer (column)
                typeind = 2, #index to find type 
                vind = 1, # value column indexer
                delimiter = '\t', #\t: tab
                skip_header = 1,
                logger=mod_logger): 
    """
    expects the second column to contain the type code for the data in that row
    
    
    There must be a better way to handle the type setting
    
    WARNING:
    req
    
    """
    
    #===========================================================================
    # prechecks
    #===========================================================================
    if not fpath.endswith(('.txt','.csv')):
        raise IOError

    logger.debug('loading from: \n %s'%fpath)
    #load the csv where everything is a string
    array = np.genfromtxt(fpath, delimiter=delimiter, dtype=np.object, skip_header=skip_header).astype(str)
    'may expect values on each row?'
    
    logger.debug('converting passed array to dict: \n %s'%array)
    
    ins_d = dict()
    
    if len(array.shape) < 2:
        logger.error('need at least 2 rows in teh data file')
        raise IOError
    
    for row in array:
        typeask = row[typeind]
        
        try:
            typef = eval(typeask) #get the type class
        except:
            raise IOError
        
        k = row[kind] #set teh key
        vraw = row[vind]
        
        #get the value and format it
        if typeask in ['str', 'int', 'float']:
            
            v = typef(vraw) #use it to set the value
        elif typeask in ['list', 'dict']:
            v = eval(vraw)
            
            """
        if 'str' in mytype:
            v = row[vind]
        elif 'int' in mytype:
            v = int(row[vind])
        elif 'float' in mytype:
            v = float(row[vind])
            
        elif 'list':
            v = dgap(row[vind])
        elif 'dict':
            v = dgap(row[vind])"""
        else:
            logger.error('got unexpected value for mytype: \'%s\''%typeask)
            raise TypeError
        
        if not isinstance(v, typef):
            raise IOError
        
        #set the entry
        ins_d[k] = v
        
    logger.debug('built dictionary with %i entries'%len(ins_d))
        
    return ins_d

#===============================================================================
# TYPE CONVERSION ------------------------------------------------------------------
#===============================================================================

def build_type_conv(): #build a numpy character to python type conversion dictionary
    """
    THIS IS BREAKING SOMETHING
    as thsi si system specific, I generally load at module init
    """
    
    d = dict() 

    for name in dir(np):
        obj = getattr(np, name)
        if hasattr(obj, 'dtype'):
            try:
                if 'time' in name:
                    npn = obj(0, 'D')
                else:
                    npn = obj(0)
                nat = npn.item()
                d[npn.dtype.char] = type(nat)
    
            except:
                pass
            
    mod_logger.info('built numpy type conversion dictionary with %i types'%len(d))
            
    return d

def np_to_pytype(npdobj, logger=mod_logger):
    
    if not isinstance(npdobj, np.dtype):
        raise Error('not passed a numpy type')
    
    try:
        return npc_pytype_d[npdobj.char]

    except Exception as e:
        log = logger.getChild('np_to_pytype')
        
        if not npdobj.char in npc_pytype_d.keys():
            log.error('passed npdtype \'%s\' not found in the conversion dictionary'%npdobj.name)
            
        raise Error('failed oto convert w/ \n    %s'%e)

def typeset_fail_bool( #identify where in the data the typeset is failing
        in_ar,
        typeset_str, 
        logger=mod_logger):
    """
    seems like there should be a built in function to do this
    """
    
    log = logger.getChild('typeset_fail_bool')
    
    typef = eval(typeset_str)
     
    boolar = np.full(len(in_ar),False, dtype=bool) #all False
    
    for loc, val in enumerate(in_ar):
        try: 
            _ = typef(val)
        except:
            boolar[loc] = True #flag this one as failing
            
    log.debug('idnetified %i (of %i) entries as failing to typeset from \'%s\''
              %(boolar.sum(), len(in_ar), typeset_str))
    
    return boolar

        
    """
    len(boolar)
    boolar.sum()
    """
    
def left_in_right( #fancy check if left elements are in right elements
        ldata_raw, rdata_raw, 
                  lname_raw = 'left',
                  rname_raw = 'right',
                  sort_values = False, #whether to sort the elements prior to checking
                  result_type = 'bool', #format to return result in
                    #missing: return a list of left elements not in the right
                    #matching: list of elements in both
                    #boolar: return boolean where True = left element found in right (np.isin)
                    #bool: return True if all left elements are found on the right
                    #exact: return True if perfect element match
                  invert = False, #flip left and right
                  
                  #expectations
                  dims= 1, #expected dimeions
                  
                  fancy_log = False, #whether to log stuff                  
                  logger=mod_logger
                  ):
    
    #===========================================================================
    # precheck
    #===========================================================================
    if isinstance(ldata_raw, str):
        raise Error('expected array type')
    if isinstance(rdata_raw, str):
        raise Error('expected array type')
    
    #===========================================================================
    # do flipping
    #===========================================================================
    if invert:
        ldata = rdata_raw
        lname = rname_raw
        rdata = ldata_raw
        rname = lname_raw
    else:
        ldata = ldata_raw
        lname = lname_raw
        rdata = rdata_raw
        rname = rname_raw
        
        
    #===========================================================================
    # data conversion
    #===========================================================================
    if not isinstance(ldata, np.ndarray):
        l_ar = np.array(list(ldata))
    else:
        l_ar = ldata
        
    if not isinstance(rdata, np.ndarray):
        r_ar = np.array(list(rdata))
    else:
        r_ar = rdata
        
    #===========================================================================
    # do any sorting
    #===========================================================================
    if sort_values:
        l_ar = np.sort(l_ar)
        r_ar = np.sort(r_ar)
        
        #check logic validty of result type
        if result_type =='boolar':
            raise Error('requested result type does not make sense with sorted=True')

        
    #===========================================================================
    # pre check
    #===========================================================================
    #check for empty containers and uniqueness
    for data, dname in (
        (l_ar, lname),
        (r_ar, rname)
        ):
        #empty container
        if data.size == 0:
            raise Error('got empty container for \'%s\''%dname)
        
        #simensions/shape
        """probably not necessary"""
        if not len(data.shape) == dims:
            raise Error('expected %i dimensions got %s'%(
                dims, str(data.shape)))
            
        
        if not pd.Series(data).is_unique:
            #get detailed print outs
            ser = pd.Series(data)
            boolidx = ser.duplicated(keep=False)            
            
            raise Error('got %i (of %i) non-unique elements for \'%s\' \n    %s'%(
                boolidx.sum(), len(boolidx), dname, ser[boolidx]))
        
        #=======================================================================
        # #uniqueness
        # if not data.size == np.unique(data).size:
        #     raise Error('got non-unique elements for \'%s\' \n    %s'%(dname, data))
        #=======================================================================
        
        """data
        data.shape
        
        """
        

    

    #===========================================================================
    # do the chekcing
    #===========================================================================

    boolar = ~np.isin(l_ar, r_ar) #misses from left to right
    
    if fancy_log:
        
        log = logger.getChild('left_in_right')
        msg = ('%i (of %i) elements in \'%s\'  not found in \'%s\': \n    mismatch: %s \n    \'%s\' %s: %s \n    \'%s\' %s: %s'
                    %(boolar.sum(),len(boolar), lname, rname, 
                      l_ar[boolar].tolist(),
                      lname, str(l_ar.shape), l_ar.tolist(), 
                      rname, str(r_ar.shape), r_ar.tolist()
                      )
                    )
        if np.any(boolar):
            logger.debug(msg)
        elif result_type=='exact' and (not np.array_equal(l_ar, r_ar)):
            logger.debug(msg)
        
    #===========================================================================
    # reformat and return result
    #===========================================================================
    if result_type == 'boolar': #left elements in the right
        return ~boolar
    elif result_type == 'bool': #all left elements in the right
        if np.any(boolar):
            return False
        else:
            return True
        
    elif result_type == 'missing':
        return l_ar[boolar].tolist()
    
    elif result_type == 'matching':
        return l_ar[~boolar].tolist()
    
    elif result_type == 'exact':
        return np.array_equal(l_ar, r_ar)
    
    else:
        raise Error('unrecognized result format')
    
    
def relation( #get the set relation between 2 unique data sets
        ldata, rdata, 
                  lname = 'left',
                  rname = 'right',                  
               
                  logger=mod_logger
                  ):
    
    log = logger.getChild('relation')
    #===========================================================================
    # precheck
    #===========================================================================
    if isinstance(ldata, str):
        raise Error('expected array type')
    if isinstance(rdata, str):
        raise Error('expected array type')
    

        
        
    #===========================================================================
    # data conversion
    #===========================================================================
    if not isinstance(ldata, np.ndarray):
        l_ar = np.array(list(ldata))
    else:
        l_ar = ldata
        
    if not isinstance(rdata, np.ndarray):
        r_ar = np.array(list(rdata))
    else:
        r_ar = rdata
        


        
    #===========================================================================
    # pre check
    #===========================================================================
    #check for empty containers and uniqueness
    for data, dname in (
        (l_ar, lname),
        (r_ar, rname)
        ):
        #empty container
        if data.size == 0:
            raise Error('got empty container for \'%s\''%dname)
                   
        if not pd.Series(data).is_unique:
            #get detailed print outs
            ser = pd.Series(data)
            boolidx = ser.duplicated(keep=False)            
            
            raise Error('got %i (of %i) non-unique elements for \'%s\' \n    %s'%(
                boolidx.sum(), len(boolidx), dname, ser[boolidx]))
            
            
    #===========================================================================
    # get the relation
    #===========================================================================
    #check for both
    dif_s = set(l_ar.tolist()).symmetric_difference(set(r_ar.tolist()))
    
    
    if len(dif_s) == 0:
        log.debug('no difference between %i %s els and %i %s els... returning \'inner\''%(
            len(l_ar),lname,  len(r_ar), rname))
        return 'inner'
    
    #check misses
    left_miss = set(l_ar.tolist()).difference(set(r_ar.tolist()))
    
    right_miss = set(r_ar.tolist()).difference(set(l_ar.tolist()))
    
    log.debug('got %i %s misses and %i %s misses'%(
        len(left_miss), lname, len(right_miss), rname))
    #===========================================================================
    # return result based on misses
    #===========================================================================
    if len(left_miss)>0 and len(right_miss)>0:
        return 'outer'
    
    if len(left_miss)<0:
        return 'left'
    
    if len(right_miss)>0:
        return 'right'
    
    
    

        


    
  
#===============================================================================
# # module vars ----------------------------------------------------------------
#===============================================================================

"""this auto is breaking. just use manual
#build type conversion
npc_pytype_d1 = build_type_conv()

for k, v in npc_pytype_d1.items():
    print(k,v)
    
e <class 'float'>
f <class 'float'>
q <class 'int'>
h <class 'int'>
l <class 'int'>
i <class 'int'>
g <class 'numpy.float64'>
V <class 'bytes'>
U <class 'str'>
m <class 'datetime.timedelta'>
B <class 'int'>
L <class 'int'>
Q <class 'int'>
H <class 'int'>
I <class 'int'>

"""

npc_pytype_d = {'?':bool,
                'b':int,
                'd':float,
                'e':float,
                'f':float,
                'q':int,
                'h':int,
                'l':int,
                'i':int,
                'g':float,
                'U':str,
                'B':int,
                'L':int,
                'Q':int,
                'H':int,
                'I':int, 
                'O':str, #this is the catchall 'object'
                }


if __name__ == '__main__':
    
    
    fn = r'C:\LocalStore\03_TOOLS\SOFDA\cplx\_ins\SOFDA-cplx_pars001.csv'
    
    ins_d = csv_to_dict(fn)
    
    mod_logger.info('finished')
    
        

