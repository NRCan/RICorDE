'''
Created on Mar 11, 2019

@author: cef

pandas functions 

py3.7

pd.__version__

'''
import logging, copy, os, time, re, xlrd, math, gc, inspect
import numpy as np
import pandas as pd

#===============================================================================
# pandas global options
#===============================================================================
pd.options.mode.chained_assignment = None   #setting with copy warning handling

#truncate thresholds
pd.set_option("display.max_rows", 20)
pd.set_option("display.max_colwidth", 20)

#truncated views
pd.set_option("display.min_rows", 15)
pd.set_option("display.min_rows", 15)
pd.set_option('display.width', 100)

"""
df = pd.DataFrame({"column1": ["foofoofoofoofoofoofoofofofoooofofofofofofofofoof  fofooffof offoff", "foo", "foo", "foo", "foo",
                         "bar", "bar", "bar", "bar"],
                   "columnB": ["one", "one", "one", "two", "two",
                         "one", "one", "two", "two"],
                   "columnC": ["small", "large", "large", "small",
                         "small", "large", "small", "small",
                         "large"],
                   "columnD": [1, 2, 2, 3, 3, 4, 5, 6, 7],
                   "columnE": [2, 4, 5, 5, 6, 6, 8, 9, 9]})
                   
                   
pd.get_option('display.max_rows')#60
pd.get_option('display.max_columns')#0
pd.get_option('display.chop_threshold')
pd.get_option('display.width')#80 #Width of the display in characters
pd.get_option('display.max_colwidth')#50
pd.get_option('display.min_rows') #10
"""

#===============================================================================
# custom imports
#===============================================================================

from hp.exceptions import Error
import hp.np
from hp.np import left_in_right as linr



mod_logger = logging.getLogger(__name__) #creates a child logger of the root

bool_strs = {'False':False,
             'false':False,
             'FALSE':False,
             0:False,
             'True':True,
             'TRUE':True,
             'true':True,
             1:True,
             False:False,
             True:True}


#===============================================================================
#VIEWS ---------------------------------------------------------
#===============================================================================



def view_web_df(df):
    if isinstance(df, pd.Series):
        df = pd.DataFrame(df)
    import webbrowser
    #import pandas as pd
    from tempfile import NamedTemporaryFile

    with NamedTemporaryFile(delete=False, suffix='.html', mode='w') as f:
        #type(f)
        df.to_html(buf=f)
        
    webbrowser.open(f.name)
    
def view(df):
    view_web_df(df)
    
#===============================================================================
#IMPORTS --------------------------------------------------------------------
#===============================================================================

def load_csv_df(#load a csv to a dataframe
                #pd.read_csv pars
                filepath,  
                
                header = 0,  #Row number(s) to use as the column names, and the start of the data. 
                                #fofor dxcol, pass a list of the column names
                index_col = None, #index column number. None: there is no index column
                
                skip_blank_lines = True, 
                skipinitialspace = True, 
                skiprows = None,
                
                parse_dates=True, 
                sep = ',', 
                na_values = None, #Additional strings to recognize as NA/NaN.
                encoding = 'utf-8',
                
                #expectations
                expect_unique_cols = True,
                
                logger=mod_logger, db_f = False,
                **kwargs #special kwqargs to pass to read_csv
                ):
    
    #===========================================================================
    # defaults
    #===========================================================================
    log = logger.getChild('load_csv_df')

    
    #===========================================================================
    # prechecks
    #===========================================================================
    if not os.path.exists(filepath): 
        raise IOError('passed filepath does not exist:\n     %s'%filepath)
    
    assert filepath.endswith('.csv') or filepath.endswith('.txt')
    
    if (not isinstance(header, int)) and (not header is None): #normal state
        if not isinstance(header, list):
            raise Error('unrecognized format for \'header\': %s'%type(header))
        else:
            for v in header:
                if not isinstance(v, int): 
                    raise IOError
    
    """
    
    pd.__version__
    
    help(pd.read_csv)

    
    """
    _, fn = os.path.split(filepath) #for logging
    
    log.debug('loading from: %s'%filepath)
    
    df_raw = pd.read_csv(filepath,
                     header = header, 
                     index_col=index_col, 
                     skip_blank_lines = skip_blank_lines,
                    skipinitialspace=skipinitialspace, 
                    skiprows = skiprows, 
                    parse_dates=parse_dates,
                    sep = sep,
                    na_values = na_values,
                    encoding=encoding,
                    **kwargs
                    )
                    #**kwargs)


        
    log.info('loaded df %s from file: \n    %s'%(df_raw.shape, filepath))
    


    df = df_raw.copy(deep=True)
        
    #===========================================================================
    # format index
    #===========================================================================
    df1 = df.copy()
    
    if not isinstance(df1.index, pd.MultiIndex):
        try:
            df1.index = df.index.astype(np.int)
        except Exception as e:
            log.warning('failed to convert index to numeric for %s \n    %s'%(
                fn, e))
        
    #===========================================================================
    # false boolean
    #===========================================================================
    for coln, col in df1.items():
        if col.dtype.char == 'O': #for object types 
            
            if np.any(
                np.isin(
                    np.array(bool_strs.keys()), col.values)):
                log.warning('\'%s\' is a possible boolean'%coln)
            
            """consider some sophisitcated null boolean handling"""
        
    #===========================================================================
    # check for phantom columns
    #===========================================================================
    srch_val = 'Unnamed'
    
    #multi dxcols
    if isinstance(df1.columns, pd.MultiIndex):
        #scan and check each level
        for level in range(0, df1.columns.nlevels, 1):
            booldf = df1.dropna(axis=1, how='all').columns.isin([srch_val], level=level)
            if np.any(booldf):
                raise Error('on level %i found %i \'%s\' values'%
                               (level, booldf.sum().sum(), srch_val))
            
    #normal dxcools
    else:
        if np.any(df1.dropna(axis=1, how='all').columns.isin([srch_val])):
            #missing some column names?
            raise Error('phantom column created from: %s'%fn)
        
        
    #===========================================================================
    # check expectations
    #===========================================================================
    if not df1.columns.is_unique:
        log.warning('got duplicate column names')
        
        if expect_unique_cols:
            raise Error('duplicated column names')
    
    
       
    return df1


def load_xls_df(filepath, 
                logger=mod_logger, 
                sheetname = 0,
                header = 0,  #Row number(s) to use as the column names, and the start of the data. 
                index_col = 0,
                dtype  = None, #Type name or dict of column -> type, default None
                parse_dates = False, 
                skiprows = None,
                convert_float = False, 
                mixed_colns = None, #optional columns to apply mixed dtype workaround
                **kwargs):
    """
    #===========================================================================
    # INPUTS
    #===========================================================================
    sheetname: None returns a dictionary of frames
        0 returns the first tab
        
    #===========================================================================
    # KNOWN ISSUES
    #===========================================================================
    converting TRUE/FALSE to 1.0/0.0 for partial columns (see pandas. read_excel)
    
    mixed int/str values... used 'mixed_colns'
    
    
    
    
    """
    #===========================================================================
    # defaults
    #===========================================================================

    log = logger.getChild('load_xls_df')
    
    #===========================================================================
    # prechecks
    #===========================================================================
    #if not filepath.endswith('.xls'): raise IOError
    if not isinstance(filepath, str): 
        raise IOError
    'todo: add some convenience methods to append xls and try again'
    if not os.path.exists(filepath): 
        raise IOError('passed filepath not found: %s'%filepath)
    
    if parse_dates: 
        raise IOError #not impelmented apparently..
    
    #version check
    """older versions dont seem to load the sheetnames properly
    """


        
    #===========================================================================
    # loader
    #===========================================================================
    try:
        #logger.debug("loading file with pandas version %s and default engine"%int(pd.__version__[2:4]))
        df_raw = pd.read_excel(
                filepath,
                sheet_name = sheetname,
                header = header,
                index_col = index_col, 
                skiprows = skiprows,
                parse_dates = parse_dates,
                convert_float = convert_float,
                dtype = dtype ,
                engine = None,
                formatting_info = False,
                verbose= False,
                **kwargs)
        
    except Exception as e:
        raise Error('failed to load df to excel from: \n    %s \n w/ error: \n    %s'%(filepath, e))
    
    #===========================================================================
    # post checks
    #===========================================================================

    #===========================================================================
    # # for loaded di ctionaries
    #===========================================================================
    if not isinstance(df_raw, pd.DataFrame):
        if not sheetname is None: 
            raise IOError('requested a sheetname but loaded the whole spreadsheet!')
        if not isinstance(df_raw, dict): 
            raise IOError('got bad type')
        
        logger.debug('sheetname = None passed. loaded as dictionary of frames')

        return df_raw
    

    #===========================================================================
    # # loaded si ngle sheet
    #===========================================================================
    log.debug('loaded df %s from sheet \'%s\' and file: \n    %s'%(df_raw.shape, sheetname, filepath))
           

    
    #===========================================================================
    # mixed types workaround
    #===========================================================================
    df=df_raw.copy()
    if not mixed_colns is None:
        #check it
        if not isinstance(mixed_colns, list):
            raise Error('expected a list')
        
        l = linr(mixed_colns, df.columns, result_type='missing')
        if len(l)>0:
            raise Error('mising %i requsted mixed_colns: %s'%(len(l), l))
        
        #loop and handle
        log.info('handling mixed types on %i columns: %s'%(len(mixed_colns), mixed_colns))
        for coln in mixed_colns:
            df = mixed_dtype(df, coln, logger=log)
            
        log.debug('finished hanlding mixed types')
        
        """
        df
        
        '2' in df['mclas_code']
        2 in df['mclas_code']
        """
                
    return df



def load_xls_d(  #load a xls collection of tabs to spreadsheet
                filepath, 
                mixed_colns_d=None,
                logger=mod_logger,                
                **kwargs):
    
    
    if mixed_colns_d is None:
        mixed_colns_d = dict()
    #===========================================================================
    # defaults
    #===========================================================================
    log = logger.getChild('load_xls_d')    
    
    #===========================================================================
    # setup
    #===========================================================================
    df_d = dict() #creat ehte dictionary for writing
    
    #get sheet list name
    try:
        xls = xlrd.open_workbook(filepath, on_demand=True)
    except:
        logger.error('failed to open xls from \n     %s'%filepath)
        raise IOError
    sheetnames_list =  xls.sheet_names()
            
    log.debug('with %i sheets from %s: %s'%(len(sheetnames_list),filepath, sheetnames_list))
    
    #===========================================================================
    # loop and load
    #===========================================================================
    for sheetname in sheetnames_list:
        log.debug('on sheet: \'%s\' \n'%sheetname)
        
        #mixed_colns
        if sheetname in mixed_colns_d:
            mixed_colns = mixed_colns_d[sheetname]
            log.debug('pulled %i values for mixed_colns'%len(mixed_colns))
            
            #check it
            if not isinstance(mixed_colns, list):
                raise Error('got unexeted type for sheet \'%s\' mixed_colns: %s'%(sheetname, type(mixed_colns)))
            
        else:
            mixed_colns = None
        
        #pull the df from the file and do the custom parameter formatting/trimming
        df_raw  = load_xls_df(filepath, sheetname = sheetname, 
                                                logger = logger, mixed_colns=mixed_colns,
                                                **kwargs)
        
        #check it
        if len(df_raw) < 1: 
            log.warning('got no data from tab \'%s\' in %s '%(sheetname, filepath))
            continue

        
        df_d[sheetname] = df_raw #update the dictionary
        
    log.info('loaded %i dfs from xls sheets: %s'%(len(df_d.keys()), df_d.keys()))
        
    return df_d

#===============================================================================
#CLEANING-------------------------------------------------------------------
#===============================================================================


def col_typeset( #format columns based on passed dictionary
        df_raw, 
        colnt_d_raw, #{column name: type function}
        fail_hndl_d = dict(), #handlese for failed typsetting {coln, handle key}
        errors='raise',
        drop = False, #whether to drop those columns not in the colnt_d
        logger=mod_logger):
    
    #===========================================================================
    # setups and defaults
    #===========================================================================
    log = logger.getChild('col_typeset')
    
    #stnadard handle celaning
    colnt_d = hndl_d_cleaner(colnt_d_raw, logger=log)
    
    
    log.debug('typesetting %s with %s'%(str(df_raw.shape), colnt_d))
    
    
    #===========================================================================
    # loop through each columna nd apply typesetting
    #===========================================================================
    df = df_raw.copy()
    
    #record of those entries to drop
    boolidx_drop = pd.Series(index = df_raw.index, dtype=bool)
    
    
    for coln, typeset_str in colnt_d.items():
        
        #===========================================================================
        # check column names are there
        #===========================================================================
        if not coln in df_raw.columns:
            log.warning('passed column name \'%s\' nto found in the frame'%coln)
            continue
        
        if pd.isnull(typeset_str):
            log.debug('coln \'%s\' got a null typeset_str. skipping'%coln)
            continue
        
        if np.any(pd.isnull(df_raw[coln])):
            log.warning('column \'%s\' has %i nulls. may result in unexpected behavior during typsetting'
                        %(coln, pd.isnull(df_raw[coln]).sum()))
        
        #===========================================================================
        #special typesetting
        #===========================================================================
        if typeset_str.startswith('*'):
            #===================================================================
            # datetime
            #===================================================================
            if typeset_str.startswith('*date'):
                """nulls here are switched to NaT
                pd.isnull(df[coln]).sum()
                
                """
                #auto/lazy date time setting
                if typeset_str == '*date':
                    log.debug('setting lazy datetime')
                    df.loc[:, coln] = pd.to_datetime(df_raw[coln])
                    
                #using explicit strftime  time codes (http://strftime.org/)
                else:
                    
                    strftime_str =  typeset_str[6:-1] #drop everything else
                    log.debug('setting datetime with strftime \'%s\''%strftime_str)
                    df.loc[:, coln] = pd.to_datetime(df_raw[coln], format=strftime_str)
                    
            #===================================================================
            # strings
            #===================================================================
            elif typeset_str.startswith('*str'):
                #conveert to a string
                df.loc[:,coln] = df[coln].astype(str)
                
                ncode = re.sub('str','',typeset_str[1:])[1:-1] #drop the special string
                
                if ',' in ncode: raise IOError('todo: allow for multiple codes')
                
                if ncode == 'lower':
                    log.debug('for coln \'%s\' dropping everything to lower case'%coln)
                    df.loc[:,coln] = df[coln].str.lower()
                
                else:
                    raise IOError
                
            #===================================================================
            # tuple like
            #===================================================================
            elif typeset_str.startswith('*tlike'):

                
                #get the sub type
                sub_dtype = re.sub('tlike','',typeset_str[1:])[1:-1] #drop the special string
                
                #do the cleaning
                df.loc[:,coln] = tlike_ser_clean(df[coln], sub_dtype=sub_dtype, logger=log)

                    
            else: 
                log.error('got unrecognized special key: \'%s\''%typeset_str)
                raise IOError
                

            
        #=======================================================================
        # normal typesetting
        #=======================================================================
        else:
            #check for uncessary attempts
            if typeset_str in df[coln].dtype.name:
                log.debug('column \'%s\' is already type \'%s\'. skipping'%(coln, typeset_str))
                continue
            
            #try and do the typesetting, with some fancy error handling
            try:
                df.loc[:, coln] = df[coln].astype(typeset_str, errors=errors)
                'this will only throw an error when error = error'

            except:
                """
                I dont really like using this anymore
                need to idetify, then handle those items failing the typesetting
                """
                if np.any(pd.isnull(df[coln])): #should clear these with the null handler first
                    log.error('found %i null values on column \'%s\''%(pd.isnull(df[coln]).sum(), coln))
                    raise IOError
                
                if not coln in fail_hndl_d.keys():
                    'defaults to *error really'
                    raise IOError('failed to cast type %s on to column \'%s\' (no handel provided)'%(typeset_str, coln))
                else:
                    #===================================================================
                    # handel failed typesetting
                    #===================================================================
                    fhndl = fail_hndl_d[coln]  #get thsi handle
                    
                    #identify which entries are failing tye typesetting
                    boolidx = hp.np3.typeset_fail_bool(df[coln].values, typeset_str, logger=log)
                    
                    #apply the value handeler
                    newv, boolidx_drop = value_handler(fhndl, coln, boolidx,
                                   boolidx_drop  = boolidx_drop, log=log)
                    
                    #set the new value (wont matter if were dropping everything)
                    df.loc[boolidx, coln] = newv
                    
                    #drop and try again
                    if np.any(boolidx_drop):
                        df = df.loc[np.invert(boolidx_drop), :]
                        
                    try:
                        df.loc[:, coln] = df[coln].astype(typeset_str, errors=errors)
                    except:
                        log.error('failed to typeset \'%s\' on \'%s\' even after dropping %i'
                                  %(typeset_str, coln, boolidx_drop.sum()))
                        raise IOError
                    

        #=======================================================================
        # post checks
        #=======================================================================
        if 'int' == typeset_str:
            #see if you set an int thats too short for the data
            if not df_raw[coln].sum() == df[coln].sum():
                raise ValueError('on \'%s\' during conversion from %s to %s. Try using \'int64\''%
                                 (coln, df_raw[coln].dtype.name, df[coln].dtype.name))
            
            

        #end column loop
        log.debug('changed column \'%s\' from %s to %s'%
                  (coln, df_raw[coln].dtype.name, df[coln].dtype.name))
                
            
            
            

        
    #===========================================================================
    # drop other columns
    #===========================================================================
    if drop:
        boolcol = df.columns.isin(colnt_d.keys()) #find those in teh list
        df1 = df.loc[:, boolcol]
        
        log.debug('dropped %i columns'%(len(df.columns) - len(df1.columns)))
    
    else: 
        df1 = df
        
    #===========================================================================
    # wrap up
    #===========================================================================
    
    res_d = dict()
    for coln, col in df1.iteritems():
        res_d[coln] = col.dtype.name
        
    log.debug('finished with column types set to\n     %s '%res_d)
    
    return df1

def coln_convert( #converts the passed header names to those in the dictionary and reorders
        df_raw, 
        on_hndl_d_raw, #conversion dictionary: {old header name : new header name}
        expect_match = False, #whether to expect that all the old values are there
        drop = False, #whether to drop those not passed in the dictionary
        logger=mod_logger): 
    """
    thsi sorts teh new headers alphabetically as the dictionary has no order
    
    """
    
    
    log = logger.getChild('coln_convert')
    df = df_raw.copy(deep=True)
    
    #standard handle cleaning
    on_hndl_d1 = hndl_d_cleaner(on_hndl_d_raw, logger=log)

    
    #===========================================================================
    # check all the headers are there
    #===========================================================================
    old_new_d = dict() #build a new one with just the matches
    for ocoln, ncoln in on_hndl_d1.items():

        if pd.isnull(ncoln):
            raise IOError
        
        if not ocoln in df_raw.columns:
            raise IOError('requested coln \'%s\' not in df'%ocoln)
        else:
            old_new_d[ocoln] = ncoln
            
    if expect_match:
        if not len(df_raw.columns) == len(old_new_d):
            raise IOError
        
    #===========================================================================
    # check there are no duplicates
    #===========================================================================
    if not len(np.unique(np.array(list(old_new_d.values())))) == len(old_new_d):
        log.error('passed non-unique new columns list:\n    %s'%list(old_new_d.values()))
        raise IOError
        

    #===========================================================================
    # buidl the new headers list
    #===========================================================================
    new_headers = [] #old headers + new headers
    focus_headers = [] #renamed headers in the dataset
    extra_headers = []
    
    for header in df.columns:
        

        if header in old_new_d: #convert me
            conv_head = old_new_d[header]
            
            if conv_head is None: raise IOError
            if pd.isnull(conv_head): raise IOError
            
            nhead = conv_head
            
            focus_headers.append(conv_head)
            log.debug('converting \'%s\' to \'%s\''%(header, conv_head))
        else: #no conversion here
            extra_headers.append(header)
            nhead = header

        #check thsi isnt a duplicate
        if nhead in new_headers:
            log.error('attempting duplicate column \'%s\''%nhead)
            raise IOError
        else:
            new_headers.append(nhead)
            
    #===========================================================================
    # #apply the new headers
    #===========================================================================
    df.columns = new_headers
    
    log.debug('renamed %i headers: \n    %s'%(len(focus_headers), focus_headers))
    

    #===========================================================================
    # reorder the columns
    #===========================================================================
    new_head_ordr = sorted(focus_headers) + extra_headers
    df_ordr = df[new_head_ordr]
     

    if not len(df_ordr.columns) == len(df_raw.columns):
        log.error('lost some columns')
        """
        len(new_head_ordr)
        """
        
        boolar = np.isin(
            df_raw.columns.values,
            np.array(new_head_ordr))
        
        df_raw.columns[boolar]
        
        
        
        raise IOError
    
    log.debug('reordered headers')
    
    
    #===========================================================================
    # drop extra columns
    #===========================================================================
    if drop:
        boolcol = df_ordr.columns.isin(old_new_d.values())
        df_ordr2 = df_ordr.loc[:, boolcol]
        
        log.debug('dropped %i columns'
                  %(len(df_ordr.columns) - len(df_ordr2.columns)))
        
    else: df_ordr2 = df_ordr
     
    return df_ordr2


def hndl_nulls( #run value_handles on null values in columns
               df_raw, 
               hnull_d_raw, #null handles {coln : treatment code}
               logger=mod_logger,
               ):
    """
    move this to hp.pd?
    """
    log = logger.getChild('hndl_nulls')
    log.info('handling null values on df %s'%(str(df_raw.shape)))
    #===========================================================================
    # clean the handles
    #===========================================================================
    """special value treatment"""
    hnull_d = hndl_d_cleaner(hnull_d_raw, logger=log)

    
    #=======================================================================
    # identify columns with nulls
    #=======================================================================
    df = df_raw.copy().dropna(axis='index', how='all') #drop total blank rows
    
    #setup slicing boolidx. all False = no slice
    boolidx_s = pd.Series(index = df.index, dtype = bool)
    
    for coln, col in df_raw.items():
        boolidx = pd.isnull(col)
        if np.any(boolidx):
            
            if not coln in hnull_d.keys():
                log.error('df column \'%s\' not in teh conversion dict'%coln)
                raise IOError
            """
            consider allowing this to not be so exlpicit
            """
            
            hnd = hnull_d[coln]
            
            log.debug('detected %i null values on column \'%s\' with handle \'%s\''
                      %(pd.isnull(col).sum(), coln, hnd))
            
            #===========================================================
            # special handles
            #===========================================================
            newv, boolidx_s = value_handler(hnd, coln, boolidx, 
                                            boolidx_drop = boolidx_s, log=log)

            #set teh new value
            log.debug('setting \'%s\' on all null values in column \'%s\''%(newv, coln))
            df.loc[boolidx, coln] = newv
            
    #=======================================================================
    # perform any slicing
    #=======================================================================
    #slice out all of those entries identified for slicing during the loop
    df1 = df.loc[np.invert(boolidx_s),:] #keeping non-slice values 
            
            
    return df1
    
    
def value_handler( #treat a value according to the passed handle
        hnd,  #handle to apply for thsi value
        coln, #column name under evaluation
        boolidx,  #identifier index
        boolidx_drop = None, #boolidx to store drop info in
        log = mod_logger,
        ):
    
    
    newv = None
    if isinstance(hnd, str):
        if hnd.startswith('*'):
            if hnd == '*error':
                raise IOError('got %i on \'%s\''%(boolidx.sum(), coln))
            elif hnd == '*ignore':
                newv = np.nan
                
            #=======================================================
            # slice index by null values on this axis
            #=======================================================
            elif hnd == '*drop':
                if boolidx_drop is None: 
                    boolidx_drop = np.full(len(boolidx), False, dtype=bool)
                    
                #update the drop list
                boolidx_drop = np.logical_or(boolidx_drop, boolidx)
                log.warning('for \'%s\' added %i to the drop list'
                          %(coln, boolidx.sum()))

            else:
                raise IOError


    #===============================================================
    # standard replacement handle
    #===============================================================
    if newv is None: newv = hnd
    
    if boolidx_drop is None: return newv
    else: return newv, boolidx_drop
    
def typeset_fail_bool( #identify where in the data the typeset is failing
        in_ser,
        typeset_str, 
        logger=mod_logger):
    """
    seems like there should be a built in function to do this
    """
    
    log = logger.getChild('typeset_fail_bool')
    
    typef = eval(typeset_str)
     
    boolar = pd.Series(index=in_ser.index, dtype=bool)
    
    
    for loc, val in in_ser.items():
        try: 
            _ = typef(val)
        except:
            boolar[loc] = True #flag this one as failing
            
    log.debug('idnetified %i (of %i) entries as failing to typeset from \'%s\''
              %(boolar.sum(), len(in_ser), typeset_str))
    
    if np.any(pd.isnull(boolar)):
        raise IOError
    
    return boolar


def clean_nulls( #replace non standard null values with NaN
        ser_raw,
        search_l = ['none'], #list of values to search and replace with null
        logger=mod_logger
        ):
    
    log = logger.getChild('detect_nulls')
    ser = ser_raw.copy()
    
    #===========================================================================
    # find the nulls
    #===========================================================================
    for srchv in search_l:
        
        if srchv in ser.values:
            occurances = ser.value_counts()[srchv]
            
            
            logger.info('swapping %i \'%s\' values for NaN on \'%s\''%
                        (occurances, srchv, ser.name))
        
            ser.replace(to_replace = srchv, value=np.nan, inplace=True)
            
    
    return ser


    
    
    
    
    

def cleaner_report(df_raw, df_clean,logger=mod_logger):
    """
    #===========================================================================
    # TESTING
    #===========================================================================
    view_df(df_raw)
    
    len(df_raw.index)
    """
    logger = logger.getChild('report')
        
    #get deltas
    rows_delta = len(df_raw.index) - len(df_clean.index)
    cols_delta = len(df_raw.columns) - len(df_clean.columns)
    
    if rows_delta < 0 or cols_delta <0:
        logger.error('cleaning ADDED rows?! switch inputs?')
        raise IOError
    
    #===========================================================================
    # #get list of headers not in df_clean
    #===========================================================================
    
    cleaned_boolhead = ~df_raw.columns.isin(df_clean.columns)
    
    removed_headers = list(df_raw.columns[cleaned_boolhead])
    
    #===========================================================================
    # identify cleaned indicies
    #===========================================================================
    cleaned_boolidx = ~df_raw.index.isin(df_clean.index)
    
    removed_ids = list(df_raw.index[cleaned_boolidx])
    
    #===========================================================================
    # check if the numbers match
    #===========================================================================
    if not rows_delta == len(removed_ids):
        logger.error('got length mismatch')
        raise IOError
    if not cols_delta == len(removed_headers):
        logger.error('got length mismatch')
        return #not handling added column names

    
    #===========================================================================
    # reporting
    #===========================================================================
    if rows_delta + cols_delta == 0:
        logger.debug('did no cleaning')
        return
    else:
        if cols_delta > 0:
            logger.debug('cleaned %i headers:  %s '%(len(removed_headers), removed_headers))
        
        if rows_delta > 0:
            logger.debug('cleaned %i idx:  %s '%(len(removed_ids), removed_ids))
            
    return  


def resolve_conf(
                df_raw, #messy data that needs resolution
                key_coln, #column name that keys the data (result should be unique here)  
                resl_od, #dictionary of {field: ranked values}
                logger = mod_logger,
                ):
    
    log = logger.getChild('resolve_conf')
    
    df = df_raw.copy()
    
    #===========================================================================
    # loop through the rpeference set
    #===========================================================================
    for coln, pref_l in resl_od.items():
        if not coln in df.columns:
            log.error('passed coln not found')
            raise IOError 
        
        #slice to just this coln
        #val_ar = df.loc[:, coln].values
        
        #loop through the preference values 
        for pickv in pref_l:
            boolidx = df.loc[:, coln] == pickv
            
            if boolidx.sum() == 0: continue #nothing found, keep going
            elif boolidx.sum() == len(df):
                log.debug('could not resolve conflict with \'%s\' colmn. all values match'%(coln))
                break
            elif boolidx.sum()>1: 
                log.debug('got partial resolution (%i to %i) with \'%s\' colmn'%
                          (len(df), len(df[boolidx]), coln))
                
                df = df[boolidx] #slice down to just these
                break #this matches everything, try the next item in the resl_od
            
            else:
                log.debug('found unique ending on \'%s\''%coln)
                
                return df[boolidx]
            """
            len(df[boolidx])
            """
            
    log.debug('unable to find unique match, trimeed from %i to %i'%(len(df_raw), len(df)))
    
    return df

def hndl_d_cleaner( #standard handle cleaning    
        d_raw, #raw handles to clean
        logger=mod_logger):
    
    log = logger.getChild('hndl_d_cleaner')
    
    if not isinstance(d_raw, dict):
        raise IOError
    
    
    d = dict()
    
    for key, hndl in d_raw.items():
        if pd.isnull(hndl): continue
        if isinstance(hndl, str):
            if hndl.startswith('~'): continue
            
        d[key] = hndl
        
    if not len(d) == len(d_raw):
        log.debug('cleaned %i (of %i) handel values'%
                  (len(d_raw) - len(d), len(d_raw)))
        
    if len(d) == 0:
        log.warning('cleaned all handles')
        
    log.debug('cleaned handels. \n from %s: \n to :%s'%(d_raw, d))
        
    return d
                  



        
def reorder_coln(  #move a set of columns to the first
        df_raw,
        req_cols, #tuple of column names you want to be first
        first = True, #send requested columns to the front of the colidx
        logger=mod_logger):
            
    log = logger.getChild('reorder_coln')
    
    df = df_raw.copy()
    
    old_cols = df_raw.columns
    
    if not isinstance(req_cols, tuple):
        req_cols = tuple(req_cols)
    
    
    #===========================================================================
    # prechecks
    #===========================================================================
    #check everything we aske dfor is in there
    boolar = np.invert(np.isin(np.array(req_cols), old_cols))
    
    if np.any(boolar):
        raise IOError('%i requested columns are not in the frame: %s'
                      %(boolar.sum(), np.array(req_cols)[boolar]))
    
    #===========================================================================
    # setup
    #===========================================================================
    log.debug('requesting %i (of %i) columns be moved to the front'%(len(req_cols), len(old_cols)))
    
    #get unrequested columns
    boolcol = old_cols.isin(req_cols)
    
    ureq_cols = old_cols[np.invert(boolcol)]
    
    #===========================================================================
    # reorder
    #===========================================================================
    if first:
        df = df.loc[:, req_cols + tuple(ureq_cols.tolist())].copy()
    else: #send the request to the end
        df = df.loc[:, tuple(ureq_cols.tolist()) + req_cols].copy()
    
    #===========================================================================
    # post checking
    #===========================================================================
    boolcol = np.invert(df_raw.columns.isin(df.columns))
    
    if np.any(boolcol):
        raise IOError('lost %i columns: %s'%(boolcol.sum(), df_raw.columns[boolcol]))
    
    return df
    
    
def fmt_null_wsub(ser, #fancy formatting for series with nulls
                  sub_type = int, #type requested for non nulls (to be presented as strings)
                  logger=mod_logger): #handle a series which may contain list or tuple like entries
    
    #===========================================================================
    # check if we are already teh correect type
    #===========================================================================
    if ser.dtype.char == 'O':
        return ser
    
    
    #===========================================================================
    # handle float types
    #===========================================================================
    log = logger.getChild('fmt_poss_tupl')
    if ser.dtype.char == 'd':
        
        #start new series
        res_ser = pd.Series(index=ser.index, dtype=str)
        
        #identify real values
        boolidx = np.invert(pd.isnull(ser))
        
        #set these as integers
        res_ser[boolidx] = ser[boolidx].astype(sub_type)
        
        log.debug('converted series from type \'%s\' to \'%s\' with sub types: \'%s\''
                  %(ser.dtype, res_ser.dtype, sub_type))
        
    else:
        raise IOError('exepected dtype \'d\' on series \'%s\'. instead got: %s'%(ser.name, ser.dtype))
    
    return res_ser
        
def mixed_dtype(#forcing correct types onto columns w/ mixed int and str values
                df_raw,
                coln,
                logger=mod_logger):
    
    
    log = logger.getChild('mixed_dtype')
        
    uq_vals_og = df_raw[coln].unique().tolist()
    
    #=======================================================================
    # work
    #=======================================================================
    

    if not df_raw[coln].dtype.char == 'O':
        raise Error('not a string type column')

    #get just the reals
    df = df_raw.loc[df_raw[coln].notna(), :]
    
    #add the working column
    df['_numeric'] = df[coln].str.isnumeric()
    
    #workaround for nulls
    df.loc[df['_numeric'].isna(), '_numeric'] = True
    
    #force thetype
    df.loc[:, '_numeric'] = df['_numeric'].astype(bool)

    
    
    #=======================================================================
    # remove pads from numeric
    #=======================================================================
    df_n = df.loc[df['_numeric'], :]
    
    log.debug('got %i (of %i) numeric values \"%s\''%(
        df['_numeric'].sum(), len(df), coln))
    
    
    #set these to integers
    try:
        df_n.loc[:, coln] = df_n[coln].astype(int).astype(str)
    except Exception as e:
        raise Error('failed to set \'int\' to \'%s\' w/ \n    %s'%(
            coln, e))
    

 
    #=======================================================================
    # reasesmble
    #=======================================================================
    #get the non-numerics
    df_str = df.loc[np.invert(df['_numeric']), :].astype(str)
    
    res_df = df_n.append(df_str).drop('_numeric', axis=1)
    
    #add back the nulls
    res_df = res_df.append(df_raw.loc[df_raw[coln].isna(), :])
    
    
    uq_vals = res_df[coln].unique().tolist()
    
    log.debug('cleaned from %i uqvals to %i'%(
        len(uq_vals_og), len(uq_vals)))
    
    """
    pd.to_str(res_df['mclas_code'])
    '2' in res_df['mclas_code'].astype(str)
    2 in res_df['mclas_code']
    """
    
    return res_df
     
                
    
    
     
#===============================================================================
# CHECKING -------------------------------------------------------------------
#===============================================================================
     
def are_dupes( #check if theer are any duplicates
            df_raw, 
            labname = None, #column name to check for duplicates on. 'index': check the index
            keep=False,
            axis=0, #axis to check. 0: search for duplicated rows
            logger=mod_logger,  
              **kwargs): 
    """
    colname = None: check for duplicates on all rows
    colname = 'index': check for duplicates on the index
    """
    
    logger = logger.getChild('are_dupes')
    df = df_raw.copy()
    #===========================================================================
    # find the rows that are duplicated
    #===========================================================================
    if axis == 0:
        
        if not isinstance(labname, list): #normal single column check
            
            if labname == 'index':
                boolar = df.index.duplicated(keep=keep)
            else:
                boolar = df.duplicated(subset = labname, keep=keep) #identify every entry and its twin
            
    
        else:
            """
            Here we want to find duplicate PAIRS
                this is different from identifying rows with values duplicated internally
                we want coupled duplictaes
                
            """
            
            #merge the columsn to check on
            chk_colnm = 'checker'
            df1 = concat_many_heads(df, labname, concat_name = chk_colnm, logger=logger)
            
            #find where there are internal duplicates on thsi merged column
            boolar = df1.duplicated(subset = chk_colnm, keep=keep) #identify every entry and its twin
            
    #===========================================================================
    # find columns that are duplicated
    #===========================================================================
    elif axis == 1:
        """this doesnt seem to be working
        boolar = df.T.duplicated(keep=keep)
        """
        """
        view(df.T)
        
        df.loc[:,'UNIT_LIVING_AREA_ABOVE_GRADE'].unique()
        
        
        """
        #logger.debug('scanning %i columns to see if there are duplicates'%len(df.columns))
        boolar = pd.Series(index = df.columns, dtype=bool)
        for coln, col in df.items(): #loop throuch each column and assess
            
            
            if len(col.unique()) == 1: #only one value , no du plicate
                boolar[coln] = False
            elif len(col.unique()) >= len(df):
                boolar[coln] = True
            

    #===========================================================================
    # closeout and report
    #=========================================================================== 
    if np.any(boolar):
        logger.debug('found %i (of %i) duplicates on \'%s\' '%(boolar.sum(),len(df_raw), labname))
    return boolar

def df_check(
                    df, #frame to check
                    exp_colns = None,
                    uexp_colns = None, #unexpected column names
                    exp_real_colns = None,
                    key = None,
                    logger=mod_logger):
        
        #=======================================================================
        # setups and default
        #=======================================================================

        log = logger.getChild('frame_check')
        
        #=======================================================================
        # basic checks
        #=======================================================================
        if not isinstance(df, pd.DataFrame):
            raise Error('expected dataframe, instead got \"%s\''%type(df))
        if len(df)==0:
            raise Error('got an empty frame')
        
        #=======================================================================
        # column name combiners
        #=======================================================================
        
        #real expectations        
        if not key is None:
            if exp_real_colns is None:
                exp_real_colns = [key]
            else:
                exp_real_colns = set(exp_real_colns).update([key])
                
        
        #combine column expectaiotns

        if not exp_real_colns is None:
            
            #nothing fo und... use these
            if exp_colns is None:
                exp_colns = exp_real_colns
            #add the values
            else:
                exp_colns = set(exp_colns).update(exp_real_colns)
                    
            

            
    
                    

                
                
        checks_l = []
        #=======================================================================
        # column presence
        #=======================================================================
        if not exp_colns is None:
            #log.info('checking \n    %s \n    %s'%(exp_colns, df.columns))
            
            l = linr( exp_colns, df.columns, result_type='missing', logger=log)
            if len(l)>0:
                raise Error('missing %i (of %i) expected columns: \n    %s'%(
                    len(l), len(df.columns), l))
            
            checks_l.append('exp_colns=%i'%len(exp_colns))
            
            
        #=======================================================================
        # column absence
        #=======================================================================
        if not uexp_colns is None:
            l = linr(df.columns, uexp_colns,  result_type='matching', logger=log)
            if len(l)>0:
                raise Error('matching %i (of %i) unexpected expected columns: \n    %s'%(
                    len(l), len(df.columns), l))
            
            checks_l.append('uexp_colns=%i'%len(uexp_colns))
            
            
        #=======================================================================
        # real values
        #=======================================================================
        if not exp_real_colns is None:
            booldf = df.loc[:, exp_real_colns].isna()
            
            if np.any(booldf):
                raise Error('got %i (of %i) null values on %i columns in %s \n\n%s'%(
                    booldf.sum().sum(), booldf.size, len(exp_real_colns), str(df.shape),
                    df.loc[:, exp_real_colns].loc[booldf.any(axis=1), booldf.any(axis=0)]
                    ))
                
            checks_l.append('reals=%i'%len(exp_real_colns))
            
            
        #=======================================================================
        # key check
        #=======================================================================
        if not key is None:
            if not df[key].is_unique:
                raise Error('non unique  keys \'%s\''%key)
            

            
            
            if not 'int' in df[key].dtype.name:
                raise Error('expected type \'int\' for \'%s\' but got \'%s\' \n%s'%(
                    key, df[key].dtype.name, df[key].to_dict()))
                
            if not df[key].sort_values().is_monotonic:
                raise Error('non monotonic  keys \'%s\''%key)
            
            
            
            checks_l.append('key=%s'%key)
            
            
        #=======================================================================
        # wrap
        #=======================================================================
        log.debug('finished checking %s w/ %i checks: %s'%(
            str(df.shape), len(checks_l), checks_l))
        
        return

#===============================================================================
#MANIPULATIONS ----------------------------------------------------------------
#===============================================================================
def boolidx_slice_d( #generate a boolidx where entries match {coln: value}
                     df, 
                     slice_d, # {coln: values}
                     slice_type = 'any', #type of slicing to perform
                        #'any': find features matching ANY of the field:values pairs
                        #'all': find features mathing ALL of the field:value pairs
                        
                    allow_none = False, #whether to allow no hits
                     logger=mod_logger,
        ):
    
    log = logger.getChild('boolidx_slice_d')
    
    #===========================================================================
    # #checks
    #===========================================================================
    if not np.all(np.isin(list(slice_d.keys()), df.columns.tolist())):
        raise IOError('missing some names from the slice dict')
    
    #===========================================================================
    # setup the loop
    #===========================================================================
    #===========================================================================
    # temp_boolidx = pd.Series(index=df.index, dtype=bool) #all Falses
    # 
    # #mboolidx = temp_boolidx.copy()
    # if slice_type == 'any':
    #     mboolidx = ~temp_boolidx #start w/ all trues, and widdle down
    # elif slice_type=='all':
    #     mboolidx = temp_boolidx #start w/ all False, and build up
    # else:
    #     raise Error('umnexpected slice_type')
    #===========================================================================
     
    #===========================================================================
    # loop and identify
    #===========================================================================
    log.info('finding \'%s\' slice on df %s w/ %i col:vals'%(
        slice_type, str(df.shape), len(slice_d)))
    bfirst = True
    for coln, vals_t in slice_d.items():
        """why not use isin?"""
        
        
        #take everything matching
        or_boolidx = df[coln].isin(vals_t)

        """
        #loop through each value
        first = True
        for val in vals_t:
            boolidx = df.loc[:,coln]==val #find those matching the slice value
             
            #first pass, start with all these trues
            if first:
                or_boolidx = boolidx
                first=False
 
            #take any hits
            elif slice_type == 'any':
                or_boolidx = np.logical_or(or_boolidx,boolidx) #take all those that hit this time and previous times
                 
            #must have all hits
            elif slice_type=='all':
                or_boolidx = np.logical_and(or_boolidx,boolidx) #this loop and all previous loops must have hit
                 
 
            else:
                raise Error('umnexpected slice_type')
 
 
         
            log.debug('        finding slice on col \'%s\' for val=\'%s\' matches %i (of %i) values'
                        %(coln, val, boolidx.sum(), len(df)))"""
             
        log.info('    finding slicing col \'%s\' with %i search values matches %i (of %i) elements: %s'
                    %(coln, len(vals_t), or_boolidx.sum(), len(df), vals_t))
        
        #=======================================================================
        # #update the master
        #=======================================================================
        if bfirst:
            mboolidx = or_boolidx
            bfirst=False
            
        #take any hits
        elif slice_type == 'any':
            mboolidx = np.logical_or(or_boolidx,mboolidx) #take all those that hit this time and previous times
             
        #must have all hits
        elif slice_type=='all':
            mboolidx = np.logical_and(or_boolidx,mboolidx) #this loop and all previous loops must have hit

            #no hits checker
            if not np.any(mboolidx):
                log.warning('search ended prematurely at \'%s\' w/ no htis'%(
                    coln))
                break

    #===========================================================================
    # wrap
    #===========================================================================
    log.info('got %i (of %i) total matches from %i coln:value sets'%(
        mboolidx.sum(), len(mboolidx), len(slice_d)))
    
    #expectation check
    if not np.any(mboolidx):
        log.warning('got 0 (of %i) hits'%len(mboolidx))
        if allow_none:
            raise Error('no hits and allow_none=False')
    
    return mboolidx


def take_first_real(#take the first real value along the given axis
                    df,  #collection of data to find real. (ORDER MATTERS!!)
                    axis=1, #axis to take the first value from
                    logger=mod_logger, 
                    ):
    """a bit surprised there is no builtin function for this"""
    
    log = logger.getChild('take_first_real')
    

    
    if axis == 1:
        
        #start with ifrst column
        res_ser = df.iloc[:, 0]
        #loop through and collect
        first = True
        
        for coln,  col in df.items():
            #===================================================================
            # add the values
            #===================================================================
            #skip the first
            if first:
                first=False
                oboolidx= ~pd.Series(index=res_ser, dtype=bool) #all trues
                
            #update with this one
            else:
                res_ser.update(col, 
                               #overwrite=False, #want to keep values we've set already
                               )
            
            #===================================================================
            #wrap
            #===================================================================
            #completion check
            if not np.any(res_ser.isna()):
                log.debug('filled remaining nulls (of %i) w/ col \"%s\''%(
                    len(res_ser), coln))
                break
            
            #log status
            else:
                
                boolidx = res_ser.isna()
                log.debug('\'%s\' filled %i (of %i, %i total) values'%(
                    coln, oboolidx.sum() - boolidx.sum(), oboolidx.sum(), len(res_ser)
                    ))
                oboolidx = boolidx

    else:
        raise Error('not impelmented')
    
    #===========================================================================
    # #wrap the loop
    #===========================================================================
    log.debug('collected first real value form %i columns, result %i real (of %i)'%(
        len(df.columns), res_ser.notna().sum(), len(res_ser)))
    
    return res_ser


#===============================================================================
#CONVERSIONS ----------------------------------------------------------------
#===============================================================================
def concat_many_heads(df_raw,  #combine many columns into one string 
                      heads_list, concat_name = 'concat', sep = ' ',
                      logger=mod_logger): 
    
    #=======================================================================
    # concat the columns of interest 
    #=======================================================================
    df1 = df_raw.copy(deep=True)
    
    for index, col in enumerate(heads_list):
        if index ==0:
            df1[concat_name] = df_raw[col].map(str)
            continue
        
        ser = df_raw[col].map(str)
        
        df1[concat_name] = df1[concat_name].str.cat(ser, sep= sep)
        
    return df1




     
        
     
#===============================================================================
# SET OPERATIONS -------------------------------------------------------------
#===============================================================================
def merge_left( #intelligently add the right_df to the left with the 'on' colname
            left_df_raw, 
            right_df_raw, 
            coln_link       = None,  #column name to link between the two frames
            #left_index      = True, 
            #right_name      = 'right', 
            allow_partial   = False, #allow right_df to be missing some keys (that are found on the left)
            ldupes          = 'fill', #treatment for duplicate key values on the left df
            rdupes          = 'no',
            #trim_dupes      = True, 
            allow_new_cols  = False, 
            allow_new_rows = False,
            #outpath         = None,  #outpath for saving results to file
            logger          = mod_logger,
            db_f            = True): 
    """
    #===========================================================================
    # USe
    #===========================================================================
    This is useful for taking a frame (left_df) with some data and a keys column (CPID)
    then adding a bunch more data per those keys (from the right_df)
    
    for example:
        survey results for each CPID
        and attaching all the assessment records for those CPIDs
        
    #===========================================================================
    # INPUTS
    #===========================================================================
    ldupes
        fill:    fill with data from the matches on teh right
        drop:    arbitrarily drop the duplicats
        no:        raise an error if these are detected
    """
    #===========================================================================
    # setup and defaults
    #===========================================================================
    logger = logger.getChild('merge_left')
    
    #===========================================================================
    # prechecks
    #===========================================================================
    if coln_link is None: 
        raise IOError('not implemented')
    else:
        if not coln_link in right_df_raw.columns: raise IOError
        if not coln_link in left_df_raw.columns: raise IOError
        
    #===========================================================================
    # pre clean
    #===========================================================================
    left_df = left_df_raw.copy()
    right_df = right_df_raw.copy()
            
    #=======================================================================
    # prechecks
    #=======================================================================
    if db_f:
        for dname, df in {'left':left_df, 'right':right_df}.items():
            #suspicious colum names
            if np.any(df.columns.str.contains('Unnamed')):
                raise IOError('got some Unamed columns')
            
            if not coln_link in df.columns:
                raise IOError('requested coln_link \'%s\' missing from \'%s\''%(coln_link, dname))
            
            if np.any(pd.isnull(df[coln_link])):
                raise IOError('\'%s\' got some nulls on the link column')%dname
            
            if not df.index.is_unique:
                raise IOError('\'%s\' has a broke index'%dname)
            
            

    logger.debug('joining left_df (%s) to right_df (%s)'%(str(left_df_raw.shape), str(right_df_raw.shape)))
    
    #=======================================================================
    # data cleaning
    #======================================================================= 
    #search the right to just those values we can find on the left
    boolidx = right_df[coln_link].isin(left_df[coln_link])
    right_df1 = right_df.loc[boolidx,:]
    
    
    #check for a perfect match match
    if not boolidx.sum() == len(left_df):
        logger.warning('found data length mismatch for entries on key \'%s\'(left=%i right = %i)'
                       %(coln_link, boolidx.sum(), len(left_df)))
        
        if allow_partial:
            pass #dont care that we aremissing some right keys
        elif  boolidx.sum() > len(left_df): #too many on the left
            if rdupes == 'no': raise IOError
        elif  boolidx.sum() < len(left_df): #too many on the left
            if ldupes == 'no': raise IOError
        else:
            raise IOError
        
    if not np.all(boolidx):
        if allow_new_rows:
            raise IOError('not ipmlemented')
        
    #===========================================================================
    # check for duplicates in source frames
    #===========================================================================
    #on the left
    boolidx = are_dupes(left_df, colname = coln_link, logger=logger)
    if np.any(boolidx): 
        if ldupes == 'no':
            logger.error('found  %i internal duplicates on passed left_df %s'%(boolidx.sum(), str(left_df.shape)))
            raise IOError
        elif ldupes == 'drop':
            left_df = left_df.drop_duplicates(coln_link)
            raise IOError #drop the duplicates
        elif ldupes == 'fill':
            logger.info('left has %i duplicated values for key \'%s\'. duplicating right values on these'%(boolidx.sum(), coln_link))
        else: raise IOError
    

    
    #on the Right
    boolidx = are_dupes(right_df, colname = coln_link, logger=logger)
    if np.any(boolidx): 
        if rdupes == 'no':
            logger.error('found  %i internal duplicates on passed right_df %s'%(boolidx.sum(), str(right_df2.shape)))
            raise IOError
        elif rdupes == 'drop':
            right_df3 = right_df1.drop_duplicates(coln_link)
            raise IOError #drop the duplicates
        elif rdupes == 'fill':
            raise IOError #not allowed
            
        else: raise IOError

        
    else: right_df3 = right_df1
    
    #===========================================================================
    # check for unexpected column matching
    #===========================================================================
    boolcol = left_df.columns.isin(right_df3.columns)
    if not boolcol.sum() ==1: #there is only one column matching between the two
        msg = 'found %i extra column matches between frames: %s'%(boolcol.sum(), left_df.columns[boolcol])
        if not allow_new_cols: raise IOError(msg)
        else: logger.debug(msg)
    
    #===========================================================================
    # #check if we need to upgrade to an mdex
    #===========================================================================
    if isinstance(left_df.columns, pd.MultiIndex):
        raise IOError('not implemented') #need to check this again
        """
        logger.debug('merging with a dxcol. upgrading right_df')
        if isinstance(right_df1.columns, pd.MultiIndex): raise IOError

        
        #get teh data from the left
        old_mdex = left_df.columns
        lvl0_vals = old_mdex.get_level_values(0).unique() 
        names = [old_mdex.names[0], right_name]
        
        #make the dummpy dxcol
        right_df4 = fill_dx_col(right_df3, lvl0_vals, names, logger=logger) #get this
        
        #perform teh merge
        merge_df = merge_dxcol(left_df, right_df4, on = coln_link, logger=logger)
        """
        
    else: 
        #=======================================================================
        # perform merge
        #=======================================================================
        merge_df = pd.merge(left_df, right_df3, 
                            on = coln_link,
                            how = 'left',
                            left_index = False,
                            right_index = False, 
                            left_on = None,
                            right_on = None,
                            sort = False,
                            indicator = False)
        
        """
        left_df.columns.tolist()
        right_df3.columns.tolist()
        """
    
    #=======================================================================
    # post checks
    #=======================================================================
    if db_f:
        if not len(merge_df) == len(left_df): 
            raise IOError
        
        if not np.all(merge_df.index.isin(left_df.index)): 
            logger.error('got some index mismatch')
            raise IOError
        
        if np.any(merge_df.columns.str.contains('Unnamed')):
            logger.error('some ghost column was created')
            raise IOError
        
        #check for duplicated index
        boolidx = are_dupes(merge_df, colname = 'index')
        if np.any(boolidx): 
            logger.error('found %i duplicated indicies in merge_df: '%(boolidx.sum()))

            raise IOError
    
    
    logger.debug('to left %s filled %s to make merge_df %s. attached as data'
             %(str(left_df.shape), str(right_df3.shape), str(merge_df.shape)))
    

    return merge_df

def update( #update all the values in df_bg from the non-null in df_sm (UNIQUE small link values)
    bg_df_raw,
    sm_df_raw, #always linked by link_coln (for index-index updates, you dont need this fancy script)
    link_coln = None, #link column name to use. if None, use index ON BOTH
    overwrite = False, #if the df_bg value is NOT null, whether to overwrite with df_sm value
    expect_unique_bigs = True, #whether to expect unique link_coln values on the bg_df_raw.
    logger=mod_logger):
    """
    handles updates by column keys (rather than indexes)
    
    handles non-unique keys in the big_df
    """

    #=======================================================================
    # setups and defaults
    #=======================================================================
    log = logger.getChild('update')
    
    #get/copy data
    bg_df =  bg_df_raw.copy()
    sm_df = sm_df_raw.copy()
    
    log.debug('updating from %s on %s with overwrite = %s'
              %(str(sm_df.shape), str(bg_df.shape), overwrite))
    
    #=======================================================================
    # checks
    #=======================================================================
    if not link_coln in sm_df.columns:
        raise IOError('missing the link column \'%s\' in teh small! '%link_coln)
    
    """just slice the small
    if not link_coln is None:
        #see if all the columns in the small are in the big
        boolcol = sm_df.columns.isin(bg_df.columns)
        
        if not np.all(boolcol):
            raise IOError('%i columns in sm_df not in teh big: %s'%(boolcol.sum(), sm_df.columns[boolcol]))
        
    else:
        pass #add some check as we wont have the link column in the df"""
        
    #test that the small has something to update
    sboolcol = sm_df.columns.isin(bg_df.columns)
    if not sboolcol.sum() > 1: #link and some others
        raise IOError('sm_df has no addiitional intersect columns with the big \n    big: %s \n    %s small :%s'
                      %(bg_df.columns.tolist(), sm_df.columns.tolist()))
        
    
    #see if we have any intersect
    if not link_coln is None:
        
        #find intersect of links
        bboolidx1 = np.logical_and(
            bg_df[link_coln].isin(sm_df[link_coln]), #bigs'intersect with small
            np.invert(pd.isnull(bg_df[link_coln])) #take only reals
                      ) 
        """
        bg_df[link_coln].isin(sm_df[link_coln]).sum()
        """
        
        sboolidx1 = sm_df[link_coln].isin(bg_df[link_coln].dropna()) #small
        
    else:
        bboolidx1 = bg_df.index.isin(sm_df[link_coln])
        sboolidx1 = sm_df[link_coln].isin(bg_df.index) #small
        
    if not np.any(sboolidx1) or not np.any(bboolidx1):
        raise IOError('no intersect!')
    
    #see if the links (intersect) is unique
    if not link_coln is None:
        #test the big
        if expect_unique_bigs:
            if not bg_df.loc[bboolidx1, link_coln].is_unique:
                raise IOError('link_coln \'%s\' intersect values are not unique on teh big_df'%link_coln)

        
    #test the small
    if not sm_df.loc[sboolidx1, link_coln].is_unique:
        """see update_compress()"""
        raise IOError('link_coln \'%s\' intersect values are not unique on the small_df'%link_coln)
    

        
    #=======================================================================
    # split into intersect and non-intersect parts
    #=======================================================================
    """because we want to allow only the intersect links to be unique,
        we need to split the dfs into intersect vs non-intersect parts, then recombine later"""
    #non intersecting, outside parts
    #bg_out_df = bg_df[np.invert(bboolidx1)]
    
    """dont care about this part
    #sm_out_df = sm_df[np.invert(sboolidx1)]"""
    
    #intersecting, inside parts
    sm_in_df = sm_df.loc[sboolidx1, sboolcol] #on columns and indicies
    bg_in_df = bg_df[bboolidx1]
    
    #===========================================================================
    # split into unique and non unique
    #===========================================================================
    
    if not bg_in_df[link_coln].is_unique:
        #=======================================================================
        # #not unique. only work on the unique portion
        #=======================================================================
        if expect_unique_bigs: 
            raise IOError
        
        bboolidx2 = bg_in_df.duplicated(subset=link_coln, keep=False)
        
        log.warning('found %i (of %i) non unique entries on \'%s\' in the big_df intersect. ignoring these'
                    %(bboolidx2.sum(), len(bboolidx2), link_coln))
        
        bg_inu_df = bg_in_df[np.invert(bboolidx2)].copy()
        
        #adjust the intersect with the small
        sboolidx2 = sm_in_df[link_coln].isin(bg_inu_df[link_coln])
        sm_in_df2 = sm_in_df[sboolidx2]
    
    else:
        sm_in_df2 = sm_in_df.copy()
        bg_inu_df =  bg_in_df.copy()


    #=======================================================================
    # reindex everything by the link 
    #=======================================================================
    if not link_coln is None:
        #big
        if 'temp_pd' in bg_inu_df.columns:
            raise IOError('temporary column name already in frame')
        
        bg_inu_df['temp_pd'] = bg_inu_df.index #temporarluy store the index
        bg_inu_df = bg_inu_df.set_index(link_coln, drop=False, verify_integrity=True)
        
    else:
        pass #means were keying on the bigs original index
        
    #small
    sm_in_df2 = sm_in_df2.set_index(link_coln, drop=True, verify_integrity=True)
        

    #=======================================================================
    # make the update
    #=======================================================================
    bg_inu_df1 = bg_inu_df.copy()
    bg_inu_df1.update(sm_in_df2, overwrite=overwrite)
    
    #=======================================================================
    # rekey
    #=======================================================================
    if not link_coln is None:
        bg_inu_df2 = bg_inu_df1.set_index('temp_pd', drop=True) #reset the index
        del bg_inu_df2.index.name #clear the index name
        
    else:
        bg_inu_df2 = bg_inu_df1.copy()
        
    #=======================================================================
    # re assemble the big
    #=======================================================================
    #those records not in the unique intersect
    bg_remain_df = bg_df[np.invert(bg_df.index.isin(bg_inu_df2.index))] 
    
    #add everything together
    bg_df1 = bg_inu_df2.append(bg_remain_df, verify_integrity=True, sort=False).sort_index()
    
    #=======================================================================
    # check and report
    #=======================================================================
    """
    boolcol = bg_df.columns.isin(sm_df.columns)
    sm_df.columns.isin(bg_df.columns)
    
    view(bg_df3.loc[boolidx1, boolcol])
    view(bg_df_raw.loc[boolidx1, boolcol])
    """
    if overwrite:
        #=======================================================================
        # #see if all the values in the das match the override
        #=======================================================================
        #column inte4rsect
        boolcol = bg_df1.columns.isin(sm_df.columns)
        sboolcol = sm_df.columns.isin(bg_df1.columns)
        
        #find the index intersect
        
        if not link_coln is None:
            boolidx2 = bg_df1[link_coln].isin(sm_df[link_coln]) #column intersect
            sboolidx = sm_df[link_coln].isin(bg_df1[link_coln])
        else:
            boolidx2 = bg_df1.index.isin(sm_df[link_coln]) #column intersect
            sboolidx = sm_df[link_coln].isin(bg_df1.index)
        
        #get intersect slices (with indexes sorted)
        if not link_coln is None:
            bgi_df = bg_df1.loc[boolidx2, boolcol].sort_index(axis=1).sort_values(link_coln)
        else:
            bgi_df = bg_df1.loc[boolidx2, boolcol].sort_index(axis=1).sort_index(axis=0)
            
        smi_df = sm_df.loc[sboolidx, sboolcol].sort_index(axis=1).sort_index(axis=0)
        
        if not bgi_df.shape == smi_df.shape:
            raise IOError('problem with intersect slicing')
        
        #compare slices
        booldf = bgi_df.values == smi_df.values
             
        if not np.all(booldf): 
            raise IOError('failed to override some values')
        
    """todo: add some null checking"""
    
    #see if the indexes match
    if not len(bg_df1) == len(bg_df_raw): 
        raise IOError('index mismatch')
    if not np.all(bg_df1.index.isin(bg_df_raw.index)):
        """order could be lost""" 
        raise IOError('index mismatch')
    if not np.all(bg_df1.columns.isin(bg_df_raw.columns)): 
        """ order could be lost"""
        raise IOError
    
    #=======================================================================
    # wrap up and report
    #=======================================================================
    log.debug('updated %i (of %i) values on %i BIG df entries with overwrite=%s'
             %(sm_df.count().sum(), bg_df_raw.count().sum(),
                                        len(bg_inu_df2), overwrite))
    
    return bg_df1.loc[:, bg_df_raw.columns] #return with columns in the same order


def vlookup( #add column(s) to big based on some link w/ small
        big_df, #big frame to fill/create data from the lkp_d
        lkp_df,
        lkp_coln_t = None, #column names to extract data from. must be in lkp_df, optional in big_df.
            #if None: attach all data
            
        #linking parameters
        blink_coln = None, #link column name in big. if None, use index ON BOTH
        klink_coln = None, #link column name in lookup df. if None, use blink_coln
        
        #value propagation handles
        update = True, #run in update mode so real lkp values overwrite na values in the big
            #true: only real lookup values are copied over
            #false: all lookup values are copied over
        overwrite = False, #for expect_overlap=True, how to handle real values in overlapping columns
            #true: ignore reals in big
            #false: preserve reals in big
        
        #expectations
        allow_null_bl = False, #whether to allow big_df[blink_coln]=na values to propagate
        allow_eq_len = False, #whether to allow the frames to be the same length (very unlikely)
        
        expect_new_cols = True,
        expect_overlap = False, #whether there will be common columns in the big and lkp (other than the link)
        expect_real_overlaps = False, #whether to expect real values in overlapping big columns
        expect_new_rows = False, #whether to expect additional rows in the result
        
        validate= 'm:1', #pd.merge validate key
        indicator = False, #print out merge results (_merge) column
        
        key_relation = 'inner', #whether to expect all the keys match between left and right
            #inner: same keys in left and right
            #left: all the left keys are in the right (right keys not in the left)
            #right:
            #outer: both key sets have keys not found in the other
            
        logger=mod_logger, db_f=False):
    
    """
    I feel like there should be a built in method for this.
    but pd.update seeems to only work with index updates on the big
    
    
    this is a simplified merge_left() ?
    """
    #===========================================================================
    # setup and defaults
    #===========================================================================
    log = logger.getChild('vlookup')
    
    #link column name logic
    if klink_coln is None:
        klink_coln = blink_coln
        
        
    #lookup column defaults
    if lkp_coln_t is None:
        #all columns except the linker
        lkp_coln_t = tuple(lkp_df.drop(klink_coln, axis=1).columns.tolist())
        #raise Error('not implmemented')
        
        
    
        

    #===========================================================================
    # #development checks
    #===========================================================================
        
    if blink_coln is None: 
        raise Error('not implemented')
    
    if not update:
        raise Error('not implemented')
    

    
    #===========================================================================
    # prechecks
    #===========================================================================
    if db_f:
        #=======================================================================
        # expectation logic
        #=======================================================================
        if expect_real_overlaps and not expect_overlap:
            raise Error('expect_overlap=FALSE but expect_real_overlaps=TRUE')
        
        if not expect_real_overlaps and overwrite:
            raise Error('expect_real_overlaps=FALSE but overwrite=TRUE') 
        
        if not expect_overlap and overwrite:
            raise Error('expect_overlap=FALSE but overwrite=TRUE') 
        
        if not expect_new_cols and not expect_overlap:
            raise Error('expect_new_cols=FALSE and expect_overlap=FALSE... what are you adding?') 
        
        
    #=======================================================================
    # data checks
    #=======================================================================
    #get those big values which will be updated by the lookup
    pboolidx = big_df[blink_coln].isin(lkp_df[klink_coln].unique())
    
    #no valid lookup check
    if not np.any(pboolidx):
        log.warning('no overlapping \'%s\' and \'%s\' keys!... skipping'%(blink_coln, klink_coln))
        return big_df
    
                                       
    log.debug('%i (of %i) big \'%s\' keys overlap %i (of %i) \'%s\' lookup keys'%(
        pboolidx.sum(), len(big_df), blink_coln, 
        len(lkp_df[klink_coln].unique()), len(lkp_df), klink_coln))
        
    if db_f:
        #=======================================================================
        # links
        #=======================================================================

        #check that the link columns are in each dset
        if not blink_coln in big_df.columns:
            raise Error('link column \'%s\' missing from the big'%blink_coln)
        if not klink_coln in lkp_df.columns:
            raise Error('link column missing')
        
    
        if not isinstance(lkp_coln_t, tuple):
            raise Error('expected tuple for the \'lkp_coln_t\'')
            
        
        #check the dtypes of the link columns
        if not big_df[blink_coln].dtype.char == lkp_df[klink_coln].dtype.char:

            raise Error('link column type mismatch: \n' + \
                        '    big_df[%s].dtype = %s \n'%(blink_coln, big_df[blink_coln].dtype) + \
                        '    lkp_df[%s].dtype = %s'%(klink_coln, lkp_df[klink_coln].dtype))
        
        #check the lengths
        if len(big_df)==len(lkp_df):
            log.warning('passed columns are the same length..\n youre probably using the wrong function')
            if not allow_eq_len:
                raise Error('allow_eq_len=False')
            
        """new oclumns and overlap are two different conditions.
        
        a big lookup could have overlaps and still add new columns"""
        

        #check for nulls in the  big links
        boolidx = pd.isnull(big_df[blink_coln])
        if np.any(boolidx):
            log.warning('got %i null link values in the big'%boolidx.sum())
            
            if not allow_null_bl:
                raise Error('got null \'%s\' links on the big'%blink_coln)
            
            bnulls_df = big_df.loc[boolidx,:] #slice these out for later
            log.debug('got %i nulls on big link \'%s\'. sclied these out %s'%(
                boolidx.sum(), blink_coln, str(bnulls_df.shape)))
        else:
            bnulls_df = pd.DataFrame(columns=big_df.columns) #just make a dummy empty frame
        
        
        #null sin the small links
        if np.any(pd.isnull(lkp_df[klink_coln])):
            raise Error('got some null link values in the small')
        
        
        #check link relation
        if not key_relation is None:
            krel_calc = hp.np.relation(big_df[blink_coln].unique(), lkp_df[klink_coln].unique(), 
                                       lname='big', rname='lkp',
                                       logger=log)
            
            log.debug('got key_relation=%s'%krel_calc)
            
            if not krel_calc == key_relation:
                raise Error('key relation \'%s\' does not match expectation \'%s\''%(krel_calc, key_relation))
        
        #=======================================================================
        # columns
        #=======================================================================
        
        #check new column expectations
        l = linr(list(lkp_coln_t)+[klink_coln], big_df.columns, result_type='missing')
        if len(l) >0:
            if expect_new_cols:
                log.debug('%i new columns expected from the lookup: %s'%(len(l), l))
            else:
                raise Error('expect_new_cols=FALSe and there are %i new columns in the lookup: \n    %s'%(
                    len(l), l))
        else:
            if expect_new_cols:
                raise Error('expect_new_cols=TRUE and there are no new columns in the lookup')
            
            
        #check column overlap
        l = linr(big_df.columns, lkp_coln_t, result_type='matching')
        if len(l) >0:
            if expect_overlap:
                log.debug('%i columns overlap between frames: %s'%(len(l), l))
            else:
                raise Error('expect_overlap=FALSe and there are %i overlapping columns: %s'%(len(l), l))
            
            #get frame pending updates/overwrites
            big_df_pending = big_df.loc[pboolidx, l]
            
            #check for nulls
            booldf = big_df_pending.isna()
            
            if np.all(booldf):
                #expectation check
                if expect_real_overlaps:
                    raise Error('expect_real_overlaps=TRUE but all %i big values on %i overlap colums are null'%(
                        booldf.sum().sum(), len(l)))
                    
                log.debug('all %i (of %i) big  values on %i overlap colums are null'%(booldf.size, big_df.size, len(l)))
            else:

                #expectation check
                if not expect_real_overlaps:
                    raise Error('expect_real_overlaps=FALSE but there are  %i (of %i) real values in the big'%(
                        booldf.size - booldf.sum().sum(), booldf.size))

                        
                    
                #overwrite statement
                if overwrite:
                    log.debug('overwriting %i (of %i) real values in the big'%(
                        booldf.size - booldf.sum().sum(), booldf.size))
                else:
                    log.debug('preserving %i (of %i) real values in the big'%(
                        booldf.size - booldf.sum().sum(), booldf.size))
                    
            #check for reals
            
            
                    
        else:
            if expect_overlap:
                raise Error('expect_overlap=TRUE and there are no overlaps!')
            
        
    #===========================================================================
    # simple merge -----------------
    #===========================================================================
    #see if any of th elookup columns are in the big columns
    if not linr(lkp_coln_t, big_df.columns, result_type='bool'):
        if overwrite: 
            raise Error('not implemented')
        
        log.debug('performing simple join')
        
        #get the full lookup set
        lkp_colns = set(lkp_coln_t)
        lkp_colns.add(klink_coln) #add the link column

        #perform merge  based on link 
        """needs the reset_index, set_index workaround to preserve the original index"""      
        res_df1 =  big_df.reset_index().merge(lkp_df.loc[:,lkp_colns], 
                                how='left', #only use keys from the left. retains nulls
                               left_on =blink_coln,
                               right_on = klink_coln,

                               sort=False,
                               validate= validate, #check if merge keys are unique in right dataset
                               indicator=indicator, #flag where the rows came from (_merge)
                               )
        
        #cleanup extra columns
        res_df2 = res_df1
        if not blink_coln == klink_coln: 
            """where the link colns are different, the lkp gets added...
                this is redundant"""
            if not klink_coln in lkp_coln_t: #make sure the user didnt ask for it
                res_df2 = res_df1.drop(klink_coln, axis=1)
                
                
        #=======================================================================
        # #add back null big values
        # res_df = res_df2.append(bnulls_df, sort=True)
        #=======================================================================
        res_df = res_df2

    #===========================================================================
    # overlapping merge----------------
    #===========================================================================
    else:
        #===========================================================================
        # update mode
        #===========================================================================
        if update:

            #get just the linkers from the big
            big_linkrs = pd.DataFrame(big_df.loc[:, blink_coln])
            
            #handle link column names
            lkp_df = lkp_df.rename(columns={klink_coln:blink_coln})
            
            #merge lookup data onto this            
            lkp_vals_df = big_linkrs.reset_index().merge(lkp_df, on=blink_coln, 
                                           how='left',
                                           validate=validate).set_index('index')
                                           
            if not np.array_equal(lkp_vals_df.index.values, big_df.index.values):
                """"
                need to figure out something for 1:m
                """
                raise Error('index mismatch')
            
            #update the big with these merge values
            res_df = big_df.copy()
            res_df.update(lkp_vals_df,  overwrite=overwrite)
            
            log.info('filled %i (of %i of %i) nulls on big %s from lookup %s in %s'%(
                big_df.isna().sum().sum() - res_df.isna().sum().sum(), big_df.isna().sum().sum(),
                big_df.size,
                str(big_df.shape), str(lkp_df.shape), lkp_coln_t))
            
            

            
            
        #=======================================================================
        # full propagation
        #=======================================================================
        else:
            """bring over nulls from the lookup as well"""
            raise Error('not implemented')
        

    #===========================================================================
    # handle index
    #===========================================================================
    #simple (m:1) revert to original index
    if len(res_df1)==len(big_df):
        res_df2 = res_df1.set_index('index', drop=True)
        
    #complex (1:m) index expansions... drop old index
    else:
        """this isn't really a vlookup...."""
        res_df2 =  res_df1.drop('index', axis=0)
    
    #===========================================================================
    # post checks-----------------
    #===========================================================================
    if '_merge' in res_df1.columns:
        log.debug('merge results :\n%s'%res_df1['_merge'].value_counts())
        res_df = res_df.drop('_merge', axis=1)
    
    if db_f:
        #index checks
        if not res_df.index.is_unique:
            raise Error('got invalid index on result')
        
        
        if not len(res_df)==len(big_df):
            msg = ('got %i additional rows in the result (%s to %s)'%(len(res_df)-  len(big_df), str(res_df.shape), str(big_df.shape)))
            if not expect_new_rows:
                raise Error(msg)
            else:
                log.debug(msg)
            
        
        #=======================================================================
        # if not linr(res_df.index, big_df.index, 'res_df.index', 'big_df.index', 
        #                            sort_values=True, result_type='exact', logger=log):
        #     raise Error('index mismatch')
        # 
        # 
        # if not np.array_equal(res_df.index.sort_values(), big_df.index.sort_values()):
        #     raise Error('index mismatch')
        #=======================================================================
        
    
    

    
    #===========================================================================
    # wrap
    #===========================================================================
    """todo: add some better reporting and post checking"""
    log.debug('finished with %s and %i (of %i) nulls'%(
        str(res_df.shape), res_df.isna().sum().sum(), res_df.size))
    
    return res_df



    

        
    
#===============================================================================
#SEARCHING ------------------------------------------------------------------
#===============================================================================
def search_str_fr_list( #find where items have all the items in the search_l 
            ser,  #series to search
            search_l, #list of strings to search for in teh series
            search_type='contains', #type fo search to perform (contains or match)
            case=False, #case sensitivite
            all_any = 'all', #flag denoting how to to treat the combined conditional
            logger=mod_logger, **kwargs):  
    """
    #===========================================================================
    # INPUTS
    #===========================================================================
    all_any: 
        all: find rows where every string in the list is there
        any: find rows where ANY string in the list is there
    """
    
    log = logger.getChild('search_str_fr_list')
    
    
    
    if not isinstance(ser, pd.Series):
        log.debug('converted %s to series'%type(ser)) 
        ser = pd.Series(ser)
        #raise Error('expected a series, instaed got a %s'%type(ser))
    
    #starter boolidx series
    df_bool = pd.DataFrame(index = search_l, columns = ser.index)
    

    #loop through and find search results for each string
    for search_str, row in df_bool.iterrows():
        if search_type is 'contains':
            boolcol = ser.astype(str).str.contains(search_str, case=case, **kwargs)
        elif search_type is 'match':
            boolcol = ser.astype(str).str.match(search_str, case=case, **kwargs)
        else:
            raise IOError
        
        df_bool.loc[search_str,:] = boolcol.values.T
        
    #find the result of all rows
    if all_any == 'all':
        boolidx = df_bool.all()
    elif all_any == 'any':
        boolidx = df_bool.any()
    else: 
        log.error('got unexpected kwarg for all_any: \'%s\''%all_any)
        raise IOError
    log.debug('found %i series match from string_l (%i): %s'
                 %(boolidx.sum(), len(search_l), search_l))
    
    if boolidx.sum() == 0: 
        log.warning('no matches found')


            
    return boolidx

#===============================================================================
# OUTPUTS --------------------------------------------------------------------
#===============================================================================
def write_to_csv(filepath, data, #write the df to a csv. intelligent
                  overwrite=False,
                  float_format=None, 
                  index=False, #write the index?
                  logger=mod_logger, 
                  **kwargs ): 
    
    logger=logger.getChild('write_to_csv')

    # checks
    if not isinstance(data, pd.DataFrame):
        if not isinstance(data, pd.Series): 
            raise Error('expected a dataframe, instead got %s'%type(data))
    
    #===========================================================================
    # defaults
    #===========================================================================
    if not filepath.endswith('.csv'): filepath = filepath + '.csv'
        
    if overwrite == False: #don't overwrite
        if os.path.isfile(filepath): #Check whether file exists 
            raise IOError('File exists already: \n %s'%filepath)
        
    #=======================================================================
    # root folder setup
    #=======================================================================
    head, tail = os.path.split(filepath)
    if not os.path.exists(head): os.makedirs(head) #make this directory

    #===========================================================================
    # writing
    #===========================================================================
    try:
        data.to_csv(filepath, float_format = float_format, index=index, **kwargs)
        logger.info('df %s written to file: \n     %s'%(str(data.shape), filepath))
    except:
        logger.warning('WriteDF Failed for filepath: \n %s'%filepath)
        logger.debug('df: \n %s'%data)
        raise IOError
    
    return 

def write_to_xls( #write a dictionary of frames to excel
        filepath, 
        df_set_dict, 
        engine='xlsxwriter',
        overwrite = False, 
        allow_fail = False, #whether to allow a tab to fail writing
        max_chars = 29, #maximum number of characters for a tab
        logger=mod_logger, **kwargs): 
    
    
    #===========================================================================
    # setup defaults
    #===========================================================================
    log=logger.getChild('write_to_xls')
    if not filepath.endswith('.xls'): filepath = filepath + '.xls'
    
    #===========================================================================
    # prechecks
    #===========================================================================
    if os.path.exists(filepath):
        if overwrite: log.warning('filepath exists. overwriting!')
        else: raise IOError('passed file path exists and overwrite=FALSE')
    
    #===========================================================================
    # make the root folder
    #===========================================================================
    head, tail = os.path.split(filepath)
    if not os.path.exists(head): os.makedirs(head)
        
    #===========================================================================
    # data setup
    #===========================================================================
    """NO! use the provided order
    #sort the dictionary by key
    od = OrderedDict(sorted(df_set_dict.items(), key=lambda t: t[0]))"""
    #===========================================================================
    # #write to multiple tabs
    #===========================================================================
    writer = pd.ExcelWriter(filepath, engine=engine)
    
    for df_name, data in df_set_dict.items():
        log.debug("on \'%s\'"%df_name)
        
        #=======================================================================
        # type conversion
        #=======================================================================
        if isinstance(data, pd.DataFrame):
            df = data
        elif isinstance(data, pd.Series):
            df = pd.DataFrame(data)
        elif isinstance(data, dict):
            df = pd.DataFrame(pd.Series(data))
        else:
            raise Error('unepxected type') 
        
        #=======================================================================
        # checking
        #=======================================================================


        if len(df) == 0: 
            log.warning('got empty frame for \'%s\''%df_name)
            continue #skip empty frames
        
        if len(df_name) > max_chars:
            log.warning('tab name length (%i) exceedx max (%i):\n    %s'%(len(df_name), max_chars, df_name))
            df_name = df_name[:max_chars]
        
        #=======================================================================
        # make the write
        #=======================================================================
        try:
            df.to_excel(writer, sheet_name=str(df_name), **kwargs)
        except:
            log.error('failed to write df \'%s\''%df_name)
            if not allow_fail: raise IOError
        
    writer.save()
    log.info('wrote %i frames/tab to:\n  %s'%(len(df_set_dict.keys()), filepath))
    
    return

#===============================================================================
#TUPLE LIKE (sub data) --------------------------------------------------------
#===============================================================================

def tlike_ser_clean( #clean a series of tlike data (into a cleaner tlike set)
        ser_raw,
        leave_singletons = True, #whether to leave single tons (vs dump them into a tuple)
        sub_dtype = None, #for mixed_type=True, what sub_dtype to place on the unitary values
        logger=mod_logger):
    
    #===========================================================================
    # defaults
    #===========================================================================
    log = logger.getChild('tlike_ser_clean')
    
    #activate the sub_dtype (incase it was passed as a string)
    if isinstance(sub_dtype, str):
        sub_dtype = eval(sub_dtype)
        
    if not isinstance(ser_raw, pd.Series):
        raise Error('only valid for series')
    
    #===========================================================================
    # shortcuts
    #===========================================================================
    #see if we are the correct unitary already (sub_dtype already matches)
    if not sub_dtype is None:
        if sub_dtype.__name__ in ser_raw.dtype.name:
            log.warning('series \'%s\' is already unitary (type: %s). skipping'
                      %(ser_raw.name, ser_raw.dtype.name))
            
            #double check were not a string type
            if ser_raw.dtype.char == 'O':
                raise Error('Object type!')
            
            return ser_raw.astype(sub_dtype)

    #===========================================================================
    # poorly formated with nulls
    #===========================================================================
    if not ser_raw.dtype.char == 'O':
        ser = ser_raw.astype(str) #reset type
        ser[ser_raw.isna()] = np.nan #remove all the nulls again
    else:
        ser = ser_raw
    
    
    if not ser.dtype.char == 'O':
        raise TypeError('unexpected type on tlike column \'%s\': %s'%(ser.name, ser.dtype.char))
    
    
    #===========================================================================
    # extract and clean
    #===========================================================================
    log.debug('cleaing tlike series \'%s\' with %i and sub_dtype: \'%s\''%
              (ser_raw.name, len(ser), sub_dtype))
       

    #extract everything into a dict witht eh formatting
    valt_d = tlike_to_valt_d(ser, 
                            leave_singletons = leave_singletons, 
                            sub_dtype = sub_dtype, 
                            leave_nulls = True,
                            logger=log)
    
    ser = pd.Series(valt_d, name=ser_raw.name)
    
    #===========================================================================
    # do some checks
    #===========================================================================
    if not len(ser) == len(ser_raw):
        raise IOError
    
    if not np.all(ser_raw.sort_index().index == ser.sort_index().index):
        raise IOError('index mismatch')
    
    if not ser.count() == ser_raw.count():
        raise IOError('value mismatch')

    return ser.loc[ser_raw.index] #return with the index in the same order
    
def tlike_to_valt_d( #convert a series (or array) of tuple like values into a dictionary of tuples
        tlike_raw, #series or array
        leave_singletons = True, #whether to leave single tons (vs dump them into a tuple)
        sub_dtype = None, #type to force on tuple elements. None: leave as found
        leave_nulls = False, #whether to leave the null values in
        
        expect_tlike = True, #flag whether to expect tuple like values or not
        logger=mod_logger, db_f=False):
    """
    Couldnt get pandas to work with tuples, 
    so this is a workaround by dumping everything into a dictionary
    
    dictionary result is keyed with the series index

    """
    #===========================================================================
    # setups
    #===========================================================================
    log = logger.getChild('tlike_to_valt_d')
    
    
        
    if not sub_dtype is None:
        if isinstance(sub_dtype, str):
            sub_dtype = eval(sub_dtype)
            
        if not inspect.isclass(sub_dtype):
            raise IOError('expected a type for \'sub_dtype\', instead got : \"%s\''%type(sub_dtype))
        
        if sub_dtype == int:
            def smart_int(v):
                return int(float(v))
            sub_dtype  = smart_int
        
        """
        smart_int('123.0')
        type(tlike_raw)
        
        """
    log.debug('on container type %s w/ %i elements and sub_dtype %s'%(
        type(tlike_raw), len(tlike_raw), sub_dtype))
    #===========================================================================
    # prechecks
    #===========================================================================
    #convience converesion from series like df to series
    if isinstance(tlike_raw, pd.DataFrame):
        if tlike_raw.shape[1]==1:
            tlike_raw = tlike_raw.T.iloc[0]
        else:
            raise Error('expected a series')

    
    
    #type checking
    if not (isinstance(tlike_raw, pd.Series) or isinstance(tlike_raw, np.ndarray)):
        raise TypeError('expected tuple or array on tlike_raw')
    
    #all null handling
    if np.all(pd.isnull(tlike_raw)):
        log.warning('got empty tlike %s'%str(tlike_raw.shape))
        """completely empty data sets will always flag as float types."""
        tlike_raw = tlike_raw.astype('O')
        

    
    if not tlike_raw.dtype.char == 'O':
        if expect_tlike:
            raise Error('expected \'object\', instead got unepxected type: %s' %(tlike_raw.dtype))
        else:
            if leave_singletons:
                return tlike_raw.to_dict()
            else:
                return {key:tuple([val]) for key, val in tlike_raw.to_dict().items()}
        
    #===========================================================================
    # type conversion
    #===========================================================================
    if isinstance(tlike_raw, pd.Series):
        ser = tlike_raw
        log.debug('extracting tuple dictionary from series \'%s\' with %i elements'%(tlike_raw.name, len(tlike_raw)))

    elif isinstance(tlike_raw, np.ndarray):
        ser = pd.Series(tlike_raw) #leave as is

    else:
        raise TypeError('unexpected type') #shouldnt hit from above

    #=======================================================================
    # reals. extraction (pulls all singletons too)
    #=======================================================================
    log.debug('assembling values on %i (of %i) reals'%(ser.notna().sum(), len(ser)))
    #id nulls
    boolidx = np.invert(pd.isnull(ser))
    
    #loop and set
    d2 = dict()
    for key, val in ser[boolidx].items():

        if isinstance(val, str):
            if "(" in val or "[" in val:
                """ if were dealing with tuple like + singleton strings, we dont want to evaluate single tons w/o this"""
                try:
                    d2[key] = eval(val) #extract it
                except Exception as e:
                    raise Error('failed to eval \'%s\' key %s = \'%s\' w/ \n    %s'%(ser.name, key, val, e))
            else:
                d2[key] = val #just leave it
        else:
            d2[key] = val
        
    #=======================================================================
    # tupleize
    #=======================================================================
    
    d3 = dict()
    for key, val in d2.items():
        #handle by type
        if isinstance(val, tuple): #already a tuple. just leave it
            nval = val 
        elif isinstance(val, list): #list like. convert it to a tuple
            nval = tuple(val)
        else: #try and convert it
            nval = tuple([val])
        
        if leave_singletons:
            if len(nval) == 1:
                nval1 = nval[0]
            else:
                nval1 = nval
        else:
            nval1 = nval
        #set it
        d3[key] = nval1
            
        log.debug('tupleizing all elements')


            
        
    #===========================================================================
    # sub_dtyping
    #===========================================================================
    if not sub_dtype is None:
        log.debug('setting sub_dtype \'%s\' on %i entries'%(sub_dtype, len(d3)))
        d4 = dict()
        for k, val_t in d3.items():
            try:
                if isinstance(val_t, tuple) or isinstance(val_t, list):#loop and format each
                    #dump and convert (wouldnt work in line)
                    l1 = []
                    for val in val_t:
                        l1.append(sub_dtype(val))
                    
                    d4[k] = tuple(l1)

                else: #singleton. just type it
                    d4[k] = sub_dtype(val_t)
            except:
                raise TypeError('failed to set \'%s\' on key \'%s\' val \'%s\''%(sub_dtype, k, val_t))
            """
            sub_dtype(val_t)
            """
        #log.debug('finished typesetting')
                
    else:
        d4 = d3

    #=======================================================================
    # add the nulls back in
    #=======================================================================
    if leave_nulls and np.any(~boolidx):
        log.debug('re-inserting placeholder keys for %i nulls'%(pd.isnull(ser).sum()))
        for key, val in ser[~boolidx].items():
            d4[key] = np.nan
    else:
        log.debug('omitting keys for %i nulls'%(pd.isnull(ser).sum()))
        
    #===========================================================================
    # do some checks
    #===========================================================================
    if db_f:
        if leave_nulls:
            if not len(d4) == len(tlike_raw):
                raise Error('result length mismatch')
            

    return d4

def tlike_flip( #swap the keys/values on a tlike dataset
        tlike,
        logger=mod_logger,
        **tvkwargs):
    
    log = logger.getChild('tlike_flip')

    #===========================================================================
    # get valtd
    #===========================================================================

    if isinstance(tlike, dict):
        valt_d = tlike
    elif isinstance(tlike, pd.DataFrame):
        valt_d = tlike_to_valt_d(tlike,
                                 leave_nulls = False, #whether to leave the null values in)
                                 **tvkwargs)
    else:
        #todo: 
        raise IOError('unrecognized type')
    
    #===========================================================================
    # helper
    #===========================================================================
    def upd_d(oldk, newk):
        nonlocal d
        
        #add a new key
        if not newk in d.keys():
            d[newk] = set([oldk]) #start the new container
        
        #update an entry
        else:
            d[newk].add(oldk)

        
        
    #===========================================================================
    # flip keys
    #===========================================================================
    d = dict()
    
    for k, valt in valt_d.items():
        if pd.isnull(valt): continue
        #singletons
        if not isinstance(valt, tuple):
            upd_d(k, valt) #just add this one
        
        #tupes
        else:
            for val in valt: #loop and add each
                upd_d(k, val)
                
    #===========================================================================
    # clean format
    #===========================================================================
    d2=dict()
    for k, v_s in d.items():
        d2[k] = tuple(v_s)
        
    log.debug('finished on old %i to new %i'%(len(valt_d), len(d2)))
    
    return d2

def tlike_uq_vals( #get list of unique values in tlike sets
        tlike_raw, #array or series or dict of tuples
        sub_dtype = int, #what sub_dtype to place on the unitary values
        logger=mod_logger,
        ):
    """
    NOTE: if you already have a valt_d built for this set,
        just copy/paste the unique value scripts from below
    
    """
    if isinstance(tlike_raw, list):
        valt_d = dict(zip(range(0,len(tlike_raw)), tlike_raw))
    
    #===========================================================================
    # for array like (build the valt_d)
    #===========================================================================
    elif not isinstance(tlike_raw, dict):
    
        if not tlike_raw.dtype.char =='O':
            raise TypeError('got unexpected type on passed tlike: %s'%(tlike_raw.dtype.name))
        
        
        
        #===========================================================================
        # type conversion
        #===========================================================================
        
        if isinstance(tlike_raw, pd.Series):
            val_ar = tlike_raw.unique()
        elif isinstance(tlike_raw, np.ndarray):
            val_ar = tlike_raw
        else:
            raise TypeError('got unepxected type on tlike: %s'%type(tlike_raw))
        
        
        #===========================================================================
        # extraction
        #===========================================================================
        valt_d = tlike_to_valt_d(val_ar,
                                 leave_singletons = True,
                                 sub_dtype= sub_dtype,
                                 leave_nulls = False) #convert htis intoa  dict
        
    else:
        valt_d = tlike_raw
    
    #===========================================================================
    # get unique indiviual values
    #===========================================================================
    full_s = set()
    for indxr, val_t in valt_d.items():
        if isinstance(val_t, str):
            full_s.add(val_t)
        else:
        
            try: #list like
                full_s.update(val_t)
            except: #singletons
                full_s.add(val_t)
            
        
    return tuple(sorted(full_s))

def tlike_take_first( #drop tlike to unitary by taking the first value
          tlike_raw,
          sub_dtype=int,
          logger=mod_logger, db_f=False,
        ):
    log = logger.getChild('tlike_take_first')
    
    log.debug('on %s w/ %i'%(type(tlike_raw), len(tlike_raw)))
    #===========================================================================
    # pre-convert
    #===========================================================================
    if isinstance(tlike_raw, list):
        valt_d = dict(zip(range(0,len(tlike_raw)), tlike_raw))
    
    #===========================================================================
    # for array like (build the valt_d)
    #===========================================================================
    elif not isinstance(tlike_raw, dict):
    
        if not tlike_raw.dtype.char =='O':
            raise TypeError('got unexpected type on passed tlike: %s'%(tlike_raw.dtype.name))
        
        
        
        #======================================================================
        # #===========================================================================
        # # type conversion
        # #===========================================================================
        # 
        # if not (isinstance(tlike_raw, pd.Series) or isinstance(tlike_raw, np.ndarray)):
        #     raise TypeError('got unepxected type on tlike: %s'%type(tlike_raw))
        # 
        #======================================================================
        
        #===========================================================================
        # extraction
        #===========================================================================
        valt_d = tlike_to_valt_d(tlike_raw,
                                 leave_singletons = True,
                                 sub_dtype= sub_dtype,
                                 leave_nulls = True,
                                 logger=log, db_f=db_f) #convert htis intoa  dict
        
    else:
        valt_d = tlike_raw
        
    #===========================================================================
    # some checks
    #===========================================================================
    if db_f:
        if not len(valt_d) == len(tlike_raw):
            raise Error('result length mismatch')
        
        if not np.array_equal(np.array(valt_d.keys()), tlike_raw.index):
            raise Error('index mismatch')
    
    #===========================================================================
    # loop and build
    #===========================================================================
    res_d = dict()
    for indxr, val_t in valt_d.items():
        #tuple like
        if isinstance(val_t, list) or isinstance(val_t, tuple):
            res_d[indxr] = val_t[0]
            
        #unitqary
        else:
            res_d[indxr] = val_t
            
    #===========================================================================
    # some checks
    #===========================================================================
    if db_f:
        if not len(res_d) == len(tlike_raw):
            raise Error('result length mismatch')
    
    #===========================================================================
    # return result in same type
    #===========================================================================
    
    if isinstance(tlike_raw, pd.Series):
        result = pd.Series(res_d, name=tlike_raw.name, 
                           #dtype=sub_dtype, #need to leave as auto for nans
                           )
    
    elif isinstance(tlike_raw, dict):
        result = res_d
    else:
        raise Error('unexetced data type')
            
    log.debug('converted to unitary \'%s\' w/ %i'%(type(result), len(result)))
        
    return result
    
    



def get_tupl_cnt( #get a series of tuple counts... handling gaps in valt_d
                  ser,
                  valt_d = None, #dictionary of tuple values {index: (value1, value2, ...)}
                  new_name = None,
                  logger=mod_logger,
                  **svkwargs
        ):
    
    #===========================================================================
    # setups and defaults
    #===========================================================================
    log = logger.getChild('get_tupl_cnt')
    if valt_d is None: valt_d = tlike_to_valt_d(ser, svkwargs)
    if new_name is None: new_name = '%s_cnt'%ser.name
    
    #===========================================================================
    # loop and calc
    #===========================================================================
    res_d = dict() #results container
    for indxr in ser.index:
        
        #see if we are in there (empty means null)
        if not indxr in valt_d.keys():
            res_d[indxr] = np.nan
        
        else:
            res_d[indxr] = len(valt_d[indxr])
            
    #===========================================================================
    # wrap up
    #===========================================================================
    
    return pd.Series(res_d, name = new_name)
    

def update_compress( #update all the values in df_bg from the non-null in df_sm (allows NON-unique)
        bg_df_raw,
        sm_df_raw, #always linked by link_coln (for index-index updates, you dont need this fancy script)
        link_coln = None, #link column name to use. if None, use index ON BOTH
        overwrite = False, #if the df_bg value is NOT null, whether to overwrite with df_sm value
        nu_hndl = 'max', #where duplicate sm_df values, how to handle
        logger=mod_logger):

    log = logger.getChild('update_compress')
    
    #get/copy data
    bg_df =  bg_df_raw.copy()
    sm_df = sm_df_raw.copy()
    
    log.debug('updating from %s on %s with overwrite = %s'
              %(str(sm_df.shape), str(bg_df.shape), overwrite))
    
    #=======================================================================
    # checks
    #=======================================================================
    if not link_coln in sm_df.columns:
        raise IOError('missing the link column \'%s\' in teh small! '%link_coln)
    if not link_coln is None:
        #see if all the columns in the small are in the big
        boolcol = sm_df.columns.isin(bg_df.columns)
        if not np.all(boolcol):
            raise IOError('columns mismatch')
        
    else:
        pass #add some check as we wont have the link column in the df
    
    #see if we have any intersect
    if not link_coln is None:
        
        #find intersect of links
        bboolidx1 = np.logical_and(
            bg_df[link_coln].isin(sm_df[link_coln]), #bigs'intersect with small
            np.invert(pd.isnull(bg_df[link_coln]))) #take only reals
        
        sboolidx1 = sm_df[link_coln].isin(bg_df[link_coln].dropna()) #small
        
    else:
        bboolidx1 = bg_df.index.isin(sm_df[link_coln])
        sboolidx1 = sm_df[link_coln].isin(bg_df.index) #small
        
    if not np.any(sboolidx1) or not np.any(bboolidx1):
        raise IOError('no intersect!')
    
    #=======================================================================
    # split into intersect and non-intersect parts
    #=======================================================================
    """because we want to allow only the intersect links to be unique,
        we need to split the dfs into intersect vs non-intersect parts, then recombine later"""
    #non intersecting, outside parts
    bg_out_df = bg_df[np.invert(bboolidx1)]
    
    """dont care about this part
    #sm_out_df = sm_df[np.invert(sboolidx1)]"""
    
    #intersecting, inside parts
    bg_in_df = bg_df[bboolidx1].copy()
    sm_in_df = sm_df[sboolidx1]
    
    #===========================================================================
    # split into unique and non unuique
    #===========================================================================
    sboolidxu = sm_in_df.duplicates(subset = link_coln, keep=False) #True for all duplicates
    
    #inside + has duplicates (non-unique)
    sm_in_n_df = sm_in_df[sboolidxu]
    
    #inside + unique
    sm_in_u_df = sm_in_df[~sboolidxu]
    
    raise IOError('not finished')

def tlike_lkp_d( #lookup some values from a nested set of keys
             indx_lkpt_d, #container of keys {bid: (key1, key2, ...)}. keys the results.
             lkp_d_raw, #container of values to lookup from {key: value}. values of the results
             nu_hndl = 'tuple', #where duplicate lookup values, how to handle
                 #max
                #concat
                #tuple
                #raise
             clean_nulls = False, #whether to clean nulls out of the lkp_d first. dangerous!

                logger=mod_logger
             ):

        """shouldnt need a subdtype because we just use whatever is in the lkp_d"""
        log = logger.getChild('tlike_lkp_d')
        
        #=======================================================================
        # null cleaner
        #=======================================================================
        if clean_nulls:
            lkp_d =dict()
            for k, v in lkp_d_raw.items():
                if pd.isnull(v): continue #skip
                lkp_d[k] = v #add
                
        else:
            lkp_d = lkp_d_raw 
        

        res_d = dict()
        for bid, key_t in indx_lkpt_d.items():
            #===================================================================
            # list like
            #===================================================================
            if isinstance(key_t, tuple) or isinstance(key_t, list):
                #pull this value from the lookup d
                fnd_s = set()
                for key in key_t:
                    try:
                        fnd_s.add(lkp_d[key]) #add it in
                    except:
                        if not key in lkp_d.keys():
                            log.error('lookup keys:\n %s'%list(lkp_d.keys()))
                            raise IOError('bid %i requsted key \'%s\' not found in the lookup d'%(bid, key))
                    
                #apply handle to results
                if len(fnd_s) >1 : #multiple. need a handle
                    lkval = hp.basic.dup_hndl(list(fnd_s), nu_hndl)
                else: #just one match. take it.
                    lkval = list(fnd_s)[0]
                    
            #===================================================================
            # singletons    
            #===================================================================
            else:
                lkval = lkp_d[key_t]
            
            #append the results 
            res_d[bid] = lkval
            
        log.debug('found %i matches with handle: %s'%(len(res_d), nu_hndl))
            
        return res_d

def vlookup_tlike( #perform a join/vlookup from the lkp to the big where the link is duplicated
        big_df_raw, 
        lkp_df, #data set with link_coln. all other columns are compressed and joined onto the big
        link_coln, #column name to use to link data between the two
        lkp_coln, #column name to lookup in the lkp_df
        ind_valt_d = None, #optional tlike dict for the big link tuples {big index: (link_coln value, lcv, lcv...)}
        overwrite = False, #whether to overwrite values in the big_df or not
        res_type = 'valt_d', #how to return teh results
        conc_hndl = 'tuple', #how to concanate the results
        logger=mod_logger,
        ):
    
    """
    This should handle 3 types of link duplication:"
        1) non unique link values in the big:
        2) non unique link values in the lookup
            concat all of these results into a list
        3) tuple like link values in teh big
            lookup all of the matches, and concat results
    """
    #===========================================================================
    # setup
    #===========================================================================
    big_df = big_df_raw.copy()
    log = logger.getChild('vlookup_compress')
    
    #build the expectation columns
    exp_coln = (link_coln, lkp_coln)
    
    #add back onto the big if necessary
    if not lkp_coln in big_df.columns:
        big_df[lkp_coln] = np.nan
    
    #===========================================================================
    # precheck
    #===========================================================================
    #see if all the testing columns are in the reporting columns
    for dname, df in {'big':big_df, 'lkp':lkp_df}.items():
        boolar = np.invert(np.isin(np.array(exp_coln), df.columns))
        if np.any(boolar):
            raise IOError('expected columns in \'%s\' are missing : %s'
                          %(dname, np.array(exp_coln)[boolar]))

    #=======================================================================
    # handle overwrite
    #=======================================================================
    if not overwrite:
        dboolidx = np.logical_and(
            pd.isnull(big_df[lkp_coln]), #no uids
            np.invert(pd.isnull(big_df[link_coln])) #realabids
            )
    else:
        dboolidx = np.invert(pd.isnull(big_df[link_coln]))  #real links
        
    if not np.any(dboolidx):
        log.info('no eligible \'%s\' values to calculate'%lkp_coln)
        return big_df_raw
    
    log.debug('identified %i eligible entries for \'%s\' lookup'%(dboolidx.sum(), lkp_coln))
    #=======================================================================
    # get the lookup values in dictionary form and check them
    #=======================================================================
    #get valt_d for {big index: (lookup value1, v2, v3...)}
    if ind_valt_d is None: 

        #build the bid_valt_d from scratch
        ind_valt_d1 = hp.pd.tlike_to_valt_d(big_df.loc[dboolidx, link_coln],
                            leave_singletons = False, 
                            sub_dtype=int, 
                            leave_nulls=False)
        
    #make a slice of the one we have already
    else:
        ind_valt_d1 = dict()
        for indxr in big_df[dboolidx].index:
            try: #pull the tuple from here
                ind_valt_d1[indxr] = ind_valt_d[indxr]
            except:
                """remember, we dont store nulls into the bid_valtd"""
                log.debug('missing index %i from the ind_valt_d'%indxr)
             
    """
    view(big_df.loc[dboolidx])
    """
    #===========================================================================
    # check it
    #===========================================================================
    #make sure we have all the link values we want in the container
    all_link_vals = np.array(tlike_uq_vals(ind_valt_d1, sub_dtype=int)) #get unique set of lookopu values
    
    boolar = np.invert(np.isin(all_link_vals, lkp_df[link_coln].unique()))
    if np.any(boolar):
        raise IOError('missing %i (of %i) lookup values in the lkp_df: %s'
                      %(boolar.sum(), len(boolar), all_link_vals[boolar]))
    
    #=======================================================================
    # perform the lookup
    #=======================================================================
    """todo: add some singleton matrix style lookup to improve performance"""
    #assessemtn lookup d {abid:uid} for the abids of interest
    lkpi_d = lkp_df[lkp_df[link_coln].isin(all_link_vals)].set_index(link_coln)[lkp_coln].to_dict()
    
    #lookup from the assessment values to get the bid to uid
    indx_lkpv_d = hp.pd.tlike_lkp_d(ind_valt_d1, #container of keys {bid: (key1, key2, ...)}. keys the results.
                 lkpi_d, #container of values to lookup from {key: value}. values of the results
                 nu_hndl = conc_hndl, #how to concanate the results
                 )
                    

    #=======================================================================
    # update results container
    #=======================================================================
    if res_type == 'valt_d': #just give back the raw results
        return indx_lkpv_d

    elif res_type == 'df': #give back an u pdated df
        big_df1 = big_df.copy()
        big_df1.update(pd.Series(indx_lkpv_d, name=lkp_coln), overwrite=overwrite)
    
        log.debug('updated %i entries'%(dboolidx.sum()))
        
        return big_df1
    
    else:
        raise IOError('unregnized \'res_type\'')   

def hcompress( #compress a data frames columns into a column like series of single values
        df_raw, #farme whose columns you want to compress into single values
        dtype = 'ser', #waht type to return the results as
        comp_type_in = tuple, #inner compression type
        comp_type_out = str,  #outer compression type
        logger=mod_logger,
        ):
    
    df = df_raw.copy()
    

    if not isinstance(df, pd.DataFrame):
        """todo, add compresion for a series"""
        raise IOError('got unexpected type for df: %s'%type(df))
    #===========================================================================
    # perform the compression
    #===========================================================================
    res_d = dict()
    for coln, col in df.items():
        
        #get the set of unique values
        val_uql = col.unique().tolist()
        
        #non-unique. simple
        if len(val_uql)==1:
            res_d[coln] = val_uql[0]
        else:
            res_d[coln] = comp_type_out(comp_type_in(val_uql))
            
            
    #===========================================================================
    # container conversions
    #===========================================================================
    if dtype=='ser':
        return pd.Series(res_d)
        
    elif dtype=='dict':
        return res_d
    else:
        raise IOError('not implemmented')
    
def vcompress( #compress values on lkp_col by building a unique set of link_col values
        df, 
        link_coln, #column name to generate unique keys on. results series will be indexed by unique values of these
        lkp_coln, #column name to extract/compress data from
        logger=mod_logger,
        ):
        
    log = logger.getChild('vcompress')
    
    #===========================================================================
    # prechecks
    #===========================================================================
    for coln in [link_coln, lkp_coln]:
        if not coln in df.columns:
            raise IOError('requseted coulmn \'%s\' not found'%coln)
        
    
    try:
        nindex = df[link_coln].astype(int).sort_values().unique()
    except:
        raise TypeError('failed to typeset link column \'%s\''%link_coln)
    
    #start the results series
    res_ser = pd.Series()
    
    #===========================================================================
    # loop and compress
    #===========================================================================
    for link_val in nindex:
        
        #get the data for thsi link val
        lkp_list = df.loc[df[link_coln]==link_val, lkp_coln].unique().tolist()
        
        #set it
        res_ser.loc[link_val] = tuple(lkp_list)
    
    
    return res_ser

def expand_nested( #expand a nested column into separate columns
                df_raw,
                coln, #name of column to separate
                delim='\n',#delimnater to use
                col_nest_delim = ':', #deliminater to use for the column name within the nested data
                prfx = 'n_', #prefix to add to unnested column names

                logger=mod_logger):
    
    log = logger.getChild('expand_nested')
    
    df = df_raw.copy(deep=True)
    
    log.debug('on df %s and coln \'%s\''%(str(df.shape), coln))
    #===========================================================================
    # prechecks
    #==========================================================================
    if not coln in df.columns:
        raise IOError
    
    if not df.loc[:, coln].dtype == np.dtype('object'):
        raise IOError #should be an 'object' string type
    
    #===========================================================================
    # setup
    #===========================================================================
    #dump the column into an array
    col_ar = df.loc[:, coln].values
    
    
    
    #===========================================================================
    # loop through and extract the data
    #===========================================================================
    data_d = dict()  #container for all the data
    coln_s = oset() #container for column names
    
    for i, val in enumerate(col_ar):
        log.debug('extractginf from data: \n%s'%val)
        vals_l =  re.split(delim, val) #split this string  by teh delim
        
        #=======================================================================
        # extract the nested values
        #=======================================================================
        d = dict() #empty container for this row
        
        for nestv in vals_l: #loop through the nested values
            if nestv == '': continue #skip spaces
            
            nest_vals_l = re.split(col_nest_delim, nestv) #split this string  by teh delim
            
            if len(nest_vals_l) == 1:
                log.warning('for %i didnt find any data. skipping'%i)
                continue
            
            elif len(nest_vals_l) > 2:
                log.error('for %i (%s) found multiple delims \'%s\''%(i, nestv, col_nest_delim))
                raise IOError #should only get 2 entries here
            
            k, v = nest_vals_l
            
            k = k.strip() #remove leading and trailing
            v = v.strip()
            
            coln_s.add(k) #add the first entry to the set of column names
            
            #check if we already have an entry for this columns
            if k in d.keys():
                log.warning('found duplicate entry for \'%s\' on %i. concanateing'%(k, i))
                
                v = d[k] + ', '+ v #just add it to the back (more nesting!)
            
            d[k] = v #store this
            
        data_d[i] = d #store everything for this entry
            

        
    #===========================================================================
    # build this nested dictionary into a data frame
    #===========================================================================
    #add the prefix
    

    df_unest = pd.DataFrame(index = df.index, columns = coln_s) #the new container
    
    for i, nest_d in data_d.items():

        #=======================================================================
        # add this data onto this row       
        #=======================================================================
        df_unest.iloc[i, :] = nest_d
        
    #add teh prefix to the column names
  
    col_l = []
    for cn in df_unest.columns.tolist():
        col_l.append('%s%s'%(prfx, cn))
        
    df_unest.columns = col_l
       
    log.debug('extracted nested data from \'%s\' with %s and headers: %s'
              %(coln, str(df_unest.shape), df_unest.columns.tolist()))
    #===========================================================================
    # add this back to the full data set 
    #===========================================================================
    """
    view(df_new)
    view(df_m)
    """
    
    df = df.drop(coln, axis=1) #drop the nested column
    df_m = df.join(df_unest) #join and return
     
    return df_m


def conc_nu_vals( #concate non-unique entries (per axis) into a tuple .returns a series
            df, #data set to concanate
            axis = 1, #axis to concanate on
            ser_name = None,
            logger=mod_logger):
              
    log = logger.getChild('conc_nu_vals')
    log.debug('concanating non-unique values on %s'%(str(df.shape)))
    
    res_ser = pd.Series(index=df.columns, name = ser_name) #start the results series
    #===========================================================================
    # loop through each column
    #===========================================================================
    if axis == 1:
        for coln, col in df.iteritems():
            
            uvals = col.unique().tolist()
            
            if len(uvals) == 1: #values unique here
                resv = uvals[0]
            else: #non unique values
                resv = tuple(uvals)
            
            #set this
            res_ser[coln] = resv
            
    else:
        raise IOError #dome
        
    return res_ser


def is_tlike( #return a boolean series of elements that are tupleike
              data = None, #raw data (tlike data will be extracted)
              valt_d = None, #tupleized dict
              result_type='ser', #format to return results
              logger=mod_logger, db_f = False, 
        ):
    
    log = logger.getChild('is_tlike')
    

    #===========================================================================
    # get data
    #===========================================================================
    if valt_d is None:
        #=======================================================================
        # precheck
        #=======================================================================
        if data is None:
            raise Error('need data or valt_d')
        
        """todo: handle data frame conversions"""
        if not (hasattr(data,  'dtype') or isinstance(data, pd.DataFrame)):
            raise Error('unrecognized data type')
        
        #=======================================================================
        # #type check
        #=======================================================================
        if not data.dtype.char == 'O':
            log.debug('got passed data type \'%s\' which does not support tupelizing... returning all false'%(
                data.dtype.name))
            
            #resturn dummy results
            res_ser = pd.Series(index=data.index, dtype=bool)
            if result_type == 'ser':
                return res_ser, data.to_dict()
            else:
                raise Error('dome')
            
            
        
        #=======================================================================
        # pull the data
        #=======================================================================
        valt_d = tlike_to_valt_d(data, leave_nulls=True, logger=log)
        
        
    if not isinstance(valt_d, dict):
        raise Error('expected a dict for valt_d, instead got %s'%type(valt_d))
    
    #===========================================================================
    # assmble boolean
    #===========================================================================
    res_d = dict()
    
    #loop through and identify if the value is tupleike or not
    for indxr, val in valt_d.items():
        if isinstance(val, str):
            res_d[indxr] = False
        elif (isinstance(val, tuple) or isinstance(val, list)):
            res_d[indxr] = True
        else:
            res_d[indxr]= False
            
    #===========================================================================
    # result it
    #===========================================================================
    if result_type == 'dict':
        return res_d, valt_d
    
    res_ser = pd.Series(res_d, name='is_tlike', dtype=bool)
    log.debug('finished identifying tlikes %i=TRUE (of %i)'%(
        res_ser.sum(), len(res_ser)))
    
    #===========================================================================
    # post check
    #===========================================================================
    if db_f:
        if not data is None:
            """need to fix this to handle multiple data types"""
            if not np.array_equal(res_ser.index, data.index):
                raise Error('result index mismatch')
        
    
    if result_type == 'ser':
        return res_ser, valt_d
    else:
        raise Error('dome')
    
        

        

#===============================================================================
# MISC --------------------------------------------------------------------
#===============================================================================

def big_splitter( #helper function to split large data sets, run some func, then recombine
        func,
        df_raw,
        split_len, #cant use defaults with variable lengths?
        *fvars, #variables to pas to the func (first has to be the df)
        **fkwargs):

    #===========================================================================
    # setups and defaults
    #===========================================================================
    log = mod_logger.getChild('big_splitter')
    
    chunks = math.ceil(len(df_raw) / split_len) #number of cuncks to use
    
    log.info('executing \'%s\' on data %s in %i chunks with %i kwargs'
             %(func, str(df_raw.shape), chunks, len(fkwargs)))
    
    df = df_raw.copy().sort_index()
    #===========================================================================
    # log stuff
    #===========================================================================
    for indxr, k in enumerate(fvars): log.debug('vars %i: %s'%(indxr, k))
    for k, v in fkwargs.items(): log.debug('kwarg %s: %s'%(k, v))
    
    if not df.index.is_unique:
        raise IOError
    
    #===========================================================================
    # loop through and handle chunks
    #===========================================================================
    res_df = pd.DataFrame() #start results container
    
    lceil = 0 #set last ceiling range
    for chnk in range(0, chunks, 1):
        
        #=======================================================================
        # get this slice
        #=======================================================================
        #new ceiling
        nceil = min(split_len + chnk*split_len, len(df))
        
        #make this slice
        
        df_chnk = df.iloc[lceil: nceil, :]
        log.info('iterating chunk %i (of %i) %s from %i to %i \n'
                 %(chnk, chunks, str(df_chnk.shape), lceil, nceil))
        
        if not len(df_chnk) <= split_len:
            raise IOError
        
        #set last ceil
        lceil = nceil
        #=======================================================================
        # get the results
        #=======================================================================
        res_df_chnk = func(df_chnk, *fvars, **fkwargs)
        
        #check these results
        if not isinstance(res_df_chnk, pd.DataFrame):
            raise IOError
        
        if not len(res_df_chnk) == len(df_chnk):
            raise IOError
        
        if not np.all(res_df_chnk.index == df_chnk.index):
            raise IOError
        #=======================================================================
        # add these back in
        #=======================================================================
        res_df = res_df.append(res_df_chnk, sort=False)
        
        gc.collect() #force garbage collection
        
    #===========================================================================
    # wrap up 
    #===========================================================================
    #checking
    if not len(res_df) == len(df):
        raise IOError
    
    if not np.all(res_df.index == df.index):
        raise IOError
    
    log.debug('finished with %s'%(str(res_df.shape)))
    
    return res_df

def data_report( #generate a data report on a frame
        df,
        out_filepath = None, #Optional filename for writing the report xls to file
        
        
        
        include_df = False, #whether to include the full dataset
        
        #value report selection
        val_rpt=True,
        skip_unique = True, #whether to skip attribute value count p ublishing on unique values
        max_uvals = 500, #maximum number of unique value check 
        
        #value report behavcior
        vc_dropna = False, #whether to drop nas from the value count tabs
        
        logger = mod_logger):
    
    #===========================================================================
    # setup
    #===========================================================================
    log = logger.getChild('data_report')
    
    #setup results ocntainer
    res_df = pd.DataFrame(index = df.columns, columns=('empty','dtype', 'isunique','unique_vals', 'nulls', 'reals', 'real_frac', 'mode'))
    
    
    res_d = dict() #empty container for unique values
    
    #===========================================================================
    # loop and calc
    #===========================================================================
    for coln, col_ser in df.iteritems():
        log.debug('collecting data for \'%s\''%coln)
        res_df.loc[coln, 'empty'] = len(col_ser) == col_ser.isna().sum()

        #type
        res_df.loc[coln, 'dtype'] = str(col_ser.dtype.name)
        
        #unique
        res_df.loc[coln, 'isunique'] = str(col_ser.is_unique)
        
        #unique values
        uq_vals_cnt = len(col_ser.unique())
        res_df.loc[coln, 'unique_vals'] = uq_vals_cnt
        
        #nulls
        res_df.loc[coln, 'nulls'] = col_ser.isna().sum()
        res_df.loc[coln, 'reals'] = len(col_ser) - col_ser.isna().sum()
        
        res_df.loc[coln, 'real_frac'] =  float((len(col_ser) - col_ser.isna().sum()))/float(len(col_ser))
        
        #mode
        if len(col_ser.mode()) ==1:
            res_df.loc[coln, 'mode'] = col_ser.mode()[0]

        
        #=======================================================================
        # float reports
        #=======================================================================
        
        if np.issubdtype(col_ser.dtype, np.number):
            res_df.loc[coln, 'min']=col_ser.min()
            res_df.loc[coln, 'max']=col_ser.max()
            res_df.loc[coln, 'mean']=col_ser.mean()
            res_df.loc[coln, 'median']=col_ser.median()
            res_df.loc[coln, 'sum']=col_ser.sum()
            

            
        
        #=======================================================================
        # value reports
        #=======================================================================
        if not val_rpt: continue
        #unique ness check
        if skip_unique and col_ser.is_unique:
            log.warning('skipping val report for \'%s\''%coln)
            continue
        
        #ratio check
        if uq_vals_cnt > max_uvals:

            log.info('skippin val report for \'%s\' unique vals (%i) > max (%i)'%(
                coln, uq_vals_cnt, max_uvals))
            continue

        vc_df = pd.DataFrame(col_ser.value_counts(dropna=vc_dropna))
        
        if len(vc_df)> 0:
            res_d[coln] = vc_df
        else:
            """shouldnt trip if dropna=True?"""
            log.warning('got no value report for \'%s\''%coln)
        
        
    #===========================================================================
    # wrap up
    #===========================================================================
    #create a new dict with this at the front
    res_d1 = {'_smry':res_df} 
    res_d1.update(res_d)
    
    if include_df:
        res_d1['data'] = df

    """
    res_d1.keys()
    """
    #===========================================================================
    # write
    #===========================================================================
    if not out_filepath is None:
        log.debug('sending report to file:\n    %s'%out_filepath)
        hp.pd.write_to_xls(out_filepath, res_d1, logger=log, allow_fail=True)
    
    
    return res_d1

        
        
        
    
    

