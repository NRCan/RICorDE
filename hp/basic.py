'''
Created on Dec. 29, 2021

@author: cefect
'''
import pprint

def set_info( #get all the  membership info from two containers
        left, right,
             result='elements'):
    
    el_d =  {
        'diff_left':set(left).difference(right), #those in left no tin right
        'diff_right':set(right).difference(left),
        'union':set(left).union(right),
        'intersection':set(left).intersection(right),
        'symmetric_difference':set(left).symmetric_difference(right),        
        }
    
    if result=='elements':
        return el_d
    elif result=='counts':
        return {k:len(v) for k,v in el_d.items()}
    else:
        raise ValueError('unrecognized results key \'%s\''%result)
    
def get_dict_str(d, #fancy formatting of a diciontary into one string (usefull for plotting)
                 num_format = '{:.2f}',
                 del_l = ['{', '}', '\'', ','],
                 #compact=True,
                 ):
    
    #convert to strings
    str_d = dict()
    for k,raw in d.items():
        if 'float' in type(raw).__name__:
            str_d[k] = num_format.format(raw)
        elif isinstance(raw, int):
            str_d[k] = str(raw)
        else:
            str_d[k] = str(raw)
            
    #get pretty print
    #txt = pprint.PrettyPrinter(indent=4).pformat(str_d, width=10)
    txt = pprint.pformat(str_d, width=30, indent=0, compact=True, sort_dicts =False)
    
    #remove some chars
    for c in del_l: 
        txt = txt.replace(c, '') #clear some unwanted characters..
    
    return txt