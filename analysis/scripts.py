'''
Created on Mar. 27, 2022

@author: cefect
'''
 

import qgis.core
import pandas as pd
import numpy as np


#===============================================================================
# setup matplotlib
#===============================================================================
 
import matplotlib
matplotlib.use('Qt5Agg') #sets the backend (case sensitive)
matplotlib.set_loglevel("info") #reduce logging level
import matplotlib.pyplot as plt

#set teh styles
plt.style.use('default')

#font
matplotlib_font = {
        'family' : 'serif',
        'weight' : 'normal',
        'size'   : 8}

matplotlib.rc('font', **matplotlib_font)
matplotlib.rcParams['axes.titlesize'] = 10 #set the figure title size
matplotlib.rcParams['figure.titlesize'] = 16
matplotlib.rcParams['figure.titleweight']='bold'

#spacing parameters
matplotlib.rcParams['figure.autolayout'] = False #use tight layout

#legends
matplotlib.rcParams['legend.title_fontsize'] = 'large'

print('loaded matplotlib %s'%matplotlib.__version__)


from ricorde.tcoms import TComs
from hp.oop import Session as baseSession

class Session(TComs, baseSession):
    
    def __init__(self, **kwargs):
        
        super().__init__( 
                         data_retrieve_hndls={},
                         #prec=prec,
                         **kwargs)
    
    def plot_beach_pts_capped(self,
                              data_fp=None,
                              ):
        """
        see build_beach2()
        """
        
        #load from csv
        df_raw = pd.read_csv(data_fp)
        
        return
        
        #plot
        
        self.plot_hand_vals(sraw, 
                    title='cap_samples',
            xval_lines_d={'max':vmax,'min':vmin}, 
                label=os.path.basename(smpls_fp),logger=log)
        
    def plot_hand_vals(self):
        """check the BC branch?"""
        pass
