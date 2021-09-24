'''
Created on Feb. 15, 2021

@author: cefect
'''


#==========================================================================
# logger setup-----------------------
#==========================================================================
import logging, configparser, datetime



#==============================================================================
# imports------------
#==============================================================================
import os
import numpy as np
import pandas as pd

#==============================================================================
# # custom
#==============================================================================
from hp.exceptions import Error

from hp.pd import view
from hp.oop import Basic


class Plotr(Basic):
    
    #===========================================================================
    # parameters from control file
    #===========================================================================
    #[plotting]

    color = 'black'
    linestyle = 'dashdot'
    linewidth = 2.0
    alpha =     0.75        #0=transparent 1=opaque
    marker =    'o'
    markersize = 4.0
    fillstyle = 'none'    #marker fill style
    impactfmt_str = '.2e'
        #',.0f' #Thousands separator
        
    impactFmtFunc = None
    
    #===========================================================================
    # controls
    #===========================================================================
    fignum = 0 #counter for figure numbers
    
    #===========================================================================
    # defaults
    #===========================================================================
    val_str='*default'
        
    """values are dummies.. upd_impStyle will reset form attributes"""
    impStyle_d = {
            'color': 'black',
            'linestyle': 'dashdot',
            'linewidth': 2.0,
            'alpha':0.75 , # 0=transparent, 1=opaque
            'marker':'o',
            'markersize':  4.0,
            'fillstyle': 'none' #marker fill style
                            }

    
    def __init__(self,


                 impStyle_d=None,
                 
                 #init controls
                 init_plt_d = {}, #container of initilzied objects
 
                  #format controls
                  grid = True, logx = False, 
                  
                  
                  #figure parametrs
                figsize     = (6.5, 4), 
                    
                #hatch pars
                    hatch =  None,
                    h_color = 'blue',
                    h_alpha = 0.1,
                    
                    impactFmtFunc=None, #function for formatting the impact results
                        
                        #Option1: pass a raw function here
                        #Option2: pass function to init_fmtFunc
                        #Option3: use 'impactfmt_str' kwarg to have init_fmtFunc build
                            #default for 'Model' classes (see init_model)


                 **kwargs
                 ):
        


        
        super().__init__( **kwargs) #initilzie teh baseclass

        #=======================================================================
        # attached passed        
        #=======================================================================

        self.plotTag = self.tag #easier to store in methods this way
 
        self.grid    =grid
        self.logx    =logx
 
        self.figsize    =figsize
        self.hatch    =hatch
        self.h_color    =h_color
        self.h_alpha    =h_alpha
        
        #init matplotlib
        if len(init_plt_d)==0:
            self.init_plt_d = self._init_plt() #setup matplotlib
        else:
            for k,v in init_plt_d.items():
                setattr(self, k, v)
                
            self.init_plt_d = init_plt_d
        

            

        
        
        self.logger.debug('init finished')
        
        """call explicitly... sometimes we want lots of children who shouldnt call this
        self._init_plt()"""
        

    
    def _init_plt(self,  #initilize matplotlib
                #**kwargs
                  ):
        """
        calling this here so we get clean parameters each time the class is instanced
        
        
        """

        
        #=======================================================================
        # imports
        #=======================================================================
        import matplotlib
        matplotlib.use('Qt5Agg') #sets the backend (case sensitive)
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
        
        #spacing parameters
        matplotlib.rcParams['figure.autolayout'] = False #use tight layout
        
        #legends
        matplotlib.rcParams['legend.title_fontsize'] = 'large'
        
        self.plt, self.matplotlib = plt, matplotlib
        
        self.logger.info('matplotlib version = %s'%matplotlib.__version__)
        return {'plt':plt, 'matplotlib':matplotlib}
    

        

        
        

    def _postFmt(self, #text, grid, leend
                 ax, 

                 
                 grid=None,
                 
                 #plot text
                 val_str=None,
                 xLocScale=0.1, yLocScale=0.1,
                 
                 #legend kwargs
                 legendLoc = 1,
                 
                 legendHandles=None, 
                 legendTitle=None,
                 ):
        
        #=======================================================================
        # defaults
        #=======================================================================
        plt, matplotlib = self.plt, self.matplotlib
        if grid is None: grid=self.grid
        
        #=======================================================================
        # Add text string 'annot' to lower left of plot
        #=======================================================================
        if isinstance(val_str, str):
            xmin, xmax = ax.get_xlim()
            ymin, ymax = ax.get_ylim()
            
            x_text = xmin + (xmax - xmin)*xLocScale # 1/10 to the right of the left axis
            y_text = ymin + (ymax - ymin)*yLocScale #1/10 above the bottom axis
            anno_obj = ax.text(x_text, y_text, val_str)
        
        #=======================================================================
        # grid
        #=======================================================================
        if grid: ax.grid()
        

        #=======================================================================
        # #legend
        #=======================================================================
        if isinstance(legendLoc, int):
            if legendHandles is None:
                h1, l1 = ax.get_legend_handles_labels() #pull legend handles from axis 1
            else:
                assert isinstance(legendHandles, tuple)
                assert len(legendHandles)==2
                h1, l1 = legendHandles
            #h2, l2 = ax2.get_legend_handles_labels()
            #ax.legend(h1+h2, l1+l2, loc=2) #turn legend on with combined handles
            ax.legend(h1, l1, loc=legendLoc, title=legendTitle) #turn legend on with combined handles
        
        return ax
    
    def _tickSet(self,
                 ax,
                 xfmtFunc=None, #function that returns a formatted string for x labels
                 xlrot=0,
                 
                 yfmtFunc=None,
                 ylrot=0):
        

        #=======================================================================
        # xaxis
        #=======================================================================
        if not xfmtFunc is None:
            # build the new ticks
            l = [xfmtFunc(value) for value in ax.get_xticks()]
                  
            #apply the new labels
            ax.set_xticklabels(l, rotation=xlrot)
        
        
        #=======================================================================
        # yaxis
        #=======================================================================
        if not yfmtFunc is None:
            # build the new ticks
            l = [yfmtFunc(value) for value in ax.get_yticks()]
                  
            #apply the new labels
            ax.set_yticklabels(l, rotation=ylrot)
        
    def _get_val_str(self, #helper to get value string for writing text on the plot
                     val_str, #cant be a kwarg.. allowing None
                     impactFmtFunc=None,
                     ):
        """
        generally just returns the val_str
            but also provides some special handles
        """
        #=======================================================================
        # defaults
        #=======================================================================
        if impactFmtFunc is None: impactFmtFunc=self.impactFmtFunc
        if val_str is None:
            val_str = self.val_str
        
        #=======================================================================
        # special keys
        #=======================================================================
        if isinstance(val_str, str):
            if val_str=='*default':
                assert isinstance(self.ead_tot, float)
                val_str='total annualized impacts = ' + impactFmtFunc(self.ead_tot)
            elif val_str=='*no':
                val_str=None
            elif val_str.startswith('*'):
                raise Error('unrecognized val_str: %s'%val_str)
                
        return val_str
    
    #===========================================================================
    # OUTPUTTRS------
    #===========================================================================
    def output_fig(self, 
                   fig,
                   
                   #file controls
                   out_dir = None, overwrite=None, 
                   out_fp=None, #defaults to figure name w/ a date stamp
                   fname = None, #filename
                   
                   #figure write controls
                 fmt='svg', 
                  transparent=True, 
                  dpi = 150,
                  logger=None,
                  ):
        #======================================================================
        # defaults
        #======================================================================
        if out_dir is None: out_dir = self.out_dir
        if overwrite is None: overwrite = self.overwrite
        if logger is None: logger=self.logger
        log = logger.getChild('output_fig')
        
        if not os.path.exists(out_dir):os.makedirs(out_dir)
        #=======================================================================
        # precheck
        #=======================================================================
        
        assert isinstance(fig, self.matplotlib.figure.Figure)
        log.debug('on %s'%fig)
        #======================================================================
        # output
        #======================================================================
        if out_fp is None:
            #file setup
            if fname is None:
                try:
                    fname = fig._suptitle.get_text()
                except:
                    fname = self.name
                    
                fname =str('%s_%s'%(fname, datetime.datetime.now().strftime('%Y%m%d'))).replace(' ','')
                
            out_fp = os.path.join(out_dir, '%s.%s'%(fname, fmt))
            
        if os.path.exists(out_fp): 
            assert overwrite
            os.remove(out_fp)

            
        #write the file
        try: 
            fig.savefig(out_fp, dpi = dpi, format = fmt, transparent=transparent)
            log.info('saved figure to file:   %s'%out_fp)
        except Exception as e:
            raise Error('failed to write figure to file w/ \n    %s'%e)
        
        return out_fp
    