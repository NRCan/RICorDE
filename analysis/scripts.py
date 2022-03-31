'''
Created on Mar. 27, 2022

@author: cefect
'''
 
import os
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


from ricorde.scripts import Session as baseSession
from hp.pd import view

 

class Session(baseSession):
    colorMap='cool'
    fignum=0
 
    
    def __init__(self, 
               #figure parametrs
                figsize     = (6.5, 4), 
                overwrite=True,
                
                 **kwargs):
        
        
        data_retrieve_hndls = {
            'b2Plotting':{
                'compiled':lambda **kwargs:self.load_b2(**kwargs), #only rasters
                #'build':lambda **kwargs:self.build_dem(**kwargs),
                },
            }
        
        super().__init__( 
                         data_retrieve_hndls=data_retrieve_hndls,overwrite=overwrite,
                         #prec=prec,
                         **kwargs)
        
        
        #=======================================================================
        # attachments
        #=======================================================================
        self.figsize=figsize
    
    #===========================================================================
    # loaders----------
    #===========================================================================
    def load_b2(self,
                fp=None,
                logger=None,
                dkey=None,
                ):
        
        assert dkey=='b2Plotting'
        
        data = self.load_pick(fp=fp, dkey=dkey)
        
        df_raw = data.pop('data')
        
        b1Bounds = data.pop('b1Bounds')
        
        return df_raw, b1Bounds
    
    #===========================================================================
    # plotters---------
    #===========================================================================
    def plot_beach_pts_capped(self,
                              df_raw=None,
                              xlim=(0, 10.0),
                              ):
        """
        see build_beach2()
        
        TODO: set xlims automatically from raw values
        
        """
        #=======================================================================
        # defaults
        #=======================================================================
        log = self.logger.getChild('plot_beach_pts_capped')
        #=======================================================================
        # retrieve
        #=======================================================================
        if df_raw is None:
            df_raw, b1Bounds = self.retrieve('b2Plotting')
        
 
        
        assert df_raw.notna().all().all()
        """
        view(df_raw)
        """
        
         
        
        #=======================================================================
        # #plot raws
        #=======================================================================
 
        vmax, vmin = b1Bounds['qhi'], b1Bounds['qlo']
        ofp_d = dict()
        for sName, col in df_raw.items():
        
            
            fig = self.plot_hand_vals(col.rename('beach2 HAND values (m)'), 
                        title='beach2 samples (%s)'%sName,
                xval_lines_d={'max':vmax,'min':vmin}, 
                    label=sName,xlim=xlim,
                    logger=log)
            
            ofp_d[sName] = self.output_fig(fig, logger=log)
         
        return ofp_d
        
 
        
        
        
    #===========================================================================
    # plotters----
    #===========================================================================
    
    def plot_hand_vals(self, #histogram of hand values
                   sraw,
                   xval_lines_d = {},
                   title=None,
                   label='HAND values', 
                   logger=None,
                   figsize=None,
                   stat_keys = ['min', 'max', 'median', 'mean', 'std'],
                   style_d = {},
                   
                   xlim=None,
                   binWidth=0.1,
                   colorMap=None,
                   ):
        """
        called by:
            get_sample_bounds
        """
        #=======================================================================
        # defaults
        #=======================================================================
        if logger is None: logger=self.logger
        log=logger.getChild('plot_hand_vals')
        
        if colorMap is None: colorMap=self.colorMap
        
        if title is None:
            title= sraw.name
        
            
        if figsize is None:
            figsize=self.figsize
        #=======================================================================
        # data
        #=======================================================================
        assert isinstance(sraw, pd.Series)
        
        data = sraw.dropna().values
        
 
        #======================================================================
        # figure setup
        #======================================================================
        plt.close()
        fig = plt.figure(self.fignum, 
                         figsize=figsize,
                     tight_layout=False,
                     constrained_layout = False,
                     )
        self.fignum+=1
        
        fig.suptitle(title)
 
        
        ax = fig.add_subplot(111)
        
        ax.set_ylabel('count')
        ax.set_xlabel(sraw.name)
        
        """
        plt.show()
        """
        if not xlim is None:
            ax.set_xlim(xlim)
        #=======================================================================
        # #add the hist
        #=======================================================================
        histVals_ar, bins_ar, patches = ax.hist(
            data, 
            bins=np.arange(data.min(), data.max()+binWidth, binWidth), 
            stacked=False,  label=label,
            alpha=0.9, **style_d)
        
        assert len(bins_ar)>1, '%s only got 1 bin!'%title
        
        """throwing warning"""
        ax.set_xticklabels(['%.1f'%value for value in ax.get_xticks()])
        
        
        #===================================================================
        # #add the summary stats
        #===================================================================

        bin_width = round(abs(bins_ar[1]-bins_ar[0]), 3)

        
        
        stat_d = {
            **{'count':len(data), #real values count
               'zeros (count)':(sraw == 0).sum(), #pre-filter 
               'bin width':bin_width,
               #'bin_max':int(max(histVals_ar)),
               },
            **{k:round(getattr(sraw, k)(), 3) for k in stat_keys}}
        
 
 
            
        #dump into a string
        annot = label #start witht the data name
        for k, v in stat_d.items():
            annot = annot + '\n%s=%s' % (k, v)
        
        anno_obj = ax.text(0.5, 0.5, annot, transform=ax.transAxes, va='center')
        
        
        #=======================================================================
        # draw vertical bands
        #=======================================================================
        
                # get colors
 
        cvals = list(xval_lines_d.keys())
        cmap = plt.cm.get_cmap(name=colorMap) 
        newColor_d = {k:matplotlib.colors.rgb2hex(cmap(ni)) for k, ni in dict(zip(cvals, np.linspace(0, 1, len(cvals)))).items()}
        
 
 
        for i, (label, xval) in enumerate(xval_lines_d.items()):
            ax.axvline(x=xval,  linewidth=0.5, linestyle='dashed', 
                       color=newColor_d[label], label='%s=%.2f'%(label, xval))
            
        #=======================================================================
        # post formatting
        #=======================================================================
        ax.legend()

        
        #=======================================================================
        # wrap
        #=======================================================================
        """
        plt.show()
        """
        return fig
        
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
        
        assert isinstance(fig, matplotlib.figure.Figure)
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
                    
                fname =str('%s_%s'%(fname, self.layName_pfx)).replace(' ','')
                
            out_fp = os.path.join(out_dir, '%s.%s'%(fname, fmt))
            
        if os.path.exists(out_fp): 
            assert overwrite
            os.remove(out_fp)

            
        #write the file
        try: 
            fig.savefig(out_fp, dpi = dpi, format = fmt, transparent=transparent)
            log.info('saved figure to file:\n   %s'%out_fp)
        except Exception as e:
            raise IOError('failed to write figure to file w/ \n    %s'%e)
        
        return out_fp

