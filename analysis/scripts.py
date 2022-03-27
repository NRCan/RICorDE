'''
Created on Mar. 27, 2022

@author: cefect
'''


class Session(TComs, baseSession):
    
    def plot_beach_pts_capped(self):
        """
        see build_beach2()
        """
        
        #load from csv
        
        #plot
        
        self.plot_hand_vals(sraw, 
                    title='cap_samples',
            xval_lines_d={'max':vmax,'min':vmin}, 
                label=os.path.basename(smpls_fp),logger=log)
        
    def plot_hand_vals(self):
        """check the BC branch?"""
        pass
