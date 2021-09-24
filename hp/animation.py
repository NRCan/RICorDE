'''
Created on Jul. 20, 2021

@author: cefect
'''


import imageio
import datetime, os

def capture_images(
        ofp,
        img_dir, #directory with images
        ):
    
    filenames =  [os.path.join(img_dir, e) for e in os.listdir(img_dir) if e.endswith('.tif')]
    
    print('on %i files\n    %s'%(len(filenames), filenames))
    
    """
    imageio.help(name='gif')
    imageio.help(name='tif')
    help(imageio.get_writer)
    """

    with imageio.get_writer(ofp, mode='I', duration=0.5) as writer:
        for filename in filenames:
            image = imageio.imread(filename)
            writer.append_data(image)
            
    print('animation saved to \n    %s'%ofp)
            
            
            
if __name__ =="__main__": 
    start =  datetime.datetime.now()
    print('start at %s'%start)


    #build_hmax()
    
    capture_images(
        r'C:\LS\03_TOOLS\_jobs\202103_InsCrve\outs\HANDin\20210720\out.gif',
        r'C:\LS\03_TOOLS\_jobs\202103_InsCrve\outs\HANDin\20210720\smoothing\avg',
        )

    
    

    
    tdelta = datetime.datetime.now() - start
    print('finished in %s'%tdelta)