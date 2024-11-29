from PIL import Image
from kraken import binarization
from kraken import pageseg
from kraken.lib import models
from kraken import blla
from kraken import serialization
from kraken import rpred
from kraken.lib import vgsl

def to_bw(im, threshold=127):
    return binarization.nlbin(im)
    # im = im.convert('L')
    # im = im.point( lambda p: 255 if p > threshold else 0 )
    # im = im.convert('1')
    # return im
    
class ImageOCR:
    def __init__(self, model, baseline_model=None, bw_threshold=127, scale=1):
        self.model = model
        
        # The baseline model actually doesn't work well for our sample, and it takes a very long time to run (~seconds for each article)
        self.baseline_model = baseline_model

        # Scaling for some newspapers
        self.scale = scale
        
        self.bw_threshold = bw_threshold
    def get_text(self, image):
        if self.scale != 1:
            image = image.resize((int(self.scale * image.size[0]), int(self.scale * image.size[1])))
        bw_im = to_bw(image, threshold=self.bw_threshold)
        
        # display(image)
        # display(bw_im)
        
        seg = None
        if self.baseline_model is not None:
            try:
                seg = blla.segment(bw_im, model=self.baseline_model, text_direction='horizontal-rl')
            except RuntimeError:
                # Failed due to memory error (probably). Happens for single lines of text
                pass
        if seg is None:
            seg = pageseg.segment(bw_im, text_direction='horizontal-rl')

        # seg = blla.segment(bw_im, model=self.baseline_model, text_direction='horizontal-rl') # This uses too much memory, crashes VM

        pred_it = rpred.rpred(self.model, image, seg, bidi_reordering='R')
        records = [record for record in pred_it]
        
        # print([record.prediction for record in records])
        
        return [record.prediction for record in records]