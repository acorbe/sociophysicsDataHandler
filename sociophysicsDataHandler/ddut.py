"""Depth data utilities for decoding and working with pedestrian depth data images 
for the Sociophysics courses at Eindhoven University of Technology.

Author: Joris Willems
"""

from io import BytesIO
from PIL import Image
import gzip, base64, json
import numpy as np


def get_depth_maps(paths, verbose=True): 
    """
    Retrieve depth maps from tar.gz file. Depth maps
    are hashed with their timestamp and are returned
    as a dictionary. The dt argument enables returning
    timestamps as datetime objects.
    """
    
    def __decode_single_depth_map_file(path, verbose=True):

        def __decode_single_im(im_encoded):

            sbuf = BytesIO()

            sbuf.write(im_encoded)

            im_decoded = np.array(Image.open(sbuf))

            sbuf.close()

            return im_decoded

        def __decode_depth_file(path):

            file = gzip.open(path)
            json_file = json.load(file)

            data = [(v['time'], __decode_single_im(base64.b64decode(v['v']))) for v in json_file]

            file.close()

            return zip(*data)

        # decode all instantaneous images
        time, depth_maps = __decode_depth_file(path)

        if verbose:
            from datetime import datetime
            min_time = datetime.strptime(min(time), "%Y-%m-%d_%H:%M:%S.%f")
            max_time = datetime.strptime(max(time), "%Y-%m-%d_%H:%M:%S.%f")
            print("{} depth maps retrieved!\nDate: {}\nBetween {} and {}\n".format(len(time), 
                                                                                 min_time.date(), 
                                                                                 min_time.time(),
                                                                                 max_time.time()))

        return  time, depth_maps
    
    # create variables for saving the extracted depth maps
    time = []
    depth_maps = []

    # loop over all depth map files in depth_map_dir
    for path in paths:

        # retrieve depth maps (from approx 1 minute of data)
        t, d = __decode_single_depth_map_file(path, verbose=verbose)

        # concatenate to data that has already been collected
        time += t
        depth_maps += d

    # for convenience, we convert to numpy arrays for easier/faster analysis
    time = np.array(time)
    depth_maps = np.stack(depth_maps)
    
    return time, depth_maps