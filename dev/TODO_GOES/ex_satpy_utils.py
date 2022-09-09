
import matplotlib.pyplot as plt
%matplotlib inline


### VIDEO ABI+GLM 
# https://github.com/gerritholl/sattools/blob/master/src/sattools/vis.py#L155
# https://github.com/gerritholl/sattools/blob/master/src/sattools/scutil.py

# Satpy examples 
# https://github.com/pytroll/pytroll-examples/blob/master/satpy/HRIT%20AHI%2C%20Hurricane%20Trami.ipynb
# https://github.com/pytroll/pytroll-examples/blob/master/satpy/ahi_true_color_pyspectral.ipynb

def get_area_latlon_center(area):
    """Get lat/lon of centre of area."""
    return area.get_lonlat(area.height//2, area.width//2)

def join_videos_together():
  """Join the individual videos together with ffmpeg."""
  # - List files 
  cmd = """for fn in C*.mp4; do echo "file '$fn'" >>channel_videos.txt; done"""
  subprocess.run(cmd, shell=True)
  # - Create video
  cmd = """ffmpeg -f concat -i channel_videos.txt -c copy channel_videos.mp4"""
  subprocess.run(cmd, shell=True)
  # - Post processing 
  cmd = """ffmpeg -i channel_videos.mp4 -vcodec libx264 -crf 38 abi_channel_videos.compress2.mp4"""



  

 



