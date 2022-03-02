

## Raw data 
# ABI-L1b-RadC (CONUS: USA view, every 5 mins)
# ABI-L1b-RadF (Full Disk: every 10 mins)
# ABI-L1b-RadM (Mesoscale interest area: Every 1 min).


# Channel 1 is blue
# Channel 2 is red
# Channel 3 is green. 
# --> Use red for best visible images. channel 10 is IR.

## Load with netCDF4
data =  netCDF4.Dataset(url, 'r')
data.title
data['Rad'][:]

# Satellite height
sat_h = data.variables['goes_imager_projection'].perspective_point_height

# Satellite longitude
sat_lon = data.variables['goes_imager_projection'].longitude_of_projection_origin

# Satellite sweep
sat_sweep = data.variables['goes_imager_projection'].sweep_angle_axis


# The projection x and y coordinates equals  the scanning angle (in radians) 
#  multiplied by the satellite height (http://proj4.org/projections/geos.html)
X = data.variables['x'][:] * sat_h
Y = data.variables['y'][:] * sat_h



#### Load with xarray 
ds = xr.open_dataset(url)                # no lazy loading 
ds = xr.open_dataset(url, chunks='auto') # lazy loading

ds
ds.attrs['title']
ds.attrs['spatial_resolution']

ds.nominal_satellite_height
ds.x_image_bounds
ds.y_image_bounds
 
#----------------------------------------------------------------------------.
#make figure
fig = plt.figure(figsize=(10, 10))
#add the map
ax = fig.add_subplot(1, 1, 1,projection=ccrs.Geostationary(satellite_height=sat_h,central_longitude=sat_lon,sweep_axis='x'))

#add coastlines
ax.add_feature(cartopy.feature.COASTLINE,zorder=1,color='w',lw=1)


#plot the image
pm = ax.pcolorfast(X,Y,rad,cmap='Greys_r',zorder=0,vmin=10)
ax.set_title('GOES-16',fontweight='bold',fontsize=14)

cbar = plt.colorbar(pm,ax=ax,shrink=0.75)
cbar.set_label('Radiance',fontsize=14)
cbar.ax.tick_params(labelsize=12)

plt.tight_layout()

def scene_generator(base_dir):
    dt = datetime(2019, 1, 1)
    base_dir = base_dir.format(dt)
    # 1200Z to 2359
    c01_files = sorted(glob(os.path.join(base_dir, 'OR_ABI-L1b-RadF-M3C01_G16_s{:%Y%j}[12]*.nc').format(dt)))
    for c01_file in c01_files:
        ctime_idx = c01_file.find('e{:%Y}'.format(dt))
        all_files = glob(c01_file.replace('C01', 'C??')[:ctime_idx] + '*.nc')
        assert len(all_files) == 16
        
        scn = Scene(reader='abi_l1b', filenames=all_files)
        scn.load(ds_names)
        new_scn = scn.resample(scn.min_area(), resampler='native')
        yield new_scn

with ProgressBar():
    mscn = MultiScene(scene_generator(base_dir))
    #mscn.load(['C{:02d}'.format(x) for x in range(1, 17)])
    #new_mscn = mscn.resample(resampler='native')
    mscn.save_animation('{name}_{start_time:%Y%m%d_%H%M%S}.mp4', fps=10, batch_size=4)

# Join the individual videos together with ffmpeg
# - List files 
!for fn in C*.mp4; do echo "file '$fn'" >>channel_videos.txt; done
# - Create video
!ffmpeg -f concat -i channel_videos.txt -c copy channel_videos.mp4
# - Post processing 
!ffmpeg -i channel_videos.mp4 -vcodec libx264 -crf 38 abi_channel_videos.compress2.mp4

# https://github.com/pytroll/pytroll-examples/blob/master/satpy/HRIT%20AHI%2C%20Hurricane%20Trami.ipynb
# https://github.com/pytroll/pytroll-examples/blob/master/satpy/ahi_true_color_pyspectral.ipynb

  

 



