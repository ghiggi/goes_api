 

#======================================================================================================
# Required Libraries 
#======================================================================================================
import matplotlib.pyplot as plt                         # Import the Matplotlib package
from mpl_toolkits.basemap import Basemap                # Import the Basemap toolkit 
import numpy as np                                      # Import the Numpy package
from remap import remap                                 # Import the Remap function  
import datetime                                         # Library to convert julian day to dd-mm-yyyy
from netCDF4 import Dataset                             # Import the NetCDF Python interface

# CMAPS 
https://geonetcast.wordpress.com/2018/06/28/goes-r-level-2-products-a-python-script/comment-page-1/

# Projection
https://proj.org/operations/projections/geos.html
https://github.com/uba/goes-latlon
https://makersportal.com/blog/2018/11/25/goes-r-satellite-latitude-and-longitude-grid-projection-algorithm

 

def get_product_varname(product):
    d = {'ACHA': 'HT',
         'ACHT': 'TEMP',
         'ACM': 'BCM',
         'ACTP': 'Phase',
         'ADPF': ['Smoke', 'Dust'],
         'AOD': ['AOD'],
         'CMIP': ['CMI']
         'CPS': ['PSD'],
         'CTP': ['PRES'],
         'DMW': ['pressure','temperature', 'wind_direction', 'wind_speed'],
         'DSI': ['CAPE', 'KI', 'LI', 'SI', 'TT'],
         'DSR': ['DSR'],
         'FDC': ['Area', 'Mask', 'Power', 'Temp'],
         'FSC': ['FSC'],
         'LST': ['LST'],
         'RRQPE': ['RRQPE'],
         'RSR': ['RSR'],
         'SST': ['SST'],
         'TPW': ['TPW'],
         'VAA': ['VAH', 'VAML]
         }
 
# colorbars
# retrieve also from others 


#======================================================================================================
# Load the GOES-16 Data
#======================================================================================================
#path = "E:\VLAB\Python\GOES-16 Samples - L2\OR_ABI-L2-DMWF-M3C14_G16_s20180581530413_e20180581541179_c20180581559098.nc" # goes-lat-lon-projection
#path = "E:\VLAB\Python\GOES-16 Samples - L2\OR_ABI-L2-DSRF-M3_G16_s20180710900420_e20180710911187_c20180710922418.nc" # goes-lat-lon-projection
#path = "E:\VLAB\Python\GOES-16 Samples - L2\OR_ABI-L2-RSRF-M3_G16_s20180581300412_e20180581311179_c20180581333436.nc" # goes-lat-lon-projection
 

# CMIPF - Cloud and Moisture Imagery: 'CMI'
if (product == "CMIPF") or (product == "CMIPC") or (product == "CMIPM"):
    variable = 'CMI'
    
# ACHAF - Cloud Top Height: 'HT'    
elif product == "ACHAF":
    variable = 'HT'
    vmin = 0
    vmax = 15000
    cmap = "rainbow"

# ACHTF - Cloud Top Temperature: 'TEMP'
elif product == "ACHTF":
    variable = 'TEMP' 
    vmin = 180
    vmax = 300
    cmap = "jet"

# ACMF - Clear Sky Masks: 'BCM'
elif product == "ACMF":
    variable = 'BCM' 
    vmin = 0
    vmax = 1
    cmap = "gray"

# ACTPF - Cloud Top Phase: 'Phase'
elif product == "ACTPF":
    variable = 'Phase' 
    vmin = 0
    vmax = 5
    cmap = "jet"     

# ADPF - Aerosol Detection: 'Smoke'
elif product == "ADPF":
    variable = 'Smoke' 
    vmin = 0
    vmax = 255
    cmap = "jet"
    
    #variable = 'Dust'  
    #vmin = 0
    #vmax = 255
    #cmap = "jet"

# AODF - Aerosol Optical Depth: 'AOD'    
elif product == "AODF":
    variable = 'AOD'
    vmin = 0
    vmax = 2
    cmap = "rainbow"  

# CODF - Cloud Optical Depth: 'COD'    
elif product == "CODF":
    variable = 'COD'
    vmin = 0
    vmax = 100
    cmap = "jet"  

# CPSF - Cloud Particle Size: 'PSD'
elif product == "CPSF":
    variable = 'PSD'
    vmin = 0
    vmax = 80
    cmap = "rainbow"

# CTPF - Cloud Top Pressure: 'PRES'
elif product == "CTPF":
    variable = 'PRES'
    vmin = 0
    vmax = 1100
    cmap = "rainbow"

# DMWF - Derived Motion Winds: 'pressure','temperature', 'wind_direction', 'wind_speed'    
elif product == "DMWF":
    variable = 'pressure' 
    #variable = 'temperature'
    #variable = 'wind_direction'
    #variable = 'wind_speed'    

# DSIF - Derived Stability Indices: 'CAPE', 'KI', 'LI', 'SI', 'TT' 
elif product == "DSIF":
    variable = 'CAPE' 
    vmin = 0
    vmax = 1000
    cmap = "jet" 
    
    #variable = 'KI'
    #vmin = -50
    #vmax = 50
    #cmap = "jet"
    
    #variable = 'LI'
    #vmin = -10
    #vmax = 30
    #cmap = "jet"
    
    #variable = 'SI'
    #vmin = -10
    #vmax = 25
    #cmap = "jet"
    
    #variable = 'TT'
    #vmin = -10
    #vmax = 60
    #cmap = "jet"

# DSRF - Downward Shortwave Radiation: 'DSR'  
#elif product == "DSRF":
#    variable = 'DSR'

# FDCF - Fire-Hot Spot Characterization: 'Area', 'Mask', 'Power', 'Temp'    
elif product == "FDCF":
    #variable = 'Area' 
    
    variable = 'Mask' 
    vmin = 0
    vmax = 255
    cmap = "jet"
    
    #variable = 'Power' 
    #variable = 'Temp' 

# FSCF - Snow Cover: 'FSC'    
elif product == "FSCF":
    variable = 'FSC' 
    vmin = 0
    vmax = 1
    cmap = "jet"

# LSTF - Land Surface (Skin) Temperature: 'LST'    
elif product == "LSTF":
    variable = 'LST'    
    vmin = 213
    vmax = 330
    cmap = "jet"

# RRQPEF - Rainfall Rate - Quantitative Prediction Estimate: 'RRQPE'    
elif product == "RRQPEF":
    variable = 'RRQPE' 
    vmin = 0
    vmax = 35
    cmap = "jet"

# RSR - Reflected Shortwave Radiation: 'RSR'    
#elif product == "RSRF":
#    variable = 'RSR'       

# SSTF - Sea Surface (Skin) Temperature: 'SST'
elif product == "SSTF":
    variable = 'SST'
    vmin = 268
    vmax = 308
    cmap = "jet"     

# TPWF - Total Precipitable Water: 'TPW'
elif product == "TPWF":
    variable = 'TPW' 
    vmin = 0
    vmax = 60
    cmap = "jet"

# VAAF - Volcanic Ash: 'VAH', 'VAML'    
elif product == "VAAF":
    #variable = 'VAH'
    #vmin = 0
    #vmax = 20000
    #cmap = "jet" 
    
    variable = 'VAML' 
    vmin = 0
    vmax = 100
    cmap = "jet"
    
#======================================================================================================
# Open the File and Reproject the Data
#======================================================================================================
# Open the file using the NetCDF4 library
nc = Dataset(path)

# Get the latitude and longitude image bounds
geo_extent = nc.variables['geospatial_lat_lon_extent']
min_lon = float(geo_extent.geospatial_westbound_longitude)
max_lon = float(geo_extent.geospatial_eastbound_longitude)
min_lat = float(geo_extent.geospatial_southbound_latitude)
max_lat = float(geo_extent.geospatial_northbound_latitude)

# Choose the visualization extent (min lon, min lat, max lon, max lat)
# Full Disk
extent = [min_lon, min_lat, max_lon, max_lat]

# Choose the image resolution (the higher the number the faster the processing is)
resolution = 5

# Calculate the image extent required for the reprojection
H = nc.variables['goes_imager_projection'].perspective_point_height
x1 = nc.variables['x_image_bounds'][0] * H 
x2 = nc.variables['x_image_bounds'][1] * H 
y1 = nc.variables['y_image_bounds'][1] * H 
y2 = nc.variables['y_image_bounds'][0] * H 

# Close the NetCDF file after getting the data
nc.close()

# Call the reprojection funcion
grid = remap(path, variable, extent, resolution, x1, y1, x2, y2)
    
# Read the data returned by the function 
data = grid.ReadAsArray()

# Convert from int16 to uint16
data = data.astype(np.float64)

print(max(data[0]))
print(min(data[0]))

if (variable == "Dust") or (variable == "Smoke") or (variable == "TPW") or (variable == "PRES") or  (variable == "HT") or \
   (variable == "TEMP") or (variable == "AOD") or (variable == "COD") or (variable == "PSD") or  (variable == "CAPE") or  (variable == "KI") or \
   (variable == "LI") or (variable == "SI") or (variable == "TT") or (variable == "FSC") or  (variable == "RRQPE") or (variable == "VAML") or (variable == "VAH") or (variable == "CMI"):     
   data[data == max(data[0])] = np.nan
   data[data == min(data[0])] = np.nan

if (variable == "SST"):
   data[data == max(data[0])] = np.nan   
   data[data == min(data[0])] = np.nan  
   
   # Call the reprojection funcion again to get only the valid SST pixels
   grid = remap(path, "DQF", extent, resolution, x1, y1, x2, y2)
   data_DQF = grid.ReadAsArray()
   
   # If the Quality Flag is not 0, set as NaN 
   data[data_DQF != 0] = np.nan 

if (variable == "Mask"):
   data[data == -99] = np.nan
   data[data == 40] = np.nan
   data[data == 50] = np.nan
   data[data == 60] = np.nan
   data[data == 150] = np.nan
   data[data == max(data[0])] = np.nan
   data[data == min(data[0])] = np.nan
   
if (variable == "BCM"):
   data[data == 255] = np.nan
   data[data == 0] = np.nan
   
if (variable == "Phase"):
   data[data >= 5] = np.nan
   data[data == 0] = np.nan   
   
if (variable == "LST"):
   data[data >= 335] = np.nan
   data[data <= 200] = np.nan
        
 

