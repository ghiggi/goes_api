#######################################################################################################
# LICENSE
# Copyright (C) 2018 - INPE - NATIONAL INSTITUTE FOR SPACE RESEARCH
# This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
# This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
# You should have received a copy of the GNU General Public License along with this program. If not, see http://www.gnu.org/licenses/.
#######################################################################################################

# SOURCE: https://geonetcast.wordpress.com/2018/06/28/goes-r-level-2-products-a-python-script/comment-page-1/
#======================================================================================================
# GNC-A Blog Python and GOES-16 Level 2 Data Python Example
#======================================================================================================

#======================================================================================================
# Required Libraries 
#======================================================================================================
import matplotlib.pyplot as plt                         # Import the Matplotlib package
from mpl_toolkits.basemap import Basemap                # Import the Basemap toolkit 
import numpy as np                                      # Import the Numpy package
from remap import remap                                 # Import the Remap function  
import datetime                                         # Library to convert julian day to dd-mm-yyyy
from netCDF4 import Dataset                             # Import the NetCDF Python interface

#======================================================================================================
# Acronym Description
#======================================================================================================
# ACHAF - Cloud Top Height: 'HT'
# ACHTF - Cloud Top Temperature: 'TEMP'
# ACMF - Clear Sky Masks: 'BCM'
# ACTPF - Cloud Top Phase: 'Phase'
# ADPF - Aerosol Detection: 'Smoke'
# ADPF - Aerosol Detection: 'Dust'
# AODF - Aerosol Optical Depth: 'AOD'
# CMIPF - Cloud and Moisture Imagery: 'CMI'
# CMIPC - Cloud and Moisture Imagery: 'CMI'
# CMIPM - Cloud and Moisture Imagery: 'CMI'
# CODF - Cloud Optical Depth: 'COD'
# CPSF - Cloud Particle Size: 'PSD'
# CTPF - Cloud Top Pressure: 'PRES'
# DMWF - Derived Motion Winds: 'pressure'
# DMWF - Derived Motion Winds: 'temperature'
# DMWF - Derived Motion Winds: 'wind_direction'
# DMWF - Derived Motion Winds: 'wind_speed'
# DSIF - Derived Stability Indices: 'CAPE' 
# DSIF - Derived Stability Indices: 'KI'
# DSIF - Derived Stability Indices: 'LI'
# DSIF - Derived Stability Indices: 'SI'
# DSIF - Derived Stability Indices: 'TT'
# DSRF - Downward Shortwave Radiation: 'DSR'   
# FDCF - Fire-Hot Spot Characterization: 'Area'
# FDCF - Fire-Hot Spot Characterization: 'Mask'
# FDCF - Fire-Hot Spot Characterization: 'Power'
# FDCF - Fire-Hot Spot Characterization: 'Temp'
# FSCF - Snow Cover: 'FSC'
# LSTF - Land Surface (Skin) Temperature: 'LST'
# RRQPEF - Rainfall Rate - Quantitative Prediction Estimate: 'RRQPE'
# RSR - Reflected Shortwave Radiation: 'RSR'
# SSTF - Sea Surface (Skin) Temperature: 'SST'
# TPWF - Total Precipitable Water: 'TPW'
# VAAF - Volcanic Ash: 'VAH'
# VAAF - Volcanic Ash: 'VAML'

#======================================================================================================
# Load the GOES-16 Data
#======================================================================================================
path = "E:\VLAB\Python\GOES-16 Samples - L2\OR_ABI-L2-ACHAF-M3_G16_s20180581400412_e20180581411179_c20180581412168.nc"
#path = "E:\VLAB\Python\GOES-16 Samples - L2\OR_ABI-L2-ACHTF-M3_G16_s20180530415387_e20180530426154_c20180530427165.nc"
#path = "E:\VLAB\Python\GOES-16 Samples - L2\OR_ABI-L2-CMIPF-M3C13_G16_s20180571300407_e20180571311185_c20180571311263.nc"
#path = "E:\VLAB\Python\GOES-16 Samples - L2\OR_ABI-L2-ACMF-M3_G16_s20180581400412_e20180581411179_c20180581411362.nc" 
#path = "E:\VLAB\Python\GOES-16 Samples - L2\OR_ABI-L2-ACTPF-M3_G16_s20180581400412_e20180581411179_c20180581411472.nc"
#path = "E:\VLAB\Python\GOES-16 Samples - L2\OR_ABI-L2-ADPF-M3_G16_s20180581345412_e20180581356179_c20180581356455.nc" 
#path = "E:\VLAB\Python\GOES-16 Samples - L2\OR_ABI-L2-AODF-M3_G16_s20180581345412_e20180581356179_c20180581359029.nc"
#path = "E:\VLAB\Python\GOES-16 Samples - L2\OR_ABI-L2-CODF-M3_G16_s20180581345412_e20180581356179_c20180581359222.nc"
#path = "E:\VLAB\Python\GOES-16 Samples - L2\OR_ABI-L2-CPSF-M3_G16_s20180530415387_e20180530426154_c20180530429165.nc"
#path = "E:\VLAB\Python\GOES-16 Samples - L2\OR_ABI-L2-CTPF-M3_G16_s20181111245441_e20181111256207_c20181111257161.nc"
#path = "E:\VLAB\Python\GOES-16 Samples - L2\OR_ABI-L2-DMWF-M3C14_G16_s20180581530413_e20180581541179_c20180581559098.nc" # goes-lat-lon-projection
#path = "E:\VLAB\Python\GOES-16 Samples - L2\OR_ABI-L2-DSIF-M3_G16_s20180581345412_e20180581356179_c20180581356568.nc"
#path = "E:\VLAB\Python\GOES-16 Samples - L2\OR_ABI-L2-DSRF-M3_G16_s20180710900420_e20180710911187_c20180710922418.nc" # goes-lat-lon-projection
#path = "E:\VLAB\Python\GOES-16 Samples - L2\OR_ABI-L2-FDCF-M3_G16_s20180581400412_e20180581411179_c20180581411307.nc"
#path = "E:\VLAB\Python\GOES-16 Samples - L2\OR_ABI-L2-FSCF-M3_G16_s20180581345412_e20180581356179_c20180581356417.nc"
#path = "E:\VLAB\Python\GOES-16 Samples - L2\OR_ABI-L2-LSTF-M3_G16_s20180581300412_e20180581311179_c20180581318348.nc"
#path = "E:\VLAB\Python\GOES-16 Samples - L2\OR_ABI-L2-RRQPEF-M3_G16_s20180581400412_e20180581411179_c20180581411294.nc"
#path = "E:\VLAB\Python\GOES-16 Samples - L2\OR_ABI-L2-RSRF-M3_G16_s20180581300412_e20180581311179_c20180581333436.nc" # goes-lat-lon-projection
#path = "E:\VLAB\Python\GOES-16 Samples - L2\OR_ABI-L2-SSTF-M3_G16_s20180581200412_e20180581256179_c20180581300238.nc"
#path = "E:\VLAB\Python\GOES-16 Samples - L2\OR_ABI-L2-TPWF-M3_G16_s20181111245441_e20181111256207_c20181111257088.nc"
#path = "E:\VLAB\Python\GOES-16 Samples - L2\OR_ABI-L2-VAAF-M3_G16_s20180581345412_e20180581356179_c20180581357045.nc"

#======================================================================================================
# Getting Information From the File Name
#======================================================================================================
# Search for the Scan start in the file name
Start = (path[path.find("_s")+2:path.find("_e")])
# Converting from julian day to dd-mm-yyyy
year = int(Start[0:4])
dayjulian = int(Start[4:7]) - 1 # Subtract 1 because the year starts at "0"
dayconventional = datetime.datetime(year,1,1) + datetime.timedelta(dayjulian) # Convert from julian to conventional
date = dayconventional.strftime('%d-%b-%Y')  # Format the date according to the strftime directives
time = Start [7:9] + ":" + Start [9:11] + ":" + Start [11:13] + " UTC" # Time of the Start of the Scan

#======================================================================================================
# Detect the product type
#======================================================================================================
product = (path[path.find("L2-")+3:path.find("-M3" or "-M4")])
print(product)

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
        
#======================================================================================================
# Define the size of the saved picture
#======================================================================================================
DPI = 150 
fig = plt.figure(figsize=(data.shape[1]/float(DPI), data.shape[0]/float(DPI)), frameon=False, dpi=DPI)
ax = plt.Axes(fig, [0., 0., 1., 1.])
ax.set_axis_off()
fig.add_axes(ax)
ax = plt.axis('off')

#======================================================================================================
# Plot the Data
#======================================================================================================
# Create the basemap reference for the Rectangular Projection
bmap = Basemap(llcrnrlon=extent[0], llcrnrlat=extent[1], urcrnrlon=extent[2], urcrnrlat=extent[3], epsg=4326)

# Add the world states and provinces shapefile
#bmap.readshapefile('E:\\VLAB\\Python\\Shapefiles\\ne_10m_admin_1_states_provinces','ne_10m_admin_1_states_provinces',linewidth=0.30,color='grey')
# Add the countries shapefile
bmap.readshapefile('E:\\VLAB\Python\\Shapefiles\\ne_10m_admin_0_countries','ne_10m_admin_0_countries',linewidth=2.00,color='white')
# Add the continents shapefile
bmap.readshapefile('E:\\VLAB\\Python\\Shapefiles\\continent','continent',linewidth=3.00,color='white')

# Draw parallels and meridians
bmap.drawparallels(np.arange(-90.0, 90.0, 10.0), linewidth=0.3, dashes=[4, 4], color='white', labels=[False,False,False,False], fmt='%g', labelstyle="+/-", xoffset=-0.80, yoffset=-1.00, size=7)
bmap.drawmeridians(np.arange(0.0, 360.0, 10.0), linewidth=0.3, dashes=[4, 4], color='white', labels=[False,False,False,False], fmt='%g', labelstyle="+/-", xoffset=-0.80, yoffset=-1.00, size=7)

# Add a background
bmap.bluemarble()

bmap.imshow(data, origin='upper', cmap=cmap, vmin=vmin, vmax=vmax)
cb = bmap.colorbar(location='bottom', size = '1%', pad = '-1.0%')
cb.outline.set_visible(False)                              # Remove the colorbar outline
cb.ax.tick_params(width = 0)                               # Remove the colorbar ticks 
cb.ax.xaxis.set_tick_params(pad=-17)                     # Put the colobar labels inside the colorbar
cb.ax.tick_params(axis='x', colors='black', labelsize=12)  # Change the color and size of the colorbar labels
    
# Date as string
date_save = dayconventional.strftime('%Y%m%d')
# Time (UTC) as string
time_save = Start [7:9] + Start [9:11]

# Save the result
plt.savefig('E:\\VLAB\\Python\\Output\\G16_' + product + '_' + variable + '_' + date_save + time_save + '.png', dpi=DPI, pad_inches=0)
#plt.close()





