#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Apr 14 11:46:53 2022

@author: ghiggi
"""
#------------------------------------------------------------------------------.
import os 
import xarray as xr
import numpy as np

# G17 
Mode6M: "340_Timeline_05M_Mode6_v2.7"                # G17 (Mode6M)
Mode6I: "ABI-Timeline03I_Mode6_Cooling_hybrid.nc"    # G17 cooling (equal to G17 Mode6M for Full Disc, but no CONUS)
Mode3G: "ABI-Timeline03G_Mode3_Cooling_ShortStars"   # G17 cooling (Mode 3G)

# G16
Mode6A: "ABI-Timeline05B_Mode 6A_20190612-183017.nc" # G16 (Mode 6A)
Mode6F: "Timeline05F_Mode6_20180828-092941.nc"       # G16 (Mode 6F) (for FD differ of 3 sec in last 2 swath)
Mode3: "Timeline03C_Mode3_20180828-092953.nc"
Mode4: "ABI-Timeline04A_Mode 4_20181219-104006.nc"

ds_M4 = xr.open_dataset(os.path.join("/home/ghiggi/Python_Packages/goes_api/data/ABI-Time_Model_LUTS", "ABI-Timeline04A_Mode 4_20181219-104006.nc"))
ds_G17_M3G_Cooling = xr.open_dataset(os.path.join("/home/ghiggi/Python_Packages/goes_api/data/ABI-Time_Model_LUTS", "ABI-Timeline03G_Mode3_Cooling_ShortStars.nc"))
ds_G17_M6I_Cooling = xr.open_dataset(os.path.join("/home/ghiggi/Python_Packages/goes_api/data/ABI-Time_Model_LUTS", "ABI-Timeline03I_Mode6_Cooling_hybrid.nc"))
ds_G17_M6M = xr.open_dataset(os.path.join("/home/ghiggi/Python_Packages/goes_api/data/ABI-Time_Model_LUTS", "340_Timeline_05M_Mode6_v2.7.nc"))

ds_G16_M3 = xr.open_dataset(os.path.join("/home/ghiggi/Python_Packages/goes_api/data/ABI-Time_Model_LUTS", "Timeline03C_Mode3_20180828-092953.nc"))
ds_G16_M6F = xr.open_dataset(os.path.join("/home/ghiggi/Python_Packages/goes_api/data/ABI-Time_Model_LUTS", "Timeline05F_Mode6_20180828-092941.nc"))
ds_G16_M6A = xr.open_dataset(os.path.join("/home/ghiggi/Python_Packages/goes_api/data/ABI-Time_Model_LUTS", "ABI-Timeline05B_Mode 6A_20190612-183017.nc"))

#-----------------------------------------------------------------------------.
#### Retrieve swath indices 
ds = ds_M6
da = ds["FD_pixel_times"]
da = da.swap_dims({"FD_cols": 'x', "FD_rows": 'y'}).transpose('y','x')
da.data = da.data.astype(int) # /1000/1000/1000 
idx = np.where(np.any(da.diff("y").data != 0, axis=1))[0] + 1
print(idx)
print(np.diff(idx))

#-----------------------------------------------------------------------------.
#### Upsample to 1km and 500m ABI fixed grid 
# 1 km 
array = da.data.repeat(2, axis = 0).repeat(2, axis = 1)

# 500m 
array = da.data.repeat(4, axis = 0).repeat(4, axis = 1)

#-----------------------------------------------------------------------------.
# Variable names 
# - No CONUS during G17-Cooling m6I and M3G and M4
# - NO Mesoscale during M4 
dict_var = {"F": "FD_pixel_times", 
            "C": "CONUS1_pixel_times", #  "CONUS2_pixel_times"
            "M": "MESO_pixel_times"
           }

##----------------------------------------------------------------------------.
#### Difference in offset between CONUS 1 and CONUS 2 
# --> Offset do not change between CONUS1 and CONUS2 

# da_CONUS1 = ds_G16_M3["CONUS1_pixel_times"].copy()
# da_CONUS2 = ds_G16_M3["CONUS2_pixel_times"].copy()

da_CONUS1 = ds_G16_M6A["CONUS1_pixel_times"].copy()
da_CONUS2 = ds_G16_M6A["CONUS2_pixel_times"].copy()

da_CONUS1 = da_CONUS1.swap_dims({"CONUS1_cols": 'x', "CONUS1_rows": 'y'})
da_CONUS1.data = da_CONUS1.data.astype(int)/1000/1000/1000 

da_CONUS2 = da_CONUS2.swap_dims({"CONUS2_cols": 'x', "CONUS2_rows": 'y'})
da_CONUS2.data = da_CONUS2.data.astype(int)/1000/1000/1000 

diff = da_CONUS1 - da_CONUS2
diff.plot.imshow(y="y", x="x", origin="upper", cmap="Spectral")

##----------------------------------------------------------------------------.
#### Difference G17 Cooling Mode 
diff_cooling = ds_G17_M3G_Cooling["FD_pixel_times"] - ds_G17_M6I_Cooling["FD_pixel_times"]
diff_cooling.data = diff_cooling.data.astype(int)/1000/1000/1000
diff_cooling.plot.imshow(y="FD_rows", x="FD_cols", origin="upper")

##----------------------------------------------------------------------------.
#### G17 M3 Cooling 
da_G17_M3 = ds_scan_modes['G17-M3']

da_G17_M3G_Cooling = ds_G17_M3G_Cooling["FD_pixel_times"]
da_G17_M3G_Cooling = da_G17_M3G_Cooling.swap_dims({"FD_cols": 'x', "FD_rows": 'y'})
da_G17_M3G_Cooling.data = da_G17_M3G_Cooling.data.astype(int)/1000/1000/1000 

da_G17_M6I_Cooling = ds_G17_M6I_Cooling["FD_pixel_times"]
da_G17_M6I_Cooling = da_G17_M6I_Cooling.swap_dims({"FD_cols": 'x', "FD_rows": 'y'})
da_G17_M6I_Cooling.data = da_G17_M6I_Cooling.data.astype(int)/1000/1000/1000 

diff = da_G17_M3 - da_G17_M3G_Cooling
diff.plot.imshow(y="y", x="x", origin="upper", cmap="Spectral")
diff.plot.imshow(y="y", x="x", origin="upper", vmin=-5, vmax=5, cmap="Spectral")

# diff = da_G17_M3 - da_G17_M6I_Cooling
# diff.plot.imshow(y="y", x="x", origin="upper", cmap="Spectral",vmin=0)

##----------------------------------------------------------------------------.
#### G17 M6 
da_G17_M6 = ds_scan_modes['G17-M6']

da_G17_M6M = ds_G17_M6M["FD_pixel_times"]
da_G17_M6M = da_G17_M6M.swap_dims({"FD_cols": 'x', "FD_rows": 'y'})
da_G17_M6M.data = da_G17_M6M.data.astype(int)/1000/1000/1000 

da_G17_M6I_Cooling = ds_G17_M6I_Cooling["FD_pixel_times"]
da_G17_M6I_Cooling = da_G17_M6I_Cooling.swap_dims({"FD_cols": 'x', "FD_rows": 'y'})
da_G17_M6I_Cooling.data = da_G17_M6I_Cooling.data.astype(int)/1000/1000/1000 

diff = da_G17_M6 - da_G17_M6M
diff.plot.imshow(y="y", x="x", origin="upper", cmap="Spectral")
diff.plot.imshow(y="y", x="x", origin="upper", vmin=-5, vmax=5, cmap="Spectral")

diff = da_G17_M6 - da_G17_M6I_Cooling
diff.plot.imshow(y="y", x="x", origin="upper", cmap="Spectral")

# Show no difference between G17 M6 modes (cooling and nominal) Full Disc
diff = da_G17_M6I_Cooling - da_G17_M6M
diff.plot.imshow(y="y", x="x", origin="upper", cmap="Spectral")

##----------------------------------------------------------------------------.
#### G16 M6A
da_G16_M6 = ds_scan_modes['G16-M6']

da_G16_M6A = ds_G16_M6A["FD_pixel_times"]
da_G16_M6A = da_G16_M6A.swap_dims({"FD_cols": 'x', "FD_rows": 'y'})
da_G16_M6A.data = da_G16_M6A.data.astype(int)/1000/1000/1000 

diff = da_G16_M6 - da_G16_M6A
diff.plot.imshow(y="y", x="x", origin="upper", cmap="Spectral")

da_G16_M6F = ds_G16_M6F["FD_pixel_times"]
da_G16_M6F = da_G16_M6F.swap_dims({"FD_cols": 'x', "FD_rows": 'y'})
da_G16_M6F.data = da_G16_M6F.data.astype(int)/1000/1000/1000 

diff = da_G16_M6F - da_G16_M6A # 3 s difference in the last 2 swath scan 
diff.plot.imshow(y="y", x="x", origin="upper", cmap="Spectral")

##----------------------------------------------------------------------------.
#### G16 M3
da_G16_M3_my = ds_scan_modes['G16-M3']

da_G16_M3 = ds_G16_M3["FD_pixel_times"]
da_G16_M3 = da_G16_M3.swap_dims({"FD_cols": 'x', "FD_rows": 'y'})
da_G16_M3.data = da_G16_M3.data.astype(int)/1000/1000/1000 

diff = da_G16_M3_my - da_G16_M3
diff.plot.imshow(y="y", x="x", origin="upper", cmap="Spectral")

##----------------------------------------------------------------------------.
#### M4
da_G16_M4_my = ds_scan_modes['G16-M4']

da_M4 = ds_M4["FD_pixel_times"]
da_M4 = da_M4.swap_dims({"FD_cols": 'x', "FD_rows": 'y'})
da_M4.data = da_M4.data.astype(int)/1000/1000/1000 

diff = da_G16_M3_my - da_G16_M3
diff.plot.imshow(y="y", x="x", origin="upper", cmap="Spectral")

##----------------------------------------------------------------------------.