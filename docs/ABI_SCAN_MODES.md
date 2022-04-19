## GOES ABI SCAN MODES 

GOES ABI has currently 3 types of `scan_modes`.

Mode 3 (GOES16, GOES17)
- Till April 2, 2019 
- FULL DISK every 15 minutes 
- CONUS every 5 minutes
- Mesoscale every 1 minutes  (TODO: when 30 seconds)

Mode 6 (difference between GOES-16  and GOES-17)
- Since  April 2, 2019 
- FULL DISK every 10 minutes 
- CONUS every 5 minutes
- Mesoscale every 1 minutes  (TODO: when 30 seconds)

Mode 4 
- Continuous 5-minute full disk imagery 
- CONUS and Mesoscale products are not available
- Example dates: `datetime.datetime(2018,10,1, ...,...,...) # TODO`  

Mode 3 GOES17 Cooling Time
- During some periods of the year between 06:00 and 12:00 UTC
- CONUS not scanned 
- Mesoscale M1 and M2 every 2 minutes
- Every day during following periods

Periods: 
- February 6 - February 28, 2021
- April 8 - May 4, 2021
- August 6 - September 7, 2021
- October 14 - November 21, 2021 
- https://www.ospo.noaa.gov/Operations/GOES/west/Mode3G_Cooling_Timeline_G17.html

GOES 16 and 17 ABI scan mode timetable are available [here](https://www.goes-r.gov/users/abiScanModeInfo.html)
The scheduled ABI scan mode are available [here](https://www.ospo.noaa.gov/Operations/GOES/schedules.html)
Look Up Tables for ABI pixel scan time are available [here](https://www.star.nesdis.noaa.gov/GOESCal/goes_tools.php)
A video showing the GOES ABI scan strategy is available [here](shttps://www.youtube.com/watch?v=qCAPwgQR13w&ab_channel=NOAASatellites)

Notes:
- ABI swaths are only in the instrument space and can vary in width
- Native ABI swaths are remapped onto the ABI fixed grid to produce the ABI L1b product. 
- The Swath Transition Lines (STLs) are a priori unknown in the ABI fixed grid
- Because of the uncertainty in locating ABI swaths onto the ABI L1B remapped data onto the ABI fixed grid, there is a pixel scan time inaccuracy of about 30-40 s around the STLs.


 
 

 