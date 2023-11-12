import os
from threading import Thread
from datetime import datetime
from scipy.io import netcdf_file

"""
NetCDF files are a self-describing binary data format.
The file contains metadata that describes the dimensions and variables in the file.
More details about NetCDF files can be found here. There are three main sections to a NetCDF data structure:
    - Dimensions
    - Variables
    - Attributes

https://docs.scipy.org/doc/scipy/reference/generated/scipy.io.netcdf_file.html#scipy.io.netcdf_file
"""

DATA_PATH='../water-supply-forecast-rodeo-runtime/data/pdsi/'

def get_fy_dirs_and_files():
    dirs = os.listdir(DATA_PATH)
    files = []
    for dir in dirs:
        if dir == '.DS_Store':
            continue

        dir_files = os.listdir(os.path.join(DATA_PATH, dir))
        for file in dir_files:
            if file[-3:] == '.nc':
                files.append(os.path.join(DATA_PATH, dir, file))
    return files

def ingest_nc_file(file_path):
    with netcdf_file(file_path, maskandscale=True) as file:
        daily_mean_palmer_drought_severity_index = file.variables['daily_mean_palmer_drought_severity_index'][:].copy()
        day = file.variables['day'][:].copy()
        latitudes = file.variables['lat'][:].copy()
        longitudes = file.variables['lon'][:].copy()

    with open(file_path.replace('.nc', '.csv'), 'w') as f:
        EASTERN_BORDER_OF_NEW_MEXICO_LONGITUDE = -103 # further negative west (desc) - east (asc)
        for lat_index, lat in enumerate(latitudes):
            lines = ''
            for lon_index, lon in enumerate(longitudes):
                if lon > EASTERN_BORDER_OF_NEW_MEXICO_LONGITUDE:
                    break

                line = f'{lat} {lon}'
                for day in daily_mean_palmer_drought_severity_index:
                    drought_severity = day[lat_index][lon_index]
                    if drought_severity != '--':
                        line += f',{drought_severity}'
                    else:
                        line += ','

                if ',,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,' not in line:
                    line += '\n'
                    lines += line
            f.write(lines)

            if lat_index % 100 == 0:
                print(f"{file_path.split('/')[-1:]} at {lat_index} of 584")

def main():
    start = datetime.now()
    print(f'Start: {start}')
    files = get_fy_dirs_and_files()
    threads = []
    for i in range(len(files)):
        t = Thread(target=ingest_nc_file, args=(files[i],))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    print(f'Total runtime: {(datetime.now() - start).seconds} seconds')

if __name__ == '__main__':
    main()
