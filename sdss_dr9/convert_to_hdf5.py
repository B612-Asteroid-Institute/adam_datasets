import pandas as pd
import numpy as np
import os
pd.set_option("display.max_columns", None)
import glob
import multiprocessing as mp

import astroquery.SDSS as SDSS
from astropy.time import Time

DATA_DIR = '/astro/store/epyc3/data3/adam_datasets/sdss_dr9_all'

def processWindow(window_file_name, observations):
    if len(observations) > 0:
        observations.to_hdf(
            window_file_name, 
            key="data", 
            mode="a", 
            append=True, 
            min_itemsize={'obs_id': 40, 'exposure_id': 40, 'filter' : 1, 'observatory_code': 1},
        )
    return

def convert_to_hdf5(observation_files, test=False):
    os.nice(10)

    pool = mp.Pool(10)

    observation_files_completed = np.array([])
    for i, observation_file in enumerate(observation_files):
        observations = pd.read_csv(observation_file, index_col=False, 
                                   dtype={'observatory_code': str, 'obs_id': str, 'exposure_id': str})

        windows = []
        window_file_names = []
        for window_start in window_starts:

            window_end = window_start + window_size
            start_isot = Time(window_start, scale="utc", format="mjd").isot.split("T")[0]
            end_isot = Time(window_end, scale="utc", format="mjd").isot.split("T")[0]

            if test:
                window_file_name = os.path.join(DATA_DIR, "hdf5_test", f"sdss_dr9_observations_{start_isot}_{end_isot}.h5")
            else:
                window_file_name = os.path.join(DATA_DIR, "hdf5", f"sdss_dr9_observations_{start_isot}_{end_isot}.h5")
            window_file_names.append(window_file_name)

            observations_window = observations[(observations["mjd_utc"] >= window_start) & (observations["mjd_utc"] < window_end)]
            windows.append(observations_window)

        pool.starmap(
            processWindow,
            zip(window_file_names, windows)
        )

        observation_files_completed = np.concatenate([observation_files_completed, np.array([observation_file])])
        np.savetxt("files_processed.txt", observation_files_completed, delimiter="\n", fmt="%s")

        if (i + 1) % 20 == 0:
            print(f"Processed {i + 1} observations files.")

    pool.close()
    

min_time = None
max_time = None

for f in ['u', 'g', 'r', 'i', 'z']:
    min_max_table = SDSS.query_sql(f"""
                                   SELECT min(TAI_{f}) as min_TAI_{f}, 
                                   max(TAI_{f}) as max_TAI_{f} 
                                   FROM PhotoObj WHERE TAI_{f} != -9999
                                   """,
                                   timeout = 3600, 
                                   data_release=9)
    if min_time is None or min_time < min_max_table[f'min_TAI_{f}']:
        min_time = min_max_table[f'min_TAI_{f}']
    if max_time is None or max_time > min_max_table[f'max_TAI_{f}']:
        max_time = min_max_table[f'max_TAI_{f}']


mjd_min = min_time/(24*3600)
mjd_max = max_time/(24*3600)
    
    
window_size = 31
window_starts = np.arange(
    np.floor(mjd_min), 
    np.ceil(mjd_max), 
    window_size
)

#test
csv_files = glob.glob(DATA_DIR + '/*')[:10]
convert_to_hdf5(csv_files, test=True)
