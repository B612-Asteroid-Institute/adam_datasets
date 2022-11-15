import pandas as pd
import numpy as np
import os
import glob
import multiprocessing as mp
import argparse
from astropy.time import Time

#DATA_DIR = '/astro/store/epyc3/data3/adam_datasets/sdss_dr9_all'

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

def convert_to_hdf5(observation_files, data_path):
    """
    Takes in a list of csv files created by create_sdss_csvs.py and their directory path
    and converts them into hdf5 files for indexing.
    """
    
    mjd_df = pd.read_csv(os.path.join(args.path, 'min_max_mjd.csv'))

    mjd_min = mjd_df['mjd_min'][0] 
    mjd_max = mjd_df['mjd_max'][0]

    window_size = 31
    window_starts = np.arange(
        np.floor(mjd_min), 
        np.ceil(mjd_max), 
        window_size
    )
    
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

            
            if not os.path.exists(os.path.join(data_path, "hdf5")):
                os.mkdir(os.path.join(data_path, "hdf5"))
            
            window_file_name = os.path.join(data_path, "hdf5", f"sdss_dr9_observations_{start_isot}_{end_isot}.h5")
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
    
if __name__ == "__main__":    
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--path', required=True, help='directory of .csv files created with create_sdss_csvs.py')
    args = parser.parse_args()

    csv_files = [log for log in glob.glob(args.path + '/sdss*.csv') if not os.path.isdir(log)]


    convert_to_hdf5(csv_files, args.path)


