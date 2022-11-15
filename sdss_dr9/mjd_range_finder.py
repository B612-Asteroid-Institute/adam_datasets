import argparse
import pandas as pd
import os
import glob

def get_times(csvs):
    
    print('Finding earliest and latest times')

    min_time = None
    max_time = None

    for csv in csvs:
        df = pd.read_csv(csv, index_col=False, 
                         dtype={'observatory_code': str, 'obs_id': str, 'exposure_id': str})
        min_df = df.groupby('filter')['mjd_utc'].min()
        max_df = df.groupby('filter')['mjd_utc'].max()
        
        min_csv = min_df.min()
        max_csv = max_df.max()
        
        if min_time is None or min_time > min_csv:
            min_time = min_csv
            print(f'min time is now: {min_time}')
        if max_time is None or max_time < max_csv:
            max_time = max_csv
            print(f'max time is now: {max_time}')
    
                
    #mjd_min = min_time/(24*3600)
    #mjd_max = max_time/(24*3600)
    
    return min_time, max_time

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument('--path', required=True, help='directory of .csv files created with create_sdss_csvs.py')

    args = parser.parse_args()

    print(args)

    # csv_files = glob.glob(args.path + '/*')
    csv_files = [log for log in glob.glob(args.path + '/*') if not os.path.isdir(log)]
    print("csv's found!")

    mjd_min, mjd_max = get_times(csv_files)

    df = pd.DataFrame({'mjd_min':[mjd_min], 'mjd_max': [mjd_max]})
    if not os.path.exists(os.path.join(args.path, "metadata")):
        os.mkdir(os.path.join(args.path, "metadata"))
    df.to_csv(os.path.join(args.path, 'metadata/min_max_mjd.csv'))

