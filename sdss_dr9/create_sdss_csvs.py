import pandas as pd
import numpy as np
import os
import glob
import argparse
from astroquery.sdss import SDSS, Conf
from astropy.time import Time


def query_by_ra(ra_start, ra_stop, path, cache, check_download_integrity=False):
    file_name = os.path.join(path, f"sdss_dr9_observations_{ra_start:06.2f}_{ra_stop:06.2f}_all.csv")
    
    if not os.path.exists(file_name):
        print(ra_start, ra_stop)
        query = f"""
                SELECT objID, fieldID, field, ra, dec, raErr, decErr, 
                u, err_u, TAI_u, 
                g, err_g, TAI_g, 
                r, err_r, TAI_r, 
                i, err_i, TAI_i,
                z, err_z, TAI_z
                FROM PhotoObj
                WHERE (ra >= {ra_start}) AND (ra < {ra_stop}) 
                """
        raw_data = SDSS.query_sql(query, timeout=6000, data_release=9).to_pandas()
        
        filters = ['u', 'g', 'r', 'i', 'z']
        columns = ['ra', 'dec', 'fieldID', 'objID', 'raErr', 'decErr', 'obs']
        df_list = []
        for f in filters:
            df = pd.DataFrame(columns=['obs_id', 'ra', 'dec', 'ra_sigma', 'dec_sigma', 
                                       'mag', 'mag_sigma', 'filter', 'observatory_code'])

            df['obs_id'] = raw_data['objID'].apply(lambda x: f'{x}_{f}')
            df['exposure_id'] = raw_data['fieldID']
            df['ra'] = raw_data['ra']
            df['ra_sigma'] = raw_data['raErr']
            df['dec'] = raw_data['dec']
            df['dec_sigma'] = raw_data['decErr']
            df['filter'] = f
            df['mag'] = raw_data[f]
            df['mag_sigma'] = raw_data[f'err_{f}']
            df['mjd_utc'] = Time(raw_data[f'TAI_{f}']/(24*3600), scale='tai', format='mjd').utc.mjd
            df['observatory_code'] = '645'
            df_list.append(df)

        df = pd.concat(df_list, ignore_index=True)
        print(f'before: {len(df)}')
        df = df[df['mag'] != -9999]
        print(f'after: {len(df)}')
        df.sort_values(by=['mjd_utc', 'exposure_id'], ignore_index=True)

        df.to_csv(file_name, index=False)
        #clear_download_cache()
        cache_files = glob.glob(cache + '/*')
        #print(len(cache_files))
        for f in cache_files:
            os.remove(f)
        #print(len(cache_files))
        
        
    if check_download_integrity:

        downloaded_results = pd.read_csv(file_name, index_col=False)

        query = f"""
                SELECT COUNT(*) AS COUNT
                FROM PhotoObj
                WHERE (ra >= {ra_start}) AND (ra < {ra_stop})
                """
        results = SDSS.query_sql(query, timeout=3600, data_release=9)
        #print(results)

        n_results = results["COUNT"][0]
        n_downloaded_results = len(downloaded_results)

        if n_results != n_downloaded_results:
            err = (f"Downloaded file ({file_name}) contains {n_results} rows while query expected {n_downloaded_results} rows.")
            raise ValueError(err)

if __name__ == "__main__":
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--path', required=True, help='directory of .csv files created with create_sdss_csvs.py')
    parser.add_argument('--cache', default='/astro/users/ejgl/.astropy/cache/astroquery/SDSS', help='directory of the astropy cache.')
    args = parser.parse_args()

    ras = np.linspace(0, 360, 360 * 10 + 1)
    for i in range(len(ras)-1):
        query_by_ra(ras[i], ras[i+1], args.path, args.cache)