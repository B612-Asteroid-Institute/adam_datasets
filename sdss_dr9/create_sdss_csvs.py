import pandas as pd
import numpy as np
import os
import glob
from astroquery.sdss import SDSS, Conf
#from astropy.config import set_temp_cache
#from astropy.utils.data import clear_download_cache
from astropy.time import Time
import shutil

DATA_DIR = '/astro/store/epyc3/data3/adam_datasets/sdss_dr9_all'
CACHE_DIR = '/astro/users/ejgl/.astropy/cache/astroquery/SDSS'

def query_by_ra(ra_start, ra_stop, check_download_integrity=False):
    file_name = os.path.join(DATA_DIR, f"sdss_dr9_observations_{ra_start:06.2f}_{ra_stop:06.2f}_all.csv")
    
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
        cache_files = glob.glob(CACHE_DIR + '/*')
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
            
ras = np.linspace(0, 360, 360 * 10 + 1)
for i in range(len(ras)-1):
    query_by_ra(ras[i], ras[i+1], check_download_integrity=False)