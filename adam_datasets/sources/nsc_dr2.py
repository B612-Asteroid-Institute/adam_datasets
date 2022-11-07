"""
ETL for NSC DR2
"""
import glob
import logging
import multiprocessing
import os
from functools import cache

import numpy as np
import pandas as pd
from dl import authClient as ac
from dl import queryClient as qc

from adam_datasets.sources import ADAMDatasetETL


@cache
def _dl_token():
    """
    Grab credentials and set token
    """
    creds = os.getenv("DL_CREDENTIALS")
    if creds is None:
        raise Exception("Could not find DL_CREDENTIALS")
    username, password = creds.split(":")

    token = ac.login(username, password)
    if not ac.isValidToken(token):
        raise Exception("Could not generate valid token.")
    return token


class NSCDR2(ADAMDatasetETL):
    dataset_id = "nsc_dr2"

    def __init__(self, *args, **kwargs):
        self.token = _dl_token()
        super().__init__()

    def query_ra_slice(self, ra_start, ra_end, check_download_integrity=True):
        """
        Divide the sky by RA and fetch in slices to avoid timeouts
        """
        logger = logging.getLogger()
        logging.basicConfig(level="INFO", format="%(message)s")

        NDET = 4  # We should parameterize this differently
        file_name = os.path.join(
            self.data_dir, f"nsc_dr2_observations_{ra_start:06.2f}_{ra_end:06.2f}.csv"
        )
        logger.info(f"Fetching results for RA {ra_start} to {ra_end}")
        if not os.path.exists(file_name):
            query = f"""
            SELECT o.id, o.ra AS mean_ra, o.dec AS mean_dec, o.ndet, o.nphot, o.mjd AS mean_mjd, o.deltamjd, m.measid, m.mjd AS mjd_start, e.exptime, m.ra, m.dec, m.raerr, m.decerr, m.mag_auto, m.magerr_auto, m.filter, m.exposure, m.class_star 
            FROM nsc_dr2.object AS o 
            JOIN nsc_dr2.meas as m 
            ON o.id = m.objectId 
            JOIN nsc_dr2.exposure as e
            ON e.exposure = m.exposure
            WHERE (o.ndet <= {NDET}) AND (o.ra >= {ra_start}) AND (o.ra < {ra_end})
            """
            results = qc.query(
                self.token,
                adql=query,
                fmt="pandas",
                timeout=3600,
                async_=True,
                wait=True,
                poll=5,
                verbose=0,
            )
            results.sort_values(
                by=["mjd_start", "measid"], inplace=True, ascending=[True, True]
            )
            results.to_csv(file_name, mode="w", index=False)

            logger.info(f"Finishing fetching {ra_start} - {ra_end}")

        if check_download_integrity:

            downloaded_results = pd.read_csv(file_name, index_col=False)

            query = f"""
            SELECT COUNT(*) 
            FROM nsc_dr2.object AS o 
            JOIN nsc_dr2.meas as m 
            ON o.id = m.objectId 
            WHERE (o.ndet <= {NDET}) AND (o.ra >= {ra_start}) AND (o.ra < {ra_end})
            """
            results = qc.query(
                self.token,
                adql=query,
                fmt="pandas",
                timeout=3600,
                async_=True,
                wait=True,
                poll=5,
                verbose=0,
            )

            n_results = results["COUNT"].values[0]
            n_downloaded_results = len(downloaded_results)
            if n_results != n_downloaded_results:
                err = f"Downloaded file ({file_name}) contains {n_results} rows while query expected {n_downloaded_results} rows."
                raise ValueError(err)
            logger.info(f"Integrity of {ra_start} - {ra_end} is valid.")

        return

    def process_window(self, window_file_name, observations):
        self.logger.info(f"Transforming {window_file_name}")

        if len(observations) > 0:
            observations.to_hdf(
                window_file_name,
                key="data",
                mode="a",
                append=True,
                min_itemsize={"id": 40, "measid": 40, "exposure": 40, "filter": 2},
            )
        return

    def extract(self):
        """
        Retrieve exposures,
        """

        ras = np.linspace(0, 360, 360 * 20 + 1)
        pool = multiprocessing.Pool(10)
        pool.starmap(self.query_ra_slice, zip(ras[:-1][:10], ras[1:][:10]))
        pool.close()

    def transform(self):

        observation_files = sorted(
            glob.glob(os.path.join(self.data_dir, "nsc_dr2_observations*.csv"))
        )

        for i, observation_file in enumerate(observation_files):
            self.logger.info("Processing {}".format(observation_file))
            df = pd.read_csv(observation_file, index_col=False)

            # Exposure times in the NSC measurements table report the start of the exposure
            df["mjd_mid"] = df["mjd_start"] + df["exptime"] / 2 / 86400
            df = df[
                [
                    "measid",
                    "exposure",
                    "mjd_start",
                    "mjd_mid",
                    "ra",
                    "dec",
                    "raerr",
                    "decerr",
                    "filter",
                    "mag_auto",
                    "magerr_auto",
                ]
            ]
            df.rename(
                columns={
                    "measid": "obs_id",
                    "exposure": "exposure_id",
                    "mjd_mid": "mjd_utc",
                    "ra": "ra",
                    "dec": "dec",
                    "raerr": "ra_sigma",
                    "decerr": "dec_sigma",
                    "mag_auto": "mag",
                    "magerr_auto": "mag_sigma",
                    "filter": "filter",
                },
                inplace=True,
            )
            df.loc[:, "ra_sigma"] /= 3600.0
            df.loc[:, "dec_sigma"] /= 3600.0
            df.loc[df["obs_id"].str[:3].isin(["c4d"]), "observatory_code"] = "W84"
            df.loc[df["obs_id"].str[:3].isin(["ksb"]), "observatory_code"] = "V00"
            df.loc[df["obs_id"].str[:3].isin(["k4m"]), "observatory_code"] = "695"

            df["cal_month"] = (
                pd.to_datetime(df["mjd_utc"] + 2400000.5, origin="julian", unit="D")
                .dt.to_period("M")
                .astype(str)
            )

            # Save the results in files unique to year-month for indexing
            unique_months = df["cal_month"].unique()
            for month in unique_months:
                month_df = df[df["cal_month"] == month].copy()

                month_df.to_hdf(
                    os.path.join(self.out_dir, f"{month}.h5"),
                    key="data",
                    index=False,
                    mode="a",
                    format="table",
                    append=True,
                    min_itemsize={"obs_id": 40, "exposure_id": 40, "filter": 2},
                )

            if (i + 1) % 20 == 0:
                self.logger.info(f"Processed {i + 1} observations files.")

        # Note, it does not matter if the files generated are different on each run.
        # HDF5 files cannot be created deterministically and so hashes will always
        # be different.
        # It would be cool if `df.to_hdf` was thread-safe, so we could run this in
        # parallel for each observation file.
