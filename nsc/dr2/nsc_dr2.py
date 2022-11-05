"""
ETL for NSC DR2


"""
import argparse
import datetime
import glob
import logging
import multiprocessing
import os
import shutil
import sys
from functools import cache
from pathlib import Path

import healpy as hp
import numpy as np
import pandas as pd
from astropy.time import Time
from dl import authClient as ac
from dl import queryClient as qc
from google.cloud import storage

logger = logging.getLogger(__name__)

storage_client = storage.Client(project="moeyens-thor-dev")

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


class ADAMDatasetETL:
    def __init__(self, data_dir="data"):
        self.data_dir = Path(data_dir).resolve()
        self.token = _dl_token()
        self.dataset_id = "nsc_dr2"

    def run(self, use_cache=True):
        logger.info(f"Running on {self.data_dir} with cache={use_cache}")
        # If we want to start fresh, clean the slate.
        if use_cache == False:
            shutil.rmtree(self.data_dir, ignore_errors=True)

        os.makedirs(self.data_dir, exist_ok=True)

        self.extract()
        self.transform()
        self.load()

    def queryRASlice(self, ra_start, ra_end, check_download_integrity=True):
        """
        Divide the sky by RA and fetch in slices to avoid timeouts
        """
        logger.info(f"Fetching RA {ra_start} - {ra_end}")
        NDET = 4  # We should parameterize this differently
        file_name = os.path.join(
            self.data_dir, f"nsc_dr2_observations_{ra_start:06.2f}_{ra_end:06.2f}.csv"
        )

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
            results.to_csv(file_name, index=False)

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


    def processWindow(self, window_file_name, observations):
        logger.info(f"Transforming {window_file_name}")

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
        logger.info("Extracting exposures")
        exposures_file_name = os.path.join(self.data_dir, "nsc_dr2_exposure.csv")
        if not os.path.exists(exposures_file_name):
            query = """SELECT * FROM nsc_dr2.exposure ORDER BY mjd ASC"""
            result = qc.query(self.token, sql=query, fmt="csv", out=exposures_file_name)

        ras = np.linspace(0, 360, 360 * 20 + 1)
        pool = multiprocessing.Pool(10)
        pool.starmap(self.queryRASlice, zip(ras[:-1][:10], ras[1:][:10]))
        pool.close()


    def transform(self):
        """ """
        exposures_file_name = os.path.join(self.data_dir, "nsc_dr2_exposure.csv")
        exposures = pd.read_csv(exposures_file_name, index_col=False)

        window_size = 31
        window_starts = np.arange(
            np.floor(exposures["mjd"].min()), np.ceil(exposures["mjd"].max()), window_size
        )
        observation_files = sorted(
            glob.glob(os.path.join(self.data_dir, "nsc_dr2_observations*.csv"))
        )

        os.makedirs(os.path.join(self.data_dir, "hdf5"), exist_ok=True)

        pool = multiprocessing.Pool(10)

        objids = []
        obsids = []
        observation_files_completed = np.array([])
        for i, observation_file in enumerate(observation_files):
            observations = pd.read_csv(observation_file, index_col=False)
            objids.append(observations["id"].unique())
            obsids.append(observations["measid"].unique())

            windows = []
            window_file_names = []
            for window_start in window_starts:

                window_end = window_start + window_size
                start_isot = Time(window_start, scale="utc", format="mjd").isot.split("T")[
                    0
                ]
                end_isot = Time(window_end, scale="utc", format="mjd").isot.split("T")[0]

                window_file_name = os.path.join(
                    self.data_dir, "hdf5", f"nsc_dr2_observations_{start_isot}_{end_isot}.h5"
                )
                window_file_names.append(window_file_name)

                observations_window = observations[
                    (observations["mjd_start"] >= window_start)
                    & (observations["mjd_start"] < window_end)
                ]
                windows.append(observations_window)

            pool.starmap(self.processWindow, zip(window_file_names, windows))

            observation_files_completed = np.concatenate(
                [observation_files_completed, np.array([observation_file])]
            )
            np.savetxt(
                "files_processed.txt", observation_files_completed, delimiter="\n", fmt="%s"
            )

            if (i + 1) % 20 == 0:
                print(f"Processed {i + 1} observations files.")

        objids = np.concatenate(objids)
        obsids = np.concatenate(obsids)
        pool.close()

        observations_h5 = sorted(glob.glob(os.path.join(self.data_dir, "hdf5", "*.h5")))

        os.makedirs(os.path.join(self.data_dir, "preprocessed"), exist_ok=True)

        for i, file_in in enumerate(observations_h5):
            logger.info("Processing {}".format(file_in))

            df = pd.read_hdf(file_in, key="data")

            # Exposure times in the NSC measurements table report the start of the exposure
            #
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
            df.sort_values(
                by=["mjd_utc", "observatory_code"], inplace=True, ignore_index=True
            )

            df["cal_month"] = (
                pd.to_datetime(df["mjd_utc"] + 2400000.5, origin="julian", unit="D")
                .dt.to_period("M")
                .astype(str)
            )

            # Save the results in files unique to year-month for indexing
            unique_months = df["cal_month"].unique()
            for month in unique_months:
                logger.info(f"Working on {month}")
                month_df = df[df["cal_month"] == month].copy()
                month_df.drop("cal_month", axis=1, inplace=True)
                # month_df = month_df.loc[: df.columns!="cal_month"]
                print(month_df.columns)
                month_df.to_hdf(
                    os.path.join(self.data_dir, "preprocessed", f"{month}.h5"),
                    key="data",
                    index=False,
                    mode="a",
                    format="table",
                    append=True,
                    min_itemsize={"obs_id": 40, "exposure_id": 40, "filter" : 2},
                )


            if (i + 1) % 20 == 0:
                print(f"Processed {i + 1} observations files.")


    def load(self):
        """
        Send the processed files to cloud bucket for downstream consumption
        """
        logger.info("Loading data to cloud bucket")
        today = datetime.datetime.utcnow().date().strftime("%Y-%m-%d")
        # TODO: make destination bucket dynamic based on environment or parameter
        bucket_name = f"adam-dataset-dev"
        folder_name = f"{today}/nsc_dr2"
        # Create bucket if it doesn't exist
        try:
            response = storage_client.create_bucket(bucket_name)
            print(response)
        except Exception as e:
            print(e)
            logger.info(f"Bucket {bucket_name} already exists.")

        bucket = storage_client.get_bucket(bucket_name)
        upload_files = sorted(glob.glob(os.path.join(self.data_dir, "preprocessed", "*.h5")))
        for filename in upload_files:
            print(filename)
            blob = bucket.blob(f"{folder_name}/{Path(filename).name}")
            blob.upload_from_filename(filename)
            






if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data_dir", type=str, default="data")
    parser.add_argument("--use_cache", type=bool, default=False)
    parser.add_argument("--log_level", type=str, default="INFO")
    args = parser.parse_args()

    logging.basicConfig(level=args.log_level, format="%(asctime)s %(message)s")

    etl = ADAMDatasetETL(data_dir=args.data_dir)
    etl.run(use_cache=args.use_cache)

