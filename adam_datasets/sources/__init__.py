"""
Base class for ADAM ETL jobs.
"""
import base64
import collections
import datetime
import glob
import logging
import os
import shutil
from pathlib import Path

import google_crc32c
from google.cloud import storage


def generate_file_crc32c(path, blocksize=2**20):
    """
    Generate a base64 encoded crc32c checksum for a file to compare with google cloud storage.
    """
    crc = google_crc32c.Checksum()
    read_stream = open(path, "rb")
    collections.deque(crc.consume(read_stream, blocksize), maxlen=0)
    read_stream.close()
    return base64.b64encode(crc.digest()).decode("utf-8")


class ADAMDatasetETL:
    def __init__(self, data_dir="data", out_dir="out", **kwargs):
        self.logger = logging.getLogger()
        logging.basicConfig(level="INFO", format="%(message)s")
        self.data_dir = Path(data_dir).resolve()
        self.out_dir = Path(out_dir).resolve()

    def run(self):
        self.logger.info(f"Running inside {self.data_dir}")

        # Start with a fresh slate
        shutil.rmtree(self.data_dir, ignore_errors=True)
        os.makedirs(self.data_dir, exist_ok=True)

        # Very important to remove out file every time, otherwise we end up appending
        # to the same files repeatedly.
        shutil.rmtree(self.out_dir, ignore_errors=True)
        os.makedirs(self.out_dir, exist_ok=True)

        self.extract()
        self.transform()
        self.load()

    def extract(self):
        """
        Fetch data from the remote resource and save it to self.data_dir
        """
        raise NotImplementedError

    def transform(self):
        """
        Process the saved data in `self.extract` and save it to YYYY-MM.h5 files

        The output files need to be saved to the self.out_dir directory.
        They files must be split into calendar month according to `mjd_utc` `YYYY-MM.h5`
        The schema for the h5 files must be:
            obs_id: str
            observatory_code: str
            exposure_id: str
            mjd_utc: float
            ra: float64
            ra_sigma: float64
            dec: float64
            dec_sigma: float64
            filter: str
        """

    def load(self):
        """
        Sends the transformed data files in self.out_dir to the destination cloud bucket.
        """
        storage_client = storage.Client(project="moeyens-thor-dev")

        self.logger.info("Loading data to cloud bucket")
        today = datetime.datetime.utcnow().date().strftime("%Y-%m-%d")
        # TODO: make destination bucket dynamic based on environment or parameter
        bucket_name = f"adam-dataset-dev"
        folder_name = f"{today}/{self.dataset_id}"

        # Create bucket if it doesn't exist
        try:
            storage_client.create_bucket(bucket_name)
        except Exception as e:
            self.logger.info(f"Bucket {bucket_name} already exists.")

        bucket = storage_client.get_bucket(bucket_name)
        local_files = sorted(glob.glob(os.path.join(self.out_dir, "*.h5")))

        # Note: We can't compare crc32 hashes for more effiecient uploads because
        # creating hdf5 is not deterministic (thereby producing different hashes)
        # Otherwise we would cross reference the remote files and only upload
        # ones that are not identical.

        # Empty the destination folder to ensure we don't have any old files
        existing_remote_files = list(
            storage_client.list_blobs(bucket_name, prefix=folder_name)
        )
        bucket.delete_blobs(existing_remote_files)

        # Upload the files that are not already on the bucket
        for filename in local_files:
            self.logger.info(
                f"Uploading {Path(filename).name} to {bucket_name}/{folder_name}"
            )
            blob = bucket.blob(f"{folder_name}/{Path(filename).name}")

            # Editing how google cloud does chunked uploads because is hilariously brittle.
            storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024 * 1024  # 5MB
            blob.chunk_size = 5 * 1024 * 1024  # Set 5 MB blob size
            blob.upload_from_filename(filename, num_retries=3)
        self.logger.info("Successfully uploaded all files.")
