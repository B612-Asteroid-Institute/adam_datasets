"""
Run one or more ADAM ETL jobs.
"""
import argparse
import importlib
import inspect
import logging
import multiprocessing

from adam_datasets.sources import ADAMDatasetETL


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dataset",
        type=str,
        action="append",
        help="Name of a dataset module within etl.sources",
    )
    parser.add_argument("--data_dir", type=str, default="data")
    parser.add_argument("--out_dir", type=str, default="out")
    parser.add_argument("--log_level", type=str, default="INFO")
    args = parser.parse_args()

    # Use a thread safe logger
    logger = multiprocessing.get_logger()
    logger.setLevel(args.log_level)
    logger.addHandler(logging.StreamHandler())

    # Initialize the dataset(s) to run.
    for dataset in args.dataset:
        dataset_module = None
        try:
            # First look for it in our built in list of modules
            dataset_module = importlib.import_module(f"adam_datasets.sources.{dataset}")
        except ImportError:
            # We didn't find it there, so assume it's a custom python module.
            dataset_module = importlib.import_module(dataset)
        if dataset_module is None:
            raise ImportError(f"Unable to import dataset {dataset}")

        # Look for the ETL class inside the specified dataset module
        etl_classes = inspect.getmembers(
            dataset_module,
            predicate=lambda x: inspect.isclass(x)
            and issubclass(x, (ADAMDatasetETL))
            and x != ADAMDatasetETL,
        )
        for name, klass in etl_classes:
            etl_instance = klass(
                data_dir=args.data_dir, out_dir=args.out_dir, logger=logger
            )
            etl_instance.run()


if __name__ == "__main__":
    main()
