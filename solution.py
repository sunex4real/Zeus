import dask.dataframe as dd
import pyarrow
import os

GA_PATH = os.environ["GA_BUCKET_PATH"]
TXN_PATH = os.environ["TXN_BUCKET_PATH"]


def fetch_data_from_gcs(gcs_path):
    """
    This function takes the GCS bucket path as input
    to download the parquet files and returns a pandas
    dataframe
    """
    data = dd.read_parquet(gcs_path, engine='pyarrow')
    data = data.compute()
    return data


def get_session_details(customer_data):
    pass


def get_transaction_details(transactionid):
    pass


def main(fullvisitorid):
    pass
