import dask.dataframe as dd
import pyarrow

def fetch_data_from_gcs(gcs_path):
    data = dd.dataframe.read_parquet(gcs_path, engine='pyarrow')
    return data

def check_address_change(fullvisitorid):
    pass

def get_transaction_details(frontendid):
    pass

def main():
    pass