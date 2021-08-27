import dask.dataframe as dd
import os

GA_PATH = os.environ["GA_BUCKET_PATH"]
TXN_PATH = os.environ["TXN_BUCKET_PATH"]


def fetch_data_from_gcs(gcs_path):
    """
    This function takes the GCS bucket path as input
    to download the parquet files and returns a pandas
    dataframe
    :param gcs_path: GCS Bucket Path
    :return : Pandas Dataframe
    """
    data = dd.read_parquet(gcs_path, engine='pyarrow')
    data = data.compute()
    return data


def get_session_details(customer_data):
    """
    This function takes the customer dataframe as input
    then convert to python dictionary for efficient processing
    to calculate the coordinates change during the session
    and also the transactionid if exist.

    :param customer_data: Customer Dataframe that contains the
    actions generated in the session.
    :return tuple: (Boolean, str) where index 0 is the flag that
    shows True if there is a change in address and False otherwise
    while index 1 is for transactionid.
    """
    events = customer_data.to_dict()[0]
    lat_lon_cordinates = set()
    transactionid = set()
    for event in events:
        custom_dimensions = event['customDimensions']

        lat_lon_cordinates.update([lat_lon['value'] for lat_lon in custom_dimensions if lat_lon['index'] in (19, 18) and lat_lon['value'] not in ('NA')])
        transactionid.update([transaction['value'] for transaction in custom_dimensions if transaction['index'] == 36])

    if len(lat_lon_cordinates) > 2:
        return (True, transactionid.pop())
    else:
        return (False, transactionid.pop())


def get_transaction_details(transactionid):
    pass


def main(fullvisitorid):
    pass
