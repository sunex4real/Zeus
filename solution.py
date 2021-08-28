import os
import sys
import logging
import dask.dataframe as dd
from argparse import ArgumentParser

logging.basicConfig(level=logging.DEBUG, filename="zeus.log", format="%(asctime)s %(levelname)s:%(message)s")

# Ensure all required environment variables are set
try:
    GA_PATH = os.environ["GA_BUCKET_PATH"]
    TXN_PATH = os.environ["TXN_BUCKET_PATH"]
except KeyError:
    logging.error("Unable to Load Environment Variables (GA_BUCKET_PATH,TXN_BUCKET_PATH)")
    sys.exit(1)


def fetch_data_from_gcs(gcs_path):
    """
    This function takes the GCS bucket path as input
    to download the parquet files and returns a pandas
    dataframe
    Parameters
    ----------
    gcs_path: GCS Bucket Path

    Returns
    ----------
    :Pandas Dataframe
    """
    logging.info("Fetching Data From GCS")
    data = dd.read_parquet(gcs_path, engine="pyarrow")
    data = data.compute()
    return data


def get_session_details(customer_data):
    """
    This function takes the customer dataframe as input
    then convert to python dictionary for efficient processing
    to calculate the coordinates change during the session
    and also the transactionid if exist.

    Parameters
    ----------
    Customer_data: Customer Dataframe that contains the
    actions generated in the session.

    Returns
    ----------
    tuple: (Boolean, str) where index 0 is the flag that
    shows True if there is a change in address and False otherwise
    while index 1 is for transactionid.
    """
    logging.info("Computing Session Details")
    events = customer_data.to_dict()[0]
    lat_lon_cordinates = set()
    transactionid = set()
    for event in events:
        custom_dimensions = event["customDimensions"]

        lat_lon_cordinates.update(
            [
                lat_lon["value"]
                for lat_lon in custom_dimensions
                if lat_lon["index"] in (19, 18) and lat_lon["value"] not in ("NA")
            ]
        )
        transactionid.update([transaction["value"] for transaction in custom_dimensions if transaction["index"] == 36])
    # checking the unique latitude and logitude to see if its greater than 2
    if len(lat_lon_cordinates) > 2:
        return (True, transactionid.pop() if len(transactionid) > 0 else None)
    else:
        return (False, transactionid.pop() if len(transactionid) > 0 else None)


def get_transaction_details(transactionid):
    """
    This function takes a str input of the transactionid
    and returns the details of the transaction.

    Parameters
    ----------
    Transactionid: transactionid generated from the session.

    Returns
    ----------
    tuple: (Boolean, Boolean) where index 0 is for order placement
    and index 1 is for order delivered.
    """
    if transactionid is None:
        return (False, False)
    # Fetch data from gcs
    transaction_data = fetch_data_from_gcs(TXN_PATH)
    logging.info("Computing Transaction Details")
    transaction_data = transaction_data[transaction_data.frontendOrderId == transactionid]
    # Check if order was received on the backend
    if len(transaction_data.index) == 0:
        return (False, False)
    # Check if order was delivered
    order_delivered = True if transaction_data["declinereason_code"].values[0] is None else False
    return (True, order_delivered)


def main(userid):
    """
    This main function recieves the userid and
    compute the summary of the session of the user

    Parameters
    ----------
    customerid str: The unique id of the customer

    Returns
    ----------
    The dictionary response that contains the value for each key
    """
    response = {}
    # input validation, return empty response if no value is passed
    if userid is None:
        return response
    if isinstance(userid, str):
        google_analytics_data = fetch_data_from_gcs(GA_PATH)
        customer_data = google_analytics_data[google_analytics_data.fullvisitorid == userid]
        # input validation, return empty response if userid not in parquet
        if len(customer_data.index) == 0:
            return response

        customer_data = customer_data.nlargest(1, "visitStartTime")
        address_changed, transactionid = get_session_details(customer_data["hit"])
        order_placed, order_delivered = get_transaction_details(transactionid)
        response.update(
            {
                "full_visitor_id": userid,
                "address_changed": address_changed,
                "is_order_placed": order_placed,
                "Is_order_delivered": order_delivered,
                "application_type": customer_data["operatingSystem"].values[0],
            }
        )
        return response
    # input validation, return empty response input isn't a string
    return response


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--fullvisitorid", type=str, required=True)
    args = parser.parse_args()
    response = main(args.fullvisitorid)
    print(response)
