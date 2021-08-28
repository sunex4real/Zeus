# Zeus

This repo contains solution to the GA task

## Setup

This project was written and tested on a MacOS using python version 3.8

### Clone Repository

```bash
git clone https://github.com/sunex4real/Zeus.git
cd Zeus
```

#### Running from Terminal

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install pipenv.

```bash
pip3 install --user pipenv
```

Install dependencies using pipenv

```bash
python3 -m pipenv install
```

Export Environment Variables (Replace BUCKET and PATH with the real value)

```bash
export GA_BUCKET_PATH='gs://{BUCKET}/{PATH}/*.parquet'
export TXN_BUCKET_PATH='gs://{BUCKET}/{PATH}/*.parquet'
```

Run tests to ensure everything is working perfectly

```bash
python3 -m pipenv run python unit_test.py
```

Test with any visitorid by passing the as an argument

```bash
python3 -m pipenv run python solution.py --fullvisitorid='{}'
```

#### Running from Docker Container

Build the docker file

```bash
docker build --tag zeus .
```

SSH into the docker container

```bash
docker run --rm -it zeus bash
```

Export Environment Variables (Replace BUCKET and PATH with the real value)

```bash
export GA_BUCKET_PATH='gs://{BUCKET}/{PATH}/*.parquet'
export TXN_BUCKET_PATH='gs://{BUCKET}/{PATH}/*.parquet'
```

Run tests to ensure everything is working perfectly

```bash
python3 -m pipenv run python unit_test.py
```

Test with any visitorid by passing the as an argument

```bash
python3 -m pipenv run python solution.py --fullvisitorid='{}'
```

### Comments

This task was done based on some key assumptions that could be wrong but I tried to download the delivery app to see the flow but it appears it's not available in my Location.
Here are the few assumptions made:

1. While analyzing the data on jupyter notebook I realised the the transactionid in the session data that will be used to join the transaction data in the backend can be gotten in two ways:  
   a. Either selecting the transactionid column after unnesting the hits data.  
   b. Looking at a key in the custom dimension. ​I ended up using the Custom Dimension column.

2. The part where we need to check if location matched wasn't clear enough as
   I understood it in two different ways:  
    a. Comparing the coordinates on frontend with the customer coordinates on the
   backend.  
    b. Comparing the two geopoints column on the backend data but this will
   lead to another assumption because realistically deliveries might not be
   exact in terms of coordinates so we might need to measure by how close
   (distance in meters).
   I used the first option.
