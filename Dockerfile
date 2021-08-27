FROM python:3.8-slim-buster

WORKDIR /Zeus

COPY . .

# install dependencies
RUN python3 -m pip install --user pipenv
RUN python3 -m pipenv install "dask[complete]" 
RUN python3 -m pipenv install pyarrow 

COPY . /Zeus/
