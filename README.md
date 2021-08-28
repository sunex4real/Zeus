# Zeus

This repo contains solution to the Google Analytics task

## Setup

### Clone Repository

```bash
git clone git@github.com:sunex4real/Zeus.git
cd Zeus
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

Export environment variables (These are the two gcs bucket path) starting with gcs://

```bash
export GA_BUCKET_PATH='{}'
export TXN_BUCKET_PATH='{}'
```

Run tests to ensure everything is working perfectly

```bash
python3 -m pipenv run python unit_test.py
```

Test with any visitorid by passing the as an argument

```bash
python3 -m pipenv run python solution.py --fullvisitorid='{}'
```
