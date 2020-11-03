# NOAA-Global-Temp-Data-Processing
Prefect jobs to load data into local postgres from CSVs

Data source: https://www.ncei.noaa.gov/data/global-summary-of-the-day/access/
- This data is already being download by a couple of different prefect workflows.
- There are a hundreds of thousands of CSVs that need to be loaded as raw data.
- The goal is to load raw data into a local postgres database, then process it into the form that will be moved into a cloud postgres instance hosted on Heroku (this allows me to keep all of the raw data, but host 10s of thousands of rows in the cloud rather than millions). The data in the cloud will be made available by a future REST endpoint (using FastAPI).