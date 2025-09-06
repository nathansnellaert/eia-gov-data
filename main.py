import os
os.environ['CONNECTOR_NAME'] = 'eia-gov-data'
os.environ['RUN_ID'] = os.getenv('RUN_ID', 'local-run')

from utils import validate_environment, upload_data
from assets.series.series import process_series
from assets.prices.prices import process_prices

def main():
    validate_environment()
    
    # Process and upload metadata first
    series_data = process_series()
    upload_data(series_data, "series")
    
    # Process and upload time series data
    prices_data = process_prices()
    upload_data(prices_data, "prices")

if __name__ == "__main__":
    main()