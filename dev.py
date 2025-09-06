import os

# Set environment variables for this run
os.environ['CONNECTOR_NAME'] = 'eia-gov-data'
os.environ['RUN_ID'] = 'local-dev-test'
os.environ['ENABLE_HTTP_CACHE'] = 'true'
os.environ['CACHE_REQUESTS'] = 'false'
os.environ['WRITE_SNAPSHOT'] = 'false'
os.environ['DISABLE_STATE'] = 'false'
os.environ['STORAGE_BACKEND'] = 'local'
os.environ['DATA_DIR'] = 'data'

# Test with just the TOTAL dataset
from utils import validate_environment, upload_data
import pyarrow as pa
from assets.series.series import download_and_extract_in_memory, extract_series_metadata
from assets.prices.prices import extract_time_series_data

validate_environment()

# Test with one dataset
test_dataset = {"code": "TOTAL", "name": "Total Energy", "zip_path": "TOTAL.zip"}
print(f"Testing with {test_dataset['name']} dataset...")

# Download once
content = download_and_extract_in_memory(test_dataset)

# Extract series
series_data = extract_series_metadata(content)
for s in series_data:
    s['dataset_code'] = test_dataset['code']
    s['dataset_name'] = test_dataset['name']
series_table = pa.Table.from_pylist(series_data)
print(f"Series table: {len(series_table)} rows, {series_table.column_names}")

# Extract prices  
prices_data = extract_time_series_data(content, test_dataset['code'])
prices_table = pa.Table.from_pylist(prices_data)
print(f"Prices table: {len(prices_table)} rows, {prices_table.column_names}")

# Upload
upload_data(series_table, "series")
upload_data(prices_table, "prices")

print("Test complete!")