#!/usr/bin/env python3
"""
Process a single EIA dataset.
Designed to be called as a subprocess to avoid memory issues.
"""
import os
import sys
import argparse
import json
import zipfile
import io
import pyarrow as pa
from datetime import datetime
from utils import get, upload_data, save_state, validate_environment

def download_and_extract_in_memory(dataset_code, zip_path):
    """Download a dataset from EIA website and extract in memory"""
    url = f"https://www.eia.gov/opendata/bulk/{zip_path}"
    print(f"Downloading {dataset_code} from {url}")
    
    response = get(url)
    
    # Load zip file into memory
    zip_data = io.BytesIO(response.content)
    
    # Extract in memory
    with zipfile.ZipFile(zip_data) as zip_ref:
        file_list = zip_ref.namelist()
        
        # For EIA data, there should be only one file
        if len(file_list) != 1:
            print(f"Warning: Expected 1 file in ZIP, found {len(file_list)}: {file_list}")
        
        # Extract the file content in memory
        file_content = zip_ref.read(file_list[0]).decode('utf-8')
        return file_content

def process_dataset(dataset_code: str, dataset_name: str, zip_path: str) -> None:
    """
    Process and upload a single EIA dataset.
    Extracts both series metadata and time series data.
    """
    # Download and extract dataset in memory
    file_content = download_and_extract_in_memory(dataset_code, zip_path)
    
    series_data = []
    prices_data = []
    
    for line in file_content.strip().split('\n'):
        if not line.strip():
            continue
            
        try:
            obj = json.loads(line)
            
            # Extract series metadata (without 'data' field)
            metadata = {k: v for k, v in obj.items() if k != 'data'}
            metadata['dataset_code'] = dataset_code
            metadata['dataset_name'] = dataset_name
            series_data.append(metadata)
            
            # Extract time series data if present
            if 'series_id' in obj and 'data' in obj and isinstance(obj['data'], list):
                series_id = obj['series_id']
                for date_val in obj['data']:
                    if isinstance(date_val, list) and len(date_val) == 2:
                        date, value = date_val
                        # Convert value to float if possible
                        try:
                            value = float(value) if value not in ['NM', 'NA', None, ''] else None
                        except (ValueError, TypeError):
                            value = None
                        
                        if value is not None:  # Only include numeric values
                            prices_data.append({
                                'series_id': series_id,
                                'dataset': dataset_code,
                                'timestamp': str(date),
                                'value': value
                            })
                            
        except json.JSONDecodeError:
            continue
    
    # Upload series metadata
    if series_data:
        series_table = pa.Table.from_pylist(series_data)
        print(f"Uploading {len(series_data)} series for {dataset_code}")
        upload_data(series_table, f"{dataset_code.lower()}_series")
        
        # Save state for series
        save_state(f"{dataset_code.lower()}_series", {
            'last_updated': datetime.now().isoformat(),
            'series_count': len(series_data),
            'dataset_code': dataset_code
        })
    
    # Upload time series data
    if prices_data:
        prices_table = pa.Table.from_pylist(prices_data)
        print(f"Uploading {len(prices_data)} data points for {dataset_code}")
        upload_data(prices_table, f"{dataset_code.lower()}_prices")
        
        # Save state for prices
        save_state(f"{dataset_code.lower()}_prices", {
            'last_updated': datetime.now().isoformat(),
            'data_points': len(prices_data),
            'dataset_code': dataset_code
        })
    
    print(f"✓ Successfully processed {dataset_code}: {len(series_data)} series, {len(prices_data)} data points")

def main():
    parser = argparse.ArgumentParser(description='Process a single EIA dataset')
    parser.add_argument('dataset_code', type=str, help='EIA dataset code (e.g., COAL)')
    parser.add_argument('dataset_name', type=str, help='Dataset name')
    parser.add_argument('zip_path', type=str, help='ZIP file path')
    
    args = parser.parse_args()
    
    # Set up environment
    os.environ['CONNECTOR_NAME'] = 'eia-gov-data'
    if not os.environ.get('RUN_ID'):
        os.environ['RUN_ID'] = f'dataset-{args.dataset_code}-{datetime.now().strftime("%Y%m%d%H%M%S")}'
    
    validate_environment()
    
    try:
        process_dataset(args.dataset_code, args.dataset_name, args.zip_path)
    except Exception as e:
        print(f"✗ Failed to process {args.dataset_code}: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()