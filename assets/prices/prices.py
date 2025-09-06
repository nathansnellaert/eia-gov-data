import pyarrow as pa
import json
import zipfile
import io
from utils import get, load_state, save_state
from datetime import datetime

# Dictionary of EIA historical datasets
EIA_HISTORICAL_DATASETS = [
    {"code": "COAL", "name": "Coal", "zip_path": "COAL.zip"},
    {"code": "EBA", "name": "U.S. Electric System Operating Data", "zip_path": "EBA.zip"},
    {"code": "ELEC", "name": "Electricity", "zip_path": "ELEC.zip"},
    {"code": "EMISS", "name": "CO2 Emissions", "zip_path": "EMISS.zip"},
    {"code": "INTL", "name": "International Energy Data", "zip_path": "INTL.zip"},
    {"code": "NG", "name": "Natural Gas", "zip_path": "NG.zip"},
    {"code": "NUC_STATUS", "name": "U.S. Nuclear Outages", "zip_path": "NUC_STATUS.zip"},
    {"code": "PET", "name": "Petroleum and other liquid fuels", "zip_path": "PET.zip"},
    {"code": "PET_IMPORTS", "name": "Crude Oil Imports", "zip_path": "PET_IMPORTS.zip"},
    {"code": "SEDS", "name": "State Energy Data System (SEDS)", "zip_path": "SEDS.zip"},
    {"code": "TOTAL", "name": "Total Energy", "zip_path": "TOTAL.zip"}
]

def download_and_extract_in_memory(dataset):
    """Download a dataset from EIA website and extract in memory"""
    url = f"https://www.eia.gov/opendata/bulk/{dataset['zip_path']}"
    print(f"Downloading {dataset['name']} from {url}")
    
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

def extract_time_series_data(content, dataset_code):
    """Extract time series data from EIA data"""
    prices_data = []
    for line in content.strip().split('\n'):
        if line.strip():
            try:
                obj = json.loads(line)
                if 'series_id' not in obj or 'data' not in obj or not isinstance(obj['data'], list):
                    continue
                
                series_id = obj['series_id']
                for date_val in obj['data']:
                    if isinstance(date_val, list) and len(date_val) == 2:
                        date, value = date_val
                        # Convert value to float if possible, otherwise store as string
                        try:
                            value = float(value) if value not in ['NM', 'NA', None, ''] else None
                        except (ValueError, TypeError):
                            value = None
                        
                        if value is not None:  # Only include numeric values
                            prices_data.append({
                                'series_id': series_id,
                                'dataset': dataset_code,
                                'timestamp': str(date),  # Ensure timestamp is string
                                'value': value
                            })
            except json.JSONDecodeError:
                continue
    
    return prices_data

def process_prices():
    """Process all EIA datasets and extract time series data"""
    state = load_state("prices")
    
    # Check if we've already processed data recently
    if state and 'last_updated' in state:
        last_updated = datetime.fromisoformat(state['last_updated'])
        days_since_update = (datetime.now() - last_updated).days
        if days_since_update < 30:
            print(f"Prices data was updated {days_since_update} days ago, skipping refresh")
            return pa.Table.from_pylist([])
    
    all_prices = []
    
    for dataset in EIA_HISTORICAL_DATASETS:
        print(f"Processing dataset: {dataset['name']}")
        try:
            # Download and extract dataset in memory
            file_content = download_and_extract_in_memory(dataset)
            
            # Extract time series data
            prices_data = extract_time_series_data(file_content, dataset['code'])
            
            all_prices.extend(prices_data)
            print(f"Extracted {len(prices_data)} data points from {dataset['name']}")
            
        except Exception as e:
            print(f"Error processing {dataset['name']}: {e}")
            continue
    
    # Update state
    save_state("prices", {
        'last_updated': datetime.now().isoformat(),
        'data_points': len(all_prices),
        'datasets_processed': len(EIA_HISTORICAL_DATASETS)
    })
    
    # Convert to PyArrow table
    if all_prices:
        return pa.Table.from_pylist(all_prices)
    else:
        return pa.Table.from_pylist([])