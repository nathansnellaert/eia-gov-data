import pyarrow as pa
import json
import zipfile
import io
from utils import get, load_state, save_state, get_run_id
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

def extract_series_metadata(content):
    """Extract series metadata (without time series data) from EIA data"""
    series_data = []
    for line in content.strip().split('\n'):
        if line.strip():
            try:
                obj = json.loads(line)
                # Extract metadata only (exclude 'data' field)
                metadata = {k: v for k, v in obj.items() if k != 'data'}
                # Add dataset code if not present
                if 'dataset' not in metadata and 'series_id' in metadata:
                    # Extract dataset code from series_id (usually first part before period)
                    dataset_code = metadata['series_id'].split('.')[0]
                    metadata['dataset'] = dataset_code
                series_data.append(metadata)
            except json.JSONDecodeError:
                continue
    
    return series_data

def process_series():
    """Process all EIA datasets and extract series metadata"""
    state = load_state("series")
    
    # Check if we've already processed data recently
    if state and 'last_updated' in state:
        last_updated = datetime.fromisoformat(state['last_updated'])
        days_since_update = (datetime.now() - last_updated).days
        if days_since_update < 30:
            print(f"Series data was updated {days_since_update} days ago, skipping refresh")
            return pa.Table.from_pylist([])
    
    all_series = []
    
    for dataset in EIA_HISTORICAL_DATASETS:
        print(f"Processing dataset: {dataset['name']}")
        try:
            # Download and extract dataset in memory
            file_content = download_and_extract_in_memory(dataset)
            
            # Extract series metadata
            series_data = extract_series_metadata(file_content)
            
            # Add dataset info to each series
            for series in series_data:
                series['dataset_code'] = dataset['code']
                series['dataset_name'] = dataset['name']
            
            all_series.extend(series_data)
            print(f"Extracted {len(series_data)} series from {dataset['name']}")
            
        except Exception as e:
            print(f"Error processing {dataset['name']}: {e}")
            continue
    
    # Update state
    save_state("series", {
        'last_updated': datetime.now().isoformat(),
        'series_count': len(all_series),
        'datasets_processed': len(EIA_HISTORICAL_DATASETS)
    })
    
    # Convert to PyArrow table
    if all_series:
        return pa.Table.from_pylist(all_series)
    else:
        return pa.Table.from_pylist([])