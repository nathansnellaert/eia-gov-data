import os
os.environ['CONNECTOR_NAME'] = 'eia-gov-data'
os.environ['RUN_ID'] = os.getenv('RUN_ID', 'local-run')

import subprocess
import sys
import platform
import resource
from pathlib import Path
from datetime import datetime
from utils import validate_environment, load_state

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

def get_memory_limit():
    """
    Get memory limit for subprocesses from MAX_PROCESS_MEMORY env var.
    Defaults to 4GB if not set.
    """
    max_memory_gb = os.getenv('MAX_PROCESS_MEMORY', '4')
    
    try:
        memory_limit_gb = float(max_memory_gb)
        memory_limit = int(memory_limit_gb * 1024 * 1024 * 1024)  # Convert GB to bytes
        return memory_limit
    except ValueError:
        print(f"Warning: Invalid MAX_PROCESS_MEMORY value '{max_memory_gb}', using 4GB default")
        return 4 * 1024 * 1024 * 1024

def process_dataset_subprocess(dataset: dict) -> bool:
    """
    Process a dataset in a subprocess with memory constraints.
    
    Returns:
        True if successful, False otherwise
    """
    memory_limit = get_memory_limit()
    
    cmd = [
        sys.executable,
        "process_dataset.py",
        dataset['code'],
        dataset['name'],
        dataset['zip_path']
    ]
    
    try:
        # Set up environment with memory limit info
        env = os.environ.copy()
        env['MEMORY_LIMIT_BYTES'] = str(memory_limit)
        
        # Platform-specific memory limiting
        preexec_fn = None
        if platform.system() == 'Linux':
            # On Linux, use RLIMIT_AS (address space) to enforce memory limit
            preexec_fn = lambda: resource.setrlimit(resource.RLIMIT_AS, (memory_limit, memory_limit))
        elif platform.system() == 'Darwin':
            # On macOS, RLIMIT_AS doesn't work well, use RLIMIT_DATA instead
            preexec_fn = lambda: resource.setrlimit(resource.RLIMIT_DATA, (memory_limit, memory_limit))
        
        # Run in subprocess with memory constraints
        result = subprocess.run(
            cmd,
            cwd=Path(__file__).parent,
            capture_output=True,
            text=True,
            timeout=1200,  # 20 minute timeout per dataset
            env=env,
            preexec_fn=preexec_fn
        )
        
        # Print subprocess output
        if result.stdout:
            print(result.stdout.strip())
        if result.stderr and result.returncode != 0:
            # Check for memory-related errors
            if "MemoryError" in result.stderr or "Cannot allocate memory" in result.stderr:
                print(f"✗ Memory limit exceeded for {dataset['code']}")
            else:
                print(result.stderr.strip(), file=sys.stderr)
        
        return result.returncode == 0
        
    except subprocess.TimeoutExpired:
        print(f"✗ Timeout processing {dataset['code']}")
        return False
    except Exception as e:
        print(f"✗ Error processing {dataset['code']}: {e}")
        return False

def main():
    validate_environment()
    
    # Check which datasets need updating
    datasets_to_process = []
    up_to_date_datasets = []
    
    for dataset in EIA_HISTORICAL_DATASETS:
        # Check state for both series and prices
        series_state = load_state(f"{dataset['code'].lower()}_series")
        prices_state = load_state(f"{dataset['code'].lower()}_prices")
        
        # Check if both are up to date
        needs_update = False
        
        if not series_state or 'last_updated' not in series_state:
            needs_update = True
        elif not prices_state or 'last_updated' not in prices_state:
            needs_update = True
        else:
            # Check age of data
            series_updated = datetime.fromisoformat(series_state['last_updated'])
            prices_updated = datetime.fromisoformat(prices_state['last_updated'])
            oldest_update = min(series_updated, prices_updated)
            days_ago = (datetime.now() - oldest_update).days
            
            if days_ago >= 30:
                needs_update = True
            else:
                up_to_date_datasets.append((dataset['name'], days_ago))
        
        if needs_update:
            datasets_to_process.append(dataset)
    
    # Print summary
    print(f"\n📊 EIA Dataset Status:")
    print(f"  ✓ Up-to-date (< 30 days): {len(up_to_date_datasets)}")
    print(f"  ⏳ To process: {len(datasets_to_process)}")
    
    if up_to_date_datasets:
        print(f"\n✓ Recently updated datasets:")
        for name, days in sorted(up_to_date_datasets, key=lambda x: x[1]):
            print(f"  - {name} ({days} days ago)")
    
    if not datasets_to_process:
        print("\n✅ All datasets are up to date!")
        return
    
    print(f"\n🚀 Processing {len(datasets_to_process)} datasets...")
    
    # Process each dataset in a subprocess
    successful = []
    failed = []
    
    for i, dataset in enumerate(datasets_to_process, 1):
        print(f"\n[{i}/{len(datasets_to_process)}] Processing {dataset['name']}...")
        
        success = process_dataset_subprocess(dataset)
        
        if success:
            successful.append(dataset['name'])
        else:
            failed.append(dataset['name'])
    
    # Print final summary
    print("\n" + "="*50)
    print("📊 EIA Data Connector Summary")
    print("="*50)
    
    if successful:
        print(f"\n✅ Successfully processed {len(successful)} datasets")
    
    if failed:
        print(f"\n❌ Failed to process {len(failed)} datasets:")
        for name in failed:
            print(f"  - {name}")
    
    print("\n✨ EIA connector run complete!")

if __name__ == "__main__":
    main()