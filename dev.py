import os

# Set environment variables for local testing
os.environ['CONNECTOR_NAME'] = 'eia-gov-data'
os.environ['RUN_ID'] = 'local-dev'
os.environ['ENABLE_HTTP_CACHE'] = 'true'
os.environ['CACHE_REQUESTS'] = 'true'
os.environ['CATALOG_TYPE'] = 'local'
os.environ['DATA_DIR'] = 'data'
os.environ['MAX_PROCESS_MEMORY'] = '2'  # Limit to 2GB for testing

# Test with just one small dataset first
from utils import validate_environment
import subprocess
import sys
from pathlib import Path

validate_environment()

# Test with NUC_STATUS first (smallest dataset)
test_dataset = {"code": "NUC_STATUS", "name": "U.S. Nuclear Outages", "zip_path": "NUC_STATUS.zip"}

print(f"Testing with {test_dataset['name']}...")

cmd = [
    sys.executable,
    "process_dataset.py",
    test_dataset['code'],
    test_dataset['name'],
    test_dataset['zip_path']
]

result = subprocess.run(
    cmd,
    cwd=Path(__file__).parent,
    capture_output=True,
    text=True,
    timeout=600
)

print(result.stdout)
if result.stderr:
    print("STDERR:", result.stderr)

print(f"Return code: {result.returncode}")