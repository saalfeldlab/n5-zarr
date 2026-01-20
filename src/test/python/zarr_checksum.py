import logging
import zarr
from crc32c import crc32c
import numpy as np
import sys
import os
import json

logger = logging.getLogger(__name__)

def zarr2_read_and_checksum_array(store_path):
    try:
        zarr_store = zarr.open(store_path, mode='r')
        array = zarr_store[:]
        flat_array = array.flatten()
        
        checksum = crc32c(flat_array)
        
        print(f"Checksum for the dataset at {store_path}: {checksum}")
        return checksum
    
    except Exception as e:
        logger.error(f"Error occurred while reading array and calculating checksum: {e}")
        raise
 
def zarr3_read_and_checksum_array(store_path):
    try:
        zarr_store = zarr.open(store_path, mode='r')
        array = zarr_store[:]
        flat_array = array.flatten()
        
        checksum = crc32c(flat_array)
        
        print(f"Checksum for the dataset at {store_path}: {checksum}")
        return checksum
    
    except Exception as e:
        logger.error(f"Error occurred while reading array and calculating checksum: {e}")
        raise

def n5_read_and_checksum_array(store_path):
    try:
        fix_attributes_json(store_path)

        n5_store = zarr.N5FSStore(store_path)
        array = zarr.open(store=n5_store, mode='r')
        flat_array = array[:].flatten()

        checksum = crc32c(flat_array)

        print(f"Checksum for the N5 dataset at {store_path}: {checksum}")
        return checksum
    
    except Exception as e:
        logger.error("Store path: " + store_path)
        logger.exception(f"Error occurred while reading N5 array and calculating checksum: {e}")
        raise


# Function to load and fix the attributes.json metadata
def fix_attributes_json(store_path):
    # Define the path to attributes.json
    attributes_json_path = os.path.join(store_path, "attributes.json")

    # Check if the file exists
    if not os.path.exists(attributes_json_path):
        raise FileNotFoundError(f"Could not find attributes.json at {attributes_json_path}")

    # Load the content of attributes.json
    with open(attributes_json_path, "r") as file:
        attributes_data = json.load(file)

    # Check if the "n5" key is present, if not add it
    if "n5" not in attributes_data:
        attributes_data["n5"] = "4.0.0"
        print(f"Added 'n5': '4.0.0' to {attributes_json_path}")

        # Write the modified data back to attributes.json
        with open(attributes_json_path, "w") as file:
            json.dump(attributes_data, file, indent=4)
    else:
        print(f"'n5' version already exists in {attributes_json_path}")


def main(store_path, *args):
    args = [arg.lower() for arg in args]
    valid_options = ['--zarr3', '--zarr2', '--n5']
    
    use_zarr3 = '--zarr3' in args
    use_n5 = '--n5' in args
    
    if any(arg.startswith('--') and arg not in valid_options for arg in args):
        raise Exception("Invalid option provided. Valid options are '--zarr3', '--zarr2', or '--n5'.")

    #format = 3 if use_zarr3 else 2
    format = 3 if use_zarr3 else (5 if use_n5 else 2)

    try:
        if format == 3:
            checksum = zarr3_read_and_checksum_array(store_path)
            print(f"Final checksum: {checksum}")
        elif format == 2:
            checksum = zarr2_read_and_checksum_array(store_path)
            print(f"Final checksum: {checksum}")
        elif format == 5:
            checksum = n5_read_and_checksum_array(store_path)
            print(f"Final checksum: {checksum}")
    
    except Exception as e:
        logger.error(f"Main processing failed: {e}")
        sys.exit(3)

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    if len(sys.argv) < 2:
        logger.error("Usage: python zarr_checksum.py <store_path>")
        sys.exit(1)

    try:
        main(*sys.argv[1:])
    except Exception as e:
        logger.error(f"Main processing failed with exception: {e}", exc_info=True)
        sys.exit(3)
