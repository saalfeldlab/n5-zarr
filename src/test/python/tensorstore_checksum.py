import logging
import tensorstore as ts
from crc32c import crc32c
import numpy as np
import sys

logger = logging.getLogger(__name__)

def zarr3_read_and_checksum_array(store_path):
    try:
        spec = {
            'driver': 'zarr3',
            'kvstore': {
                'driver': 'file',
                'path': store_path
            }
        }

        store = ts.open(spec).result()
        array = store.read().result()

        flat_array = array.flatten()

        checksum = crc32c(flat_array)

        print(f"Checksum for the dataset at {store_path}: {checksum}")

        return checksum

    except Exception as e:
        logger.error(f"Error occurred while reading array and calculating checksum: {e}")
        raise

def zarr2_read_and_checksum_array(store_path):
    try:
        spec = {
            'driver': 'zarr',
            'kvstore': {
                'driver': 'file',
                'path': store_path
            }
        }

        store = ts.open(spec).result()
        array = store.read().result()

        flat_array = array.flatten()

        checksum = crc32c(flat_array)

        print(f"Checksum for the dataset at {store_path}: {checksum}")

        return checksum

    except Exception as e:
        logger.error(f"Error occurred while reading array and calculating checksum: {e}")
        raise

def n5_read_and_checksum_array(store_path):
    try:
        spec = {
            'driver': 'n5',
            'kvstore': {
                'driver': 'file',
                'path': store_path
            }
        }

        store = ts.open(spec).result()
        array = store.read().result()

        flat_array = array.flatten()

        checksum = crc32c(flat_array)

        print(f"Checksum for the dataset at {store_path}: {checksum}")

        return checksum

    except Exception as e:
        logger.error(f"Error occurred while reading array and calculating checksum: {e}")
        raise

def main(store_path, *args):
    # Determine whether to use N5 or Zarr2 or Zarr3
    args = [arg.lower() for arg in args]
    valid_options = ['--zarr3', '--zarr2', '--n5', '--info']
    use_zarr3 = '--zarr3' in args
    use_n5 = '--n5' in args

    if any(arg.startswith('--') and arg not in valid_options for arg in args):
        raise Exception("Invalid option provided. Valid options are '--zarr3' or '--zarr2' or '--n5' or '--info'.")

    format = 3 if use_zarr3 else 2 if not use_n5 else 'n5'

    try:
        if format == 3:
            checksum = zarr3_read_and_checksum_array(store_path)
            print(f"Final checksum: {checksum}")
        elif format == 2:
            checksum = zarr2_read_and_checksum_array(store_path)
            print(f"Final checksum: {checksum}")
        elif format == 'n5':
            checksum = n5_read_and_checksum_array(store_path)
            print(f"Final checksum: {checksum}")
    
    except Exception as e:
        logger.error(f"Main processing failed: {e}")
        sys.exit(3)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    if len(sys.argv) < 2:
        logger.error("Usage: python tensorstore_checksum.py <store_path>")
        sys.exit(1)

    #store_path = sys.argv[1]

    try:
        main(*sys.argv[1:])
    except Exception as e:
        logger.error(f"Main processing failed with exception: {e}", exc_info=True)
        sys.exit(3)
