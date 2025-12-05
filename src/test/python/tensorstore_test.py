###
# #%L
# Not HDF5
# %%
# Copyright (C) 2019 - 2022 Stephan Saalfeld
# %%
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
# 
# 1. Redistributions of source code must retain the above copyright notice,
#    this list of conditions and the following disclaimer.
# 2. Redistributions in binary form must reproduce the above copyright notice,
#    this list of conditions and the following disclaimer in the documentation
#    and/or other materials provided with the distribution.
# 
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDERS OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.
# #L%
###

from pathlib import Path
import numpy as np
import sys
import os
import tempfile
import tensorstore as ts
import logging
import itertools
logger = logging.getLogger(__name__)

def ts_create_n5_test(n5_path, data=None, chunk_shape=None, compression=None, level=1, resolution=None):
    """
    Function to create an N5 dataset using TensorStore.

    - n5_path: Path where the N5 dataset will be stored.
    - data: Numpy array with the data to be stored in the N5 format.
    - chunk_shape: Tuple specifying the shape of the chunks.
    - compression: Type of compression to apply (e.g., 'gzip', 'zstd', 'blosc', 'bzip2', 'xz', 'raw').
    - level: Compression level, relevant if compression is used.
    - resolution: Resolution of the dataset, provided as a list of numbers.
    """

    logger.info(f"Creating a new N5 store at {n5_path}.")

    if chunk_shape is None:
        if data is None:
            chunk_shape = (16, 16)
        else:
            chunk_shape = data.shape

    if data is None:
        data = np.arange(np.prod(chunk_shape)).reshape(chunk_shape)

    # Set up the basic N5 store specification
    n5_store_spec = {
        'driver': 'n5',
        'kvstore': {
            'driver': 'file',
            'path': n5_path
        },
        'metadata': {
            'dimensions': list(data.shape),
            'blockSize': list(chunk_shape),
            'dataType': data.dtype.name
        }
    }

    # Handle compression
    if compression == "zlib":
        compression = "gzip"
        
    if compression is not None:
        if compression == 'gzip':
            n5_store_spec['metadata']['compression'] = {
                "type": "gzip",
                "level": level
            }
        elif compression == 'blosc':
            n5_store_spec['metadata']['compression'] = {
                "type": "blosc",
                "cname": "lz4",
                "clevel": level,
                "shuffle": 1
            }
        elif compression == 'bzip2':
            n5_store_spec['metadata']['compression'] = {
                "type": "bzip2",
                "blockSize": level
            }
        elif compression == 'xz':
            n5_store_spec['metadata']['compression'] = {
                "type": "xz",
                "preset": level
            }
        elif compression == 'raw':
            n5_store_spec['metadata']['compression'] = {
                "type": "raw"
            }
        else:
            raise Exception(f"Unknown or unsupported compression type: {compression}")

    # Handle resolution if provided
    if resolution is not None:
        n5_store_spec['metadata']['resolution'] = resolution

    n5_store = ts.open(n5_store_spec, create=True, delete_existing=True).result()
    n5_store.write(data).result()

    logger.debug("N5 store has been updated.")
   
# Function to create Zarr2 dataset with TensorStore
def ts_create_zarr2_test(zarr2_path, data=None, chunk_shape=None, compression=None, level=1, fill_value=0, order='C'):
    """
    Function to create a Zarr2 dataset using TensorStore.

    - zarr2_path: Path where the Zarr2 dataset will be stored.
    - data: Numpy array with the data to be stored in the Zarr2 format.
    - chunk_shape: Tuple specifying the shape of the chunks.
    - compression: Type of compression to apply (e.g., 'zlib', 'gzip', 'zstd', 'blosc', 'bz2').
    - level: Compression level, relevant if compression is used.
    - fill_value: Default value to use for uninitialized chunks.
    """
    
    logger.info(f"Creating a new Zarr2 store at {zarr2_path}.")

    if chunk_shape is None:
        if data is None:
            chunk_shape = (16, 16)
        else:
            chunk_shape = data.shape

    if data is None:
        data = np.arange(np.prod(chunk_shape)).reshape(chunk_shape)
    
    # TensorStore Zarr2 dtype
    dtype_str = np.dtype(data.dtype).str

    zarr2_store_spec = {
        'driver': 'zarr',
        'kvstore': {
            'driver': 'file',
            'path': zarr2_path
        },
        'metadata': {
            'shape': list(data.shape),
            'chunks': list(chunk_shape),
            'dtype': dtype_str,
            'compressor': None,
            'fill_value': fill_value,
            'order': order
        }
    }

    if compression == "gzip":
        compression = "zlib"

    if compression is not None:
        if compression == 'zlib':
            zarr2_store_spec['metadata']['compressor'] = {
                "id": "zlib",
                "level": level
            }
        elif compression == 'zstd':
            zarr2_store_spec['metadata']['compressor'] = {
                "id": "zstd",
                "level": level
            }
        elif compression == 'blosc':
            zarr2_store_spec['metadata']['compressor'] = {
                "id": "blosc",
                "cname": "lz4",
                "clevel": level,
                "shuffle": 1
            }
        elif compression == 'bz2':
            zarr2_store_spec['metadata']['compressor'] = {
                "id": "bz2",
                "level": level
            }
        else:
            raise Exception("Unknown or unsupported compression name: " + compression)

    zarr2_store = ts.open(zarr2_store_spec, create=True, delete_existing=True).result()
    zarr2_store.write(data).result()
            
    logger.debug("Zarr2 store has been updated.")
    return zarr2_store


# Function to create Zarr3 dataset with TensorStore
def ts_create_zarr3_test(zarr3_path, data=None, chunk_shape=None, shard_shape=None, compression=None, transpose_order=None, level=1, fill_value=None):
    """
    Function to create a Zarr3 dataset using TensorStore.

    - zarr3_path: Path where the Zarr3 dataset will be stored.
    - data: Numpy array with the data to be stored in the Zarr3 format.
    - chunk_shape: Tuple specifying the shape of the chunks.
    - compression: Type of compression to apply (e.g., 'gzip', 'zstd', 'blosc').
    - level: Compression level, relevant if compression is used.
    """
    
    if shard_shape is not None:
        return ts_create_zarr3_sharded_test(zarr3_path, data=data, shard_shape=shard_shape, chunk_shape=chunk_shape, compression=compression, level=level)
    
    logger.info(f"Creating a new Zarr3 store at {zarr3_path}.")

    if chunk_shape is None:
        if data is None:
            chunk_shape = (16, 16)
        else:
            chunk_shape = data.shape

    if data is None:
        data = np.arange(np.prod(chunk_shape)).reshape(chunk_shape)



    zarr3_store_spec = {
        'driver': 'zarr3',
        'kvstore': {
            'driver': 'file',
            'path': zarr3_path},
        'metadata': {
            'shape': data.shape,
            'chunk_grid': {
                'name': 'regular',
                'configuration': {'chunk_shape': chunk_shape}
            },
            'chunk_key_encoding': {'name': 'default'},
            'data_type': data.dtype.name,
            'node_type': 'array',
        },
    }

    zarr3_store_spec['metadata']['codecs'] = []

    if transpose_order:
        zarr3_store_spec['metadata']['codecs'].append({
            'name': 'transpose',
            'configuration': {
                'order': transpose_order
            }
        })

    if fill_value:
      zarr3_store_spec['metadata']['fill_value'] = fill_value

    zarr3_store_spec['metadata']['codecs'].append(
        {
            'name': 'bytes',
            'configuration': {
                'endian': 'little'
            }
        }
    )

    if compression == "zlib":
        compression = "gzip"

    if compression == 'gzip' or compression == 'zstd':
        zarr3_store_spec['metadata']['codecs'].append({
            "name": compression,
            "configuration": {
                "level": level
            }
        })
    elif compression == 'blosc':
        zarr3_store_spec['metadata']['codecs'].append({
                "name": "blosc",
                "configuration": {
                    "cname": "blosclz",
                    "clevel": level,
                    "typesize": data.dtype.itemsize,
                    "shuffle": "bitshuffle"
                }
        })

    zarr3_store = ts.open(zarr3_store_spec, create=True, delete_existing=True).result()
    zarr3_store.write(data).result()
            
    logger.debug("Zarr3 store has been updated.")
    return zarr3_store

# Function to create sharded Zarr3 dataset with TensorStore
def ts_create_zarr3_sharded_test(zarr3_path, data=None, shard_shape=None, chunk_shape=None, compression=None, level=1, fill_value=None):
    """
    Function to create a sharded Zarr3 dataset using TensorStore.
    """
    logger.info(f"Creating a new Zarr3 store at {zarr3_path}.")

    if shard_shape is None:
        if data is None:
            shard_shape = (16, 16)
        else:
            shard_shape = data.shape
        
    if chunk_shape is None:
        chunk_shape = shard_shape

    if data is None:
        data = np.arange(np.prod(chunk_shape)).reshape(shard_shape)

    zarr3_store_spec = {
        'driver': 'zarr3',
        'kvstore': {
            'driver': 'file',
            'path': zarr3_path
        },
        'metadata': {
            'shape': data.shape,
            'chunk_grid': {
                'name': 'regular',
                'configuration': {'chunk_shape': shard_shape}
            },
            'chunk_key_encoding': {'name': 'default'},
            'data_type': data.dtype.name,
            'node_type': 'array'
        },
    }

    if fill_value is not None:
      zarr3_store_spec['metadata']['fill_value'] = fill_value

    zarr3_store_spec['metadata']['codecs'] = [
        {
            'name': 'sharding_indexed',
            'configuration': {
                'chunk_shape': chunk_shape,
                'codecs': [
                    {
                        'name': 'bytes',
                        'configuration': { 'endian': 'little' }
                    }
                ],
                'index_codecs': [
                    {
                        'name': 'bytes',
                        'configuration': {
                            'endian': 'little'
                        }
                    },
                    {'name': 'crc32c'},
                  ],
                  'index_location': 'end',
            },
        }
    ]
    
    if compression is None:
        pass
    elif compression == 'gzip' or compression == 'zstd':
        zarr3_store_spec['metadata']['codecs'][0]['configuration']['codecs'].append({
            "name": compression,
            "configuration": {
                "level": level
            }
        })
    elif compression == 'blosc':
        zarr3_store_spec['metadata']['codecs'][0]['configuration']['codecs'].append({
                "name": "blosc",
                "configuration": {
                    "cname": "blosclz",
                    "clevel": level,
                    "typesize": data.dtype.itemsize,
                    "shuffle": "bitshuffle"
                }
        })
    else:
        raise Exception("Unknown compression name: " + compression)

    zarr3_store = ts.open(zarr3_store_spec, create=True, delete_existing=True).result()
    zarr3_store.write(data).result()
            
    logger.debug("Zarr3 store has been updated.")
    return zarr3_store

# For Zarr3
def runZarr3Test(group_path, array_3x2_c, array_30x20_c, array_4x3x2):

    ts_create_zarr3_test(zarr3_path=os.path.join(group_path, '3x2_c_u1'), data=array_3x2_c.astype("|u1"), chunk_shape=(2, 3))
    ts_create_zarr3_test(zarr3_path=os.path.join(group_path, '3x2_c_i8'), data=array_3x2_c.astype("<i8"), chunk_shape=(2, 3))
    ts_create_zarr3_test(zarr3_path=os.path.join(group_path, '3x2_c_u4'), data=array_3x2_c.astype(">u4"), chunk_shape=(2, 3))
    ts_create_zarr3_test(zarr3_path=os.path.join(group_path, '3x2_c_f8'), data=array_3x2_c.astype("<f8"), chunk_shape=(2, 3))
    ts_create_zarr3_test(zarr3_path=os.path.join(group_path, '3x2_c_f4'), data=array_3x2_c.astype(">f4"), chunk_shape=(2, 3))

    ts_create_zarr3_test(zarr3_path=os.path.join(group_path, '30x20_c_i8'), data=array_30x20_c.astype("<i8"), chunk_shape=(7, 13))
    ts_create_zarr3_test(zarr3_path=os.path.join(group_path, '30x20_c_u4'), data=array_30x20_c.astype(">u4"), chunk_shape=(7, 13))
    ts_create_zarr3_test(zarr3_path=os.path.join(group_path, '30x20_c_f8'), data=array_30x20_c.astype("<f8"), chunk_shape=(7, 13))
    ts_create_zarr3_test(zarr3_path=os.path.join(group_path, '30x20_c_f4'), data=array_30x20_c.astype(">f4"), chunk_shape=(7, 13))
    ts_create_zarr3_test(zarr3_path=os.path.join(group_path, '30x20_c_u8_zlib'), data=array_30x20_c.astype(">u8"), chunk_shape=(7, 13), compression='zlib')
    ts_create_zarr3_test(zarr3_path=os.path.join(group_path, '30x20_c_u8_gzip'), data=array_30x20_c.astype(">u8"), chunk_shape=(7, 13), compression='gzip')
    ts_create_zarr3_test(zarr3_path=os.path.join(group_path, '30x20_c_u8_zstd'), data=array_30x20_c.astype(">u8"), chunk_shape=(7, 13), compression='zstd')

    ts_create_zarr3_test(zarr3_path=os.path.join(group_path, '3x2_c_u4_f1'), data=array_3x2_c.astype(">u4"), chunk_shape=(3, 2), fill_value=1)
    ts_create_zarr3_test(zarr3_path=os.path.join(group_path, '3x2_c_f4_fnan'), data=array_3x2_c.astype(">f4"), chunk_shape=(3, 2), fill_value=np.nan)
    
    # For Zarr3 with sharding
    ts_create_zarr3_sharded_test(zarr3_path=os.path.join(group_path, '3x2_c_u1_sharded'),data=array_3x2_c.astype("|u1"),shard_shape=(2, 1), chunk_shape=(1, 1))
    ts_create_zarr3_sharded_test(zarr3_path=os.path.join(group_path, '3x2_c_u1_sharded_gz'),data=array_3x2_c.astype("|u1"),shard_shape=(2, 1), chunk_shape=(1, 1))
    ts_create_zarr3_sharded_test(zarr3_path=os.path.join(group_path, '30x20_c_u1_sharded'),data=array_30x20_c.astype("|u1"),shard_shape=(6, 10), chunk_shape=(3, 5))

    ts_create_zarr3_sharded_test(zarr3_path=os.path.join(group_path, '3x2_c_u4_sharded'),data=array_3x2_c.astype("|u4"),shard_shape=(1, 2), chunk_shape=(1, 1))
    ts_create_zarr3_sharded_test(zarr3_path=os.path.join(group_path, '30x20_c_u4_sharded'),data=array_30x20_c.astype("|u4"),shard_shape=(6, 10), chunk_shape=(3, 5))

    ts_create_zarr3_sharded_test(zarr3_path=os.path.join(group_path, '3x2_c_i4_sharded'),data=array_3x2_c.astype("<i4"),shard_shape=(1, 2), chunk_shape=(1, 1))
    ts_create_zarr3_sharded_test(zarr3_path=os.path.join(group_path, '30x20_c_i4_sharded'),data=array_30x20_c.astype("<i4"),shard_shape=(6, 10), chunk_shape=(3, 5))

    ts_create_zarr3_sharded_test(zarr3_path=os.path.join(group_path, '3x2_c_i8_sharded'), data=array_3x2_c.astype("<i8"), shard_shape=(1, 2), chunk_shape=(1, 1))
    ts_create_zarr3_sharded_test(zarr3_path=os.path.join(group_path, '30x20_c_i8_sharded'), data=array_30x20_c.astype("<i8"), shard_shape=(6, 10), chunk_shape=(3, 5))

    ts_create_zarr3_sharded_test(zarr3_path=os.path.join(group_path, '30x20_huge-shard'), data=array_30x20_c.astype("<i8"), shard_shape=(40, 40), chunk_shape=(4, 4))


    # transpose
    ts_create_zarr3_test(zarr3_path=os.path.join(group_path, '3x2_c_u1_transpose'), data=array_3x2_c.astype("|u1"), chunk_shape=(2, 3), transpose_order=(1,0))
    ts_create_zarr3_test(zarr3_path=os.path.join(group_path, '3x2_c_u4_transpose'), data=array_3x2_c.astype("|u4"), chunk_shape=(2, 3), transpose_order=(1,0))
    ts_create_zarr3_test(zarr3_path=os.path.join(group_path, '3x2_c_i8_transpose'), data=array_3x2_c.astype("<i8"), chunk_shape=(2, 3), transpose_order=(1,0))
    ts_create_zarr3_test(zarr3_path=os.path.join(group_path, '3x2_c_f4_transpose'), data=array_3x2_c.astype("<f4"), chunk_shape=(2, 3), transpose_order=(1,0))
    ts_create_zarr3_test(zarr3_path=os.path.join(group_path, '3x2_c_f8_transpose'), data=array_3x2_c.astype("<f8"), chunk_shape=(2, 3), transpose_order=(1,0))

    # transpose3d
    for order in itertools.permutations((0,1,2)):
        a, b, c = order
        ts_create_zarr3_test(zarr3_path=os.path.join(group_path, f"4x3x2_c_u1_transpose_{a}-{b}-{c}"), data=array_4x3x2.astype("|u1"), chunk_shape=(2, 3, 4), transpose_order=order)


# For Zarr2
def runZarr2Test(group_path, array_3x2_c, array_30x20_c):

    ts_create_zarr2_test(zarr2_path=os.path.join(group_path, '3x2_c_u1'), data=array_3x2_c.astype("|u1"), chunk_shape=(2, 3))
    #ts_create_zarr2_test(zarr2_path=os.path.join(group_path, '3x2_f_u1'), data=array_3x2_f.astype("|u1"), chunk_shape=(2, 3))

    ts_create_zarr2_test(zarr2_path=os.path.join(group_path, '3x2_c_i8'), data=array_3x2_c.astype("<i8"), chunk_shape=(2, 3))
    ts_create_zarr2_test(zarr2_path=os.path.join(group_path, '3x2_f_i8'), data=array_3x2_c.astype("<i8"), chunk_shape=(2, 3), order='F')

    ts_create_zarr2_test(zarr2_path=os.path.join(group_path, '30x20_c_i8'), data=array_30x20_c.astype("<i8"), chunk_shape=(7, 13))
    #ts_create_zarr2_test(zarr2_path=os.path.join(group_path, '30x20_f_i8'), data=array_30x20_f.astype("<i8"), chunk_shape=(7, 13))

    ts_create_zarr2_test(zarr2_path=os.path.join(group_path, '3x2_c_u4'), data=array_3x2_c.astype(">u4"), chunk_shape=(2, 3))
    #ts_create_zarr2_test(zarr2_path=os.path.join(group_path, '3x2_f_u4'), data=array_3x2_f.astype(">u4"), chunk_shape=(2, 3))

    ts_create_zarr2_test(zarr2_path=os.path.join(group_path, '30x20_c_u4'), data=array_30x20_c.astype(">u4"), chunk_shape=(7, 13))
    #ts_create_zarr2_test(zarr2_path=os.path.join(group_path, '30x20_f_u4'), data=array_30x20_f.astype(">u4"), chunk_shape=(7, 13))

    ts_create_zarr2_test(zarr2_path=os.path.join(group_path, '3x2_c_f8'), data=array_3x2_c.astype("<f8"), chunk_shape=(2, 3))
    #ts_create_zarr2_test(zarr2_path=os.path.join(group_path, '3x2_f_f8'), data=array_3x2_f.astype("<f8"), chunk_shape=(2, 3))

    ts_create_zarr2_test(zarr2_path=os.path.join(group_path, '30x20_c_f8'), data=array_30x20_c.astype("<f8"), chunk_shape=(7, 13))
    #ts_create_zarr2_test(zarr2_path=os.path.join(group_path, '30x20_f_f8'), data=array_30x20_f.astype("<f8"), chunk_shape=(7, 13))

    ts_create_zarr2_test(zarr2_path=os.path.join(group_path, '3x2_c_f4'), data=array_3x2_c.astype(">f4"), chunk_shape=(2, 3))
    #ts_create_zarr2_test(zarr2_path=os.path.join(group_path, '3x2_f_f4'), data=array_3x2_f.astype(">f4"), chunk_shape=(2, 3))

    ts_create_zarr2_test(zarr2_path=os.path.join(group_path, '30x20_c_f4'), data=array_30x20_c.astype(">f4"), chunk_shape=(7, 13))
    #ts_create_zarr2_test(zarr2_path=os.path.join(group_path, '30x20_f_f4'), data=array_30x20_f.astype(">f4"), chunk_shape=(7, 13))

    ts_create_zarr2_test(zarr2_path=os.path.join(group_path, '30x20_c_u8_zlib'), data=array_30x20_c.astype(">u8"), chunk_shape=(7, 13), compression='zlib')
    ts_create_zarr2_test(zarr2_path=os.path.join(group_path, '30x20_c_u8_gzip'), data=array_30x20_c.astype(">u8"), chunk_shape=(7, 13), compression='gzip')
    ts_create_zarr2_test(zarr2_path=os.path.join(group_path, '30x20_c_u8_bz2'), data=array_30x20_c.astype(">u8"), chunk_shape=(7, 13), compression='bz2')
    ts_create_zarr2_test(zarr2_path=os.path.join(group_path, '30x20_c_u8_zstd'), data=array_30x20_c.astype(">u8"), chunk_shape=(7, 13), compression='zstd')
    ts_create_zarr2_test(zarr2_path=os.path.join(group_path, '30x20_c_u8_blosc'), data=array_30x20_c.astype(">u8"), chunk_shape=(7, 13), compression='blosc')

    ts_create_zarr2_test(zarr2_path=os.path.join(group_path, '3x2_c_u4_f1'), data=array_3x2_c.astype(">u4"), chunk_shape=(3, 2), fill_value=1)
    ts_create_zarr2_test(zarr2_path=os.path.join(group_path, '3x2_c_f4_fnan'), data=array_3x2_c.astype(">f4"), chunk_shape=(3, 2), fill_value=np.nan)

# For N5
def runN5Test(group_path, array_3x2_c, array_3x2_f, array_30x20_c, array_30x20_f):
    ts_create_n5_test(n5_path=os.path.join(group_path, '3x2_c_u1'), data=array_3x2_c.astype("|u1"), chunk_shape=(2, 3))
    #ts_create_n5_test(n5_path=os.path.join(group_path, '3x2_f_u1'), data=array_3x2_f.astype("|u1"), chunk_shape=(2, 3))

    ts_create_n5_test(n5_path=os.path.join(group_path, '3x2_c_i8'), data=array_3x2_c.astype("<i8"), chunk_shape=(2, 3))
    #ts_create_n5_test(n5_path=os.path.join(group_path, '3x2_f_i8'), data=array_3x2_f.astype("<i8"), chunk_shape=(2, 3))

    ts_create_n5_test(n5_path=os.path.join(group_path, '30x20_c_i8'), data=array_30x20_c.astype("<i8"), chunk_shape=(7, 13))
    #ts_create_n5_test(n5_path=os.path.join(group_path, '30x20_f_i8'), data=array_30x20_f.astype("<i8"), chunk_shape=(7, 13))

    ts_create_n5_test(n5_path=os.path.join(group_path, '3x2_c_u4'), data=array_3x2_c.astype(">u4"), chunk_shape=(2, 3))
    #ts_create_n5_test(n5_path=os.path.join(group_path, '3x2_f_u4'), data=array_3x2_f.astype(">u4"), chunk_shape=(2, 3))

    ts_create_n5_test(n5_path=os.path.join(group_path, '30x20_c_u4'), data=array_30x20_c.astype(">u4"), chunk_shape=(7, 13))
    #ts_create_n5_test(n5_path=os.path.join(group_path, '30x20_f_u4'), data=array_30x20_f.astype(">u4"), chunk_shape=(7, 13))

    ts_create_n5_test(n5_path=os.path.join(group_path, '3x2_c_f8'), data=array_3x2_c.astype("<f8"), chunk_shape=(2, 3))
    #ts_create_n5_test(n5_path=os.path.join(group_path, '3x2_f_f8'), data=array_3x2_f.astype("<f8"), chunk_shape=(2, 3))

    ts_create_n5_test(n5_path=os.path.join(group_path, '30x20_c_f8'), data=array_30x20_c.astype("<f8"), chunk_shape=(7, 13))
    #ts_create_n5_test(n5_path=os.path.join(group_path, '30x20_f_f8'), data=array_30x20_f.astype("<f8"), chunk_shape=(7, 13))

    ts_create_n5_test(n5_path=os.path.join(group_path, '3x2_c_f4'), data=array_3x2_c.astype(">f4"), chunk_shape=(2, 3))
    #ts_create_n5_test(n5_path=os.path.join(group_path, '3x2_f_f4'), data=array_3x2_f.astype(">f4"), chunk_shape=(2, 3))

    ts_create_n5_test(n5_path=os.path.join(group_path, '30x20_c_f4'), data=array_30x20_c.astype(">f4"), chunk_shape=(7, 13))
    #ts_create_n5_test(n5_path=os.path.join(group_path, '30x20_f_f4'), data=array_30x20_f.astype(">f4"), chunk_shape=(7, 13))

    ts_create_n5_test(n5_path=os.path.join(group_path, '30x20_c_u8_zlib'), data=array_30x20_c.astype(">u8"), chunk_shape=(7, 13), compression='zlib')
    ts_create_n5_test(n5_path=os.path.join(group_path, '30x20_c_u8_gzip'), data=array_30x20_c.astype(">u8"), chunk_shape=(7, 13), compression='gzip')
    ts_create_n5_test(n5_path=os.path.join(group_path, '30x20_c_u8_blosc'), data=array_30x20_c.astype(">u8"), chunk_shape=(7, 13), compression='blosc')
    ts_create_n5_test(n5_path=os.path.join(group_path, '30x20_c_u8_bzip2'), data=array_30x20_c.astype(">u8"), chunk_shape=(7, 13), compression='bzip2')
    ts_create_n5_test(n5_path=os.path.join(group_path, '30x20_c_u8_raw'), data=array_30x20_c.astype(">u8"), chunk_shape=(7, 13), compression='raw')
    ts_create_n5_test(n5_path=os.path.join(group_path, '30x20_c_u8_xz'), data=array_30x20_c.astype(">u8"), chunk_shape=(7, 13), compression='xz')

def main(test_path: str | None = None, *args) -> int:

    if test_path is None:
        test_path = sys.argv[1]

    # Create test path
    if test_path == "test":
        test_path = tempfile.mkdtemp('.zarr', 'zarr3-tensorstore-test_python_')
    logger.info("Test_path: " + test_path)

    tensorstore_tests_path = os.path.join(test_path, 'tensorstore_tests')
    os.makedirs(tensorstore_tests_path, exist_ok=True)

    # Determine whether to use N5 or Zarr2 or Zarr3
    args = [arg.lower() for arg in args]
    valid_options = ['--zarr3', '--zarr', '--n5', '--info']
    use_zarr3 = '--zarr3' in args
    use_n5 = '--n5' in args

    if any(arg.startswith('--') and arg not in valid_options for arg in args):
        raise Exception("Invalid option provided. Valid options are '--zarr3' or '--zarr' or '--n5' or '--info'.")

    format = 3 if use_zarr3 else 2 if not use_n5 else 'n5'

    # Create the Zarr/N5 store using tensorstore
    if format == 3:  
        group_path = os.path.join(tensorstore_tests_path, 'zarr3')
    elif format == 2:
        group_path = os.path.join(tensorstore_tests_path, 'zarr')
        os.makedirs(group_path, exist_ok=True)
        zarr2 = open(os.path.join(group_path, '.zgroup'), 'w')
        zarr2.write('''{
            "zarr_format": 2
            }''')
        zarr2.close()
    elif format == 'n5':
        group_path = os.path.join(tensorstore_tests_path, 'n5')

    os.makedirs(group_path, exist_ok=True)
    logger.info("Group_path: " + group_path)

    # Data creation
    array_3x2_c = np.arange(0, 3 * 2).reshape(2, 3)
    array_30x20_c = np.arange(0, 30 * 20).reshape(20, 30)

    array_4x3x2 = np.arange(0, 4 * 3 * 2).reshape(2, 3, 4)

    if format == 3:
        runZarr3Test(group_path, array_3x2_c, array_30x20_c, array_4x3x2)
    elif format == 2:
        runZarr2Test(group_path, array_3x2_c, array_30x20_c) 
    elif format == 'n5':
        runN5Test(group_path, array_3x2_c, array_30x20_c)

    return 0


if __name__ == '__main__':
    logging.basicConfig(level = logging.INFO)

    if "--info" in sys.argv:
        logger.setLevel(logging.INFO)
    elif "--debug" in sys.argv:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.WARN)

    try:
        status = main(*sys.argv[1:])
        print("tensorstore_test.py completed.")
        sys.exit(status)
    except Exception as e:
        logger.error("tensorstore_test.py failed with the following exception!", exc_info=True)
        sys.exit(3)
