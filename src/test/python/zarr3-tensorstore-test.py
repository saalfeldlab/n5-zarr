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
from numcodecs import Zlib, GZip, BZ2, Zstd
import sys
import os
import tensorstore as ts

test_path = sys.argv[1]
print("Test_path: " + test_path)
group_path = os.path.join('test', 'data')
print("Group_path: " + group_path)

sys.stderr.write(test_path)

# Function to create Zarr3 dataset with TensorStore
def ts_create_zarr3_test(zarr3_path, data=None, chunk_shape=None, shard_shape=None, compression=None, level=1, fill_value=None):
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
    
    print(f"Creating a new Zarr3 store at {zarr3_path}.")

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

    if fill_value is not None:
      zarr3_store_spec['metadata']['fill_value'] = fill_value

    zarr3_store_spec['metadata']['codecs'] = [
        {
            'name': 'bytes',
            'configuration': {
                'endian': 'little'
            }
        }
    ]

    if compression == "zlib":
        compression = "gzip"

    if compression is None:
        pass
    elif compression == 'gzip' or compression == 'zstd':
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
    else:
        raise Exception("Unknown compression name: " + compression)

    zarr3_store = ts.open(zarr3_store_spec, create=True, delete_existing=True).result()
    zarr3_store.write(data).result()
            
    print("Zarr3 store has been updated.")
    return zarr3_store

# Function to create sharded Zarr3 dataset with TensorStore
def ts_create_zarr3_sharded_test(zarr3_path, data=None, shard_shape=None, chunk_shape=None, compression='gzip', level=1, fill_value=None):
    """
    Function to create a sharded Zarr3 dataset using TensorStore.
    """
    print(f"Creating a new Zarr3 store at {zarr3_path}.")

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
            
    print("Zarr3 store has been updated.")
    return zarr3_store

# Determine whether to use Zarr or Zarr3
use_zarr3 = '--zarr3' in sys.argv

zarr_format = 3 if use_zarr3 else 2

# Create the Zarr store using zarr-python or Zarr3 store using tensorstore
if zarr_format == 3:
  group_path = test_path + '\\groupV3'
  os.makedirs(group_path)
elif zarr_format == 2:
    group_path = test_path + "\\groupV2"
    os.makedirs(group_path)
    zarr2 = open(group_path + "\\.zgroup", "x")
    zarr2.write('''{
        "zarr_format": 2
        }''')
    zarr2.close()

# Data creation
array_3x2_c = np.arange(0, 3 * 2).reshape(2, 3)
array_3x2_f = np.asfortranarray(array_3x2_c)
array_3x2_str_c = np.array(["", "a", "bc", "de", "fgh", ":-þ"]).reshape(2, 3)
array_3x2_str_f = np.asfortranarray(array_3x2_str_c)

array_30x20_c = np.arange(0, 30 * 20).reshape(20, 30)
array_30x20_f = np.asfortranarray(array_30x20_c)

if zarr_format == 3:
    # For Zarr3
    ts_create_zarr3_test(zarr3_path=os.path.join(group_path, '3x2_c_u1'), data=array_3x2_c.astype("|u1"), chunk_shape=(2, 3))
    ts_create_zarr3_test(zarr3_path=os.path.join(group_path, '3x2_f_u1'), data=array_3x2_f.astype("|u1"), chunk_shape=(2, 3))

    ts_create_zarr3_test(zarr3_path=os.path.join(group_path, '3x2_c_i8'), data=array_3x2_c.astype("<i8"), chunk_shape=(2, 3))
    ts_create_zarr3_test(zarr3_path=os.path.join(group_path, '3x2_f_i8'), data=array_3x2_f.astype("<i8"), chunk_shape=(2, 3))

    ts_create_zarr3_test(zarr3_path=os.path.join(group_path, '30x20_c_i8'), data=array_30x20_c.astype("<i8"), chunk_shape=(7, 13))
    ts_create_zarr3_test(zarr3_path=os.path.join(group_path, '30x20_f_i8'), data=array_30x20_f.astype("<i8"), chunk_shape=(7, 13))

    ts_create_zarr3_test(zarr3_path=os.path.join(group_path, '3x2_c_u4'), data=array_3x2_c.astype(">u4"), chunk_shape=(2, 3))
    ts_create_zarr3_test(zarr3_path=os.path.join(group_path, '3x2_f_u4'), data=array_3x2_f.astype(">u4"), chunk_shape=(2, 3))

    ts_create_zarr3_test(zarr3_path=os.path.join(group_path, '30x20_c_u4'), data=array_30x20_c.astype(">u4"), chunk_shape=(7, 13))
    ts_create_zarr3_test(zarr3_path=os.path.join(group_path, '30x20_f_u4'), data=array_30x20_f.astype(">u4"), chunk_shape=(7, 13))

    ts_create_zarr3_test(zarr3_path=os.path.join(group_path, '3x2_c_f8'), data=array_3x2_c.astype("<f8"), chunk_shape=(2, 3))
    ts_create_zarr3_test(zarr3_path=os.path.join(group_path, '3x2_f_f8'), data=array_3x2_f.astype("<f8"), chunk_shape=(2, 3))

    ts_create_zarr3_test(zarr3_path=os.path.join(group_path, '30x20_c_f8'), data=array_30x20_c.astype("<f8"), chunk_shape=(7, 13))
    ts_create_zarr3_test(zarr3_path=os.path.join(group_path, '30x20_f_f8'), data=array_30x20_f.astype("<f8"), chunk_shape=(7, 13))

    ts_create_zarr3_test(zarr3_path=os.path.join(group_path, '3x2_c_f4'), data=array_3x2_c.astype(">f4"), chunk_shape=(2, 3))
    ts_create_zarr3_test(zarr3_path=os.path.join(group_path, '3x2_f_f4'), data=array_3x2_f.astype(">f4"), chunk_shape=(2, 3))

    ts_create_zarr3_test(zarr3_path=os.path.join(group_path, '30x20_c_f4'), data=array_30x20_c.astype(">f4"), chunk_shape=(7, 13))
    ts_create_zarr3_test(zarr3_path=os.path.join(group_path, '30x20_f_f4'), data=array_30x20_f.astype(">f4"), chunk_shape=(7, 13))

    ts_create_zarr3_test(zarr3_path=os.path.join(group_path, '30x20_c_u8_zlib'), data=array_30x20_c.astype(">u8"), chunk_shape=(7, 13), compression='zlib')
    ts_create_zarr3_test(zarr3_path=os.path.join(group_path, '30x20_c_u8_gzip'), data=array_30x20_c.astype(">u8"), chunk_shape=(7, 13), compression='gzip')
   # ts_create_zarr3_test(zarr3_path=os.path.join(group_path, '30x20_c_u8_bz2'), data=array_30x20_c.astype(">u8"), chunk_shape=(7, 13))#, compression='bz2')
    ts_create_zarr3_test(zarr3_path=os.path.join(group_path, '30x20_c_u8_zstd'), data=array_30x20_c.astype(">u8"), chunk_shape=(7, 13), compression='zstd')

    ts_create_zarr3_test(zarr3_path=os.path.join(group_path, '3x2_c_u4_f1'), data=array_3x2_c.astype(">u4"), chunk_shape=(3, 2), fill_value=1)
    ts_create_zarr3_test(zarr3_path=os.path.join(group_path, '3x2_c_f4_fnan'), data=array_3x2_c.astype(">f4"), chunk_shape=(3, 2), fill_value=np.nan)

    #ts_create_zarr3_test(zarr3_path=os.path.join(group_path, '3x2_c_str'), data=array_3x2_str_c.astype(str), chunk_shape=(2, 3))
    #ts_create_zarr3_test(zarr3_path=os.path.join(group_path, '3x2_f_str'), data=array_3x2_str_f.astype(str), chunk_shape=(2, 3)) # order='F')
    #ts_create_zarr3_test(zarr3_path=os.path.join(group_path, '3x2_c_str_zlib'), data=array_3x2_str_c.astype(str), chunk_shape=(7, 13), compression='zlib')
    #ts_create_zarr3_test(zarr3_path=os.path.join(group_path, '3x2_c_str_gzip'), data=array_3x2_str_c.astype(str), chunk_shape=(7, 13), compression='gzip')
    #ts_create_zarr3_test(zarr3_path=os.path.join(group_path, '3x2_c_str_bz2'), data=array_3x2_str_c.astype(str), chunk_shape=(2, 2))#, compression='bz2')
    #ts_create_zarr3_test(zarr3_path=os.path.join(group_path, '3x2_c_str_zstd'), data=array_3x2_str_c.astype(str), chunk_shape=(2, 2), compression='zstd')

    # For Zarr3 with sharding
    ts_create_zarr3_sharded_test(zarr3_path=os.path.join(group_path, '3x2_c_u1_sharded'),data=array_3x2_c.astype("|u1"),shard_shape=(1, 2), chunk_shape=(1, 1))
    ts_create_zarr3_sharded_test(zarr3_path=os.path.join(group_path, '3x2_f_u1_sharded'),data=array_3x2_f.astype("|u1"),shard_shape=(1, 2), chunk_shape=(1, 1))

    ts_create_zarr3_sharded_test(zarr3_path=os.path.join(group_path, '30x20_c_i8_sharded'),data=array_30x20_c.astype("<i8"),shard_shape=(10, 10), chunk_shape=(5, 5))
    ts_create_zarr3_sharded_test(zarr3_path=os.path.join(group_path, '30x20_f_i8_sharded'),data=array_30x20_f.astype("<i8"),shard_shape=(10, 10), chunk_shape=(5, 5))

else:
    # For Zarr2
    pass

   
    


"""

group.array(
  name='3x2_c_u1',
  dtype='|u1',
  data=array_3x2_c,
  chunks=(2, 3),
  overwrite=True)
group.array(
  name='3x2_f_u1',
  dtype='|u1',
  data=array_3x2_f,
  chunks=(2, 3),
  order='F',
  overwrite=True)

group.array(
  name='3x2_c_i8',
  data=array_3x2_c,
  dtype='<i8',
  chunks=(2, 3),
  overwrite=True)
group.array(
  name='3x2_f_i8',
  data=array_3x2_f,
  dtype='<i8',
  chunks=(2, 3),
  order='F',
  overwrite=True)
group.array(
  name='30x20_c_i8',
  data=array_30x20_c,
  dtype='<i8',
  chunks=(7, 13),
  overwrite=True)
group.array(
  name='30x20_f_i8',
  data=array_30x20_f,
  dtype='<i8',
  chunks=(7, 13),
  order='F',
  overwrite=True)

group.array(
  name='3x2_c_u4',
  dtype='>u4',
  data=array_3x2_c,
  chunks=(2, 3),
  overwrite=True)
group.array(
  name='3x2_f_u4',
  dtype='>u4',
  data=array_3x2_f,
  chunks=(2, 3),
  order='F',
  overwrite=True)
group.array(
  name='30x20_c_u4',
  dtype='>u4',
  data=array_30x20_c,
  chunks=(7, 13),
  overwrite=True)
group.array(
  name='30x20_f_u4',
  dtype='>u4',
  data=array_30x20_f,
  chunks=(7, 13),
  order='F',
  overwrite=True)

group.array(
  name='3x2_c_f8',
  dtype='<f8',
  data=array_3x2_c,
  chunks=(2, 3),
  overwrite=True)
group.array(
  name='3x2_f_f8',
  dtype='<f8',
  data=array_3x2_f,
  chunks=(2, 3),
  order='F',
  overwrite=True)
group.array(
  name='30x20_c_f8',
  dtype='<f8',
  data=array_30x20_c,
  chunks=(7, 13),
  overwrite=True)
group.array(
  name='30x20_f_f8',
  dtype='<f8',
  data=array_30x20_f,
  chunks=(7, 13),
  order='F',
  overwrite=True)

group.array(
  name='3x2_c_f4',
  dtype='>f4',
  data=array_3x2_c,
  chunks=(2, 3),
  overwrite=True)
group.array(
  name='3x2_f_f4',
  dtype='>f4',
  data=array_3x2_f,
  chunks=(2, 3),
  order='F',
  overwrite=True)
group.array(
  name='30x20_c_f4',
  dtype='>f4',
  data=array_30x20_c,
  chunks=(7, 13),
  overwrite=True)
group.array(
  name='30x20_f_f4',
  dtype='>f4',
  data=array_30x20_f,
  chunks=(7, 13),
  order='F',
  overwrite=True)

group.array(
  name='30x20_c_u8_zlib',
  dtype='>u8',
  compressor=Zlib(level=6),
  data=array_30x20_c,
  chunks=(7, 13),
  overwrite=True)
group.array(
  name='30x20_c_u8_gzip',
  dtype='>u8',
  compressor=GZip(level=6),
  data=array_30x20_c,
  chunks=(7, 13),
  overwrite=True)
group.array(
  name='30x20_c_u8_bz2',
  dtype='>u8',
  compressor=BZ2(level=1),
  data=array_30x20_c,
  chunks=(7, 13),
  overwrite=True)
group.array(
  name='30x20_c_u8_zstd',
  dtype='>u8',
  compressor=Zstd(level=1),
  data=array_30x20_c,
  chunks=(7, 13),
  overwrite=True)

group.array(
  name='3x2_c_u4_f1',
  dtype='>u4',
  data=array_3x2_c,
  chunks=(3, 2),
  fill_value="1",
  overwrite=True)
group.array(
  name='3x2_c_f4_fnan',
  dtype='<f4',
  data=array_3x2_c,
  chunks=(3, 2),
  fill_value="NaN",
  overwrite=True)

group.array(
  name='3x2_c_str',
  dtype=str,
  data=array_3x2_str_c,
  chunks=(2, 3),
  overwrite=True)
group.array(
  name='3x2_f_str',
  dtype=str,
  data=array_3x2_str_f,
  chunks=(2, 3),
  order='F',
  overwrite=True)
group.array(
  name='3x2_c_str_zlib',
  dtype=str,
  compressor=Zlib(level=6),
  data=array_3x2_str_c,
  chunks=(7, 13),
  overwrite=True)
group.array(
  name='3x2_c_str_gzip',
  dtype=str,
  compressor=GZip(level=6),
  data=array_3x2_str_c,
  chunks=(7, 13),
  overwrite=True)
group.array(
  name='3x2_c_str_bz2',
  dtype=str,
  compressor=BZ2(level=1),
  data=array_3x2_str_c,
  chunks=(2, 2),
  overwrite=True)
group.array(
  name='3x2_c_str_zstd',
  dtype=str,
  compressor=Zstd(level=1),
  data=array_3x2_str_c,
  chunks=(2, 2),
  overwrite=True)
"""