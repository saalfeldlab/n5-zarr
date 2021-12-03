from pathlib import Path
import numpy as np
import zarr
from numcodecs import Zlib, GZip, BZ2

# Nested directory store
nested_test_path = Path.home() / 'tmp' / 'zarr-test-nested.zarr'
group_path = 'test/data'

nested_store = zarr.NestedDirectoryStore(str(nested_test_path))
nested_root = zarr.group(store=nested_store, overwrite=True)
nested_group = nested_root.create_group(group_path)

array_3x2_c = np.arange(0,3*2).reshape(2,3)

nested_group.array(
  name='3x2_c_|u1',
  dtype='|u1',
  data=array_3x2_c,
  chunks=(2, 3),
  overwrite=True)
