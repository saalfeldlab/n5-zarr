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
import zarr
from numcodecs import Zlib, GZip, BZ2

test_path = Path.home() / 'tmp' / 'zarr-test.zarr'
group_path = 'test/data'

store = zarr.DirectoryStore(str(test_path))
root = zarr.group(store=store, overwrite=True)
group = root.create_group(group_path)

array_3x2_c = np.arange(0,3*2).reshape(2,3)
array_3x2_f = np.asfortranarray(array_3x2_c)

array_30x20_c = np.arange(0,30*20).reshape(20,30)
array_30x20_f = np.asfortranarray(array_30x20_c)

group.array(
  name='3x2_c_|u1',
  dtype='|u1',
  data=array_3x2_c,
  chunks=(2, 3),
  overwrite=True)
group.array(
  name='3x2_f_|u1',
  dtype='|u1',
  data=array_3x2_f,
  chunks=(2, 3),
  order='F',
  overwrite=True)

group.array(
  name='3x2_c_<i8',
  data=array_3x2_c,
  chunks=(2, 3),
  overwrite=True)
group.array(
  name='3x2_f_<i8',
  data=array_3x2_f,
  chunks=(2, 3),
  order='F',
  overwrite=True)
group.array(
  name='30x20_c_<i8',
  data=array_30x20_c,
  chunks=(7, 13),
  overwrite=True)
group.array(
  name='30x20_f_<i8',
  data=array_30x20_f,
  chunks=(7, 13),
  order='F',
  overwrite=True)

group.array(
  name='3x2_c_>u4',
  dtype='>u4',
  data=array_3x2_c,
  chunks=(2, 3),
  overwrite=True)
group.array(
  name='3x2_f_>u4',
  dtype='>u4',
  data=array_3x2_f,
  chunks=(2, 3),
  order='F',
  overwrite=True)
group.array(
  name='30x20_c_>u4',
  dtype='>u4',
  data=array_30x20_c,
  chunks=(7, 13),
  overwrite=True)
group.array(
  name='30x20_f_>u4',
  dtype='>u4',
  data=array_30x20_f,
  chunks=(7, 13),
  order='F',
  overwrite=True)

group.array(
  name='3x2_c_<f8',
  dtype='<f8',
  data=array_3x2_c,
  chunks=(2, 3),
  overwrite=True)
group.array(
  name='3x2_f_<f8',
  dtype='<f8',
  data=array_3x2_f,
  chunks=(2, 3),
  order='F',
  overwrite=True)
group.array(
  name='30x20_c_<f8',
  dtype='<f8',
  data=array_30x20_c,
  chunks=(7, 13),
  overwrite=True)
group.array(
  name='30x20_f_<f8',
  dtype='<f8',
  data=array_30x20_f,
  chunks=(7, 13),
  order='F',
  overwrite=True)

group.array(
  name='3x2_c_>f4',
  dtype='>f4',
  data=array_3x2_c,
  chunks=(2, 3),
  overwrite=True)
group.array(
  name='3x2_f_>f4',
  dtype='>f4',
  data=array_3x2_f,
  chunks=(2, 3),
  order='F',
  overwrite=True)
group.array(
  name='30x20_c_>f4',
  dtype='>f4',
  data=array_30x20_c,
  chunks=(7, 13),
  overwrite=True)
group.array(
  name='30x20_f_>f4',
  dtype='>f4',
  data=array_30x20_f,
  chunks=(7, 13),
  order='F',
  overwrite=True)

group.array(
  name='30x20_c_>u8_zlib',
  dtype='>u8',
  compressor=Zlib(level=6),
  data=array_30x20_c,
  chunks=(7, 13),
  overwrite=True)
group.array(
  name='30x20_c_>u8_gzip',
  dtype='>u8',
  compressor=GZip(level=6),
  data=array_30x20_c,
  chunks=(7, 13),
  overwrite=True)
group.array(
  name='30x20_c_>u8_bz2',
  dtype='>u8',
  compressor=BZ2(level=1),
  data=array_30x20_c,
  chunks=(7, 13),
  overwrite=True)

group.array(
  name='3x2_c_>u4_f1',
  dtype='>u4',
  data=array_3x2_c,
  chunks=(3, 2),
  fill_value="1",
  overwrite=True)
group.array(
  name='3x2_c_<f4_fnan',
  dtype='<f4',
  data=array_3x2_c,
  chunks=(3, 2),
  fill_value="NaN",
  overwrite=True)

