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

import sys
import argparse
import numpy as np
import tensorstore as ts

def read(path, driver='zarr3'):
    p={
        'driver': 'zarr3',
        'kvstore': {'driver':'file', 'path':path},
        'context': {'cache_pool': {'total_bytes_limit': 100_000_000}},
        'recheck_cached_data':'open'
    }
    return np.array(ts.open(p).result())

def validate(data, message):
    shp = data.shape
    correct = np.all(np.arange(np.prod(shp)).reshape(shp) == data)
    if not correct:
        print(f"fail: incorrect data at {message}")
        return 2
    else:
        print("success")
        return 0

parser = argparse.ArgumentParser(
        prog='tensorstore_read_test',
        description='Tests whether tensorstore can read zarr data')

parser.add_argument('-p', '--path')
parser.add_argument('-d', '--driver', default='zarr3')
parser.add_argument('--test', action='store_true')
args = parser.parse_args()

if args.test:
    print("tensorstore_read_test")
    sys.exit(0)

path = args.path
try: 
    data = read(path, args.driver)
except Exception as e:
    print(f"fail: error when reading at {path}")
    sys.exit(1)

sys.exit(validate(data, path))
