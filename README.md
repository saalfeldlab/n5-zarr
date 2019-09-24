# n5-zarr [![Build Status](https://travis-ci.com/saalfeldlab/n5-zarr.svg?branch=master)](https://travis-ci.com/saalfeldlab/n5-zarr)
Zarr filesystem backend for N5 (experimental).

Depends on `libblosc1`, on Ubuntu:
```
sudo apt-get install -y libblosc1
```

Build and install:
```
mvn clean install
```

This library provides best effort compatibility with existing [Zarr v2](https://zarr.readthedocs.io/en/stable/spec/v2.html) data stored in filesystem containers.  So far, I have tested

* self-consistency, i.e. data writing and reading in this implementation works,
* reading of some example zarr containers written with Python (check the [examples](https://github.com/saalfeldlab/n5-zarr/blob/master/src/test/python/zarr-test.py))

This implementation currently supports the following Zarr features

<dl>
  <dt>Groups</dt>
  <dd>as described by the <a href="https://zarr.readthedocs.io/en/stable/spec/v2.html#hierarchies">Zarr v2 spec for hierarchies</a>.</dd>
  <dt>N-dimensional arrays in C and F order</dt>
  <dd>for efficiency, both array types are opened without changing the order of elements in memory, only the chunk order and shape are adjusted such that chunks form the correct transposed tensor.  When used via <a href="https://github.com/saalfeldlab/n5-imglib2/">n5-imglib2</a>, a <a href="https://javadoc.scijava.org/ImgLib2/net/imglib2/view/Views.html#permute-net.imglib2.RandomAccessibleInterval-int-int-">view with permuted axes</a> can be easily created.</dd>
  <dt>Arbitrary meta-data</dt>
  <dd>stored as JSON for both groups and datasets.</dd>
  <dt>Compression</dt>
  <dd>currently, only the most relevant compression schemes (Blosc, GZip, Zlib, and BZ2) are supported, we can add others later as necessary.</dd>
  <dt>Primitive types as little and big endian</dt>
  <dd>so far, I have tested unsigned and signed integers with 1, 2, 4 and 8 bytes, and floats with 4 and 8 bytes.  The behavior for other types is untested because I did not have meaningful examples.  Complex numbers should be mapped into the best matching primitive real type.  Other numpy data types such as strings, timedeltas, objects, dates, or others should come out as uncompressed bytes.</dd>
</dl>

[Fill value](https://zarr.readthedocs.io/en/stable/spec/v2.html#fill-value-encoding) behavior is not well tested because I had no good idea what the best solution is.  They are currently interpreted as numbers (if possible) including the special cases "NaN", "Infinity", and "-Infinity" for floating point numbers.  Some hickups expected for other data.

Filters are not currently supported because I feel ambiguous about them.  Please let me know of use cases that I am missing, it is not hard to add data filters of any kind.  I just cannot come up with a single thing that I would like to do here right now.  

Optionally, N5 dataset attributes ("dimensions", "blockSize", "compression", "dataType") can be virtually mapped such that N5-API based code that reads or writes them directly via general attribute access will see and modify the zarray (dataset) attributes.  Keep in mind that this will lead to name clashes if a Zarr dataset uses any of these attributes for other purposes, try switching the mapping off first.
