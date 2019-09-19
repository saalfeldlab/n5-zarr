# n5-zarr [![Build Status](https://travis-ci.com/saalfeldlab/n5-zarr.svg?branch=master)](https://travis-ci.com/saalfeldlab/n5-zarr)
Zarr filesystem backend for N5 (experimental WIP).

Currently has a SNAPSHOT dependency to experimental [n5-blosc](https://github.com/saalfeldlab/n5-blosc).  Build and install that first, then build:
```
mvn clean install
```

TODO This is a work in progress experiment to provide best effort compatibility with existing [Zarr v2](https://zarr.readthedocs.io/en/stable/spec/v2.html) data stored in filesystem containers.  So far, I have only tested self-consistency, i.e. data writing and reading in this implementation works but is not guaranteed to be compatible with the Zarr v2 specification.  Stay tuned...
