/*-
 * #%L
 * Not HDF5
 * %%
 * Copyright (C) 2019 - 2022 Stephan Saalfeld
 * %%
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDERS OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 * #L%
 */
package org.janelia.saalfeldlab.n5.zarr;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import org.janelia.saalfeldlab.n5.RawCompression;

import com.google.gson.JsonNull;


/**
 * @author Stephan Saalfeld &lt;saalfelds@janelia.hhmi.org&gt;
 *
 */
public class ZArrayAttributes {

	public static final String zarrFormatKey = "zarr_format";
	public static final String shapeKey = "shape";
	public static final String chunksKey = "chunks";
	public static final String dTypeKey = "dtype";
	public static final String compressorKey = "compressor";
	public static final String fillValueKey = "fill_value";
	public static final String orderKey = "order";
	public static final String filtersKey = "filters";
	public static final String dimensionSeparatorKey = "dimension_separator";

	public static final String[] requiredKeys = new String[]{
		zarrFormatKey, shapeKey, chunksKey, dTypeKey, compressorKey, fillValueKey, filtersKey
	};

	public static final String[] allKeys = new String[] { zarrFormatKey, shapeKey, chunksKey, dTypeKey, compressorKey,
			fillValueKey, filtersKey, orderKey, dimensionSeparatorKey
	};

	private final int zarr_format;
	private final long[] shape;
	private final int[] chunks;
	private final DType dtype;
	private final ZarrCompressor compressor;
	private final String fill_value;
	private final char order;
	private final String dimensionSeparator;
	private final List<Filter> filters = new ArrayList<>();

	public ZArrayAttributes(
			final int zarr_format,
			final long[] shape,
			final int[] chunks,
			final DType dtype,
			final ZarrCompressor compressor,
			final String fill_value,
			final char order,
			final String dimensionSeparator,
			final Collection<Filter> filters) {

		this.zarr_format = zarr_format;
		this.shape = shape;
		this.chunks = chunks;
		this.dtype = dtype;
		this.compressor = compressor == null ? new ZarrCompressor.Raw() : compressor;
		this.fill_value = fill_value;
		this.order = order;
		this.dimensionSeparator = dimensionSeparator;
		if (filters != null)
			this.filters.addAll(filters);
	}

	public ZArrayAttributes(
			final int zarr_format,
			final long[] shape,
			final int[] chunks,
			final DType dtype,
			final ZarrCompressor compressor,
			final String fill_value,
			final char order,
			final Collection<Filter> filters) {

		// empty dimensionSeparator so that the reader's separator is used
		this(zarr_format, shape, chunks, dtype, compressor, fill_value, order, "", filters);
	}

	public ZarrDatasetAttributes getDatasetAttributes() {

		final boolean isRowMajor = order == 'C';
		final long[] dimensions = shape.clone();
		final int[] blockSize = chunks.clone();

		if (isRowMajor) {
			ZarrUtils.reorder(dimensions);
			ZarrUtils.reorder(blockSize);
		}

		return new ZarrDatasetAttributes(
				dimensions,
				blockSize,
				dtype,
				compressor.getCompression(),
				isRowMajor,
				fill_value,
				dimensionSeparator);
	}

	public long[] getShape() {

		return shape;
	}

	public int getNumDimensions() {

		return shape.length;
	}

	public int[] getChunks() {

		return chunks;
	}

	public ZarrCompressor getCompressor() {

		return compressor;
	}

	public DType getDType() {

		return dtype;
	}

	public int getZarrFormat() {

		return zarr_format;
	}

	public char getOrder() {

		return order;
	}

	public String getDimensionSeparator() {
		return dimensionSeparator;
	}

	public String getFillValue() {

		return fill_value;
	}

	public HashMap<String, Object> asMap() {

		final HashMap<String, Object> map = new HashMap<>();

		map.put(zarrFormatKey, zarr_format);
		map.put(shapeKey, shape);
		map.put(chunksKey, chunks);
		map.put(dTypeKey, dtype.toString());
		map.put(fillValueKey, fill_value);
		map.put(orderKey, order);
		map.put(filtersKey, filters);
		map.put(dimensionSeparatorKey, dimensionSeparator);

		// compression key is required, need to write json null
		map.put(compressorKey, compressor instanceof RawCompression ? JsonNull.INSTANCE : compressor);

		return map;
	}

	public Collection<Filter> getFilters() {

		return filters;
	}
}
