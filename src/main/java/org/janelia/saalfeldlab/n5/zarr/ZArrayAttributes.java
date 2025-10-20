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

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang3.ArrayUtils;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.RawCompression;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.google.gson.TypeAdapter;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;


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

	protected final int zarr_format;
	protected final long[] shape;
	protected final int[] chunks;
	protected final DType dtype;
	protected final ZarrCompressor compressor;
	protected final JsonElement fill_value;
	protected final char order;
	protected final String dimensionSeparator;
	protected final List<Filter> filters = new ArrayList<>();

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
		this.fill_value = parseFillValue(fill_value, dtype.getDataType());
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
			ArrayUtils.reverse(dimensions);
			ArrayUtils.reverse(blockSize);
		}

		return new ZarrDatasetAttributes(
				dimensions,
				blockSize,
				dtype,
				compressor.getCompression(),
				isRowMajor,
				fill_value.getAsString(),
				dimensionSeparator);
	}

	private static JsonElement parseFillValue(String fillValue, DataType dtype) {

		if (fillValue == null || fillValue.isEmpty())
			return JsonNull.INSTANCE;

		// Long is more than Double, so try that first
		try {
			return new JsonPrimitive(Long.parseLong(fillValue));
		} catch (final NumberFormatException ignore) {}

		return new JsonPrimitive(Double.parseDouble(fillValue));
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

		return fill_value.getAsString();
	}

	public HashMap<String, Object> asMap() {

		final HashMap<String, Object> map = new HashMap<>();

		map.put(zarrFormatKey, zarr_format);
		map.put(shapeKey, shape);
		map.put(chunksKey, chunks);
		map.put(dTypeKey, dtype.toString());
		map.put(fillValueKey, fill_value);
		map.put(orderKey, order);
		map.put(dimensionSeparatorKey, dimensionSeparator);

		if (filters.isEmpty())
			map.put(filtersKey, EMPTY_FILTERS);
		else
			map.put(filtersKey, filters);

		// compression key is required, need to write json null
		map.put(compressorKey, compressor instanceof RawCompression ? JsonNull.INSTANCE : compressor);

		return map;
	}

	public Collection<Filter> getFilters() {

		return filters;
	}

	public static JsonAdapter jsonAdapter = new JsonAdapter();

	public static class JsonAdapter implements JsonDeserializer<ZArrayAttributes>, JsonSerializer<ZArrayAttributes> {

		@Override
		public ZArrayAttributes deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {

			final JsonObject obj = json.getAsJsonObject();
			final JsonElement sepElem = obj.get("dimension_separator");
			try {

				final String typestr = context.deserialize(obj.get("dtype"), String.class);

				JsonElement filterArr = obj.get("filters");
				final Collection<Filter> filters;
				if (filterArr.isJsonNull())
					filters = Collections.EMPTY_LIST;
				else
					filters = context.deserialize(filterArr, TypeToken.getParameterized(Collection.class, Filter.class).getType());

				final DType dType = new DType(typestr, filters);

				return new ZArrayAttributes(
						obj.get("zarr_format").getAsInt(),
						context.deserialize( obj.get("shape"), long[].class),
						context.deserialize( obj.get("chunks"), int[].class),
						dType, // fix
						context.deserialize( obj.get("compressor"), ZarrCompressor.class), // fix
						obj.get("fill_value").getAsString(),
						obj.get("order").getAsCharacter(),
						sepElem != null ? sepElem.getAsString() : ".",
						filters);
			} catch (final Exception e) {
				return null;
			}
		}

		@Override
		public JsonElement serialize(ZArrayAttributes src, Type typeOfSrc, JsonSerializationContext context) {

			final JsonObject jsonObject = new JsonObject();

			jsonObject.addProperty("zarr_format", src.getZarrFormat());
			jsonObject.add("shape", context.serialize(src.getShape()));
			jsonObject.add("chunks", context.serialize(src.getChunks()));

			jsonObject.add("dtype", context.serialize(src.getDType().toString()));
			jsonObject.add("compressor", context.serialize(src.getCompressor()));
			jsonObject.addProperty("fill_value", src.getFillValue());
			jsonObject.addProperty("order", src.getOrder());
			jsonObject.addProperty("dimension_separator", src.getDimensionSeparator());

			Collection<Filter> filters = src.getFilters();
			if (filters.isEmpty())
				jsonObject.add("filters", context.serialize(EMPTY_FILTERS));
			else
				jsonObject.add("filters", context.serialize(filters));

			return jsonObject;
		}
	}

	// dummy instance for serialization
	private static EmptyZarrFilters EMPTY_FILTERS = new EmptyZarrFilters();

	public static class EmptyZarrFilters { }

	public static TypeAdapter<EmptyZarrFilters> emptyFiltersAdapter = new TypeAdapter<EmptyZarrFilters>() {

		@Override public void write(JsonWriter out, EmptyZarrFilters value) throws IOException {
			final boolean serializeNull = out.getSerializeNulls();
			out.setSerializeNulls(true);
			out.nullValue();
			out.setSerializeNulls(serializeNull);
		}

		@Override public EmptyZarrFilters read(JsonReader in) {
			return EMPTY_FILTERS;
		}
	};
}
