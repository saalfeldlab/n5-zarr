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
package org.janelia.saalfeldlab.n5.zarr.v3;

import java.lang.reflect.Type;
import java.util.Arrays;

import org.apache.commons.lang3.ArrayUtils;
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.RawCompression;
import org.janelia.saalfeldlab.n5.codec.Codec;
import org.janelia.saalfeldlab.n5.zarr.chunks.ChunkAttributes;
import org.janelia.saalfeldlab.n5.zarr.chunks.DefaultChunkKeyEncoding;
import org.janelia.saalfeldlab.n5.zarr.chunks.RegularChunkGrid;

import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

public class ZarrV3DatasetAttributes extends DatasetAttributes implements ZarrV3Node {

	private static final long serialVersionUID = 6926530453545002018L;

	public static final String SHAPE_KEY = "shape";
	public static final String DIMENSION_SEPARATOR_KEYS = "dimension_separator";
	public static final String FILL_VALUE_KEY = "fill_value";
	public static final String DATA_TYPE_KEY = "data_type";
	public static final String CHUNK_GRID_KEY = "chunk_grid";
	public static final String CHUNK_KEY_ENCODING_KEY = "chunk_key_encoding";
	public static final String DIMENSION_NAMES_KEY = "dimension_names";
	public static final String CODECS_KEY = "codecs";

	public static final int FORMAT = 3;

	public static final String[] REQUIRED_KEYS = new String[]{
			ZARR_FORMAT_KEY, NODE_TYPE_KEY,
			SHAPE_KEY, DATA_TYPE_KEY, CHUNK_GRID_KEY, CHUNK_KEY_ENCODING_KEY,
			FILL_VALUE_KEY, CODECS_KEY,
	};

	protected final long[] shape;
	protected final ChunkAttributes chunkAttributes; // only support regular chunk grids for now
	protected final ZarrV3DataType dataType;
	protected final JsonElement fillValue;
	protected final String[] dimensionNames;

	protected transient final byte[] fillBytes;

	protected static Codec[] removeRawCompression(final Codec[] codecs) {

		final Codec[] newCodecs = Arrays.stream(codecs).filter(it -> !(it instanceof RawCompression)).toArray(Codec[]::new);
		return newCodecs;
	}

	public ZarrV3DatasetAttributes(
			final long[] shape,
			final ChunkAttributes chunkAttributes,
			final ZarrV3DataType dataType,
			final String fillValue,
			final String[] dimensionNames,
			final Codec... codecs) {

		super(shape, chunkAttributes.getGrid().getShape(), dataType.getDataType(), removeRawCompression(codecs));
		this.shape = shape;
		this.chunkAttributes = chunkAttributes;
		this.dataType = dataType;
		this.fillValue = parseFillValue(fillValue, dataType.getDataType());
		this.dimensionNames = dimensionNames;
		this.fillBytes = dataType.createFillBytes(fillValue);
	}

	public ZarrV3DatasetAttributes(
			final long[] shape,
			final int[] chunkShape,
			final ZarrV3DataType dataType,
			final String fillValue,
			final String[] dimensionNames,
			final DefaultChunkKeyEncoding chunkKeyEncoding,
			final Codec[] codecs) {

		this(shape, new ChunkAttributes(new RegularChunkGrid(chunkShape), chunkKeyEncoding), dataType, fillValue, 
				dimensionNames, codecs);
	}

	public ZarrV3DatasetAttributes(
			final long[] shape,
			final int[] chunkShape,
			final ZarrV3DataType dataType,
			final String fillValue,
			final String[] dimensionNames,
			final String dimensionSeparator,
			final Codec[] codecs) {

		this(shape, chunkShape, dataType, fillValue, dimensionNames,
				new DefaultChunkKeyEncoding(dimensionSeparator), codecs);
	}

	protected static Codec[] prependArrayToBytes(Codec.ArrayCodec arrayToBytes, Codec[] codecs) {

		final Codec[] out = new Codec[codecs.length + 1];
		out[0] = arrayToBytes;
		System.arraycopy(codecs, 0, out, 1, codecs.length);
		return out;
	}

	protected static Compression inferCompression(Codec[] codecs) {

		final Codec lastCodec = codecs[codecs.length - 1];
		if (lastCodec instanceof Compression)
			return (Compression)lastCodec;
		else
			return new RawCompression();
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

	@Override
	public int getNumDimensions() {

		return shape.length;
	}

	public ChunkAttributes getChunkAttributes() {

		return chunkAttributes;
	}

	public ZarrV3DataType getDType() {

		return dataType;
	}

	public int getZarrFormat() {

		return FORMAT;
	}

	public String getFillValue() {

		return fillValue.getAsString();
	}

	public String[] getDimensionNames() {

		return dimensionNames;
	}

	public byte[] getFillBytes() {

		return fillBytes;
	}

	@Override
	public NodeType getType() {

		return NodeType.ARRAY;
	}

	public static JsonAdapter jsonAdapter = new JsonAdapter();

	public static class JsonAdapter implements JsonDeserializer<ZarrV3DatasetAttributes>, JsonSerializer<ZarrV3DatasetAttributes> {

		@Override
		public ZarrV3DatasetAttributes deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) {

			final JsonObject obj = json.getAsJsonObject();
			try {
				final int zarrFormat = obj.get(ZARR_FORMAT_KEY).getAsInt();
				if (zarrFormat != FORMAT)
					return null;

				final Codec[] codecs = context.deserialize(obj.get(CODEC_KEY), Codec[].class);

				// TODO make this work with codecs
				// final DType dType = new DType(typestr, codecs);
				final String typestr = obj.get(DATA_TYPE_KEY).getAsString();
				final ZarrV3DataType dataType = ZarrV3DataType.valueOf(typestr.toLowerCase());

				final long[] shape = context.deserialize(obj.get(SHAPE_KEY), long[].class);
				ArrayUtils.reverse(shape);

				final String[] dimensionNames = context.deserialize(obj.get(DIMENSION_NAMES_KEY), String[].class);
				ArrayUtils.reverse(dimensionNames);

				final ChunkAttributes chunkAttributes = context.deserialize(obj, ChunkAttributes.class);
				return new ZarrV3DatasetAttributes(
						shape,
						chunkAttributes,
						dataType,
						obj.get(FILL_VALUE_KEY).getAsString(),
						dimensionNames,
						codecs);

			} catch (final Exception e) {
				return null;
			}
		}

		@Override
		public JsonElement serialize(ZarrV3DatasetAttributes src, Type typeOfSrc, JsonSerializationContext context) {

			final JsonObject jsonObject = new JsonObject();
			jsonObject.addProperty(ZARR_FORMAT_KEY, src.getZarrFormat());

			final JsonElement shapeArray = context.serialize(src.getShape());
			reverseJsonArray(shapeArray);
			jsonObject.add(SHAPE_KEY, shapeArray);

			final JsonObject chunkAttrs = context.serialize(src.chunkAttributes).getAsJsonObject();
			chunkAttrs.entrySet().forEach(entry -> jsonObject.add(entry.getKey(), entry.getValue()));

			jsonObject.add(DATA_TYPE_KEY, context.serialize(src.getDType().toString()));

			jsonObject.add(FILL_VALUE_KEY, src.fillValue);

			final JsonElement dimNamesArray = context.serialize(src.getDimensionNames());
			jsonObject.add(DIMENSION_NAMES_KEY, reverseJsonArray(dimNamesArray));

			jsonObject.add(CODECS_KEY, context.serialize(src.getCodecs()));

			return jsonObject;
		}

		private static JsonArray reverseJsonArray(JsonElement paramJson) {

			final JsonArray reversedJson = new JsonArray(paramJson.getAsJsonArray().size());
			for (int i = paramJson.getAsJsonArray().size() - 1; i >= 0; i--) {
				reversedJson.add(paramJson.getAsJsonArray().get(i));
			}
			return reversedJson;
		}
	}

}
