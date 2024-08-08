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
import java.util.HashMap;

import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.codec.Codec;
import org.janelia.saalfeldlab.n5.zarr.DType;
import org.janelia.saalfeldlab.n5.zarr.ZarrCompressor;
import org.janelia.saalfeldlab.n5.zarr.chunks.ChunkAttributes;
import org.janelia.saalfeldlab.n5.zarr.chunks.DefaultChunkKeyEncoding;
import org.janelia.saalfeldlab.n5.zarr.chunks.RegularChunkGrid;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
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
	public static final String CODECS_KEY = "codecs";

	public static final String[] requiredKeys = new String[]{
			ZARR_FORMAT_KEY, NODE_TYPE_KEY,
			SHAPE_KEY, DATA_TYPE_KEY, CHUNK_GRID_KEY, CHUNK_KEY_ENCODING_KEY,
			FILL_VALUE_KEY, CODECS_KEY,
	};

	protected final int zarrFormat;
	protected final long[] shape;
	protected final ChunkAttributes chunkAttributes; // only support regular chunk grids for now
	protected final DType dtype;
	protected final JsonElement fillValue;

	protected transient final byte[] fillBytes;

	public ZarrV3DatasetAttributes(
			final int zarrFormat,
			final long[] shape,
			final ChunkAttributes chunkAttributes,
			final DType dtype,
			final String fillValue,
			final Compression compression,
			final Codec[] codecs) {

		super(shape, chunkAttributes.getGrid().getShape(), dtype.getDataType(), compression,
				appendCompression(codecs, compression));
		this.zarrFormat = zarrFormat;
		this.shape = shape;
		this.chunkAttributes = chunkAttributes;
		this.dtype = dtype;
		this.fillValue = parseFillValue(fillValue, dtype.getDataType());
		this.fillBytes = dtype.createFillBytes(fillValue);
	}

	public ZarrV3DatasetAttributes(
			final int zarrFormat,
			final long[] shape,
			final ChunkAttributes chunkAttributes,
			final DType dtype,
			final String fillValue,
			final Codec[] codecs) {

		this(zarrFormat, shape, chunkAttributes, dtype, fillValue, null, codecs);
	}

	public ZarrV3DatasetAttributes(
			final int zarrFormat,
			final long[] shape,
			final int[] chunkShape,
			final DType dtype,
			final String fillValue,
			final DefaultChunkKeyEncoding chunkKeyEncoding,
			final Codec[] codecs) {

		this(zarrFormat, shape, new ChunkAttributes(new RegularChunkGrid(chunkShape), chunkKeyEncoding), dtype, fillValue, codecs);
	}

	public ZarrV3DatasetAttributes(
			final int zarrFormat,
			final long[] shape,
			final int[] chunkShape,
			final DType dtype,
			final String fillValue,
			final String dimensionSeparator,
			final Codec[] codecs) {

		this(zarrFormat, shape, chunkShape, dtype, fillValue,
				new DefaultChunkKeyEncoding(dimensionSeparator), codecs);
	}

	private static Codec[] appendCompression(Codec[] codecs, Compression compression) {

		if (compression == null)
			return codecs;
		else if (codecs == null)
			return new Codec[]{compression};

		final Codec[] out = new Codec[codecs.length + 1];
		System.arraycopy(codecs, 0, out, 0, codecs.length);
		out[codecs.length] = compression;
		return out;
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

	public DType getDType() {

		return dtype;
	}

	public int getZarrFormat() {

		return zarrFormat;
	}

	public String getFillValue() {

		return fillValue.getAsString();
	}

	public byte[] getFillBytes() {

		return fillBytes;
	}

	@Override
	public HashMap<String, Object> asMap() {

		final HashMap<String, Object> map = new HashMap<>();

		map.put(ZARR_FORMAT_KEY, zarrFormat);
		map.put(NodeType.key(), NodeType.ARRAY.toString());
		map.put(SHAPE_KEY, shape);
		map.put(DATA_TYPE_KEY, dtype.toString());
		map.put(CHUNK_GRID_KEY, getChunkAttributes().getGrid());
		map.put(CHUNK_KEY_ENCODING_KEY, getChunkAttributes().getKeyEncoding());

		map.put(FILL_VALUE_KEY, fillValue);
		map.put(CODECS_KEY, codecsToZarrCompressors(getCodecs()));

		return map;
	}

	private Codec[] codecsToZarrCompressors(Codec[] codecs) {

		final Codec[] out = new Codec[codecs.length];
		for (int i = 0; i < out.length; i++) {
			if (codecs[i] instanceof Compression)
				out[i] = ZarrCompressor.fromCompression((Compression)codecs[i]);
			else
				out[i] = codecs[i];
		}
		return out;
	}

	@Override
	public NodeType getType() {

		return NodeType.ARRAY;
	}

	public static JsonAdapter jsonAdapter = new JsonAdapter();

	public static class JsonAdapter implements JsonDeserializer<ZarrV3DatasetAttributes>, JsonSerializer<ZarrV3DatasetAttributes> {

		@Override
		public ZarrV3DatasetAttributes deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {

			final JsonObject obj = json.getAsJsonObject();
			try {

				final Codec[] codecs = context.deserialize(obj.get("codecs"), Codec[].class);

				// TODO make this work with codecs
				// final DType dType = new DType(typestr, codecs);
				final String typestr = obj.getAsJsonPrimitive("data_type").getAsString();
				final String parsedType = DType.getTypeStr(typestr);
				final DType dType;
				if (parsedType != null)
					dType = new DType(parsedType, null);
				else
					dType = new DType(typestr, null);


				final int zarrFormat = obj.get("zarr_format").getAsInt();
				final long[] shape = context.deserialize(obj.get("shape"), long[].class);
				final ChunkAttributes chunkAttributes = context.deserialize(obj, ChunkAttributes.class);

				return new ZarrV3DatasetAttributes(
						zarrFormat,
						shape,
						chunkAttributes,
						dType, // fix
						obj.get("fill_value").getAsString(),
						codecs);
			} catch (final Exception e) {
				return null;
			}
		}

		@Override
		public JsonElement serialize(ZarrV3DatasetAttributes src, Type typeOfSrc, JsonSerializationContext context) {

			final JsonObject jsonObject = new JsonObject();
			jsonObject.addProperty(ZARR_FORMAT_KEY, src.getZarrFormat());
			jsonObject.add(SHAPE_KEY, context.serialize(src.getShape()));

			final JsonObject chunkAttrs = context.serialize(src.chunkAttributes).getAsJsonObject();
			chunkAttrs.entrySet().forEach(entry -> jsonObject.add(entry.getKey(), entry.getValue()));

			jsonObject.add("data_type", context.serialize(src.getDType().toString()));

			jsonObject.addProperty(FILL_VALUE_KEY, src.getFillValue());
			jsonObject.add(CODECS_KEY, context.serialize(src.getCodecs()));

			return jsonObject;
		}
	}

}
