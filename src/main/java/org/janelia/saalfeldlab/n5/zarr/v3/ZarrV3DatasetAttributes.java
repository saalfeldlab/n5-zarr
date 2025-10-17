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
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import org.apache.commons.lang3.ArrayUtils;
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.RawCompression;
import org.janelia.saalfeldlab.n5.codec.BlockCodecInfo;
import org.janelia.saalfeldlab.n5.codec.CodecInfo;
import org.janelia.saalfeldlab.n5.codec.DataCodecInfo;
import org.janelia.saalfeldlab.n5.shard.ShardCodecInfo;
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

	public ZarrV3DatasetAttributes(
			final long[] shape,
			int[] blockSize,
			DataType dataType,
			final BlockCodecInfo blockCodecInfo,
			final DataCodecInfo... dataCodecInfos) {

		this(
			shape,
			defaultChunkAttributes(blockSize),
			ZarrV3DataType.fromDataType(dataType),
			"0", // default fill value
			defaultDimensionNames(shape.length),
			blockCodecInfo,
			dataCodecInfos);
	}

	public ZarrV3DatasetAttributes(
			final long[] shape,
			final ChunkAttributes chunkAttributes,
			final ZarrV3DataType dataType,
			final String fillValue,
			final String[] dimensionNames,
			final BlockCodecInfo blockCodecInfo,
			final DataCodecInfo... dataCodecInfos) {

		super(shape,
				inferChunkShape(chunkAttributes, blockCodecInfo),
				dataType.getDataType(),
				blockCodecInfo,
				toZarrV3(dataCodecInfos));
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
			final BlockCodecInfo blockCodecInfo,
			final DataCodecInfo... dataCodecInfos) {

		this(shape, new ChunkAttributes(new RegularChunkGrid(chunkShape), chunkKeyEncoding), dataType, fillValue, 
				dimensionNames, blockCodecInfo, dataCodecInfos );
	}

	public ZarrV3DatasetAttributes(
			final long[] shape,
			final int[] chunkShape,
			final ZarrV3DataType dataType,
			final String fillValue,
			final String[] dimensionNames,
			final String dimensionSeparator,
			final BlockCodecInfo blockCodecInfo,
			final DataCodecInfo... dataCodecInfos) {

		this(shape, chunkShape, dataType, fillValue, dimensionNames,
				new DefaultChunkKeyEncoding(dimensionSeparator), blockCodecInfo, dataCodecInfos);
	}

	protected static DataCodecInfo[] toZarrV3(DataCodecInfo[] dataCodecInfos) {

		final DataCodecInfo[] zarr3Infos = new DataCodecInfo[dataCodecInfos.length];
		for (int i = 0; i < dataCodecInfos.length; i++) {
			ZarrV3Compressor zarrCodec = ZarrV3Compressor.fromCompression(dataCodecInfos[i]);
			if (zarrCodec != null)
				zarr3Infos[i] = zarrCodec;
			else
				zarr3Infos[i] = dataCodecInfos[i];
		}
		return zarr3Infos;
	}

	protected static String[] defaultDimensionNames(int nd) {

		return IntStream.range(0, nd)
				.mapToObj(i -> "dim_" + i)
				.toArray(i -> new String[nd]);
	}

	protected static ChunkAttributes defaultChunkAttributes(int[] blockSize) {

		return new ChunkAttributes(new RegularChunkGrid(blockSize), new DefaultChunkKeyEncoding("/"));
	}

	protected static int[] deepestChunkShape(final BlockCodecInfo blockCodecInfo) {

		// this code somewhat duplicates code in ShardedDatasetAccess
		int[] blockSize = null;
		BlockCodecInfo tmpInfo = blockCodecInfo;
		while (tmpInfo instanceof ShardCodecInfo) {
			final ShardCodecInfo info = (ShardCodecInfo)tmpInfo;
			blockSize = info.getInnerBlockSize();
			tmpInfo = info.getInnerBlockCodecInfo();
		}
		return blockSize;
	}

	protected static int[] inferChunkShape(final BlockCodecInfo blockCodecInfo, Supplier<int[]> defaultSize) {

		return Optional.ofNullable(deepestChunkShape(blockCodecInfo))
				.orElseGet(defaultSize);
	}

	protected static int[] inferChunkShape(final ChunkAttributes chunkAttributes, final BlockCodecInfo blockCodecInfo) {

		return Optional.ofNullable(deepestChunkShape(blockCodecInfo))
				.orElse(chunkAttributes.getGrid().getShape());
	}

	public static ZarrV3DatasetAttributes from(final DatasetAttributes datasetAttributes,
			final String dimensionSeparator) {

		if (datasetAttributes instanceof ZarrV3DatasetAttributes)
			return (ZarrV3DatasetAttributes)datasetAttributes;

		final long[] shape = datasetAttributes.getDimensions().clone();

		final int[] chunkShape = inferChunkShape(datasetAttributes.getBlockCodecInfo(),
				() -> datasetAttributes.getBlockSize().clone());

		// TODO this may not be correct when sharding?
		final ChunkAttributes chunkAttrs = new ChunkAttributes(
				new RegularChunkGrid(chunkShape),
				new DefaultChunkKeyEncoding(dimensionSeparator));

		return new ZarrV3DatasetAttributes(shape, chunkAttrs,
				ZarrV3DataType.fromDataType(datasetAttributes.getDataType()), "0", null,
				datasetAttributes.getBlockCodecInfo(),
				datasetAttributes.getDataCodecInfos());
	}

	public String relativeBlockPath(long... gridPosition) {

		return chunkAttributes.getChunkPath(gridPosition);
	}

	@Deprecated
	public Compression getCompression() {

		return Arrays.stream(getDataCodecInfos())
				.filter(it -> it instanceof ZarrV3Compressor)
				.map(it -> ((ZarrV3Compressor)it).getCompression())
				.findFirst()
				.orElse(new RawCompression());
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

				final CodecInfo[] codecs = context.deserialize(obj.get(CODEC_KEY), CodecInfo[].class);
				final BlockCodecInfo blockCodec = getBlockCodecInfo(codecs);
				final DataCodecInfo[] dataCodecs = getDataCodecInfos(codecs);

				final String typestr = obj.get(DATA_TYPE_KEY).getAsString();
				final ZarrV3DataType dataType = ZarrV3DataType.valueOf(typestr.toLowerCase());

				final long[] shape = context.deserialize(obj.get(SHAPE_KEY), long[].class);
				ArrayUtils.reverse(shape); // c- to f-order

				final String[] dimensionNames = context.deserialize(obj.get(DIMENSION_NAMES_KEY), String[].class);
				ArrayUtils.reverse(dimensionNames); // c- to f-order

				final ChunkAttributes chunkAttributes = context.deserialize(obj, ChunkAttributes.class);
				return new ZarrV3DatasetAttributes(
						shape,
						chunkAttributes,
						dataType,
						obj.get(FILL_VALUE_KEY).getAsString(),
						dimensionNames,
						blockCodec,
						dataCodecs);

			} catch (final Exception e) {
				return null;
			}
		}

		@Override
		public JsonElement serialize(ZarrV3DatasetAttributes src, Type typeOfSrc, JsonSerializationContext context) {

			final JsonObject jsonObject = new JsonObject();
			jsonObject.addProperty(ZarrV3Node.NODE_TYPE_KEY, ZarrV3Node.NodeType.ARRAY.toString());
			jsonObject.addProperty(ZARR_FORMAT_KEY, src.getZarrFormat());

			final JsonElement shapeArray = context.serialize(src.getShape());
			jsonObject.add(SHAPE_KEY, reverseJsonArray(shapeArray));

			final JsonObject chunkAttrs = context.serialize(src.chunkAttributes).getAsJsonObject();
			chunkAttrs.entrySet().forEach(entry -> jsonObject.add(entry.getKey(), entry.getValue()));

			jsonObject.add(DATA_TYPE_KEY, context.serialize(src.getDType().toString()));

			jsonObject.add(FILL_VALUE_KEY, src.fillValue);

			final String[] dimNames = src.getDimensionNames();
			if( dimNames != null ) {
				final JsonElement dimNamesArray = context.serialize(src.getDimensionNames());
				jsonObject.add(DIMENSION_NAMES_KEY, reverseJsonArray(dimNamesArray));
			}

			jsonObject.add(CODECS_KEY, context.serialize(concatenateCodecs(src)));

			return jsonObject;
		}

		private BlockCodecInfo getBlockCodecInfo(CodecInfo[] codecs) {

			if (!(codecs[0] instanceof BlockCodecInfo))
				throw new N5Exception("First codec must be a BlockCodec, but was: " + codecs[0].getClass());

			return (BlockCodecInfo)codecs[0];
		}

		private DataCodecInfo[] getDataCodecInfos(CodecInfo[] codecs) {

			final DataCodecInfo[] dataCodecs = new DataCodecInfo[codecs.length - 1];
			for (int i = 1; i < codecs.length; i++) {
				if (!(codecs[i] instanceof DataCodecInfo))
					throw new N5Exception("Codec at position " + i + " must be a DataCodec, but was: " + codecs[i].getClass());

				dataCodecs[i - 1] = (DataCodecInfo)codecs[i];
			}
			return dataCodecs;
		}

		private CodecInfo[] concatenateCodecs( ZarrV3DatasetAttributes attributes) {
			final CodecInfo[] codecs = new CodecInfo[ attributes.getDataCodecInfos().length + 1 ];
			codecs[0] = attributes.getBlockCodecInfo();
			System.arraycopy(attributes.getDataCodecInfos(), 0, codecs, 1, attributes.getDataCodecInfos().length);
			return codecs;
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
