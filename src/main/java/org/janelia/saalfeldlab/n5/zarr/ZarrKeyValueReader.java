/**
 * Copyright (c) 2017--2021, Stephan Saalfeld
 * All rights reserved.
 *
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
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package org.janelia.saalfeldlab.n5.zarr;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.janelia.saalfeldlab.n5.BlockReader;
import org.janelia.saalfeldlab.n5.ByteArrayDataBlock;
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.CompressionAdapter;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.GsonN5Reader;
import org.janelia.saalfeldlab.n5.KeyValueAccess;
import org.janelia.saalfeldlab.n5.LockedChannel;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5URL;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;

/**
 * {@link N5Reader} implementation through {@link KeyValueAccess} with JSON
 * attributes parsed with {@link Gson}.
 *
 * @author Stephan Saalfeld
 * @author John Bogovic
 */
public class ZarrKeyValueReader implements GsonN5Reader {

	/**
	 * Data object for caching meta data.  Elements that are null are not yet
	 * cached.
	 */
	protected static class N5GroupInfo {

		public HashSet<String> children = null;
		public JsonElement attributesCache = null;
		public Boolean isDataset = null;
	}

	protected static final N5GroupInfo emptyGroupInfo = new N5GroupInfo();

	protected final KeyValueAccess keyValueAccess;

	protected final Gson gson;

	protected final HashMap<String, N5GroupInfo> metaCache = new HashMap<>();

	protected final boolean cacheMeta;

	protected static final String zarrayFile = ".zarray";
	protected static final String zattrsFile = ".zattrs";
	protected static final String zgroupFile = ".zgroup";

	protected final String basePath;

	/**
	 * Opens an {@link ZarrKeyValueReader} at a given base path with a custom
	 * {@link GsonBuilder} to support custom attributes.
	 *
	 * @param keyValueAccess
	 * @param basePath N5 base path
	 * @param gsonBuilder
	 * @param cacheMeta cache attributes and meta data
	 *    Setting this to true avoids frequent reading and parsing of JSON
	 *    encoded attributes and other meta data that requires accessing the
	 *    store. This is most interesting for high latency backends. Changes
	 *    of cached attributes and meta data by an independent writer will
	 *    not be tracked.
	 *
	 * @throws IOException
	 *    if the base path cannot be read or does not exist,
	 *    if the N5 version of the container is not compatible with this
	 *    implementation.
	 */
	public ZarrKeyValueReader(
			final KeyValueAccess keyValueAccess,
			final String basePath,
			final GsonBuilder gsonBuilder,
			final boolean cacheMeta) throws IOException {

		this.keyValueAccess = keyValueAccess;
		this.basePath = keyValueAccess.normalize(basePath);
		gsonBuilder.registerTypeAdapter(DataType.class, new DataType.JsonAdapter());
		gsonBuilder.registerTypeHierarchyAdapter(Compression.class, CompressionAdapter.getJsonAdapter());
		gsonBuilder.disableHtmlEscaping();
		this.gson = gsonBuilder.create();
		this.cacheMeta = cacheMeta;
		if (exists("/")) {
			final Version version = getVersion();
			if (!VERSION.isCompatible(version))
				throw new IOException("Incompatible version " + version + " (this is " + VERSION + ").");
		}
	}

	@Override public Gson getGson() {

		return gson;
	}

	/**
	 *
	 * @return N5 base path
	 */
	public String getBasePath() {

		return this.basePath;
	}


	/**
	 * Parses an attribute from the given attributes map.
	 *
	 * @param map
	 * @param key
	 * @param clazz
	 * @return
	 * @throws IOException
	 */
	protected <T> T parseAttribute(
			final HashMap<String, JsonElement> map,
			final String key,
			final Class<T> clazz) throws IOException {

		final JsonElement attribute = map.get(key);
		if (attribute != null)
			return gson.fromJson(attribute, clazz);
		else
			return null;
	}

	/**
	 * Parses an attribute from the given attributes map.
	 *
	 * @param map
	 * @param key
	 * @param type
	 * @return
	 * @throws IOException
	 */
	protected <T> T parseAttribute(
			final HashMap<String, JsonElement> map,
			final String key,
			final Type type) throws IOException {

		final JsonElement attribute = map.get(key);
		if (attribute != null)
			return gson.fromJson(attribute, type);
		else
			return null;
	}

	/**
	 * Reads the attributes map from a given {@link Reader}.
	 *
	 * @param reader
	 * @return
	 * @throws IOException
	 */
	protected JsonElement readAttributes(final Reader reader) throws IOException {

		return GsonN5Reader.readAttributes(reader, gson);
	}

	@Override
	public ZarrDatasetAttributes getDatasetAttributes(final String pathName) throws IOException {

		return getZArraryAttributes(pathName).getDatasetAttributes();
	}

	public ZArrayAttributes getZArraryAttributes(final String pathName) throws IOException {

		final String normPath = normalize(pathName);
		final String zarrayPath = zArrayPath(normPath);
		final JsonElement elem = getAttributesAbsolute(zarrayPath);
		final JsonObject attributes;
		if ( elem.isJsonObject() )
			attributes = elem.getAsJsonObject();
		else
			return null;

		final JsonElement sepElem = attributes.get("dimension_separator");
		return new ZArrayAttributes(
				attributes.get("zarr_format").getAsInt(),
				gson.fromJson(attributes.get("shape"), long[].class),
				gson.fromJson(attributes.get("chunks"), int[].class),
				gson.fromJson(attributes.get("dtype"), DType.class),
				gson.fromJson(attributes.get("compressor"), ZarrCompressor.class),
				attributes.get("fill_value").getAsString(),
				attributes.get("order").getAsString().charAt(0),
				sepElem != null ? sepElem.getAsString() : ".",
				gson.fromJson(attributes.get("filters"), TypeToken.getParameterized(Collection.class, Filter.class).getType()));
	}

	/**
	 * Get and cache attributes for a group identified by an info object and a
	 * pathName.
	 *
	 * This helper method does not intelligently handle the case that the group
	 * does not exist (as indicated by info == emptyGroupInfo) which should be
	 * done in calling code.
	 *
	 * @param info
	 * @param pathName normalized group path without leading slash
	 * @return cached attributes
	 * 		empty map if the group exists but not attributes are set
	 * 		null if the group does not exist
	 * @throws IOException
	 */
	protected JsonElement getCachedAttributes(
			final N5GroupInfo info,
			final String normalPath) throws IOException {

		JsonElement metadataCache = info.attributesCache;
		if (metadataCache == null) {
			synchronized (info) {
				metadataCache = info.attributesCache;
				if (metadataCache == null) {
					metadataCache = getAttributes(normalPath);
					info.attributesCache = metadataCache;
				}
			}
		}
		return metadataCache;
	}

	@Override
	public <T> T getAttribute(
			final String pathName,
			final String key,
			final Class<T> clazz) throws IOException {

		final String normalPathName = N5URL.normalizePath(pathName);
		if (cacheMeta) {
			final N5GroupInfo info = getCachedN5GroupInfo(normalPathName);
			if (info == emptyGroupInfo)
				return null;
			final JsonElement metadataCache = getCachedAttributes(info, normalPathName);
			if (metadataCache == null)
				return null;
			return GsonN5Reader.readAttribute(metadataCache, N5URL.normalizeAttributePath(key), clazz, gson);
		} else {
			return GsonN5Reader.readAttribute(getAttributes(normalPathName), N5URL.normalizeAttributePath(key), clazz, gson);
		}
	}

	@Override
	public <T> T getAttribute(
			final String pathName,
			final String key,
			final Type type) throws IOException {

		final String normalPathName = N5URL.normalizePath(pathName);
		if (cacheMeta) {
			final N5GroupInfo info = getCachedN5GroupInfo(normalPathName);
			if (info == emptyGroupInfo)
				return null;
			final JsonElement metadataCache = getCachedAttributes(info, normalPathName);
			if (metadataCache == null)
				return null;
			return GsonN5Reader.readAttribute(metadataCache, N5URL.normalizeAttributePath(key), type, gson);
		} else {
			return GsonN5Reader.readAttribute(getAttributes(normalPathName), N5URL.normalizeAttributePath(key), type, gson);
		}
	}

	protected boolean groupExists(final String absoluteNormalPath) {

		return keyValueAccess.exists(absoluteNormalPath) && keyValueAccess.isDirectory(absoluteNormalPath);
	}

	/**
	 * Get an existing cached N5 group info object or create it.
	 *
	 * @param normalPathName normalized group path without leading slash
	 * @return
	 */
	protected N5GroupInfo getCachedN5GroupInfo(final String normalPathName) {

		N5GroupInfo info = metaCache.get(normalPathName);
		if (info == null) {

			/* I do not have a better solution yet to allow parallel
			 * exists checks for independent paths than to accept the
			 * same exists check to potentially run multiple times.
			 */
			final boolean exists = groupExists(groupPath(normalPathName));

			synchronized (metaCache) {
				info = metaCache.get(normalPathName);
				if (info == null) {
					info = exists ? new N5GroupInfo() : emptyGroupInfo;
					metaCache.put(normalPathName, info);
				}
			}
		}
		return info;
	}

	@Override
	public boolean exists(final String pathName) {

		final String normalPathName = N5URL.normalizePath(pathName);
		if (cacheMeta)
			return getCachedN5GroupInfo(normalPathName) != emptyGroupInfo;
		else
			return groupExists(groupPath(normalPathName));
	}

	@Override
	public boolean datasetExists(final String pathName) throws IOException {

		if (cacheMeta) {
			final String normalPathName = N5URL.normalizePath(pathName);
			final N5GroupInfo info = getCachedN5GroupInfo(normalPathName);
			if (info == emptyGroupInfo)
				return false;
			if (info.isDataset == null) {
				synchronized (info) {
					if (info.isDataset == null ) {

					}
					else
						return info.isDataset;
				}
			} else
				return info.isDataset;
		}
		return exists(pathName) && getDatasetAttributes(pathName) != null;
	}

	/**
	 * Reads or creates the attributes map of a group or dataset.
	 *
	 * @param pathName group path
	 * @return
	 * @throws IOException
	 */
	@Override public JsonElement getAttributes(final String pathName) throws IOException {

		final String normPath = normalize(pathName);
		final String zgroupPath = zArrayPath(normPath);
		final String zattrPath = zAttrsPath(normPath);
		final String zarrayPath = zArrayPath(normPath);

		JsonElement output = null;
		output = combineIfPossible(output, getAttributesAbsolute(zgroupPath));
		output = combineIfPossible(output, getAttributesAbsolute(zattrPath));
		output = combineIfPossible(output, getAttributesAbsolute(zarrayPath));

		return output;
	}

	protected JsonElement combineIfPossible( final JsonElement base, final JsonElement add )
	{
		if( base == null )
			return add;
		else if( add == null )
			return base;

		if( base.isJsonObject() && add.isJsonObject() )
		{
			final JsonObject baseObj = base.getAsJsonObject();
			final JsonObject addObj = base.getAsJsonObject();
			for( String k : addObj.keySet())
				baseObj.add( k, addObj.get(k));
		}
		else if( base.isJsonArray() && add.isJsonArray() )
		{
			final JsonArray baseArr = base.getAsJsonArray();
			final JsonArray addArr = base.getAsJsonArray();
			for( int i = 0; i < addArr.size(); i++ )
				baseArr.add(addArr.get(i));
		}
		return base;
	}

	/**
	 * Reads or creates the attributes map of a group or dataset.
	 *
	 * @param absolutePath absolute path
	 * @return
	 * @throws IOException
	 */
	public JsonElement getAttributesAbsolute(final String absolutePath ) throws IOException {

		if (!keyValueAccess.exists(absolutePath))
			return null;

		try (final LockedChannel lockedChannel = keyValueAccess.lockForReading(absolutePath)) {
			return readAttributes(lockedChannel.newReader());
		}
	}

	@Override
	public DataBlock<?> readBlock(
			final String pathName,
			final DatasetAttributes datasetAttributes,
			final long... gridPosition) throws IOException {

		final ZarrDatasetAttributes zarrDatasetAttributes;
		if (datasetAttributes instanceof ZarrDatasetAttributes)
			zarrDatasetAttributes = (ZarrDatasetAttributes)datasetAttributes;
		else
			zarrDatasetAttributes = getDatasetAttributes( pathName );

		final String absolutePath = keyValueAccess.compose(basePath, pathName,
				getZarrDataBlockPath(gridPosition,
						zarrDatasetAttributes.getDimensionSeparator(),
						zarrDatasetAttributes.isRowMajor()));

		try (final LockedChannel lockedChannel = keyValueAccess.lockForReading(absolutePath)) {
			return readBlock(lockedChannel.newInputStream(), zarrDatasetAttributes, gridPosition);
		}
	}

	/**
	 * Reads a {@link DataBlock} from an {@link InputStream}.
	 *
	 * @param in
	 * @param datasetAttributes
	 * @param gridPosition
	 * @return
	 * @throws IOException
	 */
	@SuppressWarnings("incomplete-switch")
	public static DataBlock<?> readBlock(
			final InputStream in,
			final ZarrDatasetAttributes datasetAttributes,
			final long... gridPosition) throws IOException {

		final int[] blockSize = datasetAttributes.getBlockSize();
		final DType dType = datasetAttributes.getDType();

		final ByteArrayDataBlock byteBlock = dType.createByteBlock(blockSize, gridPosition);
		final BlockReader reader = datasetAttributes.getCompression().getReader();
		reader.read(byteBlock, in);

		switch (dType.getDataType()) {
		case UINT8:
		case INT8:
			return byteBlock;
		}

		/* else translate into target type */
		final DataBlock<?> dataBlock = dType.createDataBlock(blockSize, gridPosition);
		final ByteBuffer byteBuffer = byteBlock.toByteBuffer();
		byteBuffer.order(dType.getOrder());
		dataBlock.readData(byteBuffer);

		/* TODO I do not think that makes sense, F order should be opened transposed, the consumer can decide what to do with them? */
//		if (!datasetAttributes.isRowMajor()) {
//
//			final long[] blockDimensions = new long[blockSize.length];
//			Arrays.setAll(blockDimensions, d -> blockSize[d]);
//
//			switch (datasetAttributes.getDataType()) {
//			case INT8:
//			case UINT8: {
//					final byte[] dataBlockData = (byte[])dataBlock.getData();
//					final ArrayImg<ByteType, ByteArray> src = ArrayImgs.bytes(dataBlockData.clone(), blockDimensions);
//					final ArrayImg<ByteType, ByteArray> dst = ArrayImgs.bytes(dataBlockData.clone(), blockDimensions);
//					copyTransposed(src, dst);
//				}
//				break;
//			case INT16:
//			case UINT16: {
//					final short[] dataBlockData = (short[])dataBlock.getData();
//					final ArrayImg<ShortType, ShortArray> src = ArrayImgs.shorts(dataBlockData.clone(), blockDimensions);
//					final ArrayImg<ShortType, ShortArray> dst = ArrayImgs.shorts(dataBlockData.clone(), blockDimensions);
//					copyTransposed(src, dst);
//				}
//				break;
//			case INT32:
//			case UINT32: {
//					final int[] dataBlockData = (int[])dataBlock.getData();
//					final ArrayImg<IntType, IntArray> src = ArrayImgs.ints(dataBlockData.clone(), blockDimensions);
//					final ArrayImg<IntType, IntArray> dst = ArrayImgs.ints(dataBlockData.clone(), blockDimensions);
//					copyTransposed(src, dst);
//				}
//				break;
//			case INT64:
//			case UINT64: {
//					final long[] dataBlockData = (long[])dataBlock.getData();
//					final ArrayImg<LongType, LongArray> src = ArrayImgs.longs(dataBlockData.clone(), blockDimensions);
//					final ArrayImg<LongType, LongArray> dst = ArrayImgs.longs(dataBlockData.clone(), blockDimensions);
//					copyTransposed(src, dst);
//				}
//				break;
//			case FLOAT32: {
//					final float[] dataBlockData = (float[])dataBlock.getData();
//					final ArrayImg<FloatType, FloatArray> src = ArrayImgs.floats(dataBlockData.clone(), blockDimensions);
//					final ArrayImg<FloatType, FloatArray> dst = ArrayImgs.floats(dataBlockData.clone(), blockDimensions);
//					copyTransposed(src, dst);
//				}
//				break;
//			case FLOAT64: {
//					final double[] dataBlockData = (double[])dataBlock.getData();
//					final ArrayImg<DoubleType, DoubleArray> src = ArrayImgs.doubles(dataBlockData.clone(), blockDimensions);
//					final ArrayImg<DoubleType, DoubleArray> dst = ArrayImgs.doubles(dataBlockData.clone(), blockDimensions);
//					copyTransposed(src, dst);
//				}
//				break;
//			}
//		}

		return dataBlock;
	}

	/**
	 *
	 * @param normalPath normalized path name
	 * @return
	 * @throws IOException
	 */
	protected String[] normalList(final String normalPath) throws IOException {

		return keyValueAccess.listDirectories(groupPath(normalPath));
	}

	@Override
	public String[] list(final String pathName) throws IOException {

		if (cacheMeta) {
			final N5GroupInfo info = getCachedN5GroupInfo(N5URL.normalizePath(pathName));
			if (info == emptyGroupInfo)
				throw new IOException("Group '" + pathName +"' does not exist.");
			else {
				Set<String> children = info.children;
				final String[] list;
				if (children == null) {
					synchronized (info) {
						children = info.children;
						if (children == null) {
							list = normalList(N5URL.normalizePath(pathName));
							info.children = new HashSet<>(Arrays.asList(list));
						} else
							list = children.toArray(new String[children.size()]);
					}
				} else
					list = children.toArray(new String[children.size()]);

				return list;
			}
		} else {
			return normalList(N5URL.normalizePath(pathName));
		}
	}

	/**
	 * Constructs the path for a data block in a dataset at a given grid position.
	 *
	 * The returned path is
	 * <pre>
	 * $gridPosition[n]$dimensionSeparator$gridPosition[n-1]$dimensionSeparator[...]$dimensionSeparator$gridPosition[0]
	 * </pre>
	 *
	 * This is the file into which the data block will be stored.
	 *
	 * @param gridPosition
	 * @param dimensionSeparator
	 * @param isRowMajor
	 *
	 * @return
	 */
	protected static String getZarrDataBlockPath(
			final long[] gridPosition,
			final String dimensionSeparator,
			final boolean isRowMajor) {

		final StringBuilder pathStringBuilder = new StringBuilder();
		if (isRowMajor) {
			pathStringBuilder.append(gridPosition[gridPosition.length - 1]);
			for (int i = gridPosition.length - 2; i >= 0 ; --i) {
				pathStringBuilder.append(dimensionSeparator);
				pathStringBuilder.append(gridPosition[i]);
			}
		} else {
			pathStringBuilder.append(gridPosition[0]);
			for (int i = 1; i < gridPosition.length; ++i) {
				pathStringBuilder.append(dimensionSeparator);
				pathStringBuilder.append(gridPosition[i]);
			}
		}

		return pathStringBuilder.toString();
	}

	/**
	 * Constructs the absolute path (in terms of this store) for the group or
	 * dataset.
	 *
	 * @param normalPath normalized group path without leading slash
	 * @return
	 */
	protected String groupPath(final String normalPath) {

		return keyValueAccess.compose(basePath, normalPath);
	}

	/**
	 * Constructs the absolute path (in terms of this store) to a .zarray
	 *
	 *
	 * @param normalPath normalized group path without leading slash
	 * @return
	 */
	protected String zArrayPath(final String normalPath) {

		return keyValueAccess.compose(basePath, normalPath, zarrayFile);
	}

	/**
	 * Constructs the absolute path (in terms of this store) to a .zattrs
	 *
	 *
	 * @param normalPath normalized group path without leading slash
	 * @return
	 */
	protected String zAttrsPath(final String normalPath) {

		return keyValueAccess.compose(basePath, normalPath, zattrsFile);
	}

	/**
	 * Constructs the absolute path (in terms of this store) to a .zgroup
	 *
	 *
	 * @param normalPath normalized group path without leading slash
	 * @return
	 */
	protected String zGroupPath(final String normalPath) {

		return keyValueAccess.compose(basePath, normalPath, zgroupFile);
	}



	/**
	 * Removes the leading slash from a given path and returns the normalized
	 * path.  It ensures correctness on both Unix and Windows, otherwise
	 * {@code pathName} is treated as UNC path on Windows, and
	 * {@code Paths.get(pathName, ...)} fails with
	 * {@code InvalidPathException}.
	 *
	 * @param path
	 * @return
	 */
	protected String normalize(final String path) {

		return keyValueAccess.normalize(path.startsWith("/") || path.startsWith("\\") ? path.substring(1) : path);
	}

	@Override
	public String toString() {

		return String.format("%s[access=%s, basePath=%s]", getClass().getSimpleName(), keyValueAccess, basePath);
	}

}
