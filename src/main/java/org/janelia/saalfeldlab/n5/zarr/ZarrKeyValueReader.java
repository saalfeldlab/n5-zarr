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
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Stream;

import org.janelia.saalfeldlab.n5.BlockReader;
import org.janelia.saalfeldlab.n5.ByteArrayDataBlock;
import org.janelia.saalfeldlab.n5.CachedGsonKeyValueReader;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.GsonUtils;
import org.janelia.saalfeldlab.n5.KeyValueAccess;
import org.janelia.saalfeldlab.n5.LockedChannel;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5URL;
import org.janelia.saalfeldlab.n5.N5Reader.Version;
import org.janelia.saalfeldlab.n5.cache.N5JsonCache;
import org.janelia.saalfeldlab.n5.cache.N5JsonCacheableContainer;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

/**
 * {@link N5Reader} implementation through {@link KeyValueAccess} with JSON
 * attributes parsed with {@link Gson}.
 *
 * @author Stephan Saalfeld
 * @author John Bogovic
 */
public class ZarrKeyValueReader implements CachedGsonKeyValueReader, N5JsonCacheableContainer {

	public static Version VERSION = new Version(2, 0, 0);
	public static final String ZARR_FORMAT_KEY = "zarr_format";

	public static final String zarrayFile = ".zarray";
	public static final String zattrsFile = ".zattrs";
	public static final String zgroupFile = ".zgroup";

	protected final KeyValueAccess keyValueAccess;

	protected final Gson gson;

	protected final N5JsonCache cache;

	protected final boolean cacheMeta;

	protected final String basePath;

	final protected boolean mapN5DatasetAttributes;

	final protected boolean mergeAttributes;

	/**
	 * Opens an {@link ZarrKeyValueReader} at a given base path with a custom
	 * {@link GsonBuilder} to support custom attributes.
	 *
	 * @param checkVersion perform version check
	 * @param keyValueAccess
	 * @param basePath N5 base path
	 * @param gsonBuilder the gson builder
	 * @param mapN5DatasetAttributes
	 * 	  If true, getAttributes and variants of getAttribute methods will
	 * 	  contain keys used by n5 datasets, and whose values are those for
	 *    their corresponding zarr fields. For example, if true, the key "dimensions"
	 *    (from n5) may be used to obtain the value of the key "shape" (from zarr).
	 * @param mergeAttributes
	 * 	  If true, fields from .zgroup, .zarray, and .zattrs will be merged
	 *    when calling getAttributes, and variants of getAttribute
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
			final boolean checkVersion,
			final KeyValueAccess keyValueAccess,
			final String basePath,
			final GsonBuilder gsonBuilder,
			final boolean mapN5DatasetAttributes,
			final boolean mergeAttributes,
			final boolean cacheMeta) throws IOException {

		this.keyValueAccess = keyValueAccess;
		this.basePath = keyValueAccess.normalize(basePath);
		this.gson = registerGson(gsonBuilder);
		this.cacheMeta = cacheMeta;
		this.mapN5DatasetAttributes = mapN5DatasetAttributes;
		this.mergeAttributes = mergeAttributes;

		if (cacheMeta)
			// note normalExists isn't quite the normal version of exists.
			// rather, it only checks for the existence of the requested on the backend
			// (this is the desired behavior the cache needs
			cache = new N5JsonCache(this);
		else
			cache = null;
	}

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
			final boolean mapN5DatasetAttributes,
			final boolean mergeAttributes,
			final boolean cacheMeta) throws IOException {

		this(true, keyValueAccess, basePath, gsonBuilder, mapN5DatasetAttributes, mergeAttributes, cacheMeta);
	}

	@Override
	public Gson getGson() {

		return gson;
	}

	@Override
	public boolean cacheMeta() {

		return cacheMeta;
	}

	@Override
	public N5JsonCache getCache() {

		return cache;
	}

	public KeyValueAccess getKeyValueAccess() {
		return keyValueAccess;
	}

	@Override
	public String getBasePath() {

		return this.basePath;
	}

	@Override
	public String groupPath(String... nodes) {

		// alternatively call compose twice, once with this functions inputs, then pass the result to the other groupPath method 
		// this impl assumes streams and array building are less expensive than keyValueAccess composition (may not always be true)
		return keyValueAccess.compose(Stream.concat(Stream.of(basePath), Arrays.stream(nodes)).toArray(String[]::new));
	}

	/**
	 * Returns the version of this zarr container.
	 *
	 * @return the {@link Version}
	 */
	@Override
	public Version getVersion() throws IOException {

		// Note: getZarray and getZGroup use cache if available.
		if( datasetExists("")) {
			return getVersion(getZArray("").getAsJsonObject());
		}
		else if( groupExists( "" )) {
			return getVersion(getZGroup("").getAsJsonObject());
		}
		return VERSION;
	}

	@Override
	public boolean exists(final String pathName) {

		// Overridden because of the difference in how n5 and zarr define "group" and "dataset". 
		// The implementation in CachedGsonKeyValueReader is simpler but more low-level
		final String normalPathName = N5URL.normalizeGroupPath( pathName );

		// Note that datasetExists and groupExists use the cache
		return datasetExists( normalPathName ) || groupExists( normalPathName );
	}

	/**
	 * Tests whether the key at normalPathName exists for the backend.
	 * Such a key may exist for paths that are neither zarr groups nor zarr datasets.
	 *
	 * @param normalPathName normalized path name
	 * @return true if the key exists 
	 */
	public boolean existsFromContainer(final String normalPathName) {

		// A cloud keyValueAccess may return false for exists(groupPath(normalPathName)),
		// so instead need to check for existence of either .zarray or .zgroup
		return keyValueAccess.exists(zGroupAbsolutePath(normalPathName)) ||
			keyValueAccess.exists(zArrayAbsolutePath(normalPathName));
	}

	@Override
	public boolean groupExists(final String pathName) {

		// TODO I overrode this because parent uses attributes.json,
		// this uses .zgroup. But may not be necessary, since isGroupFromContainer doesn't use the cache key
		// (because its hardcoded).
		final String normalPath = N5URL.normalizeGroupPath(pathName);
		if (cacheMeta()) {
			return cache.isGroup(normalPath, zgroupFile);
		}
		return isGroupFromContainer(normalPath);
	}

	public boolean isGroupFromContainer(final String normalPath) {

		return keyValueAccess.isFile(keyValueAccess.compose(basePath, normalPath, zgroupFile));
	}

	@Override
	public boolean datasetExists(final String pathName) throws N5Exception.N5IOException {

		if (cacheMeta()) {
			final String normalPathName = N5URL.normalizeGroupPath(pathName);
			return cache.isDataset(normalPathName, zarrayFile);
		}
		return isDatasetFromContainer(pathName);
	}

	public boolean isDatasetFromContainer(String normalPathName) throws N5Exception.N5IOException {

		return keyValueAccess.isFile(keyValueAccess.compose(basePath, normalPathName, zarrayFile));
	}

	protected DatasetAttributes normalReadDatasetAttributes(final String pathName) throws N5Exception.N5IOException {

		try {
			return createDatasetAttributes(getZArray(pathName));
		} catch (IOException e) {
			throw new N5Exception.N5IOException(e);
		}
	}

	@Override
	public Map<String, Class<?>> listAttributes(final String pathName) throws IOException {

		return GsonUtils.listAttributes(getAttributes(pathName));
	}

	/**
	 * Returns the {@link ZarrDatasetAttributes} located at the given path, if present.
	 *
	 * @param pathName the path relative to the container's root
	 * @return the zarr array attributes
	 * @throws IOException the exception
	 */
	@Override
	public ZarrDatasetAttributes getDatasetAttributes(final String pathName) throws N5Exception.N5IOException {

		JsonElement attributes;
		try {
			// normalizes and caches
			attributes = getZArray(pathName);
			return createDatasetAttributes(attributes);
		} catch (IOException e) {
			throw new N5Exception.N5IOException(e);
		}
	}

	/**
	 * Returns the {@link ZArrayAttributes} located at the given path, if present.
	 *
	 * @param pathName the path relative to the container's root
	 * @return the zarr array attributes
	 * @throws IOException the exception
	 */
	public ZArrayAttributes getZArrayAttributes(final String pathName) throws IOException {

		return getZArrayAttributes(getZArray(pathName));
	}

	/**
	 * Constructs {@link ZArrayAttributes} from a {@link JsonElement}.
	 *
	 * @param attributes the json element
	 * @return the zarr array attributse
	 */
	protected ZArrayAttributes getZArrayAttributes(final JsonElement attributes) {

		return gson.fromJson(attributes, ZArrayAttributes.class);
	}

	public ZarrDatasetAttributes createDatasetAttributes(final JsonElement attributes) {

		final ZArrayAttributes zarray = getZArrayAttributes(attributes);
		return zarray != null ? zarray.getDatasetAttributes() : null;
	}

	@Override
	public <T> T getAttribute(final String pathName, final String key, final Class<T> clazz) throws IOException {

		final String normalizedAttributePath = N5URL.normalizeAttributePath(key);
		final JsonElement attributes = getAttributes(pathName);
		return GsonUtils.readAttribute(attributes, normalizedAttributePath, clazz, gson);
	}

	@Override
	public <T> T getAttribute(final String pathName, final String key, final Type type) throws IOException {

		final String normalizedAttributePath = N5URL.normalizeAttributePath(key);
		final JsonElement attributes = getAttributes(pathName);
		return GsonUtils.readAttribute(attributes, normalizedAttributePath, type, gson);
	}

	/**
	 * Returns a {@link JsonElement} representing the contents of .zgroup located at
	 * the given path if present, and null if not.
	 * 
	 * @param path the path
	 * @return the json element
	 * @throws IOException the exception
	 */
	public JsonElement getZGroup(final String path) throws IOException {

		if (cacheMeta()) {
			return cache.getAttributes(N5URL.normalizeGroupPath(path), zgroupFile);
		} else {
			return getAttributesFromContainer( N5URL.normalizeGroupPath(path), zgroupFile );
		}
	}

	/**
	 * Returns a {@link JsonElement} representing the contents of .zarray located at
	 * the given path if present, and null if not.
	 * 
	 * @param path the path
	 * @return the json element
	 * @throws IOException the exception
	 */
	public JsonElement getZArray(final String path) throws IOException {

		if (cacheMeta()) {
			return cache.getAttributes(N5URL.normalizeGroupPath(path), zarrayFile);
		} else {
			return getAttributesFromContainer( N5URL.normalizeGroupPath(path), zarrayFile );
		}
	}

	/**
	 * Returns a {@link JsonElement} representing the contents of .zattrs located at
	 * the given path if present, and null if not.
	 * 
	 * @param path the path
	 * @return the json element
	 * @throws N5Exception.N5IOException the exception
	 */
	public JsonElement getZAttributes(final String path) throws N5Exception.N5IOException {

		if (cacheMeta()) {
			return cache.getAttributes(N5URL.normalizeGroupPath(path), zattrsFile);
		} else {
			return getAttributesFromContainer( N5URL.normalizeGroupPath(path), zattrsFile );
		}
	}
	
	/**
	 * Returns the attributes at the given path.
	 * <p>
	 * If this reader was constructed with mergeAttributes as true, this method will attempt to
	 * combine the contents of .zgroup, .zarray, and .zattrs, when present using {@link ZarrUtils#combineAll(JsonElement...)}.
	 * Otherwise, this method will return only the contents of .zattrs, as {@link getZAttributes}.
	 *
	 * @param path the path
	 * @return the json element
	 * @throws N5Exception.N5IOException the exception
	 */
	public JsonElement getAttributes(final String path) throws N5Exception.N5IOException {

		// TODO normalization happening three times right now
		JsonElement groupElem = null;
		JsonElement arrElem = null;
		JsonElement attrElem = null;
		if (mergeAttributes) {
			try {
				groupElem = getZGroup(path); // uses cache
			} catch (IOException e) {}

			try {
				arrElem = getZArray(path); // uses cache
			} catch (IOException e) {}

			try {
				attrElem = getZAttributes(path); // uses cache
			} catch (N5Exception.N5IOException e) {}

			return combineAll( groupElem, arrElem, attrElem );
		}
		else
			return getZAttributes(path);
	}

	public JsonElement getAttributesFromContainer(final String normalResourceParent, final String normalResourcePath)
			throws N5Exception.N5IOException {

		final String absolutePath = keyValueAccess.compose(basePath, normalResourceParent, normalResourcePath);
		if (!keyValueAccess.exists(absolutePath))
			return null;

		try (final LockedChannel lockedChannel = keyValueAccess.lockForReading(absolutePath)) {
			return GsonUtils.readAttributes(lockedChannel.newReader(), gson);
		} catch (IOException e) {
			throw new N5Exception.N5IOException("Cannot open lock for Reading", e);
		}
	}

	protected String zArrayAbsolutePath(final String normalPath) {

		return keyValueAccess.compose(basePath, normalPath, zarrayFile);
	}

	protected String zAttrsAbsolutePath(final String normalPath) {

		return keyValueAccess.compose(basePath, normalPath, zattrsFile);
	}

	protected String zGroupAbsolutePath(final String normalPath) {

		return keyValueAccess.compose(basePath, normalPath, zgroupFile);
	}

	protected String zArrayPath(final String normalPath) {

		return keyValueAccess.compose(normalPath, zarrayFile);
	}

	protected String zAttrsPath(final String normalPath) {

		return keyValueAccess.compose(normalPath, zattrsFile);
	}

	protected String zGroupPath(final String normalPath) {

		return keyValueAccess.compose(normalPath, zgroupFile);
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

		if (!keyValueAccess.exists(absolutePath))
			return null;

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

	@Override
	public String toString() {

		return String.format("%s[access=%s, basePath=%s]", getClass().getSimpleName(), keyValueAccess, basePath);
	}

	protected static Version getVersion( final JsonObject json )
	{
		final JsonElement fmt = json.get(ZARR_FORMAT_KEY);
		if (fmt.isJsonPrimitive())
			return new Version(fmt.getAsInt(), 0, 0);

		return null;
	}

	/**
	 * Returns one {@link JsonElement} that (attempts to) combine all 
	 * passed json elements. Reduces the list by repeatedly calling the
	 * two-argument {@link combine} method.
	 * 
	 * @param elements an array of json elements
	 * @return a new, combined element
	 */
	static JsonElement combineAll(final JsonElement... elements) {
		return Arrays.stream(elements).reduce(null, ZarrKeyValueReader::combine);
	}

	/**
	 * Returns one {@link JsonElement} that combines two others. The returned instance
	 * is a deep copy, and the arguments are not modified.
	 * A copy of base element is returned if the two arguments can not be combined.
	 * The two arguments may be combined if they are both {@link JsonObject}s or both
	 * {@link JsonArray}s.
	 * <p>
	 * If both arguments are {@link JsonObject}s, every key-value pair in the add argument
	 * is added to the (copy of) the base argument, overwriting any duplicate keys.
	 * If both arguments are {@link JsonArray}s, the add argument is concatenated to the 
	 * (copy of) the base argument.
	 *
	 * @param base the base element
	 * @param add the element to add
	 * @return the new element
	 */
	static JsonElement combine(final JsonElement base, final JsonElement add) {
		if (base == null)
			return add == null ? null : add.deepCopy();
		else if (add == null)
			return base == null ? null : base.deepCopy();

		if (base.isJsonObject() && add.isJsonObject()) {
			final JsonObject baseObj = base.getAsJsonObject().deepCopy();
			final JsonObject addObj = add.getAsJsonObject();
			for (String k : addObj.keySet())
				baseObj.add(k, addObj.get(k));

			return baseObj;
		} else if (base.isJsonArray() && add.isJsonArray()) {
			final JsonArray baseArr = base.getAsJsonArray().deepCopy();
			final JsonArray addArr = add.getAsJsonArray();
			for (int i = 0; i < addArr.size(); i++)
				baseArr.add(addArr.get(i));

			return baseArr;
		}
		return base == null ? null : base.deepCopy();
	}

	static Gson registerGson(final GsonBuilder gsonBuilder) {

		return addTypeAdapters(gsonBuilder).create();
	}

	static GsonBuilder addTypeAdapters(GsonBuilder gsonBuilder) {

		gsonBuilder.registerTypeAdapter(DType.class, new DType.JsonAdapter());
		gsonBuilder.registerTypeAdapter(ZarrCompressor.class, ZarrCompressor.jsonAdapter);
		gsonBuilder.registerTypeAdapter(ZArrayAttributes.class, ZArrayAttributes.jsonAdapter);
		gsonBuilder.disableHtmlEscaping();
		return gsonBuilder;
	}

	@Override
	public String getContainerURI() {
		return basePath; // TODO fix
	}

}
