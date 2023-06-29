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
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.janelia.saalfeldlab.n5.BlockReader;
import org.janelia.saalfeldlab.n5.ByteArrayDataBlock;
import org.janelia.saalfeldlab.n5.CachedGsonKeyValueN5Reader;
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.CompressionAdapter;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.GsonUtils;
import org.janelia.saalfeldlab.n5.KeyValueAccess;
import org.janelia.saalfeldlab.n5.LockedChannel;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5URI;
import org.janelia.saalfeldlab.n5.RawCompression;
import org.janelia.saalfeldlab.n5.cache.N5JsonCacheableContainer;
import org.janelia.saalfeldlab.n5.zarr.cache.ZarrJsonCache;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;

/**
 * {@link N5Reader} implementation through {@link KeyValueAccess} with JSON
 * attributes parsed with {@link Gson}.
 *
 * @author Stephan Saalfeld
 * @author John Bogovic
 */
public class ZarrKeyValueReader implements CachedGsonKeyValueN5Reader, N5JsonCacheableContainer {

	public static final Version VERSION_ZERO = new Version(0, 0, 0);
	public static final Version VERSION = new Version(2, 0, 0);
	public static final String ZARR_FORMAT_KEY = "zarr_format";

	public static final String ZARRAY_FILE = ".zarray";
	public static final String ZATTRS_FILE = ".zattrs";
	public static final String ZGROUP_FILE = ".zgroup";

	protected final KeyValueAccess keyValueAccess;

	protected final Gson gson;

	protected final ZarrJsonCache cache;

	protected final boolean cacheMeta;

	protected URI uri;

	final protected boolean mapN5DatasetAttributes;

	final protected boolean mergeAttributes;

	/**
	 * Opens an {@link ZarrKeyValueReader} at a given base path with a custom
	 * {@link GsonBuilder} to support custom attributes.
	 *
	 * @param checkVersion
	 *            perform version check
	 * @param keyValueAccess
	 * @param basePath
	 *            N5 base path
	 * @param gsonBuilder
	 *            the gson builder
	 * @param mapN5DatasetAttributes
	 *            If true, getAttributes and variants of getAttribute methods
	 *            will
	 *            contain keys used by n5 datasets, and whose values are those
	 *            for
	 *            their corresponding zarr fields. For example, if true, the key
	 *            "dimensions"
	 *            (from n5) may be used to obtain the value of the key "shape"
	 *            (from zarr).
	 * @param mergeAttributes
	 *            If true, fields from .zgroup, .zarray, and .zattrs will be
	 *            merged
	 *            when calling getAttributes, and variants of getAttribute
	 * @param cacheMeta
	 *            cache attributes and meta data
	 *            Setting this to true avoids frequent reading and parsing of
	 *            JSON
	 *            encoded attributes and other meta data that requires accessing
	 *            the
	 *            store. This is most interesting for high latency backends.
	 *            Changes
	 *            of cached attributes and meta data by an independent writer
	 *            will
	 *            not be tracked.
	 *
	 * @throws N5Exception
	 *             if the base path cannot be read or does not exist,
	 *             if the N5 version of the container is not compatible with
	 *             this
	 *             implementation.
	 */
	public ZarrKeyValueReader(
			final boolean checkVersion,
			final KeyValueAccess keyValueAccess,
			final String basePath,
			final GsonBuilder gsonBuilder,
			final boolean mapN5DatasetAttributes,
			final boolean mergeAttributes,
			final boolean cacheMeta)
			throws N5Exception {

		this.keyValueAccess = keyValueAccess;
		this.gson = registerGson(gsonBuilder);
		this.cacheMeta = cacheMeta;
		this.mapN5DatasetAttributes = mapN5DatasetAttributes;
		this.mergeAttributes = mergeAttributes;

		try {
			uri = keyValueAccess.uri(basePath);
		} catch (final URISyntaxException e) {
			throw new N5Exception(e);
		}

		if (cacheMeta)
			// note normalExists isn't quite the normal version of exists.
			// rather, it only checks for the existence of the requested on the
			// backend
			// (this is the desired behavior the cache needs
			cache = newCache();
		else
			cache = null;
	}

	/**
	 * Opens an {@link ZarrKeyValueReader} at a given base path with a custom
	 * {@link GsonBuilder} to support custom attributes.
	 *
	 * @param keyValueAccess
	 * @param basePath
	 *            N5 base path
	 * @param gsonBuilder
	 * @param cacheMeta
	 *            cache attributes and meta data
	 *            Setting this to true avoids frequent reading and parsing of
	 *            JSON
	 *            encoded attributes and other meta data that requires accessing
	 *            the
	 *            store. This is most interesting for high latency backends.
	 *            Changes
	 *            of cached attributes and meta data by an independent writer
	 *            will
	 *            not be tracked.
	 *
	 * @throws N5Exception
	 *             if the base path cannot be read or does not exist,
	 *             if the N5 version of the container is not compatible with
	 *             this
	 *             implementation.
	 */
	public ZarrKeyValueReader(
			final KeyValueAccess keyValueAccess,
			final String basePath,
			final GsonBuilder gsonBuilder,
			final boolean mapN5DatasetAttributes,
			final boolean mergeAttributes,
			final boolean cacheMeta)
			throws N5Exception {

		this(true, keyValueAccess, basePath, gsonBuilder, mapN5DatasetAttributes, mergeAttributes, cacheMeta);
	}

	@Override
	public Gson getGson() {

		return gson;
	}

	@Override
	public ZarrJsonCache newCache() {

		return new ZarrJsonCache(this);
	}

	@Override
	public boolean cacheMeta() {

		return cacheMeta;
	}

	@Override
	public ZarrJsonCache getCache() {

		return cache;
	}

	@Override
	public KeyValueAccess getKeyValueAccess() {

		return keyValueAccess;
	}

	@Override
	public URI getURI() {

		return uri;
	}

	/**
	 * Returns the version of this zarr container.
	 *
	 * @return the {@link Version}
	 */
	@Override
	public Version getVersion() throws N5Exception {

		// Note: getZarray and getZGroup use cache if available.
		if (datasetExists("")) {
			return getVersion(getZArray(""));
		} else if (groupExists("")) {
			return getVersion(getZGroup(""));
		}
		return VERSION;
	}

	@Override
	public boolean exists(final String pathName) {

		// Overridden because of the difference in how n5 and zarr define "group" and "dataset".
		// The implementation in CachedGsonKeyValueReader is simpler but more low-level
		final String normalPathName = N5URI.normalizeGroupPath(pathName);

		// Note that datasetExists and groupExists use the cache
		return groupExists(normalPathName) || datasetExists(normalPathName);
	}

	/**
	 * Tests whether the key at normalPathName exists for the backend.
	 * Such a key may exist for paths that are neither zarr groups nor zarr
	 * datasets.
	 *
	 * @param normalPathName
	 *            normalized path name
	 * @return true if the key exists
	 */
	public boolean existsFromContainer(final String normalPathName) {

		// A cloud keyValueAccess may return false for
		// exists(groupPath(normalPathName)),
		// so instead need to check for existence of either .zarray or .zgroup
		return
				keyValueAccess.exists(keyValueAccess.compose(uri, normalPathName, ZGROUP_FILE)) ||
				keyValueAccess.exists(keyValueAccess.compose(uri, normalPathName, ZARRAY_FILE));
	}

	@Override
	public boolean groupExists(final String pathName) {

		// Overriden because the parent implementation uses attributes.json,
		// this uses .zgroup.
		final String normalPath = N5URI.normalizeGroupPath(pathName);
		if (cacheMeta()) {
			return cache.isGroup(normalPath, ZGROUP_FILE);
		}
		return isGroupFromContainer(normalPath);
	}

	@Override
	public boolean isGroupFromContainer(final String normalPath) {

		return keyValueAccess.isFile(keyValueAccess.compose(uri, normalPath, ZGROUP_FILE));
	}

	@Override
	public boolean isGroupFromAttributes(final String normalCacheKey, final JsonElement attributes) {

		return attributes != null && attributes.isJsonObject() && attributes.getAsJsonObject().has(ZARR_FORMAT_KEY);
	}

	@Override
	public boolean datasetExists(final String pathName) throws N5Exception.N5IOException {

		if (cacheMeta()) {
			final String normalPathName = N5URI.normalizeGroupPath(pathName);
			return cache.isDataset(normalPathName, ZARRAY_FILE);
		}
		return isDatasetFromContainer(pathName);
	}

	@Override
	public boolean isDatasetFromContainer(final String normalPathName) throws N5Exception {

		if (keyValueAccess.isFile(keyValueAccess.compose(uri, normalPathName, ZARRAY_FILE))) {
			return isDatasetFromAttributes(ZARRAY_FILE, getAttributesFromContainer(normalPathName, ZARRAY_FILE));
		} else {
			return false;
		}
	}

	@Override
	public boolean isDatasetFromAttributes(final String normalCacheKey, final JsonElement attributes) {

		if (normalCacheKey.equals(ZARRAY_FILE) && attributes != null && attributes.isJsonObject()) {
			return createDatasetAttributes(attributes) != null;
		} else {
			return false;
		}
	}

	/**
	 * Returns the {@link ZarrDatasetAttributes} located at the given path, if
	 * present.
	 *
	 * @param pathName
	 *            the path relative to the container's root
	 * @return the zarr array attributes
	 * @throws N5Exception
	 *             the exception
	 */
	@Override
	public ZarrDatasetAttributes getDatasetAttributes(final String pathName) throws N5Exception {

		return createDatasetAttributes(getZArray(pathName));
	}

	/**
	 * Returns the {@link ZArrayAttributes} located at the given path, if
	 * present.
	 *
	 * @param pathName
	 *            the path relative to the container's root
	 * @return the zarr array attributes
	 * @throws N5Exception
	 *             the exception
	 */
	public ZArrayAttributes getZArrayAttributes(final String pathName) throws N5Exception {

		return getZArrayAttributes(getZArray(pathName));
	}

	/**
	 * Constructs {@link ZArrayAttributes} from a {@link JsonElement}.
	 *
	 * @param attributes
	 *            the json element
	 * @return the zarr array attributse
	 */
	protected ZArrayAttributes getZArrayAttributes(final JsonElement attributes) {

		return gson.fromJson(attributes, ZArrayAttributes.class);
	}

	@Override
	public ZarrDatasetAttributes createDatasetAttributes(final JsonElement attributes) {

		final ZArrayAttributes zarray = getZArrayAttributes(attributes);
		return zarray != null ? zarray.getDatasetAttributes() : null;
	}

	@Override
	public <T> T getAttribute(final String pathName, final String key, final Class<T> clazz) throws N5Exception {

		final String normalizedAttributePath = N5URI.normalizeAttributePath(key);
		final JsonElement attributes = getAttributes(pathName); // handles caching
		try {
			return GsonUtils.readAttribute(attributes, normalizedAttributePath, clazz, gson);
		} catch (JsonSyntaxException | NumberFormatException | ClassCastException e) {
			throw new N5Exception.N5ClassCastException(e);
		}
	}

	@Override
	public <T> T getAttribute(final String pathName, final String key, final Type type) throws N5Exception {

		final String normalizedAttributePath = N5URI.normalizeAttributePath(key);
		final JsonElement attributes = getAttributes(pathName); // handles caching
		try {
			return GsonUtils.readAttribute(attributes, normalizedAttributePath, type, gson);
		} catch (JsonSyntaxException | NumberFormatException | ClassCastException e) {
			throw new N5Exception.N5ClassCastException(e);
		}
	}

	/**
	 * Returns a {@link JsonElement} representing the contents of a JSON file
	 * located at the given path if present, and null if not.  Also updates the
	 * cache.
	 *
	 * @param normalPath
	 *            the normalized path
	 * @param jsonName
	 *            the name of the JSON file
	 * @return the JSON element
	 * @throws N5Exception
	 *             the exception
	 */
	protected JsonElement getJsonResource(final String normalPath, final String jsonName) throws N5Exception {

		if (cacheMeta()) {
			return cache.getAttributes(normalPath, jsonName);
		} else {
			return getAttributesFromContainer(normalPath, jsonName);
		}
	}

	protected static JsonElement reverseAttrsWhenCOrder(final JsonElement elem) {

		if (elem == null || !elem.isJsonObject())
			return elem;

		final JsonObject attrs = elem.getAsJsonObject();
		if (attrs.get(ZArrayAttributes.orderKey).getAsString().equals("C")) {

			final JsonArray shape = attrs.get(ZArrayAttributes.shapeKey).getAsJsonArray();
			ZarrKeyValueWriter.reorder(shape);
			attrs.add(ZArrayAttributes.shapeKey, shape);

			final JsonArray chunkSize = attrs.get(ZArrayAttributes.chunksKey).getAsJsonArray();
			ZarrKeyValueWriter.reorder(chunkSize);
			attrs.add(ZArrayAttributes.chunksKey, chunkSize);
		}
		return attrs;
	}

	/**
	 * Returns a {@link JsonElement} representing the contents of .zgroup
	 * located at the given path if present, and null if not.
	 *
	 * @param path
	 *            the path
	 * @return the JSON element
	 * @throws N5Exception
	 *             the exception
	 */
	protected JsonElement getZGroup(final String path) throws N5Exception {

		return getJsonResource(N5URI.normalizeGroupPath(path), ZGROUP_FILE);
	}

	/**
	 * Returns a {@link JsonElement} representing the contents of .zarray
	 * located at the given path if present, and null if not.
	 *
	 * @param path
	 *            the path
	 * @return the JSON element
	 * @throws N5Exception
	 *             the exception
	 */
	protected JsonElement getZArray(final String path) throws N5Exception {

		return getJsonResource(N5URI.normalizeGroupPath(path), ZARRAY_FILE);
	}

	protected JsonElement zarrToN5DatasetAttributes(final JsonElement elem ) {

		if( !mapN5DatasetAttributes || elem == null || !elem.isJsonObject())
			return elem;

		final JsonObject attrs = elem.getAsJsonObject();
		final ZArrayAttributes zattrs = getZArrayAttributes(attrs);
		if (zattrs == null)
			return elem;

		attrs.add(DatasetAttributes.DIMENSIONS_KEY, attrs.get(ZArrayAttributes.shapeKey));
		attrs.add(DatasetAttributes.BLOCK_SIZE_KEY, attrs.get(ZArrayAttributes.chunksKey));
		attrs.addProperty(DatasetAttributes.DATA_TYPE_KEY,
				new DType(attrs.get(ZArrayAttributes.dTypeKey).getAsString()).getDataType().toString());

		JsonElement e = attrs.get(ZArrayAttributes.compressorKey);
		if (e == JsonNull.INSTANCE) {
			attrs.add(DatasetAttributes.COMPRESSION_KEY, gson.toJsonTree(new RawCompression()));
		} else {
			attrs.add(DatasetAttributes.COMPRESSION_KEY, gson.toJsonTree(
					gson.fromJson(attrs.get(ZArrayAttributes.compressorKey), ZarrCompressor.class).getCompression()));
		}
		return attrs;
	}

	protected JsonElement n5ToZarrDatasetAttributes(final JsonElement elem ) {

		if( !mapN5DatasetAttributes || elem == null || !elem.isJsonObject())
			return elem;

		final JsonObject attrs = elem.getAsJsonObject();
		if (attrs.has(DatasetAttributes.DIMENSIONS_KEY))
			attrs.add(ZArrayAttributes.shapeKey, attrs.get(DatasetAttributes.DIMENSIONS_KEY));

		if (attrs.has(DatasetAttributes.BLOCK_SIZE_KEY))
			attrs.add(ZArrayAttributes.chunksKey, attrs.get(DatasetAttributes.BLOCK_SIZE_KEY));

		if (attrs.has(DatasetAttributes.DATA_TYPE_KEY))
			attrs.add(ZArrayAttributes.dTypeKey, attrs.get(DatasetAttributes.DATA_TYPE_KEY));

		return attrs;
	}

	/**
	 * Returns a {@link JsonElement} representing the contents of .zattrs
	 * located at the given path if present, and null if not.
	 *
	 * @param path
	 *            the path
	 * @return the JSON element
	 * @throws N5Exception
	 *             the exception
	 */
	protected JsonElement getZAttributes(final String path) throws N5Exception {

		return getJsonResource(N5URI.normalizeGroupPath(path), ZATTRS_FILE);
	}

	/**
	 * Returns the attributes at the given path.
	 * <p>
	 * If this reader was constructed with mergeAttributes as true, this method
	 * will attempt to combine the contents of .zgroup, .zarray, and .zattrs,
	 * when present using {@link ZarrUtils#combineAll(JsonElement...)}.
	 * Otherwise, this method will return only the contents of .zattrs, as
	 * {@link getZAttributes}.
	 *
	 * @param path
	 *            the path
	 * @return the json element
	 * @throws N5Exception
	 *             the exception
	 */
	@Override
	public JsonElement getAttributes(final String path) throws N5Exception {

		JsonElement out;
		if (mergeAttributes) {
			out= combineAll(
					getJsonResource(N5URI.normalizeGroupPath(path), ZGROUP_FILE),
					zarrToN5DatasetAttributes(
							reverseAttrsWhenCOrder(getJsonResource(N5URI.normalizeGroupPath(path), ZARRAY_FILE))),
					getJsonResource(N5URI.normalizeGroupPath(path), ZATTRS_FILE));
		} else
			out = getZAttributes(path);

		return out;
	}

	protected JsonElement getAttributesUnmapped(final String path) throws N5Exception {

		JsonElement out;
		if (mergeAttributes) {

			out= combineAll(
					getJsonResource(N5URI.normalizeGroupPath(path), ZGROUP_FILE),
					reverseAttrsWhenCOrder(getJsonResource(N5URI.normalizeGroupPath(path), ZARRAY_FILE)),
					getJsonResource(N5URI.normalizeGroupPath(path), ZATTRS_FILE));
		} else
			out = getZAttributes(path);

		return out;
	}

	@Override
	public JsonElement getAttributesFromContainer(
			final String normalResourceParent,
			final String normalResourcePath) throws N5Exception {

		final String absolutePath = keyValueAccess.compose(uri, normalResourceParent, normalResourcePath);
		if (!keyValueAccess.exists(absolutePath))
			return null;

		try (final LockedChannel lockedChannel = keyValueAccess.lockForReading(absolutePath)) {
			return GsonUtils.readAttributes(lockedChannel.newReader(), gson);
		} catch (final IOException e) {
			throw new N5IOException("Failed to read " + absolutePath, e);
		}
	}

	@Override
	public DataBlock<?> readBlock(
			final String pathName,
			final DatasetAttributes datasetAttributes,
			final long... gridPosition) throws N5Exception {

		final ZarrDatasetAttributes zarrDatasetAttributes;
		if (datasetAttributes instanceof ZarrDatasetAttributes)
			zarrDatasetAttributes = (ZarrDatasetAttributes)datasetAttributes;
		else
			zarrDatasetAttributes = getDatasetAttributes(pathName);

		final String absolutePath = keyValueAccess
				.compose(
						uri,
						pathName,
						getZarrDataBlockPath(
								gridPosition,
								zarrDatasetAttributes.getDimensionSeparator(),
								zarrDatasetAttributes.isRowMajor()));

		if (!keyValueAccess.exists(absolutePath))
			return null;

		try (final LockedChannel lockedChannel = keyValueAccess.lockForReading(absolutePath)) {
			return readBlock(lockedChannel.newInputStream(), zarrDatasetAttributes, gridPosition);
		} catch (final IOException e) {
			throw new N5IOException(
					"Failed to read block " + Arrays.toString(gridPosition) + " from dataset " + pathName,
					e);
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
	protected static DataBlock<?> readBlock(
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

		return dataBlock;
	}

	/**
	 * Constructs the path for a data block in a dataset at a given grid
	 * position.
	 *
	 * The returned path is
	 *
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
			for (int i = gridPosition.length - 2; i >= 0; --i) {
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

		return String.format("%s[access=%s, basePath=%s]", getClass().getSimpleName(), keyValueAccess, uri.getPath());
	}

	protected static Version getVersion(final JsonElement json) {

		if (json == null || !json.isJsonObject()) {
			return VERSION_ZERO;
		}

		final JsonElement fmt = json.getAsJsonObject().get(ZARR_FORMAT_KEY);
		if (fmt.isJsonPrimitive())
			return new Version(fmt.getAsInt(), 0, 0);

		return null;
	}

	/**
	 * Returns one {@link JsonElement} that (attempts to) combine all
	 * passed json elements. Reduces the list by repeatedly calling the
	 * two-argument {@link combine} method.
	 *
	 * @param elements
	 *            an array of json elements
	 * @return a new, combined element
	 */
	protected static JsonElement combineAll(final JsonElement... elements) {

		return Arrays.stream(elements).reduce(null, ZarrKeyValueReader::combine);
	}

	/**
	 * Returns one {@link JsonElement} that combines two others. The returned
	 * instance
	 * is a deep copy, and the arguments are not modified.
	 * A copy of base element is returned if the two arguments can not be
	 * combined.
	 * The two arguments may be combined if they are both {@link JsonObject}s or
	 * both
	 * {@link JsonArray}s.
	 * <p>
	 * If both arguments are {@link JsonObject}s, every key-value pair in the
	 * add argument
	 * is added to the (copy of) the base argument, overwriting any duplicate
	 * keys.
	 * If both arguments are {@link JsonArray}s, the add argument is
	 * concatenated to the
	 * (copy of) the base argument.
	 *
	 * @param base
	 *            the base element
	 * @param add
	 *            the element to add
	 * @return the new element
	 */
	protected static JsonElement combine(final JsonElement base, final JsonElement add) {

		if (base == null)
			return add == null ? null : add.deepCopy();
		else if (add == null)
			return base == null ? null : base.deepCopy();

		if (base.isJsonObject() && add.isJsonObject()) {
			final JsonObject baseObj = base.getAsJsonObject().deepCopy();
			final JsonObject addObj = add.getAsJsonObject();
			for (final String k : addObj.keySet())
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

	protected static GsonBuilder addTypeAdapters(final GsonBuilder gsonBuilder) {

		gsonBuilder.registerTypeAdapter(DType.class, new DType.JsonAdapter());
		gsonBuilder.registerTypeAdapter(DataType.class, new DataType.JsonAdapter());
		gsonBuilder.registerTypeAdapter(ZarrCompressor.class, ZarrCompressor.jsonAdapter);
		gsonBuilder.registerTypeHierarchyAdapter(Compression.class, CompressionAdapter.getJsonAdapter());
		gsonBuilder.registerTypeAdapter(Compression.class, CompressionAdapter.getJsonAdapter());
		gsonBuilder.registerTypeAdapter(ZArrayAttributes.class, ZArrayAttributes.jsonAdapter);
		gsonBuilder.disableHtmlEscaping();
		gsonBuilder.serializeNulls();

		return gsonBuilder;
	}

}
