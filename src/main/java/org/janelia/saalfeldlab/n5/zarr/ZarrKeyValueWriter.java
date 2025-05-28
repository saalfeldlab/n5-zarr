/*-
 * #%L
 * Not HDF5
 * %%
 * Copyright (C) 2019 - 2025 Stephan Saalfeld
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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSyntaxException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import net.imglib2.blocks.SubArrayCopy;
import net.imglib2.util.Intervals;

import org.apache.commons.lang3.ArrayUtils;
import org.janelia.saalfeldlab.n5.CachedGsonKeyValueN5Writer;
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.DefaultBlockWriter;
import org.janelia.saalfeldlab.n5.GsonKeyValueN5Writer;
import org.janelia.saalfeldlab.n5.GsonUtils;
import org.janelia.saalfeldlab.n5.KeyValueAccess;
import org.janelia.saalfeldlab.n5.LockedChannel;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.N5URI;
import org.janelia.saalfeldlab.n5.RawCompression;
import org.janelia.saalfeldlab.n5.cache.N5JsonCache;

import static net.imglib2.util.Util.safeInt;

/**
 * Zarr {@link GsonKeyValueN5Writer} implementation.
 *
 * @author Stephan Saalfeld
 * @author John Bogovic
 */
public class ZarrKeyValueWriter extends ZarrKeyValueReader implements CachedGsonKeyValueN5Writer {

	protected String dimensionSeparator;

	/**
	 * Opens an {@link ZarrKeyValueWriter} at a given base path with a custom
	 * {@link GsonBuilder} to support custom attributes.
	 *
	 * If the base path does not exist, it will be created.
	 *
	 * If the base path exists and if the N5 version of the container is
	 * compatible with this implementation, the N5 version of this container
	 * will be set to the current N5 version of this implementation.
	 *
	 * @param keyValueAccess
	 * @param basePath
	 *            n5 base path
	 * @param gsonBuilder
	 * @param cacheAttributes
	 *            cache attributes
	 *            Setting this to true avoids frequent reading and parsing of
	 *            JSON
	 *            encoded attributes, this is most interesting for high latency
	 *            file
	 *            systems. Changes of attributes by an independent writer will
	 *            not be
	 *            tracked.
	 *
	 * @throws N5Exception
	 *             if the base path cannot be written to or cannot be created.
	 */
	public ZarrKeyValueWriter(
			final KeyValueAccess keyValueAccess,
			final String basePath,
			final GsonBuilder gsonBuilder,
			final boolean mapN5DatasetAttributes,
			final boolean mergeAttributes,
			final String dimensionSeparator,
			final boolean cacheAttributes)
			throws N5Exception {

		super(
				false,
				keyValueAccess,
				basePath,
				gsonBuilder,
				mapN5DatasetAttributes,
				mergeAttributes,
				cacheAttributes,
				false);
		this.dimensionSeparator = dimensionSeparator;
		Version version = null;
		if (exists("/")) {
			version = getVersion();
			if (!isCompatible(version))
				throw new N5Exception.N5IOException(
						"Incompatible version " + version + " (this is " + ZarrKeyValueReader.VERSION + ").");
		}

		if (version == null || version == VERSION_ZERO) {
			createGroup("/");
			setVersion("/");
		}
	}

	public boolean isCompatible(Version version) {

		return ZarrKeyValueReader.VERSION.isCompatible(version);
	}

	@Override
	public void setVersion(final String path) throws N5Exception {

		setVersion(path, false);
	}

	protected void setVersion(final String path, final boolean create) throws N5Exception {

		final JsonObject versionObject = new JsonObject();
		versionObject.add(ZARR_FORMAT_KEY, new JsonPrimitive(N5ZarrReader.VERSION.getMajor()));

		final String normalPath = N5URI.normalizeGroupPath(path);
		if (create || groupExists(normalPath))
			writeZGroup(normalPath, versionObject); // updates cache
		else if (datasetExists(normalPath)) {
			final JsonObject zarrayJson = getZArray(normalPath).getAsJsonObject();
			zarrayJson.add(ZARR_FORMAT_KEY, new JsonPrimitive(N5ZarrReader.VERSION.getMajor()));
			writeZArray(normalPath, zarrayJson); // updates cache
		}
	}

	/**
	 * Creates a .zgroup file containing the zarr_format at the given path
	 * and all parent paths.
	 *
	 * @param normalPath
	 */
	protected void initializeGroup(final String normalPath) throws N5Exception {

		final String[] components = keyValueAccess.components(normalPath);
		String path = "";
		for (int i = 0; i < components.length; i++) {
			path = keyValueAccess.compose(path, components[i]);
			setVersion(path, true);
		}
	}

	@Override
	public void createGroup(final String path) throws N5Exception {

		final String normalPath = N5URI.normalizeGroupPath(path);
		// avoid hitting the backend if this path is already a group according to the cache
		// else if exists is true (then a dataset is present) so throw an exception to avoid
		// overwriting / invalidating existing data
		if (cacheMeta()) {
			if( getCache().isGroup(normalPath, ZGROUP_FILE))
				return;
			else if ( getCache().isDataset(normalPath, ZARRAY_FILE)){
				throw new N5Exception("Can't make a group on existing dataset.");
			}
		}

		// Overridden to change the cache key, though it may not be necessary
		// since the contents is null
		try {
			keyValueAccess.createDirectories(absoluteGroupPath(normalPath));
		} catch (final Throwable e) {
			throw new N5Exception.N5IOException("Failed to create group " + path, e);
		}

		final JsonObject versionObject = new JsonObject();
		versionObject.add(ZARR_FORMAT_KEY, new JsonPrimitive(N5ZarrReader.VERSION.getMajor()));

		String[] pathParts = getKeyValueAccess().components(normalPath);
		String parent = N5URI.normalizeGroupPath("/");
		if (pathParts.length == 0) {
			pathParts = new String[]{""};
		}

		for (final String child : pathParts) {

			final String childPath = parent.isEmpty() ? child : parent + "/" + child;
			if (cacheMeta()) {
				getCache().initializeNonemptyCache(childPath, ZGROUP_FILE);
				getCache().updateCacheInfo(childPath, ZGROUP_FILE, versionObject);

				// only add if the parent exists and has children cached already
				if (parent != null && !child.isEmpty())
					getCache().addChildIfPresent(parent, child);
			}

			setVersion(childPath, true);
			parent = childPath;
		}
	}

	@Override
	public void createDataset(
			final String path,
			final DatasetAttributes datasetAttributes) throws N5Exception {

		final String normalPath = N5URI.normalizeGroupPath(path);
		boolean wasGroup = false;
		if (cacheMeta()) {
			if (getCache().isDataset(normalPath, ZARRAY_FILE))
				return;
			else if (getCache().isGroup(normalPath, ZGROUP_FILE)) {
				wasGroup = true;
				// TODO tests currently require that we can make a dataset on a group
//				throw new N5Exception("Can't make a group on existing path.");
			}
		}

		// Overriding because CachedGsonKeyValueWriter calls createGroup.
		// Not correct for zarr, since groups and datasets are mutually
		// exclusive
		final String absPath = absoluteGroupPath(normalPath);
		try {
			keyValueAccess.createDirectories(absPath);
		} catch (final Throwable e) {
			throw new N5IOException("Failed to create directories " + absPath, e);
		}

		// create parent group
		final String[] pathParts = keyValueAccess.components(normalPath);
		final String parent = Arrays.stream(pathParts).limit(pathParts.length - 1).collect(Collectors.joining("/"));
		createGroup(parent);

		// These three lines are preferable to setDatasetAttributes because they
		// are more efficient wrt caching
		final ZArrayAttributes zarray = createZArrayAttributes(datasetAttributes);
		final HashMap<String, Object> zarrayMap = zarray.asMap();
		final JsonElement attributes = gson.toJsonTree(zarrayMap);
		writeJsonResource(normalPath, ZARRAY_FILE, zarrayMap);

		if( wasGroup )
			deleteJsonResource(normalPath, ZGROUP_FILE );

		if (cacheMeta()) {
			// cache dataset and add as child to parent if necessary
			getCache().initializeNonemptyCache(normalPath, ZARRAY_FILE);
			getCache().updateCacheInfo(normalPath, ZARRAY_FILE, attributes);
			if (getCache().isDataset(normalPath, ZARRAY_FILE) && wasGroup) {
				getCache().updateCacheInfo(normalPath, ZGROUP_FILE, N5JsonCache.emptyJson);
			}

			getCache().addChildIfPresent(parent, pathParts[pathParts.length - 1]);
		}
	}

	@Override
	public void setAttributes(
			final String path,
			final Map<String, ?> attributes) throws N5Exception {

		final String normalPath = N5URI.normalizeGroupPath(path);
		if (!exists(normalPath))
			throw new N5Exception.N5IOException("" + normalPath + " is not a group or dataset.");

		// cache here or in writeAttributes?
		// I think in writeAttributes is better - let it delegate to
		// writeZArray, writeZAttrs, writeZGroup
		final JsonElement existingAttributes = getAttributesUnmapped(normalPath); // uses cache
		JsonElement newAttributes = existingAttributes != null && existingAttributes.isJsonObject()
				? existingAttributes.getAsJsonObject()
				: new JsonObject();
		newAttributes = GsonUtils.insertAttributes(newAttributes, attributes, gson);

		if (newAttributes.isJsonObject()) {
			final ZarrJsonElements zje = build(newAttributes.getAsJsonObject(), getGson(), mapN5DatasetAttributes);
			// the three methods below handle caching
			writeZArray(normalPath, zje.zarray);
			writeZAttrs(normalPath, zje.zattrs);
			writeZGroup(normalPath, zje.zgroup);
		} else {
			writeZAttrs(normalPath, newAttributes);
		}
	}

	@Override
	public boolean removeAttribute(final String pathName, final String key) throws N5Exception {

		final String normalPath = N5URI.normalizeGroupPath(pathName);
		final String normalKey = N5URI.normalizeAttributePath(key);
		if (!keyValueAccess.exists(keyValueAccess.compose(uri, normalPath, ZATTRS_FILE)))
			return false;

		if (key.equals("/")) {
			writeJsonResource(normalPath, ZATTRS_FILE, JsonNull.INSTANCE);
			if (cacheMeta())
				cache.updateCacheInfo(normalPath, ZATTRS_FILE, JsonNull.INSTANCE);

			return true;
		}

		final JsonElement attributes = getZAttributes(normalPath); // uses cache

		if (GsonUtils.removeAttribute(attributes, normalKey) != null) {

			if (cacheMeta())
				cache.updateCacheInfo(normalPath, ZATTRS_FILE, attributes);

			writeJsonResource(normalPath, ZATTRS_FILE, attributes);
			return true;
		}
		return false;
	}

	@Override
	public <T> T removeAttribute(final String pathName, final String key, final Class<T> cls) throws N5Exception {

		final String normalPath = N5URI.normalizeGroupPath(pathName);
		final String normalKey = N5URI.normalizeAttributePath(key);

		final JsonElement attributes = getZAttributes(normalPath); // uses cache
		final T obj;
		try {
			obj = GsonUtils.removeAttribute(attributes, normalKey, cls, gson);
		} catch (JsonSyntaxException | NumberFormatException | ClassCastException e) {
			throw new N5Exception.N5ClassCastException(e);
		}
		if (obj != null) {
			if (cacheMeta())
				cache.updateCacheInfo(normalPath, ZATTRS_FILE, attributes);

			writeJsonResource(normalPath, ZATTRS_FILE, attributes);
		}
		return obj;
	}

	@Override
	public void setDatasetAttributes(
			final String pathName,
			final DatasetAttributes datasetAttributes) throws N5Exception {

		setZArrayAttributes(pathName, createZArrayAttributes(datasetAttributes));
	}

	protected ZArrayAttributes createZArrayAttributes(final DatasetAttributes datasetAttributes) {

		final long[] shape = datasetAttributes.getDimensions().clone();
		ArrayUtils.reverse(shape);
		final int[] chunks = datasetAttributes.getBlockSize().clone();
		ArrayUtils.reverse(chunks);
		final DType dType = new DType(datasetAttributes.getDataType());

		final ZArrayAttributes zArrayAttributes = new ZArrayAttributes(
				N5ZarrReader.VERSION.getMajor(),
				shape,
				chunks,
				dType,
				ZarrCompressor.fromCompression(datasetAttributes.getCompression()),
				"0",
				'C',
				dimensionSeparator,
				dType.getFilters());

		return zArrayAttributes;
	}

	/**
	 * Write the contents of the attributes argument to the .zarray file at
	 * the given pathName. Overwrites and existing .zarray.
	 *
	 * @param pathName the group / dataset path
	 * @param attributes ZArray attributes
	 * @throws N5Exception the exception
	 */
	public void setZArrayAttributes(final String pathName, final ZArrayAttributes attributes) throws N5Exception {

		writeZArray(pathName, gson.toJsonTree(attributes.asMap()));
	}

	protected void setZGroup(final String pathName, final Version version) throws IOException {

		// used in testVersion
		writeZGroup(pathName, gson.toJsonTree(version));
	}

	protected void deleteJsonResource(final String normalPath, final String jsonName) throws N5Exception {
		final String absolutePath = keyValueAccess.compose(uri, normalPath, jsonName);
		try {
			keyValueAccess.delete(absolutePath);
		} catch (final Throwable e1) {
			throw new N5IOException("Failed to delete " + absolutePath, e1);
		}
	}

	protected void writeJsonResource(
			final String normalPath,
			final String jsonName,
			final Object attributes) throws N5Exception {

		if (attributes == null)
			return;

		final String absolutePath = keyValueAccess.compose(uri, normalPath, jsonName);
		try (final LockedChannel lock = keyValueAccess.lockForWriting(absolutePath)) {
			final Writer writer = lock.newWriter();
			gson.toJson(attributes, writer);
			writer.flush();
		} catch (final Throwable e) {
			throw new N5IOException("Failed to write " + absolutePath, e);
		}
	}

	protected void writeZArray(
			final String normalGroupPath,
			final JsonElement attributes) throws N5Exception {

		if (attributes == null)
			return;

		writeJsonResource(normalGroupPath, ZARRAY_FILE, gson.fromJson(attributes, ZArrayAttributes.class));
		if (cacheMeta())
			cache.updateCacheInfo(normalGroupPath, ZARRAY_FILE, attributes);
	}

	protected void writeZGroup(
			final String normalGroupPath,
			final JsonElement attributes) throws N5Exception {

		if (attributes == null)
			return;

		writeJsonResource(normalGroupPath, ZGROUP_FILE, attributes);
		if (cacheMeta())
			cache.updateCacheInfo(normalGroupPath, ZGROUP_FILE, attributes);
	}

	protected void writeZAttrs(
			final String normalGroupPath,
			final JsonElement attributes) throws N5Exception {

		if (attributes == null)
			return;

		writeJsonResource(normalGroupPath, ZATTRS_FILE, attributes);
		if (cacheMeta())
			cache.updateCacheInfo(normalGroupPath, ZATTRS_FILE, attributes);
	}

	@Override
	public <T> void writeBlock(
			final String pathName,
			final DatasetAttributes datasetAttributes,
			final DataBlock<T> dataBlock) throws N5Exception {

		final ZarrDatasetAttributes zarrDatasetAttributes;
		if (datasetAttributes instanceof ZarrDatasetAttributes)
			zarrDatasetAttributes = (ZarrDatasetAttributes)datasetAttributes;
		else
			zarrDatasetAttributes = (ZarrDatasetAttributes)getDatasetAttributes(pathName);

		final String normalPath = N5URI.normalizeGroupPath(pathName);
		final String path = keyValueAccess
				.compose(
						uri,
						normalPath,
						getZarrDataBlockPath(
								dataBlock.getGridPosition(),
								zarrDatasetAttributes.getDimensionSeparator(),
								zarrDatasetAttributes.isRowMajor()).toString());

		final String[] components = keyValueAccess.components(path);
		final String parent = keyValueAccess
				.compose(Arrays.stream(components).limit(components.length - 1).toArray(String[]::new));
		try {
			keyValueAccess.createDirectories(parent);
			try (
					final LockedChannel lockedChannel = keyValueAccess.lockForWriting(path);
					final OutputStream out = lockedChannel.newOutputStream()
			) {
				DefaultBlockWriter.writeBlock(
						out,
						zarrDatasetAttributes,
						dataBlock);
			}
		} catch (final Throwable e) {
			throw new N5IOException(
					"Failed to write block " + Arrays.toString(dataBlock.getGridPosition()) + " into dataset " + path,
					e);
		}
	}

	@Override
	public boolean deleteBlock(
			final String path,
			final long... gridPosition) throws N5Exception {

		final String normPath = N5URI.normalizeGroupPath(path);
		final ZarrDatasetAttributes zarrDatasetAttributes = (ZarrDatasetAttributes)getDatasetAttributes(normPath);
		final String absolutePath = keyValueAccess
				.compose(
						uri,
						normPath,
						ZarrKeyValueReader
								.getZarrDataBlockPath(
										gridPosition,
										zarrDatasetAttributes.getDimensionSeparator(),
										zarrDatasetAttributes.isRowMajor()));

		try {
			if (keyValueAccess.exists(absolutePath))
				keyValueAccess.delete(absolutePath);
		} catch (final Throwable e) {
			throw new N5IOException(
					"Failed to delete block " + Arrays.toString(gridPosition) + " from dataset " + path,
					e);
		}

		/* an IOException should have occurred if anything had failed midway */
		return true;
	}

	public static byte[] padCrop(
			final byte[] src,
			final int[] srcBlockSize,
			final int[] dstBlockSize,
			final int nBytes,
			final int nBits,
			final byte[] fill_value) {

		assert srcBlockSize.length == dstBlockSize.length : "Dimensions do not match.";

		final int n = srcBlockSize.length;

		if (nBytes != 0) {
			int[] zero = new int[n];
			int[] srcSize = new int[n];
			int[] dstSize = new int[n];
			int[] size = new int[n];
			Arrays.setAll(srcSize, d -> srcBlockSize[d] * (d == 0 ? nBytes : 1));
			Arrays.setAll(dstSize, d -> dstBlockSize[d] * (d == 0 ? nBytes : 1));
			Arrays.setAll(size, d -> Math.min(srcSize[d], dstSize[d]));
			final byte[] dst = new byte[safeInt(Intervals.numElements(dstSize))];
			if (!Arrays.equals(dstSize, size)) {
				fill(dst, fill_value);
			}
			SubArrayCopy.copy(src, srcSize, zero, dst, dstSize, zero, size);
			return dst;
		} else {
			/* TODO deal with bit streams */
			return null;
		}
	}

	private static void fill(final byte[] dst, final byte[] fill_value) {
		byte allZero = 0;
		for (byte b : fill_value) {
			allZero |= b;
		}
		if (allZero != 0) {
			if (fill_value.length == 1) {
				Arrays.fill(dst, fill_value[0]);
			} else {
				for (int i = 0; i < dst.length; i++) {
					dst[i] = fill_value[i % fill_value.length];
				}
			}
		}
	}

	protected void redirectDatasetAttribute(
			final JsonObject src,
			final String key,
			final ZarrJsonElements zarrElems,
			final ZarrDatasetAttributes dsetAttrs) {

		redirectDatasetAttribute(src, key, zarrElems, key, dsetAttrs);
	}

	protected static void redirectDatasetAttribute(
			final JsonObject src,
			final String srcKey,
			final JsonObject dest,
			final String destKey) {

		if (src.has(srcKey)) {
			final JsonElement e = src.get(srcKey);
			dest.add(destKey, e);
			src.remove(srcKey);
		}
	}

	protected void redirectDatasetAttribute(
			final JsonObject src,
			final String srcKey,
			final ZarrJsonElements zarrElems,
			final String destKey,
			final ZarrDatasetAttributes dsetAttrs) {

		if (src.has(srcKey)) {
			if (dsetAttrs != null) {
				final JsonElement e = src.get(srcKey);
				if (e.isJsonArray() && dsetAttrs.isRowMajor())
					reverse(e.getAsJsonArray());

				zarrElems.getOrMakeZarray().add(destKey, e);
				src.remove(srcKey);
			}
		}
	}

	protected void redirectGroupDatasetAttribute(
			final JsonObject src,
			final String srcKey,
			final ZarrJsonElements zarrElems,
			final String destKey,
			final ZarrDatasetAttributes dsetAttrs) {

		if (src.has(srcKey)) {
			if (dsetAttrs != null) {
				final JsonElement e = src.get(srcKey);
				if (e.isJsonArray() && dsetAttrs.isRowMajor())
					reverse(e.getAsJsonArray());

				zarrElems.getOrMakeZarray().add(destKey, e);
				src.remove(srcKey);
			}
		}
	}

	protected static void redirectDataType(final JsonObject src, final JsonObject dest) {

		if (src.has(DatasetAttributes.DATA_TYPE_KEY)) {
			final JsonElement e = src.get(DatasetAttributes.DATA_TYPE_KEY);
			dest.addProperty(ZArrayAttributes.dTypeKey, new DType(DataType.fromString(e.getAsString())).toString());
			src.remove(DatasetAttributes.DATA_TYPE_KEY);
		}
	}

	protected static void redirectCompression(final JsonObject src, final Gson gson, final JsonObject dest) {

		if (src.has(DatasetAttributes.COMPRESSION_KEY)) {
			final Compression c = gson.fromJson(src.get(DatasetAttributes.COMPRESSION_KEY), Compression.class);
			if( c.getClass() == RawCompression.class)
				dest.add(ZArrayAttributes.compressorKey, JsonNull.INSTANCE);
			else
				dest.add(ZArrayAttributes.compressorKey, gson.toJsonTree(ZarrCompressor.fromCompression(c)));

			src.remove(DatasetAttributes.COMPRESSION_KEY);
		}
	}

	protected static ZarrJsonElements build(final JsonObject obj, final Gson gson ) {

		return build(obj, gson, true);
	}

	/**
	 * Creates a {@link ZarrJsonElements} object from a {@link JsonObject} containing attributes.
	 * <p>
	 * Used when re-directing attributes to the appropriate zarr metadata files according to
	 * attribute keys.
	 *
	 * @param obj the json attributes object
	 * @param gson the json
	 * @param mapN5Attributes if true, map n5 attribute keys to corresponding zarr attribute keys
	 * @return
	 */
	protected static ZarrJsonElements build(final JsonObject obj, final Gson gson, final boolean mapN5Attributes) {

		// first map n5 attributes
		if (mapN5Attributes) {
			redirectDatasetAttribute(obj, DatasetAttributes.DIMENSIONS_KEY, obj, ZArrayAttributes.shapeKey);
			redirectDatasetAttribute(obj, DatasetAttributes.BLOCK_SIZE_KEY, obj, ZArrayAttributes.chunksKey);
			redirectDataType( obj, obj );
			redirectCompression(obj, gson, obj);
		}

		// put relevant attributes in appropriate JsonElements
		final ZarrJsonElements zje = new ZarrJsonElements();
		// make the zarray
		if (hasRequiredArrayKeys(obj)) {
			move(obj, () -> zje.getOrMakeZarray(), ZArrayAttributes.allKeys);
			reverseAttrsWhenCOrder( zje.zarray );
		} else if (obj.has(ZArrayAttributes.zarrFormatKey)) {
			// put format key in zgroup
			move(obj, () -> zje.getOrMakeZgroup(), ZArrayAttributes.zarrFormatKey);
		}

		// whatever remains goes into zattrs
		zje.zattrs = obj;

		return zje;
	}

	protected static boolean hasRequiredArrayKeys(final JsonObject obj) {

		return obj.has(ZArrayAttributes.shapeKey) && obj.has(ZArrayAttributes.chunksKey)
				&& obj.has(ZArrayAttributes.dTypeKey) && obj.has(ZArrayAttributes.compressorKey)
				&& obj.has(ZArrayAttributes.fillValueKey) && obj.has(ZArrayAttributes.orderKey)
				&& obj.has(ZArrayAttributes.zarrFormatKey) && obj.has(ZArrayAttributes.filtersKey);
	}

	protected static void move(final JsonObject src, final Supplier<JsonObject> dstSup, final String... keys) {

		for (final String key : keys) {
			if (src.has(key)) {
				final JsonObject dst = dstSup.get();
				dst.add(key, src.get(key));
				src.remove(key);
			}
		}
	}

	/**
	 * Stores {@link JsonObject}s to be written to .zattrs, .zgroup, and .zarray.
	 * <p>
	 * Used when re-directing attributes to these files by the attribute key name.
	 */
	protected static class ZarrJsonElements {

		public JsonObject zattrs;
		public JsonObject zgroup;
		public JsonObject zarray;

		public ZarrJsonElements() {}

		public JsonObject getOrMakeZarray() {

			if (zarray == null)
				zarray = new JsonObject();

			return zarray;
		}

		public JsonObject getOrMakeZattrs() {

			if (zattrs == null)
				zattrs = new JsonObject();

			return zattrs;
		}

		public JsonObject getOrMakeZgroup() {

			if (zgroup == null)
				zgroup = new JsonObject();

			return zgroup;
		}

	}

}
