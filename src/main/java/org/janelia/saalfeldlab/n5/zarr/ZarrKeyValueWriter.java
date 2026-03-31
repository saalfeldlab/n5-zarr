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
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSyntaxException;
import java.util.Map;
import java.util.function.Supplier;
import net.imglib2.Cursor;
import net.imglib2.FinalInterval;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.basictypeaccess.array.ByteArray;
import net.imglib2.type.numeric.integer.ByteType;
import net.imglib2.util.Intervals;
import net.imglib2.view.Views;
import org.janelia.saalfeldlab.n5.CachedGsonKeyValueN5Writer;
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.GsonUtils;
import org.janelia.saalfeldlab.n5.KeyValueAccess;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.N5Path.N5GroupPath;
import org.janelia.saalfeldlab.n5.N5URI;
import org.janelia.saalfeldlab.n5.RawCompression;
import org.janelia.saalfeldlab.n5.RootedKeyValueAccess;

/**
 * Zarr {@link KeyValueAccess} implementation.
 *
 * @author Stephan Saalfeld
 * @author John Bogovic
 */
public class ZarrKeyValueWriter extends ZarrKeyValueReader implements CachedGsonKeyValueN5Writer {

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
			final RootedKeyValueAccess keyValueAccess,
			final GsonBuilder gsonBuilder,
			final boolean mapN5DatasetAttributes,
			final boolean mergeAttributes,
			final String dimensionSeparator,
			final boolean cacheAttributes)
			throws N5Exception {

		super(
				false,
				keyValueAccess,
				gsonBuilder,
				mapN5DatasetAttributes,
				mergeAttributes,
				cacheAttributes,
				false);

		validateDimensionSeparator(dimensionSeparator);
		this.dimensionSeparator = dimensionSeparator;

		Version version = null;
		if (exists("/")) {
			version = getVersion();
			if (!isCompatible(version))
				throw new N5IOException(
						"Incompatible version " + version + " (this is " + ZarrKeyValueReader.VERSION + ").");
		}

		if (version == null || version == VERSION_ZERO) {
			createGroup("/");
			setVersion("/");
		}
	}

	private void validateDimensionSeparator(final String dimSep) {

		if (!(dimSep.equals(".") || dimSep.equals("/"))) {
			throw new N5Exception("Invalid dimension_separator.\n" +
					"Must be \".\" or \"/\", but found: \""
					+ dimSep + "\"");
		}
	}

	public boolean isCompatible(Version version) {

		return ZarrKeyValueReader.VERSION.isCompatible(version);
	}

	// TODO [+]
	@Override
	public void setVersion(final String path) throws N5Exception {

		if (groupExists(path)) {
			final JsonObject versionObject = new JsonObject();
			versionObject.add(ZARR_FORMAT_KEY, new JsonPrimitive(N5ZarrReader.VERSION.getMajor()));
			writeZGroup(N5GroupPath.of(path), versionObject); // updates cache
		} else if (datasetExists(path)) {
			final JsonObject zarrayJson = getZArray(path).getAsJsonObject();
			zarrayJson.add(ZARR_FORMAT_KEY, new JsonPrimitive(N5ZarrReader.VERSION.getMajor()));
			writeZArray(N5GroupPath.of(path), zarrayJson); // updates cache
		}
	}

	// TODO [+]
	@Override
	public void createGroup(final String path) throws N5Exception {

		// avoid hitting the backend if this path is already a group according to the cache
		// else if exists is true (then a dataset is present) so throw an exception to avoid
		// overwriting / invalidating existing data
		if (groupExists(path)) {
			return;
		} else if (datasetExists(path)) {
			throw new N5Exception("Can't make a group on existing dataset.");
		}


		final JsonObject versionObject = new JsonObject();
		versionObject.add(ZARR_FORMAT_KEY, new JsonPrimitive(N5ZarrReader.VERSION.getMajor()));

		N5GroupPath group = N5GroupPath.of(path);
		getDelegateStore().store_createDirectories(group);
		while (group != null) {
			writeZGroup(group, versionObject);
			group = group.parent();
		}
	}

	// TODO [+]
	@Override
	public ZarrDatasetAttributes createDataset(
			final String path,
			final DatasetAttributes datasetAttributes) throws N5Exception {

		// NB: This method is overridden because the inherited default
		// N5Writer.createDataset() calls createGroup(). Not correct for zarr,
		// since groups and datasets are mutually exclusive.

		final N5GroupPath dataset = N5GroupPath.of(path);
		if (groupExists(path)) {
			// TODO tests currently require that we can make a dataset on a group
//			throw new N5Exception("Can't make a group on existing path.");
			metaStore.store_removeAttributesJson(dataset, ZGROUP_FILE);
		}

		// create parent group
		final N5GroupPath parent = dataset.parent();
		if (parent != null) {
			createGroup(parent.path());
		}

		final ZarrDatasetAttributes zarrDatasetAttributes = getConvertedDatasetAttributes(datasetAttributes);
		final ZArrayAttributes zarray = zarrDatasetAttributes.getZArrayAttributes();
		if (zarray != null)
			writeZArray(dataset, gson.toJsonTree(zarray));
		return zarrDatasetAttributes;
	}

	// TODO [~]
	@Override
	public void setAttributes(
			final String path,
			final Map<String, ?> attributes) throws N5Exception {

		if (!exists(path))
			throw new N5IOException("\"" + path + "\" is not a group or dataset.");

		// cache here or in writeAttributes?
		// I think in writeAttributes is better - let it delegate to
		// writeZArray, writeZAttrs, writeZGroup
		final JsonElement existingAttributes = getAttributesUnmapped(path); // uses cache
		JsonElement newAttributes = existingAttributes != null && existingAttributes.isJsonObject()
				? existingAttributes.getAsJsonObject()
				: new JsonObject();
		newAttributes = GsonUtils.insertAttributes(newAttributes, attributes, gson);

		final N5GroupPath group = N5GroupPath.of(path);
		if (newAttributes.isJsonObject()) {
			final ZarrJsonElements zje = build(newAttributes.getAsJsonObject(), gson);
			// the three methods below handle caching
			writeZArray(group, zje.zarray);
			writeZAttrs(group, zje.zattrs);
			writeZGroup(group, zje.zgroup);
		} else {
			writeZAttrs(group, newAttributes);
		}
	}

	// TODO [+]
	@Override
	public boolean removeAttribute(final String pathName, final String attributePath) throws N5Exception {

		final String normalKey = N5URI.normalizeAttributePath(attributePath);
		final N5GroupPath group = N5GroupPath.of(pathName);
		final JsonElement attributes = readZattrs(group);
		if (attributes != null) {
			if (attributePath.equals("/")) {
				writeZAttrs(group, JsonNull.INSTANCE);
				return true;
			}
			if (GsonUtils.removeAttribute(attributes, normalKey) != null) {
				writeZAttrs(group, attributes);
				return true;
			}
		}
		return false;
	}

	// TODO [+]
	@Override
	public <T> T removeAttribute(final String pathName, final String key, final Class<T> cls) throws N5Exception {

		final String normalKey = N5URI.normalizeAttributePath(key);
		final N5GroupPath group = N5GroupPath.of(pathName);
		final JsonElement attributes = readZattrs(group);
		final T obj;
		try {
			obj = GsonUtils.removeAttribute(attributes, normalKey, cls, getGson());
		} catch (JsonSyntaxException | NumberFormatException | ClassCastException e) {
			throw new N5Exception.N5ClassCastException(e);
		}
		if (obj != null) {
			writeZAttrs(group, attributes);
		}
		return obj;
	}

	// TODO [+]
	@Override
	public void setDatasetAttributes(
			final String pathName,
			final DatasetAttributes datasetAttributes) throws N5Exception {

		final ZarrDatasetAttributes zarrDatasetAttributes = getConvertedDatasetAttributes(datasetAttributes);
		final ZArrayAttributes zArray = zarrDatasetAttributes.getZArrayAttributes();
		setZArrayAttributes(pathName, zArray);
	}

	/**
	 * Write the contents of the attributes argument to the .zarray file at
	 * the given pathName. Overwrites and existing .zarray.
	 *
	 * @param pathName the group / dataset path
	 * @param attributes ZArray attributes
	 * @throws N5Exception the exception
	 */
	// TODO: inline & remove?
	public void setZArrayAttributes(final String pathName, final ZArrayAttributes attributes) throws N5Exception {

		writeZArray(N5GroupPath.of(pathName), gson.toJsonTree(attributes.asMap()));
	}

	// TODO: remove
//	protected void setZGroup(final String pathName, final Version version) throws IOException {
//
//		// used in testVersion
//		writeZGroup(N5GroupPath.of(pathName), gson.toJsonTree(version));
//	}

	// TODO remove
//	private void deleteJsonResource(
//			final N5GroupPath path,
//			final String jsonName) throws N5Exception {
//
//		keyValueAccess.delete(path.resolve(jsonName));
//	}

	// TODO remove?
//	private void writeJson(
//			final N5GroupPath path,
//			final String jsonName,
//			final JsonElement attributes) throws N5Exception {
//
//		if (attributes == null)
//			return;
//
//		keyValueAccess.write(path.resolve(jsonName).asFile(), ReadData.from(attributes.toString().getBytes()));
//	}

	// TODO remove?
//	private void writeJsonResource(
//			final N5GroupPath path,
//			final String jsonName,
//			final Object attributes) throws N5Exception {
//
//		if (attributes == null)
//			return;
//
//		writeJson(path, jsonName, gson.toJsonTree(attributes));
//	}

	// TODO [+]
	private void writeZArray(
			final N5GroupPath groupPath,
			final JsonElement attributes) throws N5Exception {

		if (attributes != null)
			metaStore.store_writeAttributesJson(groupPath, ZARRAY_FILE, attributes, gson);
	}

	// TODO [+]
	protected void writeZGroup(
			final N5GroupPath groupPath,
			final JsonElement attributes) throws N5Exception {

		if (attributes != null)
			metaStore.store_writeAttributesJson(groupPath, ZGROUP_FILE, attributes, gson);
	}

	// TODO [+]
	private void writeZAttrs(
			final N5GroupPath groupPath,
			final JsonElement attributes) throws N5Exception {

		if (attributes != null)
			metaStore.store_writeAttributesJson(groupPath, ZATTRS_FILE, attributes, gson);
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

			/* this is getting hairy, ImgLib2 alternative */
			/* byte images with 0-dimension d[0] * nBytes */
			final long[] srcIntervalDimensions = new long[n];
			final long[] dstIntervalDimensions = new long[n];
			srcIntervalDimensions[0] = srcBlockSize[0] * nBytes;
			dstIntervalDimensions[0] = dstBlockSize[0] * nBytes;
			for (int d = 1; d < n; ++d) {
				srcIntervalDimensions[d] = srcBlockSize[d];
				dstIntervalDimensions[d] = dstBlockSize[d];
			}

			final int numTargetBytes = (int)Intervals.numElements(dstIntervalDimensions);
			final byte[] dst = new byte[numTargetBytes];
			/* fill dst */
			for (int i = 0, j = 0; i < numTargetBytes; ++i) {
				dst[i] = fill_value[j];
				if (++j == fill_value.length)
					j = 0;
			}

			final ArrayImg<ByteType, ByteArray> srcImg = ArrayImgs.bytes(src, srcIntervalDimensions);
			final ArrayImg<ByteType, ByteArray> dstImg = ArrayImgs.bytes(dst, dstIntervalDimensions);

			final FinalInterval intersection = Intervals.intersect(srcImg, dstImg);
			final Cursor<ByteType> srcCursor = Views.interval(srcImg, intersection).cursor();
			final Cursor<ByteType> dstCursor = Views.interval(dstImg, intersection).cursor();
			while (srcCursor.hasNext())
				dstCursor.next().set(srcCursor.next());

			return dst;
		} else {
			/* TODO deal with bit streams */
			return null;
		}
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
	protected static ZarrJsonElements build(final JsonObject obj, final Gson gson, final boolean mapN5Attributes) { // TODO: remove mapN5Attributes argument

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
