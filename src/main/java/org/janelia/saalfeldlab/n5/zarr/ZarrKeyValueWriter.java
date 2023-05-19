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
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.janelia.saalfeldlab.n5.BlockWriter;
import org.janelia.saalfeldlab.n5.ByteArrayDataBlock;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.GsonUtils;
import org.janelia.saalfeldlab.n5.KeyValueAccess;
import org.janelia.saalfeldlab.n5.LockedChannel;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.N5URL;
import org.janelia.saalfeldlab.n5.N5Writer;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

import net.imglib2.Cursor;
import net.imglib2.FinalInterval;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.basictypeaccess.array.ByteArray;
import net.imglib2.type.numeric.integer.ByteType;
import net.imglib2.util.Intervals;
import net.imglib2.view.Views;

/**
 * Zarr {@link KeyValueWriter} implementation.
 *
 * @author Stephan Saalfeld
 * @author John Bogovic
 */
public class ZarrKeyValueWriter extends ZarrKeyValueReader implements N5Writer {

	protected String dimensionSeparator;

	protected boolean mapN5DatasetAttributes;

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
	 * @param basePath n5 base path
	 * @param gsonBuilder
	 * @param cacheAttributes cache attributes
	 *    Setting this to true avoids frequent reading and parsing of JSON
	 *    encoded attributes, this is most interesting for high latency file
	 *    systems. Changes of attributes by an independent writer will not be
	 *    tracked.
	 *
	 * @throws IOException
	 *    if the base path cannot be written to or cannot becreated.
	 */
	public ZarrKeyValueWriter(
			final KeyValueAccess keyValueAccess,
			final String basePath,
			final GsonBuilder gsonBuilder,
			final boolean mapN5DatasetAttributes,
			final boolean mergeAttributes,
			final String dimensionSeparator,
			final boolean cacheAttributes) throws IOException {

		super( false, keyValueAccess, basePath, gsonBuilder,
				mapN5DatasetAttributes, mergeAttributes, cacheAttributes );
		this.dimensionSeparator = dimensionSeparator;
		this.mapN5DatasetAttributes = mapN5DatasetAttributes;
		if (exists("/")) {
			final Version version = getVersion();
			if (!ZarrKeyValueReader.VERSION.isCompatible(version))
				throw new IOException(
						"Incompatible version " + version + " (this is " + ZarrKeyValueReader.VERSION + ").");
		} else {
			createGroup("/");
			setVersion("/");
		}
	}

	protected void setVersion(final String path) throws IOException {

		setVersion( path, false );
	}

	protected void setVersion(final String path, final boolean create) throws IOException {

		final JsonObject versionObject = new JsonObject();
		versionObject.add(ZARR_FORMAT_KEY, new JsonPrimitive(N5ZarrReader.VERSION.getMajor()));

		final String normalPath = N5URL.normalizeGroupPath(path);
		if( create || groupExists(normalPath))
			writeZGroup(normalPath, versionObject); // updates cache
		else if( datasetExists(normalPath))
		{
			final JsonObject zarrayJson = getZArray(normalPath).getAsJsonObject();
			zarrayJson.add(ZARR_FORMAT_KEY, new JsonPrimitive( N5ZarrReader.VERSION.getMajor()));
			writeZArray(normalPath, zarrayJson); // updates cache
		}
	}

	/**
	 * Creates a .zgroup file containing the zarr_format at the given path
	 * and all parent paths.
	 *
	 * @param normalPath
	 */
	protected void initializeGroup(final String normalPath) {

		final String[] components = keyValueAccess.components(normalPath);
		String path = "";
		try {
			for (int i = 0; i < components.length; i++) {
				path = keyValueAccess.compose(path, components[i]);
				setVersion(path, true);
			}
		} catch (IOException e) { }
	}

	@Override
	public void createGroup(final String path) throws IOException {

		// Overridden to change the cache key, though it may not be necessary
		// since the contents is null
		final String normalPath = N5URL.normalizeGroupPath(path);
		keyValueAccess.createDirectories(groupPath(normalPath));
		if (cacheMeta()) {
			final String[] pathParts = keyValueAccess.components(normalPath);
			if (pathParts.length > 1) {
				String parent = N5URL.normalizeGroupPath("/");
				for (String child : pathParts) {
					final String childPath = keyValueAccess.compose(parent, child);
					// add the group to the cache
					getCache().addNewCacheInfo(childPath, zgroupFile, null, true, false);
					getCache().addChild(parent, child);
					parent = childPath;
				}
			}
		}
		initializeGroup(normalPath); // caches the contents of .zgroup
	}

	@Override
	public void createDataset(
			final String path,
			final DatasetAttributes datasetAttributes) throws IOException {

		// Overriding because CachedGsonKeyValueWriter calls createGroup.
		// Not correct for zarr, since groups and datasets are mutually exclusive
		final String normalPath = N5URL.normalizeGroupPath(path);
		final String absPath = groupPath(normalPath);
		keyValueAccess.createDirectories(absPath);

		// create parent group
		final String[] pathParts = keyValueAccess.components(normalPath);
		final String parent = Arrays.stream( pathParts ).limit( pathParts.length - 1 ).collect(Collectors.joining("/"));
		createGroup( parent );

		// These three lines are preferable to setDatasetAttributes because they
		// are more efficient wrt caching
		final ZArrayAttributes zarray = createZArrayAttributes(datasetAttributes);
		final JsonElement attributes = gson.toJsonTree(zarray.asMap());
		writeJsonResource(zArrayPath(normalPath), attributes);
		if (cacheMeta()) {
			// cache dataset and add as child to parent
			getCache().addNewCacheInfo(normalPath, zarrayFile, attributes, false, true);
			getCache().addChild(parent, pathParts[pathParts.length - 1]);
		}
	}


	@Override
	public void setAttributes(
			final String path,
			final Map<String, ?> attributes) throws IOException {

		final String normalPath = N5URL.normalizeGroupPath(path);
		if (!exists(normalPath))
			throw new N5Exception.N5IOException("" + normalPath + " is not a group or dataset.");

		// cache here or in writeAttributes?
		// I think in writeAttributes is better - let it delegate to writeZArray, writeZAttrs, writeZGroup
		final JsonElement existingAttributes = getAttributes(normalPath); // uses cache
		JsonElement newAttributes = existingAttributes != null && existingAttributes.isJsonObject() ? existingAttributes.getAsJsonObject() : new JsonObject();
		newAttributes = GsonUtils.insertAttributes(newAttributes, attributes, gson);

		if( newAttributes.isJsonObject() ) {
			final ZarrJsonElements zje = build(newAttributes.getAsJsonObject());
			// the three methods below handle caching
			writeZArray(normalPath, zje.zarray);
			writeZAttrs(normalPath, zje.zattrs);
			writeZGroup(normalPath, zje.zgroup);
		}
		else {
			writeZAttrs(normalPath, newAttributes);
		}
	}

	@Override
	public boolean removeAttribute(final String pathName, final String key) throws IOException {

		final String normalPath = N5URL.normalizeGroupPath(pathName);
		final String normalAttrPathName = zAttrsPath(normalPath);
		final String absoluteNormalPath = keyValueAccess.compose(basePath, normalAttrPathName);
		final String normalKey = N5URL.normalizeAttributePath(key);
		if (!keyValueAccess.exists(absoluteNormalPath))
			return false;

		if (key.equals("/")) {
			writeJsonResource(normalAttrPathName, JsonNull.INSTANCE);
			if (cacheMeta())
				cache.updateCacheInfo( absoluteNormalPath, zattrsFile, JsonNull.INSTANCE);

			return true;
		}

		final JsonElement attributes = getZAttributes(normalPath); // uses cache
		if (GsonUtils.removeAttribute(attributes, normalKey) != null) {

			if (cacheMeta())
				cache.updateCacheInfo( absoluteNormalPath, zattrsFile, attributes);

			writeJsonResource(normalAttrPathName, attributes);
			return true;
		}
		return false;
	}

	@Override
	public <T> T removeAttribute(final String pathName, final String key, final Class<T> cls) throws IOException {

		final String normalPath = N5URL.normalizeGroupPath(pathName);
		final String normalAttrPathName = zAttrsPath(normalPath );
		final String normalKey = N5URL.normalizeAttributePath(key);

		final JsonElement attributes = getZAttributes(normalPath); // uses cache
		final T obj = GsonUtils.removeAttribute(attributes, normalKey, cls, gson);
		if (obj != null) {
			if (cacheMeta())
				cache.updateCacheInfo(normalPath, zattrsFile, attributes);

			writeJsonResource(normalAttrPathName, attributes);
		}
		return obj;
	}

	@Override
	public boolean removeAttributes(final String pathName, final List<String> attributes) throws IOException {

		final String normalPath = N5URL.normalizeGroupPath(pathName);
		boolean removed = false;
		for (final String attribute : attributes) {
			removed |= removeAttribute(normalPath, N5URL.normalizeAttributePath(attribute));
		}
		return removed;
	}

	@Override
	public void setDatasetAttributes(
			final String pathName,
			final DatasetAttributes datasetAttributes) throws IOException {

		setZArrayAttributes(pathName, createZArrayAttributes(datasetAttributes));
	}

	protected ZArrayAttributes createZArrayAttributes(final DatasetAttributes datasetAttributes) {

		final long[] shape = datasetAttributes.getDimensions().clone();
		reorder(shape);
		final int[] chunks = datasetAttributes.getBlockSize().clone();
		reorder(chunks);

		final ZArrayAttributes zArrayAttributes = new ZArrayAttributes(
				N5ZarrReader.VERSION.getMajor(),
				shape,
				chunks,
				new DType(datasetAttributes.getDataType()),
				ZarrCompressor.fromCompression(datasetAttributes.getCompression()),
				"0",
				'C',
				dimensionSeparator,
				null);

		return zArrayAttributes;
	}

	public void setZArrayAttributes( final String pathName, final ZArrayAttributes attributes) throws IOException {

		writeZArray(pathName, gson.toJsonTree(attributes.asMap()));
	}

	protected void setZGroup( final String pathName, final Version version ) throws IOException {

		// used in testVersion 
		writeZGroup(pathName, gson.toJsonTree(version));
	}

	protected void writeJsonResource(final String normalResourcePath, final JsonElement attributes) throws IOException {

		if (attributes == null)
			return;
		final String absolutePath = keyValueAccess.compose(basePath, normalResourcePath);
		try (final LockedChannel lock = keyValueAccess.lockForWriting(absolutePath)) {
			GsonUtils.writeAttributes(lock.newWriter(), attributes, gson);
		}
	}

	protected void writeZArray(
			final String normalGroupPath,
			final JsonElement attributes) throws IOException {

		writeJsonResource(zArrayPath(normalGroupPath), attributes);
		if (cacheMeta()) {
			cache.updateCacheInfo(normalGroupPath, zarrayFile, attributes);
		}
	}

	protected void writeZGroup(
			final String normalGroupPath,
			final JsonElement attributes) throws IOException {

		writeJsonResource(zGroupPath(normalGroupPath), attributes);
		if (cacheMeta()) {
			cache.updateCacheInfo(normalGroupPath, zgroupFile);
		}
	}

	protected void writeZAttrs(
			final String normalGroupPath,
			final JsonElement attributes) throws IOException {

		writeJsonResource(zAttrsPath(normalGroupPath), attributes);
		if (cacheMeta()) {
			cache.updateCacheInfo(normalGroupPath, zattrsFile);
		}
	}

	/**
	 * Writes a {@link DataBlock} into an {@link OutputStream}.
	 *
	 * @param out
	 * @param datasetAttributes
	 * @param dataBlock
	 * @throws IOException
	 */
	public static <T> void writeBlock(
			final OutputStream out,
			final ZarrDatasetAttributes datasetAttributes,
			final DataBlock<T> dataBlock) throws IOException {

		final int[] blockSize = datasetAttributes.getBlockSize();
		final DType dType = datasetAttributes.getDType();
		final BlockWriter writer = datasetAttributes.getCompression().getWriter();

		if (!Arrays.equals(blockSize, dataBlock.getSize())) {

			final byte[] padCropped = padCrop(
					dataBlock.toByteBuffer().array(),
					dataBlock.getSize(),
					blockSize,
					dType.getNBytes(),
					dType.getNBits(),
					datasetAttributes.getFillBytes());

			final DataBlock<byte[]> padCroppedDataBlock =
					new ByteArrayDataBlock(
							blockSize,
							dataBlock.getGridPosition(),
							padCropped);

			writer.write(padCroppedDataBlock, out);

		} else {

			writer.write(dataBlock, out);
		}
	}

	@Override
	public <T> void writeBlock(
			final String pathName,
			final DatasetAttributes datasetAttributes,
			final DataBlock<T> dataBlock) throws IOException {

		final ZarrDatasetAttributes zarrDatasetAttributes;
		if (datasetAttributes instanceof ZarrDatasetAttributes)
			zarrDatasetAttributes = (ZarrDatasetAttributes)datasetAttributes;
		else
			zarrDatasetAttributes = getDatasetAttributes(pathName); // TODO is this correct?

		final String normalPath = N5URL.normalizePath(pathName);
		final String path = keyValueAccess.compose(
				basePath,
				normalPath,
				getZarrDataBlockPath(
						dataBlock.getGridPosition(),
						zarrDatasetAttributes.getDimensionSeparator(),
						zarrDatasetAttributes.isRowMajor()).toString());

		final String[] components = keyValueAccess.components(path);
		final String parent = keyValueAccess.compose( Arrays.stream( components ).limit( components.length - 1 ).toArray( String[]::new ));
		keyValueAccess.createDirectories( parent );
		try ( final LockedChannel lockedChannel = keyValueAccess.lockForWriting(path)){

			writeBlock(
					lockedChannel.newOutputStream(),
					zarrDatasetAttributes,
					dataBlock);
		}
	}

	@Override
	public boolean remove(final String path) throws IOException {

		final String normalGroupPath = N5URL.normalizeGroupPath(path);
		if (cacheMeta()) {
			final String[] parts = keyValueAccess.components(normalGroupPath);
			final String parentPath = keyValueAccess
					.compose(Arrays.stream(parts).limit(parts.length - 1).toArray(String[]::new));
			cache.removeCache(parentPath, normalGroupPath);
		}

		final String normalAbsolutePath = groupPath(normalGroupPath);
		// TODO check existence first? should not matter.
		keyValueAccess.delete(normalAbsolutePath);

		// an IOException should have occurred if anything had failed midway
		return true;
	}

	@Override
	public boolean deleteBlock(
			final String path,
			final long... gridPosition) throws IOException {

		final String normPath = N5URL.normalizePath(path);
		ZarrDatasetAttributes zarrDatasetAttributes = getDatasetAttributes(normPath);
		final String absolutePath = keyValueAccess.compose(basePath, normPath,
				ZarrKeyValueReader.getZarrDataBlockPath(gridPosition,
						zarrDatasetAttributes.getDimensionSeparator(),
						zarrDatasetAttributes.isRowMajor()));

		if (keyValueAccess.exists(absolutePath))
			keyValueAccess.delete(absolutePath);

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
			final int[] srcStrides = new int[n];
			final int[] dstStrides = new int[n];
			final int[] srcSkip = new int[n];
			final int[] dstSkip  = new int[n];
			srcStrides[0] = dstStrides[0] = nBytes;
			for (int d = 1; d < n; ++d) {
				srcStrides[d] = srcBlockSize[d] * srcBlockSize[d - 1];
				dstStrides[d] = dstBlockSize[d] * dstBlockSize[d - 1];
			}
			for (int d = 0; d < n; ++d) {
				srcSkip[d] = Math.max(1, dstBlockSize[d] - srcBlockSize[d]);
				dstSkip[d] = Math.max(1, srcBlockSize[d] - dstBlockSize[d]);
			}

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

			final byte[] dst = new byte[(int)Intervals.numElements(dstIntervalDimensions)];
			/* fill dst */
			for (int i = 0, j = 0; i < n; ++i) {
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

	protected void redirectDatasetAttribute(JsonObject src, String key, ZarrJsonElements zarrElems, ZarrDatasetAttributes dsetAttrs ) {

		redirectDatasetAttribute( src, key, zarrElems, key, dsetAttrs );
	}

	protected static void redirectDatasetAttribute(JsonObject src, String srcKey, JsonObject dest, String destKey) {

		if (src.has(srcKey)) {
			final JsonElement e = src.get(srcKey);
			dest.add(destKey, e);
			src.remove(srcKey);
		}
	}

	protected void redirectDatasetAttribute(JsonObject src, String srcKey, ZarrJsonElements zarrElems, String destKey, ZarrDatasetAttributes dsetAttrs) {

		if (src.has(srcKey)) {
			if (dsetAttrs != null) {
				JsonElement e = src.get(srcKey);
				if (e.isJsonArray() && dsetAttrs.isRowMajor())
					reorder(e.getAsJsonArray());

				zarrElems.getOrMakeZarray().add(destKey, e);
				src.remove(srcKey);
			}
		}
	}

	protected void redirectGroupDatasetAttribute(JsonObject src, String srcKey, ZarrJsonElements zarrElems, String destKey, ZarrDatasetAttributes dsetAttrs ) {

		if (src.has(srcKey)) {
			if (dsetAttrs != null ) {
				JsonElement e = src.get(srcKey);
				if( e.isJsonArray() && dsetAttrs.isRowMajor() )
					reorder(e.getAsJsonArray());

				zarrElems.getOrMakeZarray().add(destKey, e );
				src.remove(srcKey);
			}
		}
	}

	protected static ZarrJsonElements build(JsonObject obj) {

		return build(obj, true);
	}

	protected static ZarrJsonElements build(JsonObject obj, boolean mapN5Attributes) {

		// first map n5 attributes
		if (mapN5Attributes) {
			redirectDatasetAttribute(obj, "dimensions", obj, ZArrayAttributes.shapeKey );
			redirectDatasetAttribute(obj, "blockSize", obj, ZArrayAttributes.chunksKey );
			redirectDatasetAttribute(obj, "dataType", obj, ZArrayAttributes.dTypeKey );
			redirectDatasetAttribute(obj, "compression", obj, ZArrayAttributes.compressorKey );
		}

		// put relevant attributes in appropriate JsonElements
		final ZarrJsonElements zje = new ZarrJsonElements();
		// make the zarray
		if (hasRequiredArrayKeys(obj)) {
			move(obj, () -> zje.getOrMakeZarray(), ZArrayAttributes.allKeys );
		}
		else if( obj.has( ZArrayAttributes.zarrFormatKey )) {
			// put format key in zgroup
			move(obj, () -> zje.getOrMakeZgroup(), ZArrayAttributes.zarrFormatKey );
		}

		// whatever remains goes into zattrs
		zje.zattrs = obj;

		return zje;
	}

	protected static boolean hasRequiredArrayKeys( final JsonObject obj )
	{
		return obj.has(ZArrayAttributes.shapeKey) && obj.has(ZArrayAttributes.chunksKey)
				&& obj.has(ZArrayAttributes.dTypeKey) && obj.has(ZArrayAttributes.compressorKey)
				&& obj.has(ZArrayAttributes.fillValueKey) && obj.has(ZArrayAttributes.orderKey)
				&& obj.has(ZArrayAttributes.zarrFormatKey) && obj.has(ZArrayAttributes.filtersKey);					
	}

	protected static void move(final JsonObject src, final Supplier<JsonObject> dstSup, final String... keys) {
		for( String key : keys )
		{
			if (src.has(key)) {
				final JsonObject dst = dstSup.get();
				dst.add(key, src.get(key));
				src.remove(key);
			}
		}
	}

	protected static class ZarrJsonElements {
		public JsonObject zattrs;
		public JsonObject zgroup;
		public JsonObject zarray;

		public ZarrJsonElements() { }

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

	static void reorder(final long[] array) {

		long a;
		final int max = array.length - 1;
		for (int i = (max - 1) / 2; i >= 0; --i) {
			final int j = max - i;
			a = array[i];
			array[i] = array[j];
			array[j] = a;
		}
	}

	static void reorder(final int[] array) {

		int a;
		final int max = array.length - 1;
		for (int i = (max - 1) / 2; i >= 0; --i) {
			final int j = max - i;
			a = array[i];
			array[i] = array[j];
			array[j] = a;
		}
	}

	static void reorder(final JsonArray array) {

		JsonElement a;
		final int max = array.size() - 1;
		for (int i = (max - 1) / 2; i >= 0; --i) {
			final int j = max - i;
			a = array.get(i);
			array.set(i, array.get(j));
			array.set(j, a );
		}
	}

}
