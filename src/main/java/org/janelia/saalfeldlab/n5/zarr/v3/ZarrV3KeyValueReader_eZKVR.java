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
package org.janelia.saalfeldlab.n5.zarr.v3;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.commons.compress.utils.IOUtils;
import org.janelia.saalfeldlab.n5.BlockReader;
import org.janelia.saalfeldlab.n5.ByteArrayDataBlock;
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.CompressionAdapter;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.DefaultBlockReader;
import org.janelia.saalfeldlab.n5.GsonUtils;
import org.janelia.saalfeldlab.n5.KeyValueAccess;
import org.janelia.saalfeldlab.n5.LockedChannel;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.N5URI;
import org.janelia.saalfeldlab.n5.RawCompression;
import org.janelia.saalfeldlab.n5.blosc.BloscCompression;
import org.janelia.saalfeldlab.n5.zarr.DType;
import org.janelia.saalfeldlab.n5.zarr.Filter;
import org.janelia.saalfeldlab.n5.zarr.ZArrayAttributes;
import org.janelia.saalfeldlab.n5.zarr.ZarrCompressor;
import org.janelia.saalfeldlab.n5.zarr.ZarrDatasetAttributes;
import org.janelia.saalfeldlab.n5.zarr.ZarrKeyValueReader;
import org.janelia.saalfeldlab.n5.zarr.ZarrStringDataBlock;
import org.janelia.saalfeldlab.n5.zarr.cache.ZarrJsonCache;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;

public class ZarrV3KeyValueReader_eZKVR extends ZarrKeyValueReader {

	// Override this constant
	// if we try supporting v2 and v3 in parallel
	public static final Version VERSION = new Version(3, 0, 0);

	public static final String ZARR_KEY = "zarr.json";

	/**
	 * Opens an {@link ZarrV3KeyValueReader_eZKVR} at a given base path with a custom
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
	public ZarrV3KeyValueReader_eZKVR(
			final boolean checkVersion,
			final KeyValueAccess keyValueAccess,
			final String basePath,
			final GsonBuilder gsonBuilder,
			final boolean mapN5DatasetAttributes,
			final boolean mergeAttributes,
			final boolean cacheMeta)
			throws N5Exception {

		this(checkVersion, keyValueAccess, basePath, gsonBuilder, mapN5DatasetAttributes, mergeAttributes, cacheMeta, true);
	}

	protected ZarrV3KeyValueReader_eZKVR(
			final boolean checkVersion,
			final KeyValueAccess keyValueAccess,
			final String basePath,
			final GsonBuilder gsonBuilder,
			final boolean mapN5DatasetAttributes,
			final boolean mergeAttributes,
			final boolean cacheMeta,
			final boolean checkRootExists) {

		super(checkVersion, keyValueAccess, basePath, gsonBuilder, mapN5DatasetAttributes,
				mergeAttributes, cacheMeta, checkRootExists);

	}

	/**
	 * Opens an {@link ZarrV3KeyValueReader_eZKVR} at a given base path with a custom
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
	public ZarrV3KeyValueReader_eZKVR(
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

		final JsonElement json = getJsonResource("", ZARR_KEY);
		if (json == null || !json.isJsonObject()) {
			return VERSION_ZERO;
		}

		final JsonElement fmt = json.getAsJsonObject().get(ZARR_FORMAT_KEY);
		if (fmt.isJsonPrimitive())
			return new Version(fmt.getAsInt(), 0, 0);

		return VERSION;
	}

	/**
	 * Tests whether the key at normalPathName exists for the backend. Such a key may exist for paths that are neither
	 * zarr groups nor zarr datasets.
	 *
	 * @param normalPathName
	 *            normalized path name
	 * @return true if the key exists
	 */
	@Override
	public boolean existsFromContainer(final String normalPathName) {

		// A cloud keyValueAccess may return false for
		// exists(groupPath(normalPathName)),
		// so instead need to check for existence of either .zarray or .zgroup
		return keyValueAccess.exists(keyValueAccess.compose(uri, normalPathName, ZARR_KEY));
	}

	@Override
	public boolean groupExists(final String pathName) {

		// Overriden because the parent implementation uses attributes.json,
		// this uses .zgroup.
		final String normalPath = N5URI.normalizeGroupPath(pathName);
		if (cacheMeta()) {
			return cache.isGroup(normalPath, ZARR_KEY);
		}
		return isGroupFromContainer(normalPath);
	}

	@Override
	public boolean isGroupFromContainer(final String normalPath) {

		return keyValueAccess.isFile(keyValueAccess.compose(uri, normalPath, ZARR_KEY));
	}

	@Override
	public boolean isGroupFromAttributes(final String normalCacheKey, final JsonElement attributes) {

		return attributes != null && attributes.isJsonObject() && attributes.getAsJsonObject().has(ZARR_FORMAT_KEY);
	}

	@Override
	public boolean datasetExists(final String pathName) throws N5Exception.N5IOException {

		if (cacheMeta()) {
			final String normalPathName = N5URI.normalizeGroupPath(pathName);
			return cache.isDataset(normalPathName, ZARR_KEY);
		}
		return isDatasetFromContainer(pathName);
	}

	@Override
	public boolean isDatasetFromContainer(final String normalPathName) throws N5Exception {

		if (keyValueAccess.isFile(keyValueAccess.compose(uri, normalPathName, ZARR_KEY))) {
			return isDatasetFromAttributes(ZARR_KEY, getAttributesFromContainer(normalPathName, ZARR_KEY));
		} else {
			return false;
		}
	}

	@Override
	public boolean isDatasetFromAttributes(final String normalCacheKey, final JsonElement attributes) {

		if (normalCacheKey.equals(ZARR_KEY) && attributes != null && attributes.isJsonObject()) {
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
	public DatasetAttributes getDatasetAttributes(final String pathName) throws N5Exception {

		return createDatasetAttributes(
				getJsonResource(N5URI.normalizeGroupPath(pathName), ZARR_KEY));
	}

	@Override
	public ZarrV3DatasetAttributes createDatasetAttributes(final JsonElement attributes) {

		return gson.fromJson(attributes, ZarrV3DatasetAttributes.class);
	}

	@Override
	protected JsonElement zarrToN5DatasetAttributes(final JsonElement elem ) {

		// TODO this may need change depending on what getZArrayAttributes does
		if (!mapN5DatasetAttributes || elem == null || !elem.isJsonObject())
			return elem;

		final JsonObject attrs = elem.getAsJsonObject();
		final ZArrayAttributes zattrs = getZArrayAttributes(attrs);
		if (zattrs == null)
			return elem;

		attrs.add(DatasetAttributes.DIMENSIONS_KEY, attrs.get(ZArrayAttributes.shapeKey));
		attrs.add(DatasetAttributes.BLOCK_SIZE_KEY, attrs.get(ZArrayAttributes.chunksKey));
		attrs.addProperty(DatasetAttributes.DATA_TYPE_KEY, zattrs.getDType().getDataType().toString());

		final JsonElement e = attrs.get(ZArrayAttributes.compressorKey);
		if (e == JsonNull.INSTANCE || e == null) {
			attrs.add(DatasetAttributes.COMPRESSION_KEY, gson.toJsonTree(new RawCompression()));
		} else {
			attrs.add(DatasetAttributes.COMPRESSION_KEY, gson.toJsonTree(
					gson.fromJson(attrs.get(ZArrayAttributes.compressorKey), ZarrCompressor.class).getCompression()));
		}
		return attrs;
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

	@Override
	protected JsonElement getAttributesUnmapped(final String path) throws N5Exception {

		JsonElement out;
		if (mergeAttributes) {

			out = combineAll(
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

		if (!keyValueAccess.isFile(absolutePath))
			return null;

		try (final LockedChannel lockedChannel = keyValueAccess.lockForReading(absolutePath)) {
			return GsonUtils.readAttributes(lockedChannel.newReader(), gson);
		} catch (final IOException | UncheckedIOException e) {
			throw new N5IOException("Failed to read " + absolutePath, e);
		}
	}

	@Override
	public DataBlock<?> readBlock(
			final String pathName,
			final DatasetAttributes datasetAttributes,
			final long... gridPosition) throws N5Exception {

		final ZarrV3DatasetAttributes zarrDatasetAttributes;
		if (datasetAttributes instanceof ZarrDatasetAttributes)
			zarrDatasetAttributes = (ZarrV3DatasetAttributes)datasetAttributes;
		else
			zarrDatasetAttributes = (ZarrV3DatasetAttributes)getDatasetAttributes(pathName);

		final String absolutePath = keyValueAccess.compose(uri, pathName,
				zarrDatasetAttributes.chunkKeyEncoding.getChunkPath(gridPosition));

		try (final LockedChannel lockedChannel = keyValueAccess.lockForReading(absolutePath)) {
			return readBlock(lockedChannel.newInputStream(), zarrDatasetAttributes, gridPosition);
		} catch (final N5Exception.N5NoSuchKeyException e) {
			return null;
		} catch (final Throwable  e) {
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
			final ZarrV3DatasetAttributes datasetAttributes,
			final long... gridPosition) throws IOException {

		final int[] blockSize = datasetAttributes.getBlockSize();
		final DType dType = datasetAttributes.getDType();

		final ByteArrayDataBlock byteBlock = dType.createByteBlock(blockSize, gridPosition);
		final BlockReader reader = datasetAttributes.getCompression().getReader();

		if (dType.getDataType() == DataType.STRING) {
			return readVLenStringBlock(in, reader, byteBlock);
		}

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

	private static ZarrStringDataBlock readVLenStringBlock(final InputStream in, final BlockReader reader, final ByteArrayDataBlock byteBlock) throws IOException {
		// read whole chunk and deserialize; this should be improved
		final ZarrStringDataBlock dataBlock = new ZarrStringDataBlock(byteBlock.getSize(), byteBlock.getGridPosition(), new String[0]);
		if (reader instanceof BloscCompression) {
			// Blosc reader reads actual data and doesn't care about buffer size (but needs special treatment in data block)
			reader.read(dataBlock, in);
		} else if (reader instanceof DefaultBlockReader) {
			try (final InputStream inflater = ((DefaultBlockReader) reader).getInputStream(in)) {
				final DataInputStream dis = new DataInputStream(inflater);
				final ByteArrayOutputStream out = new ByteArrayOutputStream();
				IOUtils.copy(dis, out);
				dataBlock.readData(ByteBuffer.wrap(out.toByteArray()));
			}
		}
		else {
			throw new UnsupportedOperationException("Only Blosc compression or algorithms that use DefaultBlockReader are supported.");
		}
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

	static Gson registerGson(final GsonBuilder gsonBuilder) {

		return addTypeAdapters(gsonBuilder).create();
	}

	protected static GsonBuilder addTypeAdapters(final GsonBuilder gsonBuilder) {

		gsonBuilder.registerTypeAdapter(DataType.class, new DataType.JsonAdapter());
		gsonBuilder.registerTypeAdapter(ZarrCompressor.class, ZarrCompressor.jsonAdapter);
		gsonBuilder.registerTypeAdapter(ZarrCompressor.Raw.class, ZarrCompressor.rawNullAdapter);
		gsonBuilder.registerTypeHierarchyAdapter(Compression.class, CompressionAdapter.getJsonAdapter());
		gsonBuilder.registerTypeAdapter(Compression.class, CompressionAdapter.getJsonAdapter());
		// gsonBuilder.registerTypeAdapter(ZArrayV3Attributes2.class, ZArrayV3Attributes2.jsonAdapter);
		gsonBuilder.registerTypeHierarchyAdapter(Filter.class, Filter.jsonAdapter);
		gsonBuilder.disableHtmlEscaping();

		return gsonBuilder;
	}

}
