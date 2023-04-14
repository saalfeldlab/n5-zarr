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
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.GsonUtils;
import org.janelia.saalfeldlab.n5.KeyValueAccess;
import org.janelia.saalfeldlab.n5.LockedChannel;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5URL;
import org.janelia.saalfeldlab.n5.cache.N5JsonCache;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;

/**
 * {@link N5Reader} implementation through {@link KeyValueAccess} with JSON
 * attributes parsed with {@link Gson}.
 *
 * @author Stephan Saalfeld
 * @author John Bogovic
 */
public class ZarrKeyValueReader implements N5Reader, ZarrUtils {

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

		this.keyValueAccess = keyValueAccess;
		this.basePath = keyValueAccess.normalize(basePath);
		this.gson = ZarrUtils.registerGson(gsonBuilder);
		this.cacheMeta = cacheMeta;
		this.mapN5DatasetAttributes = mapN5DatasetAttributes;
		this.mergeAttributes = mergeAttributes;

		if (cacheMeta)
			// note normalExists isn't quite the normal version of exists.
			// rather, it only checks for the existence of the requested on the backend
			// (this is the desired behavior the cache needs
			cache = new N5JsonCache((groupPath, cacheKey) -> normalReadJsonResource(groupPath,cacheKey),
					this::normalExists, this::normalGroupExists, this::normalDatasetExists, this::normalList);
		else
			cache = null;
	}

	public Gson getGson() {

		return gson;
	}

	public KeyValueAccess getKeyValueAccess() {
		return keyValueAccess;
	}

	@Override
	public String getBasePath() {

		return this.basePath;
	}

	/**
	 * Constructs the absolute path (in terms of this store) for the group or
	 * dataset.
	 *
	 * @param normalGroupPath normalized group path without leading slash
	 * @return the absolute path to the group
	 */
	protected String groupPath(final String normalGroupPath) {

		return keyValueAccess.compose(basePath, normalGroupPath);
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
			return ZarrUtils.getVersion(getZArray("").getAsJsonObject());
		}
		else if( groupExists( "" )) {
			return ZarrUtils.getVersion(getZGroup("").getAsJsonObject());
		}
		return ZarrUtils.VERSION;
	}

	@Override
	public boolean exists(final String pathName) {

		final String normalPathName = N5URL.normalizeGroupPath( pathName );
		// datasetExists and groupExists use the cache
		return datasetExists( normalPathName ) || groupExists( normalPathName );
	}

	private boolean normalExists(String normalPathName) {

		return keyValueAccess.exists(groupPath(normalPathName)) || normalDatasetExists(normalPathName);
	}

	protected boolean groupExists(final String normalPath) {

		if (cacheMeta) {
			return cache.isGroup(normalPath, ZarrUtils.zgroupFile);
		}
		return normalGroupExists(normalPath);
	}

	protected boolean normalGroupExists(final String normalPath) {

		return ZarrUtils.groupExists(keyValueAccess, keyValueAccess.compose(basePath, normalPath ));
	}

	@Override
	public boolean datasetExists(final String pathName) throws N5Exception.N5IOException {

		if (cacheMeta) {
			final String normalPathName = N5URL.normalizeGroupPath(pathName);
			return cache.isDataset(normalPathName, ZarrUtils.zarrayFile);
		}
		return normalDatasetExists(pathName);
	}

	protected boolean normalDatasetExists(String pathName) throws N5Exception.N5IOException {

		return normalReadDatasetAttributes(pathName) != null;
	}

	protected DatasetAttributes normalReadDatasetAttributes(final String pathName) throws N5Exception.N5IOException {

		final String normalPath = N5URL.normalizeGroupPath(zArrayPath(pathName));
		final JsonElement attributes = normalReadJsonResource(normalPath);
		return createDatasetAttributes(attributes);
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
	public ZarrDatasetAttributes getDatasetAttributes(final String pathName) throws IOException {

		final JsonElement attributes = getZArray(pathName); // normalizes and caches

//		final String normalPath = N5URL.normalizeGroupPath(pathName);
//		final JsonElement attributes;
//		if (cacheMeta && cache.isDataset(normalPath)) {
//			attributes = cache.getAttributes(normalPath, ZarrUtils.zarrayFile);
//		} else {
//			attributes = getJsonResource(zArrayPath(normalPath)); // TODO getAttributes uses cache too, use normalGetDatasetAttributes instead
//		}
		return createDatasetAttributes(attributes);
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

	protected ZarrDatasetAttributes createDatasetAttributes(final JsonElement attributes) {

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

		// TODO decide whether to apply cache logic here or one level down (in getJsonResource)
		if (cacheMeta) {
			return cache.getAttributes(N5URL.normalizeGroupPath(path), ZarrUtils.zgroupFile);
		} else {
			return readJsonResource(ZarrUtils.zGroupPath(keyValueAccess, path));
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

		// TODO decide whether to apply cache logic here or one level down (in getJsonResource)
		if (cacheMeta) {
			return cache.getAttributes(N5URL.normalizeGroupPath(path), ZarrUtils.zarrayFile);
		} else {
			return readJsonResource(ZarrUtils.zArrayPath(keyValueAccess, path));
		}
	}

	/**
	 * Returns a {@link JsonElement} representing the contents of .zattrs located at
	 * the given path if present, and null if not.
	 * 
	 * @param path the path
	 * @return the json element
	 * @throws IOException the exception
	 */
	public JsonElement getZAttributes(final String path) throws IOException {

		// TODO decide whether to apply cache logic here or one level down (in getJsonResource)
		if (cacheMeta) {
			return cache.getAttributes(N5URL.normalizeGroupPath(path), ZarrUtils.zattrsFile);
		} else {
			return readJsonResource(ZarrUtils.zAttrsPath(keyValueAccess, path));
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
	 * @throws IOException the exception
	 */
	public JsonElement getAttributes(final String path) throws IOException {

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
			} catch (IOException e) {}

			return ZarrUtils.combineAll( groupElem, arrElem, attrElem );
		}
		else
			return getZAttributes(path);
	}

	/**
	 * Returns a {@link JsonElement} containing the contents of the file located at
	 * the given resource location. This method always reads data from the backing
	 * storage, bypassing the cache, if present.
	 *
	 * @param resourcePath relative path to the resource
	 * @return the json element
	 * @throws N5Exception.N5IOException the exception
	 */
	protected JsonElement readJsonResource(final String resourcePath) throws IOException {

		// TODO decide whether to implement cache at this level or "one level up"
		final String normalResourcePath = N5URL.normalizeGroupPath(resourcePath);
		return normalReadJsonResource(normalResourcePath);
	}

	/**
	 * Returns a {@link JsonElement} containing the contents of the file located at
	 * the given, normal resource location. This method always reads data from the
	 * backing storage, bypassing the cache, if present.
	 *
	 * @param normalResourcePath relative, normal path to the resource
	 * @return the json element
	 * @throws N5Exception.N5IOException the exception
	 */
	protected JsonElement normalReadJsonResource(final String normalResourcePath) throws N5Exception.N5IOException {

		final String absoluteNormalPath = keyValueAccess.compose(basePath, N5URL.normalizeGroupPath(normalResourcePath));
		return ZarrUtils.getJsonResource(keyValueAccess, gson, absoluteNormalPath);
	}

	/**
	 * Returns a {@link JsonElement} containing the contents of the file located at
	 * the given, normal resource location. This method always reads data from the
	 * backing storage, bypassing the cache, if present.
	 *
	 * @param normalResourceParent relative, normal path to the resource's parent
	 * @param resourceName name to the resource relative to its parent
	 * @return the json element
	 * @throws N5Exception.N5IOException the exception
	 */
	protected JsonElement normalReadJsonResource(final String normalResourceParent, final String resourceName) throws N5Exception.N5IOException {

		final String absoluteNormalPath = keyValueAccess.compose(basePath, N5URL.normalizeGroupPath(normalResourceParent), resourceName );
		return ZarrUtils.getJsonResource(keyValueAccess, gson, absoluteNormalPath);
	}

	protected String zArrayAbsolutePath(final String pathName) {

		return ZarrUtils.zArrayAbsolutePath(keyValueAccess, basePath, pathName);
	}

	protected String zAttrsAbsolutePath(final String pathName) {

		return ZarrUtils.zAttrsAbsolutePath(keyValueAccess, basePath, pathName);
	}

	protected String zGroupAbsolutePath(final String pathName) {

		return ZarrUtils.zGroupAbsolutePath(keyValueAccess, basePath, pathName);
	}

	protected String zArrayPath(final String pathName) {

		return ZarrUtils.zArrayPath(keyValueAccess, pathName);
	}

	protected String zAttrsPath(final String pathName) {

		return ZarrUtils.zAttrsPath(keyValueAccess, pathName);
	}

	protected String zGroupPath(final String pathName) {

		return ZarrUtils.zGroupPath(keyValueAccess, pathName);
	}

	protected String[] normalList(final String normalPath) throws N5Exception.N5IOException {

		try {
			return keyValueAccess.listDirectories(groupPath(normalPath));
		} catch (IOException e) {
			throw new N5Exception.N5IOException("Cannot list directories for group " + normalPath, e);
		}
	}

	@Override
	public String[] list(final String pathName) throws IOException {

		final String normalPath = N5URL.normalizeGroupPath(pathName);
		if (cacheMeta) {
			return cache.list(normalPath);
		} else {
			return normalList(normalPath);
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

}
