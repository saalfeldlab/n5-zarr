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

import java.lang.reflect.Type;
import java.nio.ByteOrder;
import java.util.Arrays;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.GsonUtils;
import org.janelia.saalfeldlab.n5.KeyValueAccess;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.N5KeyValueReader;
import org.janelia.saalfeldlab.n5.N5URI;
import org.janelia.saalfeldlab.n5.NameConfigAdapter;
import org.janelia.saalfeldlab.n5.codec.CodecInfo;
import org.janelia.saalfeldlab.n5.zarr.ZarrKeyValueReader;
import org.janelia.saalfeldlab.n5.zarr.chunks.ChunkAttributes;
import org.janelia.saalfeldlab.n5.zarr.chunks.ChunkGrid;
import org.janelia.saalfeldlab.n5.zarr.chunks.ChunkKeyEncoding;
import org.janelia.saalfeldlab.n5.zarr.codec.transpose.ZarrTransposeCodecInfo;
import org.janelia.saalfeldlab.n5.zarr.codec.transpose.ZarrTransposeCodecInfo.ZarrTransposeOrder;
import org.janelia.saalfeldlab.n5.zarr.codec.transpose.ZarrTransposeCodecInfo.ZarrTransposeOrderAdapter;
import org.janelia.saalfeldlab.n5.zarr.v3.ZarrV3Node.NodeType;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonSyntaxException;

public class ZarrV3KeyValueReader extends N5KeyValueReader {

	// Override this constant
	// if we try supporting v2 and v3 in parallel
	public static final Version VERSION = new Version(3, 0, 0);

	public static final String ZARR_KEY = "zarr.json";

	protected final boolean mapN5DatasetAttributes;

	protected final boolean mergeAttributes;



	/**
	 * Opens an {@link ZarrV3KeyValueReader} at a given base path with a custom
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
	public ZarrV3KeyValueReader(
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

	/**
	 * Opens an {@link ZarrV3KeyValueReader} at a given base path with a custom
	 * {@link GsonBuilder} to support custom attributes.
	 *
	 * @param checkVersion
	 *            perform version check
	 * @param keyValueAccess
	 * @param basePath
	 *            N5 base path
	 * @param gsonBuilder
	 *            the gson builder
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
	public ZarrV3KeyValueReader(
			final boolean checkVersion,
			final KeyValueAccess keyValueAccess,
			final String basePath,
			final GsonBuilder gsonBuilder,
			final boolean cacheMeta)
			throws N5Exception {

		this(checkVersion, keyValueAccess, basePath, gsonBuilder, false, false, cacheMeta, true);
	}

	/**
	 * Opens an {@link ZarrV3KeyValueReader} at a given base path with a custom
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
	 * @throws N5Exception
	 *             if the base path cannot be read or does not exist,
	 *             if the N5 version of the container is not compatible with
	 *             this
	 *             implementation.
	 */
	public ZarrV3KeyValueReader(
			final KeyValueAccess keyValueAccess,
			final String basePath,
			final GsonBuilder gsonBuilder,
			final boolean mapN5DatasetAttributes,
			final boolean mergeAttributes,
			final boolean cacheMeta)
			throws N5Exception {

		this(true, keyValueAccess, basePath, gsonBuilder, mapN5DatasetAttributes, mergeAttributes, cacheMeta);
	}

	protected ZarrV3KeyValueReader(
			final boolean checkVersion,
			final KeyValueAccess keyValueAccess,
			final String basePath,
			final GsonBuilder gsonBuilder,
			final boolean mapN5DatasetAttributes,
			final boolean mergeAttributes,
			final boolean cacheMeta,
			final boolean checkRootExists) {

		super(checkVersion, keyValueAccess, basePath, addTypeAdapters(gsonBuilder), cacheMeta, checkRootExists);
		this.mergeAttributes = mergeAttributes;
		this.mapN5DatasetAttributes = mapN5DatasetAttributes;
	}

	@Override
	public String getAttributesKey() {

		return ZARR_KEY;
	}

	@Override
	public Version getVersion() throws N5Exception {

		return getVersion("/");
	}

	// protected?
	public Version getVersion(final String path) throws N5Exception {

		return getVersion(getRawAttribute(path, ZarrV3Node.ZARR_FORMAT_KEY, JsonElement.class));
	}

	protected static Version getVersion(final JsonElement json) {

		if (json == null)
			return ZarrKeyValueReader.VERSION_ZERO;

		if (json.isJsonPrimitive())
			return new Version(json.getAsInt(), 0, 0);

		return null;
	}

	@Override
	public boolean exists(final String pathName) {

		// Overridden because of the difference in how n5 and zarr define "group" and "dataset".
		// The implementation in CachedGsonKeyValueReader is simpler but more low-level
		final String normalPathName = N5URI.normalizeGroupPath(pathName);

		// Note that datasetExists and groupExists use the cache
		return groupExists(normalPathName) || datasetExists(normalPathName);
	}

	@Override
	public boolean isGroupFromContainer(final String normalPath) {

		return NodeType.isGroup(getRawAttribute(normalPath, ZarrV3DatasetAttributes.NODE_TYPE_KEY, String.class));
	}

	@Override
	public boolean isGroupFromAttributes(final String normalCacheKey, final JsonElement attributes) {

		if (normalCacheKey.equals(ZARR_KEY) && attributes != null && attributes.isJsonObject() && NodeType.isGroup(attributes.getAsJsonObject().getAsJsonPrimitive(ZarrV3Node.NODE_TYPE_KEY).getAsString())) {
			return true;
		} else {
			return false;
		}
	}


	@Override
	public boolean isDatasetFromAttributes(final String normalCacheKey, final JsonElement attributes) {

		if (normalCacheKey.equals(ZARR_KEY) && attributes != null && attributes.isJsonObject() && NodeType.isArray(attributes.getAsJsonObject().getAsJsonPrimitive(ZarrV3Node.NODE_TYPE_KEY).getAsString())) {
			return createDatasetAttributes(attributes) != null;
		} else {
			return false;
		}
	}

	@Override
	public ZarrV3DatasetAttributes createDatasetAttributes(final JsonElement attributes) {

		return gson.fromJson(attributes, ZarrV3DatasetAttributes.class);
	}

	public JsonElement getZarrAttributes(final String path) {

		// TODO decide how attributes work
		return getAttribute(ZARR_KEY, ZarrV3Node.ATTRIBUTES_KEY, JsonElement.class);
	}

	public JsonElement getRawAttributes(final String pathName) throws N5IOException {

		return super.getAttributes(pathName);
	}

	@Override
	public JsonElement getAttributes(final String pathName) throws N5IOException {
		final JsonElement elem = getRawAttributes(pathName);
		return elem == null ? null : elem.getAsJsonObject().get(ZarrV3Node.ATTRIBUTES_KEY);
	}

	public <T> T getAttribute(
			final String pathName,
			final String key,
			final Type type) throws N5Exception {

		final String normalPathName = N5URI.normalizeGroupPath(pathName);
		final String normalizedAttributePath = N5URI.normalizeAttributePath(key);
		JsonElement attributes;
		if (cacheMeta()) {
			final JsonElement zarrJson = getCache().getAttributes(normalPathName, getAttributesKey());
			attributes = zarrJson.getAsJsonObject().get(ZarrV3Node.ATTRIBUTES_KEY);
		} else {
			attributes = getAttributes(normalPathName);
		}
		try {
			return GsonUtils.readAttribute(attributes, normalizedAttributePath, type, getGson());
		} catch (JsonSyntaxException | NumberFormatException | ClassCastException e) {
			throw new N5Exception.N5ClassCastException(e);
		}
	}

	public <T> T getRawAttribute(
			final String pathName,
			final String key,
			final Class<T> clazz) throws N5Exception {

		return super.getAttribute(pathName, key, clazz);
	}

	@Override
	public <T> T getAttribute(
			final String pathName,
			final String key,
			final Class<T> clazz) throws N5Exception {

		final String normalizedAttributePath = N5URI.normalizeAttributePath(key);
		return super.getAttribute(pathName, ZarrV3Node.ATTRIBUTES_KEY + "/" + normalizedAttributePath, clazz);
	}

	@Override
	public String toString() {

		return String.format("%s[access=%s, basePath=%s]", getClass().getSimpleName(), keyValueAccess, uri.getPath());
	}

	protected static GsonBuilder addTypeAdapters(final GsonBuilder gsonBuilder) {

		gsonBuilder.registerTypeHierarchyAdapter(ChunkGrid.class, NameConfigAdapter.getJsonAdapter(ChunkGrid.class));
		gsonBuilder.registerTypeHierarchyAdapter(ChunkKeyEncoding.class, NameConfigAdapter.getJsonAdapter(ChunkKeyEncoding.class));
		gsonBuilder.registerTypeHierarchyAdapter(ChunkAttributes.class, ChunkAttributes.getJsonAdapter());
		gsonBuilder.registerTypeHierarchyAdapter(CodecInfo.class, NameConfigAdapter.getJsonAdapter(CodecInfo.class));

		gsonBuilder.registerTypeAdapter(ZarrV3Compressor.class, NameConfigAdapter.getJsonAdapter(ZarrV3Compressor.class));
		gsonBuilder.registerTypeAdapter(ZarrV3DatasetAttributes.class, ZarrV3DatasetAttributes.jsonAdapter);
		gsonBuilder.registerTypeAdapter(ByteOrder.class, ZarrV3DatasetAttributes.byteOrderAdapter);
		gsonBuilder.registerTypeAdapter(ZarrTransposeOrder.class, new ZarrTransposeOrderAdapter());
		gsonBuilder.disableHtmlEscaping();

		return gsonBuilder;
	}

	/**
	 * Converts an attribute path
	 *
	 * @param attributePath
	 * @return
	 */
	protected String mapAttributeKey(final String attributePath) {

		final String key = mapN5DatasetAttributes ? n5AttributeKeyToZarr(attributePath) : attributePath;
		return isAttributes(key) ? key : ZarrV3Node.ATTRIBUTES_KEY + "/" + key;
	}

	protected boolean isAttributes(final String attributePath) {

		if (!Arrays.stream(ZarrV3DatasetAttributes.REQUIRED_KEYS).anyMatch(attributePath::equals))
			return false;

		if (mapN5DatasetAttributes &&
				!Arrays.stream(DatasetAttributes.N5_DATASET_ATTRIBUTES).anyMatch(attributePath::equals)) {
			return false;
		}

		return true;
	}

	protected String n5AttributeKeyToZarr(final String attributePath) {

		switch (attributePath) {
		case DatasetAttributes.DIMENSIONS_KEY:
			return ZarrV3DatasetAttributes.SHAPE_KEY;
		case DatasetAttributes.BLOCK_SIZE_KEY:
			return ZarrV3DatasetAttributes.CHUNK_GRID_KEY + "/configuration/chunk_shape"; // TODO gross
		case DatasetAttributes.DATA_TYPE_KEY:
			return ZarrV3DatasetAttributes.DATA_TYPE_KEY;
		case DatasetAttributes.CODEC_KEY:
			return ZarrV3DatasetAttributes.CODECS_KEY;
		default:
			return attributePath;
		}
	}

	protected static <T> void fillWithFillValue(final ZarrV3DatasetAttributes datasetAttributes, final DataBlock<T> dataBlock) {

		final JsonElement fillValueJson = datasetAttributes.fillValue;
		final T data = dataBlock.getData();
		if (data instanceof byte[]) {
			Arrays.fill((byte[])data, fillValueJson.getAsByte());
		} else if (data instanceof short[]) {
			Arrays.fill((short[])data, fillValueJson.getAsShort());
		} else if (data instanceof int[]) {
			Arrays.fill((int[])data, fillValueJson.getAsInt());
		} else if (data instanceof long[]) {
			Arrays.fill((long[])data, fillValueJson.getAsLong());
		} else if (data instanceof float[]) {
			Arrays.fill((float[])data, fillValueJson.getAsFloat());
		} else if (data instanceof double[]) {
			Arrays.fill((double[])data, fillValueJson.getAsDouble());
		}
	}

}
