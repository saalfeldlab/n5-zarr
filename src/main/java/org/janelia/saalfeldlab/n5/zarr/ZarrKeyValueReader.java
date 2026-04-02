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
import com.google.gson.JsonSyntaxException;
import java.lang.reflect.Type;
import java.net.URI;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.janelia.saalfeldlab.n5.CachedGsonKeyValueN5Reader;
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.CompressionAdapter;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.GsonUtils;
import org.janelia.saalfeldlab.n5.KeyValueAccess;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.N5Exception.N5ClassCastException;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.N5Path.N5GroupPath;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5URI;
import org.janelia.saalfeldlab.n5.RawCompression;
import org.janelia.saalfeldlab.n5.RootedKeyValueAccess;
import org.janelia.saalfeldlab.n5.cache.DelegateStore;
import org.janelia.saalfeldlab.n5.serialization.JsonArrayUtils;

import static org.janelia.saalfeldlab.n5.zarr.ZarrDatasetAttributes.createZArrayAttributes;

/**
 * {@link N5Reader} implementation through {@link KeyValueAccess} with JSON attributes parsed with {@link Gson}.
 *
 * @author Stephan Saalfeld
 * @author John Bogovic
 */
public class ZarrKeyValueReader implements CachedGsonKeyValueN5Reader {

	public static final Version VERSION_ZERO = new Version(0, 0, 0);
	public static final Version VERSION = new Version(2, 0, 0);
	public static final String ZARR_FORMAT_KEY = "zarr_format";

	public static final String ZARRAY_FILE = ".zarray";
	public static final String ZATTRS_FILE = ".zattrs";
	public static final String ZGROUP_FILE = ".zgroup";

	protected final RootedKeyValueAccess keyValueAccess;

	protected final DelegateStore metaStore;

	protected final Gson gson;

	protected final boolean cacheMeta;

	protected final Map<DatasetAttributes, ZarrDatasetAttributes> datasetAttributesMap = new ConcurrentHashMap<>();

	protected final boolean mapN5DatasetAttributes;

	protected final boolean mergeAttributes;

	protected String dimensionSeparator;

	/**
	 * Opens an {@link ZarrKeyValueReader} at a given base path with a custom {@link GsonBuilder} to support custom
	 * attributes.
	 *
	 * @param checkVersion
	 *            perform version check
	 * @param keyValueAccess
	 *            the backend KeyValueAccess used
	 * @param gsonBuilder
	 *            the gson builder
	 * @param mapN5DatasetAttributes
	 *            If true, getAttributes and variants of getAttribute methods will contain keys used by n5 datasets, and
	 *            whose values are those for their corresponding zarr fields. For example, if true, the key "dimensions"
	 *            (from n5) may be used to obtain the value of the key "shape" (from zarr).
	 * @param mergeAttributes
	 *            If true, fields from .zgroup, .zarray, and .zattrs will be merged when calling getAttributes, and
	 *            variants of getAttribute
	 * @param cacheMeta
	 *            cache attributes and meta data Setting this to true avoids frequent reading and parsing of JSON
	 *            encoded attributes and other meta data that requires accessing the store. This is most interesting for
	 *            high latency backends. Changes of cached attributes and meta data by an independent writer will not be
	 *            tracked.
	 *
	 * @throws N5Exception
	 *             if the base path cannot be read or does not exist, if the N5 version of the container is not
	 *             compatible with this implementation.
	 */
	public ZarrKeyValueReader(
			final boolean checkVersion,
			final RootedKeyValueAccess keyValueAccess,
			final GsonBuilder gsonBuilder,
			final boolean mapN5DatasetAttributes,
			final boolean mergeAttributes,
			final boolean cacheMeta)
			throws N5Exception {

		this(checkVersion, keyValueAccess, gsonBuilder, mapN5DatasetAttributes, mergeAttributes, cacheMeta,
				true);
	}

	protected ZarrKeyValueReader(
			final boolean checkVersion,
			final RootedKeyValueAccess keyValueAccess,
			final GsonBuilder gsonBuilder,
			final boolean mapN5DatasetAttributes,
			final boolean mergeAttributes,
			final boolean cacheMeta,
			final boolean checkRootExists) {

		this.keyValueAccess = keyValueAccess;
		this.gson = registerGson(gsonBuilder);
		this.cacheMeta = cacheMeta;
		this.metaStore = createMetaStore(keyValueAccess, cacheMeta);
		this.mapN5DatasetAttributes = mapN5DatasetAttributes;
		this.mergeAttributes = mergeAttributes;

		if (checkRootExists && !exists("/"))
			throw new N5IOException("No container exists at " + getURI());

	}

	/**
	 * Opens an {@link ZarrKeyValueReader} at a given base path with a custom {@link GsonBuilder} to support custom
	 * attributes.
	 *
	 * @param keyValueAccess
	 *            the backend KeyValueAccess used
	 * @param gsonBuilder
	 *            the gson builder
	 * @param mapN5DatasetAttributes
	 *            If true, getAttributes and variants of getAttribute methods will contain keys used by n5 datasets, and
	 *            whose values are those for their corresponding zarr fields. For example, if true, the key "dimensions"
	 *            (from n5) may be used to obtain the value of the key "shape" (from zarr).
	 * @param mergeAttributes
	 *            If true, fields from .zgroup, .zarray, and .zattrs will be merged when calling getAttributes, and
	 *            variants of getAttribute
	 * @param cacheMeta
	 *            cache attributes and meta data Setting this to true avoids frequent reading and parsing of JSON
	 *            encoded attributes and other meta data that requires accessing the store. This is most interesting for
	 *            high latency backends. Changes of cached attributes and meta data by an independent writer will not be
	 *            tracked.
	 *
	 * @throws N5Exception
	 *             if the base path cannot be read or does not exist, if the N5 version of the container is not
	 *             compatible with this implementation.
	 */
	public ZarrKeyValueReader(
			final RootedKeyValueAccess keyValueAccess,
			final GsonBuilder gsonBuilder,
			final boolean mapN5DatasetAttributes,
			final boolean mergeAttributes,
			final boolean cacheMeta)
			throws N5Exception {

		this(true, keyValueAccess, gsonBuilder, mapN5DatasetAttributes, mergeAttributes, cacheMeta);
	}

	@Override
	public String getAttributesKey() {

		// not used internally, since the methods that use this are overridden
		return ZATTRS_FILE;
	}

	@Override
	public Gson getGson() {

		return gson;
	}

	@Override
	public RootedKeyValueAccess getRootedKeyValueAccess() {

		return keyValueAccess;
	}

	@Override
	public DelegateStore getDelegateStore() {

		return metaStore;
	}

	@Override
	public boolean cacheMeta() {

		return cacheMeta;
	}

	@Override
	public URI getURI() {

		return keyValueAccess.root();
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

		// Note that datasetExists and groupExists use the cache
		return groupExists(pathName) || datasetExists(pathName);
	}

	/*
	 * Tests whether the key at normalPathName exists for the backend. Such a key may exist for paths that are neither
	 * zarr groups nor zarr datasets.
	 *
	 * @param normalPathName
	 *            normalized path name
	 * @return true if the key exists
	 */
	// TODO remove
//	public boolean existsFromContainer(final String normalPathName) {
//
//		// A cloud keyValueAccess may return false for
//		// exists(groupPath(normalPathName)),
//		// so instead need to check for existence of either .zarray or .zgroup
//		final N5GroupPath group = N5GroupPath.of(normalPathName);
//		return keyValueAccess.isFile(group.resolve(ZGROUP_FILE)) ||
//				keyValueAccess.isFile(group.resolve(ZARRAY_FILE));
//
//		// TODO: use isGroupFromContainer || isDatasetFromContainer ????
//	}

	// TODO [+]
	@Override
	public boolean groupExists(final String pathName) {

		final N5GroupPath group = N5GroupPath.of(pathName);
		final JsonElement zgroup = metaStore.store_readAttributesJson(group, ZGROUP_FILE, gson);
		return zgroup != null;
	}

	// TODO remove
//	@Override
//	public boolean isGroupFromContainer(final String normalPath) {
//
//		final N5GroupPath group = N5GroupPath.of(normalPath);
//		return keyValueAccess.isFile(group.resolve(ZGROUP_FILE));
//	}

	// TODO remove
//	@Override
//	public boolean isGroupFromAttributes(final String normalCacheKey, final JsonElement attributes) {
//
//		return attributes != null && attributes.isJsonObject() && attributes.getAsJsonObject().has(ZARR_FORMAT_KEY);
//	}

	// TODO [+]
	@Override
	public boolean datasetExists(final String pathName) throws N5IOException {

		return getDatasetAttributes(pathName) != null;
	}

	// TODO remove
//	@Override
//	public boolean isDatasetFromContainer(final String normalPath) throws N5Exception {
//
//		final N5GroupPath group = N5GroupPath.of(normalPath);
//		if (keyValueAccess.isFile(group.resolve(ZARRAY_FILE))) {
//			return isDatasetFromAttributes(ZARRAY_FILE, getAttributesFromContainer(normalPath, ZARRAY_FILE));
//		} else {
//			return false;
//		}
//	}

	// TODO remove
//	@Override
//	public boolean isDatasetFromAttributes(final String normalCacheKey, final JsonElement attributes) {
//
//		if (normalCacheKey.equals(ZARRAY_FILE) && attributes != null && attributes.isJsonObject()) {
//			return createDatasetAttributes(attributes) != null;
//		} else {
//			return false;
//		}
//	}

	/**
	 * Returns the {@link ZarrDatasetAttributes} located at the given path, if present.
	 *
	 * @param pathName
	 *            the path relative to the container's root
	 * @return the zarr array attributes
	 * @throws N5Exception
	 *             the exception
	 */
	// TODO [+]
	@Override
	public ZarrDatasetAttributes getDatasetAttributes(final String pathName) throws N5Exception {

		return createDatasetAttributes(getZArray(pathName));
	}

	// TODO [+]
	@Override
	public ZarrDatasetAttributes getConvertedDatasetAttributes(DatasetAttributes attributes) {

		if (attributes instanceof ZarrDatasetAttributes) {
			return ((ZarrDatasetAttributes) attributes);
		}
		return datasetAttributesMap.computeIfAbsent(attributes, attr -> new ZarrDatasetAttributes(createZArrayAttributes(dimensionSeparator, attr)));
	}

	/**
	 * Returns the {@link ZArrayAttributes} located at the given path, if present.
	 *
	 * @param pathName
	 *            the path relative to the container's root
	 * @return the zarr array attributes
	 * @throws N5Exception
	 *             the exception
	 */
	// TODO [ ] ???
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
	// TODO [ ] ???
	protected ZArrayAttributes getZArrayAttributes(final JsonElement attributes) {

		return gson.fromJson(attributes, ZArrayAttributes.class);
	}

	// TODO [ ] ???
	@Override
	public ZarrDatasetAttributes createDatasetAttributes(final JsonElement attributes) {

		final ZArrayAttributes zarray = getZArrayAttributes(attributes);
		return zarray != null ? new ZarrDatasetAttributes(zarray) : null;
	}

	// TODO [+]
	@Override
	public <T> T getAttribute(final String pathName, final String key, final Type type) throws N5Exception {

		final String normalizedAttributePath = N5URI.normalizeAttributePath(key);
		final JsonElement attributes = getAttributes(pathName); // handles caching
		try {
			return GsonUtils.readAttribute(attributes, normalizedAttributePath, type, gson);
		} catch (JsonSyntaxException | NumberFormatException | ClassCastException e) {
			if (key.equals("filters") && attributes.getAsJsonObject().get("filters").isJsonNull()) {
				return (T) Collections.EMPTY_LIST;
			}
			throw new N5ClassCastException(e);
		}
	}

	/**
	 * Returns a {@link JsonElement} representing the contents of a JSON file located at the given path if present, and
	 * null if not. Also updates the cache.
	 *
	 * @param pathName
	 *            the normalized path
	 * @param jsonName
	 *            the name of the JSON file
	 * @return the JSON element
	 * @throws N5Exception
	 *             the exception
	 */
	// TODO remove
//	protected JsonElement getJsonResource(final String pathName, final String jsonName) throws N5Exception {
//
//		if (cacheMeta()) {
//			final String normalPath = N5GroupPath.of(pathName).normalPath();
//			return cache.getAttributes(normalPath, jsonName);
//		} else {
//			return getAttributesFromContainer(pathName, jsonName);
//		}
//	}

	protected static JsonElement reverseAttrsWhenCOrder(final JsonElement elem) {

		if (elem == null || !elem.isJsonObject())
			return elem;

		final JsonObject attrs = elem.getAsJsonObject();
		if (attrs.get(ZArrayAttributes.orderKey).getAsString().equals("C")) {

			final JsonArray shape = attrs.get(ZArrayAttributes.shapeKey).getAsJsonArray();
			JsonArrayUtils.reverse(shape);
			attrs.add(ZArrayAttributes.shapeKey, shape);

			final JsonArray chunkSize = attrs.get(ZArrayAttributes.chunksKey).getAsJsonArray();
			JsonArrayUtils.reverse(chunkSize);
			attrs.add(ZArrayAttributes.chunksKey, chunkSize);
		}
		return attrs;
	}

	/**
	 * Returns a {@link JsonElement} representing the contents of .zgroup located at the given path if present, and null
	 * if not.
	 *
	 * @param path
	 *            the path
	 * @return the JSON element
	 * @throws N5Exception
	 *             the exception
	 */
	// TODO [+]
	protected JsonElement getZGroup(final String path) throws N5Exception {

		final N5GroupPath group = N5GroupPath.of(path);
		return metaStore.store_readAttributesJson(group, ZGROUP_FILE, gson);
	}

	/**
	 * Returns a {@link JsonElement} representing the contents of .zarray located at the given path if present, and null
	 * if not.
	 *
	 * @param path
	 *            the path
	 * @return the JSON element
	 * @throws N5Exception
	 *             the exception
	 */
	// TODO [+]
	protected JsonElement getZArray(final String path) throws N5Exception {

		final N5GroupPath group = N5GroupPath.of(path);
		return metaStore.store_readAttributesJson(group, ZARRAY_FILE, gson);
	}

	/**
	 * Returns a {@link JsonElement} representing the contents of .zattrs located at the given path if present, and null
	 * if not.
	 *
	 * @param path
	 *            the path
	 * @return the JSON element
	 * @throws N5Exception
	 *             the exception
	 */
	// TODO [+]
	protected JsonElement getZAttributes(final String path) throws N5Exception {

		final N5GroupPath group = N5GroupPath.of(path);
		return metaStore.store_readAttributesJson(group, ZATTRS_FILE, gson);
	}
	protected JsonElement readZattrs(final N5GroupPath group) throws N5Exception {

		return metaStore.store_readAttributesJson(group, ZATTRS_FILE, gson);
	}

	// TODO [ ]
	protected JsonElement zarrToN5DatasetAttributes(final JsonElement elem) {

		if (!mapN5DatasetAttributes || elem == null || !elem.isJsonObject())
			return elem;

		final JsonObject attrs = new JsonObject();
		attrs.asMap().putAll(elem.getAsJsonObject().asMap()); // shallow copy
		final ZArrayAttributes zattrs = getZArrayAttributes(attrs);
		if (zattrs == null)
			return elem;

		// NB: The returned JsonElement will have both "shape" and "dimensions"
		//  arrays. This is a bit different than the reverse direction:
		//  ZarrKeyValueWriter.build(...), removes "dimensions" and just puts
		//  the equivalent "shape".
		//  Similar for "chunks" and "blockSize".
		final JsonArray dimensions = attrs.get(ZArrayAttributes.shapeKey).getAsJsonArray().deepCopy();
		final JsonArray blockSize = attrs.get(ZArrayAttributes.chunksKey).getAsJsonArray().deepCopy();
		final String order = attrs.get(ZArrayAttributes.orderKey).getAsString();
		if ("C".equals(order)) {
			JsonArrayUtils.reverse(dimensions);
			JsonArrayUtils.reverse(blockSize);
		}
		attrs.add(DatasetAttributes.DIMENSIONS_KEY, dimensions);
		attrs.add(DatasetAttributes.BLOCK_SIZE_KEY, blockSize);
		attrs.addProperty(DatasetAttributes.DATA_TYPE_KEY, zattrs.getDType().getDataType().toString());

		final JsonElement e = attrs.get(ZArrayAttributes.compressorKey);
		if (e == JsonNull.INSTANCE || e == null) {
			attrs.add(DatasetAttributes.COMPRESSION_KEY, gson.toJsonTree(new RawCompression()));
		} else {
			attrs.add(DatasetAttributes.COMPRESSION_KEY,
					gson.toJsonTree(gson.fromJson(e, ZarrCompressor.class).getCompression()));
		}
		return attrs;
	}

	// TODO [ ]
	protected JsonElement n5ToZarrDatasetAttributes(final JsonElement elem) {

		if (!mapN5DatasetAttributes || elem == null || !elem.isJsonObject())
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
	 * Returns the attributes at the given path.
	 * <p>
	 * If this reader was constructed with mergeAttributes as true, this method will attempt to combine the contents of
	 * .zgroup, .zarray, and .zattrs, when present using {@link #combineAll(JsonElement...)}. Otherwise, this
	 * method will return only the contents of .zattrs, as {@link #getZAttributes}.
	 *
	 * @param path
	 *            the path
	 * @return the json element
	 * @throws N5Exception
	 *             the exception
	 */
	// TODO [~] combine with slightly different method below?
	@Override
	public JsonElement getAttributes(final String path) throws N5Exception {

		final N5GroupPath group = N5GroupPath.of(path);
		final JsonElement zattrs = metaStore.store_readAttributesJson(group, ZATTRS_FILE, gson);

		if (mergeAttributes) {
			final JsonElement zgroup = metaStore.store_readAttributesJson(group, ZGROUP_FILE, gson);
			final JsonElement zarray = metaStore.store_readAttributesJson(group, ZARRAY_FILE, gson);
			return combineAll(zgroup, zattrs, zarrToN5DatasetAttributes(zarray));
		} else {
			return zattrs;
		}
	}

	// TODO [~] combine with slightly different method above?
	protected JsonElement getAttributesUnmapped(final String path) throws N5Exception {

		final N5GroupPath group = N5GroupPath.of(path);
		final JsonElement zattrs = metaStore.store_readAttributesJson(group, ZATTRS_FILE, gson);

		if (mergeAttributes) {
			final JsonElement zgroup = metaStore.store_readAttributesJson(group, ZGROUP_FILE, gson);
			final JsonElement zarray = metaStore.store_readAttributesJson(group, ZARRAY_FILE, gson);
			return combineAll(zgroup, zattrs, zarray);
		} else {
			return zattrs;
		}
	}

	// TODO remove
//	@Override
//	public JsonElement getAttributesFromContainer(
//			final String normalResourceParent,
//			final String normalResourcePath) throws N5Exception {
//
//		final N5GroupPath group = N5GroupPath.of(normalResourceParent);
//		final N5FilePath path = group.resolve(normalResourcePath).asFile();
//
//		if (!keyValueAccess.isFile(path))
//			return null;
//
//		try (final VolatileReadData rd = keyValueAccess.createReadData(path)) {
//			return gson.fromJson(new String(rd.allBytes()), JsonElement.class);
//		}
//	}

	@Override
	public String toString() {

		return String.format("%s[access=%s, basePath=%s]", getClass().getSimpleName(), keyValueAccess, getURI().getPath());
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
	 * Returns one {@link JsonElement} that (attempts to) combine all passed
	 * json elements. The returned instance is a deep copy, and the arguments
	 * are not modified.
	 * <p>
	 * If all {@code elements} are {@code null}, {@code null} is returned.
	 * Otherwise, the base element is a deep copy of the first non-null element.
	 * The remaining {@code elements} are combined into the base element one by
	 * one:
	 * <p>
	 * The base element is returned if two arguments can not be combined. The
	 * two arguments may be combined if they are both {@link JsonObject}s or
	 * both {@link JsonArray}s.
	 * <p>
	 * If both arguments are {@link JsonObject}s, every key-value pair in the
	 * add argument is added to the base argument, overwriting any duplicate
	 * keys. If both arguments are {@link JsonArray}s, the add argument is
	 * concatenated to the base argument.
	 *
	 * @param elements
	 *            an array of json elements
	 * @return a new, combined element
	 */
	// TODO [+]
	protected static JsonElement combineAll(final JsonElement... elements) {

		JsonElement base = null;
		for (final JsonElement element : elements) {
			if (element != null) {
				final JsonElement add = element.deepCopy();
				if (base == null) {
					base = add;
				} else if (base.isJsonObject() && add.isJsonObject()) {
					final JsonObject baseObj = base.getAsJsonObject();
					add.getAsJsonObject().asMap().forEach(baseObj::add);
				} else if (base.isJsonArray() && add.isJsonArray()) {
					final JsonArray baseArr = base.getAsJsonArray();
					baseArr.addAll(add.getAsJsonArray());
				} // else: trying to combine incompatible JsonElements
			}
		}
		return base;
	}

	static Gson registerGson(final GsonBuilder gsonBuilder) {

		return addTypeAdapters(gsonBuilder).serializeNulls().create();
	}

	protected static GsonBuilder addTypeAdapters(final GsonBuilder gsonBuilder) {

		gsonBuilder.registerTypeAdapter(DataType.class, new DataType.JsonAdapter());
		gsonBuilder.registerTypeAdapter(ZarrCompressor.class, ZarrCompressor.jsonAdapter);
		gsonBuilder.registerTypeHierarchyAdapter(Compression.class, CompressionAdapter.getJsonAdapter());
		gsonBuilder.registerTypeAdapter(Compression.class, CompressionAdapter.getJsonAdapter());
		gsonBuilder.registerTypeAdapter(ZArrayAttributes.class, ZArrayAttributes.jsonAdapter);
		gsonBuilder.registerTypeHierarchyAdapter(Filter.class, Filter.jsonAdapter);
		gsonBuilder.disableHtmlEscaping();

		return gsonBuilder;
	}

}
