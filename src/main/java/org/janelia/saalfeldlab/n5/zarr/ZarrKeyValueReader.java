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
import org.janelia.saalfeldlab.n5.N5Store;
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
	protected final N5Store store;
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
		this.store = new ZarrN5Store(metaStore, gson, mergeAttributes);
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
	public N5Store getN5Store() {

		return store;
	}

	@Override
	public boolean cacheMeta() {

		return cacheMeta;
	}

	/**
	 * Returns the version of this zarr container.
	 *
	 * @return the {@link Version}
	 */
	@Override
	public Version getVersion() throws N5Exception {

		final N5GroupPath root = N5GroupPath.of("");
		if (store.groupExists(root) || store.datasetExists(root)) {
			try {
				final Integer v = store.getAttribute(root, ZARR_FORMAT_KEY, Integer.class);
				if (v == null) {
					return VERSION_ZERO;
				}
				return new Version(v, 0, 0);
			} catch (N5ClassCastException e) {
				return null;
			}
		}
		return VERSION;
	}

	/**
	 * Returns the {@link ZarrDatasetAttributes} located at the given path, if present.
	 *
	 * @param pathName
	 *            the path relative to the container's root
	 * @return the zarr array attributes
	 * @throws N5Exception
	 *             the exception
	 */
	@Override
	public ZarrDatasetAttributes getDatasetAttributes(final String pathName) throws N5Exception {
		return (ZarrDatasetAttributes) store.getDatasetAttributes(N5GroupPath.of(pathName));
	}

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

		throw new N5IOException("TODO: Not implemented yet");
	}

	// TODO [+] ???
	@Deprecated
	@Override
	public ZarrDatasetAttributes createDatasetAttributes(final JsonElement attributes) {

		final ZArrayAttributes zarray = gson.fromJson(attributes, ZArrayAttributes.class);
		return zarray != null ? new ZarrDatasetAttributes(zarray) : null;
	}

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
