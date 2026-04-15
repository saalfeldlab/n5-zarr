/*-
 * #%L
 * Not HDF5
 * %%
 * Copyright (C) 2017 - 2025 Stephan Saalfeld
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
package org.janelia.saalfeldlab.n5.zarr;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.janelia.saalfeldlab.n5.CachedGsonKeyValueN5Reader;
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.CompressionAdapter;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.KeyValueAccess;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.N5Path.N5DirectoryPath;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5Store;
import org.janelia.saalfeldlab.n5.RootedKeyValueAccess;
import org.janelia.saalfeldlab.n5.cache.DelegateStore;

import static org.janelia.saalfeldlab.n5.zarr.ZarrDatasetAttributes.createZArrayAttributes;

/**
 * {@link N5Reader} implementation through {@link KeyValueAccess} with JSON attributes parsed with {@link Gson}.
 *
 * @author Stephan Saalfeld
 * @author John Bogovic
 */
public class ZarrKeyValueReader implements CachedGsonKeyValueN5Reader {

	public static class ZarrVersion extends Version {

		public ZarrVersion(final int major) {
			super(major, 0, 0);
		}

		@Override
		public boolean isCompatible(final Version version) {

			// NB: Override Version.isCompatible because
			// Zarr v2 is only compatible with v2, and
			// Zarr v3 is only compatible with v3.

			// We are always compatible with NO_VERSION (uninitialized container)
			if (NO_VERSION.equals(version))
				return true;

			// TODO should we also check (version instanceof ZarrVersion)?
			return version.getMajor() == this.getMajor();
		}
	}


	public static final ZarrVersion ZARR_2_VERSION = new ZarrVersion(2);
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
		this.store = new ZarrN5Store(metaStore, gson, mapN5DatasetAttributes, mergeAttributes);
		this.mapN5DatasetAttributes = mapN5DatasetAttributes;
		this.mergeAttributes = mergeAttributes;

		boolean versionFound = false;
		if (checkVersion) {
			/* Existence checks, if any, go in subclasses */
			/* Check that version (if there is one) is compatible. */
			final Version version = getVersion();
			versionFound = !version.equals(NO_VERSION);
			if (!ZARR_2_VERSION.isCompatible(version))
				throw new N5Exception.N5IOException(
						"Incompatible version " + version + " (this is " + ZARR_2_VERSION + ").");
		}

		// if a version was found, the container exists - don't need to check again
		if (checkRootExists && (!versionFound && !exists("/")))
			throw new N5Exception.N5IOException("No container exists at " + keyValueAccess.root());
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

		final N5DirectoryPath root = N5DirectoryPath.of("");
		final Integer v = store.getAttribute(root, ZARR_FORMAT_KEY, Integer.class);
		return v == null ? NO_VERSION : new ZarrVersion(v);
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
		return (ZarrDatasetAttributes) store.getDatasetAttributes(N5DirectoryPath.of(pathName));
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

	@Override
	public String toString() {

		return String.format("%s[access=%s, basePath=%s]", getClass().getSimpleName(), keyValueAccess, getURI().getPath());
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
