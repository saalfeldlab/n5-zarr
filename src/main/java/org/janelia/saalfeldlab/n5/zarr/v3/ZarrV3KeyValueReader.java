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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.nio.ByteOrder;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.janelia.saalfeldlab.n5.CachedGsonKeyValueN5Reader;
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.CompressionAdapter;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.N5Path.N5GroupPath;
import org.janelia.saalfeldlab.n5.N5Store;
import org.janelia.saalfeldlab.n5.NameConfigAdapter;
import org.janelia.saalfeldlab.n5.RootedKeyValueAccess;
import org.janelia.saalfeldlab.n5.cache.DelegateStore;
import org.janelia.saalfeldlab.n5.codec.CodecInfo;
import org.janelia.saalfeldlab.n5.zarr.chunks.ChunkAttributes;
import org.janelia.saalfeldlab.n5.zarr.chunks.ChunkGrid;
import org.janelia.saalfeldlab.n5.zarr.chunks.ChunkKeyEncoding;
import org.janelia.saalfeldlab.n5.zarr.codec.transpose.ZarrTransposeCodecInfo.ZarrTransposeOrder;
import org.janelia.saalfeldlab.n5.zarr.codec.transpose.ZarrTransposeCodecInfo.ZarrTransposeOrderAdapter;

import static org.janelia.saalfeldlab.n5.zarr.ZarrKeyValueReader.VERSION_ZERO;
import static org.janelia.saalfeldlab.n5.zarr.v3.ZarrV3Node.ZARR_FORMAT_KEY;

public class ZarrV3KeyValueReader implements CachedGsonKeyValueN5Reader {

	protected final Map<DatasetAttributes, ZarrV3DatasetAttributes> datasetAttributesMap = new ConcurrentHashMap<>();

	// Override this constant
	// if we try supporting v2 and v3 in parallel
	public static final Version ZARR_VERSION = new Version(3, 0, 0);

	public static final String ZARR_KEY = "zarr.json";

	public static final String DEFAULT_DIMENSION_SEPARATOR = "/";

	protected String dimensionSeparator = DEFAULT_DIMENSION_SEPARATOR;

	protected final RootedKeyValueAccess keyValueAccess;
	protected final DelegateStore metaStore;
	protected final N5Store store;
	protected final Gson gson;
	protected final boolean cacheMeta;


	/**
     * Opens an {@link ZarrV3KeyValueReader} at a given base path with a custom
     * {@link GsonBuilder} to support custom attributes.
     *
     * @param checkVersion   perform version check
     * @param keyValueAccess
     * @param gsonBuilder    the gson builder
     * @param cacheMeta      cache attributes and meta data
     *                       Setting this to true avoids frequent reading and parsing of
     *                       JSON
     *                       encoded attributes and other meta data that requires accessing
     *                       the
     *                       store. This is most interesting for high latency backends.
     *                       Changes
     *                       of cached attributes and meta data by an independent writer
     *                       will
     *                       not be tracked.
     * @throws N5Exception if the base path cannot be read or does not exist,
     *                     if the N5 version of the container is not compatible with
     *                     this
     *                     implementation.
     */
	public ZarrV3KeyValueReader(
			final boolean checkVersion,
			final RootedKeyValueAccess keyValueAccess,
			final GsonBuilder gsonBuilder,
            final boolean cacheMeta)
			throws N5Exception {

		this(checkVersion, keyValueAccess, gsonBuilder, cacheMeta, true);
	}

	/**
	 * Opens an {@link ZarrV3KeyValueReader} at a given base path with a custom
	 * {@link GsonBuilder} to support custom attributes.
	 *
	 * @param keyValueAccess
	 * @param gsonBuilder
	 * 			GSON builder
	 * @param cacheMeta
	 *            cache attributes and meta data Setting this to true avoids
	 *            frequent reading and parsing of JSON encoded attributes and
	 *            other meta data that requires accessing the store. This is
	 *            most interesting for high latency backends. Changes of cached
	 *            attributes and meta data by an independent writer will not be
	 *            tracked.
	 * @throws N5Exception
	 *             if the base path cannot be read or does not exist, if the N5
	 *             version of the container is not compatible with this
	 *             implementation.
	 */
	public ZarrV3KeyValueReader(
			final RootedKeyValueAccess keyValueAccess,
			final GsonBuilder gsonBuilder,
            final boolean cacheMeta)
			throws N5Exception {

		this(true, keyValueAccess, gsonBuilder, cacheMeta);
	}

	protected ZarrV3KeyValueReader(
			final boolean checkVersion,
			final RootedKeyValueAccess keyValueAccess,
			final GsonBuilder gsonBuilder,
			final boolean cacheMeta,
			final boolean checkRootExists) {

		this.keyValueAccess = keyValueAccess;
		this.gson = addTypeAdapters(gsonBuilder).create();
		this.cacheMeta = cacheMeta;
		this.metaStore = createMetaStore(keyValueAccess, cacheMeta);
		this.store = new ZarrV3Store(metaStore, gson);

		boolean versionFound = false;
		if (checkVersion) {
			/* Existence checks, if any, go in subclasses */
			/* Check that version (if there is one) is compatible. */
			final Version version = getVersion();
			versionFound = !version.equals(NO_VERSION);
			if (!ZARR_VERSION.isCompatible(version))
				throw new N5Exception.N5IOException(
						"Incompatible version " + version + " (this is " + ZARR_VERSION + ").");
		}

		// if a version was found, the container exists - don't need to check again
		if (checkRootExists && (!versionFound && !exists("/")))
			throw new N5Exception.N5IOException("No container exists at " + keyValueAccess.root());
	}

	public String getDimensionSeparator() {
		return dimensionSeparator;
	}

	public void setDimensionSeparator(String dimensionSeparator) {
		this.dimensionSeparator = dimensionSeparator;
	}

	@Override
	public String getAttributesKey() {

		return ZARR_KEY;
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

	@Override
	public ZarrV3DatasetAttributes getConvertedDatasetAttributes(final DatasetAttributes attributes) {

		if (attributes instanceof ZarrV3DatasetAttributes) {
			return ((ZarrV3DatasetAttributes) attributes);
		}
		return datasetAttributesMap.computeIfAbsent(attributes, attr -> ZarrV3DatasetAttributes.from(attr, getDimensionSeparator(), "0"));
	}


	@Override
	public Version getVersion() throws N5Exception {

		return getVersion("/");
	}

	protected Version getVersion(final String path) throws N5Exception {

		final N5GroupPath root = N5GroupPath.of(path);
		if (store.groupExists(root) || store.datasetExists(root)) {
			try {
				final Integer v = store.getAttribute(root, ZARR_FORMAT_KEY, Integer.class);
				if (v == null) {
					return VERSION_ZERO;
				}
				return new Version(v, 0, 0);
			} catch (N5Exception.N5ClassCastException e) {
				return null;
			}
		}
		return ZARR_VERSION;
	}

	@Override
	public String toString() {

		return String.format("%s[access=%s, basePath=%s]", getClass().getSimpleName(), keyValueAccess, getURI().getPath());
	}

	protected static GsonBuilder addTypeAdapters(final GsonBuilder gsonBuilder) {

		gsonBuilder.registerTypeAdapter(DataType.class, new DataType.JsonAdapter());
		gsonBuilder.registerTypeHierarchyAdapter(Compression.class, CompressionAdapter.getJsonAdapter());
		gsonBuilder.registerTypeHierarchyAdapter(DatasetAttributes.class, DatasetAttributes.getJsonAdapter());

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
}
