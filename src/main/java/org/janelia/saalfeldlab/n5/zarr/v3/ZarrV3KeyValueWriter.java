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

import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import org.janelia.saalfeldlab.n5.CachedGsonKeyValueN5Writer;
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.GsonUtils;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.N5Path.N5GroupPath;
import org.janelia.saalfeldlab.n5.N5URI;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.RootedKeyValueAccess;
import org.janelia.saalfeldlab.n5.zarr.v3.ZarrV3Node.NodeType;

/**
 * Zarr v3 {@link N5Writer} implementation.
 */
public class ZarrV3KeyValueWriter extends ZarrV3KeyValueReader implements CachedGsonKeyValueN5Writer {

	/**
	 * Opens an {@link ZarrV3KeyValueWriter} at a given base path with a custom
	 * {@link GsonBuilder} to support custom attributes.
	 *
	 * @param keyValueAccess
	 * @param gsonBuilder
	 * @param cacheAttributes
	 * 		cache attributes and meta data Setting this to true avoids frequent
	 * 		reading and parsing of JSON encoded attributes and other meta data that
	 * 		requires accessing the store. This is most interesting for high latency
	 * 		backends. Changes of cached attributes and meta data by an independent
	 * 		writer will not be tracked.
	 *
	 * @throws N5Exception
	 * 		if the base path cannot be read or does not exist, if the N5 version of
	 * 		the container is not compatible with this implementation.
	 */
	public ZarrV3KeyValueWriter(
			final RootedKeyValueAccess keyValueAccess,
			final GsonBuilder gsonBuilder,
            final boolean cacheAttributes)
			throws N5Exception {

		super(false, keyValueAccess, gsonBuilder,
				cacheAttributes, false);

		Version version = null;
		try {
			version = getVersion();
			if (!ZarrV3KeyValueReader.VERSION.isCompatible(version))
				throw new N5IOException(
						"Incompatible version " + version + " (this is " + ZarrV3KeyValueReader.VERSION + ").");
		} catch (final NullPointerException e) {}

		if (version == null || version.equals(new Version(0, 0, 0, ""))) {
			createGroup("/"); // sets the version
		}
	}

	@Override
	public void setVersion(final String path) throws N5Exception {

		final Version version = getVersion(path);
		if (!ZarrV3KeyValueReader.VERSION.isCompatible(version))
			throw new N5IOException(
					"Incompatible version " + version + " (this is " + ZarrV3KeyValueReader.VERSION + ").");

		// This writer may only write zarr v3
		if (!ZarrV3KeyValueReader.VERSION.equals(version))
			setRawAttribute(path,
					ZarrV3DatasetAttributes.ZARR_FORMAT_KEY,
					ZarrV3KeyValueReader.VERSION.getMajor());
	}

	// TODO [+]
	@Override
	public void createGroup(final String path) {

		if (groupExists(path)) {
			return;
		} else if (datasetExists(path)) {
			throw new N5Exception("Can't make a group on existing dataset.");
		}

		final N5GroupPath group = N5GroupPath.of(path);
		final N5GroupPath parent = group.parent();
		if (parent != null) {
			createGroup(parent.path());
		}

		final JsonObject obj = new JsonObject();
		obj.addProperty(ZarrV3Node.ZARR_FORMAT_KEY, ZarrV3KeyValueReader.VERSION.getMajor());
		obj.addProperty(ZarrV3Node.NODE_TYPE_KEY, NodeType.GROUP.toString());
		metaStore.store_createDirectories(group);
		metaStore.store_writeAttributesJson(group, ZARR_KEY, obj, gson);
	}

	@Override
	public ZarrV3DatasetAttributes createDataset(
			final String datasetPath,
			final long[] dimensions,
			final int[] blockSize,
			final DataType dataType,
			final Compression compression) {

		final ZarrV3DatasetAttributes datasetAttributes = new ZarrV3DatasetAttributes(dimensions, blockSize, dataType, compression);
		return createDataset(datasetPath, datasetAttributes);
	}

	// TODO [+]
	/**
	 * Creates a dataset at the given path only without the associated checks or recursion.
	 * Expected to be called from a method that ensures the parent of `normalPath` exists
	 * and can contain a dataset at `normalPath`.
	 */
	private void createDatasetNonrecursive(final N5GroupPath dataset, final ZarrV3DatasetAttributes datasetAttributes) {

		metaStore.store_createDirectories(dataset);

		// These three lines are preferable to setDatasetAttributes because they
		// are more efficient wrt caching
		final JsonElement attributes = getGson().toJsonTree(datasetAttributes);
		final JsonObject zarrJson = attributes.getAsJsonObject();
		// NB: getAsJsonObject already populates ZARR_FORMAT_KEY and NODE_TYPE_KEY
		metaStore.store_writeAttributesJson(dataset, ZARR_KEY, zarrJson, gson);
	}

	// TODO [+]
	@Override
	public ZarrV3DatasetAttributes createDataset(String datasetPath, DatasetAttributes datasetAttributes) throws N5Exception {

		if (datasetExists(datasetPath)) {
			throw new N5Exception("Can't make a dataset on existing dataset.");
		}

		final N5GroupPath dataset = N5GroupPath.of(datasetPath);
		final N5GroupPath parent = dataset.parent();
		if (parent != null) {
			createGroup(parent.path());
		}
		final ZarrV3DatasetAttributes zarrAttrs = getConvertedDatasetAttributes(datasetAttributes);
		createDatasetNonrecursive(dataset, zarrAttrs);
		return zarrAttrs;
	}

	@Override
	public ZarrV3DatasetAttributes getConvertedDatasetAttributes(DatasetAttributes datasetAttributes) {
		final ZarrV3DatasetAttributes zarrAttrs;
		if (datasetAttributes instanceof ZarrV3DatasetAttributes)
			zarrAttrs = ((ZarrV3DatasetAttributes)datasetAttributes);
		else if (datasetAttributesMap.containsKey(datasetAttributes)) {
			zarrAttrs = datasetAttributesMap.get(datasetAttributes);
			datasetAttributesMap.put(datasetAttributes, zarrAttrs);
		}
		else {
			zarrAttrs = ZarrV3DatasetAttributes.from(datasetAttributes, dimensionSeparator, "0");
			datasetAttributesMap.put(datasetAttributes, zarrAttrs);
		}
		return zarrAttrs;
	}

	public <T> void setRawAttribute(final String groupPath, final String attributePath, final T attribute)
			throws N5Exception {

		setRawAttributes(groupPath, Collections.singletonMap(attributePath, attribute));
	}

	@Override
	public <T> void setAttribute(
			final String groupPath,
			final String attributePath,
			final T attribute) throws N5Exception {

		final String normalizedAttributePath = N5URI.normalizeAttributePath(attributePath);
		setRawAttribute(groupPath, mapAttributeKey(normalizedAttributePath), attribute);
	}

	/**
	 * Converts an attribute path
	 *
	 * @param attributePath
	 * @return
	 */
	protected String mapAttributeKey(final String attributePath) {

		return isAttributes(attributePath) ? attributePath : ZarrV3Node.ATTRIBUTES_KEY + "/" + attributePath;
	}

	protected boolean isAttributes(final String attributePath) {

		if (!Arrays.stream(ZarrV3DatasetAttributes.REQUIRED_KEYS).anyMatch(attributePath::equals))
			return false;

		return true;
	}

	// TODO [+]
	public void setRawAttributes(final String path, final Map<String, ?> attributes) throws N5Exception {

		if (!metaStore.store_isDirectory(N5GroupPath.of(path)))
			throw new N5IOException("\"" + path + "\" is not a group or dataset.");

		if (attributes != null && !attributes.isEmpty()) {
			JsonElement root = getRawAttributes(path);
			root = root != null && root.isJsonObject()
					? root.getAsJsonObject()
					: new JsonObject();
			root = GsonUtils.insertAttributes(root, attributes, getGson());
			setRawAttributes(path, root);
		}
	}

	// TODO [+]
	public void setRawAttributes(final String path, final JsonElement attributes) throws N5Exception {

		final N5GroupPath group = N5GroupPath.of(path);

		if (!metaStore.store_isDirectory(group))
			throw new N5IOException("\"" + path + "\" is not a group or dataset.");

		getDelegateStore().store_writeAttributesJson(group, ZARR_KEY, attributes, getGson());
	}

	// TODO [ ]
	@Override
	public void setAttributes(
			final String path,
			final Map<String, ?> attributes) throws N5Exception {

		if (!exists(path))
			throw new N5IOException(path + " is not a group or dataset.");

		if (attributes != null && !attributes.isEmpty()) {
			JsonElement root = getRawAttributes(path);
			root = root != null && root.isJsonObject()
					? root.getAsJsonObject()
					: new JsonObject();

			final JsonObject rootObj = root.getAsJsonObject();
			if (!rootObj.has(ZarrV3Node.ATTRIBUTES_KEY))
				rootObj.add(ZarrV3Node.ATTRIBUTES_KEY, new JsonObject());

			final JsonElement userAttrs = rootObj.get(ZarrV3Node.ATTRIBUTES_KEY);
			GsonUtils.insertAttributes(userAttrs, attributes, getGson());

			setRawAttributes(path, root);
		}
	}

	// TODO [ ]
	@Override
	public void setAttributes(
			final String path,
			final JsonElement attributes) throws N5Exception {

		final JsonElement root = getRawAttributes(path);
		final JsonObject rootObj = root.getAsJsonObject();
		rootObj.add(ZarrV3Node.ATTRIBUTES_KEY, attributes);
		setRawAttributes(path, rootObj);
	}


	// TODO [+]
	@Override
	public boolean removeAttribute(final String pathName, final String attributePath) throws N5Exception {

		final String normalKey = N5URI.normalizeAttributePath(ZarrV3Node.ATTRIBUTES_KEY + "/" + attributePath);
		final JsonElement attributes = getRawAttributes(pathName);
		if (GsonUtils.removeAttribute(attributes, normalKey) != null) {
			setRawAttributes(pathName, attributes);
			return true;
		}
		return false;
	}

	// TODO [+]
	@Override
	public <T> T removeAttribute(final String pathName, final String key, final Class<T> cls) throws N5Exception {

		final String normalKey = N5URI.normalizeAttributePath(ZarrV3Node.ATTRIBUTES_KEY + "/" + key);
		final JsonElement attributes = getRawAttributes(pathName);
		final T obj;
		try {
			obj = GsonUtils.removeAttribute(attributes, normalKey, cls, getGson());
		} catch (JsonSyntaxException | NumberFormatException | ClassCastException e) {
			throw new N5Exception.N5ClassCastException(e);
		}
		if (obj != null) {
			setRawAttributes(pathName, attributes);
		}
		return obj;
	}
}
