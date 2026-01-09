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

import java.util.Collections;
import java.util.Map;

import org.janelia.saalfeldlab.n5.CachedGsonKeyValueN5Writer;
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.GsonUtils;
import org.janelia.saalfeldlab.n5.KeyValueAccess;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.N5URI;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.zarr.v3.ZarrV3Node.NodeType;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;

/**
 * Zarr v3 {@link N5Writer} implementation.
 */
public class ZarrV3KeyValueWriter extends ZarrV3KeyValueReader implements CachedGsonKeyValueN5Writer {

	/**
     * Opens an {@link ZarrV3KeyValueWriter} at a given base path with a custom
     * {@link GsonBuilder} to support custom attributes.
     *
     * @param keyValueAccess
     * @param basePath        N5 base path
     * @param gsonBuilder
     * @param cacheAttributes cache attributes and meta data
     *                        Setting this to true avoids frequent reading and parsing of
     *                        JSON
     *                        encoded attributes and other meta data that requires accessing
     *                        the
     *                        store. This is most interesting for high latency backends.
     *                        Changes
     *                        of cached attributes and meta data by an independent writer
     *                        will
     *                        not be tracked.
     * @throws N5Exception if the base path cannot be read or does not exist,
     *                     if the N5 version of the container is not compatible with
     *                     this
     *                     implementation.
     */
	public ZarrV3KeyValueWriter(
			final KeyValueAccess keyValueAccess,
			final String basePath,
			final GsonBuilder gsonBuilder,
            final boolean cacheAttributes)
			throws N5Exception {

		super(false, keyValueAccess, basePath, gsonBuilder,
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

		final String normalPath = N5URI.normalizeGroupPath(path);
		final Version version = getVersion(path);
		if (!ZarrV3KeyValueReader.VERSION.isCompatible(version))
			throw new N5IOException(
					"Incompatible version " + version + " (this is " + ZarrV3KeyValueReader.VERSION + ").");

		// This writer may only write zarr v3
		if (!ZarrV3KeyValueReader.VERSION.equals(version))
			setRawAttribute(normalPath,
					ZarrV3DatasetAttributes.ZARR_FORMAT_KEY,
					ZarrV3KeyValueReader.VERSION.getMajor());
	}

	/**
	 * Creates a group at the given path only without the associated checks or recursion.
	 * Expected to be called from a method that ensures the parent of `normalPath` exists
	 * and can contain a group at `normalPath`.
	 *
	 * @param normalPath path to group relative to root
	 */
	private void createGroupNonrecursive(final String normalPath) {

		if (groupExists(normalPath))
			return;
		else if (datasetExists(normalPath))
			throw new N5Exception("Can't make a group on existing dataset.");

		getKeyValueAccess().createDirectories(absoluteGroupPath(normalPath));

		final JsonObject obj = new JsonObject();
		obj.addProperty(ZarrV3DatasetAttributes.ZARR_FORMAT_KEY, ZarrV3KeyValueReader.VERSION.getMajor());
		obj.addProperty(ZarrV3Node.NODE_TYPE_KEY, NodeType.GROUP.toString());

		writeAttributes(normalPath, obj);
	}

	@Override
	public void createGroup(final String path) {


		final String normalPath = N5URI.normalizeGroupPath(path);
		if (groupExists(normalPath))
			return;
		else if (datasetExists(normalPath))
			throw new N5Exception("Can't make a group on existing dataset.");

		String[] pathParts = getKeyValueAccess().components(normalPath);

		// check all nodes that are parents of the added node, if they have
		// a children set, add the new child to it
		String parent = N5URI.normalizeGroupPath("/");
		if (pathParts.length == 0) {
			pathParts = new String[]{""};
		}
		for (final String child : pathParts) {

			final String childPath = parent.isEmpty() ? child : parent + "/" + child;
			createGroupNonrecursive(childPath);

			if (cacheMeta()) {
				// only add if the parent exists and has children cached already
				if (parent != null && !child.isEmpty())
					getCache().addChildIfPresent(parent, child);
			}

			parent = childPath;
		}
	}



	/**
	 * Creates a dataset at the given path only without the associated checks or recursion.
	 * Expected to be called from a method that ensures the parent of `normalPath` exists
	 * and can contain a dataset at `normalPath`.
	 *
	 * @param normalPath path to group relative to root
	 */
	private void createDatasetNonrecursive(final String normalPath, final ZarrV3DatasetAttributes datasetAttributes) {

		getKeyValueAccess().createDirectories(absoluteGroupPath(normalPath));

		// These three lines are preferable to setDatasetAttributes because they
		// are more efficient wrt caching
		final JsonElement attributes = gson.toJsonTree(datasetAttributes);
		final JsonObject zarrJson = attributes.getAsJsonObject();
		zarrJson.addProperty(ZarrV3DatasetAttributes.ZARR_FORMAT_KEY, ZarrV3KeyValueReader.VERSION.getMajor());
		zarrJson.addProperty(ZarrV3Node.NODE_TYPE_KEY, NodeType.ARRAY.toString());
		writeAttributes(normalPath, zarrJson);
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

	@Override
	public ZarrV3DatasetAttributes createDataset(String datasetPath, DatasetAttributes datasetAttributes) throws N5Exception {

		final String normalPath = N5URI.normalizeGroupPath(datasetPath);
		if (datasetExists(normalPath)) {
			throw new N5Exception("Can't make a dataset on existing dataset.");
		}

		final String parent;
		if (!normalPath.contains("/"))
			parent = null;
		else
			parent = N5URI.normalizeGroupPath(normalPath + "/..");

		if (parent != null) {
			createGroup(parent);
		}

		final ZarrV3DatasetAttributes zarrAttrs = getConvertedDatasetAttributes(datasetAttributes);
		createDatasetNonrecursive(normalPath, zarrAttrs);

		if (cacheMeta() && parent != null) {
			// only add if the parent exists and has children cached already
			final String child = getKeyValueAccess().relativize(normalPath, parent);
			if (parent != null && !child.isEmpty())
				getCache().addChildIfPresent(parent, child);
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

	public void setRawAttributes(final String path, final Map<String, ?> attributes) throws N5Exception {

		// TODO should this and other raw attribute methods be protected?
		// maybe best to have single public get/setRawAttributes(path,JsonObject)
		CachedGsonKeyValueN5Writer.super.setAttributes(path, attributes);
	}

	public void setRawAttributes(final String path, final JsonElement attributes) throws N5Exception {

		CachedGsonKeyValueN5Writer.super.setAttributes(path, attributes);
	}

	public void setAttributes(
			final String path,
			final Map<String, ?> attributes) throws N5Exception {

		final String normalPath = N5URI.normalizeGroupPath(path);
		if (!exists(normalPath))
			throw new N5IOException("" + normalPath + " is not a group or dataset.");

		if (attributes != null && !attributes.isEmpty()) {
			JsonElement root = getRawAttributes(normalPath);
			root = root != null && root.isJsonObject()
					? root.getAsJsonObject()
					: new JsonObject();

			final JsonObject rootObj = root.getAsJsonObject();
			if (!rootObj.has(ZarrV3Node.ATTRIBUTES_KEY))
				rootObj.add(ZarrV3Node.ATTRIBUTES_KEY, new JsonObject());

			JsonElement userAttrs = rootObj.get(ZarrV3Node.ATTRIBUTES_KEY);
			userAttrs = GsonUtils.insertAttributes(userAttrs, attributes, getGson());

			writeAttributes(normalPath, root);
		}
	}

	@Override
	public void setAttributes(
			final String path,
			final JsonElement attributes) throws N5Exception {

		final JsonElement root = getRawAttributes(path);
		final JsonObject rootObj = root.getAsJsonObject();
		rootObj.add(ZarrV3Node.ATTRIBUTES_KEY, attributes);
		setRawAttributes(path, rootObj);
	}

	@Override
	public <T> T removeAttribute(final String pathName, final String key, final Class<T> cls) throws N5Exception {

		final String normalPath = N5URI.normalizeGroupPath(pathName);
		final String normalKey = N5URI.normalizeAttributePath(ZarrV3Node.ATTRIBUTES_KEY + "/" + key);
		final JsonElement attributes = getRawAttributes(normalPath);
		final T obj;
		try {
			obj = GsonUtils.removeAttribute(attributes, normalKey, cls, getGson());
		} catch (JsonSyntaxException | NumberFormatException | ClassCastException e) {
			throw new N5Exception.N5ClassCastException(e);
		}
		if (obj != null) {
			writeAttributes(normalPath, attributes);
		}
		return obj;
	}

	@Override
	public void writeAttributes(
			final String normalGroupPath,
			final Map<String, ?> attributes) throws N5Exception {

		if (attributes != null && !attributes.isEmpty()) {
			JsonElement root = getRawAttributes(normalGroupPath);
			root = root != null && root.isJsonObject()
					? root.getAsJsonObject()
					: new JsonObject();
			root = GsonUtils.insertAttributes(root, attributes, getGson());
			writeAttributes(normalGroupPath, root);
		}
	}

	protected static String userAttributePath( final String key ) {

		return ZarrV3Node.ATTRIBUTES_KEY + "/" + key;
	}

}
