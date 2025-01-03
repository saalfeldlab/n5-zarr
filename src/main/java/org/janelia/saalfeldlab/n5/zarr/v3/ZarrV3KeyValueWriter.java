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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.janelia.saalfeldlab.n5.CachedGsonKeyValueN5Writer;
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.GsonUtils;
import org.janelia.saalfeldlab.n5.KeyValueAccess;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.N5URI;
import org.janelia.saalfeldlab.n5.codec.BytesCodec;
import org.janelia.saalfeldlab.n5.codec.Codec;
import org.janelia.saalfeldlab.n5.zarr.DType;
import org.janelia.saalfeldlab.n5.zarr.chunks.ChunkAttributes;
import org.janelia.saalfeldlab.n5.zarr.chunks.DefaultChunkKeyEncoding;
import org.janelia.saalfeldlab.n5.zarr.chunks.RegularChunkGrid;
import org.janelia.saalfeldlab.n5.zarr.v3.ZarrV3Node.NodeType;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

/**
 * Zarr {@link KeyValueWriter} implementation.
 *
 * @author Stephan Saalfeld
 * @author John Bogovic
 */
public class ZarrV3KeyValueWriter extends ZarrV3KeyValueReader implements CachedGsonKeyValueN5Writer {

	protected String dimensionSeparator;


	public ZarrV3KeyValueWriter(
			final KeyValueAccess keyValueAccess,
			final String basePath,
			final GsonBuilder gsonBuilder,
			final boolean mapN5DatasetAttributes,
			final boolean mergeAttributes,
			final String dimensionSeparator,
			final boolean cacheAttributes)
			throws N5Exception {

		super(false, keyValueAccess, basePath, gsonBuilder,
				mapN5DatasetAttributes, mergeAttributes,
				cacheAttributes, false);

		this.dimensionSeparator = dimensionSeparator;

		Version version = null;
		try {
			version = getVersion();
			if (!ZarrV3KeyValueReader.VERSION.isCompatible(version))
				throw new N5Exception.N5IOException(
						"Incompatible version " + version + " (this is " + ZarrV3KeyValueReader.VERSION + ").");
		} catch (final NullPointerException e) {}

		if (version == null || version.equals(new Version(0, 0, 0, ""))) {
			createGroup("/");
			setVersion("/");
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
			setRawAttribute(normalPath, ZarrV3DatasetAttributes.ZARR_FORMAT_KEY, 3);
	}

	@Override
	public void createGroup(final String path) {

		final String normalPath = N5URI.normalizeGroupPath(path);
		CachedGsonKeyValueN5Writer.super.createGroup(normalPath);

		// TODO possible to optimize by writing once
		setVersion(normalPath);
		setRawAttribute(normalPath, ZarrV3Node.NODE_TYPE_KEY, NodeType.GROUP.toString());
	}

	@Override
	public void createDataset(
			final String datasetPath,
			final long[] dimensions,
			final int[] blockSize,
			final DataType dataType,
			final Compression compression) throws N5Exception {

		createDataset(datasetPath, new DatasetAttributes(dimensions, blockSize, dataType, null,
				new Codec[]{new BytesCodec(), compression}));
	}

	@Override
	public void createDataset(
			final String path,
			final DatasetAttributes datasetAttributes) throws N5Exception {

		final String normalPath = N5URI.normalizeGroupPath(path);
		boolean wasGroup = false;
		if (cacheMeta()) {
			if (getCache().isDataset(normalPath, getAttributesKey()))
				return;
			else if (getCache().isGroup(normalPath, getAttributesKey())) {
				wasGroup = true;
				// TODO tests currently require that we can make a dataset on a group
				// throw new N5Exception("Can't make a group on existing path.");
			}
		}

		// Overriding because CachedGsonKeyValueWriter calls createGroup.
		// Not correct for zarr, since groups and datasets are mutually
		// exclusive
		final String absPath = absoluteGroupPath(normalPath);
		try {
			keyValueAccess.createDirectories(absPath);
		} catch (final Throwable e) {
			throw new N5IOException("Failed to create directories " + absPath, e);
		}

		// create parent group
		final String[] pathParts = keyValueAccess.components(normalPath);
		final String parent = Arrays.stream(pathParts).limit(pathParts.length - 1).collect(Collectors.joining("/"));
		createGroup(parent);

		// These three lines are preferable to setDatasetAttributes because they
		// are more efficient wrt caching
		final ZarrV3DatasetAttributes zarray = createZArrayAttributes(datasetAttributes);
		final HashMap<String, Object> zarrayMap = zarray.asMap();
		final JsonElement attributes = gson.toJsonTree(zarrayMap);
		writeAttributes(normalPath, attributes);
	}

	protected ZarrV3DatasetAttributes createZArrayAttributes(final DatasetAttributes datasetAttributes) {

		final long[] shape = datasetAttributes.getDimensions().clone();
		final int[] chunkShape = datasetAttributes.getBlockSize().clone();
		final ChunkAttributes chunkAttrs = new ChunkAttributes(
				new RegularChunkGrid(chunkShape),
				new DefaultChunkKeyEncoding(dimensionSeparator));

		final ZarrV3DatasetAttributes zArrayAttributes = new ZarrV3DatasetAttributes(
				ZarrV3KeyValueReader.VERSION.getMajor(),
				shape,
				chunkAttrs,
				ZarrV3DataType.fromDataType(datasetAttributes.getDataType()),
				"0",
				datasetAttributes.getCompression(),
				prependArrayToBytes(datasetAttributes.getArrayCodec(), datasetAttributes.getCodecs()));

		return zArrayAttributes;
	}

	private static Codec[] prependArrayToBytes(Codec.ArrayCodec arrayToBytes, Codec[] codecs) {

		final Codec[] out = new Codec[codecs.length + 1];
		out[0] = arrayToBytes;
		System.arraycopy(codecs, 0, out, 1, codecs.length);
		return out;
	}

	public <T> void setRawAttribute(
			final String groupPath,
			final String attributePath,
			final T attribute) throws N5Exception {

		setRawAttributes(groupPath, Collections.singletonMap(attributePath, attribute));
	}

	@Override
	public <T> void setAttribute(
			final String groupPath,
			final String attributePath,
			final T attribute) throws N5Exception {

		setRawAttribute(groupPath, mapAttributeKey(attributePath), attribute);
	}

	public void setRawAttributes(final String path, final Map<String, ?> attributes) throws N5Exception {

		// TODO should this and other raw attribute methods be protected?
		// maybe best to have single public get/setRawAttributes(path,JsonObject)
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

	public void setRawAttributes(final String path, final JsonElement attributes) throws N5Exception {

		CachedGsonKeyValueN5Writer.super.setAttributes(path, attributes);
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
