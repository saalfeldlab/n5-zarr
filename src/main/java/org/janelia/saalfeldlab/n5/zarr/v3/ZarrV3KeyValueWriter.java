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
import java.util.HashMap;
import java.util.stream.Collectors;

import org.janelia.saalfeldlab.n5.CachedGsonKeyValueN5Writer;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.KeyValueAccess;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.N5URI;
import org.janelia.saalfeldlab.n5.zarr.DType;
import org.janelia.saalfeldlab.n5.zarr.v3.ZarrV3Node.NodeType;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;

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
		final Version version = getVersion();
		if (!ZarrV3KeyValueReader.VERSION.isCompatible(version))
			throw new N5IOException(
					"Incompatible version " + version + " (this is " + ZarrV3KeyValueReader.VERSION + ").");

		// This writer may only write zarr v3
		if (!ZarrV3KeyValueReader.VERSION.equals(version))
			setAttribute(normalPath, ZarrV3DatasetAttributes.ZARR_FORMAT_KEY, 3);
	}

	@Override
	public void createGroup(final String path) {

		final String normalPath = N5URI.normalizeGroupPath(path);
		CachedGsonKeyValueN5Writer.super.createGroup(normalPath);

		// TODO possible to optimize by writing once
		setVersion(normalPath);
		setAttribute(normalPath, ZarrV3Node.NODE_TYPE_KEY, NodeType.GROUP.toString());
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
		reorder(shape);
		final int[] chunkShape = datasetAttributes.getBlockSize().clone();
		reorder(chunkShape);
		final DType dType = new DType(datasetAttributes.getDataType());

		final ZarrV3DatasetAttributes zArrayAttributes = new ZarrV3DatasetAttributes(
				ZarrV3KeyValueReader.VERSION.getMajor(),
				shape,
				chunkShape,
				dType,
				"0",
				dimensionSeparator,
				datasetAttributes.getCodecs());

		return zArrayAttributes;
	}

	@Override
	public <T> void setAttribute(
			final String groupPath,
			final String attributePath,
			final T attribute) throws N5Exception {

		CachedGsonKeyValueN5Writer.super.setAttribute(groupPath, mapAttributeKey(attributePath),
				attribute);
	}

	/**
	 * Converts an attribute path
	 *
	 * @param attributePath
	 * @return
	 */
	private String mapAttributeKey(final String attributePath) {

		final String key = mapN5DatasetAttributes ? n5AttributeKeyToZarr(attributePath) : attributePath;
		return isAttributes(key) ? ZarrV3Node.ATTRIBUTES_KEY + "/" + key : key;
	}

	private String n5AttributeKeyToZarr(final String attributePath) {

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

	private boolean isAttributes(final String attributePath) {

		if (!Arrays.stream(ZarrV3DatasetAttributes.requiredKeys).anyMatch(attributePath::equals))
			return false;

		if (mapN5DatasetAttributes &&
				!Arrays.stream(DatasetAttributes.N5_DATASET_ATTRIBUTES).anyMatch(attributePath::equals)) {
			return false;
		}

		return true;
	}

	private static void reorder(final long[] array) {

		long a;
		final int max = array.length - 1;
		for (int i = (max - 1) / 2; i >= 0; --i) {
			final int j = max - i;
			a = array[i];
			array[i] = array[j];
			array[j] = a;
		}
	}

	private static void reorder(final int[] array) {

		int a;
		final int max = array.length - 1;
		for (int i = (max - 1) / 2; i >= 0; --i) {
			final int j = max - i;
			a = array[i];
			array[i] = array[j];
			array[j] = a;
		}
	}

}
