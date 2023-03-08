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

import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import net.imglib2.Cursor;
import net.imglib2.FinalInterval;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.basictypeaccess.array.ByteArray;
import net.imglib2.type.numeric.integer.ByteType;
import net.imglib2.util.Intervals;
import net.imglib2.view.Views;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.janelia.saalfeldlab.n5.BlockWriter;
import org.janelia.saalfeldlab.n5.ByteArrayDataBlock;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.GsonN5Writer;
import org.janelia.saalfeldlab.n5.KeyValueAccess;
import org.janelia.saalfeldlab.n5.LockedChannel;
import org.janelia.saalfeldlab.n5.N5URL;

/**
 * Zarr {@link KeyValueWriter} implementation.
 *
 * @author Stephan Saalfeld
 * @author John Bogovic
 */
public class ZarrKeyValueWriter extends ZarrKeyValueReader implements GsonN5Writer {

	protected String dimensionSeparator;

	/**
	 * Opens an {@link ZarrKeyValueWriter} at a given base path with a custom
	 * {@link GsonBuilder} to support custom attributes.
	 *
	 * If the base path does not exist, it will be created.
	 *
	 * If the base path exists and if the N5 version of the container is
	 * compatible with this implementation, the N5 version of this container
	 * will be set to the current N5 version of this implementation.
	 *
	 * @param keyValueAccess
	 * @param basePath n5 base path
	 * @param gsonBuilder
	 * @param mapN5DatasetAttributes
	 *	  Virtually create N5 dataset attributes (dimensions, blockSize,
	 * 	  compression, dataType) for datasets such that N5 code that
	 * 	  reads or modifies these attributes directly works as expected.
	 * 	  This can lead to name collisions if a zarr container uses these
	 * 	  attribute keys for other purposes.
	 * @param cacheAttributes cache attributes
	 *    Setting this to true avoids frequent reading and parsing of JSON
	 *    encoded attributes, this is most interesting for high latency file
	 *    systems. Changes of attributes by an independent writer will not be
	 *    tracked.
	 *
	 * @throws IOException
	 *    if the base path cannot be written to or cannot becreated.
	 */
	public ZarrKeyValueWriter(
			final KeyValueAccess keyValueAccess,
			final String basePath,
			final GsonBuilder gsonBuilder,
			final String dimensionSeparator,
			final boolean mapN5DatasetAttributes,
			final boolean cacheAttributes) throws IOException {

		super(keyValueAccess, basePath, gsonBuilder, mapN5DatasetAttributes, cacheAttributes);
		this.dimensionSeparator = dimensionSeparator;
//		keyValueAccess.createDirectories(groupPath(basePath));
	}

	/**
	 * Helper method to create and cache a group.
	 *
	 * @param normalPath normalized group path without leading slash
	 * @return
	 * @throws IOException
	 */
	protected N5GroupInfo createCachedGroup(final String normalPath) throws IOException {

		N5GroupInfo info = getCachedN5GroupInfo(normalPath);
		if (info == emptyGroupInfo) {

			/* The directories may be created multiple times concurrently,
			 * but a new cache entry is inserted only if none has been
			 * inserted in the meantime (because that may already include
			 * more cached data).
			 *
			 * This avoids synchronizing on the cache for independent
			 * group creation.
			 */
			keyValueAccess.createDirectories(groupPath(normalPath));
			synchronized (metaCache) {
				info = getCachedN5GroupInfo(normalPath);
				if (info == emptyGroupInfo) {
					info = new N5GroupInfo();
					metaCache.put(normalPath, info);
				}
				for (String childPathName = normalPath; !(childPathName == null || childPathName.equals(""));) {
					final String parentPathName = keyValueAccess.parent(childPathName);
					if (parentPathName == null)
						break;
					N5GroupInfo parentInfo = getCachedN5GroupInfo(parentPathName);
					if (parentInfo == emptyGroupInfo) {
						parentInfo = new N5GroupInfo();
						parentInfo.isDataset = false;
						metaCache.put(parentPathName, parentInfo);
					}
					HashSet<String> children = parentInfo.children;
					if (children == null) {
						children = new HashSet<>();
					}
					synchronized (children) {
						children.add(
								keyValueAccess.relativize(childPathName, parentPathName));
						parentInfo.children = children;
					}
					childPathName = parentPathName;
				}
			}
		}
		return info;
	}

	@Override
	public void createGroup(final String path) throws IOException {

		final String normalPath = N5URL.normalizePath(path);
		if (cacheMeta) {
			final N5GroupInfo info = createCachedGroup(normalPath);
			synchronized (info) {
				if (info.isDataset == null)
					info.isDataset = false;
			}
		} else
		{
			final String absPath = groupPath(normalPath);
			keyValueAccess.createDirectories(absPath);
			setGroupVersion(absPath);
		}

		// TODO fix
//		final String root = basePath;
//		for ( setGroupVersion(parent); !parent.equals(root);) {
//			final String[] comps = keyValueAccess.components(parent);
//			parent = keyValueAccess.compose( Arrays.stream(comps).limit( comps.length - 1 ).toArray( String[]::new ));
//			setGroupVersion(parent);
//		}
	}

	protected void setGroupVersion(final String groupPath) throws IOException {

		final String path = zGroupPath(groupPath);
		final JsonObject obj = new JsonObject();
		obj.addProperty("zarr_format", N5ZarrReader.VERSION.getMajor());
		writeAttributes(path, obj);
	}

	@Override
	public void createDataset(
			final String path,
			final DatasetAttributes datasetAttributes) throws IOException {

		final String normalPath = N5URL.normalizePath(path);
		if (cacheMeta) {
			final N5GroupInfo info = createCachedGroup(normalPath);
			synchronized (info) {
				setDatasetAttributes(normalPath, datasetAttributes);
				info.isDataset = true;
			}
		} else {
			createGroup(path);
			setDatasetAttributes(normalPath, datasetAttributes);
		}
	}
	
	@Override
	public void setDatasetAttributes(
			final String pathName,
			final DatasetAttributes datasetAttributes) throws IOException {

		final long[] shape = datasetAttributes.getDimensions().clone();
		Utils.reorder(shape);
		final int[] chunks = datasetAttributes.getBlockSize().clone();
		Utils.reorder(chunks);

		final ZArrayAttributes zArrayAttributes = new ZArrayAttributes(
				N5ZarrReader.VERSION.getMajor(),
				shape,
				chunks,
				new DType(datasetAttributes.getDataType()),
				ZarrCompressor.fromCompression(datasetAttributes.getCompression()),
				"0",
				'C',
				dimensionSeparator,
				null);

		setZArrayAttributes(pathName, zArrayAttributes);
	}

	public void setZArrayAttributes(
			final String pathName,
			final ZArrayAttributes attributes) throws IOException {

		final String absPath = keyValueAccess.compose(basePath, zArrayPath(N5URL.normalizePath(pathName)));
		writeAttributes(absPath, attributes.asMap());
	}

	/**
	 * Helper method that reads the existing map of attributes, JSON encodes,
	 * inserts and overrides the provided attributes, and writes them back into
	 * the attributes store.
	 *
	 * @param path
	 * @param attributes
	 * @throws IOException
	 */
	protected void writeAttributes(
			final String absolutePath,
			final JsonElement attributes) throws IOException {

		try (final LockedChannel lock = keyValueAccess.lockForWriting(absolutePath)) {
			final JsonElement root = GsonN5Writer.insertAttribute(readAttributes(lock.newReader()), "/", attributes, gson);
			GsonN5Writer.writeAttributes(lock.newWriter(), root, gson);
		}
	}

	protected void writeAttributes(
			final String normalPath,
			final Map<String, ?> attributes) throws IOException {
		if (!attributes.isEmpty()) {
//			createGroup(normalPath);
			final JsonElement existingAttributes = getAttributes(normalPath);
			JsonElement newAttributesJson = existingAttributes != null && existingAttributes.isJsonObject() ? existingAttributes.getAsJsonObject() : new JsonObject();
			newAttributesJson = GsonN5Writer.insertAttributes(newAttributesJson, attributes, gson);
			writeAttributes(normalPath, newAttributesJson);
		}
	}
	
	/**
	 * Constructs the absolute path (in terms of this store) for the attributes
	 * file of a group or dataset.
	 *
	 * @param normalPath normalized group path without leading slash
	 * @return
	 */
	@Override
	protected String attributesPath(final String normalPath) {


		return keyValueAccess.compose(basePath, normalPath, jsonFile);
	}

	/**
	 * Check for attributes that are required for a group to be a dataset.
	 *
	 * @param cachedAttributes
	 * @return
	 */
	protected static boolean hasCachedDatasetAttributes(final JsonElement cachedAttributes) {

		if (cachedAttributes == null || !cachedAttributes.isJsonObject()) {
			return false;
		}

		final JsonObject metadataCache = cachedAttributes.getAsJsonObject();
		return metadataCache.has(ZArrayAttributes.shapeKey) && metadataCache.has(ZArrayAttributes.dTypeKey);
	}


	/**
	 * Helper method to cache and write attributes.
	 *
	 * @param normalPath normalized group path without leading slash
	 * @param attributes
	 * @return
	 * @throws IOException
	 */
	protected N5GroupInfo setCachedAttributes(
			final String normalPath,
			final Map<String, ?> attributes) throws IOException {

		N5GroupInfo info = getCachedN5GroupInfo(normalPath);
		if (info == emptyGroupInfo) {
			createGroup(normalPath);
			synchronized (metaCache) {
				info = getCachedN5GroupInfo(normalPath);
				if (info == emptyGroupInfo)
					throw new IOException("N5 group '" + normalPath + "' does not exist. Cannot set attributes.");
			}
		}
		final JsonElement metadataCache = getCachedAttributes(info, normalPath);
		synchronized (info) {
			/* Necessary ensure `nulls` are treated consistently regardless of reading from the cache or not */
			info.attributesCache = gson.toJsonTree(GsonN5Writer.insertAttributes(metadataCache, attributes, gson));
			writeAttributes(normalPath, info.attributesCache);
			info.isDataset = hasCachedDatasetAttributes(info.attributesCache);
		}
		return info;
	}

	@Override
	public void setAttributes(
			final String path,
			final Map<String, ?> attributes) throws IOException {

		final String normalPath = N5URL.normalizePath(path);
		if (cacheMeta)
			setCachedAttributes(normalPath, attributes);
		else
			writeAttributes(normalPath, attributes);
	}

	@Override public boolean removeAttribute(String pathName, String key) throws IOException {

		final String normalPath = N5URL.normalizePath(pathName);
		final String normalKey = N5URL.normalizeAttributePath(key);


		if (cacheMeta) {
			N5GroupInfo info;
			synchronized (metaCache) {
				info = getCachedN5GroupInfo(normalPath);
				if (info == emptyGroupInfo) {
					throw new IOException("N5 group '" + normalPath + "' does not exist. Cannot set attributes.");
				}
			}
			final JsonElement metadataCache = getCachedAttributes(info, normalPath);
			if (GsonN5Writer.removeAttribute(metadataCache, normalKey) != null) {
				writeAttributes(normalPath, metadataCache);
				return true;
			}
		} else {
			final JsonElement attributes = getAttributes(normalPath);
			if (GsonN5Writer.removeAttribute(attributes, normalKey) != null) {
				writeAttributes(normalPath, attributes);
				return true;
			}
		}
		return false;
	}

	@Override public <T> T removeAttribute(String pathName, String key, Class<T> cls) throws IOException {
		final String normalPath = N5URL.normalizePath(pathName);
		final String normalKey = N5URL.normalizeAttributePath(key);

		if (cacheMeta) {
			N5GroupInfo info;
			synchronized (metaCache) {
				info = getCachedN5GroupInfo(normalPath);
				if (info == emptyGroupInfo) {
					throw new IOException("N5 group '" + normalPath + "' does not exist. Cannot set attributes.");
				}
			}
			final JsonElement metadataCache = getCachedAttributes(info, normalPath);
			final T obj = GsonN5Writer.removeAttribute(metadataCache, normalKey, cls, gson);
			if (obj != null) {
				writeAttributes(normalPath, metadataCache);
				return obj;
			}
		} else {
			final JsonElement attributes = getAttributes(normalPath);
			final T obj = GsonN5Writer.removeAttribute(attributes, normalKey, cls, gson);
			if (obj != null) {
				writeAttributes(normalPath, attributes);
				return obj;
			}
		}
		return null;
	}

	@Override public boolean removeAttributes(String pathName, List<String> attributes) throws IOException {
		final String normalPath = N5URL.normalizePath(pathName);
		boolean removed = false;
		for (String attribute : attributes) {
			final String normalKey = N5URL.normalizeAttributePath(attribute);
			removed |= removeAttribute(normalPath, attribute);
		}
		return removed;
	}

	/**
	 * Writes a {@link DataBlock} into an {@link OutputStream}.
	 *
	 * @param out
	 * @param datasetAttributes
	 * @param dataBlock
	 * @throws IOException
	 */
	public static <T> void writeBlock(
			final OutputStream out,
			final ZarrDatasetAttributes datasetAttributes,
			final DataBlock<T> dataBlock) throws IOException {

		final int[] blockSize = datasetAttributes.getBlockSize();
		final DType dType = datasetAttributes.getDType();
		final DataOutputStream dos = new DataOutputStream(out);
		final BlockWriter writer = datasetAttributes.getCompression().getWriter();

		if (!Arrays.equals(blockSize, dataBlock.getSize())) {

			final byte[] padCropped = padCrop(
					dataBlock.toByteBuffer().array(),
					dataBlock.getSize(),
					blockSize,
					dType.getNBytes(),
					dType.getNBits(),
					datasetAttributes.getFillBytes());

			final DataBlock<byte[]> padCroppedDataBlock =
					new ByteArrayDataBlock(
							blockSize,
							dataBlock.getGridPosition(),
							padCropped);

			writer.write(padCroppedDataBlock, out);

		} else {

			writer.write(dataBlock, out);
		}
	}

	@Override
	public <T> void writeBlock(
			final String pathName,
			final DatasetAttributes datasetAttributes,
			final DataBlock<T> dataBlock) throws IOException {

		final ZarrDatasetAttributes zarrDatasetAttributes;
		if (datasetAttributes instanceof ZarrDatasetAttributes)
			zarrDatasetAttributes = (ZarrDatasetAttributes)datasetAttributes;
		else
			zarrDatasetAttributes = getZArrayAttributes(pathName).getDatasetAttributes();

		final String normalPath = N5URL.normalizePath(pathName);
		final String path = keyValueAccess.compose(
				basePath,
				normalPath,
				getZarrDataBlockPath(
						dataBlock.getGridPosition(),
						zarrDatasetAttributes.getDimensionSeparator(),
						zarrDatasetAttributes.isRowMajor()).toString());

		keyValueAccess.createDirectories( path );
		try ( final LockedChannel lockedChannel = keyValueAccess.lockForWriting(path)){

			writeBlock(
					lockedChannel.newOutputStream(),
					zarrDatasetAttributes,
					dataBlock);
		}
	}
	

	@Override
	public boolean remove(final String path) throws IOException {

		final String normalPath = N5URL.normalizePath(path);
		final String groupPath = groupPath(normalPath);
		if (cacheMeta) {
			synchronized (metaCache) {
				if (keyValueAccess.exists(groupPath)) {
					keyValueAccess.delete(groupPath);

					/* cache nonexistence for all prior children */
					for (final String key : metaCache.keySet()) {
						if (key.startsWith(normalPath))
							metaCache.put(key, emptyGroupInfo);
					}

					/* remove child from parent */
					final String parentPath = keyValueAccess.parent(normalPath);
					final N5GroupInfo parent = metaCache.get(parentPath);
					if (parent != null) {
						final HashSet<String> children = parent.children;
						if (children != null) {
							synchronized (children) {
								children.remove(keyValueAccess.relativize(normalPath, parentPath));
							}
						}
					}
				}
			}
		} else {
			if (keyValueAccess.exists(groupPath))
				keyValueAccess.delete(groupPath);
		}

		/* an IOException should have occurred if anything had failed midway */
		return true;
	}

	@Override
	public boolean deleteBlock(
			final String path,
			final long... gridPosition) throws IOException {

		final String blockPath = getDataBlockPath(N5URL.normalizePath(path), gridPosition);
		if (keyValueAccess.exists(blockPath))
			keyValueAccess.delete(blockPath);

		/* an IOException should have occurred if anything had failed midway */
		return true;
	}

	public static byte[] padCrop(
			final byte[] src,
			final int[] srcBlockSize,
			final int[] dstBlockSize,
			final int nBytes,
			final int nBits,
			final byte[] fill_value) {

		assert srcBlockSize.length == dstBlockSize.length : "Dimensions do not match.";

		final int n = srcBlockSize.length;

		if (nBytes != 0) {
			final int[] srcStrides = new int[n];
			final int[] dstStrides = new int[n];
			final int[] srcSkip = new int[n];
			final int[] dstSkip  = new int[n];
			srcStrides[0] = dstStrides[0] = nBytes;
			for (int d = 1; d < n; ++d) {
				srcStrides[d] = srcBlockSize[d] * srcBlockSize[d - 1];
				dstStrides[d] = dstBlockSize[d] * dstBlockSize[d - 1];
			}
			for (int d = 0; d < n; ++d) {
				srcSkip[d] = Math.max(1, dstBlockSize[d] - srcBlockSize[d]);
				dstSkip[d] = Math.max(1, srcBlockSize[d] - dstBlockSize[d]);
			}

			/* this is getting hairy, ImgLib2 alternative */
			/* byte images with 0-dimension d[0] * nBytes */
			final long[] srcIntervalDimensions = new long[n];
			final long[] dstIntervalDimensions = new long[n];
			srcIntervalDimensions[0] = srcBlockSize[0] * nBytes;
			dstIntervalDimensions[0] = dstBlockSize[0] * nBytes;
			for (int d = 1; d < n; ++d) {
				srcIntervalDimensions[d] = srcBlockSize[d];
				dstIntervalDimensions[d] = dstBlockSize[d];
			}

			final byte[] dst = new byte[(int)Intervals.numElements(dstIntervalDimensions)];
			/* fill dst */
			for (int i = 0, j = 0; i < n; ++i) {
				dst[i] = fill_value[j];
				if (++j == fill_value.length)
					j = 0;
			}
			final ArrayImg<ByteType, ByteArray> srcImg = ArrayImgs.bytes(src, srcIntervalDimensions);
			final ArrayImg<ByteType, ByteArray> dstImg = ArrayImgs.bytes(dst, dstIntervalDimensions);

			final FinalInterval intersection = Intervals.intersect(srcImg, dstImg);
			final Cursor<ByteType> srcCursor = Views.interval(srcImg, intersection).cursor();
			final Cursor<ByteType> dstCursor = Views.interval(dstImg, intersection).cursor();
			while (srcCursor.hasNext())
				dstCursor.next().set(srcCursor.next());

			return dst;
		} else {
			/* TODO deal with bit streams */
			return null;
		}
	}
}
