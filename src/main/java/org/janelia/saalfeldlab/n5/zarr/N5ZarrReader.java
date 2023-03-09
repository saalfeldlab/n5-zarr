/*-
 * #%L
 * Not HDF5
 * %%
 * Copyright (C) 2019 - 2022 Stephan Saalfeld
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

import java.io.IOException;
import java.nio.file.FileSystems;

import org.janelia.saalfeldlab.n5.FileSystemKeyValueAccess;

import com.google.gson.GsonBuilder;

/**
 * @author Stephan Saalfeld &lt;saalfelds@janelia.hhmi.org&gt;
 * @author John Bogovic  &lt;bogovicj@janelia.hhmi.org&gt;
 *
 */
public class N5ZarrReader extends ZarrKeyValueReader {

	protected static Version VERSION = new Version(2, 0, 0);

	/**
	 * Opens an {@link N5ZarrReader} at a given base path with a custom
	 * {@link GsonBuilder} to support custom attributes.
	 *
	 * @param basePath Zarr base path
	 * @param gsonBuilder
	 * @param cacheMeta cache attributes and meta data
	 *    Setting this to true avoids frequent reading and parsing of JSON
	 *    encoded attributes and other meta data that requires accessing the
	 *    store. This is most interesting for high latency backends. Changes
	 *    of cached attributes and meta data by an independent writer on the
	 *    same container will not be tracked.
	 *
	 * @throws IOException
	 *    if the base path cannot be read or does not exist,
	 *    if the N5 version of the container is not compatible with this
	 *    implementation.
	 */
	public N5ZarrReader(final String basePath, final GsonBuilder gsonBuilder, final boolean cacheMeta) throws IOException {

		super(
				new FileSystemKeyValueAccess(FileSystems.getDefault()),
				basePath,
				gsonBuilder
					.registerTypeAdapter(DType.class, new DType.JsonAdapter())
					.registerTypeAdapter(ZarrCompressor.class, ZarrCompressor.jsonAdapter),
				true,
				cacheMeta);
	}

	/**
	 * Opens an {@link N5ZarrReader} at a given base path.
	 *
	 * @param basePath N5 base path
	 * @param cacheMeta cache attributes and meta data
	 *    Setting this to true avoids frequent reading and parsing of JSON
	 *    encoded attributes and other meta data that requires accessing the
	 *    store. This is most interesting for high latency backends. Changes
	 *    of cached attributes and meta data by an independent writer on the
	 *    same container will not be tracked.
	 *
	 * @throws IOException
	 *    if the base path cannot be read or does not exist,
	 *    if the N5 version of the container is not compatible with this
	 *    implementation.
	 */
	public N5ZarrReader(final String basePath, final boolean cacheMeta) throws IOException {

		this(basePath, new GsonBuilder(), cacheMeta);
	}

	/**
	 * Opens an {@link N5ZarrReader} at a given base path with a custom
	 * {@link GsonBuilder} to support custom attributes.
	 *
	 * @param basePath N5 base path
	 * @param gsonBuilder
	 * @throws IOException
	 *    if the base path cannot be read or does not exist,
	 *    if the N5 version of the container is not compatible with this
	 *    implementation.
	 */
	public N5ZarrReader(final String basePath, final GsonBuilder gsonBuilder) throws IOException {

		this(basePath, gsonBuilder, false);
	}

	/**
	 * Opens an {@link N5ZarrReader} at a given base path.
	 *
	 * @param basePath N5 base path
	 * @throws IOException
	 *    if the base path cannot be read or does not exist,
	 *    if the N5 version of the container is not compatible with this
	 *    implementation.
	 */
	public N5ZarrReader(final String basePath) throws IOException {

		this(basePath, new GsonBuilder(), false);
	}

//	@Override
//	public Version getVersion() throws IOException {
//
//		final Path path;
//		if (groupExists("/")) {
//			path = Paths.get(basePath, zgroupFile);
//		} else if (datasetExists("/")) {
//			path = Paths.get(basePath, zarrayFile);
//		} else {
//			return VERSION;
//		}
//
//		if (Files.exists(path)) {
//
//			try (final LockedFileChannel lockedFileChannel = LockedFileChannel.openForReading(path)) {
//				final HashMap<String, JsonElement> attributes =
//						GsonAttributesParser.readAttributes(
//								Channels.newReader(
//										lockedFileChannel.getFileChannel(),
//										StandardCharsets.UTF_8.name()),
//								gson);
//				final Integer zarr_format = GsonAttributesParser.parseAttribute(
//						attributes,
//						"zarr_format",
//						Integer.class,
//						gson);
//
//				if (zarr_format != null)
//					return new Version(zarr_format, 0, 0);
//			}
//		}
//		return VERSION;
//	}
//
//
//	public ZArrayAttributes getZArraryAttributes(final String pathName) throws IOException {
//
//		final Path path = Paths.get(basePath, removeLeadingSlash(pathName), zarrayFile);
//		final HashMap<String, JsonElement> attributes = new HashMap<>();
//
//		if (Files.exists(path)) {
//
//			try (final LockedFileChannel lockedFileChannel = LockedFileChannel.openForReading(path)) {
//				attributes.putAll(
//						GsonAttributesParser.readAttributes(
//								Channels.newReader(
//										lockedFileChannel.getFileChannel(),
//										StandardCharsets.UTF_8.name()),
//								gson));
//			}
//		} else System.out.println(path.toString() + " does not exist.");
//
//		JsonElement sepElem = attributes.get("dimension_separator");
//		return new ZArrayAttributes(
//				attributes.get("zarr_format").getAsInt(),
//				gson.fromJson(attributes.get("shape"), long[].class),
//				gson.fromJson(attributes.get("chunks"), int[].class),
//				gson.fromJson(attributes.get("dtype"), DType.class),
//				gson.fromJson(attributes.get("compressor"), ZarrCompressor.class),
//				attributes.get("fill_value").getAsString(),
//				attributes.get("order").getAsCharacter(),
//				sepElem != null ? sepElem.getAsString() : dimensionSeparator,
//				gson.fromJson(attributes.get("filters"), TypeToken.getParameterized(Collection.class, Filter.class).getType()));
//	}
//
//	@Override
//	public DatasetAttributes getDatasetAttributes(final String pathName) throws IOException {
//
//		final ZArrayAttributes zArrayAttributes = getZArraryAttributes(pathName);
//		return zArrayAttributes == null ? null : zArrayAttributes.getDatasetAttributes();
//	}
//
//	@Override
//	public boolean datasetExists(final String pathName) throws IOException {
//
//		final Path path = Paths.get(basePath, removeLeadingSlash(pathName), zarrayFile);
//		return Files.exists(path) && Files.isRegularFile(path) && getDatasetAttributes(pathName) != null;
//	}
//
//
//	/**
//	 * @returns false if the group or dataset does not exist but also if the
//	 * 		attempt to access
//	 */
//	@Override
//	public boolean exists(final String pathName) {
//
//		try {
//			return groupExists(pathName) || datasetExists(pathName);
//		} catch (final IOException e) {
//			e.printStackTrace();
//			return false;
//		}
//	}
//
//	@Override
//	public JsonElement getAttributesJson( final String pathName ) throws IOException
//	{
//		final Path path = Paths.get(basePath, removeLeadingSlash(pathName), zattrsFile);
//		if (!Files.exists(path))
//			return null;
//
//		JsonElement attributes;
//		try (final LockedFileChannel lockedFileChannel = LockedFileChannel.openForReading(path)) {
//			attributes = GsonAttributesParser.readAttributesJson(Channels.newReader(lockedFileChannel.getFileChannel(), StandardCharsets.UTF_8.name()), getGson());
//		}
//
//		if (mapN5DatasetAttributes && datasetExists(pathName)) {
//
//			final DatasetAttributes datasetAttributes = getZArraryAttributes(pathName).getDatasetAttributes();
//			attributes.getAsJsonObject().add("dimensions", gson.toJsonTree(datasetAttributes.getDimensions()));
//			attributes.getAsJsonObject().add("blockSize", gson.toJsonTree(datasetAttributes.getBlockSize()));
//			attributes.getAsJsonObject().add("dataType", gson.toJsonTree(datasetAttributes.getDataType()));
//			attributes.getAsJsonObject().add("compression", gson.toJsonTree(datasetAttributes.getCompression()));
//		}
//		return attributes;
//	}
//
//
//	@Override
//	public DataBlock<?> readBlock(
//			final String pathName,
//			final DatasetAttributes datasetAttributes,
//			final long... gridPosition) throws IOException {
//
//		final ZarrDatasetAttributes zarrDatasetAttributes;
//		if (datasetAttributes instanceof ZarrDatasetAttributes)
//			zarrDatasetAttributes = (ZarrDatasetAttributes)datasetAttributes;
//		else
//			zarrDatasetAttributes = getZArraryAttributes(pathName).getDatasetAttributes();
//
//		final String dimSep;
//		final String attrDimensionSeparator = zarrDatasetAttributes.getDimensionSeparator();
//		if (attrDimensionSeparator != null && !attrDimensionSeparator.isEmpty())
//			dimSep = attrDimensionSeparator;
//		else
//			dimSep = dimensionSeparator;
//
//		final Path path = Paths.get(
//				basePath,
//				removeLeadingSlash(pathName),
//				getZarrDataBlockPath(
//						gridPosition,
//						dimSep,
//						zarrDatasetAttributes.isRowMajor()).toString());
//		if (!Files.exists(path))
//			return null;
//
//		try (final LockedFileChannel lockedChannel = LockedFileChannel.openForReading(path)) {
//			return readBlock(Channels.newInputStream(lockedChannel.getFileChannel()), zarrDatasetAttributes, gridPosition);
//		}
//	}
//
//	@Override
//	public String[] list(final String pathName) throws IOException {
//
//		final Path path = Paths.get(basePath, removeLeadingSlash(pathName));
//		try (final Stream<Path> pathStream = Files.list(path)) {
//
//			return pathStream
//					.filter(a -> Files.isDirectory(a))
//					.map(a -> path.relativize(a).toString())
//					.filter(a -> exists(pathName + "/" + a))
//					.toArray(n -> new String[n]);
//		}
//	}
//
//	/**
//	 * Constructs the path for a data block in a dataset at a given grid position.
//	 *
//	 * The returned path is
//	 * <pre>
//	 * $datasetPathName/$gridPosition[n]$dimensionSeparator$gridPosition[n-1]$dimensionSeparator[...]$dimensionSeparator$gridPosition[0]
//	 * </pre>
//	 *
//	 * This is the file into which the data block will be stored.
//	 *
//	 * @param datasetPathName
//	 * @param gridPosition
//	 * @param dimensionSeparator
//	 *
//	 * @return
//	 */
//	protected static Path getZarrDataBlockPath(
//			final long[] gridPosition,
//			final String dimensionSeparator,
//			final boolean isRowMajor) {
//
//		final StringBuilder pathStringBuilder = new StringBuilder();
//		if (isRowMajor) {
//			pathStringBuilder.append(gridPosition[gridPosition.length - 1]);
//			for (int i = gridPosition.length - 2; i >= 0 ; --i) {
//				pathStringBuilder.append(dimensionSeparator);
//				pathStringBuilder.append(gridPosition[i]);
//			}
//		} else {
//			pathStringBuilder.append(gridPosition[0]);
//			for (int i = 1; i < gridPosition.length; ++i) {
//				pathStringBuilder.append(dimensionSeparator);
//				pathStringBuilder.append(gridPosition[i]);
//			}
//		}
//
//		return Paths.get(pathStringBuilder.toString());
//	}
//>>>>>>> cmhulbert/attributesByPath
}
