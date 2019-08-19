/**
 * Copyright (c) 2019, Stephan Saalfeld
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

import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashMap;
import java.util.stream.Stream;

import org.janelia.saalfeldlab.n5.CompressionAdapter;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.DefaultBlockReader;
import org.janelia.saalfeldlab.n5.GsonAttributesParser;
import org.janelia.saalfeldlab.n5.N5FSReader;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.reflect.TypeToken;


/**
 * @author Stephan Saalfeld &lt;saalfelds@janelia.hhmi.org&gt;
 *
 */
public class N5ZarrReader extends N5FSReader {

	protected static Version VERSION = new Version(2, 0, 0);

	protected static final String zarrayFile = ".zarray";
	protected static final String zattrsFile = ".zattrs";
	protected static final String zgroupFile = ".zgroup";

	static private GsonBuilder initGsonBuilder(final GsonBuilder gsonBuilder) {

		gsonBuilder.registerTypeAdapter(DType.class, new DType.JsonAdapter());
		gsonBuilder.registerTypeHierarchyAdapter(ZarrCompressor.class, CompressionAdapter.getJsonAdapter());

		return gsonBuilder;
	}

	protected boolean mapN5DatasetAttributes;

	/**
	 * Opens an {@link N5ZarrReader} at a given base path with a custom
	 * {@link GsonBuilder} to support custom attributes.
	 *
	 * @param basePath Zarr base path
	 * @param gsonBuilder
	 * @param mapN5DatasetAttributes
	 * 			Virtually create N5 dataset attributes (dimensions, blockSize,
	 * 			compression, dataType) for datasets such that N5 code that
	 * 			reads or modifies these attributes directly works as expected.
	 * 			This can lead to name clashes if a zarr container uses these
	 * 			attribute keys for other purposes.
	 * @throws IOException
	 */
	public N5ZarrReader(final String basePath, final GsonBuilder gsonBuilder, final boolean mapN5DatasetAttributes) throws IOException {

		super(basePath, initGsonBuilder(gsonBuilder));
		this.mapN5DatasetAttributes = mapN5DatasetAttributes;
	}

	/**
	 * Opens an {@link N5ZarrReader} at a given base path.
	 *
	 * @param basePath Zarr base path
	 * @param mapN5DatasetAttributes
	 * 			Virtually create N5 dataset attributes (dimensions, blockSize,
	 * 			compression, dataType) for datasets such that N5 code that
	 * 			reads or modifies these attributes directly works as expected.
	 * 			This can lead to name collisions if a zarr container uses these
	 * 			attribute keys for other purposes.
	 *
	 * @throws IOException
	 */
	public N5ZarrReader(final String basePath, final boolean mapN5DatasetAttributes) throws IOException {

		this(basePath, new GsonBuilder(), mapN5DatasetAttributes);
	}

	/**
	 * Opens an {@link N5ZarrReader} at a given base path with a custom
	 * {@link GsonBuilder} to support custom attributes.
	 *
	 * Zarray metadata will be virtually mapped to N5 dataset attributes.
	 *
	 * @param basePath Zarr base path
	 * @param gsonBuilder
	 * @throws IOException
	 */
	public N5ZarrReader(final String basePath, final GsonBuilder gsonBuilder) throws IOException {

		this(basePath, gsonBuilder, true);
	}

	/**
	 * Opens an {@link N5ZarrReader} at a given base path.
	 *
	 * Zarray metadata will be virtually mapped to N5 dataset attributes.
	 *
	 * @param basePath Zarr base path
	 * @throws IOException
	 */
	public N5ZarrReader(final String basePath) throws IOException {

		this(basePath, new GsonBuilder());
	}

	@Override
	public Version getVersion() {

		return VERSION;
	}

	/**
	 *
	 * @return Zarr base path
	 */
	@Override
	public String getBasePath() {

		return this.basePath;
	}

	public boolean groupExists(final String pathName) {

		final Path path = Paths.get(basePath, removeLeadingSlash(pathName), zgroupFile);
		return Files.exists(path) && Files.isRegularFile(path);
	}

	public ZArrayAttributes getZArraryAttributes(final String pathName) throws IOException {

		final Path path = Paths.get(basePath, removeLeadingSlash(pathName), zarrayFile);
		final HashMap<String, JsonElement> attributes = new HashMap<>();
		final Gson gson = getGson();

		if (Files.exists(path)) {

			try (final LockedFileChannel lockedFileChannel = LockedFileChannel.openForReading(path)) {
				attributes.putAll(
						GsonAttributesParser.readAttributes(
								Channels.newReader(
										lockedFileChannel.getFileChannel(),
										StandardCharsets.UTF_8.name()),
								gson));
			}
		}

		return new ZArrayAttributes(
				attributes.get("zarr_format").getAsInt(),
				gson.fromJson(attributes.get("shape"), long[].class),
				gson.fromJson(attributes.get("chunks"), int[].class),
				gson.fromJson(attributes.get("dtype"), DType.class),
				gson.fromJson(attributes.get("compressor"), ZarrCompressor.class),
				attributes.get("fill_value").getAsString(),
				attributes.get("order").getAsCharacter(),
				gson.fromJson(attributes.get("filters"), TypeToken.getParameterized(Collection.class, Filter.class).getType()));
	}

	@Override
	public boolean datasetExists(final String pathName) throws IOException {

		final Path path = Paths.get(basePath, removeLeadingSlash(pathName), zarrayFile);
		return Files.exists(path) && Files.isRegularFile(path) && getDatasetAttributes(pathName) != null;
	}


	/**
	 * @returns false if the group or dataset does not exist but also if the
	 * 		attempt to access
	 */
	@Override
	public boolean exists(final String pathName) {

		try {
			return groupExists(pathName) || datasetExists(pathName);
		} catch (final IOException e) {
			e.printStackTrace();
			return false;
		}
	}


	/**
	 * If {@link #mapN5DatasetAttributes} is set, dataset attributes will
	 * override attributes with the same key.
	 */
	@Override
	public HashMap<String, JsonElement> getAttributes(final String pathName) throws IOException {

		final Path path = Paths.get(basePath, removeLeadingSlash(pathName), zattrsFile);
		final HashMap<String, JsonElement> attributes = new HashMap<>();

		if (Files.exists(path)) {

			try (final LockedFileChannel lockedFileChannel = LockedFileChannel.openForReading(path)) {
				attributes.putAll(
						GsonAttributesParser.readAttributes(
								Channels.newReader(
										lockedFileChannel.getFileChannel(),
										StandardCharsets.UTF_8.name()),
								gson));
			}
		}

		if (mapN5DatasetAttributes) {

			final DatasetAttributes datasetAttributes = getZArraryAttributes(pathName).getDatasetAttributes();
			attributes.put("dimensions", gson.toJsonTree(datasetAttributes.getDimensions()));
			attributes.put("blockSize", gson.toJsonTree(datasetAttributes.getBlockSize()));
			attributes.put("dataType", gson.toJsonTree(datasetAttributes.getDataType()));
			attributes.put("compression", gson.toJsonTree(datasetAttributes.getCompression()));
		}

		return attributes;
	}

	@Override
	public DataBlock<?> readBlock(
			final String pathName,
			final DatasetAttributes datasetAttributes,
			final long[] gridPosition) throws IOException {

		final Path path = Paths.get(basePath, getDataBlockPath(pathName, gridPosition).toString());
		if (!Files.exists(path))
			return null;

		try (final LockedFileChannel lockedChannel = LockedFileChannel.openForReading(path)) {
			return DefaultBlockReader.readBlock(Channels.newInputStream(lockedChannel.getFileChannel()), datasetAttributes, gridPosition);
		}
	}

	@Override
	public String[] list(final String pathName) throws IOException {

		final Path path = Paths.get(basePath, removeLeadingSlash(pathName));
		try (final Stream<Path> pathStream = Files.list(path)) {
			return pathStream
					.filter(a -> Files.isDirectory(a))
					.map(a -> path.relativize(a).toString())
					.filter(a -> exists(a))
					.toArray(n -> new String[n]);
		}
	}

	/**
	 * Constructs the path for a data block in a dataset at a given grid position.
	 *
	 * The returned path is
	 * <pre>
	 * $datasetPathName/$gridPosition[0]/$gridPosition[1]/.../$gridPosition[n]
	 * </pre>
	 *
	 * This is the file into which the data block will be stored.
	 *
	 * @param datasetPathName
	 * @param gridPosition
	 * @return
	 */
	protected static Path getDataBlockPath(
			final String datasetPathName,
			final long[] gridPosition) {

		final String[] pathComponents = new String[gridPosition.length];
		for (int i = 0; i < pathComponents.length; ++i)
			pathComponents[i] = Long.toString(gridPosition[i]);

		return Paths.get(removeLeadingSlash(datasetPathName), pathComponents);
	}

	/**
	 * Removes the leading slash from a given path and returns the corrected path.
	 * It ensures correctness on both Unix and Windows, otherwise {@code pathName} is treated
	 * as UNC path on Windows, and {@code Paths.get(pathName, ...)} fails with {@code InvalidPathException}.
	 *
	 * @param pathName
	 * @return
	 */
	protected static String removeLeadingSlash(final String pathName) {

		return pathName.startsWith("/") || pathName.startsWith("\\") ? pathName.substring(1) : pathName;
	}
}
