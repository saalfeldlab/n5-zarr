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
import org.janelia.saalfeldlab.n5.GsonAttributesParser;

import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileAttribute;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import org.janelia.saalfeldlab.n5.BlockWriter;
import org.janelia.saalfeldlab.n5.ByteArrayDataBlock;
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5URL;
import org.janelia.saalfeldlab.n5.LinkedAttributePathToken;
import org.janelia.saalfeldlab.n5.N5Writer;

import com.google.gson.GsonBuilder;

/**
 * @author Stephan Saalfeld
 */
public class N5ZarrWriter extends ZarrKeyValueWriter implements N5Writer {

	/**
	 * Opens an {@link N5ZarrWriter} at a given base path with a custom
	 * {@link GsonBuilder} to support custom attributes.
	 *
	 * If the base path does not exist, it will be created.
	 *
	 * If the base path exists and if the N5 version of the container is
	 * compatible with this implementation, the N5 version of this container
	 * will be set to the current N5 version of this implementation.
	 *
	 * @param basePath n5 base path
	 * @param gsonBuilder
	 * @param dimensionSeparator
	 * @param cacheAttributes cache attributes and meta data
	 *    Setting this to true avoids frequent reading and parsing of JSON
	 *    encoded attributes and other meta data that requires accessing the
	 *    store. This is most interesting for high latency backends. Changes
	 *    of cached attributes and meta data by an independent writer on the
	 *    same container will not be tracked.
	 *
	 * @throws IOException
	 *    if the base path cannot be written to or cannot be created,
	 *    if the N5 version of the container is not compatible with this
	 *    implementation.
	 */
	public N5ZarrWriter(final String basePath, final GsonBuilder gsonBuilder, final String dimensionSeparator, 
			final boolean mapN5DatasetAttributes,
			final boolean cacheAttributes) throws IOException {

//<<<<<<< HEAD
		super(
				new FileSystemKeyValueAccess(FileSystems.getDefault()),
				basePath,
				gsonBuilder,
				dimensionSeparator,
				mapN5DatasetAttributes,
				cacheAttributes);
//=======
//		super(createDirectories(Paths.get(basePath), true).toString(), gsonBuilder, dimensionSeparator, mapN5DatasetAttributes);
//>>>>>>> cmhulbert/attributesByPath
	}
	
	/**
	 * Opens an {@link N5ZarrWriter} at a given base path.
	 *
	 * If the base path does not exist, it will be created.
	 *
	 * If the base path exists and if the N5 version of the container is
	 * compatible with this implementation, the N5 version of this container
	 * will be set to the current N5 version of this implementation.
	 *
	 * @param basePath n5 base path
	 * @param dimensionSeparator
	 * @param cacheAttributes cache attributes and meta data
	 *    Setting this to true avoids frequent reading and parsing of JSON
	 *    encoded attributes and other meta data that requires accessing the
	 *    store. This is most interesting for high latency backends. Changes
	 *    of cached attributes and meta data by an independent writer on the
	 *    same container will not be tracked.
	 *
	 * @throws IOException
	 *    if the base path cannot be written to or cannot be created,
	 *    if the N5 version of the container is not compatible with this
	 *    implementation.
	 */
	public N5ZarrWriter(final String basePath, final String dimensionSeparator, final boolean cacheAttributes) throws IOException {

		this(basePath, new GsonBuilder(), dimensionSeparator, true, cacheAttributes);
	}

	/**
	 * Opens an {@link N5ZarrWriter} at a given base path.
	 *
	 * If the base path does not exist, it will be created.
	 *
	 * If the base path exists and if the N5 version of the container is
	 * compatible with this implementation, the N5 version of this container
	 * will be set to the current N5 version of this implementation.
	 *
	 * @param basePath n5 base path
	 * @param cacheAttributes cache attributes and meta data
	 *    Setting this to true avoids frequent reading and parsing of JSON
	 *    encoded attributes and other meta data that requires accessing the
	 *    store. This is most interesting for high latency backends. Changes
	 *    of cached attributes and meta data by an independent writer on the
	 *    same container will not be tracked.
	 *
	 * @throws IOException
	 *    if the base path cannot be written to or cannot be created,
	 *    if the N5 version of the container is not compatible with this
	 *    implementation.
	 */
	public N5ZarrWriter(final String basePath, final boolean cacheAttributes) throws IOException {

		this(basePath, new GsonBuilder(), ".", true, cacheAttributes);
	}

	/**
	 * Opens an {@link N5ZarrWriter} at a given base path with a custom
	 * {@link GsonBuilder} to support custom attributes.
	 * <p>
	 * If the base path does not exist, it will be created.
	 * </p>
	 * <p>
	 * If the base path exists and if the N5 version of the container is
	 * compatible with this implementation, the N5 version of this container
	 * will be set to the current N5 version of this implementation.
	 * </p>
	 *
	 * @param basePath n5 base path
	 * @param gsonBuilder
	 *
	 * @throws IOException
	 *    if the base path cannot be written to or cannot be created,
	 *    if the N5 version of the container is not compatible with this
	 *    implementation.
	 */
	public N5ZarrWriter(final String basePath, final GsonBuilder gsonBuilder) throws IOException {

		this(basePath, gsonBuilder, ".", true, false);
	}

	/**
	 * Opens an {@link N5ZarrWriter} at a given base path.
	 * <p>
	 * If the base path does not exist, it will be created.
	 * </p>
	 * <p>
	 * If the base path exists and if the N5 version of the container is
	 * compatible with this implementation, the N5 version of this container
	 * will be set to the current N5 version of this implementation.
	 * </p>
	 *
	 * @param basePath n5 base path
	 *
	 * @throws IOException
	 *    if the base path cannot be written to or cannot be created,
	 *    if the N5 version of the container is not compatible with this
	 *    implementation.
	 */
	public N5ZarrWriter(final String basePath) throws IOException {

		this(basePath, new GsonBuilder());
	}

//	@Override
//	public void createGroup(final String pathName) throws IOException {
//
//		final Path path = Paths.get(basePath, pathName);
//		createDirectories(path);
//
//		final Path root = Paths.get(basePath);
//		Path parent = path;
//		for (setGroupVersion(parent); !parent.equals(root);) {
//			parent = parent.getParent();
//			setGroupVersion(parent);
//		}
//	}
//
//	protected void setGroupVersion(final Path groupPath) throws IOException {
//
//		final Path path = groupPath.resolve(zgroupFile);
//		final HashMap<String, JsonElement> map = new HashMap<>();
//		map.put("zarr_format", new JsonPrimitive(N5ZarrReader.VERSION.getMajor()));
//
//		try (final LockedFileChannel lockedFileChannel = LockedFileChannel.openForWriting(path)) {
//			lockedFileChannel.getFileChannel().truncate(0);
//			GsonAttributesParser.writeAttributes(Channels.newWriter(lockedFileChannel.getFileChannel(), StandardCharsets.UTF_8.name()), map, gson);
//		}
//	}
//
//	public void setZArrayAttributes(
//			final String pathName,
//			final ZArrayAttributes attributes) throws IOException {
//
//		final Path path = Paths.get(basePath, removeLeadingSlash(pathName), zarrayFile);
//		final HashMap<String, JsonElement> map = new HashMap<>();
//		GsonAttributesParser.insertAttributes(map, attributes.asMap(), gson);
//
//		try (final LockedFileChannel lockedFileChannel = LockedFileChannel.openForWriting(path)) {
//
//			lockedFileChannel.getFileChannel().truncate(0);
//			GsonAttributesParser.writeAttributes(Channels.newWriter(lockedFileChannel.getFileChannel(), StandardCharsets.UTF_8.name()), map, gson);
//		}
//	}
//
//	@Override
//	public void setDatasetAttributes(
//			final String pathName,
//			final DatasetAttributes datasetAttributes) throws IOException {
//
//		final long[] shape = datasetAttributes.getDimensions().clone();
//		Utils.reorder(shape);
//		final int[] chunks = datasetAttributes.getBlockSize().clone();
//		Utils.reorder(chunks);
//
//		final ZArrayAttributes zArrayAttributes = new ZArrayAttributes(
//				N5ZarrReader.VERSION.getMajor(),
//				shape,
//				chunks,
//				new DType(datasetAttributes.getDataType()),
//				ZarrCompressor.fromCompression(datasetAttributes.getCompression()),
//				"0",
//				'C',
//				dimensionSeparator,
//				null);
//
//		setZArrayAttributes(pathName, zArrayAttributes);
//	}
//
//	@Override
//	public void createDataset(
//			final String pathName,
//			final DatasetAttributes datasetAttributes) throws IOException {
//
//        /* create parent groups */
//		int tailingIndex = removeTrailingSlash(pathName).lastIndexOf('/');
//		final String parentGroup = (tailingIndex >= 0) ? pathName.substring(0, removeTrailingSlash(pathName).lastIndexOf('/')) : pathName;
//		if (!parentGroup.equals(""))
//			createGroup(parentGroup);
//
//		final Path path = Paths.get(basePath, pathName);
//		createDirectories(path);
//
//		setDatasetAttributes(pathName, datasetAttributes);
//	}
//
//	@Override
//	public < T > void setAttribute( final String pathName, final String key, final T attribute ) throws IOException
//	{
//		ArrayList<String> zarrKeywords = new ArrayList<>();
//		zarrKeywords.add("dimensions");
//		zarrKeywords.add("blockSize");
//		zarrKeywords.add("dataType");
//		zarrKeywords.add("compression");
//
//		String attributePath = N5URL.normalizeAttributePath( key );
//		final String[] splitPath = attributePath.split( "/" );
//		/* check if referencing root, and only care about a single value */
//		final LinkedAttributePathToken firstToken = N5URL.getAttributePathTokens(attributePath);
//		if ( firstToken != null && !firstToken.hasNext()) {
//			if ( firstToken instanceof LinkedAttributePathToken.ObjectAttributeToken) {
//				final LinkedAttributePathToken.ObjectAttributeToken objectToken = (LinkedAttributePathToken.ObjectAttributeToken) firstToken;
//				final String keyword = objectToken.getKey();
//				if (zarrKeywords.contains( keyword )) {
//					final HashMap< String, T > keywordAttribute = new HashMap<>();
//					keywordAttribute.put( keyword, attribute );
//					setAttributes( pathName, keywordAttribute);
//					return;
//				}
//			}
//		}
//		/* If we get here, it means we aren't a keyword */
//		final Path path = Paths.get(basePath, removeLeadingSlash(pathName), zattrsFile);
//		createDirectories(path.getParent());
//		final JsonElement attributesJson = getAttributesJson( pathName );
//		try (final LockedFileChannel lockedFileChannel = LockedFileChannel.openForWriting(path)) {
//
//			lockedFileChannel.getFileChannel().truncate(0);
//			GsonAttributesParser.writeAttribute(
//					Channels.newWriter(lockedFileChannel.getFileChannel(), StandardCharsets.UTF_8.name()),
//					attributesJson,
//					attributePath,
//					attribute,
//					gson);
//		}
//	}
//
//	@Override
//	public void setAttributes(
//			final String pathName,
//			Map<String, ?> attributes) throws IOException {
//
//		final Path path = Paths.get(basePath, removeLeadingSlash(pathName), zattrsFile);
//		final HashMap<String, JsonElement> map = new HashMap<>();
//
//		try (final LockedFileChannel lockedFileChannel = LockedFileChannel.openForWriting(path)) {
//			map.putAll(
//					GsonAttributesParser.readAttributes(
//							Channels.newReader(
//									lockedFileChannel.getFileChannel(),
//									StandardCharsets.UTF_8.name()),
//							gson));
//
//			if (mapN5DatasetAttributes && datasetExists(pathName)) {
//
//				attributes = new HashMap<>(attributes);
//				ZArrayAttributes zArrayAttributes = getZArraryAttributes(pathName);
//				long[] shape;
//				int[] chunks;
//				final DType dtype;
//				final ZarrCompressor compressor;
//				final boolean isRowMajor = zArrayAttributes.getOrder() == 'C';
//
//				if (attributes.containsKey("dimensions")) {
//					shape = (long[])attributes.get("dimensions");
//					attributes.remove("dimensions");
//					if (isRowMajor) {
//						shape = shape.clone();
//						Utils.reorder(shape);
//					}
//				} else
//					 shape = zArrayAttributes.getShape();
//
//				if (attributes.containsKey("blockSize")) {
//					chunks = (int[])attributes.get("blockSize");
//					attributes.remove("blockSize");
//					if (isRowMajor) {
//						chunks = chunks.clone();
//						Utils.reorder(chunks);
//					}
//				} else
//					chunks = zArrayAttributes.getChunks();
//
//				if (attributes.containsKey("dataType")) {
//					dtype = new DType((DataType)attributes.get("dataType"));
//					attributes.remove("dataType");
//				} else
//					dtype = zArrayAttributes.getDType();
//
//				if (attributes.containsKey("compression")) {
//					compressor = ZarrCompressor.fromCompression((Compression)attributes.get("compression"));
//					attributes.remove("compression");
//					/* fails with null when compression is not supported by Zarr
//					 * TODO invent meaningful error behavior */
//				} else
//					compressor = zArrayAttributes.getCompressor();
//
//				zArrayAttributes = new ZArrayAttributes(
//						zArrayAttributes.getZarrFormat(),
//						shape,
//						chunks,
//						dtype,
//						compressor,
//						zArrayAttributes.getFillValue(),
//						zArrayAttributes.getOrder(),
//						zArrayAttributes.getFilters());
//
//				setZArrayAttributes(pathName, zArrayAttributes);
//			}
//
//			GsonAttributesParser.insertAttributes(map, attributes, gson);
//
//			lockedFileChannel.getFileChannel().truncate(0);
//			GsonAttributesParser.writeAttributes(
//					Channels.newWriter(lockedFileChannel.getFileChannel(), StandardCharsets.UTF_8.name()),
//					map,
//					gson);
//		}
//	}
//
//	@Override
//	public boolean remove() throws IOException {
//
//		return remove("/");
//	}
//
//	@Override
//	public boolean remove(final String pathName) throws IOException {
//
//		final Path path = Paths.get(basePath, pathName);
//		if (Files.exists(path))
//			try (final Stream<Path> pathStream = Files.walk(path)) {
//				pathStream.sorted(Comparator.reverseOrder()).forEach(
//						childPath -> {
//							if (Files.isRegularFile(childPath)) {
//								try (final LockedFileChannel channel = LockedFileChannel.openForWriting(childPath)) {
//									Files.delete(childPath);
//								} catch (final IOException e) {
//									e.printStackTrace();
//								}
//							} else
//								try {
//									Files.delete(childPath);
//								} catch (final IOException e) {
//									e.printStackTrace();
//								}
//						});
//			}
//
//		return !Files.exists(path);
//	}
//
//	/**
//	 * This is a copy of {@link Files#createDirectories(Path, FileAttribute...)}
//	 * that follows symlinks.
//	 *
//	 * Workaround for https://bugs.openjdk.java.net/browse/JDK-8130464
//	 *
//     * Creates a directory by creating all nonexistent parent directories first.
//     * Unlike the {@link #createDirectory createDirectory} method, an exception
//     * is not thrown if the directory could not be created because it already
//     * exists.
//     *
//     * <p> The {@code attrs} parameter is optional {@link FileAttribute
//     * file-attributes} to set atomically when creating the nonexistent
//     * directories. Each file attribute is identified by its {@link
//     * FileAttribute#name name}. If more than one attribute of the same name is
//     * included in the array then all but the last occurrence is ignored.
//     *
//     * <p> If this method fails, then it may do so after creating some, but not
//     * all, of the parent directories.
//     *
//     * @param   dir
//     *          the directory to create
//	 *
//	 * @param   isGroup
//	 *          if true, a .zgroup file is added at {@code dir}
//     *
//     * @param   attrs
//     *          an optional list of file attributes to set atomically when
//     *          creating the directory
//     *
//     * @return  the directory
//     *
//     * @throws  UnsupportedOperationException
//     *          if the array contains an attribute that cannot be set atomically
//     *          when creating the directory
//     * @throws  FileAlreadyExistsException
//     *          if {@code dir} exists but is not a directory <i>(optional specific
//     *          exception)</i>
//     * @throws  IOException
//     *          if an I/O error occurs
//     * @throws  SecurityException
//     *          in the case of the default provider, and a security manager is
//     *          installed, the {@link SecurityManager#checkWrite(String) checkWrite}
//     *          method is invoked prior to attempting to create a directory and
//     *          its {@link SecurityManager#checkRead(String) checkRead} is
//     *          invoked for each parent directory that is checked. If {@code
//     *          dir} is not an absolute path then its {@link Path#toAbsolutePath
//     *          toAbsolutePath} may need to be invoked to get its absolute path.
//     *          This may invoke the security manager's {@link
//     *          SecurityManager#checkPropertyAccess(String) checkPropertyAccess}
//     *          method to check access to the system property {@code user.dir}
//     */
//    private static Path createDirectories(Path dir, boolean isGroup, final FileAttribute<?>... attrs)
//        throws IOException
//    {
//        // attempt to create the directory
//        try {
//            createAndCheckIsDirectory(dir, attrs);
//			if (isGroup) {
//				final Path path = dir.resolve(zgroupFile);
//				LockedFileChannel.openForWriting(path).close();
//			}
//            return dir;
//        } catch (final FileAlreadyExistsException x) {
//            // file exists and is not a directory
//            throw x;
//        } catch (final IOException x) {
//            // parent may not exist or other reason
//        }
//        SecurityException se = null;
//        try {
//            dir = dir.toAbsolutePath();
//        } catch (final SecurityException x) {
//            // don't have permission to get absolute path
//            se = x;
//        }
//        // find a decendent that exists
//        Path parent = dir.getParent();
//        while (parent != null) {
//            try {
//            	parent.getFileSystem().provider().checkAccess(parent);
//                break;
//            } catch (final NoSuchFileException x) {
//                // does not exist
//            }
//            parent = parent.getParent();
//        }
//        if (parent == null) {
//            // unable to find existing parent
//            if (se == null) {
//                throw new FileSystemException(dir.toString(), null,
//                    "Unable to determine if root directory exists");
//            } else {
//                throw se;
//            }
//        }
//
//        // create directories
//        Path child = parent;
//        for (final Path name: parent.relativize(dir)) {
//            child = child.resolve(name);
//            createAndCheckIsDirectory(child, attrs);
//        }
//
//		if (isGroup) {
//			final Path path = dir.resolve(zgroupFile);
//			LockedFileChannel.openForWriting(path).close();
//		}
//        return dir;
//    }
//
//	/**
//	 * Overload of {@link #createDirectories(Path, boolean, FileAttribute[])} with {@code isGroup} false by default.
//	 */
//	private static Path createDirectories(Path dir, final FileAttribute<?>... attrs)
//			throws IOException
//	{
//		return createDirectories(dir, false, attrs);
//	}
//
//    /**
//     * This is a copy of {@link Files#createAndCheckIsDirectory(Path, FileAttribute...)}
//     * that follows symlinks.
//     *
//     * Workaround for https://bugs.openjdk.java.net/browse/JDK-8130464
//     *
//     * Used by createDirectories to attempt to create a directory. A no-op
//     * if the directory already exists.
//     */
//    private static void createAndCheckIsDirectory(final Path dir,
//                                                  final FileAttribute<?>... attrs)
//        throws IOException
//    {
//        try {
//            Files.createDirectory(dir, attrs);
//        } catch (final FileAlreadyExistsException x) {
//            if (!Files.isDirectory(dir))
//                throw x;
//        }
//    }
//
//    /**
//	 * Removes the trailing slash from a given path and returns the corrected path.
//	 *
//	 * @param pathName
//	 * @return
//	 */
//	protected static String removeTrailingSlash(final String pathName) {
//
//		return pathName.endsWith("/") || pathName.endsWith("\\") ? pathName.substring(0, pathName.length() - 1) : pathName;
//	}
}
