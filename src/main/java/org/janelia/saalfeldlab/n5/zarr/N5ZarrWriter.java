package org.janelia.saalfeldlab.n5.zarr;

import org.janelia.saalfeldlab.n5.FileSystemKeyValueAccess;
import org.janelia.saalfeldlab.n5.N5Exception;
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
	 * @throws N5Exception
	 *    if the base path cannot be written to or cannot be created,
	 *    if the N5 version of the container is not compatible with this
	 *    implementation.
	 */
	public N5ZarrWriter(final String basePath, final GsonBuilder gsonBuilder, final String dimensionSeparator,
			final boolean mapN5DatasetAttributes, final boolean cacheAttributes) throws N5Exception {

		super(
				new FileSystemKeyValueAccess(),
				basePath,
				gsonBuilder,
				mapN5DatasetAttributes,
				true,
				dimensionSeparator,
				cacheAttributes);
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
	 * @throws N5Exception
	 *    if the base path cannot be written to or cannot be created,
	 *    if the N5 version of the container is not compatible with this
	 *    implementation.
	 */
	public N5ZarrWriter(final String basePath, final String dimensionSeparator, final boolean cacheAttributes)
			throws N5Exception {

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
	 * @throws N5Exception
	 *    if the base path cannot be written to or cannot be created,
	 *    if the N5 version of the container is not compatible with this
	 *    implementation.
	 */
	public N5ZarrWriter(final String basePath, final boolean cacheAttributes) throws N5Exception {

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
	 * @throws N5Exception
	 *    if the base path cannot be written to or cannot be created,
	 *    if the N5 version of the container is not compatible with this
	 *    implementation.
	 */
	public N5ZarrWriter(final String basePath, final GsonBuilder gsonBuilder, final boolean cacheAttributes ) throws N5Exception {

		this(basePath, gsonBuilder, ".", true, cacheAttributes);
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
	 * @throws N5Exception
	 *    if the base path cannot be written to or cannot be created,
	 *    if the N5 version of the container is not compatible with this
	 *    implementation.
	 */
	public N5ZarrWriter(final String basePath, final GsonBuilder gsonBuilder) throws N5Exception {

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
	 * @throws N5Exception
	 *    if the base path cannot be written to or cannot be created,
	 *    if the N5 version of the container is not compatible with this
	 *    implementation.
	 */
	public N5ZarrWriter(final String basePath) throws N5Exception {

		this(basePath, new GsonBuilder());
	}

}
