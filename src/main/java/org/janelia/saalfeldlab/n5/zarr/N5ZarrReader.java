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

		if( !exists("/"))
			throw new IOException("No container exists at " + basePath );
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

}
