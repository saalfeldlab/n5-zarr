/*-
 * #%L
 * Not HDF5
 * %%
 * Copyright (C) 2017 - 2025 Stephan Saalfeld
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
package org.janelia.saalfeldlab.n5.zarr.v3;

import com.google.gson.GsonBuilder;
import org.janelia.saalfeldlab.n5.CachedGsonKeyValueN5Writer;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.KeyValueRoot;

import static org.janelia.saalfeldlab.n5.zarr.v3.ZarrV3Node.ZARR_FORMAT_KEY;

/**
 * Zarr v3 {@link N5Writer} implementation.
 */
public class ZarrV3KeyValueWriter extends ZarrV3KeyValueReader implements CachedGsonKeyValueN5Writer {

	/**
	 * Opens an {@link ZarrV3KeyValueWriter} at a given base path with a custom
	 * {@link GsonBuilder} to support custom attributes.
	 *
	 * @param keyValueRoot
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
			final KeyValueRoot keyValueRoot,
			final GsonBuilder gsonBuilder,
            final boolean cacheAttributes)
			throws N5Exception {

		super(false, keyValueRoot, gsonBuilder,
				cacheAttributes, false);

		Version version = null;
		if (exists("/")) {
			version = getVersion();
			if (!ZARR_3_VERSION.isCompatible(version))
				throw new N5IOException(
						"Incompatible version " + version + " (this is " + ZARR_3_VERSION + ").");
		}

		if (version == null || version.equals(NO_VERSION)) {
			createGroup("/");
			setVersion();
		}
	}

	@Override
	public void setVersion() throws N5Exception {

		if (!ZARR_3_VERSION.equals(getVersion()))
			setAttribute("/", ZARR_FORMAT_KEY, ZARR_3_VERSION.getMajor());;
	}
}
