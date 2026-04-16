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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.janelia.saalfeldlab.n5.CachedGsonKeyValueN5Writer;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.KeyValueRoot;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.util.SubArrayCopy;

/**
 * Zarr V2 {@link N5Writer} implementation through {@link KeyValueRoot} with
 * JSON attributes parsed with {@link Gson}.
 *
 * @author Stephan Saalfeld
 * @author John Bogovic
 */
public class ZarrKeyValueWriter extends ZarrKeyValueReader implements CachedGsonKeyValueN5Writer {

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
	 * @param keyValueRoot
	 * @param gsonBuilder
	 * @param cacheAttributes
	 *            cache attributes
	 *            Setting this to true avoids frequent reading and parsing of
	 *            JSON
	 *            encoded attributes, this is most interesting for high latency
	 *            file
	 *            systems. Changes of attributes by an independent writer will
	 *            not be
	 *            tracked.
	 *
	 * @throws N5Exception
	 *             if the base path cannot be written to or cannot be created.
	 */
	public ZarrKeyValueWriter(
			final KeyValueRoot keyValueRoot,
			final GsonBuilder gsonBuilder,
			final boolean mapN5DatasetAttributes,
			final boolean mergeAttributes,
			final String dimensionSeparator,
			final boolean cacheAttributes)
			throws N5Exception {

		super(
				false,
				keyValueRoot,
				gsonBuilder,
				mapN5DatasetAttributes,
				mergeAttributes,
				cacheAttributes,
				false);

		validateDimensionSeparator(dimensionSeparator);
		this.dimensionSeparator = dimensionSeparator;

		Version version = null;
		try {
			version = getVersion();
			if (!ZARR_2_VERSION.isCompatible(version))
				throw new N5IOException(
						"Incompatible version " + version + " (this is " + ZARR_2_VERSION + ").");
		} catch (final NullPointerException e) {}

		if (version == null || version.equals(NO_VERSION)) {
			createGroup("/");
			setVersion();
		}
	}

	private void validateDimensionSeparator(final String dimSep) {

		if (!(dimSep.equals(".") || dimSep.equals("/"))) {
			throw new N5Exception("Invalid dimension_separator.\n" +
					"Must be \".\" or \"/\", but found: \""
					+ dimSep + "\"");
		}
	}

	@Override
	public void setVersion() throws N5Exception {

		if (!ZARR_2_VERSION.equals(getVersion()))
			setAttribute("/", ZARR_FORMAT_KEY, ZARR_2_VERSION.getMajor());;
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

			final int[] srcSize = new int[n];
			final int[] dstSize = new int[n];

			srcSize[0] = srcBlockSize[0] * nBytes;
			dstSize[0] = dstBlockSize[0] * nBytes;
			for (int d = 1; d < n; ++d) {
				srcSize[d] = srcBlockSize[d];
				dstSize[d] = dstBlockSize[d];
			}

			final int numDstBytes = DataBlock.getNumElements(dstSize);
			final byte[] dst = new byte[numDstBytes];

			/* fill dst */
			for (int i = 0, j = 0; i < numDstBytes; ++i) {
				dst[i] = fill_value[j];
				if (++j == fill_value.length)
					j = 0;
			}

			final int[] size = new int[n];
			for (int d = 0; d < n; ++d)
				size[d] = Math.min(srcSize[d], dstSize[d]);
			final int[] zero = new int[n]; // start of block to copy in both src and dst is (0,0,...,0)
			SubArrayCopy.copy(src, srcSize, zero, dst, dstSize, zero, size);

			return dst;
		} else {
			/* TODO deal with bit streams */
			return null;
		}
	}

}
