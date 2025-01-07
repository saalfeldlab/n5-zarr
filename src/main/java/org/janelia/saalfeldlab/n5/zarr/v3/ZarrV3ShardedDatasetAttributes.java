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
package org.janelia.saalfeldlab.n5.zarr.v3;

import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.codec.Codec;
import org.janelia.saalfeldlab.n5.shard.ShardParameters;
import org.janelia.saalfeldlab.n5.shard.ShardingCodec;
import org.janelia.saalfeldlab.n5.shard.ShardingCodec.IndexLocation;
import org.janelia.saalfeldlab.n5.zarr.chunks.ChunkAttributes;
import org.janelia.saalfeldlab.n5.zarr.chunks.DefaultChunkKeyEncoding;

public class ZarrV3ShardedDatasetAttributes extends ZarrV3DatasetAttributes implements ZarrV3Node, ShardParameters {

	private static final long serialVersionUID = 7766316571992919411L;

	private final ShardingCodec shardingCodec;

	public ZarrV3ShardedDatasetAttributes(
			final int zarrFormat,
			final long[] shape,
			final ChunkAttributes chunkAttributes,
			final ZarrV3DataType dataType,
			final String fillValue,
			final Compression compression,
			final Codec[] codecs) {

		super(zarrFormat, shape, chunkAttributes, dataType, fillValue, compression, codecs);
		shardingCodec = (ShardingCodec) codecs[0]; // TODO validate;
	}

	public ZarrV3ShardedDatasetAttributes(
			final int zarrFormat,
			final long[] shape,
			final ChunkAttributes chunkAttributes,
			final ZarrV3DataType dataType,
			final String fillValue,
			final Codec[] codecs) {

		super(zarrFormat, shape, chunkAttributes, dataType, fillValue, codecs);
		shardingCodec = (ShardingCodec) codecs[0]; // TODO validate
	}

	public ZarrV3ShardedDatasetAttributes(
			final int zarrFormat,
			final long[] shape,
			final int[] chunkShape,
			final ZarrV3DataType dataType,
			final String fillValue,
			final DefaultChunkKeyEncoding chunkKeyEncoding,
			final Codec[] codecs) {

		super(zarrFormat, shape, chunkShape, dataType, fillValue, chunkKeyEncoding, codecs);
		shardingCodec = (ShardingCodec) codecs[0]; // TODO validate
	}

	public ZarrV3ShardedDatasetAttributes(
			final int zarrFormat,
			final long[] shape,
			final int[] chunkShape,
			final ZarrV3DataType dataType,
			final String fillValue,
			final String dimensionSeparator,
			final Codec[] codecs) {

		super(zarrFormat, shape, chunkShape, dataType, fillValue, dimensionSeparator, codecs);
		shardingCodec = (ShardingCodec) codecs[0]; // TODO validate
	}

	@Override
	public int[] getBlockSize() {

		return getShardingCodec().getBlockSize();
	}

	@Override
	public ShardingCodec getShardingCodec() {
		return shardingCodec;
	}

	@Override
	public int[] getShardSize() {
		return super.getBlockSize();
	}

	@Override
	public IndexLocation getIndexLocation() {
		return getShardingCodec().getIndexLocation();
	}

}
