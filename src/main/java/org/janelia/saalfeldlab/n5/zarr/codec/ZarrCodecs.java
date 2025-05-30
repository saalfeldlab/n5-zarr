/*-
 * #%L
 * Not HDF5
 * %%
 * Copyright (C) 2019 - 2025 Stephan Saalfeld
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
package org.janelia.saalfeldlab.n5.zarr.codec;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.stream.Stream;

import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataBlock.DataBlockFactory;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.codec.Codec;
import org.janelia.saalfeldlab.n5.codec.ConcatenatedBytesCodec;
import org.janelia.saalfeldlab.n5.codec.DataBlockCodec;
import org.janelia.saalfeldlab.n5.codec.DataCodec;
import org.janelia.saalfeldlab.n5.codec.N5Codecs;
import org.janelia.saalfeldlab.n5.codec.Codec.BytesCodec;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.zarr.DType;
import org.janelia.saalfeldlab.n5.zarr.DType.CodecProps;

import static org.janelia.saalfeldlab.n5.zarr.ZarrKeyValueWriter.padCrop;

public class ZarrCodecs {

	private ZarrCodecs() {}

	public static <T> Codec.ArrayCodec<T> createDataBlockCodec(
			final DType dtype,
			final int[] blockSize,
			final String fill_value,
			final Codec... codecs) {

		final int nBytes = dtype.getNBytes();
		final int nBits = dtype.getNBits();
		final byte[] fillBytes = dtype.createFillBytes(fill_value);

		@SuppressWarnings("unchecked")
		final CodecProps<T> codecProps = (CodecProps<T>) dtype.getCodecProps();
		final DataCodec<T> dataCodec = codecProps.getDataCodec();
		final DataBlockFactory<T> dataBlockFactory = codecProps.getDataBlockFactory();
		
		// FIXME
		BytesCodec[] bytesCodecs = Stream.of(codecs)
				.skip(1)
				.filter(c -> c instanceof BytesCodec)
				.toArray(BytesCodec[]::new);

		final ConcatenatedBytesCodec concatenatedBytesCodec = new ConcatenatedBytesCodec(bytesCodecs);
		return new DefaultZarrBlockCodec<>(blockSize, nBytes, nBits, fillBytes, dataCodec, dataBlockFactory, concatenatedBytesCodec);
	}

	private static class DefaultZarrBlockCodec<T> implements Codec.ArrayCodec<T> {

		private final int[] blockSize;
		private final DataCodec<T> dataCodec;
		private final DataBlockFactory<T> dataBlockFactory;
		private final int numElements;
		private final int nBytes;
		private final int nBits;
		private final byte[] fillBytes;
		private final Codec.BytesCodec codec;

		public DefaultZarrBlockCodec(
				final int[] blockSize,
				final int nBytes,
				final int nBits,
				final byte[] fillBytes,
				final DataCodec<T> dataCodec,
				final DataBlockFactory<T> dataBlockFactory,
				final Codec.BytesCodec codec) {

			this.blockSize = blockSize;
			this.nBytes = nBytes;
			this.nBits = nBits;
			this.fillBytes = fillBytes;
			this.codec = codec;

			final int numEntries = DataBlock.getNumElements(blockSize);
			final int numBytes = (nBytes != 0)
					? numEntries * nBytes
					: ((numEntries * nBits + 7) / 8);
			numElements = numBytes / dataCodec.bytesPerElement();

			this.dataCodec = dataCodec;
			this.dataBlockFactory = dataBlockFactory;
		}

		private ReadData encodePadded(final DataBlock<T> dataBlock) throws IOException {
			final ReadData readData = dataCodec.serialize(dataBlock.getData());
			if (Arrays.equals(blockSize, dataBlock.getSize())) {
				return readData;
			} else {
				final byte[] padCropped = padCrop(
						readData.allBytes(),
						dataBlock.getSize(),
						blockSize,
						nBytes,
						nBits,
						fillBytes);
				return ReadData.from(padCropped);
			}
		}

		@Override
		public ReadData encode(final DataBlock<T> dataBlock) throws IOException {
			final ReadData readData = encodePadded(dataBlock);
			return ReadData.from(out -> codec.encode(readData).writeTo(out));
		}

		@Override
		public DataBlock<T> decode(final ReadData readData, final long[] gridPosition) throws IOException {
			try (final InputStream in = readData.inputStream()) {
				final ReadData decompressed = codec.decode(ReadData.from(in));
				final T data = dataCodec.deserialize(decompressed, numElements);
				return dataBlockFactory.createDataBlock(blockSize, gridPosition, data);
			}
		}

		@Override
		public String getType() {
			return "internal-zarr-default";
		}

		@Override
		public void initialize(DatasetAttributes attributes, BytesCodec... byteCodecs) {
//			/*TODO: Consider an attributes.createDataBlockCodec() without parameters? */
//			final ConcatenatedBytesCodec concatenatedBytesCodec = new ConcatenatedBytesCodec(byteCodecs);
//			this.dataBlockCodec = N5Codecs.createDataBlockCodec(attributes.getDataType(), concatenatedBytesCodec);
//			this.attributes = attributes;	
		}
	}
}
