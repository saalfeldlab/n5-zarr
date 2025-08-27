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

import static net.imglib2.util.Util.safeInt;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataBlock.DataBlockFactory;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.codec.BlockCodec;
import org.janelia.saalfeldlab.n5.codec.DataCodec;
import org.janelia.saalfeldlab.n5.codec.FlatArrayCodec;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.zarr.DType;
import org.janelia.saalfeldlab.n5.zarr.DType.CodecProps;

import net.imglib2.blocks.SubArrayCopy;
import net.imglib2.util.Intervals;

public class ZarrCodecs {

	private ZarrCodecs() {}

	public static <T> BlockCodec<T> createDataBlockCodec(

			final DType dtype,
			final int[] blockSize,
			final String fill_value,
			final DataCodec... bytesCodecs) {

		final int nBytes = dtype.getNBytes();
		final int nBits = dtype.getNBits();
		final byte[] fillBytes = dtype.createFillBytes(fill_value);

		@SuppressWarnings("unchecked")
		final CodecProps<T> codecProps = (CodecProps<T>) dtype.getCodecProps();
		final FlatArrayCodec<T> dataCodec = codecProps.getDataBlockSerializer();
		final DataBlockFactory<T> dataBlockFactory = codecProps.getDataBlockFactory();
		final DataCodec concatenatedBytesCodec = DataCodec.concatenate(bytesCodecs);
		return new DefaultDataBlockCodec<>(blockSize, nBytes, nBits, fillBytes, concatenatedBytesCodec, dataCodec, dataBlockFactory);
	}

	private static class DefaultDataBlockCodec<T> implements BlockCodec<T> {

		private final int[] blockSize;
		private final FlatArrayCodec<T> dataSerializer;
		private final DataBlockFactory<T> dataBlockFactory;
		private final int numElements;
		private final int nBytes;
		private final int nBits;
		private final byte[] fillBytes;
		private final DataCodec codec;

		public DefaultDataBlockCodec(
				final int[] blockSize,
				final int nBytes,
				final int nBits,
				final byte[] fillBytes,
				final DataCodec codec,
				final FlatArrayCodec<T> dataSerializer,
				final DataBlockFactory<T> dataBlockFactory) {

			this.blockSize = blockSize;
			this.nBytes = nBytes;
			this.nBits = nBits;
			this.fillBytes = fillBytes;
			this.codec = codec;

			final int numEntries = DataBlock.getNumElements(blockSize);
			final int numBytes = (nBytes != 0)
					? numEntries * nBytes
					: ((numEntries * nBits + 7) / 8);
			numElements = numBytes / dataSerializer.bytesPerElement();

			this.dataSerializer = dataSerializer;
			this.dataBlockFactory = dataBlockFactory;
		}

		private ReadData encodePadded(final DataBlock<T> dataBlock) {
			final ReadData readData = dataSerializer.encode(dataBlock.getData());
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
		public ReadData encode(final DataBlock<T> dataBlock) {
			final ReadData readData = encodePadded(dataBlock);
			return ReadData.from(out -> codec.encode(readData).writeTo(out));
		}

		@Override
		public DataBlock<T> decode(final ReadData readData, final long[] gridPosition) {
			try (final InputStream in = readData.inputStream()) {

				final ReadData decompressed = codec.decode(ReadData.from(in));
				final T data = dataSerializer.decode(decompressed, numElements);
				return dataBlockFactory.createDataBlock(blockSize, gridPosition, data);
			} catch (IllegalStateException e) {
				throw new N5Exception(e);
			} catch (IOException e) {
				throw new N5IOException(e);
			}
		}
	}

	static byte[] padCrop(
			final byte[] src,
			final int[] srcBlockSize,
			final int[] dstBlockSize,
			final int nBytes,
			final int nBits,
			final byte[] fill_value) {

		assert srcBlockSize.length == dstBlockSize.length : "Dimensions do not match.";

		final int n = srcBlockSize.length;

		if (nBytes != 0) {
			int[] zero = new int[n];
			int[] srcSize = new int[n];
			int[] dstSize = new int[n];
			int[] size = new int[n];
			Arrays.setAll(srcSize, d -> srcBlockSize[d] * (d == 0 ? nBytes : 1));
			Arrays.setAll(dstSize, d -> dstBlockSize[d] * (d == 0 ? nBytes : 1));
			Arrays.setAll(size, d -> Math.min(srcSize[d], dstSize[d]));
			final byte[] dst = new byte[safeInt(Intervals.numElements(dstSize))];
			if (!Arrays.equals(dstSize, size)) {
				fill(dst, fill_value);
			}
			SubArrayCopy.copy(src, srcSize, zero, dst, dstSize, zero, size);
			return dst;
		} else {
			/* TODO deal with bit streams */
			return null;
		}
	}

	private static void fill(final byte[] dst, final byte[] fill_value) {
		byte allZero = 0;
		for (byte b : fill_value) {
			allZero |= b;
		}
		if (allZero != 0) {
			if (fill_value.length == 1) {
				Arrays.fill(dst, fill_value[0]);
			} else {
				for (int i = 0; i < dst.length; i++) {
					dst[i] = fill_value[i % fill_value.length];
				}
			}
		}
	}
	
}
