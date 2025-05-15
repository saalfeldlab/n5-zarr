package org.janelia.saalfeldlab.n5.zarr.codec;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataBlock.DataBlockFactory;
import org.janelia.saalfeldlab.n5.codec.DataBlockCodec;
import org.janelia.saalfeldlab.n5.codec.DataCodec;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.zarr.DType;
import org.janelia.saalfeldlab.n5.zarr.DType.CodecProps;

import static org.janelia.saalfeldlab.n5.zarr.ZarrKeyValueWriter.padCrop;

public class ZarrCodecs {

	private ZarrCodecs() {}

	public static <T> DataBlockCodec<T> createDataBlockCodec(
			final DType dtype,
			final int[] blockSize,
			final String fill_value,
			final Compression compression) {

		final int nBytes = dtype.getNBytes();
		final int nBits = dtype.getNBits();
		final byte[] fillBytes = dtype.createFillBytes(fill_value);
		@SuppressWarnings("unchecked")
		final CodecProps<T> codecProps = (CodecProps<T>) dtype.getCodecProps();
		final DataCodec<T> dataCodec = codecProps.getDataCodec();
		final DataBlockFactory<T> dataBlockFactory = codecProps.getDataBlockFactory();
		return new DefaultDataBlockCodec<>(blockSize, nBytes, nBits, fillBytes, compression, dataCodec, dataBlockFactory);
	}

	private static class DefaultDataBlockCodec<T> implements DataBlockCodec<T> {

		private final int[] blockSize;
		private final DataCodec<T> dataCodec;
		private final DataBlockFactory<T> dataBlockFactory;
		private final int numElements;
		private final int nBytes;
		private final int nBits;
		private final byte[] fillBytes;
		private final Compression compression;

		public DefaultDataBlockCodec(
				final int[] blockSize,
				final int nBytes,
				final int nBits,
				final byte[] fillBytes,
				final Compression compression,
				final DataCodec<T> dataCodec,
				final DataBlockFactory<T> dataBlockFactory) {

			this.blockSize = blockSize;
			this.nBytes = nBytes;
			this.nBits = nBits;
			this.fillBytes = fillBytes;
			this.compression = compression;

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
			return ReadData.from(out -> compression.encode(readData).writeTo(out));
		}

		@Override
		public DataBlock<T> decode(final ReadData readData, final long[] gridPosition) throws IOException {
			try (final InputStream in = readData.inputStream()) {
				final ReadData decompressed = compression.decode(ReadData.from(in));
				final T data = dataCodec.deserialize(decompressed, numElements);
				return dataBlockFactory.createDataBlock(blockSize, gridPosition, data);
			}
		}
	}
}
