package org.janelia.saalfeldlab.n5.zarr.codec;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteOrder;
import java.util.Arrays;
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.codec.DataBlockCodec;
import org.janelia.saalfeldlab.n5.codec.DataCodec;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.zarr.DType;

import static org.janelia.saalfeldlab.n5.zarr.ZarrKeyValueWriter.padCrop;

public class ZarrCodecs {


	// codecs

	private ZarrCodecs() {}


	static DataCodec<?> getDataCodec(DType dType)
	{
		final ByteOrder order = dType.getOrder();
		dType.toString();


		throw new UnsupportedOperationException("TODO revise");


	}



	static class DefaultDataBlockCodec<T> implements DataBlockCodec<T> {

		interface DataBlockFactory<T> {

			DataBlock<T> createDataBlock(int[] blockSize, long[] gridPosition, T data);
		}

		private final int[] blockSize;
		private final DataCodec<T> dataCodec;
		private final DataBlockFactory<T> dataBlockFactory;
		private final int numElements;
		private final int nBytes;
		private final int nBits;
		private final byte[] fillBytes;
		private final int numBytes;

		DefaultDataBlockCodec(
				final int[] blockSize,
				final DataCodec<T> dataCodec,
				final int nBytes,
				final int nBits,
				final byte[] fillBytes,
				final DataBlockFactory<T> dataBlockFactory) {
			this.blockSize = blockSize;

			this.nBytes = nBytes;
			this.nBits = nBits;
			this.fillBytes = fillBytes;

			numElements = DataBlock.getNumElements(blockSize);
			if (nBytes != 0)
				numBytes = numElements * nBytes;
			else
				numBytes = (numElements * nBits + 7) / 8;

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
		public ReadData encode(final DataBlock<T> dataBlock, final Compression compression) throws IOException {
			final ReadData readData = encodePadded(dataBlock);
			return ReadData.from(out -> {
				compression.encode(readData).writeTo(out);
				out.flush();
			});
		}

		@Override
		public DataBlock<T> decode(final ReadData readData, final long[] gridPosition, final Compression compression) throws IOException {
			try (final InputStream in = readData.inputStream()) {
				final T data = dataCodec.createData(numElements);
				final ReadData decompressed = compression.decode(ReadData.from(in), numBytes);
				dataCodec.deserialize(decompressed, data);
				return dataBlockFactory.createDataBlock(blockSize, gridPosition, data);
			}
		}
	}
}
