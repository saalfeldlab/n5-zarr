package org.janelia.saalfeldlab.n5.zarr.codec;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataBlock.DataBlockFactory;
import org.janelia.saalfeldlab.n5.StringDataBlock;
import org.janelia.saalfeldlab.n5.codec.DataBlockCodec;
import org.janelia.saalfeldlab.n5.codec.DataCodec;
import org.janelia.saalfeldlab.n5.readdata.ReadData;

import static org.janelia.saalfeldlab.n5.zarr.ZarrKeyValueWriter.padCrop;

public class ZarrCodecs {

	private ZarrCodecs() {}

	public static class DefaultDataBlockCodec<T> implements DataBlockCodec<T> {

		private final int[] blockSize;
		private final DataCodec<T> dataCodec;
		private final DataBlockFactory<T> dataBlockFactory;
		private final int numElements;
		private final int nBytes;
		private final int nBits;
		private final byte[] fillBytes;
		private final int numBytes;
		private final Compression compression;

		public DefaultDataBlockCodec(
				final int[] blockSize,
				final DataCodec<T> dataCodec,
				final int nBytes,
				final int nBits,
				final byte[] fillBytes,
				final Compression compression,
				final DataBlockFactory<T> dataBlockFactory) {
			this.blockSize = blockSize;

			this.nBytes = nBytes;
			this.nBits = nBits;
			this.fillBytes = fillBytes;
			this.compression = compression;

			final int numEntries = DataBlock.getNumElements(blockSize);
			if (nBytes != 0)
				numBytes = numEntries * nBytes;
			else
				numBytes = (numEntries * nBits + 7) / 8;
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
				final T data = dataCodec.createData(numElements);
				final ReadData decompressed = compression.decode(ReadData.from(in), numBytes);
				dataCodec.deserialize(decompressed, data);
				return dataBlockFactory.createDataBlock(blockSize, gridPosition, data);
			}
		}
	}

	/**
	 * DataBlockCodec for data type STRING
	 */
	public static class StringDataBlockCodec implements DataBlockCodec<String[]> {

		private static final Charset ENCODING = StandardCharsets.UTF_8;

		private final int[] blockSize;
		private final Compression compression;

		public StringDataBlockCodec(final int[] blockSize, final Compression compression) {
			this.blockSize = blockSize;
			this.compression = compression;
		}

		@Override
		public ReadData encode(final DataBlock<String[]> dataBlock) throws IOException {
			return ReadData.from(out -> {
				final ReadData serialized = serialize(dataBlock.getData());
				compression.encode(serialized).writeTo(out);
			});
		}

		@Override
		public DataBlock<String[]> decode(final ReadData readData, final long[] gridPosition) throws IOException {
			try (final InputStream in = readData.inputStream()) {
				final ReadData decompressed = compression.decode(ReadData.from(in), -1);
				final String[] actualData = deserialize(decompressed);
				return new StringDataBlock(blockSize, gridPosition, actualData);
			}
		}

		private static ReadData serialize(final String[] data) {
			final int N = data.length;
			final byte[][] encodedStrings = Arrays.stream(data).map((str) -> str.getBytes(ENCODING)).toArray(byte[][]::new);
			final int[] lengths = Arrays.stream(encodedStrings).mapToInt(a -> a.length).toArray();
			final int totalLength = Arrays.stream(lengths).sum();
			final ByteBuffer buf = ByteBuffer.wrap(new byte[totalLength + 4 * N + 4]);
			buf.order(ByteOrder.LITTLE_ENDIAN);
			buf.putInt(N);
			for (int i = 0; i < N; ++i) {
				buf.putInt(lengths[i]);
				buf.put(encodedStrings[i]);
			}
			return ReadData.from(buf.array());
		}

		private static String[] deserialize(final ReadData readData) throws IOException {
			final ByteBuffer serialized =  readData.toByteBuffer();
			serialized.order(ByteOrder.LITTLE_ENDIAN);

			// sanity check to avoid out of memory errors
			if (serialized.limit() < 4)
				throw new RuntimeException("Corrupt buffer, data seems truncated.");

			final int n = serialized.getInt();
			if (serialized.limit() < n)
				throw new RuntimeException("Corrupt buffer, data seems truncated.");

			final String[] actualData = new String[n];
			for (int i = 0; i < n; ++i) {
				final int length = serialized.getInt();
				final byte[] encodedString = new byte[length];
				serialized.get(encodedString);
				actualData[i] = new String(encodedString, ENCODING);
			}
			return actualData;
		}
	}

}
