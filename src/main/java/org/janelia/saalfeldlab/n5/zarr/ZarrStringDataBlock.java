package org.janelia.saalfeldlab.n5.zarr;

import java.io.IOException;
import org.janelia.saalfeldlab.n5.StringDataBlock;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import org.janelia.saalfeldlab.n5.readdata.ReadData;

public class ZarrStringDataBlock extends StringDataBlock {

    public ZarrStringDataBlock(int[] size, long[] gridPosition, String[] data) {
        super(size, gridPosition, data);
    }

//	@Override
//	public void readData(final ByteOrder byteOrder, final ReadData readData) throws IOException {
//		serializedData = readData.allBytes();
//
//		final ByteBuffer serialized = ByteBuffer.wrap(serializedData);
//		serialized.order(ByteOrder.LITTLE_ENDIAN);
//
//		// sanity check to avoid out of memory errors
//		if (serialized.limit() < 4)
//			throw new RuntimeException("Corrupt buffer, data seems truncated.");
//
//		final int n = serialized.getInt();
//		if (serialized.limit() < n)
//			throw new RuntimeException("Corrupt buffer, data seems truncated.");
//
//		actualData = new String[n];
//		for (int i = 0; i < n; ++i) {
//			final int length = serialized.getInt();
//			final byte[] encodedString = new byte[length];
//			serialized.get(encodedString);
//			actualData[i] = new String(encodedString, ENCODING);
//		}
//	}
//
//	@Override
//	public ReadData writeData(final ByteOrder byteOrder) {
//		final int N = actualData.length;
//		final byte[][] encodedStrings = Arrays.stream(actualData).map((str) -> str.getBytes(ENCODING)).toArray(byte[][]::new);
//		final int[] lengths = Arrays.stream(encodedStrings).mapToInt((arr) -> arr.length).toArray();
//		final int totalLength = Arrays.stream(lengths).sum();
//		final ByteBuffer buf = ByteBuffer.wrap(new byte[totalLength + 4*N + 4]);
//		buf.order(ByteOrder.LITTLE_ENDIAN);
//
//		buf.putInt(N);
//		for (int i = 0; i < N; ++i) {
//			buf.putInt(lengths[i]);
//			buf.put(encodedStrings[i]);
//		}
//
//		return ReadData.from(buf);
//	}
}
