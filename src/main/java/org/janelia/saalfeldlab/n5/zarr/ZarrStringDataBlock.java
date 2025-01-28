package org.janelia.saalfeldlab.n5.zarr;

import org.janelia.saalfeldlab.n5.StringDataBlock;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

public class ZarrStringDataBlock extends StringDataBlock {

    public ZarrStringDataBlock(int[] size, long[] gridPosition, String[] data) {
        super(size, gridPosition, data);
    }

	@Override
	public byte[] serialize(final ByteOrder byteOrder) {
		final int N = actualData.length;
		final byte[][] encodedStrings = Arrays.stream(actualData).map((str) -> str.getBytes(ENCODING)).toArray(byte[][]::new);
		final int[] lengths = Arrays.stream(encodedStrings).mapToInt((arr) -> arr.length).toArray();
		final int totalLength = Arrays.stream(lengths).sum();
		final ByteBuffer buf = ByteBuffer.wrap(new byte[totalLength + 4*N + 4]);
		buf.order(byteOrder);

		buf.putInt(N);
		for (int i = 0; i < N; ++i) {
			buf.putInt(lengths[i]);
			buf.put(encodedStrings[i]);
		}

		return buf.array();
	}

	@Override
	public void deserialize(final ByteOrder byteOrder, final byte[] serialized) {
		serializedData = serialized;

		final ByteBuffer buf = ByteBuffer.wrap(serialized);
		buf.order(byteOrder);

		// sanity check to avoid out of memory errors
		if (serialized.length < 4)
			throw new RuntimeException("Corrupt buffer, data seems truncated.");

		final int n = buf.getInt();
		if (serialized.length < n)
			throw new RuntimeException("Corrupt buffer, data seems truncated.");

		actualData = new String[n];
		for (int i = 0; i < n; ++i) {
			final int length = buf.getInt();
			final byte[] encodedString = new byte[length];
			buf.get(encodedString);
			actualData[i] = new String(encodedString, ENCODING);
		}
	}
}
