package org.janelia.saalfeldlab.n5.zarr;

import org.janelia.saalfeldlab.n5.StringDataBlock;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

public class ZarrCompatibleStringDataBlock extends StringDataBlock {

    public ZarrCompatibleStringDataBlock(int[] size, long[] gridPosition, String[] data) {
        super(size, gridPosition, data);
    }

    public ZarrCompatibleStringDataBlock(int[] size, long[] gridPosition, byte[] data) {
        super(size, gridPosition, data);
    }

    @Override
    public void readData(final ByteBuffer buffer) {
        try {
            serializedData = buffer.array();
        }
        catch (UnsupportedOperationException e) { // buffer is a DirectByteBuffer -> copy contents
            serializedData = new byte[buffer.limit()];
            buffer.get(serializedData, 0, serializedData.length);
        }
        actualData = deserialize(serializedData);
    }

    protected byte[] serialize(String[] strings) {
        final int N = strings.length;
        final byte[][] encodedStrings = Arrays.stream(strings).map((str) -> str.getBytes(ENCODING)).toArray(byte[][]::new);
        final int[] lengths = Arrays.stream(encodedStrings).mapToInt((arr) -> arr.length).toArray();
        final int totalLength = Arrays.stream(lengths).sum();
        final ByteBuffer buf = ByteBuffer.wrap(new byte[totalLength + 4*N + 4]);
        buf.order(ByteOrder.LITTLE_ENDIAN);

        buf.putInt(N);
        for (int i = 0; i < N; ++i) {
            buf.putInt(lengths[i]);
            buf.put(encodedStrings[i]);
        }

        return buf.array();
    }

    protected String[] deserialize(byte[] rawBytes) {
        ByteBuffer buf = ByteBuffer.wrap(rawBytes);
        buf.order(ByteOrder.LITTLE_ENDIAN);

        // sanity check to avoid out of memory errors
        if (rawBytes.length < 4)
            throw new RuntimeException("Corrupt buffer, data seems truncated.");

        final int n = buf.getInt();
        if (rawBytes.length < n)
            throw new RuntimeException("Corrupt buffer, data seems truncated.");

        final String[] strings = new String[n];
        for (int i = 0; i < n; ++i) {
            final int length = buf.getInt();
            final byte[] encodedString = new byte[length];
            buf.get(encodedString);
            strings[i] = new String(encodedString, ENCODING);
        }

        return strings;
    }

    public static class VLenStringFilter implements Filter {
        public final String id = "vlen-utf8";
    }
}
