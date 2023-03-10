package org.janelia.saalfeldlab.n5.zarr;

import org.janelia.saalfeldlab.n5.VLenStringDataBlock;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class ZarrCompatibleVLenStringDataBlock extends VLenStringDataBlock {

    public ZarrCompatibleVLenStringDataBlock(int[] size, long[] gridPosition, String[] data) {
        super(size, gridPosition, data);
    }

    public ZarrCompatibleVLenStringDataBlock(int[] size, long[] gridPosition, byte[] data) {
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

        buf.putInt(N);
        for (int i = 0; i < N; ++i) {
            buf.putInt(lengths[i]);
            buf.put(encodedStrings[i]);
        }

        return buf.array();
    }

    protected String[] deserialize(byte[] rawBytes) {
        ByteBuffer buf = ByteBuffer.wrap(rawBytes);

        // sanity check to avoid out of memory errors
        if (rawBytes.length < 4)
            throw new RuntimeException("Corrupt buffer, data seems truncated.");

        final int N = buf.getInt();
        if (rawBytes.length < N)
            throw new RuntimeException("Corrupt buffer, data seems truncated.");

        final String[] strings = new String[N];
        for (int i = 0; i < N; ++i) {
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
