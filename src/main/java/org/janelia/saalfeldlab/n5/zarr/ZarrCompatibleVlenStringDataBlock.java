package org.janelia.saalfeldlab.n5.zarr;

import org.janelia.saalfeldlab.n5.VLenStringDataBlock;

import java.nio.ByteBuffer;

public class ZarrCompatibleVlenStringDataBlock extends VLenStringDataBlock {

    public ZarrCompatibleVlenStringDataBlock(int[] size, long[] gridPosition, String[] data) {
        super(size, gridPosition, data);
    }

    public ZarrCompatibleVlenStringDataBlock(int[] size, long[] gridPosition, byte[] data) {
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
}
