package org.janelia.saalfeldlab.n5.zarr.codec;

import static org.janelia.saalfeldlab.n5.zarr.ZarrKeyValueWriter.padCrop;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.DataBlock.DataBlockFactory;
import org.janelia.saalfeldlab.n5.codec.Codec;
import org.janelia.saalfeldlab.n5.codec.DataCodec;
import org.janelia.saalfeldlab.n5.readdata.ReadData;

public class ZarrBlockCodec <T> implements Codec.ArrayCodec<T> {

	private final int[] blockSize;
	private final DataCodec<T> dataCodec;
	private final DataBlockFactory<T> dataBlockFactory;
	private final int numElements;
	private final int nBytes;
	private final int nBits;
	private final byte[] fillBytes;
	private final Codec.BytesCodec codec;

	public ZarrBlockCodec(
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
//		/*TODO: Consider an attributes.createDataBlockCodec() without parameters? */
//		final ConcatenatedBytesCodec concatenatedBytesCodec = new ConcatenatedBytesCodec(byteCodecs);
//		this.dataBlockCodec = N5Codecs.createDataBlockCodec(attributes.getDataType(), concatenatedBytesCodec);
//		this.attributes = attributes;	
	}
}
