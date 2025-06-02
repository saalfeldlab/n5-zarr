package org.janelia.saalfeldlab.n5.zarr.codec;

import static org.janelia.saalfeldlab.n5.zarr.ZarrKeyValueWriter.padCrop;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.DataBlock.DataBlockFactory;
import org.janelia.saalfeldlab.n5.codec.Codec;
import org.janelia.saalfeldlab.n5.codec.ConcatenatedBytesCodec;
import org.janelia.saalfeldlab.n5.codec.DataBlockCodec;
import org.janelia.saalfeldlab.n5.codec.DataCodec;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.zarr.DType;
import org.janelia.saalfeldlab.n5.zarr.ZarrDatasetAttributes;

public class ZarrBlockCodec <T> implements Codec.ArrayCodec<T> {

	private DataBlockCodec<T> dataBlockCodec;

	private int nBytes;
	private int nBits;
	private byte[] fillBytes;
	private int[] blockSize;

	public ZarrBlockCodec() {
	}

	@Override
	public ReadData encode(final DataBlock<T> dataBlock) throws IOException {
		final ReadData readData = dataBlockCodec.encode(dataBlock);
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
	public DataBlock<T> decode(final ReadData readData, final long[] gridPosition) throws IOException {
		return dataBlockCodec.decode(readData, gridPosition);
	}

	@Override
	public String getType() {
		return "internal-zarr-default";
	}

	@Override
	public void initialize(DatasetAttributes attributes, BytesCodec... byteCodecs) {

		// TODO check this cast?
		final ZarrDatasetAttributes zAttributes = (ZarrDatasetAttributes)attributes;

		final ConcatenatedBytesCodec concatenatedBytesCodec = new ConcatenatedBytesCodec(byteCodecs);
		this.dataBlockCodec = ZarrCodecs.createDataBlockCodec(zAttributes, concatenatedBytesCodec);

		blockSize = zAttributes.getBlockSize();

		final DType dtype = zAttributes.getDType();
		nBytes = dtype.getNBytes();
		nBits = dtype.getNBits();
		fillBytes = dtype.createFillBytes(zAttributes.getFillValue());

	}
}
