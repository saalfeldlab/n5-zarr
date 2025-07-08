package org.janelia.saalfeldlab.n5.zarr.codec;

import static org.janelia.saalfeldlab.n5.zarr.ZarrKeyValueWriter.padCrop;

import java.util.Arrays;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.codec.Codec;
import org.janelia.saalfeldlab.n5.codec.ConcatenatedBytesCodec;
import org.janelia.saalfeldlab.n5.codec.DataBlockCodec;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.zarr.DType;
import org.janelia.saalfeldlab.n5.zarr.ZarrDatasetAttributes;

public class ZarrBlockCodec implements Codec.ArrayCodec {

	private static final long serialVersionUID = 8468192075427909345L;

	private transient ZarrDatasetAttributes zAttributes;

	private transient BytesCodec bytesCodec;

	private int[] blockSize;

	public ZarrBlockCodec() {
	}

	@Override
	public <T> ReadData encode(final DataBlock<T> dataBlock) {
		final ReadData readData = this.<T>getDataBlockCodec().encode(dataBlock);
		if (Arrays.equals(blockSize, dataBlock.getSize())) {
			return readData;
		} else {
			final DType dtype = zAttributes.getDType();
			final byte[] padCropped = padCrop(
					readData.allBytes(),
					dataBlock.getSize(),
					blockSize,
					dtype.getNBytes(),
					dtype.getNBits(),
					dtype.createFillBytes(zAttributes.getFillValue()));
			return ReadData.from(padCropped);
		}
	}

	@Override
	public <T> DataBlock<T> decode(final ReadData readData, final long[] gridPosition) {
		return this.<T>getDataBlockCodec().decode(readData, gridPosition);
	}

	private <T> DataBlockCodec<T> getDataBlockCodec() {
		return ZarrCodecs.createDataBlockCodec(zAttributes, bytesCodec);
	}

	@Override
	public String getType() {
		return "internal-zarr-default";
	}

	@Override
	public void initialize(DatasetAttributes attributes, BytesCodec... byteCodecs) {

		// TODO check this cast?
		zAttributes = (ZarrDatasetAttributes)attributes;
		bytesCodec = new ConcatenatedBytesCodec(byteCodecs);
		blockSize = zAttributes.getBlockSize();
	}
}
