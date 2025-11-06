package org.janelia.saalfeldlab.n5.zarr.codec;

import java.nio.ByteOrder;

import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.codec.BlockCodec;
import org.janelia.saalfeldlab.n5.codec.BlockCodecInfo;
import org.janelia.saalfeldlab.n5.codec.DataCodec;
import org.janelia.saalfeldlab.n5.codec.DataCodecInfo;
import org.janelia.saalfeldlab.n5.codec.RawBlockCodecInfo;
import org.janelia.saalfeldlab.n5.serialization.NameConfig;
import org.janelia.saalfeldlab.n5.zarr.ZarrDatasetAttributes;

@NameConfig.Name(value = PaddedRawBlockCodecInfo.TYPE)
public class PaddedRawBlockCodecInfo implements BlockCodecInfo {

	public static final String TYPE = "bytes";

	@NameConfig.Parameter(value = "endian", optional = true)
	private final ByteOrder byteOrder;

	private final byte[] fillValueBytes;

	private final transient RawBlockCodecInfo codecInfo;

	public PaddedRawBlockCodecInfo() {

		// consider better default for fill value?
		// is 8 bytes enough?
		this(ByteOrder.nativeOrder(), new byte[8]);
	}

	public PaddedRawBlockCodecInfo(final ByteOrder byteOrder, final byte[] fillBytes) {

		this.byteOrder = byteOrder;
		this.fillValueBytes = fillBytes;
		this.codecInfo = new RawBlockCodecInfo(byteOrder);
	}

	@Override
	public String getType() {

		return TYPE;
	}
	
	public <T> BlockCodec<T> create(final ZarrDatasetAttributes attributes, final DataCodec... codecs) {

		DataType dataType = attributes.getDataType();
		RawBlockCodecInfo.ensureValidByteOrder(dataType, getByteOrder());
		byte[] fillBytes = attributes.getFillBytes();
		return PaddedRawBlockCodecs.create(dataType, getByteOrder(),
				attributes.getBlockSize(), DataCodec.concatenate(codecs), fillBytes);
	}

	public ByteOrder getByteOrder() {
		return byteOrder;
	}

	@Override
	public <T> BlockCodec<T> create(DataType dataType, int[] blockSize, DataCodecInfo... codecs) {

		RawBlockCodecInfo.ensureValidByteOrder(dataType, getByteOrder());
		return PaddedRawBlockCodecs.create(dataType, getByteOrder(),
				blockSize, DataCodec.create(codecs), fillValueBytes);
	}

}
