package org.janelia.saalfeldlab.n5.zarr.codec;

import java.nio.ByteOrder;
import java.util.Collections;

import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.codec.BytesCodec;
import org.janelia.saalfeldlab.n5.codec.DataBlockSerializer;
import org.janelia.saalfeldlab.n5.codec.RawBytesArrayCodec;
import org.janelia.saalfeldlab.n5.zarr.DType;

/**
 * An ArrayCodec that serializes bytes directly, but pads the data into a "full
 * chunk" per the Zarr specification.
 */
public class ZarrBytesArrayCodec extends RawBytesArrayCodec {

	private static final long serialVersionUID = 1173539891671563072L;

	private final String fillValue; 
	private final DType dtype;

	public ZarrBytesArrayCodec() {
		this(new DType(">u", Collections.EMPTY_LIST), "0");
	}

	public ZarrBytesArrayCodec(DType dtype, String fillValue) {
		this.fillValue = fillValue;
		this.dtype = dtype;
	}

	@Override
	public <T> DataBlockSerializer<T> initialize(final DatasetAttributes attributes, final BytesCodec... bytesCodecs) {

		ensureValidByteOrder(attributes.getDataType(), getByteOrder());
		return ZarrCodecs.createDataBlockCodec(dtype, attributes.getBlockSize(),
				fillValue, attributes.getCompression());
	}

	private static void ensureValidByteOrder(final DataType dataType, final ByteOrder byteOrder) {

		switch (dataType) {
		case INT8:
		case UINT8:
		case STRING:
		case OBJECT:
			return;
		}

		if (byteOrder == null)
			throw new IllegalArgumentException("DataType (" + dataType + ") requires ByteOrder, but was null");
	}
}
