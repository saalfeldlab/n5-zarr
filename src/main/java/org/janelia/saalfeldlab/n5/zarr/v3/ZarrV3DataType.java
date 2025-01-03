package org.janelia.saalfeldlab.n5.zarr.v3;

import java.nio.ByteBuffer;

import org.janelia.saalfeldlab.n5.DataType;

public enum ZarrV3DataType {
	int8(8), uint8(8), int16(16), uint16(16), int32(32), uint32(32), int64(64), uint64(64), float32(32), float64(64);

	private final int nBytes;

	private ZarrV3DataType(final int nBytes) {
		this.nBytes = nBytes;
	}

	public DataType getDataType() {

		return DataType.valueOf(toString().toUpperCase());
	}

	public static ZarrV3DataType fromDataType(final DataType dataType) {

		return valueOf(dataType.toString().toLowerCase());
	}

	public byte[] createFillBytes(final String fill_value) {

		final byte[] fillBytes = new byte[nBytes];
		final ByteBuffer fillBuffer = ByteBuffer.wrap(fillBytes);

		if (fill_value.equals("NaN")) {
			if (nBytes == 8) {
				fillBuffer.putDouble(Double.NaN);
			} else if (nBytes == 4) {
				fillBuffer.putFloat(Float.NaN);
			}
		} else if (fill_value.equals("Infinity")) {
			if (nBytes == 8) {
				fillBuffer.putDouble(Double.POSITIVE_INFINITY);
			} else if (nBytes == 4) {
				fillBuffer.putFloat(Float.POSITIVE_INFINITY);
			}
		} else if (fill_value.equals("-Infinity")) {
			if (nBytes == 8) {
				fillBuffer.putDouble(Double.NEGATIVE_INFINITY);
			} else if (nBytes == 4) {
				fillBuffer.putFloat(Float.NEGATIVE_INFINITY);
			}
		} else {
			try {
				final DataType dataType = getDataType();
				switch (dataType) {
				case INT8:
					fillBytes[0] = Byte.parseByte(fill_value);
					break;
				case UINT8:
					fillBytes[0] = (byte)(0xff & Integer.parseInt(fill_value));
					break;
				case INT16:
					fillBuffer.putShort(Short.parseShort(fill_value));
					break;
				case UINT16:
					fillBuffer.putShort((short)(0xffff & Integer.parseInt(fill_value)));
					break;
				case INT32:
					fillBuffer.putInt(Integer.parseInt(fill_value));
					break;
				case UINT32:
					fillBuffer.putInt(Integer.parseUnsignedInt(fill_value));
					break;
				case INT64:
					fillBuffer.putLong(Long.parseLong(fill_value));
					break;
				case UINT64:
					fillBuffer.putLong(Long.parseUnsignedLong(fill_value));
					break;
				case FLOAT32:
					fillBuffer.putFloat(Float.parseFloat(fill_value));
					break;
				case FLOAT64:
					fillBuffer.putDouble(Double.parseDouble(fill_value));
					break;
				}
			} catch (final NumberFormatException e) {}
		}
		return fillBytes;
	}
	
}
