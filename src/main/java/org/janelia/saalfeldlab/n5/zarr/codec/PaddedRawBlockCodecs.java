package org.janelia.saalfeldlab.n5.zarr.codec;

import java.nio.ByteOrder;
import java.util.Arrays;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.codec.BlockCodec;
import org.janelia.saalfeldlab.n5.codec.DataCodec;
import org.janelia.saalfeldlab.n5.codec.DeterministicSizeDataCodec;
import org.janelia.saalfeldlab.n5.codec.IdentityCodec;
import org.janelia.saalfeldlab.n5.codec.RawBlockCodecs;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.zarr.DType;
import org.janelia.saalfeldlab.n5.zarr.ZarrKeyValueWriter;


public class PaddedRawBlockCodecs {

	public static <T> BlockCodec<T> create(
			final DataType dataType,
			final ByteOrder byteOrder,
			final int[] blockSize,
			final DataCodec codec,
			final byte[] fillBytes ) {

		final BlockCodec<T> baseCodec = RawBlockCodecs.create(dataType, byteOrder, blockSize, new IdentityCodec());
		return new PaddedRawBlockCodec<T>( baseCodec, blockSize, new DType(dataType), codec, fillBytes);
	}

	private static class PaddedRawBlockCodec<T> implements BlockCodec<T> {

		private final BlockCodec<T> wrappedBlockCodec;
		private final int[] blockSize;
		private final DataCodec codec;
		private final DType dtype;
		private final byte[] fillBytes;

		PaddedRawBlockCodec(
				final BlockCodec<T> wrappedBlockCodec,
				final int[] blockSize,
				final DType dtype,
				final DataCodec codec,
				final byte[] fillBytes) {

			this.wrappedBlockCodec = wrappedBlockCodec;
			this.blockSize = blockSize;
			this.dtype = dtype;
			this.codec = codec;

			final int nBytes = dtype.getNBytes();
			this.fillBytes = new byte[nBytes];
			System.arraycopy(fillBytes, 0, this.fillBytes, 0, nBytes);
		}

		@Override
		public ReadData encode(DataBlock<T> dataBlock) throws N5IOException {

			// Handle null dataBlock (empty/default block from NonEmptyDataBlockSupplier)
			// Return null to signal that this block should not be written
			if (dataBlock == null) {
				return null;
			}

			final ReadData rawBlockData = wrappedBlockCodec.encode(dataBlock);
			final ReadData blockData;

			// Get block size, handling null getSize()
			final int[] dataBlockSize = dataBlock.getSize();

			// Handle three cases:
			// 1. dataBlockSize is null: use rawBlockData directly (can't pad without knowing size)
			// 2. Sizes match: use rawBlockData directly (no padding needed)
			// 3. Sizes differ: pad/crop to match expected blockSize
			if (dataBlockSize == null) {
				blockData = rawBlockData;
			} else if (Arrays.equals(blockSize, dataBlockSize)) {
				blockData = rawBlockData;
			} else {
				blockData = ReadData.from(
						ZarrKeyValueWriter.padCrop(
								rawBlockData.allBytes(),
								dataBlockSize,
								blockSize,
								dtype.getNBytes(),
								dtype.getNBits(),
								fillBytes));
			}

			return codec.encode(blockData);
		}

		@Override
		public DataBlock<T> decode(ReadData readData, long[] gridPosition) throws N5IOException {
			ReadData readfDataDecoded = codec.decode(readData);
			return wrappedBlockCodec.decode(readfDataDecoded, gridPosition);
		}

		@Override
		public long encodedSize(final int[] blockSize) throws UnsupportedOperationException {
			return wrappedBlockCodec.encodedSize(blockSize);
		}
	}

}
