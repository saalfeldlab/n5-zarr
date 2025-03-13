package org.janelia.saalfeldlab.n5.zarr.chunks;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.util.GridIterator;

public class ChunkPad {

	public static void padDataBlock( final DataBlock<?> src, final DataBlock<?> dst) {

		if (src.getData() instanceof byte[] && dst.getData() instanceof byte[]) {

			copyRange((byte[])src.getData(), src.getSize(), (byte[])dst.getData(), dst.getSize());

		} else if (src.getData() instanceof short[] && dst.getData() instanceof short[]) {

			copyRange((short[])src.getData(), src.getSize(), (short[])dst.getData(), dst.getSize());

		} else if (src.getData() instanceof int[] && dst.getData() instanceof int[]) {

			copyRange((int[])src.getData(), src.getSize(), (int[])dst.getData(), dst.getSize());

		} else if (src.getData() instanceof long[] && dst.getData() instanceof long[]) {

			copyRange((long[])src.getData(), src.getSize(), (long[])dst.getData(), dst.getSize());

		} else if (src.getData() instanceof float[] && dst.getData() instanceof float[]) {

			copyRange((float[])src.getData(), src.getSize(), (float[])dst.getData(), dst.getSize());

		} else if (src.getData() instanceof double[] && dst.getData() instanceof double[]) {

			copyRange((double[])src.getData(), src.getSize(), (double[])dst.getData(), dst.getSize());
		}
		else 
			throw new N5Exception("Data block types are either not the same, or unsupported:\n"
					+ "src: " + src.getData().getClass() + "\n"
					+ "dst: " + dst.getData().getClass());
	}

	public static void copyRange(
			final byte[] src,
			final int[] srcSize,
			final byte[] dest,
			final int[] destSize) {

		final long[] p = new long[srcSize.length];
		for (int i = 0; i < src.length; i++) {
			GridIterator.indexToPosition(i, srcSize, p);
			final int j = (int)GridIterator.positionToIndex(destSize, p);
			dest[j] = src[i];
		}
	}

	public static void copyRange(
			final short[] src,
			final int[] srcSize,
			final short[] dest,
			final int[] destSize) {

		final long[] p = new long[srcSize.length];
		for (int i = 0; i < src.length; i++) {
			GridIterator.indexToPosition(i, srcSize, p);
			final int j = (int)GridIterator.positionToIndex(destSize, p);
			dest[j] = src[i];
		}
	}

	public static void copyRange(
			final int[] src,
			final int[] srcSize,
			final int[] dest,
			final int[] destSize) {

		final long[] p = new long[srcSize.length];
		for (int i = 0; i < src.length; i++) {
			GridIterator.indexToPosition(i, srcSize, p);
			final int j = (int)GridIterator.positionToIndex(destSize, p);
			dest[j] = src[i];
		}
	}

	public static void copyRange(
			final long[] src,
			final int[] srcSize,
			final long[] dest,
			final int[] destSize) {

		final long[] p = new long[srcSize.length];
		for (int i = 0; i < src.length; i++) {
			GridIterator.indexToPosition(i, srcSize, p);
			final int j = (int)GridIterator.positionToIndex(destSize, p);
			dest[j] = src[i];
		}
	}

	public static void copyRange(
			final float[] src,
			final int[] srcSize,
			final float[] dest,
			final int[] destSize) {

		final long[] p = new long[srcSize.length];
		for (int i = 0; i < src.length; i++) {
			GridIterator.indexToPosition(i, srcSize, p);
			final int j = (int)GridIterator.positionToIndex(destSize, p);
			dest[j] = src[i];
		}
	}

	public static void copyRange(
			final double[] src,
			final int[] srcSize,
			final double[] dest,
			final int[] destSize) {

		final long[] p = new long[srcSize.length];
		for (int i = 0; i < src.length; i++) {
			GridIterator.indexToPosition(i, srcSize, p);
			final int j = (int)GridIterator.positionToIndex(destSize, p);
			dest[j] = src[i];
		}
	}

}
