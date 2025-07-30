package org.janelia.saalfeldlab.n5.zarr.codec;

import static org.junit.Assert.assertArrayEquals;

import org.junit.Test;

public class PadTest {

	@Test
	public void testPadCrop() {

		final byte[] src = new byte[]{1, 2, 3, 4}; // 2x2
		final int[] srcBlockSize = new int[]{2, 2};
		final int[] dstBlockSize = new int[]{3, 3};
		final int nBytes = 1;
		final int nBits = 0;
		final byte[] fillValue = new byte[]{5};

		final byte[] dst = ZarrCodecs.padCrop(src, srcBlockSize, dstBlockSize, nBytes, nBits, fillValue);
		assertArrayEquals(new byte[]{1, 2, 5, 3, 4, 5, 5, 5, 5}, dst);
	}

}
