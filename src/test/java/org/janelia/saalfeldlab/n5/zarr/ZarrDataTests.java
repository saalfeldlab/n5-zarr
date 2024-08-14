package org.janelia.saalfeldlab.n5.zarr;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.universe.N5Factory;
import org.junit.Test;

public class ZarrDataTests {

	@Test
	public void dimensionTests() {

		final N5Factory n5Factory = new N5Factory();
		final N5Reader n5 = n5Factory.openReader("zarr:src/test/resources/examples/dimensionOrder.zarr");

		final int N = 3 * 4 * 5; // data size
		final byte[] cExpectedData = new byte[N];
		for (int i = 0; i < N; i++)
			cExpectedData[i] = (byte)i;

		int n = 0;
		final byte[] fExpectedData = new byte[N];
		for (int i = 0; i < 3; i++)
			for (int j = 0; j < 4; j++)
				for (int k = 0; k < 5; k++)
					fExpectedData[i + 3 * j + 12 * k] = (byte)(n++);

		assertTrue("root does not exist", n5.exists("/"));

		validate(n5, "c-order", new long[]{5, 4, 3}, cExpectedData);
		validate(n5, "f-order", new long[]{3, 4, 5}, fExpectedData);

	}

	private void validate(N5Reader n5, final String dset, long[] expectedSize, byte[] expectedData) {

		assertTrue(dset + " array does not exist", n5.exists(dset));

		final DatasetAttributes attrs = n5.getDatasetAttributes(dset);
		assertArrayEquals(dset + " array size incorrect", expectedSize, attrs.getDimensions());

		@SuppressWarnings("unchecked")
		final DataBlock<byte[]> blk = (DataBlock<byte[]>)n5.readBlock(dset, attrs, 0, 0, 0);
		assertArrayEquals(dset + "data incorrect", expectedData, blk.getData());
	}

}
