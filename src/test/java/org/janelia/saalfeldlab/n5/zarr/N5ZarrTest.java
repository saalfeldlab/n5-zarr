/**
 * Copyright (c) 2019, Stephan Saalfeld
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package org.janelia.saalfeldlab.n5.zarr;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Map;

import org.janelia.saalfeldlab.n5.AbstractN5Test;
import org.janelia.saalfeldlab.n5.Bzip2Compression;
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.GzipCompression;
import org.janelia.saalfeldlab.n5.N5Reader.Version;
import org.janelia.saalfeldlab.n5.blosc.BloscCompression;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 *
 * @author Stephan Saalfeld &lt;saalfelds@janelia.hhmi.org&gt;
 */
public class N5ZarrTest extends AbstractN5Test {

	static private String testDirPath = System.getProperty("user.home") + "/tmp/n5-test.zarr";

	/**
	 * @throws IOException
	 */
	@Override
	protected N5ZarrWriter createN5Writer() throws IOException {

		return new N5ZarrWriter(testDirPath);
	}

	@Override
	protected Compression[] getCompressions() {

		return new Compression[] {
				new Bzip2Compression(),
				new GzipCompression(),
				new GzipCompression(5, true),
				new BloscCompression(),
				new BloscCompression("lz4", 6, BloscCompression.BITSHUFFLE, 0, 4)
			};
	}

	@Override
	@Test
	public void testCreateDataset() {

		try {
			n5.createDataset(datasetName, dimensions, blockSize, DataType.UINT64, getCompressions()[0]);
		} catch (final IOException e) {
			fail(e.getMessage());
		}

		if (!n5.exists(datasetName))
			fail("Dataset does not exist");

		try {
			final DatasetAttributes info = n5.getDatasetAttributes(datasetName);
			Assert.assertArrayEquals(dimensions, info.getDimensions());
			Assert.assertArrayEquals(blockSize, info.getBlockSize());
			Assert.assertEquals(DataType.UINT64, info.getDataType());
			Assert.assertEquals(getCompressions()[0].getClass(), info.getCompression().getClass());
		} catch (final IOException e) {
			fail("Dataset info cannot be opened");
			e.printStackTrace();
		}
	}

	@Override
	@Test
	public void testVersion() throws NumberFormatException, IOException {

		n5.createGroup("/");

		final Version n5Version = n5.getVersion();

		System.out.println(n5Version);

		Assert.assertTrue(n5Version.equals(N5ZarrReader.VERSION));

		Assert.assertTrue(N5ZarrReader.VERSION.isCompatible(n5.getVersion()));
	}

	@Override
	@Test
	public void testExists() {

		final String groupName2 = groupName + "-2";
		final String datasetName2 = datasetName + "-2";
		final String notExists = groupName + "-notexists";
		try {
			n5.createDataset(datasetName2, dimensions, blockSize, DataType.UINT64, getCompressions()[0]);
			Assert.assertTrue(n5.exists(datasetName2));
			Assert.assertTrue(n5.datasetExists(datasetName2));

			n5.createGroup(groupName2);
			Assert.assertTrue(n5.exists(groupName2));
			Assert.assertFalse(n5.datasetExists(groupName2));

			Assert.assertFalse(n5.exists(notExists));
			Assert.assertFalse(n5.datasetExists(notExists));

			Assert.assertTrue(n5.remove(datasetName2));
			Assert.assertTrue(n5.remove(groupName2));
		} catch (final IOException e) {
			fail(e.getMessage());
		}
	}

	@Override
	@Test
	public void testListAttributes() {

		final String groupName2 = groupName + "-2";
		final String datasetName2 = datasetName + "-2";
		try {
			n5.createDataset(datasetName2, dimensions, blockSize, DataType.UINT64, getCompressions()[0]);
			n5.setAttribute(datasetName2, "attr1", new double[] {1, 2, 3});
			n5.setAttribute(datasetName2, "attr2", new String[] {"a", "b", "c"});
			n5.setAttribute(datasetName2, "attr3", 1.0);
			n5.setAttribute(datasetName2, "attr4", "a");

			Map<String, Class<?>> attributesMap = n5.listAttributes(datasetName2);
			Assert.assertTrue(attributesMap.get("attr1") == double[].class);
			Assert.assertTrue(attributesMap.get("attr2") == String[].class);
			Assert.assertTrue(attributesMap.get("attr3") == double.class);
			Assert.assertTrue(attributesMap.get("attr4") == String.class);

			n5.createGroup(groupName2);
			n5.setAttribute(groupName2, "attr1", new double[] {1, 2, 3});
			n5.setAttribute(groupName2, "attr2", new String[] {"a", "b", "c"});
			n5.setAttribute(groupName2, "attr3", 1.0);
			n5.setAttribute(groupName2, "attr4", "a");

			attributesMap = n5.listAttributes(datasetName2);
			Assert.assertTrue(attributesMap.get("attr1") == double[].class);
			Assert.assertTrue(attributesMap.get("attr2") == String[].class);
			Assert.assertTrue(attributesMap.get("attr3") == double.class);
			Assert.assertTrue(attributesMap.get("attr4") == String.class);
		} catch (final IOException e) {
			fail(e.getMessage());
		}
	}

	@Override
	@Test
	public void testMode1WriteReadByteBlock() {

		// Not supported by Zarr
	}

//	/**
//	 * @throws IOException
//	 */
//	@AfterClass
//	public static void rampDownAfterClass() throws IOException {
//
////		Assert.assertTrue(n5.remove());
////		initialized = false;
//		n5 = null;
//	}

}
