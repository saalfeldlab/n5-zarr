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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Map;

import org.janelia.saalfeldlab.n5.AbstractN5Test;
import org.janelia.saalfeldlab.n5.Bzip2Compression;
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.GzipCompression;
import org.janelia.saalfeldlab.n5.N5Reader.Version;
import org.janelia.saalfeldlab.n5.blosc.BloscCompression;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.junit.Assert;
import org.junit.Test;

import net.imglib2.RandomAccess;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.type.numeric.IntegerType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.integer.LongType;
import net.imglib2.type.numeric.integer.UnsignedByteType;
import net.imglib2.type.numeric.integer.UnsignedIntType;
import net.imglib2.type.numeric.integer.UnsignedLongType;
import net.imglib2.type.numeric.real.DoubleType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.view.Views;

/**
 *
 *
 * @author Stephan Saalfeld &lt;saalfelds@janelia.hhmi.org&gt;
 */
public class N5ZarrTest extends AbstractN5Test {

	static private String testDirPath = System.getProperty("user.home") + "/tmp/n5-test.zarr";
	static private String testZarrDirPath = System.getProperty("user.home") + "/tmp/zarr-test.zarr";
	static private String testZarrDatasetName = "/test/data";


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

	private boolean runPythonTest() throws IOException, InterruptedException {

		final boolean isWindows = System.getProperty("os.name").toLowerCase().startsWith("windows");
		Process process;
		if (isWindows) {
		    process = Runtime.getRuntime().exec("cmd.exe /c python3 src\\test\\python\\zarr-test.py");
		} else {
		    process = Runtime.getRuntime().exec("python3 src/test/python/zarr-test.py");
		}
		final int exitCode = process.waitFor();
		new BufferedReader(new InputStreamReader(process.getErrorStream())).lines().forEach(System.out::println);
		process.destroy();

		return (exitCode == 0);
	}

	private static <T extends IntegerType<T>> void assertIsSequence(
			final RandomAccessibleInterval<T> source,
			final T ref) {

		ref.setZero();
		for (final T t : Views.flatIterable(source)) {

			if (!t.valueEquals(ref))
				throw new AssertionError("values not equal: expected " + ref + ", actual " + t);
			ref.inc();
		}
	}

	private static <T extends RealType<T>> void assertIsSequence(
			final RandomAccessibleInterval<T> source,
			final T ref) {

		ref.setReal(0);
		for (final T t : Views.flatIterable(source)) {

			if (!t.valueEquals(ref))
				throw new AssertionError("values not equal: expected " + ref + ", actual " + t);
			ref.inc();
		}
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testReadZarrPython() throws IOException, InterruptedException {

		/* create test data with python */
		if (!runPythonTest()) {
			System.out.println("Couldn't run Python test, skipping compatibility test with Python.");
			return;
		}

		final N5ZarrWriter n5Zarr = new N5ZarrWriter(testZarrDirPath, ".", true);
		final N5ZarrWriter n5ZarrWithoutMapping = new N5ZarrWriter(testZarrDirPath, ".", false);

		/* groups */
		assertTrue(n5Zarr.exists(testZarrDatasetName) && !n5Zarr.datasetExists(testZarrDatasetName));

		/* array parameters */
		final DatasetAttributes datasetAttributesC = n5Zarr.getDatasetAttributes(testZarrDatasetName + "/3x2_c_<i8");
		assertArrayEquals(datasetAttributesC.getDimensions(), new long[]{3, 2});
		assertArrayEquals(datasetAttributesC.getBlockSize(), new int[]{3, 2});
		assertEquals(datasetAttributesC.getDataType(), DataType.INT64);

		final DatasetAttributes datasetAttributesF = n5Zarr.getDatasetAttributes(testZarrDatasetName + "/3x2_f_<i8");
		assertArrayEquals(datasetAttributesF.getDimensions(), new long[]{2, 3});
		assertArrayEquals(datasetAttributesF.getBlockSize(), new int[]{2, 3});
		assertEquals(datasetAttributesF.getDataType(), DataType.INT64);

		/* N5 array parameter mapping */
		assertArrayEquals(n5Zarr.getAttribute(testZarrDatasetName + "/3x2_c_<i8", "dimensions", long[].class), new long[]{3, 2});
		assertArrayEquals(n5Zarr.getAttribute(testZarrDatasetName + "/3x2_c_<i8", "blockSize", int[].class), new int[]{3, 2});
		assertEquals(n5Zarr.getAttribute(testZarrDatasetName + "/3x2_c_<i8", "dataType", DataType.class), DataType.INT64);

		assertNull(n5ZarrWithoutMapping.getAttribute(testZarrDatasetName + "/3x2_c_<i8", "dimensions", long[].class));
		assertNull(n5ZarrWithoutMapping.getAttribute(testZarrDatasetName + "/3x2_c_<i8", "blockSize", int[].class));
		assertNull(n5ZarrWithoutMapping.getAttribute(testZarrDatasetName + "/3x2_c_<i8", "dataType", DataType.class));


		/* LE uint8 in C and F order */
		final UnsignedByteType refUnsignedByte = new UnsignedByteType();

		assertIsSequence(N5Utils.open(n5Zarr, testZarrDatasetName + "/3x2_c_|u1"), refUnsignedByte);
		assertIsSequence(Views.permute((RandomAccessibleInterval<UnsignedByteType>)N5Utils.open(n5Zarr, testZarrDatasetName + "/3x2_f_|u1"), 0, 1), refUnsignedByte);

		/* LE int64 in C and F order */
		final LongType refLong = new LongType();

		assertIsSequence(N5Utils.open(n5Zarr, testZarrDatasetName + "/3x2_c_<i8"), refLong);
		assertIsSequence(Views.permute((RandomAccessibleInterval<LongType>)N5Utils.open(n5Zarr, testZarrDatasetName + "/3x2_f_<i8"), 0, 1), refLong);

		assertIsSequence(N5Utils.open(n5Zarr, testZarrDatasetName + "/30x20_c_<i8"), refLong);
		assertIsSequence(Views.permute((RandomAccessibleInterval<LongType>)N5Utils.open(n5Zarr, testZarrDatasetName + "/30x20_f_<i8"), 0, 1), refLong);

		/* BE int32 in C and F order */
		final UnsignedIntType refUnsignedInt = new UnsignedIntType();

		assertIsSequence(N5Utils.open(n5Zarr, testZarrDatasetName + "/3x2_c_>u4"), refUnsignedInt);
		assertIsSequence(Views.permute((RandomAccessibleInterval<UnsignedIntType>)N5Utils.open(n5Zarr, testZarrDatasetName + "/3x2_f_>u4"), 0, 1), refUnsignedInt);

		assertIsSequence(N5Utils.open(n5Zarr, testZarrDatasetName + "/30x20_c_>u4"), refUnsignedInt);
		assertIsSequence(Views.permute((RandomAccessibleInterval<UnsignedIntType>)N5Utils.open(n5Zarr, testZarrDatasetName + "/30x20_f_>u4"), 0, 1), refUnsignedInt);

		/* LE float64 in C and F order */
		final DoubleType refDouble = new DoubleType();

		assertIsSequence(N5Utils.open(n5Zarr, testZarrDatasetName + "/3x2_c_<f8"), refDouble);
		assertIsSequence(Views.permute((RandomAccessibleInterval<DoubleType>)N5Utils.open(n5Zarr, testZarrDatasetName + "/3x2_f_<f8"), 0, 1), refDouble);

		assertIsSequence(N5Utils.open(n5Zarr, testZarrDatasetName + "/30x20_c_<f8"), refDouble);
		assertIsSequence(Views.permute((RandomAccessibleInterval<DoubleType>)N5Utils.open(n5Zarr, testZarrDatasetName + "/30x20_f_<f8"), 0, 1), refDouble);

		/* BE float32 in C and F order */
		final FloatType refFloat = new FloatType();

		assertIsSequence(N5Utils.open(n5Zarr, testZarrDatasetName + "/3x2_c_>f4"), refFloat);
		assertIsSequence(Views.permute((RandomAccessibleInterval<FloatType>)N5Utils.open(n5Zarr, testZarrDatasetName + "/3x2_f_>f4"), 0, 1), refFloat);

		assertIsSequence(N5Utils.open(n5Zarr, testZarrDatasetName + "/30x20_c_>f4"), refFloat);
		assertIsSequence(Views.permute((RandomAccessibleInterval<FloatType>)N5Utils.open(n5Zarr, testZarrDatasetName + "/30x20_f_>f4"), 0, 1), refFloat);

		/* compressors */
		final UnsignedLongType refUnsignedLong = new UnsignedLongType();

		assertIsSequence(N5Utils.open(n5Zarr, testZarrDatasetName + "/30x20_c_>u8_zlib"), refUnsignedLong);
		assertIsSequence(N5Utils.open(n5Zarr, testZarrDatasetName + "/30x20_c_>u8_gzip"), refUnsignedLong);
		assertIsSequence(N5Utils.open(n5Zarr, testZarrDatasetName + "/30x20_c_>u8_bz2"), refUnsignedLong);

		/* fill value 1 */
		String datasetName = testZarrDatasetName + "/3x2_c_>u4_f1";

		final RandomAccessibleInterval<UnsignedIntType> a3x2_c_bu4_f1 = N5Utils.open(n5Zarr, datasetName);
		assertIsSequence(a3x2_c_bu4_f1, refUnsignedInt);

		DatasetAttributes attributes = n5Zarr.getDatasetAttributes(datasetName);
		final long[] shape = attributes.getDimensions();
		Arrays.setAll(shape, i -> shape[i] + 5);
		n5Zarr.setAttribute(datasetName, "dimensions", shape);

		final RandomAccessibleInterval<UnsignedIntType> a3x2_c_bu4_f1_after = N5Utils.open(n5Zarr, datasetName);
		assertIsSequence(Views.interval(a3x2_c_bu4_f1_after,  a3x2_c_bu4_f1), refUnsignedInt);
		final RandomAccess<UnsignedIntType> ra = a3x2_c_bu4_f1_after.randomAccess();
		final int fill_value = Integer.parseInt(n5Zarr.getZArraryAttributes(datasetName).getFillValue());
		ra.setPosition(shape[0] - 5, 0);
		assertEquals(fill_value, ra.get().getInteger());
		ra.setPosition(shape[1] - 5, 1);
		assertEquals(fill_value, ra.get().getInteger());


		/* fill value NaN */
		datasetName = testZarrDatasetName + "/3x2_c_<f4_fnan";

		final RandomAccessibleInterval<FloatType> a3x2_c_lf4_fnan = N5Utils.open(n5Zarr, datasetName);
		assertIsSequence(a3x2_c_lf4_fnan, refFloat);

		attributes = n5Zarr.getDatasetAttributes(datasetName);
		final long[] shapef = attributes.getDimensions();
		Arrays.setAll(shapef, i -> shapef[i] + 5);
		n5Zarr.setAttribute(datasetName, "dimensions", shapef);

		final RandomAccessibleInterval<FloatType> a3x2_c_lf4_fnan_after = N5Utils.open(n5Zarr, datasetName);
		assertIsSequence(Views.interval(a3x2_c_lf4_fnan_after,  a3x2_c_lf4_fnan), refFloat);
		final RandomAccess<FloatType> raf = a3x2_c_lf4_fnan_after.randomAccess();
		raf.setPosition(shapef[0] - 5, 0);
		assertTrue(Float.isNaN(raf.get().getRealFloat()));
		raf.setPosition(shapef[1] - 5, 1);
		assertTrue(Float.isNaN(raf.get().getRealFloat()));

		/* remove the container */
		n5Zarr.remove();
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
