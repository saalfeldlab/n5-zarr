/*-
 * #%L
 * Not HDF5
 * %%
 * Copyright (C) 2019 - 2022 Stephan Saalfeld
 * %%
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
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDERS OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 * #L%
 */
package org.janelia.saalfeldlab.n5.zarr;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.janelia.saalfeldlab.n5.AbstractN5Test;
import org.janelia.saalfeldlab.n5.Bzip2Compression;
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.GzipCompression;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.N5Reader.Version;
import org.janelia.saalfeldlab.n5.RawCompression;
import org.janelia.saalfeldlab.n5.blosc.BloscCompression;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.gson.reflect.TypeToken;

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

	static private String testZarrDatasetName = "/test/data";

	protected static Set<String> tmpFiles = new HashSet<>();

	@AfterClass
	public static void cleanup() {

		for (String tmpFile : tmpFiles) {
			try {
				FileUtils.deleteDirectory(new File(tmpFile));
			} catch (Exception e) {
			}
		}
	}

	private static String createTestDirPath(String prefix) throws IOException {

		return Files.createTempDirectory(prefix).toFile().getCanonicalPath();
	}

	@Override
	protected String tempN5Location() throws URISyntaxException {

		return new URI("file", null, tmpPathName(), null).toString();
	}

	protected static String tmpPathName() {
		try {
			final File tmpFile = Files.createTempDirectory("n5-zarr-test-").toFile();
			tmpFile.deleteOnExit();
			final String tmpPath = tmpFile.getCanonicalPath();
			tmpFiles.add(tmpPath);
			return tmpPath;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	protected N5ZarrWriter createN5Writer() throws IOException {

		final String testDirPath = tmpPathName();
		return createN5Writer(testDirPath, new GsonBuilder());
	}

	@Override
	protected N5ZarrWriter createN5Writer(String location, GsonBuilder gsonBuilder) throws IOException {

		return createN5Writer(location, gsonBuilder, ".");
	}

	protected N5ZarrWriter createN5Writer(String location, String dimensionSeparator) throws IOException {

		return createN5Writer(location, new GsonBuilder(), dimensionSeparator);
	}

	protected N5ZarrWriter createN5Writer(String location, GsonBuilder gsonBuilder, String dimensionSeparator) throws IOException {

		final Path testN5Path = Paths.get(location);
		final boolean existsBefore = testN5Path.toFile().exists();
		final N5ZarrWriter zarr = new N5ZarrWriter(location, gsonBuilder, dimensionSeparator, true, false);
		final boolean existsAfter = testN5Path.toFile().exists();
		if (!existsBefore && existsAfter) {
			tmpFiles.add(location);
		}
		return zarr;
	}

	@Override
	protected N5Reader createN5Reader(String location, GsonBuilder gson) throws IOException {

		return new N5ZarrReader(location, gson);
	}

	@Override
	protected Compression[] getCompressions() {

		return new Compression[] {
				new Bzip2Compression(),
				new GzipCompression(),
				new GzipCompression(5, true),
				new BloscCompression(),
				new BloscCompression("lz4", 6, BloscCompression.BITSHUFFLE, 0, 4),
				new RawCompression()
			};
	}

	@Override
	@Test
	public void testCreateDataset() {
		try {
			n5.createDataset(datasetName, dimensions, blockSize, DataType.UINT64, getCompressions()[0]);
		} catch (final IOException e) {
			throw new AssertionError(e.getMessage(), e);
		}

		assertTrue("Dataset does not exist", n5.exists(datasetName));

		try {
			final DatasetAttributes info = n5.getDatasetAttributes(datasetName);
			assertArrayEquals(dimensions, info.getDimensions());
			assertArrayEquals(blockSize, info.getBlockSize());
			assertEquals(DataType.UINT64, info.getDataType());
			assertEquals(getCompressions()[0].getClass(), info.getCompression().getClass());
		} catch (final IOException e) {
			throw new AssertionError("Dataset info cannot be opened", e);
		}
	}

	@Test
	public void testCreateNestedDataset() throws IOException {
		final String datasetName = "/test/nested/data";

		final String testDirPath = createTestDirPath("n5-zarr-test-");
		final N5ZarrWriter n5Nested = createN5Writer( testDirPath, "/" );

		n5Nested.createDataset(datasetName, dimensions, blockSize, DataType.UINT64, getCompressions()[0]);
		assertEquals( "/", n5Nested.getZArrayAttributes(datasetName).getDimensionSeparator());

		// TODO test that parents of nested dataset are groups

		n5Nested.remove(datasetName);
		n5Nested.close();
	}

	@Test
	public void testCreateDatasetNameEmpty() throws IOException {
		final String testDirPath = createTestDirPath("n5-zarr-test-");
		N5ZarrWriter n5 = new N5ZarrWriter(testDirPath );
		n5.createDataset("", dimensions, blockSize, DataType.UINT64, getCompressions()[0]);
		n5.remove();
		n5.close();
	}

	@Test
	public void testCreateDatasetNameSlash() throws IOException {
		final String testDirPath = createTestDirPath("n5-zarr-test-");
		N5ZarrWriter n5 = new N5ZarrWriter(testDirPath );
		n5.createDataset("", dimensions, blockSize, DataType.UINT64, getCompressions()[0]);
		n5.remove();
		n5.close();
	}

	@Test
	public void testPadCrop() throws Exception {
		final byte[] src = new byte[] { 1, 1, 1, 1 };  // 2x2
		final int[] srcBlockSize = new int[] { 2, 2 };
		final int[] dstBlockSize = new int[] { 3, 3 };
		final int nBytes = 1;
		final int nBits = 0;
		final byte[] fillValue = new byte[] { 0 };

		final byte[] dst = N5ZarrWriter.padCrop(src, srcBlockSize, dstBlockSize, nBytes, nBits, fillValue);
		assertArrayEquals(new byte[] { 1, 1, 0, 1, 1, 0, 0, 0, 0 }, dst);
	}

	@Override
	@Test
	public void testVersion() throws NumberFormatException, IOException {

		try (final N5Writer writer = createN5Writer()) {

			@SuppressWarnings("resource")
			final N5ZarrWriter zarr = (N5ZarrWriter)writer;
			final Version n5Version = writer.getVersion();
			Assert.assertTrue(n5Version.equals(N5ZarrReader.VERSION));

			final JsonObject bumpVersion = new JsonObject();
			bumpVersion.add( ZarrKeyValueReader.ZARR_FORMAT_KEY, new JsonPrimitive( N5ZarrReader.VERSION.getMajor() + 1));
			zarr.writeZGroup("", bumpVersion);

			final Version version = writer.getVersion();
			assertFalse(N5ZarrReader.VERSION.isCompatible(version));

			// check that writer creation fails for incompatible version
			assertThrows(IOException.class, () -> createN5Writer( writer.getBasePath() ));

//			final Version compatibleVersion = new Version(N5ZarrReader.VERSION.getMajor(), N5ZarrReader.VERSION.getMinor(), N5Reader.VERSION.getPatch());
//			writer.setAttribute("/", ZarrUtils.ZARR_FORMAT_KEY, compatibleVersion.toString());
		}
	}

	@Test
	@Override
	public void testReaderCreation() throws IOException, URISyntaxException {

		final File tmpFile = Files.createTempDirectory("reader-create-test-").toFile();
		tmpFile.delete();
		final String canonicalPath = tmpFile.getCanonicalPath();
		try (N5Writer writer = createN5Writer(canonicalPath)) {

			final N5Reader n5r = createN5Reader(canonicalPath);
			assertNotNull(n5r);

			// existing directory without attributes is okay;
			// Remove and create to remove attributes store
			writer.remove("/");
			writer.createGroup("/");
			final N5Reader na = createN5Reader(canonicalPath);
			assertNotNull(na);

			// existing location with attributes, but no version
			writer.remove("/");
			writer.createGroup("/");
			writer.setAttribute("/", "mystring", "ms");
			final N5Reader wa = createN5Reader(canonicalPath);
			assertNotNull(wa);

			// non-existent directory should fail
			writer.remove("/");
			assertThrows("Non-existant location throws error", IOException.class,
					() -> {
						final N5Reader test = createN5Reader(canonicalPath);
						test.list("/");
					});
		}
	}

	@Override
	@Test
	public void testExists() {
		final String groupName2 = groupName + "-2";
		final String datasetName2 = datasetName + "-2";
		final String notExists = groupName + "-notexists";
		try {
			n5.createDataset(datasetName2, dimensions, blockSize, DataType.UINT64, getCompressions()[0]);
			assertTrue(n5.exists(datasetName2));
			assertTrue(n5.datasetExists(datasetName2));

			n5.createGroup(groupName2);
			assertTrue(n5.exists(groupName2));
			assertFalse(n5.datasetExists(groupName2));

			assertFalse(n5.exists(notExists));
			assertFalse(n5.datasetExists(notExists));

			assertTrue(n5.remove(datasetName2));
			assertTrue(n5.remove(groupName2));
		} catch (final IOException e) {
			throw new AssertionError(e.getMessage(), e);
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
			n5.setAttribute(datasetName2, "attr5", 5.4);

			Map<String, Class<?>> attributesMap = n5.listAttributes(datasetName2);
			assertTrue(attributesMap.get("attr1") == long[].class);
			assertTrue(attributesMap.get("attr2") == String[].class);
			assertTrue(attributesMap.get("attr3") == long.class);
			assertTrue(attributesMap.get("attr4") == String.class);
			assertTrue(attributesMap.get("attr5") == double.class);

			n5.createGroup(groupName2);
			n5.setAttribute(groupName2, "attr1", new double[] {1, 2, 3});
			n5.setAttribute(groupName2, "attr2", new String[] {"a", "b", "c"});
			n5.setAttribute(groupName2, "attr3", 1.0);
			n5.setAttribute(groupName2, "attr4", "a");
			n5.setAttribute(groupName2, "attr5", 5.4);

			attributesMap = n5.listAttributes(datasetName2);
			assertTrue(attributesMap.get("attr1") == long[].class);
			assertTrue(attributesMap.get("attr2") == String[].class);
			assertTrue(attributesMap.get("attr3") == long.class);
			assertTrue(attributesMap.get("attr4") == String.class);
			assertTrue(attributesMap.get("attr5") == double.class);
		} catch (final IOException e) {
			throw new AssertionError(e.getMessage(), e);
		}
	}

	@Override
	@Test
	@Ignore("Zarr does not currently support mode 1 data blocks.")
	public void testMode1WriteReadByteBlock() {
	}

	@Override
	@Test
	@Ignore("Zarr does not currently support mode 2 data blocks and serialized objects.")
	public void testWriteReadSerializableBlock() {
	}

	private boolean runPythonTest(String script) throws IOException, InterruptedException {

		final boolean isWindows = System.getProperty("os.name").toLowerCase().startsWith("windows");
		Process process;
		if (isWindows) {
		    process = Runtime.getRuntime().exec("cmd.exe /c python3 src\\test\\python\\" + script);
		} else {
		    process = Runtime.getRuntime().exec("python3 src/test/python/" + script );
		}
		final int exitCode = process.waitFor();
		new BufferedReader(new InputStreamReader(process.getErrorStream())).lines().forEach(System.out::println);
		process.destroy();

		return (exitCode == 0 );
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
		if (!runPythonTest("zarr-test.py")) {
			System.out.println("Couldn't run Python test, skipping compatibility test with Python.");
			return;
		}

		final String testZarrDirPath = createTestDirPath( "zarr-test-" );
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
		final int fill_value = Integer.parseInt(n5Zarr.getZArrayAttributes(datasetName).getFillValue());
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
		n5Zarr.close();
		n5ZarrWithoutMapping.close();
	}

	@Test
	public void testReadZarrNestedPython() throws IOException, InterruptedException {

		/* create test data with python */
		if (!runPythonTest("zarr-nested-test.py")) {
			System.out.println("Couldn't run Python test, skipping compatibility test with Python.");
			return;
		}

		final String testZarrNestedDirPath = createTestDirPath( "zarr-test-nested-" );
		final N5ZarrWriter n5Zarr = new N5ZarrWriter(testZarrNestedDirPath, ".", true);

		/* groups */
		System.out.println( n5Zarr.exists(testZarrDatasetName));
		System.out.println( n5Zarr.datasetExists(testZarrDatasetName));
		assertTrue(n5Zarr.exists(testZarrDatasetName) && !n5Zarr.datasetExists(testZarrDatasetName));

		/* array parameters */
		final DatasetAttributes datasetAttributesC = n5Zarr.getDatasetAttributes(testZarrDatasetName + "/3x2_c_|u1");
		assertArrayEquals(datasetAttributesC.getDimensions(), new long[]{3, 2});
		assertArrayEquals(datasetAttributesC.getBlockSize(), new int[]{3, 2});
		assertEquals(datasetAttributesC.getDataType(), DataType.UINT8);
		assertEquals( n5Zarr.getZArrayAttributes(testZarrDatasetName + "/3x2_c_|u1").getDimensionSeparator(), "/" );

		final UnsignedByteType refUnsignedByte = new UnsignedByteType();
		assertIsSequence(N5Utils.open(n5Zarr, testZarrDatasetName + "/3x2_c_|u1"), refUnsignedByte);

		/* remove the container */
		n5Zarr.remove();
		n5Zarr.close();
	}

	@Test
	public void testRawCompressorNullInZarray() throws IOException, FileNotFoundException, ParseException {
		final String testZarrDirPath = createTestDirPath( "zarr-test-" );
		final N5ZarrWriter n5 = new N5ZarrWriter(testZarrDirPath);
		n5.createDataset(testZarrDatasetName, new long[]{1, 2, 3}, new int[]{1, 2, 3}, DataType.UINT16, new RawCompression());
		final JSONParser jsonParser = new JSONParser();
		try (FileReader freader = new FileReader(testZarrDirPath + testZarrDatasetName + "/.zarray")) {
			final JSONObject zarray = (JSONObject) jsonParser.parse(freader);
			final JSONObject compressor = (JSONObject) zarray.get("compressor");
			assertTrue(compressor == null);
		} finally {
			n5.remove();
			n5.close();
		}
	}

	@Test
	@Override
	public void testAttributes() throws IOException {

		try (final N5Writer n5 = createN5Writer()) {
			n5.createGroup(groupName);

			n5.setAttribute(groupName, "key1", "value1");
			Assert.assertEquals(2, n5.listAttributes(groupName).size()); // length 2 because it includes "zarr_version"

			/* class interface */
			Assert.assertEquals("value1", n5.getAttribute(groupName, "key1", String.class));
			/* type interface */
			Assert.assertEquals("value1", n5.getAttribute(groupName, "key1", new TypeToken<String>(){}.getType()));

			final Map<String, String> newAttributes = new HashMap<>();
			newAttributes.put("key2", "value2");
			newAttributes.put("key3", "value3");
			n5.setAttributes(groupName, newAttributes);

			Assert.assertEquals(4, n5.listAttributes(groupName).size());
			/* class interface */
			Assert.assertEquals("value1", n5.getAttribute(groupName, "key1", String.class));
			Assert.assertEquals("value2", n5.getAttribute(groupName, "key2", String.class));
			Assert.assertEquals("value3", n5.getAttribute(groupName, "key3", String.class));
			/* type interface */
			Assert.assertEquals("value1", n5.getAttribute(groupName, "key1", new TypeToken<String>(){}.getType()));
			Assert.assertEquals("value2", n5.getAttribute(groupName, "key2", new TypeToken<String>(){}.getType()));
			Assert.assertEquals("value3", n5.getAttribute(groupName, "key3", new TypeToken<String>(){}.getType()));

			// test the case where the resulting file becomes shorter
			n5.setAttribute(groupName, "key1", new Integer(1));
			n5.setAttribute(groupName, "key2", new Integer(2));

			Assert.assertEquals(4, n5.listAttributes(groupName).size());
			/* class interface */
			Assert.assertEquals(new Integer(1), n5.getAttribute(groupName, "key1", Integer.class));
			Assert.assertEquals(new Integer(2), n5.getAttribute(groupName, "key2", Integer.class));
			Assert.assertEquals("value3", n5.getAttribute(groupName, "key3", String.class));
			/* type interface */
			Assert.assertEquals(new Integer(1), n5.getAttribute(groupName, "key1", new TypeToken<Integer>(){}.getType()));
			Assert.assertEquals(new Integer(2), n5.getAttribute(groupName, "key2", new TypeToken<Integer>(){}.getType()));
			Assert.assertEquals("value3", n5.getAttribute(groupName, "key3", new TypeToken<String>(){}.getType()));

			n5.removeAttribute(groupName, "key1");
			n5.removeAttribute(groupName, "key2");
			n5.removeAttribute(groupName, "key3");
			Assert.assertEquals(1, n5.listAttributes(groupName).size());
		}
	}

	@Test
	@Override
	@Ignore
	public void testNullAttributes() throws IOException {
		// TODO rework this test given that gson's serialize nulls must be turned on
	}

	@Test
	@Override
	@Ignore
	public void testRootLeaves() throws IOException {
		// probably not feasible if mergeAttributes are turned on.
	}

//	/**
//	 * @throws IOException
//	 */
//	@AfterClass
//	public static void rampDownAfterClass() throws IOException {
//
////		assertTrue(n5.remove());
////		initialized = false;
//		n5 = null;
//	}

}
