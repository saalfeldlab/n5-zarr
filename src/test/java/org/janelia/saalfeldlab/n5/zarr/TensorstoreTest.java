package org.janelia.saalfeldlab.n5.zarr;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.ProcessBuilder.Redirect;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.commons.codec.digest.PureJavaCrc32C;
import org.apache.commons.io.FileUtils;
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.FileSystemKeyValueAccess;
import org.janelia.saalfeldlab.n5.GzipCompression;

import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.RawCompression;
import org.janelia.saalfeldlab.n5.ShortArrayDataBlock;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.janelia.saalfeldlab.n5.zarr.v3.ZarrV3DatasetAttributes;
import org.janelia.saalfeldlab.n5.zarr.v3.ZarrV3KeyValueReader;
import org.janelia.saalfeldlab.n5.zarr.v3.ZarrV3KeyValueWriter;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.google.gson.GsonBuilder;

import net.imglib2.RandomAccessibleInterval;
import net.imglib2.cache.img.CachedCellImg;
import net.imglib2.img.Img;
import net.imglib2.img.array.ArrayCursor;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgFactory;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.IntegerType;
import net.imglib2.type.numeric.NumericType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.integer.LongType;
import net.imglib2.type.numeric.integer.UnsignedByteType;
import net.imglib2.type.numeric.integer.UnsignedIntType;
import net.imglib2.type.numeric.integer.UnsignedLongType;
import net.imglib2.type.numeric.integer.IntType;
import net.imglib2.type.numeric.real.DoubleType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.view.Views;

public class TensorstoreTest {

	private String testZarrBaseName = "tensorstore_tests";

	private enum Version {
		zarr, zarr3
	}

	private volatile boolean skipPython = false;

	private HashSet<Path> paths;

	protected String tempN5Location() {

		try {
			final Path tmp = Files.createTempDirectory("tensorstore-test");
			paths.add(tmp);
			return tmp.toUri().getPath();
		} catch (final IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Before
	public void before() {

		paths = new HashSet<Path>();
		if (skipPython) {
			Assume.assumeTrue("Skipping Python compatibility test", false);
		}
	}

	@After
	public void after() {

		for (final Path path : paths) {
			try {
				FileUtils.deleteDirectory(path.toFile());
			} catch (final IOException e) {
				e.printStackTrace();
			}
		}
	}

	private boolean runPythonTest(final String script, final String... args) throws InterruptedException {

		try {

			final List<String> pythonArgs = new ArrayList<>();
			pythonArgs.addAll(Arrays.asList(new String[]{"poetry", "run", "python", "src/test/python/" + script}));
			pythonArgs.addAll(Arrays.asList(args));
			final ProcessBuilder pb = new ProcessBuilder(pythonArgs.toArray(new String[0]));

			pb.redirectOutput(Redirect.INHERIT);
			pb.redirectError(Redirect.INHERIT);
			final Process process = pb.start();
			final boolean timedOut = !process.waitFor(10, TimeUnit.SECONDS);

			if (timedOut)
				System.err.println("The process timed out!");

			final int exitCode = process.exitValue();
			if (exitCode != 0)
				System.err.println("Python process exited with code " + exitCode);

			process.destroy();
			return exitCode == 0;
		} catch (final IOException e) {
			return false;
		}

	}

	private long runPythonChecksum(final String containerPath, final Version version) throws InterruptedException {

		try {
			List<String> pythonArgs = new ArrayList<>();
			String checksumResult = "<no line read>";
			pythonArgs.addAll(Arrays.asList(
					"poetry", "run", "python", "src/test/python/tensorstore_checksum.py", containerPath, version.toString()));
			System.out.println(String.join(" ", pythonArgs));

			final ProcessBuilder pb = new ProcessBuilder(pythonArgs.toArray(new String[0]));

			// pb.redirectOutput(ProcessBuilder.Redirect.INHERIT);
			pb.redirectError(ProcessBuilder.Redirect.INHERIT);
			final Process process = pb.start();

			// Wait for the process to complete or timeout after 10 seconds
			final boolean timedOut = !process.waitFor(10, TimeUnit.SECONDS);

			if (timedOut)
				System.err.println("The Python process timed out!");

			final int exitCode = process.exitValue();

			if (exitCode == 0) {
				System.out.println("Python checksum process completed successfully.");
			} else {
				System.err.println("Python checksum process failed with exit code: " + exitCode);
			}

			InputStreamReader ISReader = new InputStreamReader(process.getInputStream());
			BufferedReader BReader = new BufferedReader(ISReader);

			String line;
			while ((line = BReader.readLine()) != null) {
				System.out.println(line);
				checksumResult = line;
			}

			System.out.println("Python Checksum Result String: " + checksumResult);

//			String[] splits = checksumResult.split(" ");
//			String numStr = splits[splits.length - 1];
//
//			long checksum = Long.parseLong(numStr);
//			System.out.println("Python Checksum Result Long: " + checksum);
//
//			process.destroy();
//			return checksum;

			return -1;

		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException("An error occurred while running the Python checksum process", e);
		}
	}

	private static <T extends IntegerType<T>> void assertIsIntegerSequence(
			final RandomAccessibleInterval<T> source,
			final T ref) {

		ref.setZero();
		for (final T t : Views.flatIterable(source)) {

			if (!t.valueEquals(ref))
				throw new AssertionError("values not equal: expected " + ref + ", actual " + t);
			ref.inc();
		}
	}

	private static <T extends RealType<T>> void assertIsRealSequence(
			final RandomAccessibleInterval<T> source,
			final T ref) {

		ref.setReal(0);
		for (final T t : Views.flatIterable(source)) {

			if (!t.valueEquals(ref))
				throw new AssertionError("values not equal: expected " + ref + ", actual " + t);
			ref.inc();
		}
	}

	@Test
	public void testReadTensorstoreZarr2() throws IOException, InterruptedException {
		testReadTensorstore(Version.zarr);
	}

	@Test
	public void testWriteTensorstoreZarr2() throws IOException, InterruptedException, ExecutionException {

		testWriteTensorstore(Version.zarr);
	}

	@Test
	public void testReadTensorstoreZarr3() throws IOException, InterruptedException {

		testReadTensorstore(Version.zarr3);
	}

	@Test
	public void testWriteTensorstoreZarr3() throws IOException, InterruptedException, ExecutionException {

		testWriteTensorstore(Version.zarr3);
	}

	@Test
	@Ignore
	public void testReadTensorstoreChecksumZarr3() throws IOException, InterruptedException{
		testReadChecksum();
	}

	// ZarrV3 only
	public void testReadChecksum() throws IOException, InterruptedException {

		try ( final ZarrV3KeyValueWriter n5Zarr = new ZarrV3KeyValueWriter(
				new FileSystemKeyValueAccess(), tempN5Location(), new GsonBuilder(), false)) {

			final String testZarrDatasetName = String.join("/", testZarrBaseName, "3");
			n5Zarr.createDataset(
					testZarrDatasetName,
					new long[]{1, 2, 3},
					new int[]{1, 2, 3},
					DataType.UINT16,
					new GzipCompression(4));

			final DatasetAttributes attributes = n5Zarr.getDatasetAttributes(testZarrDatasetName);
			final short[] shortBlock = new short[]{1, 2, 3, 4, 5, 6};
			ByteBuffer byteBuffer = ByteBuffer.allocate(shortBlock.length * 2);
			byteBuffer.order(ByteOrder.nativeOrder());
			byteBuffer.asShortBuffer().put(shortBlock);
			byte[] barray = byteBuffer.array();

			PureJavaCrc32C crc32c = new PureJavaCrc32C();
			crc32c.update(barray, 0, barray.length);

			long javaChecksum = crc32c.getValue();
			if (javaChecksum == -1) {
				System.out.println("Couldn't run Checksum Java test, skipping compatibility test.");
				return;
			}

			final ShortArrayDataBlock dataBlock = new ShortArrayDataBlock(new int[]{1, 2, 3}, new long[]{0, 0, 0}, shortBlock);
			n5Zarr.writeBlock(testZarrDatasetName, attributes, dataBlock);

			// pythonZarrPath
			final String testZarrDirPath =(n5Zarr.getURI().getPath().substring(1) + testZarrBaseName + "/zarr3");
			String testZarrDirPathForPython = testZarrDirPath;
			System.err.println("For Python: " + testZarrDirPathForPython);

			long pythonChecksum = runPythonChecksum(testZarrDirPathForPython, Version.zarr3);
			if (pythonChecksum == -1) {
				System.out.println("Couldn't run Checksum Python test, skipping compatibility test with Python.");
				return;
			}
			System.out.println("\n---------------------------------------");
			System.out.println("Checksum from Python: " + pythonChecksum);
			System.out.println("Checksum from Python Path: " + testZarrDirPath);

			System.out.println("Checksum from Java: " + javaChecksum);
			System.out.println("Checksum from Java Path: "+ n5Zarr.getURI().getPath().substring(1) + "test/tensorstore");

			// TODO assert here instead
			// Compare checksums
			if (pythonChecksum == javaChecksum) {
				System.out.println("Checksums match!");
			} else {
				System.err.println("Checksums do not match!");
			}
		}
	}

	private static String versionFlag(Version version) {
		return "--" + version;
	}

	public void testReadTensorstore(Version version) throws InterruptedException {

		final String testZarrDirPath = tempN5Location();

		String testZarrDirPathForPython;
		if (System.getProperty("os.name").startsWith("Windows"))
			testZarrDirPathForPython = testZarrDirPath.substring(1);
		else
			testZarrDirPathForPython = testZarrDirPath;

		/* create test data with python */
		skipPython = !runPythonTest("tensorstore_test.py", testZarrDirPathForPython, versionFlag(version));
		if (skipPython) {
			Assume.assumeFalse("Couldn't run Python test, skipping compatibility test with Python.", skipPython);
		}

		N5Writer n5Zarr;
		switch (version) {
		case zarr3:
			n5Zarr = new ZarrV3KeyValueWriter(
					new FileSystemKeyValueAccess(), testZarrDirPath, new GsonBuilder(), false);
			break;
		default:
			n5Zarr = new ZarrKeyValueWriter(new FileSystemKeyValueAccess(),
					testZarrDirPath, new GsonBuilder(), true, true, "/", false);
			break;
		}

		/* groups */
		final String testZarrDatasetName = String.join("/", testZarrBaseName, version.toString());

		// assertTrue(n5Zarr.exists(testZarrDatasetName)); // For this to be true, the tensorstore script needs to explicitly create a group
		assertFalse(n5Zarr.datasetExists(testZarrDatasetName));

		/* array parameters */
		final String dset32Ci8 = testZarrDatasetName + "/3x2_c_i8";
		assertTrue(n5Zarr.datasetExists(dset32Ci8));

		final DatasetAttributes datasetAttributesC = n5Zarr.getDatasetAttributes(testZarrDatasetName + "/3x2_c_i8");
		assertArrayEquals(datasetAttributesC.getDimensions(), new long[]{3, 2});
		assertArrayEquals(datasetAttributesC.getBlockSize(), new int[]{3, 2});
		assertEquals(DataType.INT64, datasetAttributesC.getDataType());


		// TODO test in zarr 3 after we implement this
		/* N5 array parameter mapping */
		if (version == Version.zarr) {
			assertArrayEquals(
					n5Zarr.getAttribute(testZarrDatasetName + "/3x2_c_i8", "dimensions", long[].class),
					new long[]{3, 2});
			assertArrayEquals(
					n5Zarr.getAttribute(testZarrDatasetName + "/3x2_c_i8", "blockSize", int[].class),
					new int[]{3, 2});
			assertEquals(
					n5Zarr.getAttribute(testZarrDatasetName + "/3x2_c_i8", "dataType", DataType.class),
					DataType.INT64);
		}

		/* LE uint8 */
		final UnsignedByteType refUnsignedByte = new UnsignedByteType();
		assertIsIntegerSequence(N5Utils.open(n5Zarr, testZarrDatasetName + "/3x2_c_u1"), refUnsignedByte);

		/* LE int64  */
		final LongType refLong = new LongType();
		assertIsIntegerSequence(N5Utils.open(n5Zarr, testZarrDatasetName + "/3x2_c_i8"), refLong);
		assertIsIntegerSequence(N5Utils.open(n5Zarr, testZarrDatasetName + "/30x20_c_i8"), refLong);

		/* BE int32 */
		final UnsignedIntType refUnsignedInt = new UnsignedIntType();
		assertIsIntegerSequence(N5Utils.open(n5Zarr, testZarrDatasetName + "/3x2_c_u4"), refUnsignedInt);
		assertIsIntegerSequence(N5Utils.open(n5Zarr, testZarrDatasetName + "/30x20_c_u4"), refUnsignedInt);

		/* LE float64 */
		final DoubleType refDouble = new DoubleType();
		assertIsRealSequence(N5Utils.open(n5Zarr, testZarrDatasetName + "/3x2_c_f8"), refDouble);
		assertIsRealSequence(N5Utils.open(n5Zarr, testZarrDatasetName + "/30x20_c_f8"), refDouble);

		/* BE float32 */
		final FloatType refFloat = new FloatType();
		assertIsRealSequence(N5Utils.open(n5Zarr, testZarrDatasetName + "/3x2_c_f4"), refFloat);
		assertIsRealSequence(N5Utils.open(n5Zarr, testZarrDatasetName + "/30x20_c_f4"), refFloat);

		/* F order */
		if( version == Version.zarr ) {
			final DatasetAttributes datasetAttributesF = n5Zarr.getDatasetAttributes(testZarrDatasetName + "/3x2_f_i8");
			assertArrayEquals(datasetAttributesF.getDimensions(), new long[]{2, 3});
			assertArrayEquals(datasetAttributesF.getBlockSize(), new int[]{2, 3});
			assertEquals(DataType.INT64, datasetAttributesF.getDataType());

			final CachedCellImg<LongType, ?> img = N5Utils.open(n5Zarr, testZarrDatasetName + "/3x2_f_i8");
			assertIsIntegerSequence(Views.permute(img, 1, 0), refLong);
		}

		if (version == Version.zarr3) {
			// sharded data
			testRead((ZarrV3KeyValueReader) n5Zarr, testZarrDatasetName + "/3x2_c_u1_sharded", new UnsignedByteType());
			testRead((ZarrV3KeyValueReader) n5Zarr, testZarrDatasetName + "/30x20_c_u1_sharded", new UnsignedByteType());

			testRead((ZarrV3KeyValueReader) n5Zarr, testZarrDatasetName + "/3x2_c_u4_sharded", new UnsignedIntType());
			testRead((ZarrV3KeyValueReader) n5Zarr, testZarrDatasetName + "/30x20_c_u4_sharded", new UnsignedIntType());

			testRead((ZarrV3KeyValueReader) n5Zarr, testZarrDatasetName + "/3x2_c_i4_sharded", new IntType());
			testRead((ZarrV3KeyValueReader) n5Zarr, testZarrDatasetName + "/30x20_c_i4_sharded", new IntType());

			testRead((ZarrV3KeyValueReader) n5Zarr, testZarrDatasetName + "/30x20_c_i8_sharded", new LongType());
			testRead((ZarrV3KeyValueReader) n5Zarr, testZarrDatasetName + "/3x2_c_i8_sharded", new LongType());

			// transposed data
			testRead((ZarrV3KeyValueReader) n5Zarr, testZarrDatasetName + "/3x2_c_u1_transpose", new UnsignedByteType());
			testRead((ZarrV3KeyValueReader) n5Zarr, testZarrDatasetName + "/3x2_c_u4_transpose", new UnsignedIntType());
			testRead((ZarrV3KeyValueReader) n5Zarr, testZarrDatasetName + "/3x2_c_i8_transpose", new LongType());
			testRead((ZarrV3KeyValueReader) n5Zarr, testZarrDatasetName + "/3x2_c_f4_transpose", new FloatType());
			testRead((ZarrV3KeyValueReader) n5Zarr, testZarrDatasetName + "/3x2_c_f8_transpose", new DoubleType());

			// transposed 3d
			testRead((ZarrV3KeyValueReader) n5Zarr, testZarrDatasetName + "/4x3x2_c_u1_transpose_0-1-2", new UnsignedByteType());
			testRead((ZarrV3KeyValueReader) n5Zarr, testZarrDatasetName + "/4x3x2_c_u1_transpose_0-2-1", new UnsignedByteType());
			testRead((ZarrV3KeyValueReader) n5Zarr, testZarrDatasetName + "/4x3x2_c_u1_transpose_1-0-2", new UnsignedByteType());
			testRead((ZarrV3KeyValueReader) n5Zarr, testZarrDatasetName + "/4x3x2_c_u1_transpose_1-2-0", new UnsignedByteType());
			testRead((ZarrV3KeyValueReader) n5Zarr, testZarrDatasetName + "/4x3x2_c_u1_transpose_2-0-1", new UnsignedByteType());
			testRead((ZarrV3KeyValueReader) n5Zarr, testZarrDatasetName + "/4x3x2_c_u1_transpose_2-1-0", new UnsignedByteType());

			// shard bigger than image
			testRead((ZarrV3KeyValueReader) n5Zarr, testZarrDatasetName + "/30x20_huge-shard", new LongType());
		}

		final UnsignedLongType refUnsignedLong = new UnsignedLongType();
		assertIsIntegerSequence(N5Utils.open(n5Zarr, testZarrDatasetName + "/30x20_c_u8_zlib"), refUnsignedLong);
		assertIsIntegerSequence(N5Utils.open(n5Zarr, testZarrDatasetName + "/30x20_c_u8_gzip"), refUnsignedLong);

		// bz2 not in zarr3 (as of Dec 2025)
		if (version == Version.zarr)
			assertIsIntegerSequence(N5Utils.open(n5Zarr, testZarrDatasetName + "/30x20_c_u8_bz2"), refUnsignedLong);

		/* fill value 1 */
		String datasetName = testZarrDatasetName + "/3x2_c_u4_f1";
		final RandomAccessibleInterval<UnsignedIntType> a3x2_c_u4_f1 = N5Utils.open(n5Zarr, datasetName);
		assertIsIntegerSequence(a3x2_c_u4_f1, refUnsignedInt);

		DatasetAttributes attributes = n5Zarr.getDatasetAttributes(datasetName);
		final long[] shape = attributes.getDimensions();
		Arrays.setAll(shape, i -> shape[i] + 5);
		n5Zarr.setAttribute(datasetName, "dimensions", shape);

		final RandomAccessibleInterval<UnsignedIntType> a3x2_c_bu4_f1_after = N5Utils.open(n5Zarr, datasetName);
		assertIsIntegerSequence(Views.interval(a3x2_c_bu4_f1_after, a3x2_c_u4_f1), refUnsignedInt);

		/* fill value NaN */
		datasetName = testZarrDatasetName + "/3x2_c_f4_fnan";

		final RandomAccessibleInterval<FloatType> a3x2_c_lf4_fnan = N5Utils.open(n5Zarr, datasetName);
		assertIsRealSequence(a3x2_c_lf4_fnan, refFloat);

		/* Manually increasing the image dimensions results in empty blocks containing fill value */
		attributes = n5Zarr.getDatasetAttributes(datasetName);
		final long[] shapef = attributes.getDimensions();
		Arrays.setAll(shapef, i -> shapef[i] + 5);
		n5Zarr.setAttribute(datasetName, "dimensions", shapef);

		if (version == Version.zarr) {
			// test that non-existing blocks are filled with fill value (NaN) 
			final RandomAccessibleInterval<FloatType> a3x2_c_lf4_fnan_after = N5Utils.open(n5Zarr, datasetName);
			assertIsRealSequence(Views.interval(a3x2_c_lf4_fnan_after, a3x2_c_lf4_fnan), refFloat);
			final net.imglib2.RandomAccess<FloatType> raf = a3x2_c_lf4_fnan_after.randomAccess();
			raf.setPosition(shapef[0] - 5, 0);
			assertTrue(Float.isNaN(raf.get().getRealFloat()));
			raf.setPosition(shapef[1] - 5, 1);
			assertTrue(Float.isNaN(raf.get().getRealFloat()));
		}

	}
	
	public <T extends NativeType<T> & NumericType<T>> void testWriteTensorstore(Version version) throws IOException, InterruptedException, ExecutionException {
		
		/* 
		 * run the python script in a test mode and return early if we can't run it.
		 */
		skipPython = !runPythonTest("tensorstore_read_test.py", "--test" );
		if (skipPython) {
			Assume.assumeFalse("Couldn't run Python test, skipping compatibility test with Python.", skipPython);
		}

		final String testZarrDirPath = tempN5Location();
		//TODO: decided what to do with it for windows
		String testZarrDirPathForPython;

		if (System.getProperty("os.name").startsWith("Windows"))
			testZarrDirPathForPython = testZarrDirPath.substring(1);
		else
			testZarrDirPathForPython = testZarrDirPath;

		N5Writer n5Zarr;
		switch (version) {
		case zarr3:
			n5Zarr = new ZarrV3KeyValueWriter(
					new FileSystemKeyValueAccess(), testZarrDirPath, new GsonBuilder(), false);
			break;
		default:
			n5Zarr = new ZarrKeyValueWriter(new FileSystemKeyValueAccess(),
					testZarrDirPath, new GsonBuilder(), true, true, "/", false);
			break;
		}

		final DataType[] dtypes =  new DataType[]{ 
				DataType.INT8, DataType.UINT8,
				DataType.INT16, DataType.UINT16,
				DataType.INT32, DataType.UINT32,
				DataType.INT64, DataType.UINT64,
				DataType.FLOAT32, DataType.FLOAT64 };

		for( DataType dtype : dtypes ) {

			@SuppressWarnings("unchecked")
			final Img<T> img = generateData(new long[] { 3, 2 }, (T)N5Utils.type(dtype));

			@SuppressWarnings("unchecked")
			final Img<T> imgBig = generateData(new long[] { 12, 9 }, (T)N5Utils.type(dtype));

			// unsharded
			String dset = String.format("3x2_%s", dtype.toString());
			N5Utils.save(img, n5Zarr, dset, new int[] { 3, 2 }, new RawCompression());
			assertTrue( runPythonTest("tensorstore_read_test.py", "-p", testZarrDirPathForPython + dset, "-d", version.toString()));

			// unsharded big
			String dsetBig = String.format("12x9%s", dtype.toString());
			N5Utils.save(imgBig, n5Zarr, dsetBig, new int[] { 2, 3 }, new RawCompression());
			assertTrue( runPythonTest("tensorstore_read_test.py", "-p", testZarrDirPathForPython + dsetBig, "-d", version.toString()));

			// TODO
//			if( version == Version.zarr3 ) {
//
//				// sharded
//				String dsetSharded = String.format("3x2_%s_shard", dtype.toString());
//				ZarrV3DatasetAttributes smallAttrs = buildAttributes(
//						img.dimensionsAsLongArray(), img.getType(), new int[] {3, 2}, new int[] {1, 2}, new RawCompression());
//				N5Utils.save(img, n5Zarr, dsetSharded, smallAttrs);
//				assertTrue( runPythonTest("tensorstore_read_test.py", "-p", testZarrDirPath + dsetSharded, "-d",version.toString()));
//
//				// sharded big
//				String dsetBigSharded = String.format("12x9%s_shard", dtype.toString());
//				ZarrV3DatasetAttributes bigAttrs = buildAttributes(
//						imgBig.dimensionsAsLongArray(), img.getType(), new int[] {6, 9}, new int[] {2, 3}, new RawCompression());
//				N5Utils.save(imgBig, n5Zarr, dsetBigSharded, bigAttrs);
//				assertTrue( runPythonTest("tensorstore_read_test.py", "-p", testZarrDirPath + dsetBigSharded, "-d",version.toString()));
//			}
		}
	}

	private <T extends NativeType<T>> ZarrV3DatasetAttributes buildAttributes(long[] dimensions, T type, int[] shardSize, int[] blockSize,
			Compression compression) {

		return new ZarrV3DatasetAttributes(dimensions, shardSize, blockSize, N5Utils.dataType(type), compression);
	}

	protected <T extends NativeType<T> & NumericType<T>> Img<T> generateData( final long[] size, T type ) {

		final ArrayImg<T, ?> img = new ArrayImgFactory<T>(type).create(size);
		T val = type.copy();
		val.setZero();

		T one = type.copy();
		one.setOne();

		final ArrayCursor<T> c = img.cursor();
		while( c.hasNext()) {
			c.next().set(val);
			val.add(one);
		}

		return img;
	}

	public <T extends NumericType<T>> void testRead(final ZarrV3KeyValueReader zarr, final String dataset, T ref) {

		assertTrue("dataset " + dataset + " does not exist", zarr.exists(dataset));
		if (ref instanceof IntegerType) {
			assertIsIntegerSequence((RandomAccessibleInterval) N5Utils.open(zarr, dataset), (IntegerType) ref);
		}
		else if (ref instanceof RealType) {
			assertIsRealSequence((RandomAccessibleInterval) N5Utils.open(zarr, dataset), (RealType) ref);
		}
		else
			System.err.println("Skipping test for: " + dataset + ".  Invalide ref type.");
	}

}
