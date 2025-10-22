package org.janelia.saalfeldlab.n5.zarr;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.lang.ProcessBuilder.Redirect;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.FileSystemKeyValueAccess;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.RawCompression;
import org.janelia.saalfeldlab.n5.codec.DataCodecInfo;
import org.janelia.saalfeldlab.n5.codec.RawBlockCodecInfo;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.janelia.saalfeldlab.n5.shard.DefaultShardCodecInfo;
import org.janelia.saalfeldlab.n5.shard.ShardIndex.IndexLocation;
import org.janelia.saalfeldlab.n5.zarr.codec.PaddedRawBlockCodecInfo;
import org.janelia.saalfeldlab.n5.zarr.v3.ZarrV3DatasetAttributes;
import org.janelia.saalfeldlab.n5.zarr.v3.ZarrV3KeyValueReader;
import org.janelia.saalfeldlab.n5.zarr.v3.ZarrV3KeyValueWriter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.gson.GsonBuilder;

import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.Img;
import net.imglib2.img.array.ArrayCursor;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgFactory;
import net.imglib2.type.NativeType;
import net.imglib2.type.Type;
import net.imglib2.type.numeric.IntegerType;
import net.imglib2.type.numeric.NumericType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.integer.LongType;
import net.imglib2.type.numeric.integer.UnsignedByteType;
import net.imglib2.type.numeric.integer.UnsignedIntType;
import net.imglib2.type.numeric.integer.IntType;
import net.imglib2.type.numeric.real.DoubleType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.view.Views;

public class TensorstoreTest {

	private String testZarrBaseName = "tensorstore_tests";

	private static enum Version {
		zarr, zarr3
	};

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

			System.out.println(String.join(" ", pythonArgs));
			final ProcessBuilder pb = new ProcessBuilder(pythonArgs.toArray(new String[0]));

			pb.redirectOutput(Redirect.INHERIT);
			pb.redirectError(Redirect.INHERIT);
			final Process process = pb.start();
			final boolean timedOut = !process.waitFor(10, TimeUnit.SECONDS);

			if (timedOut)
				System.err.println("The process timed out!");

			final int exitCode = process.exitValue();

			if (exitCode == 0)
				System.out.println("Python process exited succcessfully!");
			else
				System.err.println("Python process exited with code " + exitCode);

			process.destroy();
			return exitCode == 0;
		} catch (final IOException e) {
			return false;
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

	private static String versionFlag(Version version) {

		return "--" + version;
	}

	public void testReadTensorstore(Version version) throws IOException, InterruptedException {

		final String testZarrDirPath = tempN5Location();
		//TODO: decided what to do with it for windows
		String testZarrDirPathForPython;

		if (System.getProperty("os.name").startsWith("Windows"))
			testZarrDirPathForPython = testZarrDirPath.substring(1);
		else
			testZarrDirPathForPython = testZarrDirPath;

		/* create test data with python */
		if (!runPythonTest("tensorstore_test.py", testZarrDirPathForPython, versionFlag(version))) {
			System.out.println("Couldn't run Python test, skipping compatibility test with Python.");
			return;
		}

		N5Writer n5Zarr;
		switch (version) {
		case zarr3:
			n5Zarr = new ZarrV3KeyValueWriter(
					new FileSystemKeyValueAccess(FileSystems.getDefault()), testZarrDirPath, new GsonBuilder(),
					true, true, "/", false);
			break;
		default:
			n5Zarr = new ZarrKeyValueWriter(new FileSystemKeyValueAccess(FileSystems.getDefault()),
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


		// TODO test after we implement this
		/* N5 array parameter mapping */
		// assertArrayEquals(
		// n5Zarr.getAttribute(testZarrDatasetName + "/3x2_c_i8", "dimensions", long[].class),
		// new long[]{3, 2});
		// assertArrayEquals(
		// n5Zarr.getAttribute(testZarrDatasetName + "/3x2_c_i8", "blockSize", int[].class),
		// new int[]{3, 2});
		// assertEquals(
		// n5Zarr.getAttribute(testZarrDatasetName + "/3x2_c_i8", "dataType", DataType.class),
		// DataType.INT64);

		final UnsignedByteType refUnsignedByte = new UnsignedByteType();
		assertIsIntegerSequence(N5Utils.open(n5Zarr, testZarrDatasetName + "/3x2_c_u1"), refUnsignedByte);

		/* int64 in C order */
		final LongType refLong = new LongType();
		assertIsIntegerSequence(N5Utils.open(n5Zarr, testZarrDatasetName + "/3x2_c_i8"), refLong);
		assertIsIntegerSequence(N5Utils.open(n5Zarr, testZarrDatasetName + "/30x20_c_i8"), refLong);

		/* int32 in C order */
		final UnsignedIntType refUnsignedInt = new UnsignedIntType();
		assertIsIntegerSequence(N5Utils.open(n5Zarr, testZarrDatasetName + "/3x2_c_u4"), refUnsignedInt);
		assertIsIntegerSequence(N5Utils.open(n5Zarr, testZarrDatasetName + "/30x20_c_u4"), refUnsignedInt);

		/* float64 in C order */
		final DoubleType refDouble = new DoubleType();
		assertIsRealSequence(N5Utils.open(n5Zarr, testZarrDatasetName + "/3x2_c_f8"), refDouble);
		assertIsRealSequence(N5Utils.open(n5Zarr, testZarrDatasetName + "/30x20_c_f8"), refDouble);

		/* float32 in C order */
		final FloatType refFloat = new FloatType();
		assertIsRealSequence(N5Utils.open(n5Zarr, testZarrDatasetName + "/3x2_c_f4"), refFloat);
		assertIsRealSequence(N5Utils.open(n5Zarr, testZarrDatasetName + "/30x20_c_f4"), refFloat);
		
		/* sharded data */
		if (version == Version.zarr3) {
			testReadSharded((ZarrV3KeyValueReader) n5Zarr, testZarrDatasetName + "/3x2_c_u1_sharded", new UnsignedByteType());
			testReadSharded((ZarrV3KeyValueReader) n5Zarr, testZarrDatasetName + "/30x20_c_u1_sharded", new UnsignedByteType());

			testReadSharded((ZarrV3KeyValueReader) n5Zarr, testZarrDatasetName + "/3x2_c_u4_sharded", new UnsignedIntType());
			testReadSharded((ZarrV3KeyValueReader) n5Zarr, testZarrDatasetName + "/30x20_c_u4_sharded", new UnsignedIntType());

			testReadSharded((ZarrV3KeyValueReader) n5Zarr, testZarrDatasetName + "/3x2_c_i4_sharded", new IntType());
			testReadSharded((ZarrV3KeyValueReader) n5Zarr, testZarrDatasetName + "/30x20_c_i4_sharded", new IntType());

			testReadSharded((ZarrV3KeyValueReader) n5Zarr, testZarrDatasetName + "/30x20_c_i8_sharded", new LongType());
			testReadSharded((ZarrV3KeyValueReader) n5Zarr, testZarrDatasetName + "/3x2_c_i8_sharded", new LongType());
		}

		// /* compressors */
		// final UnsignedLongType refUnsignedLong = new UnsignedLongType();
		//
		// assertIsSequence(N5Utils.open(n5Zarr, testZarrDatasetName + "/30x20_c_u8_zlib"), refUnsignedLong);
		// assertIsSequence(N5Utils.open(n5Zarr, testZarrDatasetName + "/30x20_c_u8_gzip"), refUnsignedLong);
		// assertIsSequence(N5Utils.open(n5Zarr, testZarrDatasetName + "/30x20_c_u8_bz2"), refUnsignedLong);
		//
		// /* fill value 1 */
		// String datasetName = testZarrDatasetName + "/3x2_c_u4_f1";
		//
		// final RandomAccessibleInterval<UnsignedIntType> a3x2_c_bu4_f1 = N5Utils.open(n5Zarr, datasetName);
		// assertIsSequence(a3x2_c_bu4_f1, refUnsignedInt);
		//
		// DatasetAttributes attributes = n5Zarr.getDatasetAttributes(datasetName);
		// final long[] shape = attributes.getDimensions();
		// Arrays.setAll(shape, i -> shape[i] + 5);
		// n5Zarr.setAttribute(datasetName, "dimensions", shape);
		//
		// final RandomAccessibleInterval<UnsignedIntType> a3x2_c_bu4_f1_after = N5Utils.open(n5Zarr, datasetName);
		// assertIsSequence(Views.interval(a3x2_c_bu4_f1_after, a3x2_c_bu4_f1), refUnsignedInt);
		// final RandomAccess<UnsignedIntType> ra = a3x2_c_bu4_f1_after.randomAccess();
		//
		// /* fill value NaN */
		// datasetName = testZarrDatasetName + "/3x2_c_f4_fnan";
		//
		// final RandomAccessibleInterval<FloatType> a3x2_c_lf4_fnan = N5Utils.open(n5Zarr, datasetName);
		// assertIsSequence(a3x2_c_lf4_fnan, refFloat);
		//
		// attributes = n5Zarr.getDatasetAttributes(datasetName);
		// final long[] shapef = attributes.getDimensions();
		// Arrays.setAll(shapef, i -> shapef[i] + 5);
		// n5Zarr.setAttribute(datasetName, "dimensions", shapef);
		//
		// final RandomAccessibleInterval<FloatType> a3x2_c_lf4_fnan_after = N5Utils.open(n5Zarr, datasetName);
		// assertIsSequence(Views.interval(a3x2_c_lf4_fnan_after, a3x2_c_lf4_fnan), refFloat);
		// final RandomAccess<FloatType> raf = a3x2_c_lf4_fnan_after.randomAccess();
		// raf.setPosition(shapef[0] - 5, 0);
		// assertTrue(Float.isNaN(raf.get().getRealFloat()));
		// raf.setPosition(shapef[1] - 5, 1);
		// assertTrue(Float.isNaN(raf.get().getRealFloat()));

	}
	
	public <T extends NativeType<T> & NumericType<T>> void testWriteTensorstore(Version version) throws IOException, InterruptedException, ExecutionException {
		
		/* 
		 * run the python script in a test mode and return early if we can't run it.
		 */
		if (!runPythonTest("tensorstore_read_test.py", "--test" )) {
			System.out.println("Couldn't run Python test, skipping compatibility test with Python.");
			return;
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
					new FileSystemKeyValueAccess(FileSystems.getDefault()), testZarrDirPath, new GsonBuilder(),
					true, true, "/", false);
			break;
		default:
			n5Zarr = new ZarrKeyValueWriter(new FileSystemKeyValueAccess(FileSystems.getDefault()),
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
			assertTrue( runPythonTest("tensorstore_read_test.py", "-p", testZarrDirPath + dset, "-d", version.toString()));

			// unsharded big
			String dsetBig = String.format("12x9%s", dtype.toString());
			N5Utils.save(imgBig, n5Zarr, dsetBig, new int[] { 2, 3 }, new RawCompression());
			assertTrue( runPythonTest("tensorstore_read_test.py", "-p", testZarrDirPath + dsetBig, "-d", version.toString()));

			if( version == Version.zarr3 ) {

				// sharded
				String dsetSharded = String.format("3x2_%s_shard", dtype.toString());
				ZarrV3DatasetAttributes smallAttrs = buildAttributes(
						img.dimensionsAsLongArray(), img.getType(), new int[] {3, 2}, new int[] {1, 2}, new RawCompression());
				N5Utils.save(img, n5Zarr, dsetSharded, smallAttrs);
				assertTrue( runPythonTest("tensorstore_read_test.py", "-p", testZarrDirPath + dsetSharded, "-d",version.toString()));

				// sharded big
				String dsetBigSharded = String.format("12x9%s_shard", dtype.toString());
				ZarrV3DatasetAttributes bigAttrs = buildAttributes(
						imgBig.dimensionsAsLongArray(), img.getType(), new int[] {6, 9}, new int[] {2, 3}, new RawCompression());
				N5Utils.save(imgBig, n5Zarr, dsetBigSharded, bigAttrs);
				assertTrue( runPythonTest("tensorstore_read_test.py", "-p", testZarrDirPath + dsetBigSharded, "-d",version.toString()));
			}
		}
		System.out.println("done");

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

	public <T extends NumericType<T>> void testReadSharded(final ZarrV3KeyValueReader zarr, final String dataset, T ref) {

		System.out.println("testSharded for : " + zarr.getURI() + "  " + dataset);
		if( zarr.exists(dataset)) {
			System.out.println("  exists");
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

}
