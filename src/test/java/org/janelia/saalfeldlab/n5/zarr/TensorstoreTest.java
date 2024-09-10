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
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.FileSystemKeyValueAccess;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.janelia.saalfeldlab.n5.zarr.v3.ZarrV3KeyValueWriter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.gson.GsonBuilder;

import net.imglib2.RandomAccessibleInterval;
import net.imglib2.type.numeric.IntegerType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.integer.LongType;
import net.imglib2.type.numeric.integer.UnsignedByteType;
import net.imglib2.type.numeric.integer.UnsignedIntType;
import net.imglib2.type.numeric.real.DoubleType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.view.Views;

public class TensorstoreTest {

	private String testZarrDatasetName = "/tensorstore_tests/zarr3";

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

	private boolean runPythonTest(final String script, final String containerPath, final String... args) throws InterruptedException {

		try {

			final List<String> pythonArgs = new ArrayList<>();
			pythonArgs.addAll(Arrays.asList(new String[]{"poetry", "run", "python", "src/test/python/" + script, containerPath}));
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
	public void testReadTensorstoreZarr3() throws IOException, InterruptedException{
		testReadTensorstore("--zarr3");
	}
	
	@Test
	public void testReadTensorstoreZarr2() throws IOException, InterruptedException{
		testReadTensorstore("--zarr2");
	}
	
	@Test
	public void testReadTensorstoreN5() throws IOException, InterruptedException{
		testReadTensorstore("--n5");
	}
	
	public void testReadTensorstore(String format) throws IOException, InterruptedException {

		final String testZarrDirPath = tempN5Location();
		//TODO: decided what to do with it for windows
		String testZarrDirPathForPython;

		if (System.getProperty("os.name").startsWith("Windows"))
			testZarrDirPathForPython = testZarrDirPath.substring(1);
		else
			testZarrDirPathForPython = testZarrDirPath;

		/* create test data with python */
		if (!runPythonTest("tensorstore_test.py", testZarrDirPathForPython, format)) {
			System.out.println("Couldn't run Python test, skipping compatibility test with Python.");
			return;
		}

		final ZarrV3KeyValueWriter n5Zarr = new ZarrV3KeyValueWriter(
				new FileSystemKeyValueAccess(FileSystems.getDefault()), testZarrDirPath, new GsonBuilder(),
				true, true, "/", false);

		/* groups */
		assertTrue(n5Zarr.exists(testZarrDatasetName));
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
		assertIsSequence(N5Utils.open(n5Zarr, testZarrDatasetName + "/3x2_c_u1"), refUnsignedByte);

		/* int64 in C order */
		final LongType refLong = new LongType();
		assertIsSequence(N5Utils.open(n5Zarr, testZarrDatasetName + "/3x2_c_i8"), refLong);
		assertIsSequence(N5Utils.open(n5Zarr, testZarrDatasetName + "/30x20_c_i8"), refLong);

		/* int32 in C order */
		final UnsignedIntType refUnsignedInt = new UnsignedIntType();
		assertIsSequence(N5Utils.open(n5Zarr, testZarrDatasetName + "/3x2_c_u4"), refUnsignedInt);
		assertIsSequence(N5Utils.open(n5Zarr, testZarrDatasetName + "/30x20_c_u4"), refUnsignedInt);

		/* float64 in C order */
		final DoubleType refDouble = new DoubleType();
		assertIsSequence(N5Utils.open(n5Zarr, testZarrDatasetName + "/3x2_c_f8"), refDouble);
		assertIsSequence(N5Utils.open(n5Zarr, testZarrDatasetName + "/30x20_c_f8"), refDouble);

		/* float32 in C order */
		final FloatType refFloat = new FloatType();
		assertIsSequence(N5Utils.open(n5Zarr, testZarrDatasetName + "/3x2_c_f4"), refFloat);
		assertIsSequence(N5Utils.open(n5Zarr, testZarrDatasetName + "/30x20_c_f4"), refFloat);


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

}
