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
import java.nio.ShortBuffer;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;
//import java.util.zip.CRC32;

import org.apache.commons.codec.digest.PureJavaCrc32C;
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.FileSystemKeyValueAccess;
import org.janelia.saalfeldlab.n5.GzipCompression;
import org.janelia.saalfeldlab.n5.N5Reader;

import org.apache.commons.io.FileUtils;

import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.RawCompression;
import org.janelia.saalfeldlab.n5.ShortArrayDataBlock;
import org.janelia.saalfeldlab.n5.blosc.BloscCompression;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.janelia.saalfeldlab.n5.zarr.v3.ZarrV3KeyValueWriter;
import org.janelia.scicomp.n5.zstandard.ZstandardCompression;
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
import net.imglib2.type.numeric.integer.UnsignedLongType;
import net.imglib2.type.numeric.real.DoubleType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.view.RandomAccessiblePair.RandomAccess;
import net.imglib2.view.Views;

public class TensorstoreTest {

	private String testZarrBaseName = "tensorstore_tests";

	private static enum Version {
		zarr2, zarr3, n5
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

  public long runPythonChecksum(final String containerPath, final Version version) throws InterruptedException {
	    try {
	        List<String> pythonArgs = new ArrayList<>();
	        String checksumResult = "<no line read>";
	        pythonArgs.addAll(Arrays.asList(
	            "poetry", "run", "python", "src/test/python/tensorstore_checksum.py", containerPath, version.toString()
	        ));
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
	        while((line=BReader.readLine()) !=null) {
		        System.out.println(line);
		        checksumResult = line;
	        }
	        
	        System.out.println("Python Checksum Result String: " + checksumResult);
	        
	        String[] splits = checksumResult.split(" ");
	        String numStr = splits[splits.length-1]; 
	        
	        long checksum = Long.parseLong(numStr);
	        System.out.println("Python Checksum Result Long: " + checksum);
	       
	        process.destroy();
	        return checksum;

	    } catch (IOException e) {
	        e.printStackTrace();
	        throw new RuntimeException("An error occurred while running the Python checksum process", e);
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

	@Test

	public void testReadTensorstoreZarr3() throws Exception {
		testReadTensorstore(Version.zarr3);
  }

  public void testReadTensorstoreZarr2() throws IOException, InterruptedException {
		testReadTensorstore(Version.zarr2);
	}

	@Test
	public void testReadTensorstoreN5() throws Exception {
		testReadTensorstore(Version.n5);
	}
	
	@Test
	public void testReadTensorstoreChecksumZarr2() throws IOException, InterruptedException{
		testReadChecksum(Version.zarr2);
	}

	public void testReadChecksum(Version version) throws IOException, InterruptedException {
		
		ZarrKeyValueWriter n5Zarr = new ZarrKeyValueWriter(new FileSystemKeyValueAccess(FileSystems.getDefault()), tempN5Location(), new GsonBuilder(), false, false, "/", false);
		
		final String testZarrDatasetName = String.join("/", testZarrBaseName, version.toString());

		n5Zarr.createDataset(
				testZarrDatasetName,
				new long[]{1, 2, 3},
				new int[]{1, 2, 3},
				DataType.UINT16,
				new GzipCompression(4) //new GzipCompression()
				//ZarrCompressor.fromCompression(new GzipCompression(5))
				//ZarrCompressor.fromCompression(new BloscCompression("blosc", BloscCompression.BITSHUFFLE,5,0, 1))
			    );

		
		final DatasetAttributes attributes = n5Zarr.getDatasetAttributes(testZarrDatasetName);
        final short[] shortBlock = new short[]{1, 2, 3, 4, 5, 6};
        ByteBuffer byteBuffer = ByteBuffer.allocate(shortBlock.length * 2);
        byteBuffer.order(ByteOrder.nativeOrder());
        byteBuffer.asShortBuffer().put(shortBlock);
        byte[] barray = byteBuffer.array();
        
        PureJavaCrc32C crc32c = new PureJavaCrc32C();
        crc32c.update(barray, 0, barray.length);
        
        /*
        for(byte b: barray) {
        	System.out.println("byte: " + b);
        }*/
        
        long javaChecksum = crc32c.getValue();
        if (javaChecksum == -1) {
	        System.out.println("Couldn't run Checksum Java test, skipping compatibility test.");
	        return;
	    }
        //System.out.println("Checksum from Java: " + javaChecksum);
        
		//final ShortBuffer sBuffer = ShortBuffer.wrap(shortBlock);
		final ShortArrayDataBlock dataBlock = new ShortArrayDataBlock(new int[]{1, 2, 3}, new long[]{0, 0, 0}, shortBlock);
		n5Zarr.writeBlock(testZarrDatasetName, attributes, dataBlock);
		//System.out.println("Checksum from Java Path: "+ n5Zarr.getURI().getPath() + "\\test\\tensorstore");

		// pythonZarrPath
		//final String testZarrDirPath = "C:\\Users\\chend\\AppData\\Local\\Temp\\zarr3-tensorstore-test_python_o0dnjj3f.zarr\\tensorstore_tests\\zarr2\\3x2_f_u4";
		final String testZarrDirPath =(n5Zarr.getURI().getPath().substring(1) + testZarrBaseName + "/" + version);
		
		
		//TODO: decided what to do with it for windows
		String testZarrDirPathForPython = testZarrDirPath;
		
		/*
		 * if (System.getProperty("os.name").startsWith("Windows"))
		 * testZarrDirPathForPython = testZarrDirPath.substring(1); else
		 * testZarrDirPathForPython = testZarrDirPath;
		 */
		
		System.err.println("For Python: " + testZarrDirPathForPython);
		
		long pythonChecksum = runPythonChecksum(testZarrDirPathForPython, version);
		if (pythonChecksum == -1) {
	        System.out.println("Couldn't run Checksum Python test, skipping compatibility test with Python.");
	        return;
	    }
		System.out.println("\n---------------------------------------");
	    System.out.println("Checksum from Python: " + pythonChecksum);
	    System.out.println("Checksum from Python Path: " + testZarrDirPath);
	    
	    System.out.println("Checksum from Java: " + javaChecksum);
	    System.out.println("Checksum from Java Path: "+ n5Zarr.getURI().getPath().substring(1) + "test/tensorstore");
	    
	    // Compare checksums
        if (pythonChecksum == javaChecksum) {
            System.out.println("Checksums match!");
        } else {
            System.err.println("Checksums do not match!");
        }
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
		final String testZarrGroupName = testZarrDatasetName;
		assertTrue(n5Zarr.exists(testZarrDatasetName));
		assertFalse(n5Zarr.datasetExists(testZarrDatasetName));

		/* array parameters */
		final String dset32Ci8 = testZarrDatasetName + "/3x2_c_i8";
		assertTrue(n5Zarr.datasetExists(dset32Ci8));

		final DatasetAttributes datasetAttributesC = n5Zarr.getDatasetAttributes(testZarrDatasetName + "/3x2_c_i8");
		assertArrayEquals(datasetAttributesC.getDimensions(), new long[]{3, 2});
		assertArrayEquals(datasetAttributesC.getBlockSize(), new int[]{3, 2});
		assertEquals(DataType.INT64, datasetAttributesC.getDataType());

		final DatasetAttributes datasetAttributesF = n5Zarr.getDatasetAttributes(testZarrDatasetName + "/3x2_f_i8");
		assertArrayEquals(datasetAttributesF.getDimensions(), new long[]{2, 3});
		assertArrayEquals(datasetAttributesF.getBlockSize(), new int[]{2, 3});
		assertEquals(DataType.INT64, datasetAttributesF.getDataType());

		// TODO test after we implement this
		/* N5 array parameter mapping */
		assertArrayEquals(
				n5Zarr.getAttribute(testZarrDatasetName + "/3x2_c_i8", "dimensions", long[].class),
				new long[]{3, 2});
		assertArrayEquals(
				n5Zarr.getAttribute(testZarrDatasetName + "/3x2_c_i8", "blockSize", int[].class),
				new int[]{3, 2});
		assertEquals(
				n5Zarr.getAttribute(testZarrDatasetName + "/3x2_c_i8", "dataType", DataType.class),
				DataType.INT64);

		/* LE uint8 in C and F order */
		final UnsignedByteType refUnsignedByte = new UnsignedByteType();

		assertIsSequence(N5Utils.open(n5Zarr, testZarrDatasetName + "/3x2_c_u1"), refUnsignedByte);
		assertIsSequence(
			Views.permute(
				(RandomAccessibleInterval<UnsignedByteType>)N5Utils.open(n5Zarr, testZarrGroupName + "/3x2_f_u1"),
				0,
				1),
			refUnsignedByte);

		/* LE int64 in C and F order */
		final LongType refLong = new LongType();

		assertIsSequence(N5Utils.open(n5Zarr, testZarrDatasetName + "/3x2_c_i8"), refLong);
		assertIsSequence(
				Views.permute(
					(RandomAccessibleInterval<LongType>)N5Utils.open(n5Zarr, testZarrDatasetName + "/3x2_f_i8"),
					0,
					1),
				refLong);

		assertIsSequence(N5Utils.open(n5Zarr, testZarrDatasetName + "/30x20_c_i8"), refLong);
		assertIsSequence(
				Views.permute(
					(RandomAccessibleInterval<LongType>)N5Utils.open(n5Zarr, testZarrDatasetName + "/30x20_f_i8"),
					0,
					1),
				refLong);

		/* BE int32 in C and F order */
		final UnsignedIntType refUnsignedInt = new UnsignedIntType();

		assertIsSequence(N5Utils.open(n5Zarr, testZarrDatasetName + "/3x2_c_u4"), refUnsignedInt);
		assertIsSequence(
			Views.permute(
				(RandomAccessibleInterval<UnsignedIntType>)N5Utils.open(n5Zarr, testZarrGroupName + "/3x2_f_u4"),
				0,
				1),
			refUnsignedInt);

		assertIsSequence(N5Utils.open(n5Zarr, testZarrDatasetName + "/30x20_c_u4"), refUnsignedInt);
		assertIsSequence(
			Views.permute(
				(RandomAccessibleInterval<UnsignedIntType>)N5Utils.open(n5Zarr, testZarrGroupName + "/30x20_f_u4"),
				0,
				1),
			refUnsignedInt);

		/* LE float64 in C and F order */
		final DoubleType refDouble = new DoubleType();

		assertIsSequence(N5Utils.open(n5Zarr, testZarrDatasetName + "/3x2_c_f8"), refDouble);
		assertIsSequence(
			Views.permute(
					(RandomAccessibleInterval<DoubleType>)N5Utils.open(n5Zarr, testZarrGroupName + "/3x2_f_f8"),
					0,
					1),
			refDouble);

		assertIsSequence(N5Utils.open(n5Zarr, testZarrDatasetName + "/30x20_c_f8"), refDouble);
		assertIsSequence(
			Views.permute(
				(RandomAccessibleInterval<DoubleType>)N5Utils.open(n5Zarr, testZarrGroupName + "/30x20_f_f8"),
				0,
				1),
			refDouble);

		/* BE float32 in C and F order */
		final FloatType refFloat = new FloatType();

		assertIsSequence(N5Utils.open(n5Zarr, testZarrDatasetName + "/3x2_c_f4"), refFloat);
		assertIsSequence(
			Views.permute(
				(RandomAccessibleInterval<FloatType>)N5Utils.open(n5Zarr, testZarrGroupName + "/3x2_f_f4"),
				0,
				1),
			refFloat);

		assertIsSequence(N5Utils.open(n5Zarr, testZarrDatasetName + "/30x20_c_f4"), refFloat);
		assertIsSequence(
			Views.permute(
				(RandomAccessibleInterval<FloatType>)N5Utils.open(n5Zarr, testZarrGroupName + "/30x20_f_f4"),
				0,
				1),
			refFloat);

		/* compressors */
		final UnsignedLongType refUnsignedLong = new UnsignedLongType();

		assertIsSequence(N5Utils.open(n5Zarr, testZarrDatasetName + "/30x20_c_u8_zlib"), refUnsignedLong);
		assertIsSequence(N5Utils.open(n5Zarr, testZarrDatasetName + "/30x20_c_u8_gzip"), refUnsignedLong);
		assertIsSequence(N5Utils.open(n5Zarr, testZarrDatasetName + "/30x20_c_u8_bz2"), refUnsignedLong);

		/* fill value 1 */
		String datasetName = testZarrGroupName + "/3x2_c_u4_f1";

		final RandomAccessibleInterval<UnsignedIntType> a3x2_c_bu4_f1 = N5Utils.open(n5Zarr, datasetName);
		assertIsSequence(a3x2_c_bu4_f1, refUnsignedInt);

		DatasetAttributes attributes = n5Zarr.getDatasetAttributes(datasetName);
		final long[] shape = attributes.getDimensions();
		Arrays.setAll(shape, i -> shape[i] + 5);
		n5Zarr.setAttribute(datasetName, "dimensions", shape);

		final RandomAccessibleInterval<UnsignedIntType> a3x2_c_bu4_f1_after = N5Utils.open(n5Zarr, datasetName);
		assertIsSequence(Views.interval(a3x2_c_bu4_f1_after, a3x2_c_bu4_f1), refUnsignedInt);
		final net.imglib2.RandomAccess<UnsignedIntType> ra = a3x2_c_bu4_f1_after.randomAccess();

		/* fill value NaN */
		datasetName = testZarrDatasetName + "/3x2_c_f4_fnan";

		final RandomAccessibleInterval<FloatType> a3x2_c_lf4_fnan = N5Utils.open(n5Zarr, datasetName);
		assertIsSequence(a3x2_c_lf4_fnan, refFloat);

		attributes = n5Zarr.getDatasetAttributes(datasetName);
		final long[] shapef = attributes.getDimensions();
		Arrays.setAll(shapef, i -> shapef[i] + 5);
		n5Zarr.setAttribute(datasetName, "dimensions", shapef);

		final RandomAccessibleInterval<FloatType> a3x2_c_lf4_fnan_after = N5Utils.open(n5Zarr, datasetName);
		assertIsSequence(Views.interval(a3x2_c_lf4_fnan_after, a3x2_c_lf4_fnan), refFloat);
		final net.imglib2.RandomAccess<FloatType> raf = a3x2_c_lf4_fnan_after.randomAccess();
		raf.setPosition(shapef[0] - 5, 0);
		assertTrue(Float.isNaN(raf.get().getRealFloat()));
		raf.setPosition(shapef[1] - 5, 1);
		assertTrue(Float.isNaN(raf.get().getRealFloat()));

		
	}
}
