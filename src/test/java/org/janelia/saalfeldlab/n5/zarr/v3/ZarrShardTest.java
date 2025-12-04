package org.janelia.saalfeldlab.n5.zarr.v3;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileSystems;
import java.nio.file.Files;

import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.FileSystemKeyValueAccess;
import org.janelia.saalfeldlab.n5.N5FSTest;
import org.janelia.saalfeldlab.n5.RawCompression;
import org.janelia.saalfeldlab.n5.shard.ShardTest;
import org.junit.After;

import com.google.gson.GsonBuilder;


//@RunWith(Parameterized.class)
public class ZarrShardTest extends ShardTest {

	private static final boolean LOCAL_DEBUG = false;

	private static final N5FSTest tempN5Factory = new N5FSTest() {

		@Override public ZarrV3KeyValueWriter createTempN5Writer() {

			final String basePath = new File(tempN5PathName()).toURI().normalize().getPath();
			try {
				String uri = new URI("file", null, basePath, null).toString();
				return new ZarrV3KeyValueWriter(
						new FileSystemKeyValueAccess(FileSystems.getDefault()),
						basePath, new GsonBuilder(), false, true, "/", false);
			} catch (URISyntaxException e) {
				e.printStackTrace();
			}
			return null;
		}

		private String tempN5PathName() {

			try {
				final File tmpFile = Files.createTempDirectory("zarr-shard-test-").toFile();
				tmpFile.delete();
				tmpFile.mkdir();
				tmpFile.deleteOnExit();
				return tmpFile.getCanonicalPath();
			} catch (final Exception e) {
				throw new RuntimeException(e);
			}
		}
	};

	@After
	public void removeTempWriters() {

		tempN5Factory.removeTempWriters();
	}

	protected DatasetAttributes getTestAttributes(long[] dimensions, int[] shardSize, int[] blockSize) {

		return new ZarrV3DatasetAttributes(dimensions, shardSize, blockSize,
				DataType.UINT8, new RawCompression());
	}

	@Override
	protected DatasetAttributes getTestAttributes() {

		return getTestAttributes(new long[]{8, 8}, new int[]{4, 4}, new int[]{2, 2});
	}

}
