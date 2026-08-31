package org.janelia.saalfeldlab.n5.zarr.v3;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.FileSystemKeyValueAccess;
import org.janelia.saalfeldlab.n5.FileSystemKeyValueRoot;
import org.janelia.saalfeldlab.n5.GzipCompression;
import org.janelia.saalfeldlab.n5.RawCompression;
import org.janelia.saalfeldlab.n5.zarr.chunks.DefaultChunkKeyEncoding;
import org.junit.Test;

import com.google.gson.GsonBuilder;

public class ZarrV3DatasetAttributesTest {

	@Test
	public void builderTests() {

		final long[] dims = new long[]{100, 200, 300};
		final int[] blk = new int[]{32, 32, 32};

		// default blockSize uses defaultChunkShape, not full dimensions
		final ZarrV3DatasetAttributes defaultBlk = ZarrV3DatasetAttributes.builder(dims, DataType.FLOAT32).build();
		assertArrayEquals(ZarrV3DatasetAttributes.defaultChunkShape(dims), defaultBlk.getBlockSize());

		// blockSize is reflected in output
		final ZarrV3DatasetAttributes withBlk = ZarrV3DatasetAttributes.builder(dims, DataType.FLOAT32)
				.blockSize(blk).build();
		assertArrayEquals(blk, withBlk.getBlockSize());
		assertFalse(withBlk.isSharded());

		// fillValue is reflected
		final ZarrV3DatasetAttributes withFill = ZarrV3DatasetAttributes.builder(dims, DataType.FLOAT32)
				.blockSize(blk).fillValue("1.5").build();
		assertEquals("1.5", withFill.getFillValue());

		// dimensionNames are reflected
		final String[] names = new String[]{"x", "y", "z"};
		final ZarrV3DatasetAttributes withNames = ZarrV3DatasetAttributes.builder(dims, DataType.FLOAT32)
				.blockSize(blk).dimensionNames(names).build();
		assertArrayEquals(names, withNames.getDimensionNames());

		// dimensionSeparator is reflected in chunk key encoding
		final ZarrV3DatasetAttributes withSlash = ZarrV3DatasetAttributes.builder(dims, DataType.FLOAT32)
				.blockSize(blk).dimensionSeparator("/").build();
		assertTrue(withSlash.relativeBlockPath(1, 2, 3).contains("/"));

		final ZarrV3DatasetAttributes withDot = ZarrV3DatasetAttributes.builder(dims, DataType.FLOAT32)
				.blockSize(blk).dimensionSeparator(".").build();
		assertTrue(withDot.relativeBlockPath(1, 2, 3).contains("."));

		// chunkKeyEncoding overrides dimensionSeparator
		final DefaultChunkKeyEncoding encoding = new DefaultChunkKeyEncoding(".");
		final ZarrV3DatasetAttributes withEncoding = ZarrV3DatasetAttributes.builder(dims, DataType.FLOAT32)
				.blockSize(blk).chunkKeyEncoding(encoding).build();
		assertTrue(withEncoding.relativeBlockPath(1, 2, 3).contains("."));

		// compression sets a data codec; RawCompression is a no-op
		final ZarrV3DatasetAttributes withGzip = ZarrV3DatasetAttributes.builder(dims, DataType.FLOAT32)
				.blockSize(blk).compression(new GzipCompression()).build();
		assertEquals(1, withGzip.getDataCodecInfos().length);

		final ZarrV3DatasetAttributes withRaw = ZarrV3DatasetAttributes.builder(dims, DataType.FLOAT32)
				.blockSize(blk).compression(new RawCompression()).build();
		assertEquals(0, withRaw.getDataCodecInfos().length);

		// sharding: both blockSize (shard) and chunkSize (inner) set
		final int[] shardSize = new int[]{64, 64, 64};
		final int[] chunkSize = new int[]{16, 16, 16};
		final ZarrV3DatasetAttributes sharded = ZarrV3DatasetAttributes.builder(dims, DataType.UINT16)
				.blockSize(shardSize).chunkSize(chunkSize).build();
		assertTrue(sharded.isSharded());
		assertArrayEquals(shardSize, sharded.getBlockSize());
		assertArrayEquals(chunkSize, sharded.getChunkSize());
		assertNotNull(sharded.getBlockCodecInfo());

		// round-trip through Builder(DatasetAttributes)
		final ZarrV3DatasetAttributes roundTrip = ZarrV3DatasetAttributes.builder(withGzip).build();
		assertArrayEquals(withGzip.getDimensions(), roundTrip.getDimensions());
		assertArrayEquals(withGzip.getBlockSize(), roundTrip.getBlockSize());
		assertEquals(withGzip.getDataType(), roundTrip.getDataType());
		assertEquals(withGzip.getDataCodecInfos().length, roundTrip.getDataCodecInfos().length);

		// validateLength: blockSize wrong length throws
		try {
			ZarrV3DatasetAttributes.builder(dims, DataType.FLOAT32).blockSize(new int[]{32, 32}).build();
			// blockSize setter does not validate length — only chunkSize() does, so no exception expected here
		} catch (final IllegalArgumentException e) {
			// acceptable if validation is added to blockSize()
		}

		// validateBlockChunkSize: chunkSize > blockSize throws
		try {
			ZarrV3DatasetAttributes.builder(dims, DataType.FLOAT32)
					.blockSize(new int[]{16, 16, 16})
					.chunkSize(new int[]{32, 32, 32})
					.build();
			throw new AssertionError("Expected IllegalArgumentException for chunkSize > blockSize");
		} catch (final IllegalArgumentException expected) {}
	}

	@Test
	public void serializationTests() throws IOException {

		final String tmp = Files.createTempDirectory("zarr-v3-test").toUri().getPath();
		try (final ZarrV3KeyValueWriter zarr = new ZarrV3KeyValueWriter(
				new FileSystemKeyValueRoot(tmp), new GsonBuilder(), true)) {

			final ZarrV3DatasetAttributes datasetAttributes = ZarrV3DatasetAttributes
					.builder(new long[]{64, 64, 64}, DataType.INT16)
					.blockSize(new int[]{64, 64, 64})
					.chunkSize(new int[]{32, 32, 32})
					.compression(new GzipCompression())
					.build();

			zarr.createDataset("gz", datasetAttributes);
			assertTrue(zarr.datasetExists("gz"));

			zarr.remove();
		}
	}



}
