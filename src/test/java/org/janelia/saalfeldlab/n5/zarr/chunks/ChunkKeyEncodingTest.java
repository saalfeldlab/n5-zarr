package org.janelia.saalfeldlab.n5.zarr.chunks;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.janelia.saalfeldlab.n5.ByteArrayDataBlock;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.FileSystemKeyValueAccess;
import org.janelia.saalfeldlab.n5.zarr.v3.ZarrV3DataType;
import org.janelia.saalfeldlab.n5.zarr.v3.ZarrV3DatasetAttributes;
import org.janelia.saalfeldlab.n5.zarr.v3.ZarrV3KeyValueReader;
import org.janelia.saalfeldlab.n5.zarr.v3.ZarrV3KeyValueWriter;
import org.junit.Test;

import com.google.gson.GsonBuilder;

/**
 * Tests for chunk key encodings, see
 * https://zarr-specs.readthedocs.io/en/latest/v3/chunk-key-encodings
 */
public class ChunkKeyEncodingTest {

	@Test
	public void testChunkKeys() {

		// the default encoding prefixes with "c", the v2 encoding has no prefix
		// and, in particular, no leading separator
		assertEquals("c/1/2/3", new DefaultChunkKeyEncoding("/").getChunkPath(new long[]{3, 2, 1}));
		assertEquals("c.1.2.3", new DefaultChunkKeyEncoding(".").getChunkPath(new long[]{3, 2, 1}));

		assertEquals("1.2.3", new V2ChunkKeyEncoding(".").getChunkPath(new long[]{3, 2, 1}));
		assertEquals("1/2/3", new V2ChunkKeyEncoding("/").getChunkPath(new long[]{3, 2, 1}));

		// one dimensional
		assertEquals("5", new V2ChunkKeyEncoding(".").getChunkPath(new long[]{5}));
		assertEquals("0.0.0.0", new V2ChunkKeyEncoding(".").getChunkPath(new long[]{0, 0, 0, 0}));
	}

	/**
	 * A v2-encoded chunk must be written to, and read from, a key without a
	 * leading separator, see
	 * https://github.com/saalfeldlab/n5-zarr/issues/98
	 */
	@Test
	public void testV2ChunkKeyReadWrite() throws IOException {

		final Path root = Files.createTempDirectory("zarr-v3-v2-key-test");
		final long[] dimensions = new long[]{4, 4};
		final int[] chunkShape = new int[]{2, 2};

		final ZarrV3DatasetAttributes attributes = new ZarrV3DatasetAttributes(
				dimensions,
				new ChunkAttributes(new RegularChunkGrid(chunkShape), new V2ChunkKeyEncoding(".")),
				ZarrV3DataType.fromDataType(DataType.UINT8),
				"0",
				null, // dimension names
				null, // block codec
				null); // dataset codecs

		assertEquals("0.0", attributes.relativeBlockPath(0, 0));
		assertEquals("1.0", attributes.relativeBlockPath(0, 1));

		final byte[] data = new byte[]{1, 2, 3, 4};
		try (ZarrV3KeyValueWriter zarr = new ZarrV3KeyValueWriter(
				new FileSystemKeyValueAccess(), root.toString(), new GsonBuilder(), false)) {

			zarr.createDataset("a", attributes);
			zarr.writeBlock("a", attributes, new ByteArrayDataBlock(chunkShape, new long[]{0, 0}, data));
		}

		// the chunk must be stored at "a/0.0", not "a/.0.0"
		assertTrue("chunk written to spec-correct key", Files.exists(root.resolve("a").resolve("0.0")));

		try (ZarrV3KeyValueReader zarr = new ZarrV3KeyValueReader(
				new FileSystemKeyValueAccess(), root.toString(), new GsonBuilder(), false)) {

			final ZarrV3DatasetAttributes readAttributes = (ZarrV3DatasetAttributes)zarr.getDatasetAttributes("a");
			assertTrue("v2 chunk key encoding round trips",
					readAttributes.getChunkAttributes().getKeyEncoding() instanceof V2ChunkKeyEncoding);

			final DataBlock<?> block = zarr.readBlock("a", readAttributes, 0, 0);
			assertNotNull("block read", block);
			assertArrayEquals(data, (byte[])block.getData());
		}
	}

	/**
	 * Reads a hand-written container using the v2 chunk key encoding, i.e. one
	 * that was not written by this library.
	 */
	@Test
	public void testReadV2ChunkKeyContainer() throws IOException {

		final Path root = Files.createTempDirectory("zarr-v3-v2-key-external");
		write(root.resolve("zarr.json"), "{\"zarr_format\":3,\"node_type\":\"group\",\"attributes\":{}}");

		final Path dataset = Files.createDirectories(root.resolve("a"));
		write(dataset.resolve("zarr.json"),
				"{\"zarr_format\":3,\"node_type\":\"array\",\"shape\":[4,4],\"data_type\":\"uint8\","
						+ "\"chunk_grid\":{\"name\":\"regular\",\"configuration\":{\"chunk_shape\":[2,2]}},"
						+ "\"chunk_key_encoding\":{\"name\":\"v2\",\"configuration\":{\"separator\":\".\"}},"
						+ "\"fill_value\":0,\"attributes\":{},"
						+ "\"codecs\":[{\"name\":\"bytes\",\"configuration\":{\"endian\":\"little\"}}]}");

		final byte[] data = new byte[]{1, 2, 3, 4};
		Files.write(dataset.resolve("0.0"), data);

		try (ZarrV3KeyValueReader zarr = new ZarrV3KeyValueReader(
				new FileSystemKeyValueAccess(), root.toString(), new GsonBuilder(), false)) {

			final ZarrV3DatasetAttributes attributes = (ZarrV3DatasetAttributes)zarr.getDatasetAttributes("/a");
			assertEquals("0.0", attributes.relativeBlockPath(0, 0));

			final DataBlock<?> block = zarr.readBlock("/a", attributes, 0, 0);
			assertNotNull("block read", block);
			assertArrayEquals(data, (byte[])block.getData());
		}
	}

	private static void write(final Path path, final String contents) throws IOException {

		Files.write(path, contents.getBytes(StandardCharsets.UTF_8));
	}
}