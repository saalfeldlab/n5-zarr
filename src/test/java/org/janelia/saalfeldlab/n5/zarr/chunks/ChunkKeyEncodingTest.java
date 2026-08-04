package org.janelia.saalfeldlab.n5.zarr.chunks;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import org.janelia.saalfeldlab.n5.ByteArrayDataBlock;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.FileSystemKeyValueAccess;
import org.janelia.saalfeldlab.n5.NameConfigAdapter;
import org.janelia.saalfeldlab.n5.zarr.v3.ZarrV3DataType;
import org.janelia.saalfeldlab.n5.zarr.v3.ZarrV3DatasetAttributes;
import org.janelia.saalfeldlab.n5.zarr.v3.ZarrV3KeyValueReader;
import org.janelia.saalfeldlab.n5.zarr.v3.ZarrV3KeyValueWriter;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class ChunkKeyEncodingTest {

	private static Gson gson;

	@BeforeClass
	public static void setup() {

		final GsonBuilder gsonBuilder = new GsonBuilder();
		gsonBuilder.registerTypeHierarchyAdapter(ChunkKeyEncoding.class,
				NameConfigAdapter.getJsonAdapter(ChunkKeyEncoding.class));
		gsonBuilder.disableHtmlEscaping();
		gson = gsonBuilder.create();
	}

	@Test
	public void testV2ChunkPath() {

		// chunk keys are serialized reversing the order of coordinates
		final V2ChunkKeyEncoding dot = new V2ChunkKeyEncoding(".");
		assertEquals("0.0.0", dot.getChunkPath(new long[]{0, 0, 0}));
		assertEquals("3.2.1", dot.getChunkPath(new long[]{1, 2, 3}));
		assertEquals("5", dot.getChunkPath(new long[]{5}));
		assertEquals("2.1", dot.getChunkPath(new long[]{1, 2}));

		final V2ChunkKeyEncoding slash = new V2ChunkKeyEncoding("/");
		assertEquals("0/0/0", slash.getChunkPath(new long[]{0, 0, 0}));
		assertEquals("3/2/1", slash.getChunkPath(new long[]{1, 2, 3}));
		assertEquals("5", slash.getChunkPath(new long[]{5}));

		assertThrows("invalid separator", IllegalArgumentException.class, () -> new V2ChunkKeyEncoding("#"));
		assertThrows("empty separator", IllegalArgumentException.class, () -> new V2ChunkKeyEncoding(""));
		assertThrows("null separator", IllegalArgumentException.class, () -> new V2ChunkKeyEncoding(null));
	}

	@Test
	public void testDefaultChunkPath() {

		// the default encoding prefixes the key with "c" and the separator
		// chunk keys are serialized reversing the order of coordinates
		final DefaultChunkKeyEncoding slash = new DefaultChunkKeyEncoding("/");
		assertEquals("c/0/0/0", slash.getChunkPath(new long[]{0, 0, 0}));
		assertEquals("c/3/2/1", slash.getChunkPath(new long[]{1, 2, 3}));
		assertEquals("c/5", slash.getChunkPath(new long[]{5}));

		final DefaultChunkKeyEncoding dot = new DefaultChunkKeyEncoding(".");
		assertEquals("c.0.0.0", dot.getChunkPath(new long[]{0, 0, 0}));
		assertEquals("c.3.2.1", dot.getChunkPath(new long[]{1, 2, 3}));
		assertEquals("c.5", dot.getChunkPath(new long[]{5}));
		
		assertThrows("invalid separator", IllegalArgumentException.class, () -> new DefaultChunkKeyEncoding("#"));
		assertThrows("empty separator", IllegalArgumentException.class, () -> new DefaultChunkKeyEncoding(""));
		assertThrows("null separator", IllegalArgumentException.class, () -> new DefaultChunkKeyEncoding(null));
	}

	@Test
	public void testChunkPathThroughDatasetAttributes() {

		final long[] dimensions = new long[]{100, 200, 300};
		final int[] chunkShape = new int[]{32, 32, 32};

		final ZarrV3DatasetAttributes v2Attrs = attributes(dimensions, chunkShape, new V2ChunkKeyEncoding("."));
		assertEquals("0.0.0", v2Attrs.relativeBlockPath(0, 0, 0));
		assertEquals("3.2.1", v2Attrs.relativeBlockPath(1, 2, 3));

		final ZarrV3DatasetAttributes v2AttrsSlash = attributes(dimensions, chunkShape, new V2ChunkKeyEncoding("/"));
		assertEquals("0/0/0", v2AttrsSlash.relativeBlockPath(0, 0, 0));
		assertEquals("3/2/1", v2AttrsSlash.relativeBlockPath(1, 2, 3));

		final ZarrV3DatasetAttributes defaultAttrs = attributes(dimensions, chunkShape,
				new DefaultChunkKeyEncoding("/"));
		assertEquals("c/0/0/0", defaultAttrs.relativeBlockPath(0, 0, 0));
		assertEquals("c/3/2/1", defaultAttrs.relativeBlockPath(1, 2, 3));

		final ZarrV3DatasetAttributes defaultAttrsDot = attributes(dimensions, chunkShape,
				new DefaultChunkKeyEncoding("."));
		assertEquals("c.0.0.0", defaultAttrsDot.relativeBlockPath(0, 0, 0));
		assertEquals("c.3.2.1", defaultAttrsDot.relativeBlockPath(1, 2, 3));
	}

	@Test
	public void testSerialization() {

		final JsonObject v2Json = gson.toJsonTree(new V2ChunkKeyEncoding("/"), ChunkKeyEncoding.class)
				.getAsJsonObject();
		assertEquals("v2", v2Json.get("name").getAsString());
		assertEquals("/", v2Json.get("configuration").getAsJsonObject().get("separator").getAsString());

		final JsonObject defaultJson = gson.toJsonTree(new DefaultChunkKeyEncoding("."), ChunkKeyEncoding.class)
				.getAsJsonObject();
		assertEquals("default", defaultJson.get("name").getAsString());
		assertEquals(".", defaultJson.get("configuration").getAsJsonObject().get("separator").getAsString());
	}

	@Test
	public void testDeserialization() {

		for (final String separator : V2ChunkKeyEncoding.VALID_SEPARATORS) {

			final ChunkKeyEncoding v2 = gson.fromJson(
					String.format("{\"name\":\"v2\",\"configuration\":{\"separator\":\"%s\"}}", separator),
					ChunkKeyEncoding.class);
			assertTrue(v2 instanceof V2ChunkKeyEncoding);
			assertEquals(separator, ((V2ChunkKeyEncoding)v2).getSeparator());
			assertEquals(String.join(separator, "3", "2", "1"), v2.getChunkPath(new long[]{1, 2, 3}));

			final ChunkKeyEncoding dflt = gson.fromJson(
					String.format("{\"name\":\"default\",\"configuration\":{\"separator\":\"%s\"}}", separator),
					ChunkKeyEncoding.class);
			assertTrue(dflt instanceof DefaultChunkKeyEncoding);
			assertEquals(separator, ((DefaultChunkKeyEncoding)dflt).getSeparator());
			assertEquals(String.join(separator, "c", "3", "2", "1"), dflt.getChunkPath(new long[]{1, 2, 3}));
		}
	}

	@Test
	public void testDeserializationWithoutConfiguration() {

		// separator is optional, and falls back to the encoding's default
		final ChunkKeyEncoding v2 = gson.fromJson("{\"name\":\"v2\"}", ChunkKeyEncoding.class);
		assertTrue(v2 instanceof V2ChunkKeyEncoding);
		assertEquals(V2ChunkKeyEncoding.DEFAULT_SEPARATOR, ((V2ChunkKeyEncoding)v2).getSeparator());
		assertEquals("3.2.1", v2.getChunkPath(new long[]{1, 2, 3}));

		final ChunkKeyEncoding dflt = gson.fromJson("{\"name\":\"default\"}", ChunkKeyEncoding.class);
		assertTrue(dflt instanceof DefaultChunkKeyEncoding);
		assertEquals(DefaultChunkKeyEncoding.DEFAULT_SEPARATOR, ((DefaultChunkKeyEncoding)dflt).getSeparator());
		assertEquals("c/3/2/1", dflt.getChunkPath(new long[]{1, 2, 3}));
	}

	@Test
	public void testSerializationRoundTrip() {

		for (final ChunkKeyEncoding original : new ChunkKeyEncoding[]{
				new V2ChunkKeyEncoding("."),
				new V2ChunkKeyEncoding("/"),
				new DefaultChunkKeyEncoding("."),
				new DefaultChunkKeyEncoding("/")}) {

			final JsonElement json = gson.toJsonTree(original, ChunkKeyEncoding.class);
			final ChunkKeyEncoding deserialized = gson.fromJson(json, ChunkKeyEncoding.class);

			assertEquals(original.getClass(), deserialized.getClass());
			final long[] gridPosition = new long[]{1, 2, 3};
			assertEquals(original.getChunkPath(gridPosition), deserialized.getChunkPath(gridPosition));
		}
	}

	private static ZarrV3DatasetAttributes attributes(final long[] dimensions, final int[] chunkShape,
			final ChunkKeyEncoding keyEncoding) {

		return new ZarrV3DatasetAttributes(
				dimensions,
				new ChunkAttributes(new RegularChunkGrid(chunkShape), keyEncoding),
				ZarrV3DataType.fromDataType(DataType.UINT8),
				"0",
				null,
				null, // block codec
				null // dataset codecs
		);
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


			final ZarrV3DatasetAttributes readAttributes = (ZarrV3DatasetAttributes)zarr.getDatasetAttributes("a");
			assertTrue("v2 chunk key encoding round trips",
					readAttributes.getChunkAttributes().getKeyEncoding() instanceof V2ChunkKeyEncoding);

			final DataBlock<?> block = zarr.readBlock("a", readAttributes, 0, 0);
			assertNotNull("block read", block);
			assertArrayEquals(data, (byte[])block.getData());

			// clean up
			zarr.remove();
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
		Files.write(dataset.resolve("1.0"), data);

		try (ZarrV3KeyValueReader zarr = new ZarrV3KeyValueReader(
				new FileSystemKeyValueAccess(), root.toString(), new GsonBuilder(), false)) {

			final ZarrV3DatasetAttributes attributes = (ZarrV3DatasetAttributes)zarr.getDatasetAttributes("/a");
			assertEquals("1.0", attributes.relativeBlockPath(0, 1));

			final DataBlock<?> block = zarr.readBlock("/a", attributes, 0, 1);
			assertNotNull("block read", block);
			assertArrayEquals(data, (byte[])block.getData());
		}

		// clean up
		Files.deleteIfExists(dataset.resolve("1.0"));
		Files.deleteIfExists(dataset.resolve("zarr.json"));
		Files.deleteIfExists(dataset);
		Files.deleteIfExists(root.resolve("zarr.json"));
		Files.deleteIfExists(root);
	}

	private static void write(final Path path, final String contents) throws IOException {

		Files.write(path, contents.getBytes(StandardCharsets.UTF_8));
	}
}
