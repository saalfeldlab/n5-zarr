package org.janelia.saalfeldlab.n5.zarr.v3;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.GZIPOutputStream;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.FileSystemKeyValueAccess;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * Arrays on a rectilinear chunk grid (https://github.com/zarr-developers/zarr-extensions/tree/main/chunk-grids/rectilinear),
 * written by hand as {@code zarr.json} and chunk files, and read through the ordinary reader.
 */
public class ZarrV3RectilinearTest {

	@Rule
	public TemporaryFolder folder = new TemporaryFolder();

	private static final int ROWS = 10, COLS = 7;

	/** The value of the array at a row and a column. */
	private static int value(final int row, final int col) {

		return row * 100 + col;
	}

	/** The edge lengths of the rows and the columns of the test array: {@code [4, 6]} and {@code [3, 3, 1]}. */
	private static final int[] ROW_EDGES = {4, 6}, COL_EDGES = {3, 3, 1};

	private static final String ARRAY_JSON = "{\"zarr_format\":3,\"node_type\":\"array\",\"shape\":[" + ROWS + "," + COLS + "],"
			+ "\"data_type\":\"int32\","
			+ "\"chunk_grid\":{\"name\":\"rectilinear\",\"configuration\":{\"kind\":\"inline\",\"chunk_shapes\":%s}},"
			+ "\"chunk_key_encoding\":{\"name\":\"default\",\"configuration\":{\"separator\":\"/\"}},"
			+ "\"fill_value\":0,\"codecs\":%s,\"dimension_names\":[\"y\",\"x\"]}";

	private static final String BYTES = "{\"name\":\"bytes\",\"configuration\":{\"endian\":\"little\"}}";

	private static final String GZIP = "{\"name\":\"gzip\",\"configuration\":{\"level\":5}}";

	/** The chunk at a position on the chunk grid, as the bytes a {@code bytes} codec stores: C-order int32. */
	private static byte[] chunkBytes(final int[] rowEdges, final int[] colEdges, final int chunkRow, final int chunkCol, final boolean gzip)
			throws IOException {

		int row0 = 0, col0 = 0;
		for (int i = 0; i < chunkRow; i++)
			row0 += rowEdges[i];
		for (int j = 0; j < chunkCol; j++)
			col0 += colEdges[j];
		final ByteBuffer buffer = ByteBuffer.allocate(4 * rowEdges[chunkRow] * colEdges[chunkCol]).order(ByteOrder.LITTLE_ENDIAN);
		for (int r = 0; r < rowEdges[chunkRow]; r++)
			for (int c = 0; c < colEdges[chunkCol]; c++)
				buffer.putInt(value(row0 + r, col0 + c));
		if (!gzip)
			return buffer.array();
		final ByteArrayOutputStream out = new ByteArrayOutputStream();
		try (GZIPOutputStream z = new GZIPOutputStream(out)) {
			z.write(buffer.array());
		}
		return out.toByteArray();
	}

	/** Writes the test array at {@code a} under a new root, and returns a reader of it. */
	private ZarrV3KeyValueReader write(final String chunkShapes, final String codecs, final int[] rowEdges, final int[] colEdges,
			final boolean gzip) throws IOException {

		final Path root = folder.newFolder().toPath();
		Files.write(root.resolve("zarr.json"), "{\"zarr_format\":3,\"node_type\":\"group\"}".getBytes(StandardCharsets.UTF_8));
		final Path array = Files.createDirectories(root.resolve("a"));
		Files.write(array.resolve("zarr.json"), String.format(ARRAY_JSON, chunkShapes, codecs).getBytes(StandardCharsets.UTF_8));
		for (int i = 0; i < rowEdges.length; i++) {
			for (int j = 0; j < colEdges.length; j++) {
				final Path chunk = array.resolve("c").resolve(Integer.toString(i)).resolve(Integer.toString(j));
				Files.createDirectories(chunk.getParent());
				Files.write(chunk, chunkBytes(rowEdges, colEdges, i, j, gzip));
			}
		}
		return new ZarrV3KeyValueReader(new FileSystemKeyValueAccess(), root.toString(), new com.google.gson.GsonBuilder(), false);
	}

	private ZarrV3KeyValueReader writePlain() throws IOException {

		return write("[[4,6],[[3,2],1]]", "[" + BYTES + "]", ROW_EDGES, COL_EDGES, false);
	}

	/** Reads every block of the array and checks it against the values. */
	private static void assertEveryBlock(final ZarrV3KeyValueReader reader, final int blockRows, final int blockCols) {

		final DatasetAttributes attributes = reader.getDatasetAttributes("a");
		assertNotNull("the array is a dataset", attributes);
		// N5 lists the fastest axis first: x, then y
		assertArrayEquals(new long[]{COLS, ROWS}, attributes.getDimensions());
		assertArrayEquals(new int[]{blockCols, blockRows}, attributes.getBlockSize());
		final long blocksX = (COLS + blockCols - 1) / blockCols, blocksY = (ROWS + blockRows - 1) / blockRows;
		for (long by = 0; by < blocksY; by++) {
			for (long bx = 0; bx < blocksX; bx++) {
				final DataBlock<?> block = reader.readBlock("a", attributes, bx, by);
				assertNotNull("block " + bx + "," + by, block);
				assertArrayEquals(new long[]{bx, by}, block.getGridPosition());
				final int[] data = (int[])block.getData();
				assertEquals(blockCols * blockRows, data.length);
				for (int y = 0; y < blockRows; y++)
					for (int x = 0; x < blockCols; x++)
						assertEquals("block " + bx + "," + by + " element " + x + "," + y,
								value((int)by * blockRows + y, (int)bx * blockCols + x), data[x + y * blockCols]);
			}
		}
	}

	@Test
	public void readsEveryBlockOfARectilinearArray() throws IOException {

		// rows are cut in 4 and 6, columns in 3, 3 and 1: N5 sees blocks of gcd(4, 6) = 2 rows and gcd(3, 3, 1) = 1 column
		assertEveryBlock(writePlain(), 2, 1);
	}

	@Test
	public void readsThroughTheCodecChainOfTheArray() throws IOException {

		assertEveryBlock(write("[[4,6],[[3,2],1]]", "[" + BYTES + "," + GZIP + "]", ROW_EDGES, COL_EDGES, true), 2, 1);
	}

	@Test
	public void readsARegularGridWrittenAsRectilinear() throws IOException {

		// integers are the steps of a regular grid, as the extension says: 5 rows and 4 columns a chunk
		final int[] rows = {5, 5}, cols = {4, 4};
		assertEveryBlock(write("[5,4]", "[" + BYTES + "]", rows, cols, false), 5, 4);
	}

	@Test
	public void aBlockIsReadFromTheKeyOfItsChunk() throws IOException {

		final DatasetAttributes attributes = writePlain().getDatasetAttributes("a");
		// blocks are (x, y): a column x is in the chunk column of edges 3, 3, 1; a row pair y in the chunk row of edges 4, 6
		assertEquals("c/0/0", attributes.relativeBlockPath(0, 0));
		assertEquals("c/0/0", attributes.relativeBlockPath(2, 1));
		assertEquals("c/0/1", attributes.relativeBlockPath(3, 0));
		assertEquals("c/1/1", attributes.relativeBlockPath(4, 3));
		assertEquals("c/1/2", attributes.relativeBlockPath(6, 4));
	}

	@Test
	public void blockExistsLooksAtTheChunk() throws IOException {

		final ZarrV3KeyValueReader reader = writePlain();
		final DatasetAttributes attributes = reader.getDatasetAttributes("a");
		assertTrue(reader.blockExists("a", attributes, 4, 3));
		Files.delete(java.nio.file.Paths.get(reader.getURI()).resolve("a").resolve("c").resolve("1").resolve("1"));
		assertFalse(reader.blockExists("a", attributes, 4, 3));
		assertNull("a chunk that is not stored has no blocks", reader.readBlock("a", attributes, 4, 3));
		assertNotNull("others are unaffected", reader.readBlock("a", attributes, 0, 0));
	}

	@Test
	public void theMetadataKeepsTheGridAndTheCodecsOfTheArray() throws IOException {

		final ZarrV3KeyValueReader reader = writePlain();
		final DatasetAttributes attributes = reader.getDatasetAttributes("a");
		assertTrue(attributes instanceof ZarrV3DatasetAttributes);
		final JsonObject json = reader.getGson().toJsonTree(attributes, ZarrV3DatasetAttributes.class).getAsJsonObject();
		final JsonObject grid = json.getAsJsonObject("chunk_grid");
		assertEquals("rectilinear", grid.get("name").getAsString());
		assertEquals(JsonParser.parseString("[[4,6],[[3,2],1]]"), grid.getAsJsonObject("configuration").get("chunk_shapes"));
		assertEquals("inline", grid.getAsJsonObject("configuration").get("kind").getAsString());
		// the codecs are the array's own, not the wrapper that reads blocks out of its chunks
		assertEquals(1, json.getAsJsonArray("codecs").size());
		assertEquals("bytes", json.getAsJsonArray("codecs").get(0).getAsJsonObject().get("name").getAsString());
	}

	private void assertNotADataset(final String chunkShapes, final String why) throws IOException {

		final DatasetAttributes attributes = write(chunkShapes, "[" + BYTES + "]", ROW_EDGES, COL_EDGES, false).getDatasetAttributes("a");
		assertNull(why + " (" + chunkShapes + ") is not a dataset", attributes);
	}

	@Test
	public void refusesWhatTheExtensionForbids() throws IOException {

		assertNotADataset("[[4,6]]", "a grid with too few axes");
		assertNotADataset("[[4,6],[3,3,1],[2]]", "a grid with too many axes");
		assertNotADataset("[[4,0],[3,3,1]]", "an edge of 0");
		assertNotADataset("[[4,-6],[3,3,1]]", "a negative edge");
		assertNotADataset("[[4,6.5],[3,3,1]]", "a fractional edge");
		assertNotADataset("[[4,\"6\"],[3,3,1]]", "an edge that is a string");
		assertNotADataset("[[],[3,3,1]]", "an empty list of edges");
		assertNotADataset("[0,[3,3,1]]", "a step of 0");
		assertNotADataset("[[[4,0],6],[3,3,1]]", "a run of 0 chunks");
		assertNotADataset("[[[0,2],6],[3,3,1]]", "a run of 0 long chunks");
		assertNotADataset("[[[4,2,1],6],[3,3,1]]", "a run-length pair of three");
		assertNotADataset("[[[4]],[3,3,1]]", "a run-length pair of one");
		assertNotADataset("[[[[4,2]]],[3,3,1]]", "runs nested in runs");
		assertNotADataset("[{\"a\":1},[3,3,1]]", "an object");
		assertNotADataset("[[4,5],[3,3,1]]", "edges that sum to 9 of 10 rows");
		assertNotADataset("[[4,6],[3,3]]", "edges that sum to 6 of 7 columns");
		assertNotADataset("[[4,6],[3,3,1,99999999999]]", "an edge too long for an int");
	}

	@Test
	public void acceptsEdgesThatOverflowTheArray() throws IOException {

		// the extension allows the sum of the edges to exceed the length, even by several chunks
		final ZarrV3KeyValueReader reader = write("[[4,6,8],[[3,2],1,5]]", "[" + BYTES + "]", new int[]{4, 6, 8}, new int[]{3, 3, 1, 5}, false);
		final DatasetAttributes attributes = reader.getDatasetAttributes("a");
		assertNotNull(attributes);
		assertArrayEquals(new int[]{1, 2}, attributes.getBlockSize());
		final int[] data = (int[])reader.readBlock("a", attributes, 6, 4).getData();
		assertArrayEquals(new int[]{value(8, 6), value(9, 6)}, data);
	}

	@Test
	public void aRectilinearArrayCannotBeWrittenBlockByBlock() throws IOException {

		final ZarrV3KeyValueReader reader = writePlain();
		final DatasetAttributes attributes = reader.getDatasetAttributes("a");
		final ZarrV3KeyValueWriter writer = new ZarrV3KeyValueWriter(new FileSystemKeyValueAccess(), reader.getURI().getPath(),
				new com.google.gson.GsonBuilder(), false);
		@SuppressWarnings({"unchecked", "rawtypes"})
		final DataBlock block = attributes.getDataType().createDataBlock(attributes.getBlockSize(), new long[]{0, 0});
		try {
			writer.writeBlock("a", attributes, block);
			fail("a block of a rectilinear grid is part of a chunk: writing it should have been refused");
		} catch (final UnsupportedOperationException expected) {
			assertTrue(expected.getMessage(), expected.getMessage().contains("part of a chunk"));
		}
	}

	/** The reason a grid is refused: the dataset is not found, and the reader does not say why, so ask the grid. */
	private static void assertGridRejected(final String chunkShapesInJson, final String reason, final long... dimensionsInZarrOrder) {

		try {
			final org.janelia.saalfeldlab.n5.zarr.chunks.RectilinearChunkGrid grid =
					org.janelia.saalfeldlab.n5.zarr.chunks.RectilinearChunkGrid.fromZarr(JsonParser.parseString(chunkShapesInJson));
			if (dimensionsInZarrOrder.length > 0) {
				final long[] n5Order = new long[dimensionsInZarrOrder.length];
				for (int i = 0; i < n5Order.length; i++)
					n5Order[i] = dimensionsInZarrOrder[n5Order.length - 1 - i];
				grid.validate(n5Order);
			}
			fail(chunkShapesInJson + " should be refused: " + reason);
		} catch (final com.google.gson.JsonParseException e) {
			assertTrue(chunkShapesInJson + " was refused, but for " + e.getMessage() + ", not for " + reason,
					e.getMessage().contains(reason));
		}
	}

	@Test
	public void refusesEachRuleForItsOwnReason() {

		assertGridRejected("[[4,0],[3,3,1]]", "must be at least 1");
		assertGridRejected("[[4,-6],[3,3,1]]", "must be at least 1");
		assertGridRejected("[0,[3,3,1]]", "must be at least 1");
		assertGridRejected("[[[4,0],6],[3,3,1]]", "must be at least 1");
		assertGridRejected("[[[0,2],6],[3,3,1]]", "must be at least 1");
		assertGridRejected("[[4,6.5],[3,3,1]]", "is not an integer");
		assertGridRejected("[[4,\"6\"],[3,3,1]]", "is not an integer");
		assertGridRejected("[[4,true],[3,3,1]]", "is not an integer");
		assertGridRejected("[[4,null],[3,3,1]]", "is not an integer");
		assertGridRejected("[[],[3,3,1]]", "integer or a non-empty array");
		assertGridRejected("[{\"a\":1},[3,3,1]]", "integer or a non-empty array");
		assertGridRejected("[[[4,2,1],6],[3,3,1]]", "[value, count]");
		assertGridRejected("[[[4]],[3,3,1]]", "[value, count]");
		assertGridRejected("[[[[4,2]]],[3,3,1]]", "[value, count]");
		assertGridRejected("[[4,99999999999],[3,3,1]]", "too long");
		assertGridRejected("[[[4,9223372036854775807],[4,9223372036854775807]],[3]]", "too many or too long");
		assertGridRejected("{\"a\":1}", "chunk_shapes is not an array");
		assertGridRejected("[[4,5],[3,3,1]]", "sum to 9", 10, 7);
		assertGridRejected("[[4,6],[3,3]]", "sum to 6", 10, 7);
		assertGridRejected("[[4,6]]", "has 1 axes, the array has 2", 10, 7);
		assertGridRejected("[[4,6],[3,3,1],[2]]", "has 3 axes, the array has 2", 10, 7);
	}

	@Test
	public void theExtensionsOwnExampleExpandsAsItSays() {

		// the example of the extension's README: one axis of each kind, in the order of the JSON
		final JsonElement zarr = JsonParser.parseString("[4, [1, 2, 3], [[4, 2]], [[1, 3], 3], [4, 4, 4]]");
		final org.janelia.saalfeldlab.n5.zarr.chunks.RectilinearChunkGrid grid =
				org.janelia.saalfeldlab.n5.zarr.chunks.RectilinearChunkGrid.fromZarr(zarr);
		// "expands to [4, 4]" and so on, which N5 has in the reverse order
		final int[][] expansion = {{4, 4, 4, 4}, {1, 2, 3}, {4, 4}, {1, 1, 1, 3}, {4, 4, 4}};
		final int nd = expansion.length;
		assertEquals(nd, grid.numDimensions());
		final int[] shape = grid.getShape();
		for (int a = 0; a < nd; a++) {
			final int[] edges = expansion[a];
			final int d = nd - 1 - a;
			int gcd = 0;
			for (final int e : edges)
				gcd = gcd(gcd, e);
			assertEquals("the block size of the zarr axis " + a, gcd, shape[d]);
			int start = 0;
			for (int c = 0; c < edges.length; c++) {
				final long[] chunk = new long[nd];
				chunk[d] = c;
				assertEquals("edge " + c + " of the zarr axis " + a, edges[c], grid.chunkShape(chunk)[d]);
				assertEquals("start of chunk " + c + " of the zarr axis " + a, start, grid.chunkStart(chunk)[d]);
				// every block in the chunk is in that chunk
				for (int p = start; p < start + edges[c]; p += gcd) {
					final long[] block = new long[nd];
					block[d] = p / gcd;
					assertEquals("block at " + p + " of the zarr axis " + a, c, grid.toChunkPosition(block)[d]);
				}
				start += edges[c];
			}
		}
		// the extension's own example for sizes: with a length of 10 and a step of 3, the chunks are 3, 3, 3, 3
		final org.janelia.saalfeldlab.n5.zarr.chunks.RectilinearChunkGrid regular =
				org.janelia.saalfeldlab.n5.zarr.chunks.RectilinearChunkGrid.fromZarr(JsonParser.parseString("[3]"));
		assertArrayEquals(new int[]{3}, regular.getShape());
		assertEquals(3, regular.toChunkPosition(new long[]{3})[0]);
		regular.validate(new long[]{10}); // the grid covers it: no end that the array could pass
	}

	private static int gcd(final int a, final int b) {

		return b == 0 ? a : gcd(b, a % b);
	}

	@Test
	public void runLengthPairsDoNotExpand() {

		// a billion chunks of one element: held as one run, looked up by search
		final org.janelia.saalfeldlab.n5.zarr.chunks.RectilinearChunkGrid grid =
				org.janelia.saalfeldlab.n5.zarr.chunks.RectilinearChunkGrid.fromZarr(JsonParser.parseString("[[[1, 1000000000], 7]]"));
		assertArrayEquals(new int[]{1}, grid.getShape());
		assertEquals(999_999_999L, grid.toChunkPosition(new long[]{999_999_999L})[0]);
		assertEquals(1_000_000_000L, grid.toChunkPosition(new long[]{1_000_000_000L})[0]);
		assertEquals(7, grid.chunkShape(new long[]{1_000_000_000L})[0]);
		assertEquals(1_000_000_000L, grid.chunkStart(new long[]{1_000_000_000L})[0]);
		grid.validate(new long[]{1_000_000_006L});
		try {
			grid.chunkStart(new long[]{1_000_000_001L});
			fail("there is no chunk past the last one");
		} catch (final IndexOutOfBoundsException expected) {
			// the edges end where the array's chunks do
		}
	}
}
