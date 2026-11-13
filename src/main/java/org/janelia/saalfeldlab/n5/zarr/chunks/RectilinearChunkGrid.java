package org.janelia.saalfeldlab.n5.zarr.chunks;

import java.util.Arrays;

import org.janelia.saalfeldlab.n5.serialization.N5Annotations;
import org.janelia.saalfeldlab.n5.serialization.NameConfig;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;

/**
 * The rectilinear chunk grid of
 * <a href="https://github.com/zarr-developers/zarr-extensions/tree/main/chunk-grids/rectilinear">zarr-extensions</a>:
 * a grid whose chunk edge lengths vary along each axis, given per axis as
 * <ul>
 * <li>an integer {@code m}, the step of a regular grid on that axis;</li>
 * <li>a non-empty array of edge lengths, each an integer or a run-length pair {@code [value, count]}.</li>
 * </ul>
 * <p>
 * N5 describes a dataset by one block size, so this grid presents N5 with a regular grid of the
 * <em>largest blocks that every chunk is a whole number of</em>: on each axis, the greatest common
 * divisor of the edge lengths ({@link #getShape()}). Such a block lies inside one chunk, whose key
 * {@link #toChunkPosition(long[])} gives. {@code RectilinearBlockCodecInfo} reads a block out of
 * its chunk.
 * <p>
 * The edge lengths are kept as runs, so a pair like {@code [1, 1000000000]} costs one entry
 * and a lookup is a binary search.
 * <p>
 * In memory the axes are in N5's order, the fastest first, the reverse of the order of the JSON,
 * as {@link RegularChunkGrid} has them.
 */
@NameConfig.Name("rectilinear")
public class RectilinearChunkGrid implements ChunkGrid {

	public static final String KIND_INLINE = "inline";

	@NameConfig.Parameter(value = "kind")
	private final String kind;

	/** One entry for each axis, in N5's order: the JSON of the extension's {@code chunk_shapes}, reversed. */
	@N5Annotations.ReverseArray
	@NameConfig.Parameter(value = "chunk_shapes")
	private final JsonElement chunkShapes;

	private transient volatile Axis[] axes;

	private transient volatile int[] shape;

	protected RectilinearChunkGrid() {

		kind = null;
		chunkShapes = null;
	}

	/**
	 * @param chunkShapes
	 *            the extension's {@code chunk_shapes} with its axes in <em>N5's</em> order: see
	 *            {@link #fromZarr(JsonElement)} for the order of the JSON
	 */
	public RectilinearChunkGrid(final JsonElement chunkShapes) {

		this.kind = KIND_INLINE;
		this.chunkShapes = chunkShapes;
		axes(); // fail now, not at the first read
	}

	/**
	 * @param chunkShapes
	 *            the extension's {@code chunk_shapes}, one entry for each axis in the order of the
	 *            array's shape
	 * @return the grid, its axes reversed to N5's order
	 */
	public static RectilinearChunkGrid fromZarr(final JsonElement chunkShapes) {

		if (!chunkShapes.isJsonArray())
			throw new JsonParseException("chunk_shapes is not an array");
		final JsonArray reversed = new JsonArray();
		final JsonArray in = chunkShapes.getAsJsonArray();
		for (int i = in.size() - 1; i >= 0; i--)
			reversed.add(in.get(i));
		return new RectilinearChunkGrid(reversed);
	}

	public int numDimensions() {

		return axes().length;
	}

	/**
	 * @return on each axis the greatest common divisor of the edge lengths: the block shape N5 sees
	 */
	@Override
	public int[] getShape() {

		int[] s = shape;
		if (s == null) {
			final Axis[] a = axes();
			s = new int[a.length];
			for (int d = 0; d < a.length; d++)
				s[d] = a[d].gcd;
			shape = s;
		}
		return s.clone();
	}

	@Override
	public long[] toChunkPosition(final long[] blockPosition) {

		final Axis[] a = axes();
		final long[] chunkPosition = new long[a.length];
		for (int d = 0; d < a.length; d++)
			chunkPosition[d] = a[d].chunkIndexAt(blockPosition[d] * a[d].gcd);
		return chunkPosition;
	}

	/**
	 * @param chunkPosition
	 *            the position of a chunk on the chunk grid
	 * @return the shape of that chunk
	 */
	public int[] chunkShape(final long[] chunkPosition) {

		final Axis[] a = axes();
		final int[] chunkShape = new int[a.length];
		for (int d = 0; d < a.length; d++)
			chunkShape[d] = a[d].edgeLength(chunkPosition[d]);
		return chunkShape;
	}

	/**
	 * @param chunkPosition
	 *            the position of a chunk on the chunk grid
	 * @return the position, in the array, of the first element of that chunk
	 */
	public long[] chunkStart(final long[] chunkPosition) {

		final Axis[] a = axes();
		final long[] start = new long[a.length];
		for (int d = 0; d < a.length; d++)
			start[d] = a[d].start(chunkPosition[d]);
		return start;
	}

	/**
	 * Checks the grid against the shape of the array it chunks: the extension requires the edge
	 * lengths of an axis to sum to at least its length.
	 *
	 * @param dimensions
	 *            the shape of the array, in N5's order
	 * @throws JsonParseException
	 *             if the grid does not cover the array
	 */
	public void validate(final long[] dimensions) {

		final Axis[] a = axes();
		if (a.length != dimensions.length)
			throw new JsonParseException(
					"chunk_shapes has " + a.length + " axes, the array has " + dimensions.length);
		for (int d = 0; d < a.length; d++)
			if (a[d].end < dimensions[d])
				throw new JsonParseException("the chunk edge lengths of an axis sum to " + a[d].end
						+ ", short of the axis's " + dimensions[d]);
	}

	private Axis[] axes() {

		Axis[] a = axes;
		if (a == null) {
			if (kind != null && !KIND_INLINE.equals(kind))
				throw new JsonParseException("the kind of a rectilinear chunk grid is \"inline\", not \"" + kind + "\"");
			if (chunkShapes == null || !chunkShapes.isJsonArray())
				throw new JsonParseException("chunk_shapes is not an array");
			final JsonArray in = chunkShapes.getAsJsonArray();
			a = new Axis[in.size()];
			for (int d = 0; d < a.length; d++)
				a[d] = Axis.parse(in.get(d));
			axes = a;
		}
		return a;
	}

	@Override
	public String toString() {

		return String.format("%s[%s]", getClass().getSimpleName(), chunkShapes);
	}

	/** The chunk edge lengths of one axis, as runs of equal edges. */
	private static final class Axis {

		/** The edge length of each run. */
		private final int[] runEdge;

		/** The number of chunks in each run. */
		private final long[] runCount;

		/** The index of the first chunk of each run. */
		private final long[] runFirstChunk;

		/** The position, in the array, of the start of each run. */
		private final long[] runStart;

		/** The sum of the edge lengths. */
		private final long end;

		private final int gcd;

		private Axis(final int[] edge, final long[] count) {

			runEdge = edge;
			runCount = count;
			runFirstChunk = new long[edge.length];
			runStart = new long[edge.length];
			long chunks = 0, position = 0;
			int g = 0;
			try {
				for (int i = 0; i < edge.length; i++) {
					runFirstChunk[i] = chunks;
					runStart[i] = position;
					chunks = Math.addExact(chunks, count[i]);
					position = Math.addExact(position, Math.multiplyExact(count[i], (long)edge[i]));
					g = gcd(g, edge[i]);
				}
			} catch (final ArithmeticException e) {
				throw new JsonParseException("the chunk edge lengths of an axis are too many or too long");
			}
			end = position;
			gcd = g;
		}

		private static int gcd(int a, int b) {

			while (b != 0) {
				final int t = a % b;
				a = b;
				b = t;
			}
			return a;
		}

		/**
		 * An integer is the step of a regular grid: its runs go on past any array length. An array is
		 * a list of edge lengths and run-length pairs.
		 */
		static Axis parse(final JsonElement e) {

			if (e.isJsonPrimitive()) {
				final int step = edge(e);
				// as many chunks of the step as fit before the sum of the edges would overflow a long
				return new Axis(new int[]{step}, new long[]{Long.MAX_VALUE / step});
			}
			if (!e.isJsonArray() || e.getAsJsonArray().size() == 0)
				throw new JsonParseException("the edge lengths of an axis are an integer or a non-empty array");
			final JsonArray list = e.getAsJsonArray();
			final int[] edge = new int[list.size()];
			final long[] count = new long[list.size()];
			for (int i = 0; i < edge.length; i++) {
				final JsonElement item = list.get(i);
				if (item.isJsonArray()) {
					final JsonArray pair = item.getAsJsonArray();
					if (pair.size() != 2)
						throw new JsonParseException("a run-length pair is [value, count]");
					edge[i] = edge(pair.get(0));
					count[i] = positiveLong(pair.get(1), "the count of a run");
				} else {
					edge[i] = edge(item);
					count[i] = 1;
				}
			}
			return new Axis(edge, count);
		}

		private static int edge(final JsonElement e) {

			final long v = positiveLong(e, "a chunk edge length");
			if (v > Integer.MAX_VALUE)
				throw new JsonParseException("a chunk edge length of " + v + " is too long");
			return (int)v;
		}

		private static long positiveLong(final JsonElement e, final String what) {

			if (!e.isJsonPrimitive() || !((JsonPrimitive)e).isNumber())
				throw new JsonParseException(what + " is not an integer: " + e);
			final java.math.BigDecimal d = e.getAsBigDecimal();
			final long v;
			try {
				v = d.longValueExact();
			} catch (final ArithmeticException x) {
				throw new JsonParseException(what + " is not an integer: " + e);
			}
			if (v < 1)
				throw new JsonParseException(what + " is " + v + ", and must be at least 1");
			return v;
		}

		/** The run that holds the position, in the array, of an element. */
		private int runAtPosition(final long position) {

			if (position < 0 || position >= end)
				throw new IndexOutOfBoundsException(
						"the position " + position + " is outside the chunk edge lengths, which sum to " + end);
			final int i = Arrays.binarySearch(runStart, position);
			return i >= 0 ? i : -i - 2;
		}

		private int runAtChunk(final long chunk) {

			if (chunk < 0)
				throw new IndexOutOfBoundsException("the chunk position " + chunk);
			final int i = Arrays.binarySearch(runFirstChunk, chunk);
			final int run = i >= 0 ? i : -i - 2;
			if (run < 0 || chunk - runFirstChunk[run] >= runCount[run])
				throw new IndexOutOfBoundsException("the chunk position " + chunk + " is past the last chunk");
			return run;
		}

		long chunkIndexAt(final long position) {

			final int run = runAtPosition(position);
			return runFirstChunk[run] + (position - runStart[run]) / runEdge[run];
		}

		int edgeLength(final long chunk) {

			return runEdge[runAtChunk(chunk)];
		}

		long start(final long chunk) {

			final int run = runAtChunk(chunk);
			return runStart[run] + (chunk - runFirstChunk[run]) * runEdge[run];
		}
	}
}
