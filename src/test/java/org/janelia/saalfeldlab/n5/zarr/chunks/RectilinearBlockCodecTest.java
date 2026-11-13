package org.janelia.saalfeldlab.n5.zarr.chunks;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.codec.BlockCodec;
import org.janelia.saalfeldlab.n5.codec.BlockCodecInfo;
import org.janelia.saalfeldlab.n5.codec.DataCodecInfo;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.zarr.chunks.RectilinearBlockCodecInfo.RectilinearBlockCodec;
import org.junit.Test;

import com.google.gson.JsonParser;

/**
 * The codec that reads blocks out of the chunks of a rectilinear grid, on a stand-in for the codec of the array that counts
 * its decodes and makes each chunk's elements from their positions in the array.
 */
public class RectilinearBlockCodecTest {

	/** Rows are cut in 4 and 6, columns in 3, 3 and 1: six chunks, and blocks of 2 rows and 1 column. */
	private static final RectilinearChunkGrid GRID = RectilinearChunkGrid.fromZarr(JsonParser.parseString("[[4,6],[[3,2],1]]"));

	private static final int[] BLOCK = {1, 2};

	private static final long BLOCKS_X = 7, BLOCKS_Y = 5;

	private static int element(final long x, final long y) {

		return (int)(y * 1000 + x);
	}

	/** Decodes a chunk to a block of its shape, whose elements are {@link #element} of their positions in the array. */
	private static class Counting implements BlockCodecInfo {

		final AtomicInteger decodes = new AtomicInteger();

		/** Decodes that fail before one succeeds. */
		final AtomicInteger failures = new AtomicInteger();

		@Override
		public String getType() {

			return "counting";
		}

		@Override
		public <T> BlockCodec<T> create(final DataType dataType, final int[] chunkShape, final DataCodecInfo... codecs) {

			return new BlockCodec<T>() {

				@Override
				public ReadData encode(final DataBlock<T> dataBlock) {

					throw new UnsupportedOperationException();
				}

				@SuppressWarnings("unchecked")
				@Override
				public DataBlock<T> decode(final ReadData readData, final long[] chunkPosition) {

					decodes.incrementAndGet();
					if (failures.getAndDecrement() > 0)
						throw new N5Exception.N5IOException("the stand-in is told to fail");
					final long[] start = GRID.chunkStart(chunkPosition);
					final DataBlock<T> chunk = (DataBlock<T>)dataType.createDataBlock(chunkShape, chunkPosition);
					final int[] data = (int[])chunk.getData();
					for (int y = 0; y < chunkShape[1]; y++)
						for (int x = 0; x < chunkShape[0]; x++)
							data[x + y * chunkShape[0]] = element(start[0] + x, start[1] + y);
					return chunk;
				}
			};
		}
	}

	private static RectilinearBlockCodec<int[]> codec(final Counting inner, final long cacheBytes) {

		return new RectilinearBlockCodec<>(DataType.INT32, BLOCK, inner, GRID, new DataCodecInfo[0], cacheBytes);
	}

	private static ReadData noRead() {

		return ReadData.from(new byte[0]);
	}

	/** A {@link ReadData} that fails if anything looks at it: a chunk that was decoded does not have its bytes read again. */
	private static ReadData untouchable() {

		return (ReadData)Proxy.newProxyInstance(ReadData.class.getClassLoader(), new Class<?>[]{ReadData.class},
				(proxy, method, args) -> {
					throw new AssertionError("the bytes of a decoded chunk were read again: " + method.getName());
				});
	}

	private static void assertBlock(final DataBlock<int[]> block, final long bx, final long by) {

		assertArrayEquals(new long[]{bx, by}, block.getGridPosition());
		assertArrayEquals(BLOCK, block.getSize());
		assertArrayEquals("block " + bx + "," + by, new int[]{element(bx, by * 2), element(bx, by * 2 + 1)}, block.getData());
	}

	@Test
	public void decodesEachChunkOnceForAllItsBlocks() {

		final Counting inner = new Counting();
		final BlockCodec<int[]> codec = codec(inner, 1L << 20);
		for (long by = 0; by < BLOCKS_Y; by++)
			for (long bx = 0; bx < BLOCKS_X; bx++)
				assertBlock(codec.decode(noRead(), new long[]{bx, by}), bx, by);
		assertEquals("2 chunk rows by 3 chunk columns", 6, inner.decodes.get());
	}

	@Test
	public void laterBlocksOfAChunkDoNotReadItAgain() {

		final Counting inner = new Counting();
		final BlockCodec<int[]> codec = codec(inner, 1L << 20);
		assertBlock(codec.decode(noRead(), new long[]{0, 0}), 0, 0);
		// the rest of the chunk: its bytes would be fetched if the read data were touched
		assertBlock(codec.decode(untouchable(), new long[]{1, 0}), 1, 0);
		assertBlock(codec.decode(untouchable(), new long[]{2, 1}), 2, 1);
		assertEquals(1, inner.decodes.get());
	}

	@Test
	public void threadsAskingForBlocksOfOneChunkDecodeItOnce() throws Exception {

		final Counting inner = new Counting();
		final BlockCodec<int[]> codec = codec(inner, 1L << 20);
		final List<long[]> positions = new ArrayList<>();
		for (long by = 0; by < BLOCKS_Y; by++)
			for (long bx = 0; bx < BLOCKS_X; bx++)
				positions.add(new long[]{bx, by});

		final int threads = 16;
		final ExecutorService pool = Executors.newFixedThreadPool(threads);
		try {
			final List<Future<?>> done = new ArrayList<>();
			for (int t = 0; t < threads; t++) {
				final List<long[]> mine = new ArrayList<>(positions);
				Collections.shuffle(mine, new java.util.Random(t));
				done.add(pool.submit(() -> {
					for (final long[] p : mine)
						assertBlock(codec.decode(noRead(), p), p[0], p[1]);
				}));
			}
			for (final Future<?> f : done)
				f.get();
		} finally {
			pool.shutdownNow();
		}
		assertEquals("16 threads over 35 blocks of 6 chunks", 6, inner.decodes.get());
	}

	@Test
	public void keepsOnlyAsMuchAsItsBudgetHolds() {

		final Counting inner = new Counting();
		// a budget of one byte: only the chunk just decoded is kept
		final BlockCodec<int[]> codec = codec(inner, 1);
		final long[][] chunkA = {{0, 0}, {1, 0}}, chunkB = {{3, 0}, {4, 1}};
		for (final long[] p : chunkA)
			assertBlock(codec.decode(noRead(), p), p[0], p[1]);
		assertEquals(1, inner.decodes.get());
		for (final long[] p : chunkB)
			assertBlock(codec.decode(noRead(), p), p[0], p[1]);
		assertEquals("a second chunk, which pushed the first out", 2, inner.decodes.get());
		for (final long[] p : chunkA)
			assertBlock(codec.decode(noRead(), p), p[0], p[1]);
		assertEquals("the first again: decoded again", 3, inner.decodes.get());
	}

	@Test
	public void aFailedDecodeIsNotKept() {

		final Counting inner = new Counting();
		inner.failures.set(1);
		final BlockCodec<int[]> codec = codec(inner, 1L << 20);
		try {
			codec.decode(noRead(), new long[]{0, 0});
			fail("the stand-in was told to fail");
		} catch (final N5Exception.N5IOException expected) {
			// the failure reaches the reader
		}
		assertBlock(codec.decode(noRead(), new long[]{0, 0}), 0, 0);
		assertEquals("the next read tries again", 2, inner.decodes.get());
	}

	@Test
	public void aBlockIsNotWrittenOnItsOwn() {

		final BlockCodec<int[]> codec = codec(new Counting(), 1L << 20);
		try {
			codec.encode(codec.decode(noRead(), new long[]{0, 0}));
			fail("a block is part of a chunk");
		} catch (final UnsupportedOperationException expected) {
			// the chunk's other blocks would be lost
		}
	}

	@Test
	public void shardingOnARectilinearGridIsRefused() {

		final BlockCodecInfo shard = new org.janelia.saalfeldlab.n5.shard.DefaultShardCodecInfo(
				new int[]{1, 1}, new Counting(), new DataCodecInfo[0], new Counting(), new DataCodecInfo[0],
				org.janelia.saalfeldlab.n5.shard.ShardIndex.IndexLocation.END);
		try {
			new RectilinearBlockCodecInfo(shard, GRID);
			fail("the shards of a rectilinear grid would not be a whole number of blocks");
		} catch (final UnsupportedOperationException expected) {
			// not supported
		}
	}
}
