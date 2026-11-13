package org.janelia.saalfeldlab.n5.zarr.chunks;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.stream.Collectors;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.codec.BlockCodec;
import org.janelia.saalfeldlab.n5.codec.BlockCodecInfo;
import org.janelia.saalfeldlab.n5.codec.DataCodecInfo;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.shard.ShardCodecInfo;

/**
 * Reads the blocks of an array on a {@link RectilinearChunkGrid}: the block codec of the array
 * decodes a stored <em>chunk</em>, whose shape varies from chunk to chunk, and this reads out of it
 * the block N5 asked for. A block is a piece of one chunk, because the grid's block shape divides
 * every edge length.
 * <p>
 * A decoded chunk is kept (the most recent ones, up to a budget of bytes), and when several
 * threads ask for blocks of one chunk, one decodes it and the others wait: a chunk is decoded once
 * however many blocks are read from it, and the bytes of a chunk are not read again for the
 * second block (the {@link ReadData} of a block is lazy, and the cache hit does not touch it).
 * <p>
 * Reading only: a block is a part of a chunk, so it cannot be written back.
 * <p>
 * The budget is {@value #CACHE_BYTES_PROPERTY} bytes, 256 MiB by default.
 */
public class RectilinearBlockCodecInfo implements BlockCodecInfo {

	private static final long serialVersionUID = 4523965423098542358L;

	public static final String CACHE_BYTES_PROPERTY = "n5.zarr.rectilinear.cache.bytes";

	private static final long DEFAULT_CACHE_BYTES = 256L << 20;

	private final BlockCodecInfo inner;

	private final RectilinearChunkGrid grid;

	/**
	 * @param inner
	 *            the block codec of the array, which decodes a chunk
	 * @param grid
	 *            the array's chunk grid
	 */
	public RectilinearBlockCodecInfo(final BlockCodecInfo inner, final RectilinearChunkGrid grid) {

		if (inner instanceof ShardCodecInfo)
			throw new UnsupportedOperationException("sharding on a rectilinear chunk grid is not supported");
		this.inner = inner;
		this.grid = grid;
	}

	/** @return the codec of the array, which is the one its metadata names */
	public BlockCodecInfo getInner() {

		return inner;
	}

	public RectilinearChunkGrid getGrid() {

		return grid;
	}

	@Override
	public String getType() {

		return inner.getType();
	}

	@Override
	public long[] getKeyPositionForBlock(final DatasetAttributes attributes, final DataBlock<?> datablock) {

		return grid.toChunkPosition(datablock.getGridPosition());
	}

	@Override
	public long[] getKeyPositionForBlock(final DatasetAttributes attributes, final long... blockPosition) {

		return grid.toChunkPosition(blockPosition);
	}

	@Override
	public <T> BlockCodec<T> create(final DataType dataType, final int[] blockSize, final DataCodecInfo... codecs) {

		return new RectilinearBlockCodec<>(dataType, blockSize, inner, grid, codecs, cacheBytes());
	}

	private static long cacheBytes() {

		return Long.getLong(CACHE_BYTES_PROPERTY, DEFAULT_CACHE_BYTES);
	}

	/** The codec of one array: it decodes chunks with the array's codec, and reads blocks out of them. */
	static class RectilinearBlockCodec<T> implements BlockCodec<T> {

		private final DataType dataType;

		private final int[] blockSize;

		private final BlockCodecInfo inner;

		private final RectilinearChunkGrid grid;

		private final DataCodecInfo[] dataCodecs;

		/** The codec of the array, for each chunk shape: it is made for the shape of its chunk. */
		private final Map<List<Integer>, BlockCodec<T>> chunkCodecs = new ConcurrentHashMap<>();

		private final ChunkCache<T> cache;

		RectilinearBlockCodec(final DataType dataType, final int[] blockSize, final BlockCodecInfo inner,
				final RectilinearChunkGrid grid, final DataCodecInfo[] dataCodecs, final long cacheBytes) {

			this.dataType = dataType;
			this.blockSize = blockSize.clone();
			this.inner = inner;
			this.grid = grid;
			this.dataCodecs = dataCodecs;
			this.cache = new ChunkCache<>(cacheBytes);
		}

		@Override
		public ReadData encode(final DataBlock<T> dataBlock) throws N5Exception.N5IOException {

			throw new UnsupportedOperationException(
					"A block of a rectilinear chunk grid is part of a chunk, and cannot be written on its own");
		}

		private BlockCodec<T> codecFor(final int[] chunkShape) {

			final List<Integer> key = Arrays.stream(chunkShape).boxed().collect(Collectors.toList());
			return chunkCodecs.computeIfAbsent(key, k -> inner.create(dataType, chunkShape, dataCodecs));
		}

		@Override
		public DataBlock<T> decode(final ReadData readData, final long[] gridPosition) throws N5Exception.N5IOException {

			final long[] chunkPosition = grid.toChunkPosition(gridPosition);
			final int[] chunkShape = grid.chunkShape(chunkPosition);
			final long[] chunkStart = grid.chunkStart(chunkPosition);
			final DataBlock<T> chunk = cache.get(chunkPosition, () -> codecFor(chunkShape).decode(readData, chunkPosition));
			return blockOf(chunk, gridPosition, chunkStart);
		}

		/** The block at {@code gridPosition}, cut out of the chunk that starts at {@code chunkStart}. */
		@SuppressWarnings("unchecked")
		private DataBlock<T> blockOf(final DataBlock<T> chunk, final long[] gridPosition, final long[] chunkStart) {

			final int nd = blockSize.length;
			final int[] chunkSize = chunk.getSize();
			final long[] offset = new long[nd];
			for (int d = 0; d < nd; d++) {
				offset[d] = gridPosition[d] * blockSize[d] - chunkStart[d];
				if (offset[d] < 0 || offset[d] + blockSize[d] > chunkSize[d])
					throw new N5Exception.N5IOException("the block at " + Arrays.toString(gridPosition) + " is not inside its chunk: it starts "
							+ offset[d] + " into an axis of " + chunkSize[d] + " elements, and is " + blockSize[d] + " long");
			}

			final DataBlock<T> block = (DataBlock<T>)dataType.createDataBlock(blockSize, gridPosition);
			final Object from = chunk.getData(), to = block.getData();

			// the first axis is the fastest: a run of blockSize[0] elements is contiguous in both
			final long[] chunkStride = new long[nd];
			long stride = 1;
			for (int d = 0; d < nd; d++) {
				chunkStride[d] = stride;
				stride *= chunkSize[d];
			}
			final int[] index = new int[nd]; // the position in the block of the run's first element, with index[0] = 0
			int written = 0;
			final int run = blockSize[0];
			final int runs = DataBlock.getNumElements(blockSize) / run;
			for (int r = 0; r < runs; r++) {
				long source = offset[0];
				for (int d = 1; d < nd; d++)
					source += (offset[d] + index[d]) * chunkStride[d];
				System.arraycopy(from, Math.toIntExact(source), to, written, run);
				written += run;
				for (int d = 1; d < nd; d++) { // the next run: carry along the axes after the first
					if (++index[d] < blockSize[d])
						break;
					index[d] = 0;
				}
			}
			return block;
		}
	}

	/**
	 * The most recently used decoded chunks, up to a budget of bytes. Asking for a chunk that is being
	 * decoded waits for that decode, so a chunk is decoded once at a time.
	 */
	static class ChunkCache<T> {

		private final long budget;

		private final LinkedHashMap<List<Long>, FutureTask<DataBlock<T>>> entries = new LinkedHashMap<>(16, 0.75f, true);

		private final Map<List<Long>, Long> sizes = new ConcurrentHashMap<>();

		private long bytes;

		ChunkCache(final long budget) {

			this.budget = budget;
		}

		DataBlock<T> get(final long[] chunkPosition, final Callable<DataBlock<T>> loader) {

			final List<Long> key = Arrays.stream(chunkPosition).boxed().collect(Collectors.toList());
			final FutureTask<DataBlock<T>> task;
			final boolean owner;
			synchronized (this) {
				final FutureTask<DataBlock<T>> existing = entries.get(key);
				owner = existing == null;
				task = owner ? new FutureTask<>(loader) : existing;
				if (owner)
					entries.put(key, task);
			}
			if (owner)
				task.run();

			final DataBlock<T> chunk;
			try {
				chunk = task.get();
			} catch (final InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new N5Exception.N5IOException("interrupted while a chunk was decoded", e);
			} catch (final ExecutionException e) {
				synchronized (this) {
					entries.remove(key, task); // a failed decode is not kept: the next read tries again
				}
				final Throwable cause = e.getCause();
				if (cause instanceof RuntimeException)
					throw (RuntimeException)cause;
				if (cause instanceof Error)
					throw (Error)cause;
				throw new N5Exception.N5IOException("a chunk could not be decoded", cause);
			}

			if (owner)
				account(key, chunk);
			return chunk;
		}

		private synchronized void account(final List<Long> key, final DataBlock<T> chunk) {

			if (!entries.containsKey(key))
				return;
			final long size = estimateBytes(chunk.getData());
			sizes.put(key, size);
			bytes += size;
			// drop the least recently used until the budget holds, but never the chunk just decoded
			final java.util.Iterator<Map.Entry<List<Long>, FutureTask<DataBlock<T>>>> it = entries.entrySet().iterator();
			while (bytes > budget && it.hasNext()) {
				final Map.Entry<List<Long>, FutureTask<DataBlock<T>>> e = it.next();
				if (e.getKey().equals(key))
					continue;
				if (!e.getValue().isDone())
					continue; // still decoding: it is not counted yet
				it.remove();
				final Long dropped = sizes.remove(e.getKey());
				if (dropped != null)
					bytes -= dropped;
			}
		}

		private static long estimateBytes(final Object data) {

			if (data instanceof byte[])
				return ((byte[])data).length;
			if (data instanceof short[])
				return 2L * ((short[])data).length;
			if (data instanceof int[])
				return 4L * ((int[])data).length;
			if (data instanceof float[])
				return 4L * ((float[])data).length;
			if (data instanceof long[])
				return 8L * ((long[])data).length;
			if (data instanceof double[])
				return 8L * ((double[])data).length;
			if (data instanceof Object[])
				return 16L * ((Object[])data).length; // strings: a guess
			return 0;
		}
	}
}
