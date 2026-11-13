package org.janelia.saalfeldlab.n5.zarr.chunks;

import org.janelia.saalfeldlab.n5.serialization.NameConfig;

@NameConfig.Prefix("chunk-grid")
public interface ChunkGrid {

	/**
	 * The shape of the blocks N5 sees, which is the chunk shape of a regular grid.
	 *
	 * @return the block shape
	 */
	public abstract int[] getShape();

	/**
	 * The position, on the chunk grid, of the chunk that holds the block at {@code blockPosition}
	 * on the grid of {@link #getShape() blocks}. The two are the same for a regular grid; a grid
	 * whose chunks are larger than its blocks (a rectilinear grid, whose blocks are the largest
	 * ones every chunk is a whole number of) maps one to the other.
	 *
	 * @param blockPosition a position on the block grid
	 * @return the position of the chunk that holds the block, which names its key
	 */
	public default long[] toChunkPosition(final long[] blockPosition) {
		return blockPosition;
	}
}