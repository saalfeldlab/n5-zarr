package org.janelia.saalfeldlab.n5.zarr.chunks;

import java.util.Arrays;

@ChunkGrid.Name("regular")
public class RegularChunkGrid extends ChunkGrid {

	@ReverseArray
	@ChunkGrid.Parameter(value = "chunk_shape")
	private final int[] shape;

	protected RegularChunkGrid() {

		shape = null;
	}

	public RegularChunkGrid(final int[] shape) {

		this.shape = shape;
	}

	@Override
	public int[] getShape() {

		return shape;
	}

	@Override
	public String toString() {

		return String.format("%s[shape=%s]", getClass().getSimpleName(), Arrays.toString(getShape()));
	}
}
