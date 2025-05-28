package org.janelia.saalfeldlab.n5.zarr.chunks;

import org.janelia.saalfeldlab.n5.serialization.N5Annotations;
import org.janelia.saalfeldlab.n5.serialization.NameConfig;

import java.util.Arrays;

@NameConfig.Name("regular")
public class RegularChunkGrid implements ChunkGrid {

	@N5Annotations.ReverseArray
	@NameConfig.Parameter(value = "chunk_shape")
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
