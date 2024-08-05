package org.janelia.saalfeldlab.n5.zarr.chunks;

import java.util.Arrays;

@ChunkGrid.Name("regular")
public class RegularChunkGrid implements ChunkGrid {

	@ChunkGrid.Parameter
	private final Configuration configuration;

	private RegularChunkGrid() {
		this.configuration = null;
	}

	private RegularChunkGrid(final Configuration config) {

		this.configuration = config;
	}

	public RegularChunkGrid(final int[] shape) {

		this.configuration = new Configuration(shape);
	}

	@Override
	public int[] getShape() {
		return configuration.chunk_shape;
	}

	private static class Configuration {

		private final int[] chunk_shape;

		private Configuration(int[] shape) {

			this.chunk_shape = shape;
		}
	}

	@Override public String toString() {

		return String.format("%s[shape=%s]", getClass().getSimpleName(), Arrays.toString(getShape()));
	}
}
