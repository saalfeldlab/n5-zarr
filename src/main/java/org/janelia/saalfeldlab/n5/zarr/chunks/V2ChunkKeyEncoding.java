package org.janelia.saalfeldlab.n5.zarr.chunks;

public class V2ChunkKeyEncoding extends AbstractChunkKeyEncoding {

	public V2ChunkKeyEncoding(final String separator) {

		this(separator, true);
	}

	public V2ChunkKeyEncoding(final String separator, final boolean isRowMajor) {

		// TODO not sure isRowMajor = false will ever happen
		super("v2", "", separator, isRowMajor);
	}

	@Override
	public String getChunkPath(
			final long[] gridPosition) {

		final StringBuilder pathStringBuilder = new StringBuilder();
		if (isRowMajor) {
			pathStringBuilder.append(gridPosition[gridPosition.length - 1]);
			for (int i = gridPosition.length - 2; i >= 0; --i) {
				pathStringBuilder.append(separator);
				pathStringBuilder.append(gridPosition[i]);
			}
		} else {
			pathStringBuilder.append(gridPosition[0]);
			for (int i = 1; i < gridPosition.length; ++i) {
				pathStringBuilder.append(separator);
				pathStringBuilder.append(gridPosition[i]);
			}
		}

		return pathStringBuilder.toString();
	}

}
