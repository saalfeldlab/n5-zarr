package org.janelia.saalfeldlab.n5.zarr.chunks;

public abstract class AbstractChunkKeyEncoding implements ChunkKeyEncoding {

	public final String name;

	public final String prefix;

	public final String separator;

	public final boolean isRowMajor;

	public AbstractChunkKeyEncoding(final String name, final String prefix, final String separator,
			final boolean isRowMajor) {

		assert (VALID_SEPARATORS.contains(separator));
		this.name = name;
		this.prefix = prefix;
		this.separator = separator;
		this.isRowMajor = isRowMajor;
	}

}
