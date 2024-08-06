package org.janelia.saalfeldlab.n5.zarr.chunks;

@ChunkKeyEncoding.Name("v2")
public class V2ChunkKeyEncoding extends DefaultChunkKeyEncoding {

	private V2ChunkKeyEncoding() {
		super();
	}

	public V2ChunkKeyEncoding(final String separator) {

		super(separator);
	}
}
