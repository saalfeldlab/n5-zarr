package org.janelia.saalfeldlab.n5.zarr.chunks;

import org.janelia.saalfeldlab.n5.serialization.NameConfig;

@NameConfig.Prefix("chunk_key_encoding")
public interface ChunkKeyEncoding  {

	public  String getChunkPath(final long[] gridPosition);
}
