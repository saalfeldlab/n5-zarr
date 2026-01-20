package org.janelia.saalfeldlab.n5.zarr.chunks;

import org.janelia.saalfeldlab.n5.serialization.NameConfig;

@NameConfig.Prefix("chunk-key-encoding")
public interface ChunkKeyEncoding  {

	public  String getChunkPath(final long[] gridPosition);
}
