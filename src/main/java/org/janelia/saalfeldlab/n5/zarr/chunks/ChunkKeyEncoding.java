package org.janelia.saalfeldlab.n5.zarr.chunks;

import org.janelia.saalfeldlab.n5.zarr.serialization.ZarrNameConfig;

@ZarrNameConfig.Prefix("chunk_key_encoding")
public abstract class ChunkKeyEncoding implements ZarrNameConfig {

	public abstract String getChunkPath(final long[] gridPosition);
}
