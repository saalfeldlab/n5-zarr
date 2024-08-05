package org.janelia.saalfeldlab.n5.zarr.chunks;

import org.janelia.saalfeldlab.n5.zarr.serialization.ZarrNameConfig;

public interface ChunkKeyEncoding extends ZarrNameConfig {

	String getChunkPath(final long[] gridPosition);
}
