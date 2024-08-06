package org.janelia.saalfeldlab.n5.zarr.chunks;

import org.janelia.saalfeldlab.n5.zarr.serialization.ZarrNameConfig;

@ChunkGrid.Prefix("chunk_grid")
public abstract class ChunkGrid implements ZarrNameConfig {

	public abstract int[] getShape();
}