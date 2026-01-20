package org.janelia.saalfeldlab.n5.zarr.chunks;

import org.janelia.saalfeldlab.n5.serialization.NameConfig;

@NameConfig.Prefix("chunk-grid")
public interface ChunkGrid {

	public abstract int[] getShape();
}