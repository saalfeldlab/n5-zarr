package org.janelia.saalfeldlab.n5.zarr.chunks;

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

public interface ChunkKeyEncoding {

	public static Set<String> VALID_SEPARATORS = Collections.unmodifiableSet(
			Arrays.stream(new String[]{".", "/"}).collect(Collectors.toSet()));

	public String getChunkPath(final long[] gridPosition);
}
