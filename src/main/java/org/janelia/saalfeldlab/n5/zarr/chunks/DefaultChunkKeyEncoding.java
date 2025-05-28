package org.janelia.saalfeldlab.n5.zarr.chunks;

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

import org.janelia.saalfeldlab.n5.serialization.NameConfig;

@NameConfig.Name("default")
public class DefaultChunkKeyEncoding implements ChunkKeyEncoding {

	private static final long serialVersionUID = 2215709434854968911L;

	public static Set<String> VALID_SEPARATORS = Collections.unmodifiableSet(
			Arrays.stream(new String[]{".", "/"}).collect(Collectors.toSet())
	);

	@NameConfig.Parameter(optional = true)
	final String separator;

	protected DefaultChunkKeyEncoding() {

		this.separator = "/";
	}

	public DefaultChunkKeyEncoding(final String separator) {

		assert (VALID_SEPARATORS.contains(separator));
		this.separator = separator;
	}

	public String getSeparator() {

		return separator;
	}

	@Override
	public String getChunkPath(final long[] gridPosition) {

		final StringBuilder pathStringBuilder = new StringBuilder();
		pathStringBuilder.append("c");
		pathStringBuilder.append(getSeparator());
		pathStringBuilder.append(gridPosition[gridPosition.length - 1]);
		for (int i = gridPosition.length - 2; i >= 0; --i) {
			pathStringBuilder.append(getSeparator());
			pathStringBuilder.append(gridPosition[i]);
		}

		return pathStringBuilder.toString();
	}

	@Override public String toString() {

		return String.format("%s[separator=%s]", getClass().getSimpleName(), getSeparator());
	}
}
