package org.janelia.saalfeldlab.n5.zarr.chunks;

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

@ChunkKeyEncoding.Name("default")
public class DefaultChunkKeyEncoding implements ChunkKeyEncoding {

	public static Set<String> VALID_SEPARATORS = Collections.unmodifiableSet(
			Arrays.stream(new String[]{".", "/"}).collect(Collectors.toSet())
	);

	@ChunkKeyEncoding.Parameter
	private final Config configuration;

	private DefaultChunkKeyEncoding() {

		this.configuration = null;
	}

	private DefaultChunkKeyEncoding(final Config config) {

		assert (VALID_SEPARATORS.contains(config.separator));
		this.configuration = config;
	}

	public DefaultChunkKeyEncoding(final String separator) {

		this.configuration = new Config(separator);
	}

	public String getSeparator() {

		return configuration.separator;
	}

	private static class Config {

		final String separator;

		private Config(String separator) {

			this.separator = separator;
		}
	}

	@Override
	public String getChunkPath(final long[] gridPosition) {

		final StringBuilder pathStringBuilder = new StringBuilder();
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
