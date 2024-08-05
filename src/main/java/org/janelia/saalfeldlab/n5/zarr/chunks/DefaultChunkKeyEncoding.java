package org.janelia.saalfeldlab.n5.zarr.chunks;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;

public class DefaultChunkKeyEncoding extends AbstractChunkKeyEncoding {

	public DefaultChunkKeyEncoding(final String separator) {

		super("default", "c", separator, true);
	}

	@Override
	public String getChunkPath(final long[] gridPosition) {

		final StringBuilder pathStringBuilder = new StringBuilder();
		pathStringBuilder.append(prefix);
		pathStringBuilder.append(gridPosition[gridPosition.length - 1]);
		for (int i = gridPosition.length - 2; i >= 0; --i) {
			pathStringBuilder.append(separator);
			pathStringBuilder.append(gridPosition[i]);
		}

		return pathStringBuilder.toString();
	}

	/**
	 * Quick and dirty, not meant to stick around for long. Please make this better, more extensible, and safer.
	 *
	 * @param json
	 *            a json element
	 * @return the chunk key encoding
	 */
	public static DefaultChunkKeyEncoding deserialize(final JsonDeserializationContext context,
			final JsonElement json) {

		if (!json.isJsonObject())
			return null;

		final JsonObject jsonObj = json.getAsJsonObject();
		final String name = jsonObj.get("name").getAsString();
		if (!name.equals("default"))
			return null;

		final String separator = jsonObj.get("configuration").getAsJsonObject().get("separator")
				.getAsString();
		return new DefaultChunkKeyEncoding(separator);
	}

	public static JsonObject serialize(JsonSerializationContext context,
			final DefaultChunkKeyEncoding cke) {

		final JsonObject out = new JsonObject();
		out.addProperty("name", "default");

		final JsonObject config = new JsonObject();
		config.addProperty("separator", cke.separator);
		out.add("configuration", config);

		return out;
	}

}
