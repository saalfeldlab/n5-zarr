package org.janelia.saalfeldlab.n5.zarr.chunks;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;

public class RegularChunkGrid implements ChunkGrid {

	public static final String CHUNK_SHAPE_KEY = "chunk_shape";

	public static final String REGULAR_CHUNK_GRID_NAME = "regular";

	protected final String name = REGULAR_CHUNK_GRID_NAME;

	public final int[] chunkShape;

	public RegularChunkGrid(final int[] chunkShape) {

		this.chunkShape = chunkShape;
	}

	/**
	 * Quick and dirty, not meant to stick around for long. Please make this better, more extensible, and safer.
	 *
	 * @param json
	 *            a json element
	 * @return the chunk grid
	 */
	public static RegularChunkGrid deserialize(final JsonDeserializationContext context, final JsonElement json) {

		if (!json.isJsonObject())
			return null;

		final JsonObject jsonObj = json.getAsJsonObject();
		final String name = jsonObj.get("name").getAsString();
		if (!name.equals(REGULAR_CHUNK_GRID_NAME))
			return null;

		final JsonElement shapeJson = jsonObj.get("configuration").getAsJsonObject().get(CHUNK_SHAPE_KEY);
		// final int[] chunkShape = gson.fromJson(shapeJson, int[].class);
		final int[] chunkShape = context.deserialize(shapeJson, int[].class);
		return new RegularChunkGrid(chunkShape);
	}

	public static JsonElement serialize(final JsonSerializationContext context, final RegularChunkGrid rcg) {

		final JsonObject out = new JsonObject();
		out.addProperty("name", "regular");

		final JsonObject config = new JsonObject();
		config.add("chunk_Shape", context.serialize(rcg.chunkShape));
		out.add("configuration", config);
		return out;
	}

}
