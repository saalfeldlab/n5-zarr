package org.janelia.saalfeldlab.n5.zarr.chunks;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.io.Serializable;
import java.lang.reflect.Type;

public class ChunkAttributes implements Serializable, JsonSerializer<ChunkAttributes>, JsonDeserializer<ChunkAttributes> {

	private final ChunkGrid grid;

	private final ChunkKeyEncoding keyEncoding;
	private ChunkAttributes() {

		this(null, null);
	}

	public ChunkAttributes(ChunkGrid grid, ChunkKeyEncoding keyEncoding) {

		this.grid = grid;
		this.keyEncoding = keyEncoding;
	}

	public ChunkGrid getGrid() {

		return grid;
	}

	public ChunkKeyEncoding getKeyEncoding() {

		return keyEncoding;
	}

	public String getChunkPath(final long[] gridPosition) {
		return getKeyEncoding().getChunkPath(gridPosition);
	}

	@Override public String toString() {

		return String.format("%s\t%s", grid, keyEncoding);
	}

	public static final String CHUNK_GRID = "chunk_grid";
	public static final String CHUNK_KEY_ENCODING = "chunk_key_encoding";

	private static ChunkAttributes instance = null;

	public static ChunkAttributes getJsonAdapter() {

		if (instance == null)
			instance = new ChunkAttributes();
		return instance;
	}

	@Override public ChunkAttributes deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {

		final JsonObject obj = json.getAsJsonObject();
		if (obj == null)
			return null;

		final JsonElement chunkGridJson = obj.get(CHUNK_GRID);
		final ChunkGrid chunkGrid = context.deserialize(chunkGridJson, ChunkGrid.class);

		final JsonElement chunkKeyEncodingJson = obj.get(CHUNK_KEY_ENCODING);
		final ChunkKeyEncoding chunkKeyEncoding = context.deserialize(chunkKeyEncodingJson, ChunkKeyEncoding.class);
		return new ChunkAttributes(chunkGrid, chunkKeyEncoding);
	}

	@Override public JsonElement serialize(ChunkAttributes src, Type typeOfSrc, JsonSerializationContext context) {

		final JsonObject obj = new JsonObject();
		obj.add(CHUNK_GRID, context.serialize(src.grid));
		obj.add(CHUNK_KEY_ENCODING, context.serialize(src.keyEncoding));
		return obj;
	}
}

