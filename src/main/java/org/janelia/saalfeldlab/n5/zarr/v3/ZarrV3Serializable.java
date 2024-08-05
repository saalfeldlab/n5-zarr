package org.janelia.saalfeldlab.n5.zarr.v3;

public abstract class ZarrV3Serializable<T> {

	public static final String NAME_KEY = "name";
	public static final String CONFIG_KEY = "configuration";

	public final String name;

	public final T configuration;

	public ZarrV3Serializable(final String name, T obj) {

		this.name = name;
		this.configuration = obj;
	}

	// public class RegularChunkGridAdapter
	// implements JsonDeserializer<RegularChunkGrid>, JsonSerializer<RegularChunkGrid> {
	//
	// @Override
	// public JsonElement serialize(RegularChunkGrid src, Type typeOfSrc, JsonSerializationContext context) {
	//
	// final JsonObject config = new JsonObject();
	// config.add(CHUNK_SHAPE_KEY, context.serialize(src.chunkShape));
	//
	// final JsonObject obj = new JsonObject();
	// obj.addProperty(NAME_KEY, src.name);
	// obj.add(CONFIG_KEY, config);
	// return obj;
	// }
	//
	// @Override
	// public RegularChunkGrid deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
	// throws JsonParseException {
	//
	// if (json.isJsonObject()) {
	// final JsonObject obj = json.getAsJsonObject();
	// if (obj.has(NAME_KEY) && obj.has(CONFIG_KEY)) {
	// final int[] chunkShape = context
	// .deserialize(obj.get(CONFIG_KEY).getAsJsonObject().get(CHUNK_SHAPE_KEY), int[].class);
	// return new RegularChunkGrid(obj.get(NAME_KEY).getAsString(), chunkShape);
	// }
	// }
	//
	// return null;
	// }
	// }

}
