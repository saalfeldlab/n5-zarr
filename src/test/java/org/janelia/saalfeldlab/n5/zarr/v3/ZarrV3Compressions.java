package org.janelia.saalfeldlab.n5.zarr.v3;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.janelia.saalfeldlab.n5.GsonUtils;
import org.janelia.saalfeldlab.n5.GzipCompression;
import org.janelia.saalfeldlab.n5.codec.Codec;
import org.janelia.saalfeldlab.n5.zarr.v3.ZarrV3Compressor.Blosc;
import org.junit.Before;
import org.junit.Test;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class ZarrV3Compressions {
	

	private Gson gson;

	@Before
	public void before() {

		final GsonBuilder gsonBuilder = new GsonBuilder();
		GsonUtils.registerGson(gsonBuilder);
		gson = gsonBuilder.create();
	}

	@Test
	public void testSerializeBloscCompression() {

		final Codec codec = new Blosc("zstd", 5, "shuffle", 0, 1, 1);
		final JsonObject jsonCodec = gson.toJsonTree(codec).getAsJsonObject();
		final JsonObject expected = gson.fromJson(
				"{\"name\":\"blosc\",\"configuration\":{\"clevel\":5,\"blocksize\":0,\"typesize\":1,\"cname\":\"zstd\",\"shuffle\":\"shuffle\"}}",
				JsonElement.class).getAsJsonObject();

		assertEquals("blosc codec", expected, jsonCodec.getAsJsonObject());

		final Codec codecsDeserialized = gson.fromJson(expected.toString(), Codec.class);
		assertTrue("codec not blosc", codecsDeserialized instanceof Blosc);
	}

	@Test
	public void testSerializeGzipCompression() {


		Codec[] codecs = new Codec[]{
				new GzipCompression()
		};

		final String jsonCodecArrayString = gson.toJsonTree(codecs).getAsJsonArray().toString();
		System.out.println(jsonCodecArrayString);

//		JsonElement expected = gson.fromJson(
//				"[{\"name\":\"astype\",\"configuration\":{\"dataType\":\"float64\",\"encodedType\":\"int16\"}},{\"name\":\"gzip\",\"configuration\":{\"level\":-1,\"useZlib\":false}}]",
//				JsonElement.class);
//		assertEquals("codec array", expected, jsonCodecArray.getAsJsonArray());

		final Codec[] codecsDeserialized = gson.fromJson(jsonCodecArrayString, Codec[].class);
		assertEquals("codecs length not 1", 1, codecsDeserialized.length);
		assertTrue("codec not gzip", codecsDeserialized[0] instanceof GzipCompression);
	}

	@Test
	public void testSerializeZstandardCompression() {

		final Codec codec = new ZarrV3Compressor.Zstandard();
		final JsonElement serialized = gson.toJsonTree(codec).getAsJsonObject();
		final JsonElement expected = gson.fromJson( "{\n"
				+ "    \"configuration\": {\n"
				+ "        \"level\": 5,\n"
				+ "        \"checksum\": true\n"
				+ "    },\n"
				+ "    \"name\": \"zstd\"\n"
				+ "}", JsonElement.class).getAsJsonObject();

		assertEquals(expected, serialized);
	}

	@Test
	public void testDeserializeZstandardCompression() {

		JsonElement expected = gson.fromJson( "{\n"
						+ "    \"configuration\": {\n"
						+ "        \"level\": 5,\n"
						+ "        \"checksum\": true\n"
						+ "    },\n"
						+ "    \"name\": \"zstd\"\n"
						+ "}",
				JsonElement.class);

		final Codec codecsDeserialized = gson.fromJson(expected, Codec.class);
		assertTrue("codec not zstd", codecsDeserialized instanceof ZarrV3Compressor.Zstandard );
	}
}
