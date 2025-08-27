package org.janelia.saalfeldlab.n5.zarr.v3;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.janelia.saalfeldlab.n5.GzipCompression;
import org.janelia.saalfeldlab.n5.codec.CodecInfo;
import org.janelia.saalfeldlab.n5.zarr.v3.ZarrV3Compressor.Blosc;
import org.janelia.scicomp.n5.zstandard.ZstandardCompression;
import org.junit.Before;
import org.junit.Test;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class ZarrV3Compressions {
	

	private Gson gson;

	@Before
	public void before() {

		final GsonBuilder gsonBuilder = new GsonBuilder();
		gson = gsonBuilder.create();
	}

	@Test
	public void testSerializeBloscCompression() {

		final ZarrV3Compressor codec = new Blosc("zstd", 5, "shuffle", 0, 1, 1);
		final JsonObject jsonCodec = gson.toJsonTree(codec).getAsJsonObject();
		final JsonObject expected = gson.fromJson(
				"{\"name\":\"blosc\",\"configuration\":{\"clevel\":5,\"blocksize\":0,\"typesize\":1,\"cname\":\"zstd\",\"shuffle\":\"shuffle\"}}",
				JsonElement.class).getAsJsonObject();

		assertEquals("blosc codec", expected, jsonCodec.getAsJsonObject());

		final CodecInfo codecsDeserialized = gson.fromJson(expected.toString(), CodecInfo.class);
		assertTrue("codec not blosc", codecsDeserialized instanceof Blosc);
	}

	@Test
	public void testSerializeGzipCompression() {


		CodecInfo[] codecs = new CodecInfo[]{
				new GzipCompression()
		};

		final String jsonCodecArrayString = gson.toJsonTree(codecs).getAsJsonArray().toString();
		System.out.println(jsonCodecArrayString);

//		JsonElement expected = gson.fromJson(
//				"[{\"name\":\"astype\",\"configuration\":{\"dataType\":\"float64\",\"encodedType\":\"int16\"}},{\"name\":\"gzip\",\"configuration\":{\"level\":-1,\"useZlib\":false}}]",
//				JsonElement.class);
//		assertEquals("codec array", expected, jsonCodecArray.getAsJsonArray());

		final CodecInfo[] codecsDeserialized = gson.fromJson(jsonCodecArrayString, CodecInfo[].class);
		assertEquals("codecs length not 1", 1, codecsDeserialized.length);
		assertTrue("codec not gzip", codecsDeserialized[0] instanceof GzipCompression);
	}

	@Test
	public void testSerializeZstandardCompression() {

		final CodecInfo codec1 = new ZarrV3Compressor.Zstandard();
		final CodecInfo codec2 = new ZarrV3Compressor.Zstandard(10, false);
		final CodecInfo codec3 = new ZarrV3Compressor.Zstandard(new ZstandardCompression());
		final JsonElement serialized1 = gson.toJsonTree(codec1).getAsJsonObject();
		final JsonElement serialized2 = gson.toJsonTree(codec2).getAsJsonObject();
		final JsonElement serialized3 = gson.toJsonTree(codec3).getAsJsonObject();
		final JsonElement expected1 = gson.fromJson( "{\n"
				+ "    \"configuration\": {\n"
				+ "        \"level\": 5,\n"
				+ "        \"checksum\": true\n"
				+ "    },\n"
				+ "    \"name\": \"zstd\"\n"
				+ "}", JsonElement.class).getAsJsonObject();
		final JsonElement expected2 = gson.fromJson( "{\n"
				+ "    \"configuration\": {\n"
				+ "        \"level\": 10,\n"
				+ "        \"checksum\": false\n"
				+ "    },\n"
				+ "    \"name\": \"zstd\"\n"
				+ "}", JsonElement.class).getAsJsonObject();
		final JsonElement expected3 = gson.fromJson( "{\n"
				+ "    \"name\": \"zstd\",\n"
				+ "    \"configuration\": {\n"
				+ "        \"level\": 3,\n"
				+ "        \"checksum\": false\n"
				+ "    }\n"
				+ "}", JsonElement.class).getAsJsonObject();

		assertEquals(expected1, serialized1);
		assertEquals(expected2, serialized2);
		assertEquals(expected3, serialized3);
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

		final CodecInfo codecsDeserialized = gson.fromJson(expected, CodecInfo.class);
		assertTrue("codec not zstd", codecsDeserialized instanceof ZarrV3Compressor.Zstandard);
	}
}
