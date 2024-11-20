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


		Codec[] codecs = new Codec[]{
				new Blosc("zstd", 5, "shuffle", 0, 1, 1)
		};

		final String jsonCodecArrayString = gson.toJsonTree(codecs).getAsJsonArray().toString();
		System.out.println(jsonCodecArrayString);

//		JsonElement expected = gson.fromJson(
//				"[{\"name\":\"astype\",\"configuration\":{\"dataType\":\"float64\",\"encodedType\":\"int16\"}},{\"name\":\"gzip\",\"configuration\":{\"level\":-1,\"useZlib\":false}}]",
//				JsonElement.class);
//		assertEquals("codec array", expected, jsonCodecArray.getAsJsonArray());

		final Codec[] codecsDeserialized = gson.fromJson(jsonCodecArrayString, Codec[].class);
		assertEquals("codecs length not 1", 1, codecsDeserialized.length);
		assertTrue("codec not blosc", codecsDeserialized[0] instanceof Blosc);
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

}
