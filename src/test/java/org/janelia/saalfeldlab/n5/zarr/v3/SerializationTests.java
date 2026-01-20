package org.janelia.saalfeldlab.n5.zarr.v3;

import static org.junit.Assert.assertArrayEquals;

import java.util.Arrays;

import org.janelia.saalfeldlab.n5.NameConfigAdapter;
import org.janelia.saalfeldlab.n5.codec.CodecInfo;
import org.janelia.saalfeldlab.n5.zarr.chunks.ChunkAttributes;
import org.janelia.saalfeldlab.n5.zarr.chunks.ChunkGrid;
import org.janelia.saalfeldlab.n5.zarr.chunks.ChunkKeyEncoding;
import org.janelia.saalfeldlab.n5.zarr.codec.transpose.ZarrTransposeCodecInfo;
import org.janelia.saalfeldlab.n5.zarr.codec.transpose.ZarrTransposeCodecInfo.ZarrTransposeOrder;
import org.janelia.saalfeldlab.n5.zarr.codec.transpose.ZarrTransposeCodecInfo.ZarrTransposeOrderAdapter;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;

public class SerializationTests {

	static Gson gson;

	@BeforeClass
	public static void setup() {
		GsonBuilder gsonBuilder = new GsonBuilder();
		gsonBuilder.registerTypeHierarchyAdapter(CodecInfo.class, NameConfigAdapter.getJsonAdapter(CodecInfo.class));
		gsonBuilder.registerTypeAdapter(ZarrTransposeOrder.class, new ZarrTransposeOrderAdapter());
		gson = gsonBuilder.create();
	}

	@Test
	public void testTransposeSerialization() {

		final int[] origOrder = new int[] {2,0,1};
		final ZarrTransposeCodecInfo codec = new ZarrTransposeCodecInfo(origOrder);
		final JsonElement json = gson.toJsonTree(codec);

		JsonElement orderJsonArr = json.getAsJsonObject().get("configuration").getAsJsonObject().get("order");
		int[] serializedOrder = gson.fromJson(orderJsonArr, int[].class);
		assertArrayEquals(new int[]{1,2,0}, serializedOrder);

		final ZarrTransposeCodecInfo deserializedCodec = (ZarrTransposeCodecInfo)gson.fromJson(json, CodecInfo.class);
		assertArrayEquals(origOrder, deserializedCodec.getOrder());
	}
}