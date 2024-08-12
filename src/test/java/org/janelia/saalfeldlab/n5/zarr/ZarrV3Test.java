package org.janelia.saalfeldlab.n5.zarr;

import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.CompressionAdapter;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.NameConfigAdapter;
import org.janelia.saalfeldlab.n5.universe.N5Factory;
import org.janelia.saalfeldlab.n5.zarr.chunks.ChunkAttributes;
import org.janelia.saalfeldlab.n5.zarr.chunks.ChunkGrid;
import org.janelia.saalfeldlab.n5.zarr.chunks.ChunkKeyEncoding;
import org.janelia.saalfeldlab.n5.zarr.v3.ZarrV3DatasetAttributes;
import org.junit.Test;

import com.google.gson.GsonBuilder;

public class ZarrV3Test {

	private static GsonBuilder addZarrAdapters(GsonBuilder gsonBuilder) {

		gsonBuilder.registerTypeAdapter(DataType.class, new DataType.JsonAdapter());
		gsonBuilder.registerTypeAdapter(ZarrCompressor.class, ZarrCompressor.jsonAdapter);
		gsonBuilder.registerTypeAdapter(ZarrCompressor.Raw.class, ZarrCompressor.rawNullAdapter);
		gsonBuilder.registerTypeHierarchyAdapter(Compression.class, CompressionAdapter.getJsonAdapter());
		gsonBuilder.registerTypeAdapter(Compression.class, CompressionAdapter.getJsonAdapter());
		gsonBuilder.registerTypeAdapter(ZarrV3DatasetAttributes.class, ZarrV3DatasetAttributes.jsonAdapter);

		gsonBuilder.registerTypeHierarchyAdapter(ChunkGrid.class, NameConfigAdapter.getJsonAdapter(ChunkGrid.class));
		gsonBuilder.registerTypeHierarchyAdapter(ChunkKeyEncoding.class, NameConfigAdapter.getJsonAdapter(ChunkKeyEncoding.class));

		gsonBuilder.registerTypeHierarchyAdapter(ChunkAttributes.class, ChunkAttributes.getJsonAdapter());
		gsonBuilder.registerTypeHierarchyAdapter(Filter.class, Filter.jsonAdapter);
		gsonBuilder.disableHtmlEscaping();
		return gsonBuilder;

	}

	@Test
	public void serializationTest() {

		final N5Factory n5Factory = new N5Factory();
		n5Factory.gsonBuilder(addZarrAdapters(new GsonBuilder()));
		final N5Reader n5 = n5Factory.openReader("src/test/resources/shardExamples/test.zarr/mid_sharded");

		final ChunkGrid chunkGrid = n5.getAttribute("/", "chunk_grid", ChunkGrid.class);
		System.out.println(chunkGrid);

		final ChunkKeyEncoding chunkKeyEncoding = n5.getAttribute("/", "chunk_key_encoding", ChunkKeyEncoding.class);
		System.out.println(chunkKeyEncoding);

		final ChunkAttributes chunkAttributes = n5.getAttribute("/", "/", ChunkAttributes.class);
		System.out.println(chunkAttributes);
	}

}
