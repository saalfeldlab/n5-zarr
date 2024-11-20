/*-
 * #%L
 * Not HDF5
 * %%
 * Copyright (C) 2019 - 2022 Stephan Saalfeld
 * %%
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDERS OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 * #L%
 */
package org.janelia.saalfeldlab.n5.zarr.v3;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.janelia.saalfeldlab.n5.Bzip2Compression;
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.GzipCompression;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.RawCompression;
import org.janelia.saalfeldlab.n5.blosc.BloscCompression;
import org.janelia.saalfeldlab.n5.codec.Codec;
import org.janelia.scicomp.n5.zstandard.ZstandardCompression;

import org.janelia.saalfeldlab.n5.serialization.NameConfig;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

/**
 * @author Stephan Saalfeld &lt;saalfelds@janelia.hhmi.org&gt;
 *
 */
public interface ZarrV3Compressor extends Codec.BytesCodec {

	/*
	 * idiotic stream based initialization because Java cannot have static
	 * initialization code in interfaces
	 */
	public static Map<String, Class<? extends ZarrV3Compressor>> registry = Stream
			.of(new SimpleImmutableEntry<>("zstd", Zstandard.class), new SimpleImmutableEntry<>("blosc", Blosc.class),
					new SimpleImmutableEntry<>("zlib", Zlib.class), new SimpleImmutableEntry<>("gzip", Gzip.class),
					new SimpleImmutableEntry<>("bz2", Bz2.class))
			.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

	public static ZarrV3Compressor fromCompression(final Compression compression) {

		try {
			if (compression instanceof BloscCompression) {
				return new Blosc((BloscCompression) compression);
			} else if (compression instanceof GzipCompression) {
				final Class<? extends Compression> clazz = compression.getClass();
				final Field field = clazz.getDeclaredField("useZlib");
				field.setAccessible(true);
				final Boolean useZlib = (Boolean) field.get(compression);
				field.setAccessible(false);
				return useZlib != null && useZlib ? new Zlib((GzipCompression) compression)
						: new Gzip((GzipCompression) compression);
			} else if (compression instanceof Bzip2Compression) {
				return new Bz2((Bzip2Compression) compression);
			} else if (compression instanceof ZstandardCompression) {
				return new Zstandard((ZstandardCompression) compression);
			} else {
				return new Raw();
			}
		} catch (NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException e) {
			return null;
		}

	}

	@Override
	public default OutputStream encode(final OutputStream in) throws IOException {
    	return getCompression().encode(in);
    }

	@Override
	public default InputStream decode(final InputStream in) throws IOException {
    	return getCompression().decode(in);
    }

    public Compression getCompression();

  public static class Zstandard implements ZarrV3Compressor {
	  
	  // TODO implement get type

    @SuppressWarnings("unused")
    private final String id = "zstd";
    private final int level;
    private final transient int nbWorkers;

    public Zstandard(int level) {
      this(level, 0);
    }

    public Zstandard(int level, int nbWorkers) {
      this.level = level;
      this.nbWorkers = nbWorkers;
    }

    public Zstandard(ZstandardCompression compression) {
      this.level = compression.getLevel();
      this.nbWorkers = compression.getNbWorkers();
    }

    @Override
    public Compression getCompression() {
      final ZstandardCompression compression = new ZstandardCompression(level);
      if(this.nbWorkers != 0)
        compression.setNbWorkers(this.nbWorkers);
      return compression;
    }

  }

  @NameConfig.Name("blosc")
  public static class Blosc implements ZarrV3Compressor {

    @SuppressWarnings("unused")
    private final String id = "blosc";

    @NameConfig.Parameter
    private final String cname;

    @NameConfig.Parameter
    private final int clevel;

    @NameConfig.Parameter
    private final String shuffle;

    @NameConfig.Parameter
    private final int blocksize;

    @NameConfig.Parameter
    private final int typesize;

    @NameConfig.Parameter
    private final transient int nthreads;

    public Blosc(
        final String cname,
        final int clevel,
        final String shuffle,
        final int blockSize,
        final int typeSize,
        final int nthreads) {

      this.cname = cname;
      this.clevel = clevel;
      this.shuffle = shuffle;
      this.blocksize = blockSize;
      this.typesize = typeSize;
      this.nthreads = nthreads;
    }
    
    public Blosc(final BloscCompression compression) throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {

      final Class<? extends BloscCompression> clazz = compression.getClass();

      Field field = clazz.getDeclaredField("cname");
      field.setAccessible(true);
      cname = (String)field.get(compression);
      field.setAccessible(false);

      field = clazz.getDeclaredField("clevel");
      field.setAccessible(true);
      clevel = field.getInt(compression);
      field.setAccessible(false);

      field = clazz.getDeclaredField("typesize");
      field.setAccessible(true);
      typesize = field.getInt(compression);
      field.setAccessible(false);

      field = clazz.getDeclaredField("shuffle");
      field.setAccessible(true);
      int _shuffle = field.getInt(compression);
      switch (_shuffle) {
        case BloscCompression.NOSHUFFLE:
          shuffle = "noshuffle"; break;
        case BloscCompression.SHUFFLE:
          shuffle = "shuffle"; break;
        case BloscCompression.BITSHUFFLE:
          shuffle = "bitshuffle"; break;
        case BloscCompression.AUTOSHUFFLE:
          shuffle = typesize == 1 ? "bitshuffle" : "shuffle"; break;
        default:
          throw new N5Exception("Invalid shuffle: " + _shuffle);
      }
      field.setAccessible(false);

      field = clazz.getDeclaredField("blocksize");
      field.setAccessible(true);
      blocksize = field.getInt(compression);
      field.setAccessible(false);

      field = clazz.getDeclaredField("nthreads");
      field.setAccessible(true);
      nthreads = field.getInt(compression);
      field.setAccessible(false);
    }

    int getShuffle(String s){
      switch (s) {
        case "noshuffle":
          return BloscCompression.NOSHUFFLE;
        case "shuffle":
          return BloscCompression.SHUFFLE;
        case "bitshuffle":
          return BloscCompression.BITSHUFFLE;
      }
      throw new N5Exception("Invalid shuffle: " + s);
    }

    @Override
    public BloscCompression getCompression() {

      return new BloscCompression(cname, clevel, getShuffle(shuffle), blocksize, Math.max(1, nthreads));
    }
   
    @Override
    public String getType() {
    	return id;
    }
  }

  public static class Zlib implements ZarrV3Compressor {

    @SuppressWarnings("unused")
    private final String id = "zlib";
    private final int level;

    public Zlib(final int level) {

      this.level = level;
    }

    public Zlib(final GzipCompression compression) throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {

      final Class<? extends GzipCompression> clazz = compression.getClass();

      final Field field = clazz.getDeclaredField("level");
      field.setAccessible(true);
      level = field.getInt(compression);
      field.setAccessible(false);
    }

    @Override
    public GzipCompression getCompression() {

      return new GzipCompression(level, true);
    }
  }

  public static class Gzip implements ZarrV3Compressor {

    @SuppressWarnings("unused")
    private final String id = "gzip";
    private final int level;

    public Gzip(final int level) {

      this.level = level;
    }

    public Gzip(final GzipCompression compression) throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {

      final Class<? extends GzipCompression> clazz = compression.getClass();

      final Field field = clazz.getDeclaredField("level");
      field.setAccessible(true);
      level = field.getInt(compression);
      field.setAccessible(false);
    }

    @Override
    public GzipCompression getCompression() {

      return new GzipCompression(level);
    }
  }

  public static class Bz2 implements ZarrV3Compressor {

    @SuppressWarnings("unused")
    private final String id = "bz2";
    private final int level;

    public Bz2(final int level) {

      this.level = level;
    }

    public Bz2(final Bzip2Compression compression) throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {

      final Class<? extends Bzip2Compression> clazz = compression.getClass();

      final Field field = clazz.getDeclaredField("blockSize");
      field.setAccessible(true);
      level = field.getInt(compression);
      field.setAccessible(false);
    }

    @Override
    public Bzip2Compression getCompression() {

      return new Bzip2Compression(level);
    }
  }

  class Raw implements ZarrV3Compressor {
    public Raw() {}
    @Override
    public RawCompression getCompression() {

      return new RawCompression();
    }
  }

  public static JsonAdapter jsonAdapter = new JsonAdapter();

  static public class JsonAdapter implements JsonDeserializer<ZarrV3Compressor> {

    @Override
    public ZarrV3Compressor deserialize(final JsonElement json, final Type typeOfT, final JsonDeserializationContext context)
        throws JsonParseException {

      final JsonObject jsonObject = json.getAsJsonObject();
      final JsonElement jsonId = jsonObject.get("id");
      if (jsonId == null)
        return null;
      final String id = jsonId.getAsString();
      final Class<? extends ZarrV3Compressor> compressorClass = registry.get(id);
      if (compressorClass == null)
        return null;

      return context.deserialize(json, compressorClass);
    }
  }

  TypeAdapter<Raw> rawNullAdapter = new TypeAdapter<Raw>() {

    @Override public void write(JsonWriter out, Raw value) throws IOException {
      final boolean serializeNull = out.getSerializeNulls();
      out.setSerializeNulls(true);
      out.nullValue();
      out.setSerializeNulls(serializeNull);
    }

    @Override public Raw read(JsonReader in) {

      return new Raw();
    }
  };
}
