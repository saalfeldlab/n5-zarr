/**
 * Copyright (c) 2017, Stephan Saalfeld
 * All rights reserved.
 * <p>
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * <p>
 * 1. Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 * <p>
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package org.janelia.saalfeldlab.n5.zarr;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;

import java.io.IOException;
import java.util.Collection;

import org.janelia.saalfeldlab.n5.GsonN5Reader;
import org.janelia.saalfeldlab.n5.KeyValueAccess;

/**
 * A Zarr {@link GsonN5Reader} for JSON attributes parsed by {@link Gson}.
 *
 */
public interface GsonZarrReader extends GsonN5Reader {
	
	public static final String zarrayFile = ".zarray";
	public static final String zattrsFile = ".zattrs";
	public static final String zgroupFile = ".zgroup";

	@Override
	default JsonElement getAttributes(final String pathName) throws IOException {
		return getAttributesZAttrs(pathName);
	}

	@Override
	default ZarrDatasetAttributes getDatasetAttributes(final String pathName) throws IOException {

		final ZArrayAttributes zattrs = getZArrayAttributes(pathName);
		if (zattrs == null)
			return null;
		else
			return zattrs.getDatasetAttributes();
	}

	public default ZArrayAttributes getZArrayAttributes(final String pathName) throws IOException {

		final Gson gson = getGson();
		final JsonElement elem = getAttributesZArray(pathName);
		if (elem == null)
			return null;

		final JsonObject attributes;
		if (elem.isJsonObject())
			attributes = elem.getAsJsonObject();
		else
			return null;

		final JsonElement sepElem = attributes.get("dimension_separator");
		return new ZArrayAttributes(
				attributes.get("zarr_format").getAsInt(),
				gson.fromJson(attributes.get("shape"), long[].class),
				gson.fromJson(attributes.get("chunks"), int[].class),
				gson.fromJson(attributes.get("dtype"), DType.class),
				gson.fromJson(attributes.get("compressor"), ZarrCompressor.class),
				attributes.get("fill_value").getAsString(),
				attributes.get("order").getAsString().charAt(0),
				sepElem != null ? sepElem.getAsString() : ".",
				gson.fromJson(attributes.get("filters"), TypeToken.getParameterized(Collection.class, Filter.class).getType()));
	}

	public static Gson registerGson(final GsonBuilder gsonBuilder) {
		return addTypeAdapters( gsonBuilder ).create();
	}

	public static GsonBuilder addTypeAdapters(GsonBuilder gsonBuilder) {
		gsonBuilder.registerTypeAdapter(DType.class, new DType.JsonAdapter());
		gsonBuilder.registerTypeAdapter(ZarrCompressor.class, ZarrCompressor.jsonAdapter);
		gsonBuilder.disableHtmlEscaping();
		return gsonBuilder;
	}

	public abstract KeyValueAccess getKeyValueAccess();
	
	/**
	 * Constructs the relative path (in terms of this store) to a .zarray
	 *
	 *
	 * @param normalPath normalized group path without leading slash
	 * @return
	 */
	public default String zArrayPath(final String normalPath) {

		return getKeyValueAccess().compose(normalPath, zarrayFile);
	}

	/**
	 * Constructs the relative path (in terms of this store) to a .zattrs
	 *
	 *
	 * @param normalPath normalized group path without leading slash
	 * @return
	 */
	public default String zAttrsPath(final String normalPath) {

		return getKeyValueAccess().compose(normalPath, zattrsFile);
	}

	/**
	 * 
	 * @param resourcePath path of file / resource relative to root
	 * @return
	 * @throws IOException
	 */
	public abstract JsonElement getAttributesCache(final String resourcePath) throws IOException;

	/**
	 * Constructs the relative path (in terms of this store) to a .zgroup
	 *
	 *
	 * @param normalPath normalized group path without leading slash
	 * @return
	 */
	public default String zGroupPath(final String normalPath) {

		return getKeyValueAccess().compose(normalPath, zgroupFile);
	}
	
	public default JsonElement getAttributesZAttrs( final String pathName ) throws IOException {

		return getAttributesCache( zAttrsPath( normalize( pathName ) ) );
	}

	public default JsonElement getAttributesZArray( final String pathName ) throws IOException {

		return getAttributesCache( zArrayPath( normalize( pathName ) ) );
	}

	public default JsonElement getAttributesZGroup( final String pathName ) throws IOException {

		return getAttributesCache( zGroupPath( normalize( pathName ) ) );
	}

	/**
	 * Constructs the path for a data block in a dataset at a given grid position.
	 *
	 * The returned path is
	 * <pre>
	 * $gridPosition[n]$dimensionSeparator$gridPosition[n-1]$dimensionSeparator[...]$dimensionSeparator$gridPosition[0]
	 * </pre>
	 *
	 * This is the file into which the data block will be stored.
	 *
	 * @param gridPosition
	 * @param dimensionSeparator
	 * @param isRowMajor
	 *
	 * @return
	 */
	public static String getZarrDataBlockPath(
			final long[] gridPosition,
			final String dimensionSeparator,
			final boolean isRowMajor) {

		final StringBuilder pathStringBuilder = new StringBuilder();
		if (isRowMajor) {
			pathStringBuilder.append(gridPosition[gridPosition.length - 1]);
			for (int i = gridPosition.length - 2; i >= 0 ; --i) {
				pathStringBuilder.append(dimensionSeparator);
				pathStringBuilder.append(gridPosition[i]);
			}
		} else {
			pathStringBuilder.append(gridPosition[0]);
			for (int i = 1; i < gridPosition.length; ++i) {
				pathStringBuilder.append(dimensionSeparator);
				pathStringBuilder.append(gridPosition[i]);
			}
		}

		return pathStringBuilder.toString();
	}

	/**
	 * Removes the leading slash from a given path and returns the normalized
	 * path.  It ensures correctness on both Unix and Windows, otherwise
	 * {@code pathName} is treated as UNC path on Windows, and
	 * {@code Paths.get(pathName, ...)} fails with
	 * {@code InvalidPathException}.
	 *
	 *
	 * @param keyValueAccess
	 * @param path
	 * @return the normalized path, without leading slash
	 */
	public default String normalize(final String path) {

		return normalize(getKeyValueAccess(), path);
	}

	/**
	 * Removes the leading slash from a given path and returns the normalized
	 * path.  It ensures correctness on both Unix and Windows, otherwise
	 * {@code pathName} is treated as UNC path on Windows, and
	 * {@code Paths.get(pathName, ...)} fails with
	 * {@code InvalidPathException}.
	 *
	 *
	 * @param keyValueAccess
	 * @param path
	 * @return the normalized path, without leading slash
	 */
	public static String normalize(final KeyValueAccess keyValueAccess, final String path) {

		return keyValueAccess.normalize(path.startsWith("/") || path.startsWith("\\") ? path.substring(1) : path);
	}

}
