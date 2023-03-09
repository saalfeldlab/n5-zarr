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
import org.janelia.saalfeldlab.n5.LockedChannel;
import org.janelia.saalfeldlab.n5.N5KeyValueReader.N5GroupInfo;

/**
 * A Zarr {@link GsonN5Reader} for JSON attributes parsed by {@link Gson}.
 *
 */
public interface GsonZarrReader extends GsonN5Reader {
	
	public static final String zarrayFile = ".zarray";
	public static final String zattrsFile = ".zattrs";
	public static final String zgroupFile = ".zgroup";

	@Override
	default ZarrDatasetAttributes getDatasetAttributes(final String pathName) throws IOException {

		final ZArrayAttributes zattrs = getZArrayAttributes(pathName);
		if (zattrs == null)
			return null;
		else
			return zattrs.getDatasetAttributes();
	}

//	public abstract ZArrayAttributes getZArrayAttributes(final String pathName) throws IOException;

	public static Gson registerGson(final GsonBuilder gsonBuilder) {
		return addTypeAdapters( gsonBuilder ).create();
	}

	public static GsonBuilder addTypeAdapters( GsonBuilder gsonBuilder )
	{
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
	 * Constructs the relative path (in terms of this store) to a .zgroup
	 *
	 *
	 * @param normalPath normalized group path without leading slash
	 * @return
	 */
	public default String zGroupPath(final String normalPath) {

		return getKeyValueAccess().compose(normalPath, zgroupFile);
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

}
