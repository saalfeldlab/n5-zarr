package org.janelia.saalfeldlab.n5.zarr;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.janelia.saalfeldlab.n5.GsonUtils;
import org.janelia.saalfeldlab.n5.KeyValueAccess;
import org.janelia.saalfeldlab.n5.LockedChannel;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.N5Reader;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;

public interface ZarrUtils extends N5Reader {

	public static Version VERSION = new Version(2, 0, 0);

	public static final String zarrayFile = ".zarray";
	public static final String zattrsFile = ".zattrs";
	public static final String zgroupFile = ".zgroup";

	public static final String ZARR_FORMAT_KEY = "zarr_format";

	static Version getVersion( final KeyValueAccess keyValueAccess, final Gson gson, final String basePath ) throws IOException {
		final JsonElement elem;
		if (groupExists(keyValueAccess, basePath, "")) {
			elem = getAttributesZGroup(keyValueAccess, gson, basePath, "");
		} else if (datasetExists(keyValueAccess, basePath, "")) {
			elem = getAttributesZArray(keyValueAccess, gson, basePath, "");
		} else {
			return VERSION;
		}

		if (elem != null && elem.isJsonObject()) {
			final JsonElement fmt = elem.getAsJsonObject().get( ZARR_FORMAT_KEY );
			if (fmt.isJsonPrimitive())
				return new Version(fmt.getAsInt(), 0, 0);
		}
		return VERSION;
	}

	static Version getVersion( final JsonObject json )
	{
		final JsonElement fmt = json.get(ZARR_FORMAT_KEY);
		if (fmt.isJsonPrimitive())
			return new Version(fmt.getAsInt(), 0, 0);

		return null;
	}

	static boolean groupExists(final KeyValueAccess keyValueAccess, final String absolutePath) {

		return keyValueAccess.isFile(keyValueAccess.compose(absolutePath, zgroupFile));
	}

	static boolean groupExists(final KeyValueAccess keyValueAccess, final String basePath, final String normalPath) {

		return keyValueAccess.isFile(zGroupAbsolutePath(keyValueAccess, basePath, normalPath));
	}

	static boolean datasetExists( final KeyValueAccess keyValueAccess, final String basePath, final String normalPath) throws IOException {

		return keyValueAccess.isFile(zArrayAbsolutePath(keyValueAccess, basePath, normalPath));
	}

//	@Override
//	default JsonElement getAttributes(final String pathName) throws IOException {
//		return getMergedAttributes(pathName);
//	}

//	public static ZarrDatasetAttributes getDatasetAttributes(final String pathName) throws IOException {
//
//		final ZArrayAttributes zattrs = getZArrayAttributes(pathName);
//		if (zattrs == null)
//			return null;
//		else
//			return zattrs.getDatasetAttributes();
//	}

	static ZArrayAttributes getZArrayAttributes(final KeyValueAccess keyValueAccess, final Gson gson, final String basePath, final String pathName) throws IOException {

		final JsonElement elem = getAttributesZArray(keyValueAccess, gson, basePath, pathName);
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

	static JsonElement getMergedAttributes( final KeyValueAccess keyValueAccess, final Gson gson, final String basePath, final String pathName ) {

		JsonElement groupElem = null;
		JsonElement arrElem = null;
		JsonElement attrElem = null;
		try {
			groupElem = getAttributesZGroup(keyValueAccess, gson, basePath, pathName);
		} catch (IOException e) { }

		try {
			arrElem = getAttributesZArray( keyValueAccess, gson, basePath, pathName );
		} catch (IOException e) { }

		try {
			attrElem = getAttributesZAttrs( keyValueAccess, gson, basePath, pathName );
		} catch (IOException e) { }

		return combineAll( groupElem, arrElem, attrElem );
	}

	static JsonElement combineAll(final JsonElement ...elements ) {
		return Arrays.stream(elements).reduce(null, ZarrUtils::combine);
	}

	static JsonElement combine(final JsonElement base, final JsonElement add) {
		if (base == null)
			return add == null ? null : add.deepCopy();
		else if (add == null)
			return base == null ? null : base.deepCopy();

		if (base.isJsonObject() && add.isJsonObject()) {
			final JsonObject baseObj = base.getAsJsonObject().deepCopy();
			final JsonObject addObj = add.getAsJsonObject();
			for (String k : addObj.keySet())
				baseObj.add(k, addObj.get(k));
		} else if (base.isJsonArray() && add.isJsonArray()) {
			final JsonArray baseArr = base.getAsJsonArray().deepCopy();
			final JsonArray addArr = add.getAsJsonArray();
			for (int i = 0; i < addArr.size(); i++)
				baseArr.add(addArr.get(i));
		}
		return base == null ? null : base.deepCopy();
	}

	static Gson registerGson(final GsonBuilder gsonBuilder) {

		return addTypeAdapters(gsonBuilder).create();
	}

	static GsonBuilder addTypeAdapters(GsonBuilder gsonBuilder) {

		gsonBuilder.registerTypeAdapter(DType.class, new DType.JsonAdapter());
		gsonBuilder.registerTypeAdapter(ZarrCompressor.class, ZarrCompressor.jsonAdapter);
		gsonBuilder.registerTypeAdapter(ZArrayAttributes.class, ZArrayAttributes.jsonAdapter);
		gsonBuilder.disableHtmlEscaping();
		return gsonBuilder;
	}

	/**
	 * Constructs the absolute path (in terms of this store) to a .zarray
	 *
	 * @param normalPath normalized group path without leading slash
	 * @return
	 */
	static String zArrayAbsolutePath(final KeyValueAccess keyValueAccess, final String basePath, final String normalPath) {

		return keyValueAccess.compose(basePath, normalPath, zarrayFile);
	}

	/**
	 * Constructs the absolute path (in terms of this store) to a .zattrs
	 *
	 * @param normalPath normalized group path without leading slash
	 * @return
	 */
	static String zAttrsAbsolutePath(final KeyValueAccess keyValueAccess, final String basePath, final String normalPath) {

		return keyValueAccess.compose(basePath, normalPath, zattrsFile);
	}

	/**
	 * Constructs the absolute path (in terms of this store) to a .zgroup
	 *
	 *
	 * @param normalPath normalized group path without leading slash
	 * @return
	 */
	static String zGroupAbsolutePath(final KeyValueAccess keyValueAccess, final String basePath, final String normalPath) {

		return keyValueAccess.compose(basePath, normalPath, zgroupFile);
	}

	/**
	 * Constructs the absolute path (in terms of this store) to a .zarray
	 *
	 * @param normalPath normalized group path without leading slash
	 * @return
	 */
	static String zArrayPath(final KeyValueAccess keyValueAccess, final String normalPath) {

		return keyValueAccess.compose(normalPath, zarrayFile);
	}

	/**
	 * Constructs the absolute path (in terms of this store) to a .zattrs
	 *
	 * @param normalPath normalized group path without leading slash
	 * @return
	 */
	static String zAttrsPath(final KeyValueAccess keyValueAccess, final String normalPath) {

		return keyValueAccess.compose(normalPath, zattrsFile);
	}

	/**
	 * Constructs the absolute path (in terms of this store) to a .zgroup
	 *
	 *
	 * @param normalPath normalized group path without leading slash
	 * @return
	 */
	static String zGroupPath(final KeyValueAccess keyValueAccess, final String normalPath) {

		return keyValueAccess.compose(normalPath, zgroupFile);
	}

	static JsonElement getJsonResource(final KeyValueAccess keyValueAccess, final Gson gson, final String absolutePath) throws N5Exception.N5IOException {

		if (!keyValueAccess.exists(absolutePath))
			return null;

		try (final LockedChannel lockedChannel = keyValueAccess.lockForReading(absolutePath)) {
			return GsonUtils.readAttributes(lockedChannel.newReader(), gson);
		} catch (IOException e) {
			throw new N5Exception.N5IOException("Cannot open lock for Reading", e);
		}
	}

	static JsonElement getAttributesZAttrs( final KeyValueAccess keyValueAccess, final Gson gson, final String basePath, final String normalPathName ) throws IOException {

		return getJsonResource( keyValueAccess, gson, zAttrsAbsolutePath(keyValueAccess, basePath, normalPathName));
	}

	static JsonElement getAttributesZArray(final KeyValueAccess keyValueAccess,final Gson gson, final String basePath, final String normalPathName ) throws IOException {

		return getJsonResource( keyValueAccess, gson, zArrayAbsolutePath( keyValueAccess, basePath, normalPathName));
	}

	static JsonElement getAttributesZGroup( final KeyValueAccess keyValueAccess,final Gson gson, final String basePath, final String normalPathName ) throws IOException {

		return getJsonResource( keyValueAccess, gson,  zGroupAbsolutePath(keyValueAccess, basePath, normalPathName ));
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
	static String getZarrDataBlockPath(
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
	
	static void reorder(final long[] array) {

		long a;
		final int max = array.length - 1;
		for (int i = (max - 1) / 2; i >= 0; --i) {
			final int j = max - i;
			a = array[i];
			array[i] = array[j];
			array[j] = a;
		}
	}

	static void reorder(final int[] array) {

		int a;
		final int max = array.length - 1;
		for (int i = (max - 1) / 2; i >= 0; --i) {
			final int j = max - i;
			a = array[i];
			array[i] = array[j];
			array[j] = a;
		}
	}

	static void reorder(final JsonArray array) {

		JsonElement a;
		final int max = array.size() - 1;
		for (int i = (max - 1) / 2; i >= 0; --i) {
			final int j = max - i;
			a = array.get(i);
			array.set(i, array.get(j));
			array.set(j, a );
		}
	}

}
