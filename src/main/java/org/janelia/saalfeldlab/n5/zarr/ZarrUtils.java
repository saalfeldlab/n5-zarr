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

	/**
	 * Returns one {@link JsonElement} that (attempts to) combine all 
	 * passed json elements. Reduces the list by repeatedly calling the
	 * two-argument {@link combine} method.
	 * 
	 * @param elements an array of json elements
	 * @return a new, combined element
	 */
	static JsonElement combineAll(final JsonElement... elements) {
		return Arrays.stream(elements).reduce(null, ZarrUtils::combine);
	}

	/**
	 * Returns one {@link JsonElement} that combines two others. The returned instance
	 * is a deep copy, and the arguments are not modified.
	 * A copy of base element is returned if the two arguments can not be combined.
	 * The two arguments may be combined if they are both {@link JsonObject}s or both
	 * {@link JsonArray}s.
	 * <p>
	 * If both arguments are {@link JsonObject}s, every key-value pair in the add argument
	 * is added to the (copy of) the base argument, overwriting any duplicate keys.
	 * If both arguments are {@link JsonArray}s, the add argument is concatenated to the 
	 * (copy of) the base argument.
	 *
	 * @param base the base element
	 * @param add the element to add
	 * @return the new element
	 */
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

			return baseObj;
		} else if (base.isJsonArray() && add.isJsonArray()) {
			final JsonArray baseArr = base.getAsJsonArray().deepCopy();
			final JsonArray addArr = add.getAsJsonArray();
			for (int i = 0; i < addArr.size(); i++)
				baseArr.add(addArr.get(i));

			return baseArr;
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
