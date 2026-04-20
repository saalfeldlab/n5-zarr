package org.janelia.saalfeldlab.n5.zarr;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSyntaxException;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.GsonUtils;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.N5Exception.N5ClassCastException;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.N5Exception.N5JsonParseException;
import org.janelia.saalfeldlab.n5.N5Path.N5DirectoryPath;
import org.janelia.saalfeldlab.n5.ContainerDialect;
import org.janelia.saalfeldlab.n5.N5URI;
import org.janelia.saalfeldlab.n5.RawCompression;
import org.janelia.saalfeldlab.n5.HierarchyStore;
import org.janelia.saalfeldlab.n5.serialization.JsonArrayUtils;

import static org.janelia.saalfeldlab.n5.GsonUtils.parseAttributeElement;
import static org.janelia.saalfeldlab.n5.zarr.ZarrKeyValueReader.ZARRAY_FILE;
import static org.janelia.saalfeldlab.n5.zarr.ZarrKeyValueReader.ZATTRS_FILE;
import static org.janelia.saalfeldlab.n5.zarr.ZarrKeyValueReader.ZGROUP_FILE;

/**
 * {@code ContainerDialect} for Zarr v2.
 * <ul>
 * <li>Every directory that has a ".zgroup" file is a group.</li>
 * <li>Every directory that has a ".zarray" file is a dataset.</li>
 * <li>User-defined attributes are in an additional ".zattrs" file.</li>
 * </ul>
 */
public final class ZarrV2Dialect implements ContainerDialect {

	private final HierarchyStore store;
	private final Gson gson;
	private final boolean mapN5Attributes;
	private final boolean mergeAttributes;
	private final JsonObject groupAttr;

	/**
	 * @param mapN5DatasetAttributes
	 * 		If true, getAttributes and variants of getAttribute methods will contain keys used by n5 datasets, and
	 * 		whose values are those for their corresponding zarr fields. For example, if true, the key "dimensions"
	 * 		(from n5) may be used to obtain the value of the key "shape" (from zarr).
	 * @param mergeAttributes
	 * 		If true, fields from .zgroup, .zarray, and .zattrs will be merged when calling getAttributes, and
	 * 		variants of getAttribute
	 */
	public ZarrV2Dialect(
			final HierarchyStore store,
			final Gson gson,
			final boolean mapN5DatasetAttributes,
			final boolean mergeAttributes) {

		this.store = store;
		this.gson = gson;
		this.mapN5Attributes = mapN5DatasetAttributes;
		this.mergeAttributes = mergeAttributes;

		groupAttr = new JsonObject();
		groupAttr.add(ZarrKeyValueReader.ZARR_FORMAT_KEY, new JsonPrimitive(N5ZarrReader.VERSION.getMajor()));
	}

	private <T> T getAttribute(
			final N5DirectoryPath path,
			final String filename,
			final String normalizedAttributePath,
			final Type type) throws N5IOException, N5ClassCastException {

		final JsonElement root = store.readAttributesJson(path, filename, gson);

		try {
			return GsonUtils.readAttribute(root, normalizedAttributePath, type, gson);
		} catch (JsonSyntaxException | NumberFormatException | ClassCastException e) {
			if (normalizedAttributePath.equals("filters") && root.getAsJsonObject().get("filters").isJsonNull()) {
				return (T) Collections.EMPTY_LIST;
			}
			throw new N5ClassCastException(e);
		}
	}

	private static final List<String> mappedN5Attributes = List.of(
			DatasetAttributes.DIMENSIONS_KEY,
			DatasetAttributes.BLOCK_SIZE_KEY,
			DatasetAttributes.DATA_TYPE_KEY,
			DatasetAttributes.COMPRESSION_KEY);

	@Override
	public <T> T getAttribute(
			final N5DirectoryPath path,
			final String attributePath,
			final Type type) throws N5IOException, N5ClassCastException {

		final String normalizedAttributePath = N5URI.normalizeAttributePath(attributePath);

		if (mapN5Attributes && mappedN5Attributes.contains(normalizedAttributePath)) {
			final DatasetAttributes attr = getDatasetAttributes(path);
			if (attr != null) {
				switch (normalizedAttributePath) {
				case DatasetAttributes.DIMENSIONS_KEY:
					return parseAttributeElement(gson.toJsonTree(attr.getDimensions()), gson, type);
				case DatasetAttributes.BLOCK_SIZE_KEY:
					return parseAttributeElement(gson.toJsonTree(attr.getBlockSize()), gson, type);
				case DatasetAttributes.DATA_TYPE_KEY:
					return parseAttributeElement(gson.toJsonTree(attr.getDataType()), gson, type);
				case DatasetAttributes.COMPRESSION_KEY:
					return parseAttributeElement(gson.toJsonTree(attr.getCompression()), gson, type);
				}
			}
		}

		T obj = getAttribute(path, ZATTRS_FILE, normalizedAttributePath, type);
		if (obj == null)
			obj = getAttribute(path, ZARRAY_FILE, normalizedAttributePath, type);
		if (obj == null)
			obj = getAttribute(path, ZGROUP_FILE, normalizedAttributePath, type);
		return obj;
	}

	@Override
	public DatasetAttributes getDatasetAttributes(
			final N5DirectoryPath path) throws N5IOException {

		final JsonElement json = store.readAttributesJson(path, ZARRAY_FILE, gson);
		final ZArrayAttributes zarray = gson.fromJson(json, ZArrayAttributes.class);
		return zarray != null ? new ZarrDatasetAttributes(zarray) : null;
	}

	@Override
	public boolean datasetExists(
			final N5DirectoryPath path) throws N5IOException {

		return getDatasetAttributes(path) != null;
	}

	@Override
	public boolean groupExists(
			final N5DirectoryPath path) throws N5IOException {

		return store.readAttributesJson(path, ZGROUP_FILE, gson) != null;
	}

	@Override
	public String[] list(
			final N5DirectoryPath group) throws N5IOException {

		return store.listDirectories(group);
	}

	// NB: does not do any attribute mapping
	@Override
	public JsonElement getAttributes(final N5DirectoryPath path) throws N5IOException {

		if (mergeAttributes) {
			final JsonElement zgroup = store.readAttributesJson(path, ZGROUP_FILE, gson);
			final JsonElement zarray = store.readAttributesJson(path, ZARRAY_FILE, gson);
			if (zgroup == null && zarray == null) {
				return null;
			}
			final JsonElement zattrs = store.readAttributesJson(path, ZATTRS_FILE, gson);
			return combineAll(zgroup, zarray, zattrs);
		} else {
			return store.readAttributesJson(path, ZATTRS_FILE, gson);
		}
	}

	/**
	 * Returns one {@link JsonElement} that (attempts to) combine all passed
	 * json elements. The returned instance is not a deep copy. The arguments
	 * may be modified!
	 * <p>
	 * If all {@code elements} are {@code null}, {@code null} is returned.
	 * Otherwise, the base element is a deep copy of the first non-null element.
	 * The remaining {@code elements} are combined into the base element one by
	 * one:
	 * <p>
	 * The base element is returned if two arguments can not be combined. The
	 * two arguments may be combined if they are both {@link JsonObject}s or
	 * both {@link JsonArray}s.
	 * <p>
	 * If both arguments are {@link JsonObject}s, every key-value pair in the
	 * add argument is added to the base argument, overwriting any duplicate
	 * keys. If both arguments are {@link JsonArray}s, the add argument is
	 * concatenated to the base argument.
	 *
	 * @param elements
	 * 		an array of json elements
	 *
	 * @return a new, combined element
	 */
	private static JsonElement combineAll(final JsonElement... elements) {

		JsonElement base = null;
		for (final JsonElement element : elements) {
			if (element != null) {
				final JsonElement add = element;
				if (base == null) {
					base = add;
				} else if (base.isJsonObject() && add.isJsonObject()) {
					final JsonObject baseObj = base.getAsJsonObject();
					add.getAsJsonObject().asMap().forEach(baseObj::add);
				} else if (base.isJsonArray() && add.isJsonArray()) {
					final JsonArray baseArr = base.getAsJsonArray();
					baseArr.addAll(add.getAsJsonArray());
				} // else: trying to combine incompatible JsonElements
			}
		}
		return base;
	}

	@Override
	public <T> void setAttribute(
			final N5DirectoryPath path,
			final String attributePath,
			final T attribute) throws N5IOException {

		setAttributes(path, Collections.singletonMap(attributePath, attribute));
	}

	private enum Order {
		C,
		F
	}

	private static Order order(final JsonObject src) {
		final JsonElement e = src.get(ZArrayAttributes.orderKey);
		return e != null && "C".equals(e.getAsString()) ? Order.C : Order.F;
	}

	private static void redirectDimensions(final JsonObject obj, final Order order) {
		final JsonElement element = obj.remove(DatasetAttributes.DIMENSIONS_KEY);
		if (element != null) {
			final JsonArray shape = element.getAsJsonArray();
			if (order == Order.C)
				JsonArrayUtils.reverse(shape);
			obj.add(ZArrayAttributes.shapeKey, shape);
		}
	}

	private static void redirectBlockSize(final JsonObject obj, final Order order) {
		final JsonElement element = obj.remove(DatasetAttributes.BLOCK_SIZE_KEY);
		if (element != null) {
			final JsonArray chunkSize = element.getAsJsonArray();
			if (order == Order.C)
				JsonArrayUtils.reverse(chunkSize);
			obj.add(ZArrayAttributes.chunksKey, chunkSize);
		}
	}

	private static void redirectDataType(final JsonObject obj) {
		final JsonElement element = obj.remove(DatasetAttributes.DATA_TYPE_KEY);
		if (element != null) {
			obj.addProperty(ZArrayAttributes.dTypeKey, new DType(DataType.fromString(element.getAsString())).toString());
		}
	}

	private static void redirectCompression(final JsonObject obj, final Gson gson) {
		final JsonElement element = obj.remove(DatasetAttributes.COMPRESSION_KEY);
		if (element != null) {
			final Compression c = gson.fromJson(element, Compression.class);
			if (c.getClass() == RawCompression.class)
				obj.add(ZArrayAttributes.compressorKey, JsonNull.INSTANCE);
			else
				obj.add(ZArrayAttributes.compressorKey, gson.toJsonTree(ZarrCompressor.fromCompression(c)));
		}
	}

	private static JsonObject extract(final JsonObject src, final String... keys) {
		final JsonObject dest = new JsonObject();
		for (final String key : keys) {
			final JsonElement value = src.remove(key);
			if (value != null) {
				dest.add(key, value);
			}
		}
		return dest;
	}

	@Override
	public void setAttributes(
			final N5DirectoryPath path,
			final Map<String, ?> attributes) throws N5IOException {

		final JsonElement zarray = store.readAttributesJson(path, ZARRAY_FILE, gson);
		final JsonElement zgroup = store.readAttributesJson(path, ZGROUP_FILE, gson);
		if (zarray == null && zgroup == null)
			throw new N5IOException(String.format("Directory does not exist: %s", path));

		if (attributes == null || attributes.isEmpty())
			return;

		JsonObject obj = new JsonObject();

		final JsonElement zattrs = store.readAttributesJson(path, ZATTRS_FILE, gson);
		if (zattrs != null) {
			zattrs.getAsJsonObject().asMap().forEach(obj::add);
		}

		if (zarray != null) { // path is an array

			// merge existing zarray and new attributes
			zarray.getAsJsonObject().asMap().forEach(obj::add);
			obj = GsonUtils.insertAttributes(obj, attributes, gson).getAsJsonObject();

			// map n5 attributes
			if (mapN5Attributes) {
				final Order order = order(obj);
				redirectDimensions(obj, order);
				redirectBlockSize(obj, order);
				redirectDataType(obj);
				redirectCompression(obj, gson);
			}

			// extract and write zarray attributes
			store.writeAttributesJson(path,
					ZARRAY_FILE,
					extract(obj, ZArrayAttributes.allKeys),
					gson);

		} else { // path is a group

			// merge existing zgroup and new attributes
			zgroup.getAsJsonObject().asMap().forEach(obj::add);
			obj = GsonUtils.insertAttributes(obj, attributes, gson).getAsJsonObject();

			// extract and write zgroup attributes
			store.writeAttributesJson(path,
					ZGROUP_FILE,
					extract(obj, ZArrayAttributes.zarrFormatKey),
					gson);
		}

		// whatever remains goes into .zattrs
		store.writeAttributesJson(path, ZATTRS_FILE, obj, gson);
	}

	private boolean removeAttribute(
			final N5DirectoryPath path,
			final String filename,
			final String normalizedAttributePath) throws N5IOException {

		final JsonElement root = store.readAttributesJson(path, filename, gson);
		if (root != null) {
			if (null != GsonUtils.removeAttribute(root, normalizedAttributePath)) {
				store.writeAttributesJson(path, filename, root, gson);
				return true;
			}
		}
		return false;
	}

	@Override
	public boolean removeAttribute(
			final N5DirectoryPath path,
			final String attributePath) throws N5IOException {

		final String normalizedAttributePath = N5URI.normalizeAttributePath(attributePath);
		return removeAttribute(path, ZATTRS_FILE, normalizedAttributePath) ||
				removeAttribute(path, ZARRAY_FILE, normalizedAttributePath) ||
				removeAttribute(path, ZGROUP_FILE, normalizedAttributePath);
	}

	private <T> T removeAttribute(
			final N5DirectoryPath path,
			final String filename,
			final String normalizedAttributePath,
			final Class<T> clazz) throws N5IOException, N5ClassCastException {

		final JsonElement root = store.readAttributesJson(path, filename, gson);
		if (root == null)
			return null;

		final T obj;
		try {
			obj = GsonUtils.removeAttribute(root, normalizedAttributePath, clazz, gson);
		} catch (JsonSyntaxException | NumberFormatException | ClassCastException e) {
			throw new N5ClassCastException(e);
		}

		if (obj != null)
			store.writeAttributesJson(path, filename, root, gson);

		return obj;
	}

	@Override
	public <T> T removeAttribute(
			final N5DirectoryPath path,
			final String attributePath,
			final Class<T> clazz) throws N5IOException, N5ClassCastException {

		final String normalizedAttributePath = N5URI.normalizeAttributePath(attributePath);
		T obj = removeAttribute(path, ZATTRS_FILE, normalizedAttributePath, clazz);
		if (obj == null)
			obj = removeAttribute(path, ZARRAY_FILE, normalizedAttributePath, clazz);
		if (obj == null)
			obj = removeAttribute(path, ZGROUP_FILE, normalizedAttributePath, clazz);
		return obj;
	}

	@Override
	public void setDatasetAttributes(
			final N5DirectoryPath path,
			final DatasetAttributes attributes) throws N5IOException {

		final ZArrayAttributes zarray = ((ZarrDatasetAttributes) attributes).getZArrayAttributes();
		final JsonElement json = gson.toJsonTree(zarray);
		store.writeAttributesJson(path, ZARRAY_FILE, json, gson);
	}

	@Override
	public void createDataset(
			final N5DirectoryPath path,
			final DatasetAttributes attributes) throws N5IOException {

		if (path.parent() != null)
			createGroup(path.parent());

		final ZArrayAttributes zarray = ((ZarrDatasetAttributes) attributes).getZArrayAttributes();
		final JsonElement json = gson.toJsonTree(zarray);
		store.writeAttributesJson(path, ZARRAY_FILE, json, gson);
	}

	@Override
	public void createGroup(
			final N5DirectoryPath path) throws N5IOException {

		// Avoid hitting the backend if this path is already a group according to the cache.
		// If path is a dataset then throw an exception to avoid overwriting / invalidating existing data.
		if (groupExists(path)) {
			return;
		} else if (datasetExists(path)) {
			throw new N5Exception("Can't make a group on existing dataset.");
		}

		if (path.parent() != null)
			createGroup(path.parent());

		store.writeAttributesJson(path, ZGROUP_FILE, groupAttr, gson);
	}

	@Override
	public boolean remove(
			final N5DirectoryPath path) throws N5IOException {

		if (store.isDirectory(path))
			store.removeDirectory(path);

		// an IOException should have occurred if anything had failed midway
		return true;
	}
}
