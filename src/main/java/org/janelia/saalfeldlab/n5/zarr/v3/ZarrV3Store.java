package org.janelia.saalfeldlab.n5.zarr.v3;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.Map;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.GsonUtils;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.N5Exception.N5ClassCastException;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.N5Exception.N5JsonParseException;
import org.janelia.saalfeldlab.n5.N5Path.N5GroupPath;
import org.janelia.saalfeldlab.n5.N5Store;
import org.janelia.saalfeldlab.n5.N5URI;
import org.janelia.saalfeldlab.n5.cache.DelegateStore;

import static org.janelia.saalfeldlab.n5.zarr.v3.ZarrV3DatasetAttributes.CHUNK_GRID_KEY;
import static org.janelia.saalfeldlab.n5.zarr.v3.ZarrV3DatasetAttributes.CHUNK_KEY_ENCODING_KEY;
import static org.janelia.saalfeldlab.n5.zarr.v3.ZarrV3DatasetAttributes.CODECS_KEY;
import static org.janelia.saalfeldlab.n5.zarr.v3.ZarrV3DatasetAttributes.DATA_TYPE_KEY;
import static org.janelia.saalfeldlab.n5.zarr.v3.ZarrV3DatasetAttributes.DIMENSION_NAMES_KEY;
import static org.janelia.saalfeldlab.n5.zarr.v3.ZarrV3DatasetAttributes.FILL_VALUE_KEY;
import static org.janelia.saalfeldlab.n5.zarr.v3.ZarrV3DatasetAttributes.SHAPE_KEY;
import static org.janelia.saalfeldlab.n5.zarr.v3.ZarrV3KeyValueReader.ZARR_KEY;
import static org.janelia.saalfeldlab.n5.zarr.v3.ZarrV3Node.ATTRIBUTES_KEY;
import static org.janelia.saalfeldlab.n5.zarr.v3.ZarrV3Node.NODE_TYPE_KEY;
import static org.janelia.saalfeldlab.n5.zarr.v3.ZarrV3Node.NodeType.GROUP;
import static org.janelia.saalfeldlab.n5.zarr.v3.ZarrV3Node.ZARR_FORMAT_KEY;

// TODO: Rename something like that: N5Store -> FormatStore / ZarrV3FormatStore ???
public final class ZarrV3Store implements N5Store {

	/**
	 * If {@code flattenAttributes == true} then group/array attributes will be
	 * merged into the "user attributes" nested under the {@code attributes/}
	 * key in {@code zarr.json} for {@link #getAttributes} and {@link
	 * #setAttributes}.
	 * <p>
	 * If {@code flattenAttributes == false} then only the "user attributes" are
	 * considered.
	 */
	private static final boolean flattenAttributes = true;

	private final DelegateStore store;
	private final Gson gson;
	private final JsonObject groupAttr;

	public ZarrV3Store(
			final DelegateStore store,
			final Gson gson) {
		this.store = store;
		this.gson = gson;

		groupAttr = new JsonObject();
		groupAttr.addProperty(ZarrV3Node.ZARR_FORMAT_KEY, ZarrV3KeyValueReader.ZARR_3_VERSION.getMajor());
		groupAttr.addProperty(NODE_TYPE_KEY, ZarrV3Node.NodeType.GROUP.toString());
	}

	@Override
	public <T> T getAttribute(
			final N5GroupPath path,
			final String key,
			final Type type) throws N5IOException, N5ClassCastException {

		final JsonElement attributes = store.store_readAttributesJson(path, ZARR_KEY, gson);
		final String insertionPath = insertionPath(N5URI.normalizeAttributePath(key));
		try {
			return GsonUtils.readAttribute(attributes, insertionPath, type, gson);
		} catch (JsonSyntaxException | NumberFormatException | ClassCastException e) {
			throw new N5ClassCastException(e);
		}
	}

	@Override
	public DatasetAttributes getDatasetAttributes(
			final N5GroupPath path) throws N5IOException {

		final JsonElement attributes = store.store_readAttributesJson(path, ZARR_KEY, gson);
		return gson.fromJson(attributes, ZarrV3DatasetAttributes.class);
	}

	@Override
	public boolean datasetExists(
			final N5GroupPath path) throws N5IOException {

		return getDatasetAttributes(path) != null;
	}

	private static ZarrV3Node.NodeType getNodeType(final JsonElement attributes) {

		if (attributes == null || !attributes.isJsonObject())
			return null;
		final JsonObject obj = attributes.getAsJsonObject();

		final JsonElement format = obj.get(ZARR_FORMAT_KEY);
		if (format == null || !format.isJsonPrimitive())
			return null;

		final JsonElement type = obj.get(NODE_TYPE_KEY);
		if (type == null || !type.isJsonPrimitive())
			return null;

		return ZarrV3Node.NodeType.of(type.getAsString());
	}

	@Override
	public boolean groupExists(
			final N5GroupPath path) throws N5IOException {

		final JsonElement attributes = store.store_readAttributesJson(path, ZARR_KEY, gson);
		return GROUP == getNodeType(attributes);
	}

	@Override
	public String[] list(
			final N5GroupPath group) throws N5IOException {

		return store.store_listDirectories(group);
	}

	@Override
	public Map<String, Class<?>> listAttributes(
			final N5GroupPath path) throws N5IOException, N5JsonParseException {

		return GsonUtils.listAttributes(getAttributes(path));
	}

	@Deprecated
	@Override
	public JsonElement getAttributes(final N5GroupPath path) throws N5IOException {

		final JsonElement json = store.store_readAttributesJson(path, ZARR_KEY, gson);
		if (json == null || !json.isJsonObject())
			return null;

		JsonObject obj = json.getAsJsonObject();
		if (!flattenAttributes)
			return obj.get(ATTRIBUTES_KEY);

		if (obj.has(ATTRIBUTES_KEY)) {
			final JsonElement attr = obj.remove(ATTRIBUTES_KEY);
			if (attr != null && attr.isJsonObject())
				attr.getAsJsonObject().asMap().forEach(obj::add);
		}
		return obj;
	}

	@Override
	public <T> void setAttribute(
			final N5GroupPath path,
			final String attributePath,
			final T attribute) throws N5IOException {

		setAttributes(path, Collections.singletonMap(attributePath, attribute));
	}

	/**
	 * Insert attribute into {@code root} element (root of parsed {@code
	 * zarr.json} file), trying to guess the correct insertion place based on
	 * the {@code key}:
	 * <p>
	 * If {@code key} is a (mandatory or optional) array or group attribute then
	 * {@code key} is the insertion path ({@code attribute} is inserted into the
	 * {@code root} at {@code key}). The same is true if {@code key} starts with
	 * an array or group attribute followed by "/" (for example {@code
	 * key="chunk_grid/configuration/chunk_shape"}). In particular, this holds
	 * also for keys starting with {@code "attributes/"}.
	 * <p>
	 * TODO: Certain N5 attributes are translated to the corresponding Zarr attributes.
	 *  <ul>
	 *      <li>"dimensions":</li>
	 *      <li>"blockSize":</li>
	 *      <li>"dataType":</li>
	 *      <li>"compression":</li>
	 *  </ul>
	 * <p>
	 * Otherwise, {@code key} is considered to be user metadata and the
	 * insertion path is {@code "attributes/" + key}.
	 *
	 * @param root
	 * 		root element
	 * @param key
	 * 		normalized attribute path
	 * @param attribute
	 * 		attribute value
	 * @param <T>
	 *
	 * @return
	 */
	private <T> JsonElement insertAttribute(
			JsonElement root,
			final String key,
			final T attribute) {

		// TODO: N5 attribute mapping, OR
		//       inline and move javadoc to #insertionPath.
		return GsonUtils.insertAttribute(root, insertionPath(key), attribute, gson);
	}

	private static String insertionPath(String key) {

		if (key.startsWith("/"))
			key = key.substring(1);

		final String k = key + "/";
		for (final String prefix : prefixes)
			if (k.startsWith(prefix))
				return key;
		return ATTRIBUTES_KEY + "/" + key;
	}

	private static final String[] prefixes = new String[] {
			ZARR_FORMAT_KEY + "/",
			ATTRIBUTES_KEY + "/",
			NODE_TYPE_KEY + "/",
			SHAPE_KEY + "/",
			FILL_VALUE_KEY + "/",
			DATA_TYPE_KEY + "/",
			CHUNK_GRID_KEY + "/",
			CHUNK_KEY_ENCODING_KEY + "/",
			DIMENSION_NAMES_KEY + "/",
			CODECS_KEY + "/",
	};

	@Override
	public void setAttributes(
			final N5GroupPath path,
			final Map<String, ?> attributes) throws N5IOException {

		JsonElement root = store.store_readAttributesJson(path, ZARR_KEY, gson);
		if (root == null || !root.isJsonObject())
			throw new N5IOException(String.format("Directory does not exist: %s", path));

		if (attributes == null || attributes.isEmpty())
			return;

		for (final Map.Entry<String, ?> attribute : attributes.entrySet())
			root = insertAttribute(root, N5URI.normalizeAttributePath(attribute.getKey()), attribute.getValue());

		store.store_writeAttributesJson(path, ZARR_KEY, root, gson);
	}

	@Override
	public boolean removeAttribute(
			final N5GroupPath path,
			final String attributePath) throws N5IOException {

		final JsonElement root = store.store_readAttributesJson(path, ZARR_KEY, gson);
		if (root == null)
			return false;

		final String insertionPath = insertionPath(N5URI.normalizeAttributePath(attributePath));
		if (null != GsonUtils.removeAttribute(root, insertionPath)) {
			store.store_writeAttributesJson(path, ZARR_KEY, root, gson);
			return true;
		}

		return false;
	}

	@Override
	public <T> T removeAttribute(
			final N5GroupPath path,
			final String attributePath,
			final Class<T> clazz) throws N5Exception {

		final JsonElement root = store.store_readAttributesJson(path, ZARR_KEY, gson);
		if (root == null)
			return null;

		final String insertionPath = insertionPath(N5URI.normalizeAttributePath(attributePath));
		final T obj;
		try {
			obj = GsonUtils.removeAttribute(root, insertionPath, clazz, gson);
		} catch (JsonSyntaxException | NumberFormatException | ClassCastException e) {
			throw new N5ClassCastException(e);
		}

		if (obj != null)
			store.store_writeAttributesJson(path, ZARR_KEY, root, gson);

		return obj;
	}

	@Override
	public void setDatasetAttributes(
			final N5GroupPath path,
			final DatasetAttributes attributes) throws N5IOException {

		JsonElement root = store.store_readAttributesJson(path, ZARR_KEY, gson);
		if (root == null || !root.isJsonObject())
			root = new JsonObject();

		// NB: We serialize to a JsonObject because ZarrV3DatasetAttributes.asMap()
		// is not overridden from DatasetAttributes, so will have the wrong attributes.
		final JsonElement json = gson.toJsonTree((ZarrV3DatasetAttributes) attributes);
		json.getAsJsonObject().asMap().forEach(root.getAsJsonObject()::add);

		store.store_writeAttributesJson(path, ZARR_KEY, root, gson);
	}

	@Override
	public void createGroup(
			final N5GroupPath path) throws N5IOException {

		// Avoid hitting the backend if this path is already a group according to the cache.
		// If path is a dataset then throw an exception to avoid overwriting / invalidating existing data.
		if (groupExists(path)) {
			return;
		} else if (datasetExists(path)) {
			throw new N5Exception("Can't make a group on existing dataset.");
		}

		if (path.parent() != null)
			createGroup(path.parent());

		store.store_writeAttributesJson(path, ZARR_KEY, groupAttr, gson);
	}

	@Override
	public void createDataset(
			final N5GroupPath path,
			final DatasetAttributes attributes) throws N5IOException {

		// TODO: revise createDataset / setDatasetAttributes / setAttributes
		//       implemenetation and javadoc
		//       to make it clear when something is created and when non-existence is an error.
		setDatasetAttributes(path, attributes);
	}

	@Override
	public boolean remove(
			final N5GroupPath path) throws N5IOException {

		if (store.store_isDirectory(path))
			store.store_removeDirectory(path);

		// an IOException should have occurred if anything had failed midway
		return true;
	}
}
