package org.janelia.saalfeldlab.n5.zarr.cache;

import org.janelia.saalfeldlab.n5.cache.N5JsonCache;
import org.janelia.saalfeldlab.n5.cache.N5JsonCacheableContainer;
import org.janelia.saalfeldlab.n5.zarr.ZarrKeyValueReader;

import com.google.gson.JsonElement;

public class ZarrJsonCache extends N5JsonCache {

	public ZarrJsonCache(final N5JsonCacheableContainer container) {
		super(container);
	}

	@Override
	public void updateCacheInfo(final String normalPathKey, final String normalCacheKey, final JsonElement uncachedAttributes) {

		N5CacheInfo cacheInfo = getCacheInfo(normalPathKey);
		if (cacheInfo == null ){
			addNewCacheInfo(normalPathKey, normalCacheKey, uncachedAttributes);
			return;
		}

		if( cacheInfo == emptyCacheInfo )
			cacheInfo = newCacheInfo();

		if (normalCacheKey != null) {
			final JsonElement attributesToCache = uncachedAttributes == null
					? container.getAttributesFromContainer(normalPathKey, normalCacheKey)
					: uncachedAttributes;

			updateCacheAttributes(cacheInfo, normalCacheKey, attributesToCache);

			// if this path is a group, it it not a dataset
			// if this path is a dataset, it it not a group
			if (normalCacheKey.equals(ZarrKeyValueReader.ZGROUP_FILE)) {
				if (container.isGroupFromAttributes(normalCacheKey, attributesToCache)) {
					updateCacheIsGroup(cacheInfo, true);
					updateCacheIsDataset(cacheInfo, false);
				}
			} else if (normalCacheKey.equals(ZarrKeyValueReader.ZARRAY_FILE)) {
				if (container.isDatasetFromAttributes(normalCacheKey, attributesToCache)) {
					updateCacheIsGroup(cacheInfo, false);
					updateCacheIsDataset(cacheInfo, true);
				}
			}
		}
		else {
			updateCacheIsGroup(cacheInfo, container.isGroupFromContainer(normalPathKey));
			updateCacheIsDataset(cacheInfo, container.isDatasetFromContainer(normalPathKey));
		}
		updateCache(normalPathKey, cacheInfo);
	}

	@Override public N5CacheInfo addNewCacheInfo(String normalPathKey, String normalCacheKey, JsonElement uncachedAttributes) {

		return cacheGroupAndDataset(normalPathKey);
	}

	@Deprecated
	public N5CacheInfo forceAddNewCacheInfo(final String normalPathKey, final String normalCacheKey, final JsonElement uncachedAttributes,
			final boolean isGroup, final boolean isDataset) {

		// getting the current cache info is useful if it has a list of children
		N5CacheInfo cacheInfo = getCacheInfo(normalPathKey);
		if (cacheInfo == null || cacheInfo == emptyCacheInfo)
			cacheInfo = newCacheInfo();

		// initialize cache keys to null, those that exist will be set later
		// and having null's in the cache avoid backend calls for nonexisting files
		updateCacheAttributes(cacheInfo, ZarrKeyValueReader.ZGROUP_FILE, null);
		updateCacheAttributes(cacheInfo, ZarrKeyValueReader.ZARRAY_FILE, null);
		updateCacheAttributes(cacheInfo, ZarrKeyValueReader.ZATTRS_FILE, null);

		if (normalCacheKey != null)
			updateCacheAttributes(cacheInfo, normalCacheKey, uncachedAttributes);

		updateCacheIsGroup(cacheInfo, isGroup);
		updateCacheIsDataset(cacheInfo, isDataset);
		updateCache(normalPathKey, cacheInfo);

		return cacheInfo;
	}

	@Override
	public JsonElement getAttributes(final String normalPathKey, final String normalCacheKey) {

		N5CacheInfo cacheInfo = getCacheInfo(normalPathKey);
		if (cacheInfo == null) {
			cacheGroupAndDataset(normalPathKey);
			cacheInfo = getCacheInfo(normalPathKey);
		}

		if (cacheInfo == emptyCacheInfo || cacheInfo.getCache(normalCacheKey) == emptyJson) {
			return null;
		}
		synchronized (cacheInfo) {
			if (!cacheInfo.containsKey(normalCacheKey)) {
				updateCacheInfo(normalPathKey, normalCacheKey, null);
			}
		}

		final JsonElement output = cacheInfo.getCache(normalCacheKey);
		return output == null ? null : output.deepCopy();
	}

	@Override
	public boolean isDataset(final String normalPathKey, final String normalCacheKey) {

		N5CacheInfo cacheInfo = getCacheInfo(normalPathKey);
		if (cacheInfo == null) {
			cacheGroupAndDataset(normalPathKey);
			cacheInfo = getCacheInfo(normalPathKey);
		}
		else if (cacheInfo == emptyCacheInfo || cacheInfo.isGroup())
			return cacheInfo.isDataset();
		else if (!cacheInfo.containsKey(ZarrKeyValueReader.ZARRAY_FILE)) {
			// if the cache info is not tracking .zarray, then we don't yet know
			// if there is a dataset at this path key
			updateCacheIsDataset(cacheInfo, container.isDatasetFromContainer(normalPathKey));
		}

		return cacheInfo.isDataset();
	}

	@Override
	public boolean isGroup(final String normalPathKey, final String normalCacheKey) {

		N5CacheInfo cacheInfo = getCacheInfo(normalPathKey);
		if (cacheInfo == null) {
			cacheGroupAndDataset(normalPathKey);
			cacheInfo = getCacheInfo(normalPathKey);
		}
		else if (cacheInfo == emptyCacheInfo || cacheInfo.isDataset())
			return cacheInfo.isGroup();
		else if (!cacheInfo.containsKey(ZarrKeyValueReader.ZGROUP_FILE)) {
			// if the cache info is not tracking .zgroup, then we don't yet know
			// if there is a group at this path key
			updateCacheIsGroup(cacheInfo, container.isGroupFromContainer(normalPathKey));
		}

		return cacheInfo.isGroup();
	}

	public N5CacheInfo cacheGroupAndDataset(final String normalPathKey) {

		final JsonElement zgroup = container.getAttributesFromContainer(normalPathKey, ZarrKeyValueReader.ZGROUP_FILE);
		final boolean isGroup = container.isGroupFromAttributes(normalPathKey, zgroup);

		final JsonElement zarray = container.getAttributesFromContainer(normalPathKey, ZarrKeyValueReader.ZARRAY_FILE);
		final boolean isDataset = container.isGroupFromAttributes(normalPathKey, zarray);

		if( isGroup || isDataset ) {

			final N5CacheInfo cacheInfo = newCacheInfo();

			updateCacheIsGroup(cacheInfo, isGroup);
			updateCacheAttributes(cacheInfo, ZarrKeyValueReader.ZGROUP_FILE, zgroup);

			updateCacheIsDataset(cacheInfo, isDataset);
			updateCacheAttributes(cacheInfo, ZarrKeyValueReader.ZARRAY_FILE, zarray);

			updateCache(normalPathKey, cacheInfo);
			return cacheInfo;
		}

		updateCache(normalPathKey, emptyCacheInfo);
		return emptyCacheInfo;
	}

	public N5CacheInfo cacheAttributes(final String normalPathKey, final String normalCacheKey) {

		final N5CacheInfo cacheInfo = getCacheInfo(normalPathKey);
		if (cacheInfo != null) {
			final JsonElement zattrs = container.getAttributesFromContainer(normalPathKey,
					normalCacheKey);
			updateCacheAttributes(cacheInfo, normalCacheKey, zattrs);
		}

		return cacheInfo;
	}

}
