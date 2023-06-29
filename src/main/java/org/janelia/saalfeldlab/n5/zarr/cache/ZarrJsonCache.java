package org.janelia.saalfeldlab.n5.zarr.cache;

import org.janelia.saalfeldlab.n5.cache.N5JsonCache;
import org.janelia.saalfeldlab.n5.cache.N5JsonCacheableContainer;
import org.janelia.saalfeldlab.n5.zarr.ZarrKeyValueReader;

import com.google.gson.JsonElement;

public class ZarrJsonCache extends N5JsonCache {

	public ZarrJsonCache(N5JsonCacheableContainer container) {
		super(container);
	}

	@Override
	public void updateCacheInfo(final String normalPathKey, final String normalCacheKey, final JsonElement uncachedAttributes) {

		N5CacheInfo cacheInfo = getCacheInfo(normalPathKey);
		if (cacheInfo == null ){
			addNewCacheInfo(normalPathKey, normalCacheKey, uncachedAttributes );
			return;
		}

		// TODO revisit pending N5JsonCache changes
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

	public N5CacheInfo forceAddNewCacheInfo(String normalPathKey, String normalCacheKey, JsonElement uncachedAttributes,
			boolean isGroup, boolean isDataset) {

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

}
