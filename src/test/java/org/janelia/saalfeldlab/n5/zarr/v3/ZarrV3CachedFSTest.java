package org.janelia.saalfeldlab.n5.zarr.v3;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.FileSystemKeyValueAccess;
import org.janelia.saalfeldlab.n5.KeyValueAccess;
import org.janelia.saalfeldlab.n5.N5CachedFSTest.TrackingStorage;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class ZarrV3CachedFSTest extends ZarrV3Test {

	@Override
	protected String tempN5Location() {

		try {
			return Files.createTempDirectory("n5-zarr-cached-test").toUri().getPath();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	protected N5Writer createN5Writer() {

		final String testDirPath = tempN5Location();
		return new ZarrV3KeyValueWriter(new FileSystemKeyValueAccess(FileSystems.getDefault()), testDirPath, new GsonBuilder(), true, true, "/", true);
	}

	protected N5Writer createTempN5Writer(final boolean cacheAttributes) {

		return createTempN5Writer(tempN5PathName(), new GsonBuilder(), "/",  cacheAttributes);
	}

	@Override
	protected N5Writer createN5Writer(final String location, final GsonBuilder gsonBuilder) throws IOException {

		return createTempN5Writer(location, gsonBuilder, "/");
	}

	@Override
	protected N5Writer createTempN5Writer(final String location, final String dimensionSeparator) throws IOException {

		return createTempN5Writer(location, new GsonBuilder(), dimensionSeparator);
	}

	@Override
	protected N5Reader createN5Reader(final String location, final GsonBuilder gson) throws IOException {

		return new ZarrV3KeyValueReader(new FileSystemKeyValueAccess(FileSystems.getDefault()), location, gson, false, false, true);
	}

	protected N5Writer createTempN5Writer(
			final String location,
			final GsonBuilder gsonBuilder,
			final String dimensionSeparator) throws IOException {

		return new ZarrV3KeyValueWriter(new FileSystemKeyValueAccess(FileSystems.getDefault()), location, gsonBuilder, dimensionSeparator, true);
	}

	protected static String tempN5PathName() {
		try {
			final File tmpFile = Files.createTempDirectory("zarr-cached-test-").toFile();
			tmpFile.deleteOnExit();
			return tmpFile.getCanonicalPath();
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Test
	public void cachedRootDatasetTest() throws IOException {

		final String testDirPath = tempN5Location();
		try (N5Writer writer = createN5Writer( testDirPath, new GsonBuilder() )) {
			writer.createDataset("/", dimensions, blockSize, DataType.UINT8, getCompressions()[0]);
			assertTrue( writer.exists("/"));
		}

		try (ZarrV3KeyValueReader reader = (ZarrV3KeyValueReader) createN5Reader( testDirPath, new GsonBuilder() )) {
			assertTrue( reader.exists("/"));
		}
	}

	@Test
	public void cacheTest() throws IOException {
		/* Test the cache by setting many attributes, then manually deleting the underlying file.
		* The only possible way for the test to succeed is if it never again attempts to read the file, and relies on the cache. */

		final String cachedGroup = "cachedGroup";
		try (ZarrV3KeyValueWriter zarr = (ZarrV3KeyValueWriter) createTempN5Writer()) {
			zarr.createGroup(cachedGroup);
			final String attributesPath = new File(zarr.getURI()).toPath()
					.resolve(cachedGroup)
					.resolve(ZarrV3KeyValueReader.ZARR_KEY)
					.toAbsolutePath().toString();

			final ArrayList<TestData<?>> tests = new ArrayList<>();
			addAndTest(zarr, tests, new TestData<>(cachedGroup, "a/b/c", 100));
			addAndTest(zarr, tests, new TestData<>(cachedGroup, "a/a[5]", "asdf"));
			addAndTest(zarr, tests, new TestData<>(cachedGroup, "a/a[2]", 0));

			Files.delete(Paths.get(attributesPath));
			runTests(zarr, tests);
		}

		try (final ZarrV3KeyValueWriter zarr = (ZarrV3KeyValueWriter)createTempN5Writer(false)) {
			zarr.createGroup(cachedGroup);

			final String attributesPath = new File(zarr.getURI()).toPath()
					.resolve(cachedGroup)
					.resolve(ZarrV3KeyValueReader.ZARR_KEY)
					.toAbsolutePath().toString();


			final ArrayList<TestData<?>> tests = new ArrayList<>();
			addAndTest(zarr, tests, new TestData<>(cachedGroup, "a/b/c", 100));
			addAndTest(zarr, tests, new TestData<>(cachedGroup, "a/a[5]", "asdf"));
			addAndTest(zarr, tests, new TestData<>(cachedGroup, "a/a[2]", 0));

			Files.delete(Paths.get(attributesPath));
			Assert.assertThrows(AssertionError.class, () -> runTests(zarr, tests));
			zarr.remove();
		}
	}

	@Test
	public void cacheBehaviorTest() {

		final String loc = tempN5Location();
		// make an uncached n5 writer
		final FileSystemKeyValueAccess keyValueAccess = new FileSystemKeyValueAccess(FileSystems.getDefault());
		try (final ZarrV3TrackingStorage n5 = new ZarrV3TrackingStorage(keyValueAccess, loc, new GsonBuilder(), true)) {

			zarrCacheBehaviorHelper(n5);
			n5.remove();
		}
	}

	public static void zarrCacheBehaviorHelper(final TrackingStorage n5) {

		// non existant group
		final String groupA = "groupA";
		final String groupB = "groupB";

		// expected backend method call counts
		int expectedExistCount = 0;
		final int expectedGroupCount = 0;
		final int expectedDatasetCount = 0;
		int expectedAttributeCount = 0; // isGroup and isDataset are called when creating the reader
		int expectedListCount = 0;

		boolean exists = n5.exists(groupA);
		expectedExistCount++;

		boolean groupExists = n5.groupExists(groupA);
		expectedAttributeCount++; // attributes (zarr.json) are called by groupExists and cached
		boolean datasetExists = n5.datasetExists(groupA);
		assertFalse(exists); // group does not exist
		assertFalse(groupExists); // group does not exist
		assertFalse(datasetExists); // dataset does not exist
		assertEquals(expectedExistCount, n5.getExistCallCount());
		assertEquals(expectedGroupCount, n5.getGroupCallCount());
		assertEquals(expectedDatasetCount, n5.getDatasetCallCount());
		assertEquals(expectedAttributeCount, n5.getAttrCallCount());

		n5.createGroup(groupA);
		assertEquals(expectedAttributeCount, n5.getAttrCallCount());

		// group B
		exists = n5.exists(groupB);
		expectedExistCount++;
		groupExists = n5.groupExists(groupB);
		datasetExists = n5.datasetExists(groupB);
		expectedAttributeCount++; // attributes (zarr.json) are called by groupExists and datasetExists
		assertFalse(exists); // group now exists
		assertFalse(groupExists); // group now exists
		assertFalse(datasetExists); // dataset does not exist
		assertEquals(expectedExistCount, n5.getExistCallCount());
		assertEquals(expectedGroupCount, n5.getGroupCallCount());
		assertEquals(expectedDatasetCount, n5.getDatasetCallCount());
		assertEquals(expectedAttributeCount, n5.getAttrCallCount());

		exists = n5.exists(groupA);
		groupExists = n5.groupExists(groupA);
		datasetExists = n5.datasetExists(groupA);
		assertTrue(exists); // group now exists
		assertTrue(groupExists); // group now exists
		assertFalse(datasetExists); // dataset does not exist
		assertEquals(expectedExistCount, n5.getExistCallCount());
		assertEquals(expectedGroupCount, n5.getGroupCallCount());
		assertEquals(expectedDatasetCount, n5.getDatasetCallCount());
		assertEquals(expectedAttributeCount, n5.getAttrCallCount());

		final String cachedGroup = "cachedGroup";
		// should not check existence when creating a group (TODO: Is this true for zarr v3?)
		n5.createGroup(cachedGroup);
		expectedExistCount++;
		expectedAttributeCount++; // createGroup calls isGroup and isDataset
		n5.createGroup(cachedGroup); // be annoying
		assertEquals(expectedExistCount, n5.getExistCallCount());
		assertEquals(expectedGroupCount, n5.getGroupCallCount());
		assertEquals(expectedDatasetCount, n5.getDatasetCallCount());
		assertEquals(expectedAttributeCount, n5.getAttrCallCount());
		assertEquals(expectedListCount, n5.getListCallCount());

		// should not check existence when this instance created a group
		n5.exists(cachedGroup);
		n5.groupExists(cachedGroup);
		n5.datasetExists(cachedGroup);
		assertEquals(expectedExistCount, n5.getExistCallCount());
		assertEquals(expectedGroupCount, n5.getGroupCallCount());
		assertEquals(expectedDatasetCount, n5.getDatasetCallCount());
		assertEquals(expectedAttributeCount, n5.getAttrCallCount());
		assertEquals(expectedListCount, n5.getListCallCount());

		// zarr.json is cached, shouldn't increment the expectedAttributeCount
		n5.setAttribute(cachedGroup, "one", 1);
		assertEquals(expectedExistCount, n5.getExistCallCount());
		assertEquals(expectedGroupCount, n5.getGroupCallCount());
		assertEquals(expectedDatasetCount, n5.getDatasetCallCount());
		assertEquals(expectedAttributeCount, n5.getAttrCallCount());
		assertEquals(expectedListCount, n5.getListCallCount());

		n5.setAttribute(cachedGroup, "two", 2);
		assertEquals(expectedExistCount, n5.getExistCallCount());
		assertEquals(expectedGroupCount, n5.getGroupCallCount());
		assertEquals(expectedDatasetCount, n5.getDatasetCallCount());
		assertEquals(expectedAttributeCount, n5.getAttrCallCount());
		assertEquals(expectedListCount, n5.getListCallCount());

		n5.list("");
		assertEquals(expectedExistCount, n5.getExistCallCount());
		assertEquals(expectedGroupCount, n5.getGroupCallCount());
		assertEquals(expectedDatasetCount, n5.getDatasetCallCount());
		assertEquals(expectedAttributeCount, n5.getAttrCallCount());
		assertEquals(++expectedListCount, n5.getListCallCount());

		n5.list(cachedGroup);
		assertEquals(expectedExistCount, n5.getExistCallCount());
		assertEquals(expectedGroupCount, n5.getGroupCallCount());
		assertEquals(expectedDatasetCount, n5.getDatasetCallCount());
		assertEquals(expectedAttributeCount, n5.getAttrCallCount());
		assertEquals(++expectedListCount, n5.getListCallCount());

		/*
		 * Check existence for groups that have not been made by this reader but isGroup
		 * and isDatatset must be false if it does not exists so then should not be
		 * called.
		 *
		 * Similarly, attributes can not exist for a non-existent group, so should not
		 * attempt to get attributes from the container.
		 *
		 * Finally,listing on a non-existent group is pointless, so don't call the
		 * backend storage
		 */
		final String nonExistentGroup = "doesNotExist";
		n5.exists(nonExistentGroup);
		expectedExistCount++;
		expectedAttributeCount++; // exists calls isGroup and isDataset
		assertEquals(expectedExistCount, n5.getExistCallCount());
		assertEquals(expectedGroupCount, n5.getGroupCallCount());
		assertEquals(expectedDatasetCount, n5.getDatasetCallCount());
		assertEquals(expectedAttributeCount, n5.getAttrCallCount());
		assertEquals(expectedListCount, n5.getListCallCount());

		n5.groupExists(nonExistentGroup);
		assertEquals(expectedExistCount, n5.getExistCallCount());
		assertEquals(expectedGroupCount, n5.getGroupCallCount());
		assertEquals(expectedDatasetCount, n5.getDatasetCallCount());
		assertEquals(expectedAttributeCount, n5.getAttrCallCount());
		assertEquals(expectedListCount, n5.getListCallCount());

		n5.datasetExists(nonExistentGroup);
		assertEquals(expectedExistCount, n5.getExistCallCount());
		assertEquals(expectedGroupCount, n5.getGroupCallCount());
		assertEquals(expectedDatasetCount, n5.getDatasetCallCount());
		assertEquals(expectedAttributeCount, n5.getAttrCallCount());
		assertEquals(expectedListCount, n5.getListCallCount());

		n5.getAttributes(nonExistentGroup);
		assertEquals(expectedExistCount, n5.getExistCallCount());
		assertEquals(expectedGroupCount, n5.getGroupCallCount());
		assertEquals(expectedDatasetCount, n5.getDatasetCallCount());
		assertEquals(expectedAttributeCount, n5.getAttrCallCount());
		assertEquals(expectedListCount, n5.getListCallCount());

		assertThrows(N5Exception.class, () -> n5.list(nonExistentGroup));
		assertEquals(expectedExistCount, n5.getExistCallCount());
		assertEquals(expectedGroupCount, n5.getGroupCallCount());
		assertEquals(expectedDatasetCount, n5.getDatasetCallCount());
		assertEquals(expectedAttributeCount, n5.getAttrCallCount());
		assertEquals(expectedListCount, n5.getListCallCount());

		final String a = "a";
		final String ab = "a/b";
		final String abc = "a/b/c";
		// create "a/b/c"
		n5.createGroup(abc);
		expectedAttributeCount+=3; // createGroup calls isGroup and isDataset
		expectedExistCount+=3; // createGroup calls isGroup and isDataset
		assertTrue(n5.exists(abc));
		assertTrue(n5.groupExists(abc));
		assertFalse(n5.datasetExists(abc));
		assertEquals(expectedExistCount, n5.getExistCallCount());
		assertEquals(expectedGroupCount, n5.getGroupCallCount());
		assertEquals(expectedDatasetCount, n5.getDatasetCallCount());
		assertEquals(expectedAttributeCount, n5.getAttrCallCount());
		assertEquals(expectedListCount, n5.getListCallCount());

		// ensure that backend need not be checked when testing existence of "a/b"
		// TODO how does this work
		assertTrue(n5.exists(ab));
		assertTrue(n5.groupExists(ab));
		assertFalse(n5.datasetExists(ab));
		assertEquals(expectedExistCount, n5.getExistCallCount());
		assertEquals(expectedGroupCount, n5.getGroupCallCount());
		assertEquals(expectedDatasetCount, n5.getDatasetCallCount());
		assertEquals(expectedAttributeCount, n5.getAttrCallCount());
		assertEquals(expectedListCount, n5.getListCallCount());

		// remove a nested group
		// checks for all children should not require a backend check
		n5.remove(a);
		assertFalse(n5.exists(a));
		assertFalse(n5.groupExists(a));
		assertFalse(n5.datasetExists(a));
		assertEquals(expectedExistCount, n5.getExistCallCount());
		assertEquals(expectedGroupCount, n5.getGroupCallCount());
		assertEquals(expectedDatasetCount, n5.getDatasetCallCount());
		assertEquals(expectedAttributeCount, n5.getAttrCallCount());
		assertEquals(expectedListCount, n5.getListCallCount());

		assertFalse(n5.exists(ab));
		assertFalse(n5.groupExists(ab));
		assertFalse(n5.datasetExists(ab));
		assertEquals(expectedExistCount, n5.getExistCallCount());
		assertEquals(expectedGroupCount, n5.getGroupCallCount());
		assertEquals(expectedDatasetCount, n5.getDatasetCallCount());
		assertEquals(expectedAttributeCount, n5.getAttrCallCount());
		assertEquals(expectedListCount, n5.getListCallCount());

		assertFalse(n5.exists(abc));
		assertFalse(n5.groupExists(abc));
		assertFalse(n5.datasetExists(abc));
		assertEquals(expectedExistCount, n5.getExistCallCount());
		assertEquals(expectedGroupCount, n5.getGroupCallCount());
		assertEquals(expectedDatasetCount, n5.getDatasetCallCount());
		assertEquals(expectedAttributeCount, n5.getAttrCallCount());
		assertEquals(expectedListCount, n5.getListCallCount());

		n5.createGroup("a");
		assertEquals(expectedExistCount, n5.getExistCallCount());
		assertEquals(expectedAttributeCount, n5.getAttrCallCount());
		n5.createGroup("a/a");
		expectedExistCount++;
		expectedAttributeCount++;
		assertEquals(expectedExistCount, n5.getExistCallCount());
		assertEquals(expectedAttributeCount, n5.getAttrCallCount());
		n5.createGroup("a/b");
		assertEquals(expectedExistCount, n5.getExistCallCount());
		assertEquals(expectedAttributeCount, n5.getAttrCallCount());
		n5.createGroup("a/c");
		expectedExistCount++;
		expectedAttributeCount++;
		assertEquals(expectedExistCount, n5.getExistCallCount());
		assertEquals(expectedAttributeCount, n5.getAttrCallCount());

		assertArrayEquals(new String[] {"a", "b", "c"}, n5.list("a")); // call list
		assertEquals(expectedGroupCount, n5.getGroupCallCount());
		assertEquals(expectedDatasetCount, n5.getDatasetCallCount());
		assertEquals(expectedAttributeCount, n5.getAttrCallCount());
		assertEquals(++expectedListCount, n5.getListCallCount()); // list incremented

		// remove a
		n5.remove("a/a");
		assertArrayEquals(new String[] {"b", "c"}, n5.list("a")); // call list
		assertEquals(expectedExistCount, n5.getExistCallCount());
		assertEquals(expectedGroupCount, n5.getGroupCallCount());
		assertEquals(expectedDatasetCount, n5.getDatasetCallCount());
		assertEquals(expectedAttributeCount, n5.getAttrCallCount());
		assertEquals(expectedListCount, n5.getListCallCount()); // list NOT incremented

		// TODO repeat the above exercise when creating dataset
	}

	public static class ZarrV3TrackingStorage extends ZarrV3KeyValueWriter implements TrackingStorage {

		public int attrCallCount = 0;
		public int existsCallCount = 0;
		public int groupCallCount = 0;
		public int groupAttrCallCount = 0;
		public int datasetCallCount = 0;
		public int datasetAttrCallCount = 0;
		public int listCallCount = 0;
		public int writeAttrCallCount = 0;

		public ZarrV3TrackingStorage(final KeyValueAccess keyValueAccess, final String basePath,
				final GsonBuilder gsonBuilder, final boolean cacheAttributes) {

			super(keyValueAccess, basePath, gsonBuilder, true, true, "/", cacheAttributes);
		}

		@Override
		public JsonElement getAttributesFromContainer(final String key, final String cacheKey) {
			attrCallCount++;
			return super.getAttributesFromContainer(key, cacheKey);
		}

		@Override
		public boolean existsFromContainer(final String path, final String cacheKey) {
			existsCallCount++;
			return super.existsFromContainer(path, cacheKey);
		}

		@Override
		public boolean isGroupFromContainer(final String key) {
			groupCallCount++;
			return super.isGroupFromContainer(key);
		}

		@Override
		public boolean isGroupFromAttributes(final String normalCacheKey, final JsonElement attributes) {
			groupAttrCallCount++;
			return super.isGroupFromAttributes(normalCacheKey, attributes);
		}

		@Override
		public boolean isDatasetFromContainer(final String key) {
			datasetCallCount++;
			return super.isDatasetFromContainer(key);
		}

		@Override
		public boolean isDatasetFromAttributes(final String normalCacheKey, final JsonElement attributes) {
			datasetAttrCallCount++;
			return super.isDatasetFromAttributes(normalCacheKey, attributes);
		}

		@Override
		public String[] listFromContainer(final String key) {
			listCallCount++;
			return super.listFromContainer(key);
		}

		@Override public void writeAttributes(final String normalGroupPath, final JsonElement attributes) throws N5Exception {
			writeAttrCallCount++;
			super.writeAttributes(normalGroupPath, attributes);
		}

		@Override
		public int getAttrCallCount() {
			return attrCallCount;
		}

		@Override
		public int getExistCallCount() {
			return existsCallCount;
		}

		@Override
		public int getGroupCallCount() {
			return groupCallCount;
		}

		@Override
		public int getGroupAttrCallCount() {
			return groupAttrCallCount;
		}

		@Override
		public int getDatasetCallCount() {
			return datasetCallCount;
		}

		@Override
		public int getDatasetAttrCallCount() {
			return datasetAttrCallCount;
		}

		@Override
		public int getListCallCount() {
			return listCallCount;
		}

		@Override public int getWriteAttrCallCount() {
			return writeAttrCallCount;
		}
	}
}
