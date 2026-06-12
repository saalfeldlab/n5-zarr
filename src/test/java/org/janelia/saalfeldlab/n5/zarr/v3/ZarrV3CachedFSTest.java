package org.janelia.saalfeldlab.n5.zarr.v3;

import com.google.gson.GsonBuilder;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.KeyValueRootHierarchyStore;
import org.janelia.saalfeldlab.n5.HierarchyStoreCounters;
import org.janelia.saalfeldlab.n5.N5CachedFSTest.TrackingStorage;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.FileSystemKeyValueRoot;
import org.janelia.saalfeldlab.n5.KeyValueRoot;
import org.janelia.saalfeldlab.n5.TrackingHierarchyStore;
import org.janelia.saalfeldlab.n5.HierarchyStore;
import org.janelia.saalfeldlab.n5.cache.HierarchyCache;
import org.junit.Assert;
import org.junit.Test;

import static org.janelia.saalfeldlab.n5.HierarchyStoreCounters.assertEqualCounters;
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
		return new ZarrV3KeyValueWriter(new FileSystemKeyValueRoot(testDirPath), new GsonBuilder(), true);
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

		return new ZarrV3KeyValueReader(new FileSystemKeyValueRoot(location), gson, true);
	}

	protected N5Writer createTempN5Writer(
			final String location,
			final GsonBuilder gsonBuilder,
			final String dimensionSeparator) throws IOException {

		return new ZarrV3KeyValueWriter(new FileSystemKeyValueRoot(location), gsonBuilder, true);
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
		try (final ZarrV3TrackingStorage n5 = new ZarrV3TrackingStorage(new FileSystemKeyValueRoot(loc), new GsonBuilder(), true)) {
			zarrCacheBehaviorHelper(n5);
			n5.remove();
		}
	}

	public static void zarrCacheBehaviorHelper(final TrackingStorage n5) {

		// non existant group
		final String groupA = "groupA";
		final String groupB = "groupB";

		// expected backend method call counts
		final HierarchyStoreCounters expected = new HierarchyStoreCounters();
		n5.counters().reset();

		boolean exists = n5.exists(groupA);
		expected.incReadAttr(); // attributes (zarr.json) are read by exists() and cached
		boolean groupExists = n5.groupExists(groupA);
		boolean datasetExists = n5.datasetExists(groupA);
		assertFalse(exists); // group does not exist
		assertFalse(groupExists); // group does not exist
		assertFalse(datasetExists); // dataset does not exist
		assertEqualCounters(expected, n5.counters());

		n5.createGroup(groupA);
		// Needs attributes (zarr.json) are already cached to check for
		// existence (to make sure there is not already a dataset at this path.
		// No read, because attributes (zarr.json) are already cached.
		expected.incWriteAttr(); // attributes (zarr.json) are written (implies directory existence)
		assertEqualCounters(expected, n5.counters());

		// group B
		// attributes (zarr.json) are read by exists(), groupExists(), and datasetExists()
		exists = n5.exists(groupB);
		groupExists = n5.groupExists(groupB);
		datasetExists = n5.datasetExists(groupB);
		expected.incReadAttr();
		assertFalse(exists); // group now exists
		assertFalse(groupExists); // group now exists
		assertFalse(datasetExists); // dataset does not exist
		assertEqualCounters(expected, n5.counters());

		exists = n5.exists(groupA);
		groupExists = n5.groupExists(groupA);
		datasetExists = n5.datasetExists(groupA);
		assertTrue(exists); // group now exists
		assertTrue(groupExists); // group now exists
		assertFalse(datasetExists); // dataset does not exist
		assertEqualCounters(expected, n5.counters());

		final String cachedGroup = "cachedGroup";
		n5.createGroup(cachedGroup);
		// Needs attributes (zarr.json) are already cached to check for
		// existence (to make sure there is not already a dataset at this path.
		expected.incReadAttr();
		expected.incWriteAttr(); // attributes (zarr.json) are written (implies directory existence)
		assertEqualCounters(expected, n5.counters());
		n5.createGroup(cachedGroup); // be annoying
		assertEqualCounters(expected, n5.counters());

		// should not check existence when this instance created a group
		n5.exists(cachedGroup);
		n5.groupExists(cachedGroup);
		n5.datasetExists(cachedGroup);
		assertEqualCounters(expected, n5.counters());

		// zarr.json is cached, shouldn't read the attributes again, only write
		n5.setAttribute(cachedGroup, "one", 1);
		expected.incWriteAttr();
		assertEqualCounters(expected, n5.counters());

		n5.setAttribute(cachedGroup, "two", 2);
		expected.incWriteAttr();
		assertEqualCounters(expected, n5.counters());

		n5.list("");
		expected.incList();
		assertEqualCounters(expected, n5.counters());

		n5.list(cachedGroup);
		expected.incList();
		assertEqualCounters(expected, n5.counters());

		// Check existence for groups that have not been made by this reader but isGroup
		// and isDatatset must be false if it does not exists so then should not be
		// called.
		//
		// Similarly, attributes can not exist for a non-existent group, so should not
		// attempt to get attributes from the container.
		//
		// Finally,listing on a non-existent group is pointless, so don't call the
		// backend storage

		final String nonExistentGroup = "doesNotExist";
		n5.exists(nonExistentGroup);
		expected.incReadAttr();
		assertEqualCounters(expected, n5.counters());

		n5.groupExists(nonExistentGroup);
		n5.datasetExists(nonExistentGroup);
		n5.getAttributes(nonExistentGroup);
		assertEqualCounters(expected, n5.counters());

		assertThrows(N5Exception.class, () -> n5.list(nonExistentGroup));
		// NB: in principle, if non-existence of a group is cached, we don't
		// need to attempt to list it. However we want to be robust to not-quite
		// correct Zarr hierarchies (not all parent directories of a group need
		// to have .zgroup). Therefore, we try to list (the directory) anyway.
		expected.incList();
		assertEqualCounters(expected, n5.counters());

		final String a = "a";
		final String ab = "a/b";
		final String abc = "a/b/c";
		// create "a/b/c"
		n5.createGroup(abc);
		expected.incReadAttr(3);
		expected.incWriteAttr(3);
		assertEqualCounters(expected, n5.counters());
		assertTrue(n5.exists(abc));
		assertTrue(n5.groupExists(abc));
		assertFalse(n5.datasetExists(abc));
		assertEqualCounters(expected, n5.counters());

		// ensure that backend need not be checked when testing existence of "a/b"
		// TODO how does this work
		assertTrue(n5.exists(ab));
		assertTrue(n5.groupExists(ab));
		assertFalse(n5.datasetExists(ab));
		assertEqualCounters(expected, n5.counters());

		// remove a nested group
		// checks for all children should not require a backend check
		n5.remove(a);
		expected.incRmDir();
		assertFalse(n5.exists(a));
		assertFalse(n5.groupExists(a));
		assertFalse(n5.datasetExists(a));
		assertEqualCounters(expected, n5.counters());

		assertFalse(n5.exists(ab));
		assertFalse(n5.groupExists(ab));
		assertFalse(n5.datasetExists(ab));
		assertEqualCounters(expected, n5.counters());

		assertFalse(n5.exists(abc));
		assertFalse(n5.groupExists(abc));
		assertFalse(n5.datasetExists(abc));
		assertEqualCounters(expected, n5.counters());

		n5.createGroup("a");
		expected.incWriteAttr();
		assertEqualCounters(expected, n5.counters());
		n5.createGroup("a/a");
		expected.incReadAttr();
		expected.incWriteAttr();
//		expectedExistCount++;
//		expectedAttributeCount++;
		assertEqualCounters(expected, n5.counters());
		n5.createGroup("a/b");
		expected.incWriteAttr();
		assertEqualCounters(expected, n5.counters());
		n5.createGroup("a/c");
		expected.incReadAttr();
		expected.incWriteAttr();
		assertEqualCounters(expected, n5.counters());

		final Set<String> abcListSet = Arrays.stream(n5.list("a")).collect(Collectors.toSet());
		assertEquals(Stream.of("a", "b", "c").collect(Collectors.toSet()), abcListSet);
		expected.incList();
		assertEqualCounters(expected, n5.counters());

		// remove a
		n5.remove("a/a");
		expected.incRmDir();
		final Set<String> bc = Arrays.stream(n5.list("a")).collect(Collectors.toSet());
		assertEquals(Stream.of("b", "c").collect(Collectors.toSet()), bc);
		// list NOT incremented
		assertEqualCounters(expected, n5.counters());

		// TODO repeat the above exercise when creating dataset
	}

	public static class ZarrV3TrackingStorage extends ZarrV3KeyValueWriter implements TrackingStorage {

		private TrackingHierarchyStore trackingStore;

		public ZarrV3TrackingStorage(final KeyValueRoot keyValueRoot,
				final GsonBuilder gsonBuilder, final boolean cacheAttributes) {

			super(keyValueRoot, gsonBuilder, cacheAttributes);
		}

		@Override
		public HierarchyStore createHierarchyStore(
				final KeyValueRoot keyValueRoot,
				final boolean cacheMeta) {

			trackingStore = new TrackingHierarchyStore(new KeyValueRootHierarchyStore(keyValueRoot));
			return cacheMeta ? new HierarchyCache(trackingStore) : trackingStore;
		}

		@Override
		public HierarchyStoreCounters counters() {
			return trackingStore.counters();
		}
	}
}
