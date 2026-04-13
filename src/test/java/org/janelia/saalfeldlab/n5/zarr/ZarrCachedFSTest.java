/*-
 * #%L
 * Not HDF5
 * %%
 * Copyright (C) 2019 - 2025 Stephan Saalfeld
 * %%
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDERS OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 * #L%
 */
package org.janelia.saalfeldlab.n5.zarr;

import static org.janelia.saalfeldlab.n5.MetaStoreCounters.assertEqualCounters;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.KeyValueAccessMetaStore;
import org.janelia.saalfeldlab.n5.MetaStoreCounters;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5CachedFSTest.TrackingStorage;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.RootedFileSystemKeyValueAccess;
import org.janelia.saalfeldlab.n5.RootedKeyValueAccess;
import org.janelia.saalfeldlab.n5.TrackingMetaStore;
import org.janelia.saalfeldlab.n5.cache.DelegateStore;
import org.janelia.saalfeldlab.n5.cache.MyJsonCache;
import org.junit.Assert;
import org.junit.Test;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;

public class ZarrCachedFSTest extends N5ZarrTest {

	@Override
	protected String tempN5Location() {

		try {
			return Files.createTempDirectory("n5-zarr-cached-test").toUri().getPath();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	protected N5ZarrWriter createN5Writer() {

		final String testDirPath = tempN5Location();
		return new N5ZarrWriter(testDirPath, new GsonBuilder(), ".", true, true);
	}

	protected N5Writer createTempN5Writer(final boolean cacheAttributes) throws IOException {

		return createTempN5Writer(tempN5PathName(), new GsonBuilder(), ".", true,  cacheAttributes);
	}

	@Override
	protected N5ZarrWriter createN5Writer(final String location, final GsonBuilder gsonBuilder) throws IOException {

		return createTempN5Writer(location, gsonBuilder, ".", true);
	}

	@Override
	protected N5ZarrWriter createTempN5Writer(final String location, final String dimensionSeparator) throws IOException {

		return createTempN5Writer(location, new GsonBuilder(), dimensionSeparator, true);
	}

	@Override
	protected N5Reader createN5Reader(final String location, final GsonBuilder gson) throws IOException {

		return new N5ZarrReader(location, gson, true);
	}

	@Override
	protected N5ZarrWriter createTempN5Writer(
			final String location,
			final GsonBuilder gsonBuilder,
			final String dimensionSeparator,
			final boolean mapN5DatasetAttributes) throws IOException {

		return new N5ZarrWriter(location, gsonBuilder, dimensionSeparator, mapN5DatasetAttributes, true);
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
		try (ZarrKeyValueWriter writer = (ZarrKeyValueWriter) createN5Writer( testDirPath, new GsonBuilder() )) {
			writer.createDataset("/", dimensions, blockSize, DataType.UINT8, getCompressions()[0]);
			assertTrue( writer.exists("/"));
		}

		try (ZarrKeyValueReader reader = (ZarrKeyValueReader) createN5Reader( testDirPath, new GsonBuilder() )) {
			assertTrue( reader.exists("/"));
		}
	}

	@Test
	public void cacheTest() throws IOException, URISyntaxException {
		/* Test the cache by setting many attributes, then manually deleting the underlying file.
		* The only possible way for the test to succeed is if it never again attempts to read the file, and relies on the cache. */

		final String cachedGroup = "cachedGroup";
		try (ZarrKeyValueWriter zarr = (ZarrKeyValueWriter) createTempN5Writer()) {
			zarr.createGroup(cachedGroup);
			final String attributesPath = new File(zarr.getURI()).toPath()
					.resolve(cachedGroup)
					.resolve(ZarrKeyValueReader.ZATTRS_FILE)
					.toAbsolutePath().toString();

			final ArrayList<TestData<?>> tests = new ArrayList<>();
			addAndTest(zarr, tests, new TestData<>(cachedGroup, "a/b/c", 100));
			addAndTest(zarr, tests, new TestData<>(cachedGroup, "a/a[5]", "asdf"));
			addAndTest(zarr, tests, new TestData<>(cachedGroup, "a/a[2]", 0));

			Files.delete(Paths.get(attributesPath));
			runTests(zarr, tests);
		}

		try (final ZarrKeyValueWriter zarr = (ZarrKeyValueWriter) createTempN5Writer(false)) {
			zarr.createGroup(cachedGroup);

			final String attributesPath = new File(zarr.getURI()).toPath()
					.resolve(cachedGroup)
					.resolve(ZarrKeyValueReader.ZATTRS_FILE)
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
	public void cacheBehaviorTest() throws IOException, URISyntaxException {

		final String loc = tempN5Location();
		// make an uncached n5 writer
		try (final ZarrTrackingStorage n5 = new ZarrTrackingStorage(new RootedFileSystemKeyValueAccess(loc), new GsonBuilder(), true)) {

			zarrCacheBehaviorHelper(n5);
			n5.remove();
		}
	}

	public static void zarrCacheBehaviorHelper(final TrackingStorage n5) {

		// non existant group
		final String groupA = "groupA";
		final String groupB = "groupB";

		// expected backend method call counts
		final MetaStoreCounters expected = new MetaStoreCounters();
		n5.counters().reset();

		boolean exists = n5.exists(groupA);
		expected.incReadAttr(2); // attributes (.zarray and .zgroup) are read by exists() and cached
		boolean groupExists = n5.groupExists(groupA);
		boolean datasetExists = n5.datasetExists(groupA);
		assertFalse(exists); // group does not exist
		assertFalse(groupExists); // group does not exist
		assertFalse(datasetExists); // dataset does not exist
		assertEqualCounters(expected, n5.counters());

		n5.createGroup(groupA);
		expected.incWriteAttr(); // attributes (zarr.json) are written (implies directory existence)
		assertEqualCounters(expected, n5.counters());

		// group B
		exists = n5.exists(groupB);
		expected.incReadAttr(2); // attributes (.zarray and .zgroup) are read by exists() and cached
		groupExists = n5.groupExists(groupB);
		datasetExists = n5.datasetExists(groupB);
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
		expected.incReadAttr(); // reads attributes (.zgroup) to check whether there is already a group at this path
//		TODO CACHE: This could be improved by caching:
//		  the Cached.createGroup could
//		  - check whether group existence is cached
//        - if existence is cached, do nothing
//        - if existence is not cached: call non-cached createGroup
//		    !!! which should not check for existence of group, instead just write .zgroup
//		  - cache that now the group exists
		expected.incReadAttr(); // reads attributes (.zarray) to make sure there is not already a dataset at this path
		expected.incWriteAttr(); // attributes (zarr.json) are written (implies directory existence)
		n5.createGroup(cachedGroup); // be annoying
		assertEqualCounters(expected, n5.counters());

		// should not check existence when this instance created a group
		n5.exists(cachedGroup);
		n5.groupExists(cachedGroup);
		n5.datasetExists(cachedGroup);
		assertEqualCounters(expected, n5.counters());

		n5.setAttribute(cachedGroup, "one", 1);
		expected.incReadAttr(); // reads zattrs. zgroup and (non-existence of) zarray are cached
		expected.incWriteAttr(2); // writes attributes (zattrs and zgroup)
//		TODO CACHE: This should be improved:
//		  written zgroup should be the same as existing zgroup? don't override!?
		assertEqualCounters(expected, n5.counters());

		n5.setAttribute(cachedGroup, "two", 2);
		expected.incWriteAttr(2); // writes attributes (zattrs and zgroup)
//		TODO CACHE: This should be improved:
//		  written zgroup should be the same as existing zgroup? don't override!?
		assertEqualCounters(expected, n5.counters());

		n5.list("");
		expected.incList();
		assertEqualCounters(expected, n5.counters());

		n5.list(cachedGroup);
		expected.incList();
		assertEqualCounters(expected, n5.counters());

		// Check existence for non-existing group
		final String nonExistentGroup = "doesNotExist";
		n5.exists(nonExistentGroup);
		expected.incReadAttr(2); // attributes (.zarray and .zgroup) are read by exists() and cached
		assertEqualCounters(expected, n5.counters());

		n5.groupExists(nonExistentGroup);
		n5.datasetExists(nonExistentGroup);
		n5.getAttributes(nonExistentGroup);
//		TODO CACHE: This should be improved:
//		  if neither zgroup nor zarray exist, don't attempt to read zattrs
		expected.incReadAttr(); // read zattrs

		assertEqualCounters(expected, n5.counters());

		// Listing on a non-existent group is pointless, so don't call the backend storage
		assertThrows(N5Exception.class, () -> n5.list(nonExistentGroup));
//		TODO CACHE: This should be improved:
//		  if non-existence of a group is cached, we shouldn't attempt to list it
		expected.incList(); // TODO: shouldn't be necessary
		assertEqualCounters(expected, n5.counters());

		final String a = "a";
		final String ab = "a/b";
		final String abc = "a/b/c";
		// create "a/b/c"
		n5.createGroup(abc);
		expected.incReadAttr(6); // try to read zarray and zgroup for each level
		expected.incWriteAttr(3); // write zgroup for each level
		assertEqualCounters(expected, n5.counters());

		assertTrue(n5.exists(abc));
		assertTrue(n5.groupExists(abc));
		assertFalse(n5.datasetExists(abc));
		assertEqualCounters(expected, n5.counters());

		// ensure that backend need not be checked when testing existence of "a/b"
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

		// TODO CACHE: potential improvement
		//             When we remove a directory (/prefix) this implies non-existence of everything nested below
		//             (not only the things whose existence we checked before, and which therefore have a cache entry)
		//             --> This could be implemented on the DelegateStore level. Maybe additional "removed" flag?
		//             Add a test for this here:
		//             Test that exists("a/b/d") doesn't need to do any tests.

		n5.createGroup("a");
		expected.incWriteAttr(); // writes zgroup, doesn't need to read zarray or zgroup because non-existence is cached
		assertEqualCounters(expected, n5.counters());

		n5.createGroup("a/a");
		expected.incReadAttr(); // reads attributes (.zgroup) to check whether there is already a group at this path
		expected.incReadAttr(); // reads attributes (.zarray) to make sure there is not already a dataset at this path
		expected.incWriteAttr(); // attributes (zarr.json) are written (implies directory existence)
		assertEqualCounters(expected, n5.counters());

		n5.createGroup("a/b");
		expected.incWriteAttr(); // writes zgroup, doesn't need to read zarray or zgroup because non-existence is cached
		assertEqualCounters(expected, n5.counters());

		n5.createGroup("a/c");
		expected.incReadAttr(); // reads attributes (.zgroup) to check whether there is already a group at this path
		expected.incReadAttr(); // reads attributes (.zarray) to make sure there is not already a dataset at this path
		expected.incWriteAttr(); // attributes (zarr.json) are written (implies directory existence)
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

	public static class ZarrTrackingStorage extends ZarrKeyValueWriter implements TrackingStorage {

		private TrackingMetaStore trackingStore;

		public ZarrTrackingStorage(final RootedKeyValueAccess keyValueAccess,
				final GsonBuilder gsonBuilder, final boolean cacheAttributes) {

			super(keyValueAccess, gsonBuilder, true, true, ".", cacheAttributes);
		}

		@Override
		public DelegateStore createMetaStore(
				final RootedKeyValueAccess keyValueAccess,
				final boolean cacheMeta) {

			trackingStore = new TrackingMetaStore(new KeyValueAccessMetaStore(keyValueAccess));
			return cacheMeta ? new MyJsonCache(trackingStore) : trackingStore;
		}

		@Override
		public MetaStoreCounters counters() {
			return trackingStore.counters();
		}
	}
}
