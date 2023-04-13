package org.janelia.saalfeldlab.n5.zarr;

import org.junit.Assert;
import org.junit.Test;

import com.google.gson.GsonBuilder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;

public class ZarrCachedFSTest extends N5ZarrTest {

	@Override
	protected N5ZarrWriter createN5Writer() throws IOException {

		return createN5Writer(true);
	}

	protected N5ZarrWriter createN5Writer(final boolean cacheAttributes) throws IOException {

		return createN5Writer(tempN5PathName(), new GsonBuilder(), ".", cacheAttributes);
	}

	@Override
	protected N5ZarrWriter createN5Writer(String location, GsonBuilder gsonBuilder) throws IOException {

		return createN5Writer(location, gsonBuilder, ".");
	}

	protected N5ZarrWriter createN5Writer(String location, String dimensionSeparator) throws IOException {

		return createN5Writer(location, new GsonBuilder(), dimensionSeparator);
	}

	protected N5ZarrWriter createN5Writer(String location, GsonBuilder gsonBuilder, String dimensionSeparator, boolean cachedAttributes ) throws IOException {

		final Path testN5Path = Paths.get(location);
		final boolean existsBefore = testN5Path.toFile().exists();
		final N5ZarrWriter zarr = new N5ZarrWriter(location, gsonBuilder, dimensionSeparator, true, cachedAttributes);
		final boolean existsAfter = testN5Path.toFile().exists();
		if (!existsBefore && existsAfter) {
			tmpFiles.add(location);
		}
		return zarr;
	}

	protected static String tempN5PathName() {
		try {
			final File tmpFile = Files.createTempDirectory("zarr-cached-test-").toFile();
			tmpFile.deleteOnExit();
			final String tmpPath = tmpFile.getCanonicalPath();
			tmpFiles.add(tmpPath);
			return tmpPath;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Test
	public void cacheTest() throws IOException {
		/* Test the cache by setting many attributes, then manually deleting the underlying file.
		* The only possible way for the test to succeed is if it never again attempts to read the file, and relies on the cache. */

		final String cachedGroup = "cachedGroup";
		try (ZarrKeyValueWriter zarr = (ZarrKeyValueWriter) createN5Writer()) {
			zarr.createGroup(cachedGroup);
			final String attributesPath = zarr.zAttrsAbsolutePath(cachedGroup);

			final ArrayList<TestData<?>> tests = new ArrayList<>();
			addAndTest(zarr, tests, new TestData<>(cachedGroup, "a/b/c", 100));
			addAndTest(zarr, tests, new TestData<>(cachedGroup, "a/a[5]", "asdf"));
			addAndTest(zarr, tests, new TestData<>(cachedGroup, "a/a[2]", 0));

			Files.delete(Paths.get(attributesPath));
			runTests(zarr, tests);
		}

		try (ZarrKeyValueWriter zarr = (ZarrKeyValueWriter) createN5Writer(false)) {
			zarr.createGroup(cachedGroup);
			final String attributesPath = zarr.zAttrsAbsolutePath(cachedGroup);

			final ArrayList<TestData<?>> tests = new ArrayList<>();
			addAndTest(zarr, tests, new TestData<>(cachedGroup, "a/b/c", 100));
			addAndTest(zarr, tests, new TestData<>(cachedGroup, "a/a[5]", "asdf"));
			addAndTest(zarr, tests, new TestData<>(cachedGroup, "a/a[2]", 0));

			Files.delete(Paths.get(attributesPath));
			Assert.assertThrows(AssertionError.class, () -> runTests(zarr, tests));
		}
	}
}
