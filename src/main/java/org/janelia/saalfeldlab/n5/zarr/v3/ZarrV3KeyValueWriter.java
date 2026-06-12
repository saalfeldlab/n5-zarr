package org.janelia.saalfeldlab.n5.zarr.v3;

import com.google.gson.GsonBuilder;
import org.janelia.saalfeldlab.n5.CachedGsonKeyValueN5Writer;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.KeyValueRoot;

import static org.janelia.saalfeldlab.n5.zarr.v3.ZarrV3Node.ZARR_FORMAT_KEY;

/**
 * Zarr v3 {@link N5Writer} implementation.
 */
public class ZarrV3KeyValueWriter extends ZarrV3KeyValueReader implements CachedGsonKeyValueN5Writer {

	/**
	 * Opens an {@link ZarrV3KeyValueWriter} at a given base path with a custom
	 * {@link GsonBuilder} to support custom attributes.
	 *
	 * @param keyValueRoot
	 * @param gsonBuilder
	 * 		the gson builder
	 * @param cacheMeta
	 * 		cache attributes and meta data. Setting this to true avoids frequent
	 * 		reading and parsing of JSON encoded attributes and other meta data
	 * 		that requires accessing the store. This is most interesting for high
	 * 		latency backends. Changes of cached attributes and meta data by an
	 * 		independent writer will not be tracked.
	 *
	 * @throws N5Exception
	 * 		if the base path cannot be read or does not exist, if the N5 version
	 * 		of the container is not compatible with this implementation.
	 */
	public ZarrV3KeyValueWriter(
			final KeyValueRoot keyValueRoot,
			final GsonBuilder gsonBuilder,
			final boolean cacheMeta)
			throws N5Exception {

		super(false, keyValueRoot, gsonBuilder,
				cacheMeta, false);

		Version version = null;
		if (exists("/")) {
			version = getVersion();
			if (!ZARR_3_VERSION.isCompatible(version))
				throw new N5IOException(
						"Incompatible version " + version + " (this is " + ZARR_3_VERSION + ").");
		}

		if (version == null || version.equals(NO_VERSION)) {
			createGroup("/");
			setVersion();
		}
	}

	@Override
	public void setVersion() throws N5Exception {

		if (!ZARR_3_VERSION.equals(getVersion()))
			setAttribute("/", ZARR_FORMAT_KEY, ZARR_3_VERSION.getMajor());;
	}
}
