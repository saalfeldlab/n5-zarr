package org.janelia.saalfeldlab.n5.zarr;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import org.apache.commons.lang3.ArrayUtils;
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.codec.BlockCodecInfo;
import org.janelia.saalfeldlab.n5.zarr.codec.PaddedRawBlockCodecInfo;

import java.nio.ByteOrder;
import java.util.HashMap;

/**
 * @author Stephan Saalfeld &lt;saalfelds@janelia.hhmi.org&gt;
 *
 */
public class ZarrDatasetAttributes extends DatasetAttributes {

	protected final ZArrayAttributes zarray;
	protected final transient byte[] fillBytes;

	public ZarrDatasetAttributes(final ZArrayAttributes zarray) {
		super(
				dimensionsFromZArray(zarray),
				blockSizeFromZArray(zarray),
				zarray.getDType().getDataType(),
				paddedRawBlockCodecInfoFromZArray(zarray),
				zarray.getCompressor().getCompression()
		);
		this.zarray = zarray;
		this.fillBytes = zarray.getDType().createFillBytes(fillValueFromJson(zarray.fillValue));
	}

	public ZarrDatasetAttributes(
			final long[] dimensions,
			final int[] blockSize,
			final DType dType,
			final Compression compression,
			final boolean isRowMajor,
			final String fill_value,
			final String dimensionSeparator ) {

		super(dimensions, blockSize, dType.getDataType(),
				toJson(fill_value),
				new PaddedRawBlockCodecInfo(dType.getOrder(), dType.createFillBytes(fill_value)),
				null, compression);

		this.zarray = createZArrayAttributes(dType, dimensionSeparator, isRowMajor ? 'C' : 'F', this);
		this.fillBytes = dType.createFillBytes(fill_value);
	}

	public ZarrDatasetAttributes(
			final long[] dimensions,
			final int[] blockSize,
			final DType dType,
			final Compression compression,
			final boolean isRowMajor,
			final String fill_value) {

		this( dimensions, blockSize, dType, compression, isRowMajor, fill_value, ".");
	}

	private static JsonElement toJson(String jsonString) {
		return new Gson().toJsonTree(jsonString);
	}

	protected BlockCodecInfo defaultBlockCodecInfo() {

		return new PaddedRawBlockCodecInfo(getDType().getOrder(), getFillBytes());
	}

	public ZArrayAttributes getZArrayAttributes() {
		return zarray;
	}

	public boolean isRowMajor() {

		return zarray.order == 'C';
	}

	public DType getDType() {

		return zarray.getDType();
	}

	public byte[] getFillBytes() {

		return fillBytes;
	}

	public String getDimensionSeparator() {
		return zarray.getDimensionSeparator();
	}

	public String relativeBlockPath(long... gridPosition) {

		final StringBuilder pathStringBuilder = new StringBuilder();
		final String dimensionSeparator = getDimensionSeparator();
		if (isRowMajor()) {
			pathStringBuilder.append(gridPosition[gridPosition.length - 1]);
			for (int i = gridPosition.length - 2; i >= 0; --i) {
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

	@Override
	public HashMap<String, Object> asMap() {
		return zarray.asMap();
	}

	private static boolean isRowMajor(final ZArrayAttributes zarray) {
		return zarray.order == 'C';
	}

	private static long[] dimensionsFromZArray(final ZArrayAttributes zarray) {

		final long[] shape = zarray.getShape().clone();
		if (isRowMajor(zarray)) {
			ArrayUtils.reverse(shape);
		}
		return shape;
	}

	private static int[] blockSizeFromZArray(final ZArrayAttributes zarray) {

		final int[] chunks = zarray.getChunks().clone();
		if (isRowMajor(zarray)) {
			ArrayUtils.reverse(chunks);
		}
		return chunks;
	}

	private static String fillValueFromJson(final JsonElement fillValue) {
		return fillValue == null || fillValue.isJsonNull() ? null : fillValue.getAsString();
	}

	private static PaddedRawBlockCodecInfo paddedRawBlockCodecInfoFromZArray(final ZArrayAttributes zarray) {

		final DType dType = zarray.getDType();
		final ByteOrder order = dType.getOrder();
		final String fillValue = fillValueFromJson(zarray.fillValue);
		final byte[] fillBytes = dType.createFillBytes(fillValue);
		return new PaddedRawBlockCodecInfo(order, fillBytes);
	}

	public static ZArrayAttributes createZArrayAttributes(final String dimensionSeparator, final DatasetAttributes datasetAttributes) {

		return createZArrayAttributes(new DType(datasetAttributes.getDataType()), dimensionSeparator, 'C', datasetAttributes);
	}

	public static ZArrayAttributes createZArrayAttributes(final DType dType, final String dimensionSeparator, char order, final DatasetAttributes datasetAttributes) {

		if (datasetAttributes instanceof ZarrDatasetAttributes) {
			final ZArrayAttributes zarray = ((ZarrDatasetAttributes)datasetAttributes).getZArrayAttributes();
			if (zarray != null)
				return zarray;
		}

		final long[] shape = datasetAttributes.getDimensions().clone();
		ArrayUtils.reverse(shape);
		final int[] chunks = datasetAttributes.getChunkSize().clone();
		ArrayUtils.reverse(chunks);

		final ZArrayAttributes zArrayAttributes = new ZArrayAttributes(
				N5ZarrReader.VERSION.getMajor(),
				shape,
				chunks,
				dType,
				ZarrCompressor.fromCompression(datasetAttributes.getCompression()),
				datasetAttributes.getDefaultValue(),
				order,
				dimensionSeparator,
				dType.getFilters());

		return zArrayAttributes;
	}
}
