/*-
 * #%L
 * Not HDF5
 * %%
 * Copyright (C) 2019 - 2022 Stephan Saalfeld
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

import com.google.gson.JsonElement;
import org.apache.commons.lang3.ArrayUtils;
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.codec.BlockCodecInfo;
import org.janelia.saalfeldlab.n5.zarr.codec.PaddedRawBlockCodecInfo;

import java.nio.ByteOrder;

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
				new PaddedRawBlockCodecInfo(dType.getOrder(), dType.createFillBytes(fill_value)),
				compression);

		this.zarray = createZArrayAttributes(dimensionSeparator, isRowMajor ? 'C' : 'F', this);
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
		return createZArrayAttributes(dimensionSeparator, 'C', datasetAttributes);

	}

	public static ZArrayAttributes createZArrayAttributes(final String dimensionSeparator, char order, final DatasetAttributes datasetAttributes) {
		if (datasetAttributes instanceof ZarrDatasetAttributes)
			return ((ZarrDatasetAttributes)datasetAttributes).getZArrayAttributes();


		final long[] shape = datasetAttributes.getDimensions().clone();
		ArrayUtils.reverse(shape);
		final int[] chunks = datasetAttributes.getBlockSize().clone();
		ArrayUtils.reverse(chunks);
		final DType dType = new DType(datasetAttributes.getDataType());

		final ZArrayAttributes zArrayAttributes = new ZArrayAttributes(
				N5ZarrReader.VERSION.getMajor(),
				shape,
				chunks,
				dType,
				ZarrCompressor.fromCompression(datasetAttributes.getCompression()),
				"0",
				order,
				dimensionSeparator,
				dType.getFilters());

		return zArrayAttributes;
	}
}
