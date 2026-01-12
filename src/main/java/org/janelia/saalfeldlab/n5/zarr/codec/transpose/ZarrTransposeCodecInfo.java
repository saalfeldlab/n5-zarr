package org.janelia.saalfeldlab.n5.zarr.codec.transpose;

import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.stream.IntStream;

import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.codec.DatasetCodec;
import org.janelia.saalfeldlab.n5.codec.DatasetCodecInfo;
import org.janelia.saalfeldlab.n5.codec.transpose.TransposeCodec;
import org.janelia.saalfeldlab.n5.codec.transpose.TransposeCodecInfo;
import org.janelia.saalfeldlab.n5.serialization.NameConfig;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

/**
 * Describes a permutation of the dimensions of a block.
 * <p>
 * The {@code order} parameter parameterizes the permutation.
 * The ith element of the order array gives the destination index of the ith element of the input.
 * Example:
 * 		order  = [1, 2, 0]
 * 		input  = [7, 8, 9] 		// interpret as a block size
 * 		result = [9, 7, 8] 		// permuted block size
 *
 * <p>
 * See the specification of <a href="https://zarr-specs.readthedocs.io/en/latest/v3/codecs/transpose/index.html#transpose-codec">Zarr's Transpose codec<a>.
 */
@NameConfig.Prefix("data-codec")
@NameConfig.Name(value = ZarrTransposeCodecInfo.TYPE)
public class ZarrTransposeCodecInfo implements DatasetCodecInfo {

	public static final String TYPE = "transpose";

	@NameConfig.Parameter
	private ZarrTransposeOrder order;

	public ZarrTransposeCodecInfo() {
		// for serialization
	}

	public ZarrTransposeCodecInfo(int[] order) {

		this.order = new ZarrTransposeOrder(order);
	}

	@Override
	public String getType() {

		return TYPE;
	}

	public int[] getOrder() {

		return order.param;
	}

	@Override
	public TransposeCodec<?> create(DatasetAttributes datasetAttributes) {

		validate();
		return new TransposeCodec<>(datasetAttributes.getDataType(), getOrder());
	}

	@Override
	public boolean equals(Object obj) {

		if (obj instanceof ZarrTransposeCodecInfo)
			return Arrays.equals(order.param, ((ZarrTransposeCodecInfo)obj).getOrder());

		return false;
	}

	private void validate() {

		int[] param = this.order.param;
		final boolean[] indexFound = new boolean[param.length];
		for (int i : param)
			indexFound[i] = true;

		final int[] missingIndexes = IntStream.range(0, param.length).filter(i -> !indexFound[i]).toArray();
		if (missingIndexes.length > 0)
			throw new N5Exception("Invalid order for TransposeCodec. Missing indexes: " + Arrays.toString(missingIndexes));

	}
	
	public static ZarrTransposeCodecInfo concatenate(ZarrTransposeCodecInfo[] infos) {

		if( infos == null || infos.length == 0)
			return null;
		else if (infos.length == 1)
			return infos[0];

		// copy the initial order so we don't modify to the original
		int[] order = new int[infos[0].order.param.length];
		System.arraycopy(infos[0].order, 0, order, 0, order.length);

		for( int i = 1; i < infos.length; i++ )
			order = TransposeCodec.concatenatePermutations(order, infos[i].order.param);

		return new ZarrTransposeCodecInfo(order);
	}

	/**
	 * A permutation parameter that is processed before being serialized.
	 */
	public static class ZarrTransposeOrder {

		int[] param;

		ZarrTransposeOrder(int[] param) {

			this.param = param;
		}
	}

	/**
	 * Note:
	 * The implementation layer can not directly consume the permutation stored in 'order'
	 * due to changes of indexing (C- vs F-) order.
	 *
	 * Zarr specifies that a block with size [z, y, x] will be flattened such that the x-dimension
	 * varies fastest and the z-dimension varies slowest. Because it uses F-order, N5 reverses the
	 * order of these dimensions: the same block in N5 will have size [x, y, z].
	 *
	 * If a TranposeCodec is present, conjugating the permutation with a reversal permutation:
	 *  	reverse * order * reverse
	 * gives the proper behavior.
	 *
	 * Example
	 * (C-order)
	 * Suppose order = [1, 2, 0], and labeling the dimensions [z, y, x]
	 * The resulting transposed array will have size [x, z, y].
	 * As a result, the y (x) dimension will vary fastest (slowest).
	 *
	 * (F-order)
	 * The F-order axis relabeling of this is [x, y, z] with x (z) varying fastest (slowest)
	 * The permutation we need to apply has to result in [y, z, x] because
	 * we need the y (x) dimensions to vary fastest (slowest) as above
	 * i.e. the permutation is [2, 0, 1]
	 *
	 * input: 	[x, y, z]
	 * 		"reverse" / "c-to-f-order"
	 *  		[z, y, x]
	 * 		apply the given permutation ([1, 2, 0] in this example)
	 *  		[x, z, y]
	 * 		"un-reverse" / "f-to-c-order"
	 *  		[y, z, x]
	 *
	 * gives the result tha	t we need.
	 */
	public static class ZarrTransposeOrderAdapter implements JsonDeserializer<ZarrTransposeOrder>, JsonSerializer<ZarrTransposeOrder> {

		@Override
		public JsonElement serialize(
				final ZarrTransposeOrder object,
				final Type typeOfSrc,
				final JsonSerializationContext context) {

			return context.serialize(TransposeCodec.conjugateWithReverse(object.param));
		}

		@Override
		public ZarrTransposeOrder deserialize(
				final JsonElement json,
				final Type typeOfT,
				final JsonDeserializationContext context) throws JsonParseException {

			final int[] order = context.deserialize(json, int[].class);
			return new ZarrTransposeOrder(TransposeCodec.conjugateWithReverse(order));
		}
	}

}