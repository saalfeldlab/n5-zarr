package org.janelia.saalfeldlab.n5.zarr.v3;

public interface ZarrV3Node {

	public static final String NODE_TYPE_KEY = "node_type";

	public static enum NodeType {

		GROUP, ARRAY;

		public String serializeString() {

			return toString().toLowerCase();
		}

		public static String key() {

			return NODE_TYPE_KEY;
		}
	};

	public NodeType getType();

}
