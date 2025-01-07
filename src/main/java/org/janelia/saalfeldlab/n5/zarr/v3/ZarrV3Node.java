package org.janelia.saalfeldlab.n5.zarr.v3;

public interface ZarrV3Node {

	public static final String NODE_TYPE_KEY = "node_type";
	public static final String ZARR_FORMAT_KEY = "zarr_format";
	public static final String ATTRIBUTES_KEY = "attributes";

	public static enum NodeType {

		GROUP, ARRAY;

		@Override
		public String toString() {

			switch (this) {
			case GROUP:
				return "group";
			case ARRAY:
				return "array";
			}
			return "";
		}

		public static boolean isGroup(final String type) {

			return type != null && type.equals(GROUP.toString());
		}

		public static boolean isArray(final String type) {

			return type != null && type.equals(ARRAY.toString());
		}

		public static String key() {

			return NODE_TYPE_KEY;
		}

	};

	public NodeType getType();

}
