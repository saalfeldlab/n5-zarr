package org.janelia.saalfeldlab.n5.zarr.v3;

public interface ZarrV3Node {

	public static final String NODE_TYPE_KEY = "node_type";

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

			return type.equals(GROUP.toString());
		}

		public static boolean isArray(final String type) {

			return type.equals(ARRAY.toString());
		}

		public static String key() {

			return NODE_TYPE_KEY;
		}

	};

	public NodeType getType();

}
