package org.janelia.saalfeldlab.n5.zarr.v3;

public interface ZarrV3Node {

	String NODE_TYPE_KEY = "node_type";
	String ZARR_FORMAT_KEY = "zarr_format";
	String ATTRIBUTES_KEY = "attributes";

	enum NodeType {

		GROUP("group"),
		ARRAY("array");

		static NodeType of(String json) {
			if ("group".equals(json))
				return GROUP;
			else if ("array".equals(json))
				return ARRAY;
			else
				return null;
		}

		private final String label;

		NodeType(final String label) {
			this.label = label;
		}

		@Override
		public String toString() {
			return label;
		}
	};

	NodeType getType();

}
