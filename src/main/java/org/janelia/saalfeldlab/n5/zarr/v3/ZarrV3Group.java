package org.janelia.saalfeldlab.n5.zarr.v3;

public class ZarrV3Group implements ZarrV3Node {

	public ZarrV3Group() {}

	@Override
	public NodeType getType() {

		return NodeType.GROUP;
	}

}
