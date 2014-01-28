package com.silverwzw.kabiFS.structure;

import org.bson.types.ObjectId;

public abstract class KabiNode {
	protected ObjectId id;
	
	public final ObjectId id(){
		return id;
	}

	public static enum KabiNodeType {
		SUB,
		FILE,
		DIRECTORY
	}
	
	public abstract KabiNodeType type();
	/**
	 * reflect a node by id, this id will NOT be checked against patches
	 * @param id the object id of the node
	 * @return the KabiNode instance
	 */
	public static KabiNode reflectNode(ObjectId id) {
		//TODO: read db get and build the Node object
		return null;
	}
}
