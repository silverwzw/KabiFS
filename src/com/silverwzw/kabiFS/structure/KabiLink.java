package com.silverwzw.kabiFS.structure;

import org.bson.types.ObjectId;

public final class KabiLink {
	private int counter;
	private ObjectId kabiNodeID;
	{
		counter = 1;
	}
	public KabiLink(KabiNode kabiNode) {
		this.kabiNodeID = kabiNode.id();
	}
	public KabiLink(ObjectId kabiNodeID) {
		this.kabiNodeID = kabiNodeID;
	}
	public final KabiNode readKabiNode() {
		return KabiNode.get(kabiNodeID);
	}
	public final int counter() {
		return counter;
	}
	public final ObjectId kabiID(){
		return kabiNodeID;
	}
}
