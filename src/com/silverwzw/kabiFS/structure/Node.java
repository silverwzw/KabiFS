package com.silverwzw.kabiFS.structure;

import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.silverwzw.kabiFS.structure.Commit.NodeId;

public abstract class Node {
	
	@SuppressWarnings("unused")
	private static final Logger logger;

	public static enum KabiNodeType {
		SUB,
		FILE,
		DIRECTORY
	}
	
	static {
		logger = Logger.getLogger(Node.class);
	}
	
	protected NodeId nid;
	private DBObject dbo;
	protected int counter;
	protected KabiNodeType type;
	
	{
		counter = -1;
		dbo = null;
	}
	
	protected final DBObject dbo() {
		if (dbo == null) {
			dbo = this.commit().datastore().db().getCollection(nodeType2CollectionName(type)).findOne(new BasicDBObject("_id", nid.oid()));
		}
		return dbo;
	}
	
	public final NodeId id(){
		return nid;
	}
	
	public final int counter() {
		if (counter < 0) {
			counter = (Integer) dbo().get("counter");
		}
		return counter;
	}


	public final KabiNodeType type() {
		return type;
	}

	public abstract Commit commit();

	public final void updateCounter(int newCounter) {
		commit().datastore().db().getCollection(nodeType2CollectionName(this.type()))
			.update(new BasicDBObject("_id", nid.oid()), new BasicDBObject().append("$set", new BasicDBObject("counter", newCounter)));
		counter = newCounter;
	}
	
	public final static String nodeType2CollectionName(KabiNodeType type) {
		switch (type) {
			case SUB:
				return "subfile";
			case FILE:
				return "file";
			case DIRECTORY:
				return "tree";
		}
		return null;
	}
}
