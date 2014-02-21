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
	protected KabiNodeType type;
	
	protected Node(NodeId nid) {
		this.nid = nid;
		dbo = null;
	}
	
	protected Node(Commit commit, DBObject dbo) {
		this.dbo = dbo;
		this.nid = commit.new NodeId((ObjectId) dbo.get("_id"));
	}
	
	protected final DBObject dbo() {
		if (dbo == null) {
			dbo = commit().datastore().db()
					.getCollection(commit().datastore().fsoptions().collection_name(type))
					.findOne(new BasicDBObject("_id", nid.oid()));
		}
		return dbo;
	}
	
	public final NodeId id(){
		return nid;
	}

	public final KabiNodeType type() {
		return type;
	}

	protected abstract Commit commit();
}
