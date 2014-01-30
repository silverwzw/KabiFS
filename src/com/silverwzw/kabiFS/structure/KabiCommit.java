package com.silverwzw.kabiFS.structure;

import java.util.Map;

import org.apache.log4j.Logger;
import org.bson.types.ObjectId;


public class KabiCommit {
	
	@SuppressWarnings("serial")
	public static class CommitNotFoundException extends Exception {};
	private final static Logger logger;
	
	static {
		logger = Logger.getLogger(KabiCommit.class);
	}
	
	protected String name;
	protected ObjectId id;
	protected ObjectId basedOn;
	protected Map<ObjectId, ObjectId> patches;
	
	/**
	 * NodeId is the id of the node, an inter-media between ObjectId and Node
	 * @author silverwzw
	 */
	public final class NodeId implements Comparable<NodeId> {
		private ObjectId objId;
		public NodeId(ObjectId objId) {
			this.objId = patches.get(objId);
			if (this.objId == null) {
				this.objId = objId; 
			}
		}
		public final int compareTo(NodeId nodeId) {
			return objId.compareTo(nodeId.objId);
		}
		/**
		 * get the object id wrapped in node id
		 * @return
		 */
		public final ObjectId oid() {
			return objId;
		}
		public final int hashCode() {
			return objId.hashCode();
		}
		public final boolean equals(Object o) {
			return (o instanceof NodeId) ?  objId.equals(((NodeId) o).objId) : false;
		}
	}
	
	public abstract class KabiNode extends Node {
		KabiNode(ObjectId oid) {
			this.nid = new NodeId(oid);
		}
		KabiNode(NodeId nid) {
			this.nid = nid;
		}
		public final KabiCommit commit() {
			return KabiCommit.this;
		}
	}
	
	public KabiCommit(ObjectId id) throws CommitNotFoundException {
		buildKabiCommit(id);
	}
	
	public KabiCommit(String name) throws CommitNotFoundException {
		buildKabiCommit(findIdByName(name));
	}
	/**
	 * real constructor
	 * @param id
	 * @throws CommitNotFoundException if corresponding commit not found, or if id == null 
	 */
	private final void buildKabiCommit(ObjectId id) throws CommitNotFoundException {
		if (id == null) {
			throw new CommitNotFoundException();
		}
		//TODO: get patches, base commit, name from db
	}
	/**
	 * find the Object id of commit by its name
	 * @param name name of the commit in String
	 * @return the object id correspond to the commit name, null if not found
	 */
	private final ObjectId findIdByName(String name) {
		//TODO : find ObjectID by Commit Name
		return null;
	}
}
