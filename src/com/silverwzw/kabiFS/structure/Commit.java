package com.silverwzw.kabiFS.structure;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

import com.mongodb.DBObject;
import com.silverwzw.kabiFS.MetaFS.DatastoreAdapter;
import com.silverwzw.kabiFS.util.Tuple2;


public abstract class Commit {
	
	@SuppressWarnings("serial")
	public static class CommitNotFoundException extends Exception {};
	@SuppressWarnings("unused")
	private final static Logger logger;
	
	static {
		logger = Logger.getLogger(Commit.class);
	}
	
	protected String branch;
	protected long timestamp;
	protected ObjectId id;
	protected Map<ObjectId, ObjectId> patches;
	
	public abstract KabiDirectoryNode root();
	
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
		protected KabiNode(NodeId nid) {
			this.nid = nid;
		}
		public final Commit commit() {
			return Commit.this;
		}
	}
	
	public abstract class KabiNoneDataNode extends KabiNode {
		protected long uid, gid;
		protected int mode;
		
		{
			uid = gid = -1;
			mode = -1;
		}
		
		protected KabiNoneDataNode(NodeId nid) {
			super(nid);
		}
		public final long uid() {
			if (uid < 0) {
				uid = ((Number)dbo.get("owner")).longValue();
			}
			return uid;
		}
		public final long gid() {
			if (gid < 0) {
				gid = ((Number)dbo.get("gowner")).longValue();
			}
			return gid;
		}
		public final int mode() {
			if (mode < 0) {
				mode = ((Number) dbo.get("mode")).intValue();
			}
			return mode;
		}
	}

	public final class KabiDirectoryNode extends KabiNoneDataNode {
		private Collection<Tuple2<ObjectId, String>> subNodes;
		{
			type = KabiNodeType.DIRECTORY;
			subNodes = null;
		}
		public KabiDirectoryNode(NodeId nid) {
			super(nid);
			init(nid.oid());
		}
		public Collection<Tuple2<ObjectId, String>> subNodes() {
			if (subNodes == null) {
				List<?> arc;
				arc = (List<?>) dbo.get("arc");
				subNodes = new LinkedList<Tuple2<ObjectId, String>>();
				if (arc != null) {
					for (Object o : arc) {
						if (o instanceof DBObject) {
							Tuple2<ObjectId, String> tuple;
							tuple = new Tuple2<ObjectId, String>();
							tuple.item1 = (ObjectId)((DBObject) o).get("obj");
							tuple.item2 = (String)((DBObject) o).get("name");
							subNodes.add(tuple);
						}
					}
				}
			}
			return subNodes;
		}
	}
	
	public final class KabiFileNode extends KabiNoneDataNode {
		private Collection<Tuple2<ObjectId, Long>> subNodes;
		{
			type = KabiNodeType.FILE;
			subNodes = null;
		}
		public KabiFileNode(NodeId nid) {
			super(nid);
			init(nid.oid());
		}
		public Collection<Tuple2<ObjectId, Long>> subNodes() {
			if (subNodes == null) {
				List<?> arc;
				arc = (List<?>) dbo.get("arc");
				subNodes = new LinkedList<Tuple2<ObjectId, Long>>();
				if (arc != null) {
					for (Object o : arc) {
						if (o instanceof DBObject) {
							Tuple2<ObjectId, Long> tuple;
							tuple = new Tuple2<ObjectId, Long>();
							tuple.item1 = (ObjectId)((DBObject) o).get("obj");
							tuple.item2 = ((Number)((DBObject) o).get("offset")).longValue();
							subNodes.add(tuple);
						}
					}
				}
			}
			return subNodes;
		}
	}
	
	public final class KabiSubNode extends KabiNode {
		private String data;
		{
			data = null;
			type = KabiNodeType.SUB;
		}
		protected KabiSubNode(NodeId nid) {
			super(nid);
			init(nid.oid());
		}
		public final String data(){
			if (data == null) {
				data = (String) dbo.get("data");
			}
			return data;
		}
		
	}
	
	public abstract DatastoreAdapter datastore();
}
