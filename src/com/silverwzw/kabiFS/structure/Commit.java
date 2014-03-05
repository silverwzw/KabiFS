package com.silverwzw.kabiFS.structure;

import java.util.Collection;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

import com.mongodb.DBObject;
import com.silverwzw.kabiFS.KabiDBAdapter;
import com.silverwzw.kabiFS.util.Tuple2;
import com.silverwzw.kabiFS.util.Tuple3;

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

	protected abstract ObjectId getActualOid(ObjectId oid);
	protected abstract KabiDBAdapter datastore();
	
	/**
	 * NodeId is the id of the node, an inter-media between ObjectId and Node
	 * @author silverwzw
	 */

	public final class NodeId implements Comparable<NodeId> {
		private ObjectId objId;
		public NodeId(ObjectId oid) {
			objId = Commit.this.getActualOid(oid);
		}
		public final int compareTo(NodeId extnodeId) {
			return objId.compareTo(extnodeId.objId);
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
	
	public final KabiDirectoryNode root() {
		return new KabiDirectoryNode(new NodeId(null));
	}
	
	public final NodeId getNodeId(ObjectId oid) {
		return new NodeId(oid);
	}

	public final KabiFileNode getFileNode(NodeId nid) {
		return new KabiFileNode(nid);
	}

	public final KabiSubNode getSubNode(NodeId nid) {
		return new KabiSubNode(nid);
	}
	
	public final KabiDirectoryNode getDirNode(NodeId nid) {
		return new KabiDirectoryNode(nid);
	}
	
	public abstract class KabiNode extends Node {
		protected KabiNode(NodeId nid) {
			super(nid);
		}
		protected KabiNode(DBObject dbo) {
			super(Commit.this, dbo);
		}
		public final Commit commit() {
			return Commit.this;
		}
	}
	
	public abstract class KabiNoneDataNode extends KabiNode {
		protected long uid, gid;
		protected int mode;
		protected Date access, create;
		
		{
			uid = gid = -1;
			mode = -1;
			access = null;
			create = null;
		}

		protected KabiNoneDataNode(NodeId nid) {
			super(nid);
		}
		protected KabiNoneDataNode(DBObject dbo) {
			super(dbo);
		}
		public final long uid() {
			if (uid < 0) {
				uid = ((Number) super.dbo().get("owner")).longValue();
			}
			return uid;
		}
		public final long gid() {
			if (gid < 0) {
				gid = ((Number) super.dbo().get("gowner")).longValue();
			}
			return gid;
		}
		public final int mode() {
			if (mode < 0) {
				mode = ((Number) super.dbo().get("mode")).intValue();
			}
			return mode;
		}
		public final Date modify() {
			if (create == null) {
				create = (Date) super.dbo().get("modify");
			}
			return create;
		}
		
		public abstract Collection<?> subNodes();
	}
	public static class DirectoryItem extends Tuple2<ObjectId, String> {
		private DBObject dbo;
		DirectoryItem(DBObject dbo) {
			this.dbo = dbo;
			item1 = null;
			item2 = null;
		}
		public DirectoryItem(ObjectId newDir, String nameOf) {
			item1 = newDir;
			item2 = nameOf;
			dbo = null;
		}
		public String name() {
			if (item2 == null) {
				if (dbo == null) {
					return null;
				}
				item2 = (String) dbo.get("name");
			}
			return item2;
		}
		public ObjectId oid() {
			if (item1 == null) {
				if (dbo == null) {
					return null;
				}
				item1 = (ObjectId) dbo.get("obj");
			}
			return item1;
		}
	}
	public final class KabiDirectoryNode extends KabiNoneDataNode {
		
		private Collection<DirectoryItem> subNodes;
		{
			type = KabiNodeType.DIRECTORY;
			subNodes = null;
		}
		public KabiDirectoryNode(NodeId nid) {
			super(nid);
		}
		public KabiDirectoryNode(DBObject dbo) {
			super(dbo);
		}
		public Collection<DirectoryItem> subNodes() {
			if (subNodes == null) {
				List<?> arc;
				arc = (List<?>) super.dbo().get("arc");
				subNodes = new LinkedList<DirectoryItem>();
				if (arc != null) {
					for (Object o : arc) {
						if (o instanceof DBObject) {
							subNodes.add(new DirectoryItem((DBObject) o));
						}
					}
				}
			}
			return subNodes;
		}
	}
	
	public static class DataBlock extends Tuple3<ObjectId, Long, Long> {
		private DBObject dbo;
		public DataBlock(DBObject dbo) {
			this.dbo = dbo;
			item1 = null;
			item2 = null;
			item3 = null;
		}
		public DataBlock(ObjectId subnodeoid, long offset, long omit) {
			dbo = null;
			if (subnodeoid == null) {
				throw new NullPointerException();
			}
			item1 = subnodeoid;
			item2 = offset;
			item3 = omit;
		}
		public DataBlock(ObjectId subnodeoid, long offset) {
			dbo = null;
			if (subnodeoid == null) {
				throw new NullPointerException();
			}
			item1 = subnodeoid;
			item2 = offset;
			item3 = 0L;
		}
		public final long omit() {
			if (item3 == null) {
				Number b;
				b = (Number) dbo.get("omit");
				item3 = b == null ? 0 : b.longValue(); 
			}
			return item3;
		}
		public final long endoffset() {
			if (item2 == null) {
				item2 = ((Number) dbo.get("offset")).longValue();
			}
			return item2;
		}
		public final ObjectId oid() {
			if (item1 == null) {
				item1 = (ObjectId) dbo.get("obj");
			}
			return item1;
		}
	}
	
	public final class KabiFileNode extends KabiNoneDataNode {
		
		
		private LinkedList<DataBlock> subNodes;
		private long size;
		{
			type = KabiNodeType.FILE;
			subNodes = null;
			size = -1;
		}
		public KabiFileNode(NodeId nid) {
			super(nid);
		}
		public KabiFileNode(DBObject dbo) {
			super(dbo);
		}
		public LinkedList<DataBlock> subNodes() {
			if (subNodes == null) {
				List<?> arc;
				arc = (List<?>) super.dbo().get("arc");
				subNodes = new LinkedList<DataBlock>();
				if (arc != null) {
					for (Object o : arc) {
						if (o instanceof DBObject) {
							subNodes.add(new DataBlock((DBObject)o));
						}
					}
				}
			}
			return subNodes;
		}
		public long size() {
			if (size < 0) {
				Number sizeN;
				sizeN = (Number) dbo().get("size");
				size = (sizeN != null) ? sizeN.longValue() : subNodes().peekLast().endoffset();
			}
			return size;
		}
	}
	
	public final class KabiSubNode extends KabiNode {
		private byte[] data;
		{
			data = null;
			type = KabiNodeType.SUB;
		}
		public KabiSubNode(NodeId nid) {
			super(nid);
		}
		public KabiSubNode(DBObject dbo) {
			super(dbo);
		}
		public final byte[] data(){
			if (data == null) {
				Object o;
				o = super.dbo().get("data");
				if (o instanceof byte[]) {
					data = (byte[])o;
				} else if (o instanceof String) {
					data = ((String) o).getBytes();
				} else {
					throw new RuntimeException("unknown data type : " + o.getClass().getName());
				}
			}
			return data;
		}
		public final byte[] data(int start, int end) {
			if (end <= start) {
				return new byte[0];
			}
			
			byte[] slice, data;
			slice = new byte[end - start];
			data = data();
			
			for (int i = 0; start + i < end; i++) {
				slice[i] = data[start + i];
			}
			
			return slice;
		}
		public final byte[] data(int end) {
			return data(0, end);
		}
	}
	
}
