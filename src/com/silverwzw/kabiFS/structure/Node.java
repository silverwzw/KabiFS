package com.silverwzw.kabiFS.structure;

import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

import com.silverwzw.kabiFS.structure.KabiCommit.NodeId;
import com.silverwzw.kabiFS.util.Util;

public abstract class Node {
	
	private static final Logger logger;
	
	static {
		logger = Logger.getLogger(Node.class);
	}
	
	protected NodeId nid;
	protected int counter;
	
	public class KabiArc extends Arc {

		public KabiArc(Node kabiNode) {
			this.nid = kabiNode.id();
		}
		
		public KabiArc(NodeId nid) {
			this.nid = nid;
		}
		
		public final Node parentNode() {
			return Node.this;
		}
		
		public final Node childNode() {
			return null;//Util.Nodes.reflect(nid); 
		}
	}
	
	public final NodeId id(){
		return nid;
	}
	
	public final int counter() {
		if (counter <= 0) {
			logger.error("counter of node should be negative, OID = " + nid.oid().toString());
		}
		return counter;
	}

	public static enum KabiNodeType {
		SUB,
		FILE,
		DIRECTORY
	}

	public abstract KabiNodeType type();
	
	public abstract KabiCommit commit();
}
