package com.silverwzw.kabiFS.structure;

import org.bson.types.ObjectId;

import com.silverwzw.kabiFS.structure.KabiCommit.NodeId;
import com.silverwzw.kabiFS.util.Util;

public abstract class Node {
	
	protected NodeId nid;
	
	
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
			return Util.Nodes.reflect(nid); 
		}
	}
	
	public final NodeId id(){
		return nid;
	}

	public static enum KabiNodeType {
		SUB,
		FILE,
		DIRECTORY
	}

	public abstract KabiNodeType type();
	
	public abstract KabiCommit commit();
}
