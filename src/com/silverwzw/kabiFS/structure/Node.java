package com.silverwzw.kabiFS.structure;

import org.apache.log4j.Logger;

import com.silverwzw.kabiFS.structure.Commit.NodeId;

public abstract class Node {
	
	private static final Logger logger;
	
	static {
		logger = Logger.getLogger(Node.class);
	}
	
	protected NodeId nid;
	protected int counter;
	
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
	
	public abstract Commit commit();
}
