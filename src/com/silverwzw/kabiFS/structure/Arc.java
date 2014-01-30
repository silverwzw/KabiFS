package com.silverwzw.kabiFS.structure;

import com.silverwzw.kabiFS.structure.KabiCommit.NodeId;

public abstract class Arc {
	
	private int counter;
	protected NodeId nid;
	
	{
		counter = 1;
	}
	
	public final int counter() {
		return counter;
	}
	
	public final int counterDec() {
		return --counter;
	}
	
	public final NodeId id() {
		return nid;
	}
	
	public abstract Node childNode();
	
	public abstract Node parentNode();
}
