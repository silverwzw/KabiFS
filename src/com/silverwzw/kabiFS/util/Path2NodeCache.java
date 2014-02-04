package com.silverwzw.kabiFS.util;

import java.util.HashMap;
import java.util.LinkedList;

import com.silverwzw.kabiFS.structure.Commit.NodeId;

public final class Path2NodeCache {
	protected HashMap<String, NodeId> map;
	protected LinkedList<String> mtfList;
	protected int size;
	
	{
		map = new HashMap<String, NodeId>();
		mtfList = new LinkedList<String>();
	}
	
	public Path2NodeCache(int size) {
		this.size = size;
	}
	
	public synchronized void put(String path, NodeId nid) {
		if (mtfList.size() > size) {
			map.remove(mtfList.removeLast());
		}
		mtfList.offerFirst(path);
		map.put(path,nid);
	}
	
	public synchronized NodeId get(String path) {
		NodeId ret;
		ret = map.get(path);
		if (ret != null) {
			mtfList.remove(path);
			mtfList.offerFirst(path);
		}
		return ret;
	}
}
