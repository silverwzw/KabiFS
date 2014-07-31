package com.silverwzw.kabiFS.util;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.silverwzw.kabiFS.structure.Node;

public final class FSOptions {
	private long max_volumn;
	private long block_size;
	private String subnode_collection, file_collection, directory_collection, commit_collection;
	private final String fsoptions_collection;
	{
		block_size = 2048L;
		max_volumn = Long.MAX_VALUE;
		subnode_collection = "subfile";
		file_collection = "file";
		directory_collection = "tree";
		commit_collection = "commit";
	}
	public FSOptions(DBCollection dbcoll) {
		DBObject dbo;
		
		fsoptions_collection = dbcoll.getName();
		
		dbo = dbcoll.findOne(new BasicDBObject("_id", "block_size"));
		if (dbo != null) {
			block_size = ((Number) dbo.get("value")).longValue();
		}
		
		dbo = dbcoll.findOne(new BasicDBObject("_id", "max_volumn"));
		if (dbo != null) {
			max_volumn = ((Number) dbo.get("value")).longValue();
			if (max_volumn < 0) {
				max_volumn = 0;
			}
		}
		
		dbo = dbcoll.findOne(new BasicDBObject("_id", "file_collection"));
		if (dbo != null) {
			String value = (String) dbo.get("value");
			if (!value.isEmpty()) {
				file_collection = value;
			}
		}

		dbo = dbcoll.findOne(new BasicDBObject("_id", "subnode_collection"));
		if (dbo != null) {
			String value = (String) dbo.get("value");
			if (!value.isEmpty()) {
				subnode_collection = value;
			}
		}
		
		dbo = dbcoll.findOne(new BasicDBObject("_id", "directory_collection"));
		if (dbo != null) {
			String value = (String) dbo.get("value");
			if (!value.isEmpty()) {
				directory_collection = value;
			}
		}
		
		dbo = dbcoll.findOne(new BasicDBObject("_id", "commit_collection"));
		if (dbo != null) {
			String value = (String) dbo.get("value");
			if (!value.isEmpty()) {
				commit_collection = value;
			}
		}
	}
	public final String toString(){
		return
				"block_size = " + block_size + "byte\n"
				+ "max_volumn = " + max_volumn + "\n\n"
				+ "file collection : " + file_collection + "\n"
				+ "subnode collection : " + subnode_collection + "\n"
				+ "diretcoy collection : " + directory_collection + "\n";
	}
	public final long block_size(){
		return block_size;
	}
	public final long max_volumn(){
		return max_volumn;
	}
	public final String fsoptions_collection_name() {
		return fsoptions_collection;
	}
	public final String collection_name(Node.KabiNodeType type) {
		switch (type) {
		case SUB:
			return subnode_collection;
		case FILE:
			return file_collection;
		case DIRECTORY:
			return directory_collection;
		default:
			return null;
		}
	}
	public final String subnode_collection() {
		return subnode_collection;
	}
	public final String file_collection() {
		return subnode_collection;
	}
	public final String directory_collection() {
		return directory_collection;
	}
	public final String commit_collection() {
		return commit_collection;
	}
}
