package com.silverwzw.kabiFS.util;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.silverwzw.kabiFS.structure.Node;

public final class FSOptions {
	private long max_volumn;
	private int min_block_size_kilo, max_block_size_kilo;
	private String subnode_collection, file_collection, directory_collection;
	private final String fsoptions_collection;
	{
		min_block_size_kilo = 1024;
		max_block_size_kilo = 4096;
		max_volumn = Long.MAX_VALUE;
		subnode_collection = "subfile";
		file_collection = "file";
		directory_collection = "tree";
	}
	public FSOptions(DBCollection dbcoll) {
		DBObject dbo;
		
		fsoptions_collection = dbcoll.getName();
		
		dbo = dbcoll.findOne(new BasicDBObject("_id", "min_block_size"));
		if (dbo != null) {
			min_block_size_kilo = ((Number) dbo.get("value")).intValue();
			if (min_block_size_kilo < 512) {
				min_block_size_kilo = 512;
			} else if (min_block_size_kilo > 3145728) {
				min_block_size_kilo = 3145728;
			}
		}
		
		dbo = dbcoll.findOne(new BasicDBObject("_id", "max_block_size"));
		if (dbo != null) {
			max_block_size_kilo = ((Number) dbo.get("value")).intValue();
			if (max_block_size_kilo < 3 * min_block_size_kilo) {
				max_block_size_kilo = 3 * min_block_size_kilo;
			}
			if (max_block_size_kilo > 12582912) {
				max_block_size_kilo = 12582912;
			}
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
	}
	public final String toString(){
		return
				"min_block_size = " + min_block_size_kilo + "kb\n"
				+ "max_block_size = " + max_block_size_kilo + "kb\n"
				+ "max_volumn = " + max_volumn + "\n\n"
				+ "file collection : " + file_collection + "\n"
				+ "subnode collection : " + subnode_collection + "\n"
				+ "diretcoy collection : " + directory_collection + "\n";
	}
	public final long min_block_size(){
		return min_block_size_kilo * 1024;
	}
	public final long max_block_size(){
		return max_block_size_kilo * 1204;
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
}
