package com.silverwzw.kabiFS;

import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import com.mongodb.ServerAddress;
import com.silverwzw.JSON.JSON;
import com.silverwzw.JSON.JSON.JsonStringFormatException;
import com.silverwzw.kabiFS.util.FSOptions;
import com.silverwzw.kabiFS.util.MongoConn;

public final class CleanUp {
	public final static void main(String[] args) {
		
		if (args.length != 1) {
			System.err.println("usage: CleanUp <config_file>");
			return;
		}
		
		MongoConn mongoConn;
		JSON config;
		@SuppressWarnings("unused")
		FSOptions fsoptions;
		DB db;
		@SuppressWarnings("unused")
		DBCollection log_coll, sub_coll, dir_coll, file_coll;
		
		try {
			config = JSON.parse(new File(args[0]));
		} catch (JsonStringFormatException e) {
			System.err.println("cannot parse config file");
			return;
		} catch (IOException e) {
			System.err.println("IO Error while reading config file");
			return;
		}
		
		if (config.get("mongo") == null) {
			System.err.println("cannot find \"mongo\" section in config file");
		}
		
		mongoConn = new MongoConn(config.get("mongo"));
		
		List<ServerAddress> servers;
		servers = new ArrayList<ServerAddress>(mongoConn.servers().size());
		for (MongoConn.Server s : mongoConn.servers()) {
			try {
				servers.add(new ServerAddress(s.address(), s.port()));
			} catch (UnknownHostException e) {
				System.err.println("unknow host : " + s.address() + ":" + s.port());
			}
		}
		db = new Mongo(servers).getDB(mongoConn.db());
		fsoptions = new FSOptions(db.getCollection(mongoConn.fsoptions()));
		
		//TODO
		//cleanup
	}

}
