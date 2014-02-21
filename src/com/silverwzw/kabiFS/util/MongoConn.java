package com.silverwzw.kabiFS.util;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.Map.Entry;

import com.silverwzw.JSON.JSON;


/**
 * Object representation of all the info need to connect a db in mongodb
 * @author silverwzw
 */
public final class MongoConn {
	/**
	 * Object representation of mongo server
	 * @author silverwzw
	 */
	public static class Server {
		private String address;
		private int port;
		private Server(String address, Integer port) {
			this.address = address == null ? "localhost" : address;
			this.port = port == null ? 27017 : (int)port;
		}
		public final int hashCode() {
			return port;
		}
		public final boolean equals(Object o) {
			if (o instanceof Server) {
				Server lhs;
				lhs = (Server) o;
				return address.equals(lhs.address) && (port == lhs.port);
			} else {
				return false;
			}
			
		}
		/**
		 * the port of mongo
		 * @return port in int
		 */
		public final int port() {
			return port;
		}
		/**
		 * the address of the server
		 * @return the address in String
		 */
		public final String address() {
			return address;
		}
	}
	
	private final Set<Server> servers;
	private final String db;
	private final String fsoptions;
	{
		servers = new HashSet<Server>();
	}
	/**
	 * parse a json config to MongoConn, if null -> use default value {dbName="kabi", server=["localhost:27017"]}
	 * @param mongoConfig
	 */
	public MongoConn(JSON mongoConfig) {
		if (mongoConfig == null) {
			servers.add(new Server(null, null));
			db = "kabi";
			fsoptions = "parameters";
			return;
		}
		
		if (mongoConfig.get("fsoptions") == null) {
			fsoptions = "parameters";
		} else {
			fsoptions = (String) mongoConfig.get("fsoptions").toObject();
		}
		
		if (mongoConfig.get("db") == null) {
			db = "kabi";
		} else {
			db = (String) mongoConfig.get("db").toObject();
		}
		if (mongoConfig.get("servers") == null) {
			servers.add(new Server(null, null));
		} else {
			for (Entry<String, JSON>  en : mongoConfig.get("servers")) {
				String address;
				Integer port;
				JSON serverConfig;
				serverConfig = en.getValue();
				if (serverConfig.get("address") == null) {
					address = null;
				} else {
					address = (String) serverConfig.get("address").toObject();
				}
				if (serverConfig.get("address") == null) {
					address = null;
				} else {
					address = (String) serverConfig.get("address").toObject();
				}
				if (serverConfig.get("port") == null) {
					port = null;
				} else {
					Object jsonNum;
					jsonNum = serverConfig.get("port").toObject();
					if (jsonNum instanceof Float) {
						port = (int)(float)(Float) jsonNum;
					} else if (jsonNum instanceof Double) {
						port = (int)(double)(Double) jsonNum;
					} else {
						port = (int)(Integer) jsonNum;
					}
				}
				servers.add(new Server(address, port));
			}
		}
	}
	/**
	 * get the collection of mongo server
	 * @return collection of Server Object
	 */
	public final Collection<Server> servers(){
		return servers;
	}
	/**
	 * get the db name
	 * @return name of the db in String
	 */
	public final String db() {
		return db;
	}
	public final String fsoptions() {
		return fsoptions;
	}
}