package com.silverwzw.kabiFS.util;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

import com.mongodb.DBObject;
import com.silverwzw.JSON.JSON;


/**
 * Helper Class
 * @author silverwzw
 *
 */
public final class Util {

	private static final Pattern commitNamePattern, branchNamePattern;
	private static final Logger logger;
	
	static {
		commitNamePattern = Pattern.compile("^[0-9a-zA-Z_]+@[0-9]*$");
		branchNamePattern = Pattern.compile("^[0-9a-zA-Z_]+$");
		logger = Logger.getLogger(Util.class);
	}
	/**
	 * Build a path, e.g. input [a,b,c] returns /a/b/c
	 * @param args directory names
	 * @return corresponding path
	 */
	public static final String buildPath(String ... args) {
		if (args.length == 0) {
			return File.separator;
		}
		String path = "";
		for (String dir : args) {
			path += File.separator + dir;
		}
		return path;
	}

	/**
	 * check if the input is a valid commit name
	 * @param inputName commit name to be tested
	 * @return true if valid, otherwise false
	 */
	public static final boolean commitNameCheck(String inputName) {
		return commitNamePattern.matcher(inputName).matches();
	}

	/**
	 * check if the input is a valid branch name
	 * @param inputName branch name to be tested
	 * @return true if valid, otherwise false
	 */
	public static final boolean branchNameCheck(String inputName) {
		return branchNamePattern.matcher(inputName).matches();
	}

	/**
	 * extract branch name from commit name, Note: assume valid commit name
	 * @param commitName the commit name
	 * @return the branch name
	 */
	public static final String getBranchNameByCommitName(String commitName) {
		return commitName.split("@")[0];
	}
	/**
	 * Compute substring asked to read
	 * @param contents the String content
	 * @param buffer read buffer
	 * @param size read size
	 * @param offset read offset
	 * @return length of chars read
	 */
	public static final int readString(String contents, final ByteBuffer buffer, final long size, final long offset) {
		final String s = contents.substring((int) offset,
				(int) Math.max(offset, Math.min(contents.length() - offset, offset + size)));
		buffer.put(s.getBytes());
		return s.getBytes().length;
	}
	/**
	 * assign 500 to directory or 400 to file
	 * @param stat the State Wrapper
	 * @param type directory or file, will get exception otherwise 
	 * @return the state wrapper
	 */
	public static final net.fusejna.StructStat.StatWrapper statMetaMode(net.fusejna.StructStat.StatWrapper stat, net.fusejna.types.TypeMode.NodeType type) {
		if (type.equals(net.fusejna.types.TypeMode.NodeType.FILE)) {
			return stat.setMode(net.fusejna.types.TypeMode.NodeType.FILE
					, true, false, false, false, false, false, false, false, false);
		} else if (type.equals(net.fusejna.types.TypeMode.NodeType.DIRECTORY)) {
			return stat.setMode(net.fusejna.types.TypeMode.NodeType.DIRECTORY
					, true, false, true, false, false, false, false, false, false);
		} else {
			logger.error("Util.statMetaMode does not support node type of " + type.toString());
			return stat.setMode(type);
		}
	}
	@SuppressWarnings("serial")
	public static final class ObjectNotFoundException extends Exception {};
	/**
	 * quick function to find dbobject in bson
	 * @param dbo the DBObject to search
	 * @param clazz the class of the final object
	 * @param field_names a set of field name in order
	 * @return corresponding Object (not necessary DBObject)
	 * @throws ObjectNotFoundException if any one of the field is not found in the path<br>or the final object cannot be cast to specific class
	 */
	@SuppressWarnings("unchecked")
	public static final <T> T getObject(final DBObject dbo, Class<T> clazz, final String ... field_names) throws ObjectNotFoundException {
		Object o = dbo;
		for (int i = 0; i < field_names.length; i++) {
			String field_name;
			field_name = field_names[i];
			if (o instanceof DBObject && ((DBObject) o).containsField(field_name)) {
				o = ((DBObject) o).get(field_name);
			} else {
				throw new ObjectNotFoundException();
			}
		}
		try {
			return (T)o;
		} catch (java.lang.ClassCastException ex){
			throw new ObjectNotFoundException();
		}
	}
	/**
	 * Object representation of all the info need to connect a db in mongodb
	 * @author silverwzw
	 */
	public static final class MongoConn {
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
		private String db;
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
				return;
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
	}
	/**
	 * store Tuple
	 */
	public static class Tuple<C1, C2, C3> {
		public C1 item1;
		public C2 item2;
		public C3 item3;
		{
			item1 = null;
			item2 = null;
			item3 = null;
		}
	}
	/**
	 * transform CommitList to meta file String
	 */
	public final static String commitList2MetaFile(Collection<Tuple<ObjectId, String, ObjectId>> list) {
		String content;
		content = "#ObjectId\tName\tbase\n";
		for (Util.Tuple<ObjectId, String, ObjectId> entry : list) {
			content += entry.item1.toString() + '\t' + entry.item2 + '\t' + (entry.item3 == null ? "null" : entry.item3.toString()) + '\n';
		}
		return content;
	}
}