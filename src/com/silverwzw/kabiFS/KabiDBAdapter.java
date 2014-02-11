package com.silverwzw.kabiFS;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.ServerAddress;
import com.silverwzw.kabiFS.MetaFS.DatastoreAdapter;
import com.silverwzw.kabiFS.structure.Commit;
import com.silverwzw.kabiFS.structure.Node;
import com.silverwzw.kabiFS.util.Helper;
import com.silverwzw.kabiFS.util.MongoConn;
import com.silverwzw.kabiFS.util.Tuple2;
import com.silverwzw.kabiFS.util.Tuple3;
import com.silverwzw.kabiFS.util.Helper.ObjectNotFoundException;

public class KabiDBAdapter implements DatastoreAdapter {
	
	private static final Logger logger;
	
	static {
		logger = Logger.getLogger(KabiDBAdapter.class);
	}
	
	private DB db;
	
	public final class KabiPersistentCommit extends Commit {
		
		protected final ObjectId id;
		protected final Map<ObjectId, ObjectId> patches;
		private final DBObject dbo;

		{
			patches = new HashMap<ObjectId, ObjectId>();
		}
		
		protected KabiPersistentCommit(DBObject dbo){
			this.dbo = dbo;
			id = (ObjectId) dbo.get("_id");
			branch = (String) dbo.get("name");
			timestamp = ((Date) dbo.get("timestamp")).getTime();
			
			DBObject baseCommitDBObj;
			ObjectId baseCommitOid;
			List<?> patchList;
			
			baseCommitDBObj = dbo; 

			while (baseCommitDBObj != null) {
				patchList = (List<?>) dbo.get("patch");
				for (Object obj : patchList) {
					ObjectId origin, replace;
					origin = (ObjectId)((DBObject) obj).get("origin");
					replace = (ObjectId)((DBObject) obj).get("replace");
					if (patches.containsKey(replace)) {
						patches.put(origin, patches.remove(replace));
					} else {
						patches.put(origin, replace);
					}
				}
				baseCommitOid = (ObjectId) dbo.get("base");
				if (baseCommitOid == null) {
					patches.put(null, (ObjectId) dbo.get("root"));
					return;
				} else {
					baseCommitDBObj = KabiDBAdapter.this.db().getCollection("commit").findOne(new BasicDBObject("_id", baseCommitOid));
				}
			}
		}
		
		public final DatastoreAdapter datastore() {
			return KabiDBAdapter.this;
		}

		public final ObjectId getActualOid(ObjectId oid) {
			ObjectId objId;
			objId = patches.get(oid);
			return (objId == null) ? oid : objId;
		}
		
		protected final DBObject dbo() {
			return dbo;
		}

		public final KabiWritableCommit createNewDiffCommit(String branch) {
			return new KabiDiffWrittingCommit(branch);
		} 
		
		public final KabiWritableCommit createNewDiffCommit() {
			return new KabiDiffWrittingCommit();
		} 
		
		public final KabiWritableCommit createNewRebaseCommit() {
			if (dbo().get("base") == null) {
				return new KabiDiffWrittingCommit(branch);
			} else {
				return createNewDiffCommit();
			}
		} 
		
		public final KabiWritableCommit createShadow() {
			return new KabiShadowCommit();
		}
		
		public abstract class KabiWritableCommit extends Commit {

			protected Collection<ObjectId> newObjIds;
			
			{
				newObjIds = new HashSet<ObjectId>();
				timestamp = new Date().getTime();
			}
			
			protected KabiWritableCommit() {
				branch = KabiPersistentCommit.this.branch;
			}
			protected KabiWritableCommit(String branchName) {
				branch = branchName;
			}
			
			public final ObjectId addDirNode2db(long owner, long gowner, int mode, Tuple2<ObjectId,String> ... subnodes) {
				
				DBObject dirDBObj;
				List<DBObject> arcs;
				ObjectId newObjId;
				
				arcs = new ArrayList<DBObject>(subnodes.length);
				
				for (Tuple2<ObjectId,String> tuple : subnodes) {
					arcs.add(new BasicDBObject("obj", tuple.item1).append("name", tuple.item2));
				}
				
				dirDBObj = new BasicDBObject("counter", 0)
					.append("gowner", gowner)
					.append("owner", owner)
					.append("mode", mode)
					.append("arc", arcs);
				
				KabiPersistentCommit.this.datastore().db()
					.getCollection(Node.nodeType2CollectionName(Node.KabiNodeType.DIRECTORY)).insert(dirDBObj);
				newObjId = (ObjectId) dirDBObj.get("_id");
				newObjIds.add(newObjId);
				return newObjId;
				
			}
			public final ObjectId addFileNode2db(long owner, long gowner, int mode, Tuple2<ObjectId, Long> ... subnodes) {
				
				DBObject fileDBObj;
				List<DBObject> arcs;
				ObjectId newObjId;
				
				arcs = new ArrayList<DBObject>(subnodes.length);
				
				for (Tuple2<ObjectId, Long> tuple : subnodes) {
					arcs.add(new BasicDBObject("obj", tuple.item1).append("offset", tuple.item2));
				}
				
				fileDBObj = new BasicDBObject("counter", 0)
					.append("gowner", gowner)
					.append("owner", owner)
					.append("mode", mode)
					.append("arc", arcs);
				
				KabiPersistentCommit.this.datastore().db()
					.getCollection(Node.nodeType2CollectionName(Node.KabiNodeType.FILE)).insert(fileDBObj);
				newObjId = (ObjectId) fileDBObj.get("_id");
				newObjIds.add(newObjId);
				return newObjId;
				
			}
			public final ObjectId addSubNode2db(byte[] bytes) {
				DBObject subDBObj;
				ObjectId newObjId;
				
				subDBObj = new BasicDBObject("counter", 0).append("data", bytes);
				KabiPersistentCommit.this.datastore().db()
					.getCollection(Node.nodeType2CollectionName(Node.KabiNodeType.SUB)).insert(subDBObj);
				newObjId = (ObjectId) subDBObj.get("_id");
				newObjIds.add(newObjId);
				return newObjId;
			}

			public final DatastoreAdapter datastore() {
				return KabiDBAdapter.this;
			}
			
			public final boolean isNew(ObjectId oid) {
				return newObjIds.contains(oid);
			}
			
			public abstract void applyPatch(ObjectId origin, ObjectId replace);
		}
		/**
		 * a writable commit based on current.
		 * @author silverwzw
		 */
		public final class KabiDiffWrittingCommit extends KabiWritableCommit {
			
			private DBObject dbo;
			
			protected final Map<ObjectId, ObjectId> diffPatches;
			
			{
				dbo = null;
				diffPatches = new HashMap<ObjectId, ObjectId>();
			}
			
			protected KabiDiffWrittingCommit() {
				super();
			}
			
			protected KabiDiffWrittingCommit(String branchName) {
				super(branchName);
			}
			
			public final ObjectId getActualOid(ObjectId oid) {
				ObjectId oidFromBase;
				oidFromBase = KabiPersistentCommit.this.getActualOid(oid);
				if (diffPatches.containsKey(oidFromBase)) {
					return diffPatches.get(oidFromBase);
				} else {
					return oidFromBase;
				}
			}

			public void applyPatch(ObjectId origin, ObjectId replace) {
				diffPatches.put(origin, replace);
				if (dbo == null) {
					List<DBObject> patches;
					patches = new ArrayList<DBObject>(1);
					patches.add(new BasicDBObject("origin", origin).append("replace", replace));
					dbo = new BasicDBObject("name", branch)
						.append("timestamp", new Date(timestamp))
						.append("root", null)
						.append("base", KabiPersistentCommit.this.id)
						.append("patch", patches);
					KabiPersistentCommit.this.datastore().db().getCollection("commit").insert(dbo);
				} else {
					DBObject query, listItem, update;
					query = new BasicDBObject("_id", (ObjectId) dbo.get("_id"));
					listItem = new BasicDBObject("patch", new BasicDBObject("origin", origin).append("replace", replace));
					update = new BasicDBObject("$push", listItem);
					KabiPersistentCommit.this.datastore().db().getCollection("commit").update(query, update);
				}
			}
			
		}
		/**
		 * make base to current writting.
		 * @author silverwzw
		 */
		public class KabiRebaseCommit extends KabiWritableCommit {
			private DBObject dbo;
			{
				dbo = null;
			}
			
			protected KabiRebaseCommit() {
				super();
			}
			
			protected KabiRebaseCommit(String branchName) {
				super(branchName);
			}
			
			public void applyPatch(ObjectId originId, ObjectId replaceId) {
				DBCollection commitCollection;
				DBObject persistentCommitDBObj;
				
				persistentCommitDBObj = KabiPersistentCommit.this.dbo();
				commitCollection = KabiPersistentCommit.this.datastore().db().getCollection("commit"); 
				
				if (dbo == null) {
					
					dbo = new BasicDBObject("name", branch)
						.append("timestamp", new Date(timestamp))
						.append("root", (ObjectId) KabiPersistentCommit.this.dbo().get("root"))
						.append("base", null)
						.append("patch", new ArrayList<DBObject>(0));
					
					commitCollection.insert(dbo);
					
					persistentCommitDBObj.put("root", null);
					persistentCommitDBObj.put("base", (ObjectId) dbo.get("_id"));
					
					commitCollection.save(persistentCommitDBObj);
					
				}
				
				
				DBObject originDBObj, replaceDBObj, originQuery, replaceQuery;
				DBCollection nodeCollections[], originCollection, replaceCollection;
				DB db;
				
				db = KabiPersistentCommit.this.datastore().db();
				nodeCollections = new DBCollection[3];
				nodeCollections[0] = db.getCollection(Node.nodeType2CollectionName(Node.KabiNodeType.DIRECTORY));
				nodeCollections[1] = db.getCollection(Node.nodeType2CollectionName(Node.KabiNodeType.FILE));
				nodeCollections[2] = db.getCollection(Node.nodeType2CollectionName(Node.KabiNodeType.SUB));
				originQuery = new BasicDBObject("_id", originId);
				replaceQuery = new BasicDBObject("_id", replaceId);
				originDBObj = null;
				replaceDBObj = null;
				originCollection = null;
				replaceCollection = null;
				
				for (DBCollection collection : nodeCollections) {
					if (originDBObj == null) {
						originDBObj = collection.findOne(originQuery);
					}
					if (replaceDBObj == null) {
						replaceDBObj = collection.findOne(replaceQuery);
					}
					if (originDBObj != null && originCollection == null) {
						originCollection = collection;
					}
					if (replaceDBObj != null && replaceCollection == null) {
						replaceCollection = collection;
					}
					if (replaceDBObj != null && originDBObj != null) {
						break;
					}
				}
				
				originDBObj.put("_id", replaceId);
				replaceDBObj.put("_id", originId);
				
				originCollection.remove(originQuery);
				originCollection.remove(replaceQuery);
				
				originCollection.save(originDBObj);
				replaceCollection.save(replaceDBObj);
				
				commitCollection.update(
						new BasicDBObject("_id", KabiPersistentCommit.this.id), // query
						new BasicDBObject("$push", // push command
								new BasicDBObject("patch",new BasicDBObject("origin", originId).append("replace", replaceId))
								)
						);
			}

			public final ObjectId getActualOid(ObjectId oid) {
				return oid != null ? oid : (ObjectId) dbo.get("root");
			}
		}
		/**
		 * a commit that store its patches info in mem, roll back db when unmount
		 * @author silverwzw
		 */
		public final class KabiShadowCommit extends KabiWritableCommit {
			
			private final Map<ObjectId, ObjectId> diffPatches;
			
			{
				diffPatches = new HashMap<ObjectId, ObjectId>();
			}

			public ObjectId getActualOid(final ObjectId oid) {
				ObjectId oidFromBase;
				oidFromBase = KabiPersistentCommit.this.getActualOid(oid);
				if (diffPatches.containsKey(oidFromBase)) {
					return diffPatches.get(oidFromBase);
				} else {
					return null;
				}
			}

			public final void applyPatch(final ObjectId origin, final ObjectId replace) {
				diffPatches.put(origin, replace);
			}
			
			public final void earse() {
				DB db;
				DBCollection tree, file, sub;
				db = KabiPersistentCommit.this.datastore().db();
				tree = db.getCollection(Node.nodeType2CollectionName(Node.KabiNodeType.DIRECTORY));
				file = db.getCollection(Node.nodeType2CollectionName(Node.KabiNodeType.FILE));
				sub = db.getCollection(Node.nodeType2CollectionName(Node.KabiNodeType.SUB));
				for (ObjectId oid : newObjIds) {
					DBObject query;
					query = new BasicDBObject("_id", oid);
					if (tree.remove(query).getN() == 0) {
						if (file.remove(query).getN() == 0) {
							sub.remove(query);
						}
					}
				}
				newObjIds = new HashSet<ObjectId>();
				diffPatches.clear();
			}
			
			public void finalize() {
				earse();
			}
		}
	}
	
	
	
	
	public KabiDBAdapter(MongoConn connCFG) {
		List<ServerAddress> servers;
		servers = new ArrayList<ServerAddress>(connCFG.servers().size());
		for (MongoConn.Server s : connCFG.servers()) {
			try {
				servers.add(new ServerAddress(s.address(), s.port()));
			} catch (UnknownHostException e) {
				logger.fatal("unknow host : " + s.address() + ":" + s.port());
			}
		}
		db = new Mongo(servers).getDB(connCFG.db());
	}

	@Override
	public final Collection<Tuple3<ObjectId, String, ObjectId>> getCommitList() {
		if (!db.collectionExists("commit")) {
			logger.error("connot find commit collection");
		}
		
		Collection<Tuple3<ObjectId, String, ObjectId>> commitList;
		
		commitList = new LinkedList<Tuple3<ObjectId, String, ObjectId>>();
		
		DBCursor commitCur = db.getCollection("commit").find();
		
		while (commitCur.hasNext()) {
			DBObject dbo;
			dbo = commitCur.next();
			String branchName;
			Date timestamp;
			Tuple3<ObjectId, String, ObjectId> tuple;
			tuple = new Tuple3<ObjectId, String, ObjectId>();
			try {
				branchName = Helper.getObject(dbo, String.class, "name");
				if (!Helper.branchNameCheck(branchName)) {
					throw new ObjectNotFoundException();
				}
			} catch (ObjectNotFoundException e) {
				logger.error("commit " + dbo.get("_id").toString() + " does not have a proper branch name");
				continue;
			}
			try {
				timestamp = Helper.getObject(dbo, Date.class, "timestamp");
			} catch (ObjectNotFoundException e) {
				logger.error("commit " + dbo.get("_id").toString() + " does not have timestamp (Date) field, use null");
				timestamp = null;
			}
			tuple.item1 = (ObjectId) dbo.get("_id");
			tuple.item2 = branchName + (timestamp == null ? "@" : "@" + timestamp.getTime());
			try {
				tuple.item3 = Helper.getObject(dbo, ObjectId.class, "base");
			} catch (ObjectNotFoundException e) {
				logger.error("commit " + dbo.get("_id").toString() + " does not have base (ObjectId) field, use null");
				tuple.item3 = null;
			}
			commitList.add(tuple);
		}
		commitCur.close();
		
		return commitList;
	}

	public Commit getCommit(String commitName) {
		String branchName;
		long timestamp;
		if (commitName == null) {
			branchName = "MAIN";
			timestamp = 0;
		} else {
			try {
				if (!Helper.commitNameCheck(commitName)) {
					throw new Exception();
				}
				String[] sa;
				sa = commitName.split("@");
				if (sa.length > 2 || sa.length < 1) {
					throw new Exception();
				}
				branchName = sa[0];
				if (!Helper.branchNameCheck(branchName)) {
					throw new Exception();
				}
				if (sa.length == 1) {
					timestamp = 0;
				} else {
					String tss;
					tss = sa[1];
					try {
						timestamp = Long.parseLong(tss);
					} catch (NumberFormatException ex) {
						logger.error("commit timestamp parse error, use latest");
						timestamp = 0;
					}
				}
			} catch (Exception ex) {
				if (ex instanceof RuntimeException) {
					throw (RuntimeException) ex;
				}
				logger.error("commit name " + commitName + " not valid, use default commit");
				branchName = "MAIN";
				timestamp = 0;
			}
		}
		
		DBCursor cur;
		DBObject query, commitDBObj;
		DBCollection commitCollection;
		
		commitCollection = db.getCollection("commit");
		commitDBObj = null;
		
		if (timestamp != 0) {
			query = new BasicDBObject("timestamp", new Date(timestamp)).append("name", branchName);
			cur = commitCollection.find(query);
			if (cur.hasNext()) {
				commitDBObj = cur.next();
			}
			cur.close();
		} else {
			query = new BasicDBObject("name", branchName);
			cur = commitCollection.find(query).sort(new BasicDBObject("timestamp", -1)).limit(1);
			if (cur.hasNext()) {
				commitDBObj = cur.next();
			}
			cur.close();
		}
		
		return new KabiPersistentCommit(commitDBObj);
	}
	



	public final void initFS() {
		DBObject dbo;
		ObjectId rootId;
		
		if (db.collectionExists("file")) {
			db.getCollection("file").drop();
		}
		if (db.collectionExists("subfile")) {
			db.getCollection("subfile").drop();
		}
		if (db.collectionExists("tree")) {
			db.getCollection("tree").drop();
		}
		if (db.collectionExists("commit")) {
			db.getCollection("commit").drop();
		}
		
		db.createCollection("file", new BasicDBObject());
		db.createCollection("subfile", new BasicDBObject());
		db.createCollection("tree", new BasicDBObject());
		db.createCollection("commit", new BasicDBObject());
		
		dbo  = new BasicDBObject("counter", 1)
			.append("gowner", 0)
			.append("owner", 0)
			.append("mode", 0777)
			.append("arc", new ArrayList<DBObject>(0));

		db.getCollection("tree").save(dbo);
		
		rootId = (ObjectId) db.getCollection("tree").findOne().get("_id");
		
		dbo = new BasicDBObject("name", "MAIN")
			.append("timestamp", new Date())
			.append("base", null)
			.append("patch", new ArrayList<DBObject>(0))
			.append("root", rootId);
		
		db.getCollection("commit").save(dbo);
		
	}
	
	@Override
	public void deleteCommit(ObjectId commit) {
		db.getCollection("commit").remove(new BasicDBObject("_id", commit));
		//TODO : release nodes
	}
	
	public final DB db() {
		return db;
	}
}
