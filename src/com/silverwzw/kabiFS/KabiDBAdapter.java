package com.silverwzw.kabiFS;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

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

import com.silverwzw.kabiFS.util.Helper;
import com.silverwzw.kabiFS.util.MongoConn;
import com.silverwzw.kabiFS.util.Tuple3;
import com.silverwzw.kabiFS.util.Helper.ObjectNotFoundException;

public class KabiDBAdapter implements DatastoreAdapter {
	
	private static final Logger logger;
	
	static {
		logger = Logger.getLogger(KabiDBAdapter.class);
	}
	
	private DB db;
	
	public final class KabiCommit extends Commit {
		
		protected ObjectId id;
		protected Map<ObjectId, ObjectId> patches;
		
		protected KabiCommit(String branch, long timestamp, ObjectId id, ObjectId root, Map<ObjectId, ObjectId> patches){
			this.branch = branch;
			this.timestamp = timestamp;
			this.id = id;
			this.patches = patches;
		}

		public KabiDirectoryNode root() {
			return new KabiDirectoryNode(new NodeId(null));
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
			//TODO:
			return null;
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
		
		
		return new KabiCommit((String) commitDBObj.get("name"),
				((Date) commitDBObj.get("timestamp")).getTime(),
				(ObjectId) commitDBObj.get("_id"),
				(ObjectId) commitDBObj.get("root"),
				getPatches(commitDBObj));
	}
	
	private Map<ObjectId, ObjectId> getPatches(DBObject commitDBObj) {
		Map<ObjectId, ObjectId> patches, patchesBase;
		DBObject baseCommitDBObj;
		ObjectId baseCommitOid;
		Object o;
		List<?> patchList;
		
		patches = new HashMap<ObjectId, ObjectId>();
		
		o = commitDBObj.get("patch");
		if (o != null && o instanceof List<?>) {
			patchList = (List<?>) commitDBObj.get("patch");
			for (Object obj : patchList) {
				if (obj instanceof DBObject) {
					patches.put((ObjectId)((DBObject) obj).get("origin"), (ObjectId)((DBObject) obj).get("replace"));
				}
			}
		}
		
		baseCommitOid = (ObjectId) commitDBObj.get("base");
		if (baseCommitOid == null) {
			patches.put(null, (ObjectId) commitDBObj.get("root"));
			return patches;
		}
		baseCommitDBObj = db.getCollection("commit").findOne(new BasicDBObject("_id", baseCommitOid));
		patchesBase = this.getPatches(baseCommitDBObj);
		
		for (ObjectId origin : patchesBase.keySet()) {
			ObjectId replace;
			replace = patchesBase.get(origin);
			if (patches.containsKey(replace)) {
				patchesBase.put(origin, patches.get(replace));
				patches.remove(replace);
			}
		}
		
		for (Entry<ObjectId, ObjectId> en : patches.entrySet()) {
			patchesBase.put(en.getKey(), en.getValue());
		}
		
		return patchesBase;
	}
	
	public void initFix(){

		
		DBCursor cur;
		DBObject query, commitObj;
		DBCollection commitCollection;
		
		commitCollection = db.getCollection("commit");
		
		cur = commitCollection.find(new BasicDBObject("name", "SHADOW"));
		while (cur.hasNext()) {
			deleteCommit((ObjectId)cur.next().get("_id"));
		}
		cur.close();
		
		query = new BasicDBObject("timestamp", null);
		cur = commitCollection.find(query);
		if (cur.hasNext()) {
			commitObj = cur.next();
			commitObj.put("timestamp", new Date());
			commitCollection.save(commitObj);
		}
		cur.close();
		
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
