package com.silverwzw.kabiFS;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
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

		public final KabiDirectoryNode root() {
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
			return dbo;
		}
		
		public final KabiWrittingCommit createNewCommit() {
			//TODO :
			return null;
		} 
		
		public final KabiShadowCommit createShadow() {
			//TODO:
			return null;
		}
		
		public class KabiWrittingCommit extends Commit {

			@Override
			public KabiDirectoryNode root() {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public ObjectId getActualOid(ObjectId oid) {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public DatastoreAdapter datastore() {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			protected DBObject dbo() {
				// TODO Auto-generated method stub
				return null;
			}
			
		}
		
		public class KabiShadowCommit extends KabiWrittingCommit {

			@Override
			public KabiDirectoryNode root() {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public ObjectId getActualOid(ObjectId oid) {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public DatastoreAdapter datastore() {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			protected DBObject dbo() {
				// TODO Auto-generated method stub
				return null;
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
