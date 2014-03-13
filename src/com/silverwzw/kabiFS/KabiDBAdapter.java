package com.silverwzw.kabiFS;

import java.net.UnknownHostException;

import java.security.NoSuchAlgorithmException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.log4j.Logger;

import org.bson.types.ObjectId;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;

import com.silverwzw.kabiFS.structure.Commit;
import com.silverwzw.kabiFS.structure.Node;
import com.silverwzw.kabiFS.structure.Node.KabiNodeType;
import com.silverwzw.kabiFS.util.FSOptions;
import com.silverwzw.kabiFS.util.Helper;
import com.silverwzw.kabiFS.util.MongoConn;
import com.silverwzw.kabiFS.util.Tuple3;
import com.silverwzw.kabiFS.util.Helper.ObjectNotFoundException;

public class KabiDBAdapter {
	
	private static final Logger logger;
	private static final java.security.MessageDigest sha256;
	
	static {
		logger = Logger.getLogger(KabiDBAdapter.class);
		try {
			sha256 = java.security.MessageDigest.getInstance("SHA-256");
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException("Algorithm SHA-256 not found.");
		}
	}
	
	private DB db;
	private final FSOptions fsoptions;
	private DBCollection collections[];
	
	{
		collections = null;
	}
	
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
				patchList = (List<?>) baseCommitDBObj.get("patch");
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
				baseCommitOid = (ObjectId) baseCommitDBObj.get("base");
				
				if (baseCommitOid == null) {
					ObjectId baseroot;
					
					baseroot = (ObjectId) baseCommitDBObj.get("root");
					
					if (!patches.containsKey(baseroot)) {
						patches.put(null, baseroot);
					} else {
						patches.put(null, patches.get(baseroot));
						patches.remove(baseroot);
					}

					return;
				} else {
					baseCommitDBObj = KabiDBAdapter.this.db().getCollection(fsoptions.commit_collection()).findOne(new BasicDBObject("_id", baseCommitOid));
				}
			}
			
		}
		
		protected final KabiDBAdapter datastore() {
			return KabiDBAdapter.this;
		}

		protected final ObjectId getActualOid(ObjectId oid) {
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
		
		public abstract class KabiWritableCommit extends Commit implements ReadWriteLock {

			protected Collection<ObjectId> newObjIds;
			private final ReadWriteLock locallock;
			
			{
				newObjIds = new HashSet<ObjectId>();
				timestamp = new Date().getTime();
				locallock = new ReentrantReadWriteLock();
			}
			
			protected KabiWritableCommit() {
				branch = KabiPersistentCommit.this.branch;
			}
			
			protected KabiWritableCommit(String branchName) {
				branch = branchName;
			}
			
			@SuppressWarnings("unchecked")
			private final BasicDBObject newNoneDataDBO(long owner, long gowner, int mode, Date modify, Collection<?> subnodes) {
				final List<DBObject> arcs;
				
				arcs = new ArrayList<DBObject>(subnodes.size());
				
				if (subnodes.size() > 0 && subnodes.iterator().next() instanceof DirectoryItem) {
					for (DirectoryItem item : (Collection<DirectoryItem>) subnodes) {
						arcs.add(new BasicDBObject("obj", item.oid()).append("name", item.name()));
					}
				} else {
					for (DataBlock block : (Collection<DataBlock>) subnodes) {
						BasicDBObject blockdbo;
						blockdbo = block.omit() == 0 ? new BasicDBObject() : new BasicDBObject("omit", block.omit());
						arcs.add(blockdbo.append("obj", block.oid()).append("offset", block.endoffset()));
					}
				}
				
				return new BasicDBObject()
					.append("gowner", gowner)
					.append("owner", owner)
					.append("mode", mode % 01000)
					.append("arc", arcs)
					.append("modify", modify);
			}
			
			public final ObjectId addDirNode2db (long owner, long gowner, int mode, Date modify, Collection<DirectoryItem> subnodes) {
				
				ObjectId newObjId;
				DBObject dirDBObj;
				
				dirDBObj = newNoneDataDBO(owner, gowner, mode, modify, subnodes);
				
				KabiPersistentCommit.this.datastore().db()
					.getCollection(fsoptions.collection_name(Node.KabiNodeType.DIRECTORY))
					.insert(dirDBObj);
				newObjId = (ObjectId) dirDBObj.get("_id");
				newObjIds.add(newObjId);
				return newObjId;
				
			}
			public final ObjectId addFileNode2db(long owner, long gowner, int mode, Date modify, List<DataBlock> subnodes, long size) {
				
				DBObject fileDBObj;
				ObjectId newObjId;
				
				fileDBObj = newNoneDataDBO(owner, gowner, mode,  modify, subnodes).append("size", size);
				
				KabiPersistentCommit.this.datastore().db()
					.getCollection(fsoptions.collection_name(Node.KabiNodeType.FILE))
					.insert(fileDBObj);
				newObjId = (ObjectId) fileDBObj.get("_id");
				newObjIds.add(newObjId);
				return newObjId;
				
			}
			
			public final ObjectId addSubNode2db(byte[] bytes) {
				DBObject subDBObj;
				ObjectId newObjId;
				byte[] digest, id;
				
				
				digest = sha256.digest(bytes);
				id = new byte[12];
				for (int i = 0; i < id.length; i++) {
					id[i] = 0;
				}
				
				for (int i = 0; i < digest.length; i++) {
					id[i % 12] ^= digest[i];
				}

				newObjId = new ObjectId(id);
				subDBObj = new BasicDBObject("_id", newObjId).append("data", bytes);
				try {
					KabiPersistentCommit.this.datastore().db()
						.getCollection(fsoptions.collection_name(Node.KabiNodeType.SUB)).insert(subDBObj);
					newObjIds.add(newObjId);
				} catch (MongoException ex) {
					;
				}
				
				return newObjId;
			}

			protected final KabiDBAdapter datastore() {
				return KabiPersistentCommit.this.datastore();
			}
			
			public final boolean isNew(ObjectId oid) {
				return newObjIds.contains(oid);
			}
			
			public final class PatchResult extends Tuple3<Boolean, ObjectId, ObjectId>{
				PatchResult(boolean b, ObjectId o, ObjectId r){
					item1 = b;
					item2 = o;
					item3 = r;
				}
				public final boolean patched() {
					return item1;
				}
				public final ObjectId oldId() {
					return item2;
				}
				public final ObjectId newId() {
					return item3;
				}
			} 
			
			public PatchResult patch(ObjectId origin, ObjectId replace) {
				if (isNew(origin)) { // direct replace.
					NodeInfo nodeinfo;
					nodeinfo = KabiDBAdapter.this.getNodeDBO(replace);
					nodeinfo.dbo().put("_id", origin);
					nodeinfo.collection().save(nodeinfo.dbo());
					nodeinfo.collection().remove(new BasicDBObject("_id", replace));
					return new PatchResult(false, null, origin);
				}
				// otherwise
				return applyPatch(origin, replace);
			}
			
			public void try2remove(ObjectId objid) {
				if (objid == null) {
					return;
				}
				if (newObjIds.remove(objid)) {
					NodeInfo nodeinfo;
					nodeinfo = getNodeDBO(objid);
					if (nodeinfo != null) {
						nodeinfo.collection().remove(new BasicDBObject("_id", objid));
					}
				}
			}
			
			protected abstract PatchResult applyPatch(ObjectId origin, ObjectId replace);
			
			public final Lock readLock() {
				return locallock.readLock();
			}
			
			public final Lock writeLock() {
				return locallock.writeLock();
			}
			
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
			
			protected final ObjectId getActualOid(ObjectId oid) {
				ObjectId oidFromBase;
				oidFromBase = KabiPersistentCommit.this.getActualOid(oid);
				if (diffPatches.containsKey(oidFromBase)) {
					return diffPatches.get(oidFromBase);
				} else {
					return oidFromBase;
				}
			}

			protected final PatchResult applyPatch(ObjectId origin, ObjectId replace) {
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
				return new PatchResult(true, origin, replace);
			}
			
		}
		/**
		 * make base to current writting commit.
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
			
			protected PatchResult applyPatch(ObjectId originId, ObjectId replaceId) {
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
				
				
				NodeInfo originTuple, replaceTuple;
				
				originTuple = KabiDBAdapter.this.getNodeDBO(originId);
				replaceTuple = KabiDBAdapter.this.getNodeDBO(replaceId);
				
				originTuple.dbo().put("_id", replaceId);
				replaceTuple.dbo().put("_id", originId);

				originTuple.collection().remove(new BasicDBObject("_id", originId));
				replaceTuple.collection().remove(new BasicDBObject("_id", replaceId));
				
				originTuple.collection().save(originTuple.dbo());
				replaceTuple.collection().save(replaceTuple.dbo());
				
				if (newObjIds.remove(replaceId)) {
					newObjIds.add(originId);
				}
				
				commitCollection.update(
						new BasicDBObject("_id", KabiPersistentCommit.this.id), // query
						new BasicDBObject("$push", // push command
								new BasicDBObject("patch",new BasicDBObject("origin", originId).append("replace", replaceId))
								)
						);
				return new PatchResult(true, null, originId);
			}

			protected final ObjectId getActualOid(ObjectId oid) {
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

			protected ObjectId getActualOid(final ObjectId oid) {
				ObjectId oidFromBase;
				oidFromBase = KabiPersistentCommit.this.getActualOid(oid);
				if (diffPatches.containsKey(oidFromBase)) {
					return diffPatches.get(oidFromBase);
				} else {
					return oidFromBase;
				}
			}

			protected final PatchResult applyPatch(final ObjectId origin, final ObjectId replace) {
				diffPatches.put(origin, replace);
				return new PatchResult(true, origin, replace);
			}
			
			public final void earse() {
				DB db;
				DBCollection tree, file, sub;
				db = KabiPersistentCommit.this.datastore().db();
				tree = db.getCollection(fsoptions.collection_name(Node.KabiNodeType.DIRECTORY));
				file = db.getCollection(fsoptions.collection_name(Node.KabiNodeType.FILE));
				sub = db.getCollection(fsoptions.collection_name(Node.KabiNodeType.SUB));
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
		fsoptions = new FSOptions(db.getCollection(connCFG.fsoptions()));
	}

	public static final class CommitListItem extends Tuple3<ObjectId, String, ObjectId> {
		CommitListItem(ObjectId commitId, String commitName, ObjectId baseCommitId) {
			item1 = commitId;
			item2 = commitName;
			item3 = baseCommitId;
		}
		public final ObjectId oid() {
			return item1; 
		}
		public final String name() {
			return item2; 
		}
		public final ObjectId base() {
			return item3; 
		}
	}
	public final Collection<CommitListItem> getCommitList() {
		if (!db.collectionExists("commit")) {
			logger.error("connot find commit collection");
		}
		
		Collection<CommitListItem> commitList;
		
		commitList = new LinkedList<CommitListItem>();
		
		DBCursor commitCur = db.getCollection("commit").find();
		
		while (commitCur.hasNext()) {
			DBObject dbo;
			dbo = commitCur.next();
			String branchName;
			Date timestamp;
			ObjectId item1, item3;
			String item2;
			try {
				branchName = Helper.getObject(dbo, String.class, "name");
				if (!Helper.branchNameCheck(branchName)) {
					throw new ObjectNotFoundException();
				}
			} catch (ObjectNotFoundException e) {
				continue;
			}
			try {
				timestamp = Helper.getObject(dbo, Date.class, "timestamp");
			} catch (ObjectNotFoundException e) {
				timestamp = null;
			}
			item1 = (ObjectId) dbo.get("_id");
			item2 = branchName + (timestamp == null ? "@" : "@" + timestamp.getTime());
			try {
				item3 = Helper.getObject(dbo, ObjectId.class, "base");
			} catch (ObjectNotFoundException e) {
				item3 = null;
			}
			commitList.add(new CommitListItem(item1, item2, item3));
		}
		commitCur.close();
		
		return commitList;
	}

	public KabiPersistentCommit getPersistentCommit(String commitName) {
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
		
		commitCollection = db.getCollection(fsoptions.commit_collection());
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
		
		dbo  = new BasicDBObject()
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
	
	public void deleteCommit(ObjectId commit) {
		db.getCollection("commit").remove(new BasicDBObject("_id", commit));
	}
	
	public final DB db() {
		return db;
	}
	
	public final FSOptions fsoptions() {
		return fsoptions;
	}
	
	private final DBCollection[] collections() {
		if (collections == null) {
			collections = new DBCollection[] { // do not modify the order, getNodeDBO() depends on that
				db().getCollection(fsoptions.collection_name(Node.KabiNodeType.DIRECTORY)),
				db().getCollection(fsoptions.collection_name(Node.KabiNodeType.FILE)),
				db().getCollection(fsoptions.collection_name(Node.KabiNodeType.SUB))
			};
		}
		return collections;
	}
	final static class NodeInfo extends Tuple3<KabiNodeType, DBCollection, DBObject> {
		private KabiNodeType type;
		private DBCollection collection;
		private DBObject o;
		NodeInfo(KabiNodeType type, DBCollection collection, DBObject o) {
			this.o = o;
			this.collection = collection;
			this.type = type;
		}
		KabiNodeType type() {
			return type;
		}
		DBCollection collection() {
			return collection;
		}
		DBObject dbo() {
			return o;
		}
	}
	final NodeInfo getNodeDBO(ObjectId oid) {
		DBObject query, queryResult;
		
		query = new BasicDBObject("_id", oid);
		
		for (int i = 0; i < collections().length; i++) {
			DBCollection collection;
			collection = collections()[i];
			queryResult = collection.findOne(query); 
			if (queryResult != null) {
				switch (i) {
					case 0:
						return new NodeInfo(Node.KabiNodeType.DIRECTORY, collection, queryResult);
					case 1:
						return new NodeInfo(Node.KabiNodeType.FILE, collection, queryResult);
					default:
						return new NodeInfo(Node.KabiNodeType.SUB, collection, queryResult);
				}
			}
		}
		return null;
	}
}
