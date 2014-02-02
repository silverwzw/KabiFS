package com.silverwzw.kabiFS;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

import com.mongodb.DB;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.ServerAddress;
import com.silverwzw.kabiFS.KabiFS.DatastoreAdapter;
import com.silverwzw.kabiFS.structure.KabiCommit;
import com.silverwzw.kabiFS.structure.KabiCommit.NodeId;
import com.silverwzw.kabiFS.structure.Node;
import com.silverwzw.kabiFS.util.Util;
import com.silverwzw.kabiFS.util.Util.MongoConn;
import com.silverwzw.kabiFS.util.Util.ObjectNotFoundException;

public class KabiDBAdapter implements DatastoreAdapter {
	
	private static final Logger logger;
	
	static {
		logger = Logger.getLogger(KabiDBAdapter.class);
	}
	
	private DB db;
	
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
	public final Collection<Util.Tuple<ObjectId, String, ObjectId>> getCommitList() {
		if (!db.collectionExists("commit")) {
			logger.error("connot find commit collection");
		}
		
		Collection<Util.Tuple<ObjectId, String, ObjectId>> commitList;
		
		commitList = new LinkedList<Util.Tuple<ObjectId, String, ObjectId>>();
		
		DBCursor commitCur = db.getCollection("commit").find();
		
		while (commitCur.hasNext()) {
			DBObject dbo;
			dbo = commitCur.next();
			String branchName;
			Date timestamp;
			Util.Tuple<ObjectId, String, ObjectId> tuple;
			tuple = new Util.Tuple<ObjectId, String, ObjectId>();
			try {
				branchName = Util.getObject(dbo, String.class, "name");
				if (!Util.branchNameCheck(branchName)) {
					throw new ObjectNotFoundException();
				}
			} catch (ObjectNotFoundException e) {
				logger.error("commit " + dbo.get("_id").toString() + " does not have a proper branch name");
				continue;
			}
			try {
				timestamp = Util.getObject(dbo, Date.class, "timestamp");
			} catch (ObjectNotFoundException e) {
				logger.error("commit " + dbo.get("_id").toString() + " does not have timestamp (Date) field, use null");
				timestamp = null;
			}
			tuple.item1 = (ObjectId) dbo.get("_id");
			tuple.item2 = branchName + (timestamp == null ? "@" : "@" + timestamp.getTime());
			try {
				tuple.item3 = Util.getObject(dbo, ObjectId.class, "base");
			} catch (ObjectNotFoundException e) {
				logger.error("commit " + dbo.get("_id").toString() + " does not have base (ObjectId) field, use null");
				tuple.item3 = null;
			}
			commitList.add(tuple);
		}
		
		return commitList;
	}

	@Override
	public KabiCommit getCommit() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public KabiCommit getCommit(String branch) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public KabiCommit getCommit(String branch, long timestamp) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public KabiCommit getCommit(ObjectId oid) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Node getNode(NodeId nid) {
		// TODO Auto-generated method stub
		return null;
	}
}
