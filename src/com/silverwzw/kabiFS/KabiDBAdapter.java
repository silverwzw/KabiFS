package com.silverwzw.kabiFS;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

import com.mongodb.DB;
import com.mongodb.DBCursor;
import com.mongodb.Mongo;
import com.mongodb.ServerAddress;
import com.silverwzw.kabiFS.KabiFS.DatastoreAdapter;
import com.silverwzw.kabiFS.structure.KabiCommit;
import com.silverwzw.kabiFS.structure.KabiCommit.NodeId;
import com.silverwzw.kabiFS.structure.Node;
import com.silverwzw.kabiFS.util.Util.MongoConn;

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
	public final Collection<String> getCommitSet() {
		if (!db.collectionExists("commit")) {
			logger.error("connot find commit collection");
		}
		
		Collection<ObjectId> commitSet;
		
		commitSet = new HashSet<ObjectId>();
		
		DBCursor commitCur = db.getCollection("commit").find();
		
		while (commitCur.hasNext()) {
			commitSet.add(commitCur.next())
		}
		
		return commitSet;
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
