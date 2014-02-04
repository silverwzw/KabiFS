package com.silverwzw.kabiFS;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

import com.mongodb.BasicDBObject;
import com.silverwzw.kabiFS.structure.Commit;
import com.silverwzw.kabiFS.structure.Node;
import com.silverwzw.kabiFS.structure.Commit.NodeId;
import com.silverwzw.kabiFS.structure.Node.KabiNodeType;
import com.silverwzw.kabiFS.util.MountOptions;
import com.silverwzw.kabiFS.util.Helper;

import net.fusejna.DirectoryFiller;
import net.fusejna.ErrorCodes;
import net.fusejna.StructFuseFileInfo.FileInfoWrapper;
import net.fusejna.StructStat.StatWrapper;
import net.fusejna.types.TypeMode.NodeType;

public class KabiFS extends MetaFS {
	
	@SuppressWarnings("unused")
	private static final Logger logger;
	
	static {
		logger = Logger.getLogger(KabiFS.class);
	}

	protected final Commit writtingCommit, baseCommit;
	protected final Map<String, ObjectId> path2nodeCache;
	
	{
		path2nodeCache = new HashMap<String, ObjectId>();
	}
	
	public KabiFS(MountOptions options) {
		super(options);
		baseCommit = datastore.getCommit(options.baseCommit());
		//TODO : assign writtingCommit commit
		writtingCommit = datastore.getCommit(options.baseCommit());
	}
	
	protected boolean nodeIsDirectory(NodeId nid) {
		return datastore.db().getCollection(Node.nodeType2CollectionName(KabiNodeType.DIRECTORY)).find(new BasicDBObject("_id", nid.oid())).hasNext();
	}
	
	@Override
	public int getattr(final String path, final StatWrapper stat)
	{
		if (path.equals(Helper.buildPath())) { // Root directory
			stat.setMode(NodeType.DIRECTORY);
			return 0;
		}
		if (super.getattr(path, stat) >= 0) {
			return 0;
		}
		return -ErrorCodes.ENOENT();
	}

	@Override
	public int read(final String path, final ByteBuffer buffer, final long size, final long offset, final FileInfoWrapper info)
	{
		int superRead;
		superRead = super.read(path, buffer, size, offset, info);
		if (superRead >= 0) {
			return superRead;
		}
		return -ErrorCodes.ENOENT();
	}

	@Override
	public final int readdir(final String path, final DirectoryFiller filler)
	{
		if (path.equals(Helper.buildPath())) {
			filler.add(metaDirName);
		}
		if (super.readdir(path, filler) >= 0) {
			return 0;
		}
		return 0;
	}
}
