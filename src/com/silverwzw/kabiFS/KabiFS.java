package com.silverwzw.kabiFS;

import java.io.File;
import java.nio.ByteBuffer;

import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

import com.mongodb.BasicDBObject;
import com.silverwzw.kabiFS.structure.Commit;
import com.silverwzw.kabiFS.structure.Commit.KabiDirectoryNode;
import com.silverwzw.kabiFS.structure.Commit.KabiFileNode;
import com.silverwzw.kabiFS.structure.Commit.KabiNoneDataNode;
import com.silverwzw.kabiFS.structure.Commit.KabiSubNode;
import com.silverwzw.kabiFS.structure.Node;
import com.silverwzw.kabiFS.structure.Commit.NodeId;
import com.silverwzw.kabiFS.structure.Node.KabiNodeType;
import com.silverwzw.kabiFS.util.MountOptions;
import com.silverwzw.kabiFS.util.Helper;
import com.silverwzw.kabiFS.util.Path2NodeCache;
import com.silverwzw.kabiFS.util.Tuple2;

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
	protected final Path2NodeCache path2nodeCache;
	
	{
		path2nodeCache = new Path2NodeCache(100);
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
	
	protected NodeId findNodeByPath(String path){

		NodeId nid;
		
		nid = path2nodeCache.get(path);
		
		if (nid != null) {
			return nid;
		}

		nid = baseCommit.root().id();
		if (path.equals(Helper.buildPath())) {
			path2nodeCache.put(path, nid);
			return nid;
		}
		
		String[] comps;
		comps = path.split(File.separator);
		
		
		for (int i = 1; i < comps.length; i++) {
			if (!nodeIsDirectory(nid)) {
				return null;
			}
			KabiDirectoryNode dnode;
			dnode = baseCommit.new KabiDirectoryNode(nid);
			nid = null;
			for (Tuple2<ObjectId, String> sub : dnode.subNodes()) {
				if (sub.item2.equals(comps[i])) {
					nid = baseCommit.new NodeId(sub.item1);
					break;
				}
			}
			if (nid == null) {
				return null;
			}
		}
		
		path2nodeCache.put(path, nid);
		return nid;
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
		
		NodeId nid;
		nid = findNodeByPath(path);
		if (nid == null) {
			return -ErrorCodes.ENOENT();
		}
		KabiNoneDataNode ndnode; 
		if (nodeIsDirectory(nid)) {
			ndnode = baseCommit.new KabiDirectoryNode(nid);
		} else {
			ndnode = baseCommit.new KabiFileNode(nid);
		}
		Helper.setMode(stat, ndnode);
		return 0;
	}

	@Override
	public int read(final String path, final ByteBuffer buffer, final long size, final long offset, final FileInfoWrapper info)
	{
		int superRead;
		superRead = super.read(path, buffer, size, offset, info);
		if (superRead >= 0) {
			return superRead;
		}
		NodeId nid;
		nid = findNodeByPath(path);
		if (nid == null || nodeIsDirectory(nid)) {
			return -ErrorCodes.ENOENT();
		}
		String content;
		long start;
		boolean sflag, eflag;
		content = "";
		start = 0;
		sflag = false;
		for (Tuple2<ObjectId, Long> tuple : baseCommit.new KabiFileNode(nid).subNodes()) {
			if ((!sflag) && tuple.item2 > offset) {
				sflag = true;
			}
			if (!sflag) {
				start = tuple.item2;
			}
			if (sflag) {
				KabiSubNode subNode;
				subNode = baseCommit.new KabiSubNode(baseCommit.new NodeId(tuple.item1));
				content += subNode.data();
			}
			if (tuple.item2 > offset + size) {
				break;
			}
		}
		
		return Helper.readString(content, buffer, size, offset - start);
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
		NodeId nid;
		nid = findNodeByPath(path);
		if (nid == null || !nodeIsDirectory(nid)) {
			return -ErrorCodes.ENOENT();
		}
		for (Tuple2<ObjectId, String> sub : baseCommit.new KabiDirectoryNode(nid).subNodes()) {
			filler.add(sub.item2);
		}
		return 0;
	}
}
