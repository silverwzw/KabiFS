package com.silverwzw.kabiFS;

import java.io.File;
import java.nio.ByteBuffer;

import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

import com.mongodb.BasicDBObject;
import com.silverwzw.kabiFS.KabiDBAdapter.KabiPersistentCommit.KabiShadowCommit;
import com.silverwzw.kabiFS.KabiDBAdapter.KabiPersistentCommit.KabiWritableCommit;
import com.silverwzw.kabiFS.structure.Commit.KabiDirectoryNode;
import com.silverwzw.kabiFS.structure.Commit.KabiFileNode;
import com.silverwzw.kabiFS.structure.Commit.KabiNoneDataNode;
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
import net.fusejna.types.TypeMode.ModeWrapper;
import net.fusejna.types.TypeMode.NodeType;

public class KabiFS extends MetaFS {
	
	@SuppressWarnings("unused")
	private static final Logger logger;
	
	static {
		logger = Logger.getLogger(KabiFS.class);
	}

	protected final KabiWritableCommit commit;
	protected final Path2NodeCache path2nodeCache;
	
	{
		path2nodeCache = new Path2NodeCache(100);
	}
	
	public KabiFS(MountOptions options) {
		super(options);
		commit = datastore.getPersistentCommit(options.baseCommit()).createShadow();
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

		nid = commit.root().id();
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
			dnode = commit.getDirNode(nid);
			nid = null;
			for (Tuple2<ObjectId, String> sub : dnode.subNodes()) {
				if (sub.item2.equals(comps[i])) {
					nid = commit.getNodeId(sub.item1);
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
			ndnode = commit.getDirNode(nid);
		} else {
			ndnode = commit.getFileNode(nid);
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
		int byte_count = 0;
		for (Tuple2<ObjectId, Long> tuple : commit.getFileNode(nid).subNodes()) {
			if (tuple.item2 <= offset) {
				continue;
			}
			byte[] data;
			data = commit.getSubNode(commit.getNodeId(tuple.item1)).data();
			if (tuple.item2 < offset + size) {
				buffer.put(data);
				byte_count += data.length;
			} else {
				for (int i = 0; i < data.length && byte_count < size; i++, byte_count++) {
					buffer.put(data[i]);
				}
				break;
			}
		}
		
		return byte_count;
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
		for (Tuple2<ObjectId, String> sub : commit.getDirNode(nid).subNodes()) {
			filler.add(sub.item2);
		}
		return 0;
	}
	
	public final void beforeUnmount(File mountPoint) {
		if (commit instanceof KabiShadowCommit) {
			((KabiShadowCommit) commit).earse();
		}
	}
	
	public final int chmod(String path, ModeWrapper mode) {
		NodeId nid;
		
		nid = findNodeByPath(path);
		
		if (nid == null) {
			return -ErrorCodes.ENOSYS();
		}

		ObjectId newObjId;
		
		if (nodeIsDirectory(nid)) {
			KabiDirectoryNode dirNode;
			
			dirNode = commit.getDirNode(nid);
			newObjId = commit.addDirNode2db(
					dirNode.uid(), 
					dirNode.gid(),
					(int) mode.mode(),
					dirNode.subNodes()
					);
		} else {
			KabiFileNode fileNode;
			
			fileNode = commit.getFileNode(nid);
			newObjId = commit.addFileNode2db(
					fileNode.uid(), 
					fileNode.gid(),
					(int) mode.mode(),
					fileNode.subNodes()
					);
		}
		
		commit.patch(nid.oid(), newObjId);
		path2nodeCache.dirty(path);
		return 0;
	}

	public final int chown(String path, long uid, long gid) {
		NodeId nid;
		
		nid = findNodeByPath(path);
		
		if (nid == null) {
			return -ErrorCodes.ENOSYS();
		}

		ObjectId newObjId;
		
		if (nodeIsDirectory(nid)) {
			KabiDirectoryNode dirNode;
			
			dirNode = commit.getDirNode(nid);
			newObjId = commit.addDirNode2db(
					uid, 
					gid,
					dirNode.mode(),
					dirNode.subNodes()
					);
		} else {
			KabiFileNode fileNode;
			
			fileNode = commit.getFileNode(nid);
			newObjId = commit.addFileNode2db(
					uid, 
					gid,
					fileNode.mode(),
					fileNode.subNodes()
					);
		}
		
		commit.patch(nid.oid(), newObjId);
		path2nodeCache.dirty(path);
		return 0;
	}
}
