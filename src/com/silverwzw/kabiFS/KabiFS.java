package com.silverwzw.kabiFS;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;

import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.silverwzw.kabiFS.KabiDBAdapter.KabiPersistentCommit.KabiShadowCommit;
import com.silverwzw.kabiFS.KabiDBAdapter.KabiPersistentCommit.KabiWritableCommit;
import com.silverwzw.kabiFS.structure.Commit.KabiDirectoryNode;
import com.silverwzw.kabiFS.structure.Commit.KabiFileNode;
import com.silverwzw.kabiFS.structure.Commit.KabiNoneDataNode;
import com.silverwzw.kabiFS.structure.Node;
import com.silverwzw.kabiFS.structure.Commit.NodeId;
import com.silverwzw.kabiFS.structure.Node.KabiNodeType;
import com.silverwzw.kabiFS.util.Constant;
import com.silverwzw.kabiFS.util.MountOptions;
import com.silverwzw.kabiFS.util.Helper;
import com.silverwzw.kabiFS.util.Path2NodeCache;
import com.silverwzw.kabiFS.util.Tuple2;
import com.silverwzw.kabiFS.util.Tuple3;

import net.fusejna.DirectoryFiller;
import net.fusejna.ErrorCodes;
import net.fusejna.StructFuseContext;
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

	private final KabiWritableCommit commit;
	private final Path2NodeCache path2nodeCache;
	
	{
		path2nodeCache = new Path2NodeCache(100);
	}
	
	public KabiFS(MountOptions options) {
		super(options);
		commit = datastore.getPersistentCommit(options.baseCommit()).createShadow();
	}

	private final Tuple3<Boolean, Node.KabiNodeType , Node> getNode(NodeId nid, int access) {
		Tuple3<Boolean, Node.KabiNodeType , Node> tp3;
		
		tp3 = new Tuple3<Boolean, Node.KabiNodeType , Node>();
		
		if (nid == null) {
			tp3.item1 = false;
			tp3.item2 = null;
			tp3.item3 = null;
			return tp3;
		}
		
		Tuple3<KabiNodeType, DBCollection, DBObject> dboinfo;
		
		dboinfo = commit.datastore().getNodeDBO(nid.oid());
		tp3.item1 = false;
		tp3.item2 = dboinfo.item1;
		switch (tp3.item2) {
			case FILE:
				tp3.item3 = commit.new KabiFileNode(dboinfo.item3);
				break;
			case DIRECTORY:
				tp3.item3 = commit.new KabiDirectoryNode(dboinfo.item3);
				break;
			case SUB:
				tp3.item3 = commit.new KabiSubNode(dboinfo.item3);
				tp3.item1 = true;
				return tp3;
		}

		KabiNoneDataNode node;
		StructFuseContext context;
		context = getFuseContext();
		
		node = (KabiNoneDataNode) tp3.item3;
		
		if (
				((access & node.mode()) == access)
				||
				(context.gid.longValue() == node.gid() && ((access & (node.mode()>>3)) == access))
				||
				(context.uid.longValue() == node.uid() && ((access & (node.mode()>>6)) == access))
			){
			tp3.item1 = true;;
		} else {
			tp3.item1 = false;
		}
		
		return tp3;
	}
	
	private NodeId findNodeByPath(String path){

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
			DBObject dbo;
			dbo = datastore.db().getCollection(Node.type2CollectionName(KabiNodeType.DIRECTORY)).findOne(new BasicDBObject("_id", nid.oid())); 
			if (dbo == null) {
				return null;
			}
			KabiDirectoryNode dnode;
			dnode = commit.new KabiDirectoryNode(dbo);
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
		Helper.setMode(stat, (KabiNoneDataNode) getNode(nid, 0).item3);
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
		Tuple3<Boolean, KabiNodeType, Node> nodeinfo;
		
		nid = findNodeByPath(path);
		nodeinfo = getNode(nid, Constant.R_OK);
		if (nid == null || nodeinfo.item2 != KabiNodeType.FILE) {
			return -ErrorCodes.ENOENT();
		}
		if (!nodeinfo.item1) {
			return -ErrorCodes.EACCES();
		}
		int byte_count = 0;
		for (Tuple2<ObjectId, Long> tuple : ((KabiFileNode) nodeinfo.item3).subNodes()) {
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

	public final int opendir(String path, FileInfoWrapper info) {
		if (super.opendir(path, info) == 0) {
			return 0;
		}

		NodeId nid;
		Tuple3<Boolean, KabiNodeType, Node> nodeinfo;
		
		nid = findNodeByPath(path);
		
		if (nid == null) {
			return -ErrorCodes.ENOENT();
		}
		
		nodeinfo = getNode(nid, Constant.R_OK);
		
		return nodeinfo.item1 ? 0 : -ErrorCodes.EACCES();
	}
	
	public final int readdir(final String path, final DirectoryFiller filler)
	{
		if (path.equals(Helper.buildPath())) {
			filler.add(metaDirName);
		}
		if (super.readdir(path, filler) >= 0) {
			return 0;
		}

		NodeId nid;
		Tuple3<Boolean, KabiNodeType, Node> nodeinfo;
		
		nid = findNodeByPath(path);
		nodeinfo = getNode(nid, Constant.R_OK);
		if (nid == null) {
			return -ErrorCodes.ENOENT();
		}
		if (nodeinfo.item2 != KabiNodeType.DIRECTORY) {
			return -ErrorCodes.ENOTDIR();
		}
		if (!nodeinfo.item1) {
			return -ErrorCodes.EACCES();
		}
		for (Tuple2<ObjectId, String> sub : ((KabiDirectoryNode) nodeinfo.item3).subNodes()) {
			filler.add(sub.item2);
		}
		return 0;
	}
	
	public final void destroy() {
		if (commit instanceof KabiShadowCommit) {
			((KabiShadowCommit) commit).earse();
		}
	}
	
	public final int chmod(String path, ModeWrapper mode) {

		NodeId nid;
		Tuple3<Boolean, KabiNodeType, Node> nodeinfo;
		
		nid = findNodeByPath(path);
		
		if (nid == null) {
			return -ErrorCodes.ENOENT();
		}
		
		nodeinfo = getNode(nid, Constant.W_OK);

		if (!(getFuseContext().uid.longValue() == 0 || nodeinfo.item1)) {
			return -ErrorCodes.EACCES();
		}
		
		ObjectId newObjId;
		
		if (nodeinfo.item2 == KabiNodeType.DIRECTORY) {
			KabiDirectoryNode dirNode;
			
			dirNode = (KabiDirectoryNode) nodeinfo.item3;
			
			if (dirNode.mode() == (int) (mode.mode() % 01000)) { // no change
				return 0;
			}
			
			newObjId = commit.addDirNode2db(
					dirNode.uid(), 
					dirNode.gid(),
					(int) mode.mode(),
					dirNode.subNodes()
					);
		} else if (nodeinfo.item2 == KabiNodeType.FILE) {
			KabiFileNode fileNode;

			fileNode = (KabiFileNode) nodeinfo.item3;
			
			if (fileNode.mode() == (int) (mode.mode() % 01000)) { // no change
				return 0;
			}
			
			newObjId = commit.addFileNode2db(
					fileNode.uid(), 
					fileNode.gid(),
					(int) mode.mode(),
					fileNode.subNodes()
					);
		} else {
			return -1;
		}
		
		commit.patch(nid.oid(), newObjId);
		path2nodeCache.dirty(path);
		return 0;
	}

	public final int chown(String path, long uid, long gid) {
		NodeId nid;
		Tuple3<Boolean, KabiNodeType, Node> nodeinfo;
		
		nid = findNodeByPath(path);
		
		if (nid == null) {
			return -ErrorCodes.ENOENT();
		}
		
		nodeinfo = getNode(nid, Constant.W_OK);

		if (!(getFuseContext().uid.longValue() == 0 || nodeinfo.item1)) {
			return -ErrorCodes.EACCES();
		}

		ObjectId newObjId;
		
		if (nodeinfo.item2 == KabiNodeType.DIRECTORY) {
			KabiDirectoryNode dirNode;
			
			dirNode = (KabiDirectoryNode) nodeinfo.item3;
			
			if (dirNode.uid() == uid && dirNode.gid() == gid) { // no change
				return 0;
			}
			
			newObjId = commit.addDirNode2db(
					uid, 
					gid,
					dirNode.mode(),
					dirNode.subNodes()
					);
		} else if (nodeinfo.item2 == KabiNodeType.FILE) {
			KabiFileNode fileNode;
			
			fileNode = (KabiFileNode) nodeinfo.item3;
			
			if (fileNode.uid() == uid && fileNode.gid() == gid) { // no change
				return 0;
			}
			
			newObjId = commit.addFileNode2db(
					uid, 
					gid,
					fileNode.mode(),
					fileNode.subNodes()
					);
		} else {
			return -1;
		}
		
		commit.patch(nid.oid(), newObjId);
		path2nodeCache.dirty(path);
		return 0;
	}
	
	public final int access(String path, int access) {
		NodeId nid;
		
		nid = findNodeByPath(path);
		
		if (nid == null) {
			if (access == Constant.F_OK) {
				return -1;
			} else {
				return -ErrorCodes.ENOENT();
			}
		}
		
		return getNode(nid, access).item1 ? 0 : -1 ;
	}
	
	public final int mkdir(String path, ModeWrapper mode) {
		int superMkdir;
		superMkdir = super.mkdir(path, mode);
		if (superMkdir != 0) {
			return superMkdir;
		}
		
		if (findNodeByPath(path) != null) {
			return -ErrorCodes.EEXIST();
		}
		
		String parentPath;
		StructFuseContext context;
		ObjectId newDir, newParent;
		NodeId parentNid;
		KabiDirectoryNode parent;
		Tuple3<Boolean, KabiNodeType, Node> tp3;
		
		parentPath = Helper.parentPath(path);
		context = getFuseContext();
		parentNid = findNodeByPath(parentPath);
		
		if (parentNid == null) {
			mkdir(parentPath, mode);
		}
		
		tp3 = getNode(parentNid, Constant.W_OK);
		
		if (!tp3.item1) {
			System.out.print("not permitted on " + Helper.parentPath(path) + "\n");
			return -ErrorCodes.EACCES();
		}

		Collection<Tuple2<ObjectId, String>> subs;
		
		parent = (KabiDirectoryNode) tp3.item3;
		subs = new LinkedList<Tuple2<ObjectId, String>>(parent.subNodes());
		
		newDir = commit.addDirNode2db(
				context.uid.longValue(), 
				context.gid.longValue(), 
				(int)(mode.mode() % 01000), 
				new ArrayList<Tuple2<ObjectId, String>>(0)
				);
		
		Tuple2<ObjectId, String> newDirTp2;
		
		newDirTp2 = new Tuple2<ObjectId, String>();
		newDirTp2.item1 = newDir;
		newDirTp2.item2 = path.substring(path.lastIndexOf(File.separator) + 1);
		
		subs.add(newDirTp2);
		
		newParent = commit.addDirNode2db(
				parent.uid(),
				parent.gid(), 
				parent.mode(),
				subs);
		
		commit.patch(parentNid.oid(), newParent);
		path2nodeCache.dirty(parentPath);
		return 0;
	}
}
