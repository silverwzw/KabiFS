package com.silverwzw.kabiFS;

import java.io.File;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoException;
import com.silverwzw.kabiFS.KabiDBAdapter.KabiPersistentCommit.KabiShadowCommit;
import com.silverwzw.kabiFS.KabiDBAdapter.KabiPersistentCommit.KabiWritableCommit;
import com.silverwzw.kabiFS.KabiDBAdapter.KabiPersistentCommit.KabiWritableCommit.PatchResult;
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
import net.fusejna.StructTimeBuffer.TimeBufferWrapper;
import net.fusejna.types.TypeMode.ModeWrapper;
import net.fusejna.types.TypeMode.NodeType;

public class KabiFS extends MetaFS {
	
	@SuppressWarnings("unused")
	private static final Logger logger;
	
	static {
		logger = Logger.getLogger(KabiFS.class);
	}
	
	private MessageDigest msgdigest;
	private final KabiWritableCommit commit;
	private final Path2NodeCache path2nodeCache;
	
	{
		path2nodeCache = new Path2NodeCache(100);
		try {
			msgdigest = MessageDigest.getInstance("SHA-256");
		} catch (java.security.NoSuchAlgorithmException ex){
			throw new RuntimeException("SHA-256 is required but not implemented.", ex);
		}
	}
	
	public KabiFS(MountOptions options) {
		super(options);
		commit = datastore.getPersistentCommit(options.baseCommit()).createShadow();
	}

	private final static class AccessNode extends Tuple2<Boolean, Node> {
		public final boolean permission() {
			return super.item1 == null ? false : super.item1;
		}
		public final Node node() {
			return super.item2;
		}
	}
	
	private final AccessNode getNode(NodeId nid, int access) {
		AccessNode an;
		KabiNodeType type;
		
		
		an = new AccessNode();
		
		if (nid == null) {
			an.item1 = false;
			an.item2 = null;
			return an;
		}
		
		Tuple3<KabiNodeType, DBCollection, DBObject> dboinfo;
		
		dboinfo = commit.datastore().getNodeDBO(nid.oid());
		an.item1 = false;
		type = dboinfo.item1;
		switch (type) {
			case FILE:
				an.item2 = commit.new KabiFileNode(dboinfo.item3);
				break;
			case DIRECTORY:
				an.item2 = commit.new KabiDirectoryNode(dboinfo.item3);
				break;
			case SUB:
				an.item2 = commit.new KabiSubNode(dboinfo.item3);
				an.item1 = true;
				return an;
		}

		KabiNoneDataNode node;
		StructFuseContext context;
		context = getFuseContext();
		
		node = (KabiNoneDataNode) an.item2;
		
		if (
				((access & node.mode()) == access)
				||
				(context.gid.longValue() == node.gid() && ((access & (node.mode()>>3)) == access))
				||
				(context.uid.longValue() == node.uid() && ((access & (node.mode()>>6)) == access))
			){
			an.item1 = true;;
		} else {
			an.item1 = false;
		}
		
		return an;
	}
	
	private final AccessNode getNode(String path, int access) throws PathResolException {
		return getNode(findNodeByPath(path).nodeId(), access);
	}
	
	private final static class NodeAndParent extends Tuple2<NodeId, NodeId> {
		public final NodeId nodeId() {
			return super.item1;
		}
		public final NodeId parentId() {
			return super.item2;
		}
	}
	
	@SuppressWarnings("serial")
	private static class PathResolException extends Exception {}
	
	private NodeAndParent findNodeByPath(String path) throws PathResolException {

		NodeAndParent np;
		
		np = new NodeAndParent();
		
		np.item1 = path2nodeCache.get(path);
		if (!path.equals(Helper.buildPath())) {
			np.item2 = path2nodeCache.get(Helper.parentPath(path));
		} else {
			np.item2 = null;
		}
		
		if (np.item1 != null) {
			return np;
		}

		np.item1 = commit.root().id();
		np.item2 = null;
		
		if (path.equals(Helper.buildPath())) {
			path2nodeCache.put(path, np.item1);
			return np;
		}
		
		String[] comps;
		int uid, gid;
		
		comps = path.split(File.separator);
		uid = getFuseContext().uid.intValue();
		gid = getFuseContext().gid.intValue();
		
		for (int i = 1; i < comps.length; i++) {
			DBObject dbo;
			dbo = datastore.db().getCollection(fsoptions.collection_name(KabiNodeType.DIRECTORY)).findOne(new BasicDBObject("_id", np.item1.oid())); 
			if (dbo == null) {
				return null;
			}
			KabiDirectoryNode dnode;
			dnode = commit.new KabiDirectoryNode(dbo);
			
			if (
					!(
						uid == 0
						||
						(dnode.mode() & 0001) != 0
						||
						(uid == dnode.uid() && (dnode.mode() & 0100) != 0)
						||
						(gid == dnode.gid() && (dnode.mode() & 0010) != 0)
					)
				) {
				throw new PathResolException();
			}
			
			np.item2 = np.item1;
			np.item1 = null;
			for (Tuple2<ObjectId, String> sub : dnode.subNodes()) {
				if (sub.item2.equals(comps[i])) {
					np.item1 = commit.getNodeId(sub.item1);
					break;
				}
			}
			if (np.item1 == null) {
				return np;
			}
		}
		
		path2nodeCache.put(path, np.item1);
		return np;
	}
	
	public final boolean isFull() {
		return ((Number) commit.datastore().db().getStats().get("fileSize")).longValue()
				>= fsoptions.max_volumn();
	}
	
	public final int getattr(final String path, final StatWrapper stat)
	{
		commit.readLock().lock();
		try {
			if (path.equals(Helper.buildPath())) { // Root directory
				stat.setMode(NodeType.DIRECTORY);
				return 0;
			}
			if (super.getattr(path, stat) >= 0) {
				return 0;
			}
			
			NodeId nid;
			nid = findNodeByPath(path).nodeId();
			if (nid == null) {
				return -ErrorCodes.ENOENT();
			}
			Helper.setMode(stat, (KabiNoneDataNode) getNode(nid, 0).node());
			return 0;
		} catch (PathResolException ex) {
			return -ErrorCodes.EACCES();
		} catch (MongoException ex) {
			return -ErrorCodes.EIO();
		} finally {
			commit.readLock().unlock();
		}
	}

	public final int read(final String path, final ByteBuffer buffer, final long read_size, final long read_offset, final FileInfoWrapper info)
	{
		int superRead;
		superRead = super.read(path, buffer, read_size, read_offset, info);
		if (superRead >= 0) {
			return superRead;
		}
		
		commit.readLock().lock();
		
		try {
			NodeId nid;
			AccessNode nodeinfo;
			
			nid = findNodeByPath(path).nodeId();
			nodeinfo = getNode(nid, Constant.R_OK);
			if (nid == null) {
				return -ErrorCodes.ENOENT();
			}

			if (nodeinfo.node().type() == KabiNodeType.DIRECTORY) {
				return ErrorCodes.EISDIR();
			}

			if (nodeinfo.node().type() != KabiNodeType.FILE) {
				return ErrorCodes.ENOENT();
			}
			
			if (!nodeinfo.permission()) {
				return -ErrorCodes.EACCES();
			}
			
			int byte_count;
			long lastoffset;
			KabiFileNode filenode;
			
			byte_count = 0;
			lastoffset = 0;
			filenode = (KabiFileNode) nodeinfo.node();
			
			for (Tuple2<ObjectId, Long> tuple : filenode.subNodes()) {
				if (tuple.item2 <= read_offset) {
					lastoffset = tuple.item2;
					continue;
				}
				
				byte[] data2write;
				long endbyte, startbyte;
				
				endbyte = tuple.item2 < read_offset + read_size ? tuple.item2 : read_offset + read_size;
				startbyte =read_offset > lastoffset ? read_offset : lastoffset;
				data2write = commit.getSubNode(commit.getNodeId(tuple.item1)).data((int) (endbyte - startbyte));
				buffer.put(data2write);
				byte_count += data2write.length;
				lastoffset = tuple.item2;
				
				if (tuple.item2 >= read_offset + read_size) {
					break;
				}
			}
			
			while (byte_count < read_size && read_offset + byte_count < filenode.size()) {
				buffer.put((byte)'\0');
				byte_count++;
			}
			
			return byte_count;
			
		} catch (PathResolException ex) {
			return -ErrorCodes.EACCES();
		} catch (MongoException ex) {
			return -ErrorCodes.EIO();
		} finally {
			commit.readLock().unlock();
		}
	}

	public final int opendir(String path, FileInfoWrapper info) {
		if (super.opendir(path, info) == 0) {
			return 0;
		}

		commit.readLock().lock();
		
		try {
			NodeId nid;
			AccessNode nodeinfo;
			
			nid = findNodeByPath(path).nodeId();
			
			if (nid == null) {
				return -ErrorCodes.ENOENT();
			}
			
			nodeinfo = getNode(nid, Constant.R_OK);
			
			return nodeinfo.permission() ? 0 : -ErrorCodes.EACCES();
		} catch (PathResolException ex) {
			return -ErrorCodes.EACCES();
		} catch (MongoException ex) {
			return -ErrorCodes.EIO();
		} finally {
			commit.readLock().unlock();
		}
	}
	
	public final int readdir(final String path, final DirectoryFiller filler)
	{
		if (path.equals(Helper.buildPath())) {
			filler.add(metaDirName);
		}
		if (super.readdir(path, filler) >= 0) {
			return 0;
		}

		
		commit.readLock().lock();
		
		try {
			NodeId nid;
			AccessNode nodeinfo;
			nid = findNodeByPath(path).nodeId();
			nodeinfo = getNode(nid, Constant.R_OK);
			if (nid == null) {
				return -ErrorCodes.ENOENT();
			}
			if (nodeinfo.node().type() != KabiNodeType.DIRECTORY) {
				return -ErrorCodes.ENOTDIR();
			}
			if (!nodeinfo.permission()) {
				return -ErrorCodes.EACCES();
			}
			for (Tuple2<ObjectId, String> sub : ((KabiDirectoryNode) nodeinfo.node()).subNodes()) {
				filler.add(sub.item2);
			}
			return 0;
		} catch (PathResolException ex) {
			return -ErrorCodes.EACCES();
		} catch (MongoException ex) {
			return -ErrorCodes.EIO();
		} finally {
			commit.readLock().unlock();
		}
	}
	
	public final void destroy() {
		if (commit instanceof KabiShadowCommit) {
			((KabiShadowCommit) commit).earse();
		}
	}
	
	public final int chmod(String path, ModeWrapper mode) {

		commit.writeLock().lock();
		try {
			NodeId nid;
			AccessNode nodeinfo;
			
			nid = findNodeByPath(path).nodeId();
			
			if (nid == null) {
				return -ErrorCodes.ENOENT();
			}
			
			nodeinfo = getNode(nid, Constant.F_OK);
	
			if (!(getFuseContext().uid.longValue() == 0 || getFuseContext().uid.longValue() == ((KabiNoneDataNode)nodeinfo.node()).uid())) {
				return -ErrorCodes.EACCES();
			}
			
			ObjectId newObjId;
			
			if (nodeinfo.node().type() == KabiNodeType.DIRECTORY) {
				KabiDirectoryNode dirNode;
				
				dirNode = (KabiDirectoryNode) nodeinfo.node();
				
				if (dirNode.mode() == (int) (mode.mode() % 01000)) { // no change
					return 0;
				}
				
				newObjId = commit.addDirNode2db(
						dirNode.uid(), 
						dirNode.gid(),
						(int) mode.mode(),
						dirNode.modify(),
						dirNode.subNodes()
						);
			} else if (nodeinfo.node().type() == KabiNodeType.FILE) {
				KabiFileNode fileNode;
	
				fileNode = (KabiFileNode) nodeinfo.node();
				
				if (fileNode.mode() == (int) (mode.mode() % 01000)) { // no change
					return 0;
				}
				
				newObjId = commit.addFileNode2db(
						fileNode.uid(), 
						fileNode.gid(),
						(int) mode.mode(),
						fileNode.modify(),
						fileNode.subNodes(),
						fileNode.size());
			} else {
				return -1;
			}
			
			PatchResult pr;
			
			pr = commit.patch(nid.oid(), newObjId);
			path2nodeCache.dirty(path);
			commit.try2remove(pr.oldId());
			return 0;
		} catch (PathResolException ex) {
			return -ErrorCodes.EACCES();
		} catch (MongoException ex) {
			return -ErrorCodes.EIO();
		} finally {
			commit.writeLock().unlock();
		}
	}

	public final int chown(String path, long uid, long gid) {
		commit.writeLock().lock();
		
		try {
			NodeId nid;
			AccessNode nodeinfo;
			
			nid = findNodeByPath(path).nodeId();
			
			if (nid == null) {
				return -ErrorCodes.ENOENT();
			}
			
			nodeinfo = getNode(nid, Constant.W_OK);
	
			if (!(getFuseContext().uid.longValue() == 0 || nodeinfo.item1)) {
				return -ErrorCodes.EACCES();
			}
	
			ObjectId newObjId;
			
			if (nodeinfo.node().type() == KabiNodeType.DIRECTORY) {
				KabiDirectoryNode dirNode;
				
				dirNode = (KabiDirectoryNode) nodeinfo.node();
				
				if (dirNode.uid() == uid && dirNode.gid() == gid) { // no change
					return 0;
				}
				
				newObjId = commit.addDirNode2db(
						uid, 
						gid,
						dirNode.mode(),
						dirNode.modify(),
						dirNode.subNodes()
						);
			} else if (nodeinfo.node().type() == KabiNodeType.FILE) {
				KabiFileNode fileNode;
				
				fileNode = (KabiFileNode) nodeinfo.node();
				
				if (fileNode.uid() == uid && fileNode.gid() == gid) { // no change
					return 0;
				}
				
				newObjId = commit.addFileNode2db(
						uid, 
						gid,
						fileNode.mode(),
						fileNode.modify(),
						fileNode.subNodes(),
						fileNode.size()
						);
			} else {
				return -1;
			}
			
			PatchResult pr;
			pr = commit.patch(nid.oid(), newObjId);
			commit.try2remove(pr.oldId());
			path2nodeCache.dirty(path);
			return 0;
		} catch (PathResolException ex) {
			return -ErrorCodes.EACCES();
		} catch (MongoException ex) {
			return -ErrorCodes.EIO();
		} finally {
			commit.writeLock().unlock();
		}
	}
	
	public final int access(String path, int access) {
		commit.readLock().lock();
		
		try {
			int superAccess;
			superAccess = super.access(path, access);
			if (superAccess != -ErrorCodes.EEXIST()) {
				return superAccess;
			}
			
			NodeId nid;
			
			nid = findNodeByPath(path).nodeId();
			
			if (nid == null) {
				if (access == Constant.F_OK) {
					return -1;
				} else {
					return -ErrorCodes.ENOENT();
				}
			}
			
			return getNode(nid, access).permission() ? 0 : -1 ;
			
		} catch (PathResolException ex) {
			return -ErrorCodes.EACCES();
		} catch (MongoException ex) {
			return -ErrorCodes.EIO();
		} finally {
			commit.readLock().unlock();
		}
	}
	
	public final int mkdir(String path, ModeWrapper mode) {
		commit.writeLock().lock();
		
		try {
			int superMkdir;
			superMkdir = super.mkdir(path, mode);
			if (superMkdir != 0) {
				return superMkdir;
			}
			
			if (findNodeByPath(path).nodeId() != null) {
				return -ErrorCodes.EEXIST();
			}
			
			if (isFull()) {
				return -ErrorCodes.ENOSPC();
			}
			
			String parentPath;
			StructFuseContext context;
			ObjectId newDir, newParent;
			NodeId parentNid;
			KabiDirectoryNode parent;
			AccessNode an;
			
			parentPath = Helper.parentPath(path);
			context = getFuseContext();
			parentNid = findNodeByPath(parentPath).nodeId();
			
			if (parentNid == null) {
				mkdir(parentPath, mode);
			}
			
			an = getNode(parentNid, Constant.W_OK);
			
			if (!an.permission()) {
				return -ErrorCodes.EACCES();
			}
	
			Collection<Tuple2<ObjectId, String>> subs;
			
			parent = (KabiDirectoryNode) an.node();
			subs = new LinkedList<Tuple2<ObjectId, String>>(parent.subNodes());
			
			newDir = commit.addDirNode2db(
					context.uid.longValue(), 
					context.gid.longValue(), 
					(int)(mode.mode() % 01000), 
					new Date(),
					new ArrayList<Tuple2<ObjectId, String>>(0)
					);
			
			subs.add(new Tuple2<ObjectId, String>(newDir, Helper.nameOf(path)));
			
			newParent = commit.addDirNode2db(
					parent.uid(),
					parent.gid(), 
					parent.mode(),
					new Date(),
					subs);
			
			PatchResult pr;
			pr = commit.patch(parentNid.oid(), newParent);
			commit.try2remove(pr.oldId());
			path2nodeCache.dirty(parentPath);
			return 0;
			
		} catch (PathResolException ex) {
			return -ErrorCodes.EACCES();
		} catch (MongoException ex) {
			return -ErrorCodes.EIO();
		} finally {
			commit.writeLock().lock();
		}
	}
	
	public final int unlink(String path) {
		int superUnlink;
		superUnlink = super.unlink(path);
		if (superUnlink != -ErrorCodes.ENOENT()) {
			return superUnlink;
		}
		commit.writeLock().lock();
		try {
			NodeId fileNid, parentNid;
			AccessNode fnodeinfo, pnodeinfo;
			NodeAndParent n2;
			String ppath;
			KabiDirectoryNode dnode;
			Collection<Tuple2<ObjectId, String>> subs;
			ObjectId newpoid;
			
			n2 = findNodeByPath(path);
			fileNid = n2.nodeId();
			if (fileNid == null) {
				return 0;
			}
			
			ppath = Helper.parentPath(path);
			parentNid = n2.parentId() == null ? findNodeByPath(ppath).nodeId() : n2.parentId();
			
			fnodeinfo = getNode(fileNid, Constant.F_OK);
			pnodeinfo = getNode(parentNid, Constant.W_OK);
			if (fnodeinfo.node().type() == KabiNodeType.DIRECTORY) {
				return -ErrorCodes.EISDIR();
			}
			if (!pnodeinfo.permission()) {
				return -ErrorCodes.EACCES();
			}
			
			dnode = (KabiDirectoryNode) pnodeinfo.node();
			
			subs = new LinkedList<Tuple2<ObjectId, String>>();
			
			for (Tuple2<ObjectId, String> en : dnode.subNodes()) {
				if (!en.item2.equals(Helper.nameOf(path))) {
					subs.add(en);
				}
			}
			
			PatchResult pr;
			
			newpoid = commit.addDirNode2db(dnode.uid(), dnode.gid(), dnode.mode(), new Date(), subs);
			pr = commit.patch(parentNid.oid(), newpoid);
			
			path2nodeCache.dirty(ppath);
			commit.try2remove(fileNid.oid());
			commit.try2remove(pr.oldId());
			
			return 0;
		} catch (PathResolException ex) {
			return -ErrorCodes.EACCES();
		} catch (MongoException ex) {
			return -ErrorCodes.EIO();
		} finally {
			commit.writeLock().unlock();
		}
	}
	
	public final int rmdir(String path) {
		int superRmdir;
		superRmdir = super.rmdir(path);
		if (superRmdir != -ErrorCodes.EEXIST()) {
			return superRmdir;
		}
		
		if (path.equals(Helper.buildPath())) {
			return -ErrorCodes.EFAULT();
		}
		
		commit.writeLock().lock();
		
		try {
			
			NodeAndParent nids;
			AccessNode dir, pdir;
			KabiDirectoryNode dnode;
			List<Tuple2<ObjectId, String>> subs;
			ObjectId newoid;
			
			nids = findNodeByPath(path);
			
			dir = getNode(nids.nodeId(), Constant.F_OK);
			
			if (!dir.permission()) {
				return -ErrorCodes.EEXIST();
			}
			if (dir.node().type() != KabiNodeType.DIRECTORY) {
				return -ErrorCodes.ENOTDIR();
			}
			if (!((KabiDirectoryNode)dir.node()).subNodes().isEmpty()) {
				return -ErrorCodes.ENOTEMPTY();
			}
			
			pdir = getNode(nids.parentId() == null ? findNodeByPath(Helper.parentPath(path)).nodeId() : nids.parentId(), Constant.W_OK);
			
			if (!pdir.permission()) {
				return -ErrorCodes.EACCES();
			}
			
			dnode = (KabiDirectoryNode) pdir.node();
			subs = new LinkedList<Tuple2<ObjectId, String>>();
			for (Tuple2<ObjectId, String> tp : dnode.subNodes()) {
				if (!tp.item2.equals(Helper.nameOf(path))) {
					subs.add(tp);
				}
			}
			
			PatchResult pr;
			
			newoid = commit.addDirNode2db(dnode.uid(), dnode.gid(), dnode.mode(), new Date(), subs);
			pr = commit.patch(pdir.node().id().oid(), newoid);
			path2nodeCache.dirty(path);
			path2nodeCache.dirty(Helper.parentPath(path));
			commit.try2remove(nids.nodeId().oid());
			commit.try2remove(pr.oldId());
			return 0;
		} catch (PathResolException ex) {
			return -ErrorCodes.EACCES();
		} catch (MongoException ex) {
			return -ErrorCodes.EIO();
		} finally {
			commit.writeLock().unlock();
		}
	}
	
	public final int rename(String path, String newName) {

		int superRename;
		superRename = super.rename(path, newName);
		if (superRename != -ErrorCodes.ENOENT()) {
			return superRename;
		}
		
		if (path.equals(newName)) {
			return 0;
		}
		
		if (path.equals(Helper.buildPath()) || newName.equals(Helper.buildPath())) {
			return -ErrorCodes.EBUSY();
		}
		
		if (newName.startsWith(path) && newName.charAt(path.length()) == File.separatorChar) {
			return -ErrorCodes.EINVAL();
		}

		if (isFull()) {
			return -ErrorCodes.ENOSPC();
		}
		
		commit.writeLock().lock();
		
		try {
			NodeAndParent nids;
			AccessNode ps, s, pt, t;
			
			nids = findNodeByPath(path);
			
			s = getNode(nids.nodeId(), Constant.F_OK);
			ps = getNode(nids.parentId() == null ? findNodeByPath(Helper.parentPath(path)).nodeId(): nids.parentId(), Constant.W_OK + Constant.X_OK);
			
			nids = findNodeByPath(newName);
			
			t = getNode(nids.nodeId(), Constant.F_OK);
			
			pt = getNode(nids.parentId() == null ? findNodeByPath(Helper.parentPath(newName)).nodeId() : nids.parentId(), Constant.W_OK + Constant.X_OK);
			
			
			if (!s.permission()) {
				return -ErrorCodes.ENOENT();
			}
			
			if ((!ps.permission()) || !pt.permission()) {
				return -ErrorCodes.EACCES();
			}
			
			if (t.permission()) {
				if (t.node().type() == KabiNodeType.DIRECTORY && s.node().type() != KabiNodeType.DIRECTORY) {
					return -ErrorCodes.EISDIR(); 
				}
				if (t.node().type() != KabiNodeType.DIRECTORY && s.node().type() == KabiNodeType.DIRECTORY) {
					return -ErrorCodes.ENOTDIR(); 
				}
				if (t.node().type() == KabiNodeType.DIRECTORY && !((KabiDirectoryNode)t.node()).subNodes().isEmpty()) {
					return -ErrorCodes.ENOTEMPTY();
				}
			}
			
			KabiDirectoryNode ptn, psn;
			
			ptn = (KabiDirectoryNode) pt.node();
			psn = (KabiDirectoryNode) ps.node();
			
			if (t.permission()) {// target exist
				Collection<Tuple2<ObjectId, String>> subnodes;
				subnodes = new LinkedList<Tuple2<ObjectId, String>>();
				
				for (Tuple2<ObjectId, String> en : psn.subNodes()) {
					if (!en.item2.equals(Helper.nameOf(path))) {
						subnodes.add(en);
					}
				}
				
				ObjectId newps;
				newps = commit.addDirNode2db(psn.uid(), psn.gid(), psn.mode(), new Date(), subnodes);
				
				PatchResult pr1, pr2;
				
				path2nodeCache.dirty(path);
				path2nodeCache.dirty(newName);
				pr1 = commit.patch(psn.id().oid(), newps);
				pr2 = commit.patch(t.node().id().oid(), s.node().id().oid());
				commit.try2remove(pr1.oldId());
				commit.try2remove(pr2.oldId());
			} else if (!Helper.parentPath(path).equals(Helper.parentPath(newName))){ // target not exist && not same folder
				Collection<Tuple2<ObjectId, String>> subnodes_ps, subnodes_pt;
				subnodes_ps = new LinkedList<Tuple2<ObjectId, String>>();
				subnodes_pt = new LinkedList<Tuple2<ObjectId, String>>();
				
				String name;
				
				name = Helper.nameOf(path);
				
				for (Tuple2<ObjectId, String> en : psn.subNodes()) {
					if (!en.item2.equals(name)) {
						subnodes_ps.add(en);
					}
				}
				
				subnodes_pt.addAll(ptn.subNodes());
				subnodes_pt.add(new Tuple2<ObjectId, String>(s.node().id().oid(), Helper.nameOf(newName)));
	
				path2nodeCache.dirty(path);
				ObjectId newps, newpt;
				PatchResult pr1, pr2;
				newps = commit.addDirNode2db(psn.uid(), psn.gid(), psn.mode(), new Date(), subnodes_ps);
				newpt = commit.addDirNode2db(ptn.uid(), ptn.gid(), ptn.mode(), new Date(), subnodes_pt);
				pr1 = commit.patch(psn.id().oid(), newps);
				pr2 = commit.patch(ptn.id().oid(), newpt);
				commit.try2remove(pr1.oldId());
				commit.try2remove(pr2.oldId());
			} else {
				Collection<Tuple2<ObjectId, String>> subnodes;
				String name;
				ObjectId noid;
				PatchResult pr;
				
				name = Helper.nameOf(path);
				subnodes = new LinkedList<Tuple2<ObjectId, String>>();
				
				for (Tuple2<ObjectId, String> en : psn.subNodes()) {
					if (!en.item2.equals(name)) {
						subnodes.add(en);
					} else {
						subnodes.add(new Tuple2<ObjectId, String>(en.item1, Helper.nameOf(newName)));
					}
				}
				
				noid = commit.addDirNode2db(psn.uid(), psn.gid(), psn.mode(), new Date(), subnodes);
				pr = commit.patch(psn.id().oid(), noid);
				commit.try2remove(pr.oldId());
			}
			return 0;
		} catch (PathResolException ex) {
			return -ErrorCodes.EACCES();
		} catch (MongoException ex) {
			return -ErrorCodes.EIO();
		} finally {
			commit.writeLock().unlock();
		}
	}

	public final int truncate(String path, long offset) {
		int superTruncate;
		superTruncate = super.truncate(path, offset);
		if (superTruncate != -ErrorCodes.ENOENT()) {
			return superTruncate;
		}
		
		if (offset < 0) {
			return -ErrorCodes.EINVAL();
		}
		
		commit.writeLock().lock();
		
		try {
			AccessNode an;
			KabiFileNode fnode;
			Collection<Tuple2<ObjectId, Long>> subs;
			
			an = getNode(path, Constant.W_OK);
			
			if (an.node() == null) {
				return -ErrorCodes.ENOENT();
			}
			if (an.node().type() == KabiNodeType.DIRECTORY) {
				return -ErrorCodes.EISDIR();
			}
			if (!an.permission()) {
				return -ErrorCodes.EACCES();
			}
			
			fnode = (KabiFileNode) an.node();
			subs = new LinkedList<Tuple2<ObjectId, Long>>();
			
			for (Tuple2<ObjectId, Long> en : fnode.subNodes()) {
				if (en.item2 < offset) {
					subs.add(en);
				} else if (en.item2 == offset){
					subs.add(en);
					break;
				} else {
					subs.add(new Tuple2<ObjectId, Long>(en.item1, offset));
					break;
				}
			}
			ObjectId oid;
			PatchResult pr;
			
			oid = commit.addFileNode2db(fnode.uid(), fnode.gid(), fnode.mode(), new Date(), fnode.subNodes(), offset);
			pr = commit.patch(fnode.id().oid(), oid);
			commit.try2remove(pr.oldId());
			path2nodeCache.dirty(path);
			return 0;
		} catch (MongoException ex) {
			return -ErrorCodes.EIO();
		} catch (PathResolException ex) {
			return -ErrorCodes.EACCES();
		} finally {
			commit.writeLock().unlock();
		}
	}
	public final int utimens(String path, TimeBufferWrapper wrapper) {
		int superUtimens;
		superUtimens = super.utimens(path, null);
		if (superUtimens != -ErrorCodes.ENOENT()) {
			return superUtimens;
		}
		
		AccessNode an;
		ObjectId oid;
		
		commit.writeLock().lock();
		
		try {
			an = getNode(path, Constant.W_OK);
			
			if (an.node() == null) {
				return -ErrorCodes.ENOENT();
			}
			
			if (!an.permission() || wrapper == null) {
				return -ErrorCodes.EACCES();
			}
			
			if (path == null || !Helper.timeValid(wrapper)) {
				return -ErrorCodes.EINVAL();
			}
			
			if (wrapper.mod_nsec() == Constant.UTIME_OMIT) {
				return 0;
			}
			
			Date mod;
			PatchResult pr;
			
			mod = wrapper.mod_nsec() == Constant.UTIME_NOW
					? new Date()
					: new Date(wrapper.mod_sec() * 1000 + wrapper.mod_nsec() / 1000000);
			
			if (an.node().type() == KabiNodeType.FILE) {
				KabiFileNode fnode;
				fnode = (KabiFileNode) an.node();
				oid = commit.addFileNode2db(fnode.uid(), fnode.gid(), fnode.mode(), mod, fnode.subNodes(), fnode.size());
			} else {
				KabiDirectoryNode dnode;
				dnode = (KabiDirectoryNode) an.node();
				oid = commit.addDirNode2db(dnode.uid(), dnode.gid(), dnode.mode(), mod, dnode.subNodes());
			}
			pr = commit.patch(an.node().id().oid(), oid);
			commit.try2remove(pr.oldId());
			path2nodeCache.dirty(path);
			return 0;
		} catch (MongoException ex) {
			return -ErrorCodes.EIO();
		} catch (PathResolException ex) {
			return -ErrorCodes.EACCES();
		} finally {
			commit.writeLock().unlock();
		}
	}
}
