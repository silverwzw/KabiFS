package com.silverwzw.kabiFS;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.MongoException;
import com.silverwzw.kabiFS.KabiDBAdapter.KabiPersistentCommit.KabiShadowCommit;
import com.silverwzw.kabiFS.KabiDBAdapter.KabiPersistentCommit.KabiWritableCommit;
import com.silverwzw.kabiFS.KabiDBAdapter.KabiPersistentCommit.KabiWritableCommit.PatchResult;
import com.silverwzw.kabiFS.KabiDBAdapter.NodeInfo;
import com.silverwzw.kabiFS.structure.Commit.DataBlock;
import com.silverwzw.kabiFS.structure.Commit.KabiDirectoryNode;
import com.silverwzw.kabiFS.structure.Commit.DirectoryItem;
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

import net.fusejna.DirectoryFiller;
import net.fusejna.ErrorCodes;
import net.fusejna.StructFuseContext;
import net.fusejna.StructFuseFileInfo.FileInfoWrapper;
import net.fusejna.StructStat.StatWrapper;
import net.fusejna.StructTimeBuffer.TimeBufferWrapper;
import net.fusejna.types.TypeMode.ModeWrapper;
import net.fusejna.types.TypeMode.NodeType;

public class KabiFS extends HamFS {
	
	private static final Logger fsoplogger;
	
	static {
		fsoplogger = Logger.getLogger("FSOP");
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

	private final static class AccessNode extends Tuple2<Boolean, Node> {
		AccessNode(boolean permission, Node node) {
			item1 = permission;
			item2 = node;
		}
		public final boolean permission() {
			return super.item1 == null ? false : super.item1;
		}
		public final Node node() {
			return super.item2;
		}
	}
	
	private final AccessNode getNode(NodeId nid, int access) {
		
		if (nid == null) {
			return new AccessNode(false, null);
		}
		
		NodeInfo nodeinfo;
		KabiNoneDataNode node;
		
		nodeinfo = commit.datastore().getNodeDBO(nid.oid());
		
		switch (nodeinfo.type()) {
			case FILE:
				node = commit.new KabiFileNode(nodeinfo.dbo());
				break;
			case DIRECTORY:
				node = commit.new KabiDirectoryNode(nodeinfo.dbo());
				break;
			case SUB:
				return new AccessNode(true, commit.new KabiSubNode(nodeinfo.dbo()));
			default:
				throw new RuntimeException("dead code");
		}

		StructFuseContext context;
		context = getFuseContext();
		
		if (
				((access & node.mode()) == access)
				||
				(context.gid.longValue() == node.gid() && ((access & (node.mode()>>3)) == access))
				||
				(context.uid.longValue() == node.uid() && ((access & (node.mode()>>6)) == access))
			){
			return new AccessNode(true, node);
		} else {
			return new AccessNode(false, node);
		}
	}
	
	private final AccessNode getNode(String path, int access) throws PathResolException {
		return getNode(findNodeByPath(path).nodeId(), access);
	}
	
	private final static class NodeAndParent extends Tuple2<NodeId, NodeId> {
		NodeAndParent(NodeId nid, NodeId parentnid) {
			item1 = nid;
			item2 = parentnid;
		}
		public final NodeId nodeId() {
			return item1;
		}
		public final NodeId parentId() {
			return item2;
		}
	}
	
	@SuppressWarnings("serial")
	private static class PathResolException extends Exception {}
	
	private NodeAndParent findNodeByPath(String path) throws PathResolException {

		NodeId nodenid, parentnid;
		
		nodenid = path2nodeCache.get(path);
		if (!path.equals(Helper.buildPath())) {
			parentnid = path2nodeCache.get(Helper.parentPath(path));
		} else {
			parentnid = null;
		}
		
		if (nodenid != null) {
			return new NodeAndParent(nodenid, parentnid);
		}
		
		nodenid = commit.root().id();
		parentnid = null;
		
		if (path.equals(Helper.buildPath())) {
			path2nodeCache.put(path, nodenid);
			return new NodeAndParent(nodenid, parentnid);
		}
		
		String[] comps;
		int uid, gid;
		
		comps = path.split(File.separator);
		uid = getFuseContext().uid.intValue();
		gid = getFuseContext().gid.intValue();
		
		for (int i = 1; i < comps.length; i++) {
			DBObject dbo;
			dbo = datastore.db().getCollection(fsoptions.collection_name(KabiNodeType.DIRECTORY)).findOne(new BasicDBObject("_id", nodenid.oid())); 
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
			
			parentnid = nodenid;
			nodenid = null;
			for (DirectoryItem sub : dnode.subNodes()) {
				if (sub.name().equals(comps[i])) {
					nodenid = commit.getNodeId(sub.oid());
					break;
				}
			}
			if (nodenid == null) {
				return new NodeAndParent(nodenid, parentnid);
			}
		}
		
		path2nodeCache.put(path, nodenid);
		return new NodeAndParent(nodenid, parentnid);
	}
	
	public final boolean isFull() {
		return ((Number) commit.datastore().db().getStats().get("fileSize")).longValue()
				>= fsoptions.max_volumn();
	}
	
	public final int getattr(final String path, final StatWrapper stat)
	{
		commit.readLock().lock();
		try {
			fsoplogger.info("getattr : " + path);
			if (path.equals(Helper.buildPath())) { // Root directory
				stat.setMode(NodeType.DIRECTORY);
				fsoplogger.info("\t0");
				return 0;
			}
			if (super.getattr(path, stat) >= 0) {
				fsoplogger.info("\t0");
				return 0;
			}
			
			NodeId nid;
			nid = findNodeByPath(path).nodeId();
			if (nid == null) {
				fsoplogger.info("\tENOENT");
				return -ErrorCodes.ENOENT();
			}
			Helper.setMode(stat, (KabiNoneDataNode) getNode(nid, 0).node());
			fsoplogger.info("\t0");
			return 0;
		} catch (PathResolException ex) {
			fsoplogger.info("\tEACCESS");
			return -ErrorCodes.EACCES();
		} catch (MongoException ex) {
			fsoplogger.info("\tEIO");
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
			fsoplogger.info("read : " + path + ", " + read_offset + " - " + (read_offset + read_size));
			
			NodeId nid;
			AccessNode nodeinfo;
			
			nid = findNodeByPath(path).nodeId();
			nodeinfo = getNode(nid, Constant.R_OK);
			if (nid == null) {
				fsoplogger.info("\tENOENT");
				return -ErrorCodes.ENOENT();
			}

			if (nodeinfo.node().type() == KabiNodeType.DIRECTORY) {
				fsoplogger.info("\tEISDIR");
				return ErrorCodes.EISDIR();
			}

			if (nodeinfo.node().type() != KabiNodeType.FILE) {
				fsoplogger.info("\tENOENT");
				return ErrorCodes.ENOENT();
			}
			
			if (!nodeinfo.permission()) {
				fsoplogger.info("\tEACCESS");
				return -ErrorCodes.EACCES();
			}
			
			int byte_count;
			long lastoffset;
			KabiFileNode filenode;
			
			byte_count = 0;
			lastoffset = 0;
			filenode = (KabiFileNode) nodeinfo.node();
			
			for (DataBlock block : filenode.subNodes()) {
				if (block.endoffset() <= read_offset) {
					lastoffset = block.endoffset();
					continue;
				}
				
				byte[] data2write;
				long endoffset, startoffset;
				
				endoffset = block.endoffset() < read_offset + read_size ? block.endoffset() : read_offset + read_size;
				startoffset = read_offset > lastoffset ? read_offset : lastoffset;
				data2write = commit.getSubNode(commit.getNodeId(block.oid()))
						.data((int)(startoffset - lastoffset + block.omit()), (int) (endoffset - lastoffset + block.omit()));
				buffer.put(data2write);
				byte_count += data2write.length;
				lastoffset = block.endoffset();
				
				if (lastoffset >= read_offset + read_size) {
					break;
				}
			}
			
			while (byte_count < read_size && read_offset + byte_count < filenode.size()) {
				buffer.put((byte)'\0');
				byte_count++;
			}
			fsoplogger.info("\t0");
			return byte_count;
			
		} catch (PathResolException ex) {
			fsoplogger.info("\tEACESS");
			return -ErrorCodes.EACCES();
		} catch (MongoException ex) {
			fsoplogger.info("\tEIO");
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
			fsoplogger.info("opendir : " + path);
			
			NodeId nid;
			AccessNode nodeinfo;
			
			nid = findNodeByPath(path).nodeId();
			
			if (nid == null) {
				fsoplogger.info("\tENOENT");
				return -ErrorCodes.ENOENT();
			}
			
			nodeinfo = getNode(nid, Constant.R_OK);
			
			fsoplogger.info("\t0");
			return nodeinfo.permission() ? 0 : -ErrorCodes.EACCES();
		} catch (PathResolException ex) {
			fsoplogger.info("\tEACCESS");
			return -ErrorCodes.EACCES();
		} catch (MongoException ex) {
			fsoplogger.info("\tEIO");
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
			fsoplogger.info("readdir : " + path);
			
			NodeId nid;
			AccessNode nodeinfo;
			nid = findNodeByPath(path).nodeId();
			nodeinfo = getNode(nid, Constant.R_OK);
			if (nid == null) {
				fsoplogger.info("\tENOENT");
				return -ErrorCodes.ENOENT();
			}
			if (nodeinfo.node().type() != KabiNodeType.DIRECTORY) {
				fsoplogger.info("\tENOTDIR");
				return -ErrorCodes.ENOTDIR();
			}
			if (!nodeinfo.permission()) {
				fsoplogger.info("\tEACCESS");
				return -ErrorCodes.EACCES();
			}
			for (DirectoryItem diritem : ((KabiDirectoryNode) nodeinfo.node()).subNodes()) {
				filler.add(diritem.name());
			}
			fsoplogger.info("\t0");
			return 0;
		} catch (PathResolException ex) {
			fsoplogger.info("\tEACCESS");
			return -ErrorCodes.EACCES();
		} catch (MongoException ex) {
			fsoplogger.info("\tEIO");
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
			fsoplogger.info("chmod : " + path);
			
			NodeId nid;
			AccessNode nodeinfo;
			
			nid = findNodeByPath(path).nodeId();
			
			if (nid == null) {
				fsoplogger.info("\tENOENT");
				return -ErrorCodes.ENOENT();
			}
			
			nodeinfo = getNode(nid, Constant.F_OK);
	
			if (!(getFuseContext().uid.longValue() == 0 || getFuseContext().uid.longValue() == ((KabiNoneDataNode)nodeinfo.node()).uid())) {
				fsoplogger.info("\tEACCESS");
				return -ErrorCodes.EACCES();
			}
			
			ObjectId newObjId;
			
			if (nodeinfo.node().type() == KabiNodeType.DIRECTORY) {
				KabiDirectoryNode dirNode;
				
				dirNode = (KabiDirectoryNode) nodeinfo.node();
				
				if (dirNode.mode() == (int) (mode.mode() % 01000)) { // no change
					fsoplogger.info("\t0");
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
					fsoplogger.info("\t0");
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
				fsoplogger.info("\t-1");
				return -1;
			}
			
			PatchResult pr;
			
			pr = commit.patch(nid.oid(), newObjId);
			path2nodeCache.dirty(path);
			commit.try2remove(pr.oldId());
			fsoplogger.info("\t0");
			return 0;
		} catch (PathResolException ex) {
			fsoplogger.info("\tEACCESS");
			return -ErrorCodes.EACCES();
		} catch (MongoException ex) {
			fsoplogger.info("\tEIO");
			return -ErrorCodes.EIO();
		} finally {
			commit.writeLock().unlock();
		}
	}

	public final int chown(String path, long uid, long gid) {
		commit.writeLock().lock();
		
		try {
			fsoplogger.info("chown : " + path);
			
			AccessNode nodeinfo;

			nodeinfo = getNode(path, Constant.W_OK);
			
			if (nodeinfo.node() == null) {
				fsoplogger.info("\tENOENT");
				return -ErrorCodes.ENOENT();
			}
			
			if (!(getFuseContext().uid.longValue() == 0 || nodeinfo.permission())) {
				fsoplogger.info("\tEACCESS");
				return -ErrorCodes.EACCES();
			}
	
			ObjectId newObjId;
			
			if (nodeinfo.node().type() == KabiNodeType.DIRECTORY) {
				KabiDirectoryNode dirNode;
				
				dirNode = (KabiDirectoryNode) nodeinfo.node();
				
				if (dirNode.uid() == uid && dirNode.gid() == gid) { // no change
					fsoplogger.info("\t0");
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
					fsoplogger.info("\t0");
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
				fsoplogger.info("\t-1");
				return -1;
			}
			
			PatchResult pr;
			pr = commit.patch(nodeinfo.node().id().oid(), newObjId);
			commit.try2remove(pr.oldId());
			path2nodeCache.dirty(path);
			fsoplogger.info("\t0");
			return 0;
		} catch (PathResolException ex) {
			fsoplogger.info("\tEACCESS");
			return -ErrorCodes.EACCES();
		} catch (MongoException ex) {
			fsoplogger.info("\tEIO");
			return -ErrorCodes.EIO();
		} finally {
			commit.writeLock().unlock();
		}
	}
	
	public final int access(String path, int access) {
		commit.readLock().lock();
		
		try {

			fsoplogger.info("access : " + path + ", " + access);
			
			int superAccess;
			superAccess = super.access(path, access);
			if (superAccess != -ErrorCodes.ENOENT()) {
				return superAccess;
			}
			
			NodeId nid;
			
			nid = findNodeByPath(path).nodeId();
			
			if (nid == null) {
				if (access == Constant.F_OK) {
					fsoplogger.info("\t-1");
					return -1;
				} else {
					fsoplogger.info("\tENOENT");
					return -ErrorCodes.ENOENT();
				}
			}
			
			
			//return getNode(nid, access).permission() ? 0 : -1 ;
			int ret = getNode(nid, access).permission() ? 0 : -1 ;
			fsoplogger.info(ret);
			return ret;
		} catch (PathResolException ex) {
			fsoplogger.info("\tEACCESS");
			return -ErrorCodes.EACCES();
		} catch (MongoException ex) {
			fsoplogger.info("\tEIO");
			return -ErrorCodes.EIO();
		} finally {
			commit.readLock().unlock();
		}
	}
	
	public final int mkdir(String path, ModeWrapper mode) {
		commit.writeLock().lock();
		
		try {
			fsoplogger.info("mkdir : " + path);
			
			int superMkdir;
			superMkdir = super.mkdir(path, mode);
			if (superMkdir != 0) {
				return superMkdir;
			}
			
			if (findNodeByPath(path).nodeId() != null) {
				fsoplogger.info("\tEEXIST");
				return -ErrorCodes.EEXIST();
			}
			
			if (isFull()) {
				fsoplogger.info("\tENOSPC");
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
				fsoplogger.info("\tEACCESS");
				return -ErrorCodes.EACCES();
			}
	
			Collection<DirectoryItem> subs;
			
			parent = (KabiDirectoryNode) an.node();
			subs = new LinkedList<DirectoryItem>(parent.subNodes());
			
			newDir = commit.addDirNode2db(
					context.uid.longValue(), 
					context.gid.longValue(), 
					(int)(mode.mode() % 01000), 
					new Date(),
					new ArrayList<DirectoryItem>(0)
					);
			
			subs.add(new DirectoryItem(newDir, Helper.nameOf(path)));
			
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
			fsoplogger.info("\t0");
			return 0;
			
		} catch (PathResolException ex) {
			fsoplogger.info("\tEACCESS");
			return -ErrorCodes.EACCES();
		} catch (MongoException ex) {
			fsoplogger.info("\tEIO");
			return -ErrorCodes.EIO();
		} finally {
			commit.writeLock().lock();
		}
	}
	
	public final int unlink(String path) {

		commit.writeLock().lock();
		
		try {
			fsoplogger.info("unlink : " + path);
			int superUnlink;
			superUnlink = super.unlink(path);
			if (superUnlink != -ErrorCodes.ENOENT()) {
				return superUnlink;
			}
			
			NodeId fileNid, parentNid;
			AccessNode fnodeinfo, pnodeinfo;
			NodeAndParent n2;
			String ppath;
			KabiDirectoryNode dnode;
			Collection<DirectoryItem> subs;
			ObjectId newpoid;
			
			n2 = findNodeByPath(path);
			fileNid = n2.nodeId();
			if (fileNid == null) {
				fsoplogger.info("\t0");
				return 0;
			}
			
			ppath = Helper.parentPath(path);
			parentNid = n2.parentId() == null ? findNodeByPath(ppath).nodeId() : n2.parentId();
			
			fnodeinfo = getNode(fileNid, Constant.F_OK);
			pnodeinfo = getNode(parentNid, Constant.W_OK);
			if (fnodeinfo.node().type() == KabiNodeType.DIRECTORY) {
				fsoplogger.info("\tEISDIR");
				return -ErrorCodes.EISDIR();
			}
			if (!pnodeinfo.permission()) {
				fsoplogger.info("\tEACCESS");
				return -ErrorCodes.EACCES();
			}
			
			dnode = (KabiDirectoryNode) pnodeinfo.node();
			
			subs = new LinkedList<DirectoryItem>();
			
			for (DirectoryItem en : dnode.subNodes()) {
				if (!en.name().equals(Helper.nameOf(path))) {
					subs.add(en);
				}
			}
			
			PatchResult pr;
			
			newpoid = commit.addDirNode2db(dnode.uid(), dnode.gid(), dnode.mode(), new Date(), subs);
			pr = commit.patch(parentNid.oid(), newpoid);

			path2nodeCache.dirty(ppath);
			path2nodeCache.dirty(path);
			commit.try2remove(fileNid.oid());
			commit.try2remove(pr.oldId());
			
			fsoplogger.info("\t0");
			return 0;
		} catch (PathResolException ex) {
			fsoplogger.info("\tEACCESS");
			return -ErrorCodes.EACCES();
		} catch (MongoException ex) {
			fsoplogger.info("\tEIO");
			return -ErrorCodes.EIO();
		} finally {
			commit.writeLock().unlock();
		}
	}
	
	public final int rmdir(String path) {
		commit.writeLock().lock();
		
		try {

			fsoplogger.info("rmdir : " + path);
			
			int superRmdir;
			superRmdir = super.rmdir(path);
			if (superRmdir != -ErrorCodes.EEXIST()) {
				return superRmdir;
			}
			
			if (path.equals(Helper.buildPath())) {
				fsoplogger.info("\tEFAULT");
				return -ErrorCodes.EFAULT();
			}
		
			
			NodeAndParent nids;
			AccessNode dir, pdir;
			KabiDirectoryNode dnode;
			List<DirectoryItem> subs;
			ObjectId newoid;
			
			nids = findNodeByPath(path);
			
			dir = getNode(nids.nodeId(), Constant.F_OK);
			
			if (!dir.permission()) {
				fsoplogger.info("\tEEXIST");
				return -ErrorCodes.EEXIST();
			}
			if (dir.node().type() != KabiNodeType.DIRECTORY) {
				fsoplogger.info("\tENOTDIR");
				return -ErrorCodes.ENOTDIR();
			}
			if (!((KabiDirectoryNode)dir.node()).subNodes().isEmpty()) {
				fsoplogger.info("\tENOTEMPTY");
				return -ErrorCodes.ENOTEMPTY();
			}
			
			pdir = getNode(nids.parentId() == null ? findNodeByPath(Helper.parentPath(path)).nodeId() : nids.parentId(), Constant.W_OK);
			
			if (!pdir.permission()) {
				fsoplogger.info("\tEACCESS");
				return -ErrorCodes.EACCES();
			}
			
			dnode = (KabiDirectoryNode) pdir.node();
			subs = new LinkedList<DirectoryItem>();
			for (DirectoryItem di : dnode.subNodes()) {
				if (!di.name().equals(Helper.nameOf(path))) {
					subs.add(di);
				}
			}
			
			PatchResult pr;
			
			newoid = commit.addDirNode2db(dnode.uid(), dnode.gid(), dnode.mode(), new Date(), subs);
			pr = commit.patch(pdir.node().id().oid(), newoid);
			path2nodeCache.dirty(path);
			path2nodeCache.dirty(Helper.parentPath(path));
			commit.try2remove(nids.nodeId().oid());
			commit.try2remove(pr.oldId());
			fsoplogger.info("\t0");
			return 0;
		} catch (PathResolException ex) {
			fsoplogger.info("\tEACCESS");
			return -ErrorCodes.EACCES();
		} catch (MongoException ex) {
			fsoplogger.info("\tEIO");
			return -ErrorCodes.EIO();
		} finally {
			commit.writeLock().unlock();
		}
	}
	
	public final int rename(String path, String newName) {

		commit.writeLock().lock();
		
		try {

			fsoplogger.info("rename : " + path + " -> " + newName);
			
			int superRename;
			superRename = super.rename(path, newName);
			if (superRename != -ErrorCodes.ENOENT()) {
				return superRename;
			}
			
			if (path.equals(newName)) {
				fsoplogger.info("\t0");
				return 0;
			}
			
			if (path.equals(Helper.buildPath()) || newName.equals(Helper.buildPath())) {
				fsoplogger.info("\tEBUSY");
				return -ErrorCodes.EBUSY();
			}
			
			if (newName.startsWith(path) && newName.charAt(path.length()) == File.separatorChar) {
				fsoplogger.info("\tEINVAL");
				return -ErrorCodes.EINVAL();
			}
	
			if (isFull()) {
				fsoplogger.info("\tENOSPC");
				return -ErrorCodes.ENOSPC();
			}
		
			NodeAndParent nids;
			AccessNode ps, s, pt, t;
			
			nids = findNodeByPath(path);
			
			s = getNode(nids.nodeId(), Constant.F_OK);
			ps = getNode(nids.parentId() == null ? findNodeByPath(Helper.parentPath(path)).nodeId(): nids.parentId(), Constant.W_OK + Constant.X_OK);
			
			nids = findNodeByPath(newName);
			
			t = getNode(nids.nodeId(), Constant.F_OK);
			
			pt = getNode(nids.parentId() == null ? findNodeByPath(Helper.parentPath(newName)).nodeId() : nids.parentId(), Constant.W_OK + Constant.X_OK);
			
			
			if (!s.permission()) {
				fsoplogger.info("\tENOENT");
				return -ErrorCodes.ENOENT();
			}
			
			if ((!ps.permission()) || !pt.permission()) {
				fsoplogger.info("\tEACCESS");
				return -ErrorCodes.EACCES();
			}
			
			if (t.permission()) {
				if (t.node().type() == KabiNodeType.DIRECTORY && s.node().type() != KabiNodeType.DIRECTORY) {
					fsoplogger.info("\tEISDIR");
					return -ErrorCodes.EISDIR(); 
				}
				if (t.node().type() != KabiNodeType.DIRECTORY && s.node().type() == KabiNodeType.DIRECTORY) {
					fsoplogger.info("\tENOTDIR");
					return -ErrorCodes.ENOTDIR(); 
				}
				if (t.node().type() == KabiNodeType.DIRECTORY && !((KabiDirectoryNode)t.node()).subNodes().isEmpty()) {
					fsoplogger.info("\tENOTEMPTY");
					return -ErrorCodes.ENOTEMPTY();
				}
			}
			
			KabiDirectoryNode ptn, psn;
			
			ptn = (KabiDirectoryNode) pt.node();
			psn = (KabiDirectoryNode) ps.node();
			
			if (t.permission()) {// target exist
				Collection<DirectoryItem> subnodes;
				subnodes = new LinkedList<DirectoryItem>();
				
				for (DirectoryItem en : psn.subNodes()) {
					if (!en.name().equals(Helper.nameOf(path))) {
						subnodes.add(en);
					}
				}
				
				ObjectId newps;
				newps = commit.addDirNode2db(psn.uid(), psn.gid(), psn.mode(), new Date(), subnodes);
				
				PatchResult pr1, pr2;

				path2nodeCache.dirty(Helper.parentPath(path));
				path2nodeCache.dirty(path);
				path2nodeCache.dirty(newName);
				pr1 = commit.patch(psn.id().oid(), newps);
				pr2 = commit.patch(t.node().id().oid(), s.node().id().oid());
				commit.try2remove(pr1.oldId());
				commit.try2remove(pr2.oldId());
			} else if (!Helper.parentPath(path).equals(Helper.parentPath(newName))){ // target not exist && not same folder
				Collection<DirectoryItem> subnodes_ps, subnodes_pt;
				subnodes_ps = new LinkedList<DirectoryItem>();
				subnodes_pt = new LinkedList<DirectoryItem>();
				
				String name;
				
				name = Helper.nameOf(path);
				
				for (DirectoryItem en : psn.subNodes()) {
					if (!en.name().equals(name)) {
						subnodes_ps.add(en);
					}
				}
				
				subnodes_pt.addAll(ptn.subNodes());
				subnodes_pt.add(new DirectoryItem(s.node().id().oid(), Helper.nameOf(newName)));
	
				ObjectId newps, newpt;
				PatchResult pr1, pr2;
				newps = commit.addDirNode2db(psn.uid(), psn.gid(), psn.mode(), new Date(), subnodes_ps);
				newpt = commit.addDirNode2db(ptn.uid(), ptn.gid(), ptn.mode(), new Date(), subnodes_pt);
				
				path2nodeCache.dirty(path);
				path2nodeCache.dirty(Helper.parentPath(path));
				path2nodeCache.dirty(Helper.parentPath(newName));
				
				pr1 = commit.patch(psn.id().oid(), newps);
				pr2 = commit.patch(ptn.id().oid(), newpt);
				
				commit.try2remove(pr1.oldId());
				commit.try2remove(pr2.oldId());
			} else {
				Collection<DirectoryItem> subnodes;
				String name;
				ObjectId noid;
				PatchResult pr;
				
				name = Helper.nameOf(path);
				subnodes = new LinkedList<DirectoryItem>();
				
				for (DirectoryItem en : psn.subNodes()) {
					if (!en.name().equals(name)) {
						subnodes.add(en);
					} else {
						subnodes.add(new DirectoryItem(en.oid(), Helper.nameOf(newName)));
					}
				}
				
				path2nodeCache.dirty(path);
				path2nodeCache.dirty(Helper.parentPath(path));
				noid = commit.addDirNode2db(psn.uid(), psn.gid(), psn.mode(), new Date(), subnodes);
				pr = commit.patch(psn.id().oid(), noid);
				commit.try2remove(pr.oldId());
			}
			fsoplogger.info("\t0");
			return 0;
		} catch (PathResolException ex) {
			fsoplogger.info("\tEACCESS");
			return -ErrorCodes.EACCES();
		} catch (MongoException ex) {
			fsoplogger.info("\tEIO");
			return -ErrorCodes.EIO();
		} finally {
			commit.writeLock().unlock();
		}
	}
	
	public final int truncate(String path, long offset) {
		commit.writeLock().lock();
		
		try {

			fsoplogger.info("truncate : " + path + ", " + offset);
			
			int superTruncate;
			superTruncate = super.truncate(path, offset);
			if (superTruncate != -ErrorCodes.ENOENT()) {
				return superTruncate;
			}
			
			if (offset < 0) {
				fsoplogger.info("\tEINVAL");
				return -ErrorCodes.EINVAL();
			}
			
			AccessNode an;
			KabiFileNode fnode;
			List<DataBlock> subs;
			
			an = getNode(path, Constant.W_OK);
			
			if (an.node() == null) {
				fsoplogger.info("\tENOENT");
				return -ErrorCodes.ENOENT();
			}
			if (an.node().type() == KabiNodeType.DIRECTORY) {
				fsoplogger.info("\tEISDIR");
				return -ErrorCodes.EISDIR();
			}
			if (!an.permission()) {
				fsoplogger.info("\tEACCESS");
				return -ErrorCodes.EACCES();
			}
			
			fnode = (KabiFileNode) an.node();
			
			if (fnode.size() == offset) {
				fsoplogger.info("\t0");
				return 0;
			}
			
			subs = new LinkedList<DataBlock>();
			
			if (offset != 0) {
				for (DataBlock en : fnode.subNodes()) {
					if (en.endoffset() < offset) {
						subs.add(en);
					} else if (en.endoffset() == offset){
						subs.add(en);
						break;
					} else {
						subs.add(new DataBlock(en.oid(), offset));
						break;
					}
				}
			}
			
			ObjectId oid;
			PatchResult pr;
			
			oid = commit.addFileNode2db(fnode.uid(), fnode.gid(), fnode.mode(), new Date(), subs, offset);
			pr = commit.patch(fnode.id().oid(), oid);
			commit.try2remove(pr.oldId());
			path2nodeCache.dirty(path);
			fsoplogger.info("\t0");
			return 0;
		} catch (MongoException ex) {
			fsoplogger.info("\tEIO");
			return -ErrorCodes.EIO();
		} catch (PathResolException ex) {
			fsoplogger.info("\tEACCESS");
			return -ErrorCodes.EACCES();
		} finally {
			commit.writeLock().unlock();
		}
	}
	public final int utimens(String path, TimeBufferWrapper wrapper) {
		commit.writeLock().lock();
		
		try {
			
			fsoplogger.info("utimens : " + path);
				
			int superUtimens;
			superUtimens = super.utimens(path, null);
			if (superUtimens != -ErrorCodes.ENOENT()) {
				return superUtimens;
			}
			
			AccessNode an;
			ObjectId oid;
			
			an = getNode(path, Constant.W_OK);
			
			if (an.node() == null) {
				fsoplogger.info("\tENOENT");
				return -ErrorCodes.ENOENT();
			}
			
			if (!an.permission() || wrapper == null) {
				fsoplogger.info("\tEACCESS");
				return -ErrorCodes.EACCES();
			}
			
			if (path == null || !Helper.timeValid(wrapper)) {
				fsoplogger.info("\tEINVAL");
				return -ErrorCodes.EINVAL();
			}
			
			if (wrapper.mod_nsec() == Constant.UTIME_OMIT) {
				fsoplogger.info("\t0");
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
			fsoplogger.info("\t0");
			return 0;
		} catch (MongoException ex) {
			fsoplogger.info("\tEIO");
			return -ErrorCodes.EIO();
		} catch (PathResolException ex) {
			fsoplogger.info("\tEACCESS");
			return -ErrorCodes.EACCES();
		} finally {
			commit.writeLock().unlock();
		}
	}
	public final int write(String path, ByteBuffer buf, long writeSize, long writeOffset, FileInfoWrapper info) {
		commit.writeLock().lock();
		try {
			fsoplogger.info("write : " + path + ", " + writeOffset + " - " + (writeOffset + writeSize));
			
			int superWrite;
			superWrite = super.write(path, buf, writeSize, writeOffset, info);
			if (superWrite != -ErrorCodes.ENOENT()) {
				return superWrite;
			}
			
			AccessNode an;

			an = getNode(path, Constant.W_OK);
			
			if (!an.permission()) {
				fsoplogger.info("\tEACCESS");
				return -ErrorCodes.EACCES();
			}
			if (writeSize <= 0) {
				fsoplogger.info("0");
				return 0;
			}
			if (isFull()) {
				fsoplogger.info("\tENOSPC");
				return -ErrorCodes.ENOSPC();
			}
			
			if (an.node().type() != KabiNodeType.FILE) {
				fsoplogger.info("\tEINVAL");
				return -ErrorCodes.EINVAL();
			}
			
			KabiFileNode fnode;
			List<DataBlock> newSubNodes;
			long lastoffset = 0;
			
			fnode = (KabiFileNode) an.node();
			newSubNodes = new LinkedList<DataBlock>();
			
			
			for ( DataBlock block : fnode.subNodes()) {
				if (block.endoffset() <= writeOffset || lastoffset <= writeOffset + writeSize) {
					newSubNodes.add(block);
				} else if (writeOffset > lastoffset + fsoptions.min_block_size() && writeOffset + writeSize >= block.endoffset() && block.endoffset() - writeOffset >= fsoptions.min_block_size()) {
					byte[] bytes2write;
					ObjectId newSubNodeOid;
					
					bytes2write = new byte[(int) (block.endoffset() - writeOffset)];
					
					for (int counter = 0; counter < bytes2write.length; counter++) {
						bytes2write[counter] = buf.get(counter);
					}
					
					newSubNodeOid = commit.addSubNode2db(bytes2write);
					newSubNodes.add(new DataBlock(block.oid(), writeOffset, block.omit()));
					newSubNodes.add(new DataBlock(newSubNodeOid, block.endoffset(), 0));
					
				} else if (writeOffset <= lastoffset && writeOffset + writeSize >= lastoffset + fsoptions.min_block_size() && writeOffset + writeSize <= block.endoffset() - fsoptions.min_block_size()) {
					byte[] bytes2write;
					ObjectId newSubNodeOid;
					
					bytes2write = new byte[(int) (writeOffset + writeSize - lastoffset)];
					
					for (int counter = 0; counter < bytes2write.length; counter++) {
						bytes2write[counter] = buf.get((int) (lastoffset - writeOffset + counter));
					}
					
					newSubNodeOid = commit.addSubNode2db(bytes2write);
					newSubNodes.add(new DataBlock(newSubNodeOid, writeOffset + writeSize, 0));
					newSubNodes.add(new DataBlock(block.oid(), block.endoffset(), writeOffset + writeSize - lastoffset));
					
				} else if (lastoffset <= writeOffset && writeOffset + writeSize < block.endoffset()) {
					
					byte[] bytes2write, old_block_data;
					int write_pointer;
					ObjectId newSubNodeOid;
					
					write_pointer = 0;
					bytes2write = new byte[(int) (block.endoffset() - lastoffset)];
					old_block_data = commit.getSubNode(commit.getNodeId(block.oid()))
							.data((int)block.omit(), (int)block.endoffset());
							
					
					while (write_pointer < bytes2write.length) {
						if (write_pointer + lastoffset < writeOffset || write_pointer + lastoffset >= writeOffset + writeSize) {
							bytes2write[write_pointer] = old_block_data[write_pointer];
						} else {
							bytes2write[write_pointer] = buf.get((int) (lastoffset + write_pointer - writeOffset));
						}
						write_pointer++;
					}
					
					newSubNodeOid = commit.addSubNode2db(bytes2write);
					newSubNodes.add(new DataBlock(newSubNodeOid, block.endoffset(), 0));
				}
				lastoffset = block.endoffset();
			}
			
			if (lastoffset < writeOffset + writeSize) {
				long currentOffset;
				byte[] bBuffer;
				int buffer_count;
				
				currentOffset = lastoffset;
				bBuffer = null;
				buffer_count = 0;
				
				while (currentOffset < writeOffset + writeSize) {
					if (bBuffer == null || bBuffer.length == buffer_count) {
						if (bBuffer != null) {
							newSubNodes.add(new DataBlock(commit.addSubNode2db(bBuffer), currentOffset, 0));
						}
						if (writeOffset + writeSize - currentOffset > fsoptions.max_block_size()) {
							bBuffer = new byte[(int) fsoptions.max_block_size()];
						} else {
							bBuffer = new byte[(int) (writeOffset + writeSize - currentOffset)];
						}
						buffer_count = 0;
					}
					bBuffer[buffer_count] = buf.get((int) (currentOffset - writeOffset));
					currentOffset++;
					buffer_count++;
				}
				
				newSubNodes.add(new DataBlock(commit.addSubNode2db(bBuffer), currentOffset, 0));
			}
			
			ObjectId newFileNode;
			PatchResult pr;
			long newSize;
			
			if (fnode.size() >= writeOffset + writeSize) {
				newSize = fnode.size();
			} else {
				newSize = writeOffset + writeSize;
			}
			
			newFileNode = commit.addFileNode2db(fnode.uid(), fnode.gid(), fnode.mode(), new Date(), newSubNodes, newSize);
			pr = commit.patch(fnode.id().oid(), newFileNode);
			commit.try2remove(pr.oldId());
			path2nodeCache.dirty(path);
			fsoplogger.info("\t0");
			return (int) writeSize;
		} catch (MongoException ex) {
			fsoplogger.info("\tEIO");
			return -ErrorCodes.EIO();
		} catch (PathResolException ex) {
			fsoplogger.info("\tEACCESS");
			return -ErrorCodes.EACCES();
		} finally {
			commit.writeLock().unlock();
		}
	}
	
	public final int create(String path, ModeWrapper mode, FileInfoWrapper info) {
		commit.writeLock().lock();
		try {

			fsoplogger.info("create : " + path);
			int ham;
			ham = super.create(path, mode, info);
			if (ham != -ErrorCodes.ENOENT()) {
				return ham;
			}
			
			AccessNode an;
			
			an = getNode(Helper.parentPath(path), Constant.W_OK);
			
			if (!an.permission()) {
				fsoplogger.info("\tEACCESS");
				return -ErrorCodes.EACCES();
			} else if (an.node().type() != KabiNodeType.DIRECTORY) {
				fsoplogger.info("\tENOTDIR");
				return -ErrorCodes.ENOTDIR(); 
			}
			
			ObjectId newFileOid, newDirOid;
			PatchResult pr;
			KabiDirectoryNode dir;
			StructFuseContext context;
			Collection<DirectoryItem> items;
			
			context = getFuseContext();
			dir = (KabiDirectoryNode) an.node();
			items = dir.subNodes();
			
			newFileOid = commit.addFileNode2db(
					context.uid.longValue(),
					context.gid.longValue(),
					(int)mode.mode() % 01000,
					new Date(),
					new ArrayList<DataBlock>(0),
					0);
			
			items.add(new DirectoryItem(newFileOid, Helper.nameOf(path)));
			
			newDirOid = commit.addDirNode2db(dir.uid(), dir.gid(), dir.mode(), new Date(), items);
			
			pr = commit.patch(dir.id().oid(), newDirOid);
			path2nodeCache.dirty(Helper.parentPath(path));
			commit.try2remove(pr.oldId());
			fsoplogger.info("\t0");
			return 0;
			
		} catch (MongoException ex) {
			fsoplogger.info("\tEIO");
			return -ErrorCodes.EIO();
		} catch (PathResolException ex) {
			fsoplogger.info("\tEACCESS");
			return -ErrorCodes.EACCES();
		} finally {
			commit.writeLock().unlock();
		}
	}
}
