package com.silverwzw.kabiFS;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Collection;

import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

import com.mongodb.DB;
import com.silverwzw.kabiFS.structure.Commit;
import com.silverwzw.kabiFS.util.MountOptions;
import com.silverwzw.kabiFS.util.Tuple3;
import com.silverwzw.kabiFS.util.Helper;

import net.fusejna.DirectoryFiller;
import net.fusejna.ErrorCodes;
import net.fusejna.FlockCommand;
import net.fusejna.FuseFilesystem;
import net.fusejna.StructFlock.FlockWrapper;
import net.fusejna.StructFuseFileInfo.FileInfoWrapper;
import net.fusejna.StructStat.StatWrapper;
import net.fusejna.StructStatvfs.StatvfsWrapper;
import net.fusejna.StructTimeBuffer.TimeBufferWrapper;
import net.fusejna.XattrListFiller;
import net.fusejna.types.TypeMode.ModeWrapper;
import net.fusejna.types.TypeMode.NodeType;

public abstract class MetaFS extends FuseFilesystem {
	
	@SuppressWarnings("unused")
	private static final Logger logger;
	protected static final String metaDirName;
	
	public static interface DatastoreAdapter {
		/**
		 * get the Commit list
		 * @return collection of tuples, 1st item is ObjectId of commit, 2nd is name, 3rd is the ObjectId of base commit.
		 */
		public Collection<Tuple3<ObjectId, String, ObjectId>> getCommitList();
		/**
		 * get the commit object by commit name
		 * @param commitName not necessary the full name
		 * @return the Commit Object
		 */
		public Commit getCommit(String commitName);
		/**
		 * delete a commit
		 * @param commitName the FULL name of the commit
		 */
		public void deleteCommit(ObjectId commitName);
		/**
		 * return the db reference
		 * @return DB
		 */
		public DB db();
		/**
		 * initiate the File System (will earse everything)
		 */
		public void initFS();
	}
	
	static {
		metaDirName = ".kabimeta";
		logger = Logger.getLogger(MetaFS.class);
	}
	

	protected final MountOptions mntoptions;
	protected final DatastoreAdapter datastore;
	
	public MetaFS(MountOptions options) {
		this.mntoptions = options; 
		datastore = new KabiDBAdapter(options.mongoConn());
	}
	
	@Override
	public int access(String path, int access) {
		// TODO Auto-generated method stub
		return -ErrorCodes.ENOSYS();
	}

	@Override
	public void afterUnmount(File mountPoint) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void beforeMount(File mountPoint) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void beforeUnmount(File mountPoint) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int bmap(String path, FileInfoWrapper info) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int chmod(String path, ModeWrapper mode) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int chown(String path, long uid, long gid) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int create(String path, ModeWrapper mode, FileInfoWrapper info) {
		// TODO Auto-generated method stub
		return -ErrorCodes.ENOSYS();
	}

	@Override
	public void destroy() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int fgetattr(String path, StatWrapper stat, FileInfoWrapper info) {
		// TODO Auto-generated method stub
		return getattr(path, stat);
	}

	@Override
	public int flush(String path, FileInfoWrapper info) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int fsync(String path, int datasync, FileInfoWrapper info) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int fsyncdir(String path, int datasync, FileInfoWrapper info) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int ftruncate(String path, long offset, FileInfoWrapper info) {
		// TODO Auto-generated method stub
		return truncate(path, offset);
	}

	@Override
	public int getattr(String path, StatWrapper stat) {
		if (path.equals(Helper.buildPath(metaDirName))) {
			stat.setMode(NodeType.DIRECTORY, true, false, true, true, false, true, false, false, false);
			stat.uid(getFuseContextUid().intValue());
			stat.gid(getFuseContextGid().intValue());
			return 0;
		}
		if (path.equals(Helper.buildPath(metaDirName, "mount_options"))) {
			stat.setMode(NodeType.FILE, true, false, false, true, false, false, false, false, false);
			stat.size(mntoptions.toMetaFile().length());
			stat.uid(getFuseContextUid().intValue());
			stat.gid(getFuseContextGid().intValue());
			return 0;
		}
		if (path.equals(Helper.buildPath(metaDirName, "fs_parameters"))) {
			stat.setMode(NodeType.FILE, true, false, false, true, false, false, false, false, false);
			stat.uid(getFuseContextUid().intValue());
			stat.gid(getFuseContextGid().intValue());
			return 0;
		}
		if (path.equals(Helper.buildPath(metaDirName, "commits"))) {
			stat.setMode(NodeType.FILE, true, false, false, true, false, false, false, false, false);
			stat.size(Helper.commitList2MetaFile(datastore.getCommitList()).length());
			stat.uid(getFuseContextUid().intValue());
			stat.gid(getFuseContextGid().intValue());
			return 0;
		}
		if (path.equals(Helper.buildPath(metaDirName, "kabi_console_out"))) {
			stat.setMode(NodeType.FILE, true, false, false, true, false, false, false, false, false);
			stat.uid(getFuseContextUid().intValue());
			stat.gid(getFuseContextGid().intValue());
			return 0;
		}
		if (path.equals(Helper.buildPath(metaDirName, "kabi_console_in"))) {
			stat.setMode(NodeType.FILE, false, true, false, false, false, false, false, false, false);
			stat.uid(getFuseContextUid().intValue());
			stat.gid(getFuseContextGid().intValue());
			return 0;
		}
		return -1;
	}

	@Override
	protected String getName() {
		// TODO Auto-generated method stub
		return null;
	}
	/**
	 * Overrides getOptions() in super class, returns FUSE options
	 * @return FUSE Options, in String[], start with the 1st option
	 */
	protected final String[] getOptions() {
		return mntoptions.fuseOptions();
	}

	@Override
	public int getxattr(String path, String xattr, ByteBuffer buf, long size,
			long position) {
		// TODO Auto-generated method stub
		return -ErrorCodes.ENOSYS();
	}

	public void init() {}

	@Override
	public int link(String path, String target) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int listxattr(String path, XattrListFiller filler) {
		// TODO Auto-generated method stub
		return -ErrorCodes.ENOSYS();
	}

	@Override
	public int lock(String path, FileInfoWrapper info, FlockCommand command,
			FlockWrapper flock) {
		// TODO Auto-generated method stub
		return -ErrorCodes.ENOSYS();
	}

	@Override
	public int mkdir(String path, ModeWrapper mode) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int mknod(String path, ModeWrapper mode, long dev) {
		// TODO Auto-generated method stub
		return create(path, mode, null);
	}

	@Override
	public int open(String path, FileInfoWrapper info) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int opendir(String path, FileInfoWrapper info) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int read(String path, ByteBuffer buffer, long size, long offset, FileInfoWrapper info) {
		if (path.equals(Helper.buildPath(metaDirName, "mount_options"))) {
			return Helper.readString(mntoptions.toMetaFile(), buffer, size, offset);
		}
		if (path.equals(Helper.buildPath(metaDirName, "commits"))) {
			return Helper.readString(Helper.commitList2MetaFile(datastore.getCommitList()), buffer, size, offset);
		}
		return -1;
	}

	public int readdir(String path, DirectoryFiller filler) {
		if (path.equals(Helper.buildPath(metaDirName))) {
			filler.add("commits");
			filler.add("fs_parameters");
			filler.add("mount_options");
			filler.add("kabi_console_in");
			filler.add("kabi_console_out");
			return 0;
		} else {
			return -1;
		}
	}

	@Override
	public int readlink(String path, ByteBuffer buffer, long size) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int release(String path, FileInfoWrapper info) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int releasedir(String path, FileInfoWrapper info) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int removexattr(String path, String xattr) {
		// TODO Auto-generated method stub
		return -ErrorCodes.ENOSYS();
	}

	@Override
	public int rename(String path, String newName) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int rmdir(String path) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int setxattr(String path, ByteBuffer buf, long size, int flags,
			long position) {
		// TODO Auto-generated method stub
		return -ErrorCodes.ENOSYS();
	}

	@Override
	public int statfs(String path, StatvfsWrapper wrapper) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int symlink(String path, String target) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int truncate(String path, long offset) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int unlink(String path) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int utimens(String path, TimeBufferWrapper wrapper) {
		// TODO Auto-generated method stub
		return -ErrorCodes.ENOSYS();
	}

	@Override
	public int write(String path, ByteBuffer buf, long bufSize,
			long writeOffset, FileInfoWrapper info) {
		// TODO Auto-generated method stub
		return 0;
	}

	public final void setLog(Logger logger) {
		super.log(logger);
	}
}
