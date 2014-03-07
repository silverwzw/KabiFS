package com.silverwzw.kabiFS;

import java.io.File;
import java.nio.ByteBuffer;

import org.apache.log4j.Logger;

import com.silverwzw.kabiFS.util.Constant;
import com.silverwzw.kabiFS.util.FSOptions;
import com.silverwzw.kabiFS.util.MountOptions;
import com.silverwzw.kabiFS.util.Helper;

import net.fusejna.DirectoryFiller;
import net.fusejna.ErrorCodes;
import net.fusejna.FlockCommand;
import net.fusejna.FuseFilesystem;
import net.fusejna.StructFlock.FlockWrapper;
import net.fusejna.StructFuseContext;
import net.fusejna.StructFuseFileInfo.FileInfoWrapper;
import net.fusejna.StructStat.StatWrapper;
import net.fusejna.StructStatvfs.StatvfsWrapper;
import net.fusejna.StructTimeBuffer.TimeBufferWrapper;
import net.fusejna.XattrListFiller;
import net.fusejna.types.TypeMode.ModeWrapper;
import net.fusejna.types.TypeMode.NodeType;

public class HamFS extends FuseFilesystem {
	
	@SuppressWarnings("unused")
	private static final Logger logger;
	protected static final String metaDirName;
	
	static {
		metaDirName = ".kabimeta";
		logger = Logger.getLogger(HamFS.class);
	}
	

	protected final MountOptions mntoptions;
	protected final KabiDBAdapter datastore;
	public final FSOptions fsoptions;
	
	public HamFS(MountOptions options) {
		mntoptions = options; 
		datastore = new KabiDBAdapter(options.mongoConn());
		fsoptions = datastore.fsoptions();
	}

	public int access(String path, int access) {
		if (!path.startsWith(Helper.buildPath(metaDirName))) {
			return -ErrorCodes.ENOENT();
		}
		StructFuseContext context;
		context = getFuseContext();
		if (context.gid.longValue() !=0 && context.uid.longValue() != 0) {
			return -ErrorCodes.EACCES();
		}
		if (path.equals(Helper.buildPath(metaDirName))) {
			if (access == Constant.F_OK) {
				return 0;
			}
			if ((access & Constant.W_OK) != 0) {
				return -ErrorCodes.EACCES();
			}
			return 0;
		}
		if (path.equals(Helper.buildPath(metaDirName, "commits"))
			|| path.equals(Helper.buildPath(metaDirName, "fs_parameters"))
			|| path.equals(Helper.buildPath(metaDirName, "mount_options"))
			) {
			if (access == Constant.F_OK || access == Constant.R_OK) {
				return 0;
			} else {
				return -ErrorCodes.EACCES();
			}
		}
		if (path.equals(Helper.buildPath(metaDirName, "kabi_console"))) {
			if (access == Constant.F_OK || access == Constant.W_OK) {
				return 0;
			} else {
				return -ErrorCodes.EACCES();
			}
		}
		return -ErrorCodes.ENOENT();
	}
	
	public final void beforeMount(File mountPoint) {}

	public final void afterUnmount(File mountPoint) {}

	public final int bmap(String path, FileInfoWrapper info) {
		return 0;
	}

	public int create(String path, ModeWrapper mode, FileInfoWrapper info) {
		return metaNoAccess(path);
	}
	
	public final void beforeUnmount(File mountPoint) {}

	public final int fgetattr(String path, StatWrapper stat, FileInfoWrapper info) {
		return getattr(path, stat);
	}

	public final int flush(String path, FileInfoWrapper info) {
		return 0;
	}

	public final int fsync(String path, int datasync, FileInfoWrapper info) {
		return 0;
	}

	public final int fsyncdir(String path, int datasync, FileInfoWrapper info) {
		return 0;
	}

	public final int ftruncate(String path, long offset, FileInfoWrapper info) {
		return truncate(path, offset);
	}

	public int getattr(String path, StatWrapper stat) {
		if (!path.startsWith(Helper.buildPath(metaDirName))) {
			return -1;
		}
		if (path.equals(Helper.buildPath(metaDirName))) {
			stat.setMode(NodeType.DIRECTORY, true, false, true, true, false, true, false, false, false);
			stat.uid(0);
			stat.gid(0);
			return 0;
		}
		if (path.equals(Helper.buildPath(metaDirName, "mount_options"))) {
			stat.setMode(NodeType.FILE, true, false, false, true, false, false, false, false, false);
			stat.size(mntoptions.toMetaFile().length());
			stat.uid(0);
			stat.gid(0);
			return 0;
		}
		if (path.equals(Helper.buildPath(metaDirName, "fs_parameters"))) {
			stat.setMode(NodeType.FILE, true, false, false, true, false, false, false, false, false);
			stat.size(fsoptions.toString().length());
			stat.uid(0);
			stat.gid(0);
			return 0;
		}
		if (path.equals(Helper.buildPath(metaDirName, "commits"))) {
			stat.setMode(NodeType.FILE, true, false, false, true, false, false, false, false, false);
			stat.size(Helper.commitList2MetaFile(datastore.getCommitList()).length());
			stat.uid(0);
			stat.gid(0);
			return 0;
		}
		if (path.equals(Helper.buildPath(metaDirName, "kabi_console"))) {
			stat.setMode(NodeType.FILE, false, true, false, false, false, false, false, false, false);
			stat.uid(0);
			stat.gid(0);
			return 0;
		}
		return -1;
	}

	protected final String getName() {
		return "KabiFS";
	}
	/**
	 * Overrides getOptions() in super class, returns FUSE options
	 * @return FUSE Options, in String[], start with the 1st option
	 */
	protected final String[] getOptions() {
		return mntoptions.fuseOptions();
	}

	public final int getxattr(String path, String xattr, ByteBuffer buf, long size,
			long position) {
		return -ErrorCodes.ENOSYS();
	}

	public final void init() {}

	public final int link(String path, String target) {
		return -ErrorCodes.EMLINK();
	}

	public int listxattr(String path, XattrListFiller filler) {
		return -ErrorCodes.ENOSYS();
	}

	public final int lock(String path, FileInfoWrapper info, FlockCommand command,
			FlockWrapper flock) {
		return -ErrorCodes.ENOSYS();
	}

	public int mkdir(String path, ModeWrapper mode) {
		if (path.equals(Helper.buildPath(metaDirName))) {
			return -ErrorCodes.EEXIST();
		}
		return 0;
	}

	public final int mknod(String path, ModeWrapper mode, long dev) {
		if (mode.type() != NodeType.FILE) {
			return -ErrorCodes.EPERM();
		}
		return create(path, mode, null);
	}

	public int open(String path, FileInfoWrapper info) {
		return 0;
	}
	
	public int opendir(String path, FileInfoWrapper info) {
		return path.equals(Helper.buildPath(metaDirName)) ? 0 : -1;
	}

	public int read(String path, ByteBuffer buffer, long size, long offset, FileInfoWrapper info) {
		if (path.equals(Helper.buildPath(metaDirName, "mount_options"))) {
			return Helper.readString(mntoptions.toMetaFile(), buffer, size, offset);
		}
		if (path.equals(Helper.buildPath(metaDirName, "commits"))) {
			return Helper.readString(Helper.commitList2MetaFile(datastore.getCommitList()), buffer, size, offset);
		}
		if (path.equals(Helper.buildPath(metaDirName, "fs_parameters"))) {
			return Helper.readString(fsoptions.toString(), buffer, size, offset);
		}
		return -1;
	}

	public int readdir(String path, DirectoryFiller filler) {
		if (path.equals(Helper.buildPath(metaDirName))) {
			filler.add("commits");
			filler.add("fs_parameters");
			filler.add("mount_options");
			filler.add("kabi_console");
			return 0;
		} else {
			return -1;
		}
	}

	public final int readlink(String path, ByteBuffer buffer, long size) {
		return -ErrorCodes.EINVAL();
	}

	public int release(String path, FileInfoWrapper info) {
		return 0;
	}

	public int releasedir(String path, FileInfoWrapper info) {
		return 0;
	}

	public final int removexattr(String path, String xattr) {
		return -ErrorCodes.ENOSYS();
	}

	
	public int rename(String path, String newName) {
		return metaNoAccess(path);
	}

	public int rmdir(String path) {
		if (!path.startsWith(Helper.buildPath(metaDirName) + File.separator)) {
			return -ErrorCodes.EEXIST();
		}
		if (path.equals(Helper.buildPath(metaDirName))) {
			return -ErrorCodes.ENOTEMPTY();
		} else {
			return -ErrorCodes.ENOTDIR();
		}
	}

	public final int setxattr(String path, ByteBuffer buf, long size, int flags,
			long position) {
		return -ErrorCodes.ENOSYS();
	}

	public final int statfs(String path, StatvfsWrapper wrapper) {
		return 0;
	}

	public final int symlink(String path, String target) {
		return -ErrorCodes.EIO();
	}

	public int truncate(String path, long offset) {
		if (path.equals(Helper.buildPath(metaDirName, "kabi_console"))) {
			return 0;
		}
		return metaNoAccess(path);
	}

	public int unlink(String path) {
		return metaNoAccess(path);
	}

	public int utimens(String path, TimeBufferWrapper wrapper) {
		return metaNoAccess(path);
	}

	public int write(String path, ByteBuffer buf, long bufSize,
			long writeOffset, FileInfoWrapper info) {
		if (path.equals(Helper.buildPath(metaDirName, "kabi_console"))) {
			return 0;
		}
		return metaNoAccess(path);
	}

	public final void setLog(Logger logger) {
		super.log(logger);
	}

	public int chmod(String path, ModeWrapper mode) {
		return 0;
	}

	public int chown(String path, long uid, long gid) {
		return 0;
	}

	public void destroy() {}
	
	private final int metaNoAccess(String path) {
		String meta;
		meta = Helper.buildPath(metaDirName);
		return (path.equals(meta) || path.startsWith(meta + File.separator)) ? -ErrorCodes.EACCES() : -ErrorCodes.ENOENT(); 
	}
}
