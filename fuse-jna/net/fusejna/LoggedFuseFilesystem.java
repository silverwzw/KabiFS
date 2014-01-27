package net.fusejna;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.log4j.Logger;

import net.fusejna.StructFlock.FlockWrapper;
import net.fusejna.StructFuseFileInfo.FileInfoWrapper;
import net.fusejna.StructStat.StatWrapper;
import net.fusejna.StructStatvfs.StatvfsWrapper;
import net.fusejna.StructTimeBuffer.TimeBufferWrapper;
import net.fusejna.types.TypeMode.ModeWrapper;

final class LoggedFuseFilesystem extends FuseFilesystem
{
	private final Logger logger;
	private final FuseFilesystem filesystem;

	LoggedFuseFilesystem(final FuseFilesystem filesystem, final Logger logger)
	{
		this.filesystem = filesystem;
		if (logger != null) {
			this.logger = logger;
		} else {
			this.logger = Logger.getLogger(LoggedFuseFilesystem.class);
		}
	}

	private final String log(Object ... args){
		return "FUSE-JNA : " + (args.length == 0 ? "" : Arrays.toString(args));
	}
	
	@Override
	public int access(final String path, final int access)
	{
		logger.debug(log(path,access));
		return filesystem.access(path, access);
	}

	@Override
	public void afterUnmount(final File mountPoint)
	{
		logger.debug(log(mountPoint));
		filesystem.afterUnmount(mountPoint);
	}

	@Override
	public void beforeMount(final File mountPoint)
	{
		logger.debug(log(mountPoint));
		filesystem.beforeMount(mountPoint);
	}

	@Override
	public void beforeUnmount(final File mountPoint)
	{
		logger.debug(log(mountPoint));
		filesystem.beforeUnmount(mountPoint);
	}

	@Override
	public int bmap(final String path, final FileInfoWrapper info)
	{
		logger.debug(log(path, info));
		return filesystem.bmap(path, info);
	}

	@Override
	public int chmod(final String path, final ModeWrapper mode)
	{
		logger.debug(log(path,mode));
		return filesystem.chmod(path, mode);
	}

	@Override
	public int chown(final String path, final long uid, final long gid)
	{
		logger.debug(log(path,uid,gid));
		return filesystem.chown(path, uid, gid);
	}

	@Override
	public int create(final String path, final ModeWrapper mode, final FileInfoWrapper info)
	{
		logger.debug(log(path, mode, info));
		return filesystem.create(path, mode, info);
	}

	@Override
	public void destroy()
	{
		logger.debug(log());
		filesystem.destroy();
	}

	@Override
	public int fgetattr(final String path, final StatWrapper stat, final FileInfoWrapper info)
	{
		logger.debug(log(path, stat, info));
		return filesystem.fgetattr(path, stat, info);
	}

	@Override
	public int flush(final String path, final FileInfoWrapper info)
	{
		logger.debug(log(path, info));
		return filesystem.flush(path, info);
	}

	@Override
	public int fsync(final String path, final int datasync, final FileInfoWrapper info)
	{
		logger.debug(log(path, datasync, info));
		return filesystem.fsync(path, datasync, info);
	}

	@Override
	public int fsyncdir(final String path, final int datasync, final FileInfoWrapper info)
	{
		logger.debug(log(path, datasync, info));
		return filesystem.fsyncdir(path, datasync, info);
	}

	@Override
	public int ftruncate(final String path, final long offset, final FileInfoWrapper info)
	{
		logger.debug(log(path, offset, info));
		return filesystem.ftruncate(path, offset, info);
	}

	@Override
	public int getattr(final String path, final StatWrapper stat)
	{
		logger.debug(log(path, stat));
		return filesystem.getattr(path, stat);
	}

	@Override
	protected String getName()
	{
		logger.debug(log());
		return filesystem.getName();
	}

	@Override
	protected String[] getOptions()
	{
		logger.debug(log());
		return filesystem.getOptions();
	}

	@Override
	public int getxattr(final String path, final String xattr, final ByteBuffer buf, final long size, final long position)
	{
		logger.debug(log(path, xattr, buf, size, position));
		return filesystem.getxattr(path, xattr, buf, size, position);
	}

	@Override
	public void init()
	{
		logger.debug(log());
		filesystem.init();
	}

	@Override
	public int link(final String path, final String target)
	{
		logger.debug(log(path, target));
		return filesystem.link(path, target);
	}

	@Override
	public int listxattr(final String path, final XattrListFiller filler)
	{
		logger.debug(log(path, filler));
		return filesystem.listxattr(path, filler);
	}

	@Override
	public int lock(final String path, final FileInfoWrapper info, final FlockCommand command, final FlockWrapper flock)
	{
		logger.debug(log(path, info, command, flock));
		return filesystem.lock(path, info, command, flock);
	}

	@Override
	public int mkdir(final String path, final ModeWrapper mode)
	{
		logger.debug(log(path, mode));
		return filesystem.mkdir(path, mode);
	}

	@Override
	public int mknod(final String path, final ModeWrapper mode, final long dev)
	{
		logger.debug(log(path, mode, dev));
		return filesystem.mknod(path, mode, dev);
	}

	@Override
	public int open(final String path, final FileInfoWrapper info)
	{
		logger.debug(log(path, info));
		return filesystem.open(path, info);
	}

	@Override
	public int opendir(final String path, final FileInfoWrapper info)
	{
		logger.debug(log(path, info));
		return filesystem.opendir(path, info);
	}

	@Override
	public int read(final String path, final ByteBuffer buffer, final long size, final long offset, final FileInfoWrapper info)
	{
		logger.debug(log(path, buffer, size, offset, info));
		return filesystem.read(path, buffer, size, offset, info);
	}

	@Override
	public int readdir(final String path, final DirectoryFiller filler)
	{
		logger.debug(log(path, filler));
		return filesystem.readdir(path, filler);
	}

	@Override
	public int readlink(final String path, final ByteBuffer buffer, final long size)
	{
		logger.debug(log(path, buffer, size));
		return filesystem.readlink(path, buffer, size);
	}

	@Override
	public int release(final String path, final FileInfoWrapper info)
	{
		logger.debug(log(path, info));
		return filesystem.release(path, info);
	}

	@Override
	public int releasedir(final String path, final FileInfoWrapper info)
	{
		logger.debug(log(path, info));
		return filesystem.releasedir(path, info);
	}

	@Override
	public int removexattr(final String path, final String xattr)
	{
		logger.debug(log(path, xattr));
		return filesystem.removexattr(path, xattr);
	}

	@Override
	public int rename(final String path, final String newName)
	{
		logger.debug(log(path, newName));
		return filesystem.rename(path, newName);
	}

	@Override
	public int rmdir(final String path)
	{
		logger.debug(log(path));
		return filesystem.rmdir(path);
	}

	@Override
	void setFinalMountPoint(final File mountPoint)
	{
		logger.debug(log(mountPoint));
		super.setFinalMountPoint(mountPoint);
		filesystem.setFinalMountPoint(mountPoint);
	}

	@Override
	public int setxattr(final String path, final ByteBuffer buf, final long size, final int flags, final long position)
	{
		logger.debug(log(path, buf, size, flags, position));
		return filesystem.setxattr(path, buf, size, flags, position);
	}

	@Override
	public int statfs(final String path, final StatvfsWrapper wrapper)
	{
		logger.debug(log(path, wrapper));
		return filesystem.statfs(path, wrapper);
	}

	@Override
	public int symlink(final String path, final String target)
	{
		logger.debug(log(path, target));
		return filesystem.symlink(path, target);
	}

	@Override
	public int truncate(final String path, final long offset)
	{
		logger.debug(log(path, offset));
		return filesystem.truncate(path, offset);
	}

	@Override
	public int unlink(final String path)
	{
		logger.debug(log(path));
		return filesystem.unlink(path);
	}

	@Override
	public int utimens(final String path, final TimeBufferWrapper wrapper)
	{
		logger.debug(log(path, wrapper));
		return filesystem.utimens(path, wrapper);
	}

	@Override
	public int write(final String path, final ByteBuffer buf, final long bufSize, final long writeOffset,
			final FileInfoWrapper wrapper)
	{
		logger.debug(log(path, buf, bufSize, writeOffset, wrapper));
		return filesystem.write(path, buf, bufSize, writeOffset, wrapper);
	}
}
