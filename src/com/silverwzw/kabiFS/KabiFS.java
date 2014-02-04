package com.silverwzw.kabiFS;

import java.nio.ByteBuffer;

import org.apache.log4j.Logger;

import com.silverwzw.kabiFS.util.MountOptions;
import com.silverwzw.kabiFS.util.Helper;

import net.fusejna.DirectoryFiller;
import net.fusejna.ErrorCodes;
import net.fusejna.StructFuseFileInfo.FileInfoWrapper;
import net.fusejna.StructStat.StatWrapper;
import net.fusejna.types.TypeMode.NodeType;

public class KabiFS extends MetaFS {
	private static final Logger logger;

	static {
		logger = Logger.getLogger(KabiFS.class);
	}
	
	public KabiFS(MountOptions options) {
		super(options);
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
