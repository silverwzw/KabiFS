package com.silverwzw.kabiFS;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.log4j.Logger;

import com.silverwzw.kabiFS.Options.ParseException;
import com.silverwzw.kabiFS.Options.ParseNameException;

import net.fusejna.DirectoryFiller;
import net.fusejna.ErrorCodes;
import net.fusejna.FuseException;
import net.fusejna.StructFuseFileInfo.FileInfoWrapper;
import net.fusejna.StructStat.StatWrapper;
import net.fusejna.types.TypeMode.NodeType;
import net.fusejna.util.FuseFilesystemAdapterFull;

public class KabiFS extends FuseFilesystemAdapterFull
{
	private static final Logger logger;
	private static final String metaDirName;
	
	static {
		logger = Logger.getLogger(KabiFS.class);
		metaDirName = ".kabimeta";
	}
	
	public static void main(final String... args) throws FuseException
	{
		KabiFS kabifs;
		Options options = null;
		
		logger.info("parse command line args: " + Arrays.toString(args));
		try {
			options = new Options(args);
		} catch (ParseException ex) {
			System.err.print(Options.usage);
			if (ex instanceof ParseNameException) {
				System.err.print(Options.namePolicy);
			}
			logger.fatal("parse exception", ex);
			return;
		} 
		logger.info("parsed args: " + options);
		
		kabifs = new KabiFS(options);
		kabifs.log(Logger.getLogger("fuse"));
		kabifs.mount();
	}

	protected final Options options;
	
	public KabiFS(Options options) {
		this.options = options; 
	}

	protected String[] getOptions() {
		return options.fuseOptions;
	}
	
	public void mount() throws FuseException {
		mount(options.mountPoint);
	}
	
	@Override
	public int getattr(final String path, final StatWrapper stat)
	{
		if (path.equals(Util.buildPath())) { // Root directory
			stat.setMode(NodeType.DIRECTORY);
			return 0;
		}
		if (path.equals(Util.buildPath(metaDirName))) {
			stat.setMode(NodeType.DIRECTORY,
					true, false, true,
					true, false, true,
					false, false, false);
			return 0;
		}
		if (path.equals(Util.buildPath(metaDirName, "mount_options.txt"))) {
			stat.setMode(NodeType.FILE,
					true, false, false,
					true, false, false,
					false, false, false);
			stat.size(options.toFile().length());
			return 0;
		}
		if (path.equals(Util.buildPath(metaDirName, "fs_parameters.txt"))) {
			stat.setMode(NodeType.FILE,
					true, false, false,
					true, false, false,
					false, false, false);
			stat.size("".length());
			return 0;
		}
		if (path.equals(Util.buildPath(metaDirName, "commits.txt"))) {
			stat.setMode(NodeType.FILE,
					true, false, false,
					true, false, false,
					false, false, false);
			stat.size("".length());
			return 0;
		}
		return -ErrorCodes.ENOENT();
	}

	@Override
	public int read(final String path, final ByteBuffer buffer, final long size, final long offset, final FileInfoWrapper info)
	{
		if (path.equals(Util.buildPath(metaDirName, "mount_options.txt"))) {
			return Util.readString(options.toFile(), buffer, size, offset);
		}
		return Util.readString("KabiFS default string content\n", buffer, size, offset);
	}

	@Override
	public int readdir(final String path, final DirectoryFiller filler)
	{
		if (path.equals(Util.buildPath())) {
			filler.add(metaDirName);
		}
		if (path.equals(Util.buildPath(metaDirName))) {
			filler.add("commits.txt");
			filler.add("fs_parameters.txt");
			filler.add("mount_options.txt");
		}
		return 0;
	}
}
