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
import net.fusejna.FuseJna;
import net.fusejna.StructFuseFileInfo.FileInfoWrapper;
import net.fusejna.StructStat.StatWrapper;
import net.fusejna.types.TypeMode.NodeType;
import net.fusejna.util.FuseFilesystemAdapterFull;

public class KabiFS extends FuseFilesystemAdapterFull
{
	private static final Logger logger;
	
	static {
		logger = Logger.getLogger(KabiFS.class);
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

	protected Options options;
	private final String filename = "/hello.txt";
	private final String contents = "Hello World!\n";
	
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
		if (path.equals(File.separator)) { // Root directory
			stat.setMode(NodeType.DIRECTORY);
			return 0;
		}
		if (path.equals(filename)) { // hello.txt
			stat.setMode(NodeType.FILE).size(contents.length());
			return 0;
		}
		return -ErrorCodes.ENOENT();
	}

	@Override
	public int read(final String path, final ByteBuffer buffer, final long size, final long offset, final FileInfoWrapper info)
	{
		// Compute substring that we are being asked to read
		final String s = contents.substring((int) offset,
				(int) Math.max(offset, Math.min(contents.length() - offset, offset + size)));
		buffer.put(s.getBytes());
		return s.getBytes().length;
	}

	@Override
	public int readdir(final String path, final DirectoryFiller filler)
	{
		filler.add(filename);
		return 0;
	}
}
