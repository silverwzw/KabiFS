package com.silverwzw.kabiFS;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;

import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

import com.silverwzw.JSON.JSON.JsonStringFormatException;
import com.silverwzw.kabiFS.structure.KabiCommit;
import com.silverwzw.kabiFS.structure.KabiCommit.NodeId;
import com.silverwzw.kabiFS.structure.Node;
import com.silverwzw.kabiFS.util.MountOptions;
import com.silverwzw.kabiFS.util.Util;
import com.silverwzw.kabiFS.util.MountOptions.ParseException;
import com.silverwzw.kabiFS.util.MountOptions.ParseNameException;

import net.fusejna.DirectoryFiller;
import net.fusejna.ErrorCodes;
import net.fusejna.FuseException;
import net.fusejna.StructFuseFileInfo.FileInfoWrapper;
import net.fusejna.StructStat.StatWrapper;
import net.fusejna.types.TypeMode.NodeType;
import net.fusejna.util.FuseFilesystemAdapterFull;

public class KabiFS extends FuseFilesystemAdapterFull {
	private static final Logger logger;
	private static final String metaDirName;
	
	public static interface DatastoreAdapter {
		/**
		 * get the Commit list
		 * @return collection of tuples, 1st item is ObjectId of commit, 2nd is name, 3rd is the ObjectId of base commit.
		 */
		public Collection<Util.Tuple<ObjectId, String, ObjectId>> getCommitList();
		public KabiCommit getCommit();
		public KabiCommit getCommit(String branch);
		public KabiCommit getCommit(String branch, long timestamp);
		public KabiCommit getCommit(ObjectId oid);
		public Node getNode(NodeId nid);
	}
	
	static {
		logger = Logger.getLogger(KabiFS.class);
		metaDirName = ".kabimeta";
	}
	
	public static void main(final String... args) throws FuseException
	{
		KabiFS kabifs;
		MountOptions options = null;
		
		logger.info("parse command line args: " + Arrays.toString(args));
		try {
			options = new MountOptions(args);
		} catch (ParseException ex) {
			System.err.print(MountOptions.usage);
			logger.fatal("Parse Exception", ex);
			return;
		} catch (ParseNameException ex) {
			System.err.print(MountOptions.namePolicy);
			logger.fatal("Parse Name Policy Exception");
			return;
		} catch (JsonStringFormatException ex) {
			System.err.print("config json isn't in correct format, only standrad JSON string is accepted.\n");
			logger.fatal("JSON parse Exception", ex);
			return;
		} catch (IOException ex) {
			System.err.print("IOException when parsing.\n");
			logger.fatal("I/O Exception when parsing, ", ex);
			return;
		}
		logger.info("parsed args: " + options);
		
		kabifs = new KabiFS(options);
		kabifs.log(Logger.getLogger("fuse"));
		kabifs.mount();
	}

	protected final MountOptions mntoptions;
	protected final DatastoreAdapter db;
	
	public KabiFS(MountOptions options) {
		this.mntoptions = options; 
		db = new KabiDBAdapter(options.mongoConn());
	}

	/**
	 * Overrides getOptions() in super class, returns FUSE options
	 * @return FUSE Options, in String[], start with the 1st option
	 */
	protected String[] getOptions() {
		return mntoptions.fuseOptions();
	}
	
	public void mount() throws FuseException {
		mount(mntoptions.mountPoint());
	}
	
	@Override
	public int getattr(final String path, final StatWrapper stat)
	{
		if (path.equals(Util.buildPath())) { // Root directory
			stat.setMode(NodeType.DIRECTORY);
			return 0;
		}
		if (path.equals(Util.buildPath(metaDirName))) {
			Util.statMetaMode(stat, NodeType.DIRECTORY);
			return 0;
		}
		if (path.equals(Util.buildPath(metaDirName, "mount_options"))) {
			Util.statMetaMode(stat, NodeType.FILE);
			stat.size(mntoptions.toMetaFile().length());
			return 0;
		}
		if (path.equals(Util.buildPath(metaDirName, "fs_parameters"))) {
			Util.statMetaMode(stat, NodeType.FILE);
			return 0;
		}
		if (path.equals(Util.buildPath(metaDirName, "commits"))) {
			Util.statMetaMode(stat, NodeType.FILE);
			stat.size(Util.commitList2MetaFile(db.getCommitList()).length());
			return 0;
		}
		if (path.equals(Util.buildPath(metaDirName, "kabi_console_out"))) {
			Util.statMetaMode(stat, NodeType.FILE);
			return 0;
		}
		if (path.equals(Util.buildPath(metaDirName, "kabi_console_in"))) {
			stat.setMode(NodeType.FILE, false, true, false, false, false, false, false, false, false);
			return 0;
		}
		return -ErrorCodes.ENOENT();
	}

	@Override
	public int read(final String path, final ByteBuffer buffer, final long size, final long offset, final FileInfoWrapper info)
	{
		if (path.equals(Util.buildPath(metaDirName, "mount_options"))) {
			return Util.readString(mntoptions.toMetaFile(), buffer, size, offset);
		}
		if (path.equals(Util.buildPath(metaDirName, "commits"))) {
			return Util.readString(Util.commitList2MetaFile(db.getCommitList()), buffer, size, offset);
		}
		return Util.readString("", buffer, size, offset);
	}

	@Override
	public int readdir(final String path, final DirectoryFiller filler)
	{
		if (path.equals(Util.buildPath())) {
			filler.add(metaDirName);
		}
		if (path.equals(Util.buildPath(metaDirName))) {
			filler.add("commits");
			filler.add("fs_parameters");
			filler.add("mount_options");
			filler.add("kabi_console_in");
			filler.add("kabi_console_out");
		}
		return 0;
	}
}
