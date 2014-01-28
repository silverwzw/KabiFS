package com.silverwzw.kabiFS.util;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import com.silverwzw.JSON.JSON;
import com.silverwzw.JSON.JSON.JsonStringFormatException;
import com.silverwzw.kabiFS.util.Util.MongoConn;


/**
 * This Object carries all options passed to KabiFS
 * @author silverwzw
 */
public class MountOptions {
	/**
	 * if commit name is given:
	 * 	mount that commit;<br>
	 * if branch name is given:
	 * 	mount the latest commit in that branch;<br> 
	 * if nothing is given:
	 * 	mount MAIN branch;<br>
	 * @author silverwzw
	 */
	public enum MountMode {
		/**
		 * write will be discard after unmount
		 */
		SHADOW,
		/**
		 * write will append to that commit
		 */
		DIRECT,
		/**
		 * write will create a new commit:<br>
		 * if new branch name is given, this will create a new branch.<br>
		 * otherwise, will append a new commit in current branch.
		 */
		LOG;
		/**
		 * convert a String representation of mount mode to MountMode enum
		 * @param mode the String representation of mount mode
		 * @return corresponding MountMode instance
		 */
		public final static MountMode getMode(String mode){
			try {
				return valueOf(mode.toUpperCase());
			} catch (IllegalArgumentException ex) {
				return LOG;
			}
		}
	};

	@SuppressWarnings("serial")
	public static class ParseException extends Exception {};
	@SuppressWarnings("serial")
	public static class ParseNameException extends Exception {};
	
	public static final String usage, namePolicy;
	
	static {
		usage = "Usage: KabiFS <config_file> <mount_point>\n"
				+ "\tconfig : a json config file, use \'-\' to read from stdin.\n"
				+ "\tmount_point : the location to mount the filesystem\n\n"
				+ "\tmode(String): by default is LOG, other options are: SHADOW, DIRECT\n"
				+ "\tbase(String): name of base commit/branch, by default is \"MAIN\" branch\n"
				+ "\tnew (String): name of new branch\n"
				+ "\tmongo(Array): mongodb connection details\n"
				+ "\tfuse (Array): fuse options\n";
		namePolicy = "Note:\tBranch name should match regexp ^[0-9a-zA-Z_]+$\n"
				+ "\tCommit name shoud match regexp ^[0-9a-zA-Z_]+@[0-9]*$\n";
	}
	
	private String originJson;
	
	protected MountMode mountMode;
	protected String baseCommitName;
	protected String newBranchName;
	protected String[] fuseOptions;
	protected String mountPoint;
	protected Util.MongoConn mongoConn;
	
	// commit name 	= branch_name@unmount_time
	// or 			= branch_name@
	//
	// unexpected unmount will result in a commit name without unmount time,
	// fix the name when next time mounting this branch
	
	{
		mountMode = MountMode.LOG;
		baseCommitName = "MAIN@"; // latest commit in MAIN branch
		newBranchName = "";
		fuseOptions = null;
	}
	
	/**
	 * parse command line arguments to an Option object
	 * @param args command line arguments
	 * @throws JsonStringFormatException if the json file isn't in correct json format
	 * @throws IOException if I/O exception occurs
	 * @throws ParseNameException if parse error occurs due to naming policy 
	 */
	public MountOptions(String ... args) throws ParseException, ParseNameException, IOException, JsonStringFormatException {
		
		if (args.length != 2) {
			throw new ParseException();
		}
		
		mountPoint = args[1];
		
		JSON config;
		if (args[0].equals("-")) {
			config = JSON.parse(System.in);
		} else {
			config = JSON.parse(new File(args[0]));
		}
		
		originJson = config.format();

		if (config.get("mode") != null) {
			mountMode = MountMode.valueOf((String) config.get("mode").toObject());
		} else {
			mountMode = MountMode.LOG;
		}
		
		if (config.get("base") != null) {
			String base;
			base = (String) config.get("base").toObject();
			if (Util.branchNameCheck(base)) {
				base = base + "@";
			} else if (!Util.commitNameCheck(base)) {
				throw new ParseNameException();
			}
			baseCommitName = base;
		} else {
			baseCommitName = "MAIN@";
		}
		
		if (config.get("new") != null) {
			String newB;
			newB = (String) config.get("new").toObject();
			if (!Util.branchNameCheck(newB)) {
				throw new ParseNameException();
			}
			newBranchName = newB;
		} else {
			newBranchName = "";
		}
		
		if (config.get("fuse") != null) {
			fuseOptions = new String[config.get("fuse").size()];
			for (int i = 0; i < config.get("fuse").size(); i++) {
				fuseOptions[i] = (String) config.get("fuse").at(i).toObject();
			}
		} else {
			fuseOptions = null;
		}
		
		mongoConn = new MongoConn(config.get("mongo"));
	}
	/**
	 * @return mount mode
	 */
	public final MountMode mountMode() {
		return mountMode;
	}
	/**
	 * @return fuse options
	 */
	public final String[] fuseOptions() {
		return fuseOptions;
	}
	/**
	 * @return mount point
	 */
	public final String mountPoint(){
		return mountPoint;
	}
	/**
	 * @return <br>base commit name, like "branch@timestamp"
	 * <br> or "branch@", refer to latest commit
	 */
	public final String baseCommit() {
		return baseCommitName;
	}
	/**
	 * @return new branch name (if there's one specified, null otherwise)
	 */
	public final String newBranch() {
		return newBranchName.equals("") ? Util.getBranchNameByCommitName(baseCommit()): newBranchName;
	}
	/**
	 * @return String representation of this instance, good for debug
	 */
	public String toString() {
		return "Options:{"
				+ " mntPoint:" + mountPoint()
				+ " mode:" + mountMode().toString()
				+ " base:" + baseCommit()
				+ " branch:" + newBranch()
				+ " fuse: " + Arrays.toString(fuseOptions())
				+ "}";
	}
	/**
	 * to meta file format.
	 * @return corresponding meta file content.
	 */
	public String toMetaFile() {
		return "Mount on: " + new File(mountPoint).getAbsolutePath() + "\n\nOptions:" + originJson + "\n";
	}
}
