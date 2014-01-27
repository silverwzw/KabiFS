package com.silverwzw.kabiFS;

import java.util.Arrays;

/**
 * This Object carries all options passed to KabiFS
 * @author silverwzw
 */
public class Options {
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
	public static class ParseNameException extends ParseException {};
	
	public static final String usage, namePolicy;
	
	static {
		usage = "Usage: KabiFS [mode [base [new]]] [--fuse <fuse_options>] mount_point\n"
				+ "\tmode: by default is LOG, other options are: SHADOW, DIRECT\n"
				+ "\tbase: name of base commit/branch, by default is \"MAIN\" branch\n"
				+ "\tnew: name of new branch\n";
		namePolicy = "Note:\tBranch name should match regexp ^[0-9a-zA-Z_]+$\n"
				+ "\tCommit name shoud match regexp ^[0-9a-zA-Z_]+@[0-9]*$\n";
	}
	
	protected MountMode mountMode;
	protected String baseCommitName;
	protected String newBranchName;
	protected String[] fuseOptions;
	protected String mountPoint;
	
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
	 * @throws ParseException if parse error occurs
	 * @throws ParseNameException if parse error occurs due to naming policy 
	 */
	public Options(String ... args) throws ParseException {
		int len;
		len = args.length;
		
		if (len == 0) {
			throw new ParseException();
		}
		
		mountPoint = args[len - 1];
		
		int i;
		for (i = 0; i < len - 1; i++) {
			if (args[i].equals("--fuse")) {
				break;
			}
		}
		
		if (i != len - 1){
			fuseOptions = new String[len - 2 - i];
			for (int j = 0; j < fuseOptions.length; j++) {
				fuseOptions[j] = args[i + j + 1];
			}
		}
		
		if (i >= 1) {
			mountMode = MountMode.getMode(args[0]);
		}
		
		if (i >= 2) {
			if (Util.branchNameCheck(args[1])) {
				baseCommitName = args[1] + "@";
			} else if (Util.commitNameCheck(args[1])) {
				baseCommitName = args[1];
			} else {
				throw new ParseNameException();
			}
		}
		
		if (i == 3) {
			if (Util.branchNameCheck(args[2])) {
				newBranchName = args[2];
			} else {
				throw new ParseNameException();
			}
		}
		
		if (i > 3) {
			throw new ParseException();
		}
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
	
	public String toFile() {
		return 		"mount point\t= " + mountPoint() + "\n"
				+ 	"mount mode\t= " + mountMode().toString() + "\n"
				+ 	"base commit\t= " + baseCommit() + "\n"
				+	"on branch\t= " + newBranch() + "\n"
				+ 	"fuse option\t= " + Arrays.toString(fuseOptions()) + "\n";
	}
}