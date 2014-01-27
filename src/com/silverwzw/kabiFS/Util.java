package com.silverwzw.kabiFS;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.regex.Pattern;

/**
 * Helper Class
 * @author silverwzw
 *
 */
public final class Util {

	private static final Pattern commitNamePattern, branchNamePattern;
	
	static {
		commitNamePattern = Pattern.compile("^[0-9a-zA-Z_]+@[0-9]*$");
		branchNamePattern = Pattern.compile("^[0-9a-zA-Z_]+$");
	}
	/**
	 * Build a path, e.g. input [a,b,c] returns /a/b/c
	 * @param args directory names
	 * @return corresponding path
	 */
	public static final String buildPath(String ... args) {
		if (args.length == 0) {
			return File.separator;
		}
		String path = "";
		for (String dir : args) {
			path += File.separator + dir;
		}
		return path;
	}

	/**
	 * check if the input is a valid commit name
	 * @param inputName commit name to be tested
	 * @return true if valid, otherwise false
	 */
	public static final boolean commitNameCheck(String inputName) {
		return commitNamePattern.matcher(inputName).matches();
	}

	/**
	 * check if the input is a valid branch name
	 * @param inputName branch name to be tested
	 * @return true if valid, otherwise false
	 */
	public static final boolean branchNameCheck(String inputName) {
		return branchNamePattern.matcher(inputName).matches();
	}

	/**
	 * extract branch name from commit name, Note: assume valid commit name
	 * @param commitName the commit name
	 * @return the branch name
	 */
	public static final String getBranchNameByCommitName(String commitName) {
		return commitName.split("@")[0];
	}
	/**
	 * Compute substring asked to read
	 * @param contents the String content
	 * @param buffer read buffer
	 * @param size read size
	 * @param offset read offset
	 * @return length of chars read
	 */
	public static final int readString(String contents, final ByteBuffer buffer, final long size, final long offset) {
		final String s = contents.substring((int) offset,
				(int) Math.max(offset, Math.min(contents.length() - offset, offset + size)));
		buffer.put(s.getBytes());
		return s.getBytes().length;
	} 
}
