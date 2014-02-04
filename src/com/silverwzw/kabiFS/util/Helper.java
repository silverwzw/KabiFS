package com.silverwzw.kabiFS.util;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.regex.Pattern;

import net.fusejna.StructStat.StatWrapper;
import net.fusejna.types.TypeMode.NodeType;

import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

import com.mongodb.DBObject;
import com.silverwzw.kabiFS.structure.Commit.KabiFileNode;
import com.silverwzw.kabiFS.structure.Commit.KabiNoneDataNode;
import com.silverwzw.kabiFS.structure.Node.KabiNodeType;


/**
 * Helper Class
 * @author silverwzw
 *
 */
public final class Helper {

	private static final Pattern commitNamePattern, branchNamePattern;
	@SuppressWarnings("unused")
	private static final Logger logger;
	
	static {
		commitNamePattern = Pattern.compile("^[0-9a-zA-Z_]+@[0-9]*$");
		branchNamePattern = Pattern.compile("^[0-9a-zA-Z_]+$");
		logger = Logger.getLogger(Helper.class);
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
	@SuppressWarnings("serial")
	public static final class ObjectNotFoundException extends Exception {};
	/**
	 * quick function to find dbobject in bson
	 * @param dbo the DBObject to search
	 * @param clazz the class of the final object
	 * @param field_names a set of field name in order
	 * @return corresponding Object (not necessary DBObject)
	 * @throws ObjectNotFoundException if any one of the field is not found in the path<br>or the final object cannot be cast to specific class
	 */
	@SuppressWarnings("unchecked")
	public static final <T> T getObject(final DBObject dbo, Class<T> clazz, final String ... field_names) throws ObjectNotFoundException {
		Object o = dbo;
		for (int i = 0; i < field_names.length; i++) {
			String field_name;
			field_name = field_names[i];
			if (o instanceof DBObject && ((DBObject) o).containsField(field_name)) {
				o = ((DBObject) o).get(field_name);
			} else {
				throw new ObjectNotFoundException();
			}
		}
		try {
			return (T)o;
		} catch (java.lang.ClassCastException ex){
			throw new ObjectNotFoundException();
		}
	}
	/**
	 * transform CommitList to meta file String
	 */
	public final static String commitList2MetaFile(Collection<Tuple3<ObjectId, String, ObjectId>> list) {
		String content;
		content = "#ObjectId\tName\tbase\n";
		for (Tuple3<ObjectId, String, ObjectId> entry : list) {
			content += entry.item1.toString() + '\t' + entry.item2 + '\t' + (entry.item3 == null ? "null" : entry.item3.toString()) + '\n';
		}
		return content;
	}
	
	public final static void setMode(final StatWrapper stat, KabiNoneDataNode node) {
		int mode;
		mode = node.mode();
		
		stat.setMode(
				(node.type() == KabiNodeType.DIRECTORY ? NodeType.DIRECTORY : NodeType.FILE),
				(mode & 0400) != 0,
				(mode & 0200) != 0,
				(mode & 0100) != 0,
				(mode & 040) != 0, 
				(mode & 020) != 0,
				(mode & 010) != 0,
				(mode & 04) != 0, 
				(mode & 02) != 0,
				(mode & 01) != 0
			);
		
		stat.gid(node.gid());
		stat.uid(node.uid());
		
		if (node.type() == KabiNodeType.FILE) {
			stat.size(((KabiFileNode) node).size());
		}
	}
}