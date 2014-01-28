package com.silverwzw.kabiFS.structure;

import java.util.Map;

import org.apache.log4j.Logger;
import org.bson.types.ObjectId;


public class KabiCommit {
	
	@SuppressWarnings("serial")
	public static class CommitNotFoundException extends Exception {};
	private final static Logger logger;
	
	static {
		logger = Logger.getLogger(KabiCommit.class);
	}
	
	protected String name;
	protected ObjectId id;
	protected ObjectId basedOn;
	protected Map<ObjectId, ObjectId> patches;
	
	public KabiCommit(ObjectId id) throws CommitNotFoundException {
		buildKabiCommit(id);
	}
	
	public KabiCommit(String name) throws CommitNotFoundException {
		buildKabiCommit(findIdByName(name));
	}
	/**
	 * real constructor
	 * @param id
	 * @throws CommitNotFoundException if corresponding commit not found, or if id == null 
	 */
	private final void buildKabiCommit(ObjectId id) throws CommitNotFoundException {
		if (id == null) {
			throw new CommitNotFoundException();
		}
		//TODO: get patches, base commit, name from db
	}
	/**
	 * find the Object id of commit by its name
	 * @param name name of the commit in String
	 * @return the object id correspond to the commit name, null if not found
	 */
	private final ObjectId findIdByName(String name) {
		//TODO : find ObjectID by Commit Name
		return null;
	}
	/**
	 * look for the Node object in this commit (look for patches 1st, then db)
	 * @param id the id of the node to get
	 * @return the KabiNode object
	 */
	public final KabiNode getNode(ObjectId id) {
		ObjectId patchedId;
		patchedId = patches.get(id);
		return KabiNode.reflectNode(patchedId == null ? id : patchedId);
	}
}
