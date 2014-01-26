package com.silverwzw.kabiFS;

import java.util.Arrays;

import net.fusejna.examples.HelloFS;
import net.fusejna.util.FuseFilesystemAdapterFull;

import org.apache.log4j.Logger; 

public final class KabiFS extends HelloFS {
    
	private static final Logger logger;

	static {
		logger =  Logger.getLogger(KabiFS.class);
	}
	// -oallow_user
}
