package com.silverwzw.kabiFS;

import java.util.Arrays;

import com.silverwzw.kabiFS.FuseTemp.Fuse;
import com.silverwzw.kabiFS.FuseTemp.StructFuseOperations;
import com.sun.jna.Callback;
import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.Platform;
import com.sun.jna.Pointer;
import com.sun.jna.Structure;

import org.apache.log4j.Logger; 

public final class KabiFS {

    public static interface FuseLib extends Library {
        int fuse_main_real(int argc, String[] argv, StructFuseOperations op, long size, Pointer user_data);
    }
    
	private static final Logger logger;
    private static final FuseLib fuseLib;

	static {
		logger =  Logger.getLogger(KabiFS.class);
		logger.debug("JNA is Loading FUSE Library");
		fuseLib = (FuseLib) Native.loadLibrary("fuse", Fuse.class);
	}
	
	public static void main(final String args[]){
		String[] fuseArg;
		fuseArg = args;
		logger.debug("calling fuse.main, args = " + Arrays.toString(fuseArg));
		
	}
}
