package com.silverwzw.kabiFS;

import java.io.IOException;
import java.util.Arrays;

import net.fusejna.FuseException;

import org.apache.log4j.Logger;

import com.silverwzw.JSON.JSON.JsonStringFormatException;
import com.silverwzw.kabiFS.util.MountOptions;
import com.silverwzw.kabiFS.util.MountOptions.ParseException;
import com.silverwzw.kabiFS.util.MountOptions.ParseNameException;

public class Mount {
	
	private static final Logger logger;
	
	static {
		logger = Logger.getLogger(Mount.class);
	}
	
	/**
	 * entrance function
	 */
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
		kabifs.setLog(null);
		//kabifs.setLog(Logger.getLogger("fuse"));
		kabifs.mount(options.mountPoint());
	}
}
