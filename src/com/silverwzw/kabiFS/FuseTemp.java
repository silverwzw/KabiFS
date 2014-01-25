package com.silverwzw.kabiFS;

import java.util.List;

import com.sun.jna.Callback;
import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.Pointer;
import com.sun.jna.Structure;

public class FuseTemp
{
    public static interface Fuse extends Library
    {
        int fuse_main_real(int argc, String[] argv, StructFuseOperations op, long size, Pointer user_data);
    }

    @SuppressWarnings("unused")
    public static class StructFuseOperations extends Structure
    {
        public static class ByReference extends StructFuseOperations implements Structure.ByReference
        {
        }

        public Callback getattr = new Callback()
        {
            public int callback(final String path, final Pointer stat)
            {
                System.out.println("getattr was called");
                return 0;
            }
        };
        public Callback readlink = null;
        public Callback mknod = null;
        public Callback mkdir = null;
        public Callback unlink = null;
        public Callback rmdir = null;
        public Callback symlink = null;
        public Callback rename = null;
        public Callback link = null;
        public Callback chmod = null;
        public Callback chown = null;
        public Callback truncate = null;
        public Callback utime = null;
        public Callback open = new Callback()
        {
            public int callback(final String path, final Pointer info)
            {
                System.out.println("open was called");
                return 0;
            }
        };
        public Callback read = new Callback()
        {
            public int callback(final String path, final Pointer buffer, final long size, final long offset, final Pointer fi)
            {
                System.out.println("read was called");
                return 0;
            }
        };
        public Callback write = null;
        public Callback statfs = null;
        public Callback flush = null;
        public Callback release = null;
        public Callback fsync = null;
        public Callback setxattr = null;
        public Callback getxattr = null;
        public Callback listxattr = null;
        public Callback removexattr = null;
        public Callback opendir = null;
        public Callback readdir = new Callback()
        {
            public int callback(final String path, final Pointer buffer, final Pointer filler, final long offset,
                    final Pointer fi)
            {
                System.out.println("readdir was called");
                return 0;
            }
        };
        public Callback releasedir = null;
        public Callback fsyncdir = null;
        public Callback init = null;
        public Callback destroy = null;
        public Callback access = null;
        public Callback create = null;
        public Callback ftruncate = null;
        public Callback fgetattr = null;
        public Callback lock = null;
        public Callback utimens = null;
        public Callback bmap = null;
        public int flag_nullpath_ok;
        public int flag_reserved;
        public Callback ioctl = null;
        public Callback poll = null;
		@Override
		protected List getFieldOrder() {
			// TODO Auto-generated method stub
			return null;
		}
    }

    public static void main(final String[] args)
    {
        final String[] actualArgs = { "-f", "/some/mount/point" };
        final Fuse fuse = (Fuse) Native.loadLibrary("fuse", Fuse.class);
        final StructFuseOperations.ByReference operations = new StructFuseOperations.ByReference();
        System.out.println("Mounting");
        final int result = fuse.fuse_main_real(actualArgs.length, actualArgs, operations, operations.size(), null);
        System.out.println("Result: " + result);
        System.out.println("Mounted");
    }
}