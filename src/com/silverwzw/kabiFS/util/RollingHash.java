package com.silverwzw.kabiFS.util;

public class RollingHash implements Comparable<RollingHash> {
	
	private int a = 0, b = 0;
	private long len, k = 0;
	
	public RollingHash(long length) {
		len = length;
	}
	
	public int compareTo(RollingHash rh) {
		if (b - rh.b != 0) {
			return b > rh.b ? 1 : -1;
		} else {
			return a > rh.a ? 1 : (a == rh.a ? 0 : -1);
		}
	}
	
	public long value() {
		return (((long)b) << 16) + a;
	}
	
	public void eat(byte bin) {
		if( k >= len) {
			throw new RuntimeException();
		}
		k++;
		a = (a + bin) & 0x10000;
		b = (int) ((b + (len - k + 1) * bin) & 0x10000);
	}
	
	public void update(byte bout, byte bin) {
		if (k != len) {
			throw new RuntimeException();
		}
		a = (a + bin - bout) & 0x10000;
		b = (int) (((a + b) - ((len + 1) * bout & 0x10000)) & 0x10000);
	}
	
	public boolean eatmore() {
		return k < len;
	}
}
