package com.silverwzw.kabiFS.util;

import java.util.Iterator;

public abstract class WriteBuffer implements Comparable<WriteBuffer>, Iterable<Byte> {
	
	public abstract long startByteIndex();
	public abstract long endByteIndex();
	public abstract byte get(int i);
	
	public final byte getByByteIndex(long byteIndex) {
		return get((int)(byteIndex - startByteIndex()));
	}

	public final int compareTo(WriteBuffer o) {
		return (int) (startByteIndex() - o.startByteIndex());
	}
	
	public int size() {
		return (int) (endByteIndex() - startByteIndex()) + 1;
	}
	
	public final Iterator<Byte> iterator() {
		return new Iterator<Byte>() {
			private int i;
			{
				i = 0;
			}
			public final boolean hasNext() {
				return i < endByteIndex() - startByteIndex() + 1;
			}
			public final Byte next() {
				return get(i++);
			}
			public void remove() {
				throw new 	UnsupportedOperationException();
			}
		};
	}
	
	
	public final static boolean mergable(WriteBuffer wb1, WriteBuffer wb2) {
		return (wb2.startByteIndex() >= wb1.startByteIndex() && wb2.startByteIndex() <= wb1.endByteIndex())
				||
				(wb2.endByteIndex() <= wb1.endByteIndex() && wb2.endByteIndex() >= wb1.startByteIndex());
	}
	

	public final static WriteBuffer create(final byte[] bytes, final long offset, final long size) {
		return new WriteBuffer() {
			public final long startByteIndex() {
				return offset;
			}
			public final long endByteIndex() {
				return offset + size - 1;
			}
			public final byte get(int i) {
				return bytes[i];
			}
			public final int size() {
				return (int) size;
			}
		};
	}
	
	public final static WriteBuffer merge(final WriteBuffer exist, final WriteBuffer adding){
		if (adding.startByteIndex() <= exist.startByteIndex() && exist.endByteIndex() <= adding.endByteIndex()) {
			return adding;
		}
		return new WriteBuffer() {
				
				final private long offset, end;
				
				{
					offset = exist.startByteIndex() > adding.startByteIndex() ? adding.startByteIndex() : exist.startByteIndex();
					end = exist.endByteIndex() < adding.endByteIndex() ? adding.endByteIndex() : exist.endByteIndex();
				}
				
				public final long startByteIndex() {
					return offset;
				}
				
				public final long endByteIndex() {
					return end;
				}
				
				public final byte get(int i) {
					long indexOfByte;
					indexOfByte = i + offset;
					if (indexOfByte <= adding.endByteIndex() && indexOfByte >= adding.startByteIndex()) {
						return adding.getByByteIndex(indexOfByte);
					} else {
						return exist.getByByteIndex(indexOfByte);
					}
				}
		};
	}
}