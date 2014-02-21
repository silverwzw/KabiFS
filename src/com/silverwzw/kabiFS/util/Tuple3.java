package com.silverwzw.kabiFS.util;

public class Tuple3<C1, C2, C3> extends Tuple2<C1, C2> {
	public C3 item3;
	{
		item3 = null;
	}
	
	public Tuple3() {}
	public Tuple3(C1 c1, C2 c2, C3 c3) {
		super(c1,c2);
		item3 = c3;
	}
	
	public int hashCode() {
		return super.hashCode() ^ (item3 == null ? 0 : item3.hashCode());
	}
	public boolean equals(Object o) {
		if (o == null || !(o instanceof Tuple3<?, ?, ?>)) {
			return false;
		}
		
		Tuple3<?, ?, ?> tpo;
		tpo = (Tuple3<?, ?, ?>) o;
		
		if ((item1 != null && tpo.item1 == null) ||
				(item2 != null && tpo.item2 == null) ||
				(item3 != null && tpo.item3 == null) ||
				(item1 == null && tpo.item1 != null) ||
				(item2 == null && tpo.item2 != null) ||
				(item3 == null && tpo.item3 != null)) {
			return false;
		}
		
		return item1.equals(tpo.item1) && item2.equals(tpo.item2) && item3.equals(tpo.item3);
	}
}