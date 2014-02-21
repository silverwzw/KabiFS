package com.silverwzw.kabiFS.util;

public class Tuple2 <C1, C2> {
	public C1 item1;
	public C2 item2;
	{
		item1 = null;
		item2 = null;
	}
	
	public Tuple2() {}
	
	public Tuple2(C1 c1, C2 c2) {
		item1 = c1;
		item2 = c2;
	}
	
	public boolean equals(Object o) {
		if (o == null || !(o instanceof Tuple2<?, ?>)) {
			return false;
		}
		
		Tuple2<?, ?> tpo;
		tpo = (Tuple2<?, ?>) o;
		
		if ((item1 != null && tpo.item1 == null) ||
				(item2 != null && tpo.item2 == null) ||
				(item1 == null && tpo.item1 != null) ||
				(item2 == null && tpo.item2 != null)) {
			return false;
		}
		
		return item1.equals(tpo.item1) && item2.equals(tpo.item2);
	}
	public int hashCode() {
		return (item1 == null ? 0 : item1.hashCode())
				^
				(item2 == null ? 0 : item2.hashCode());
	}
}
