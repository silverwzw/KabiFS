package com.silverwzw.JSON;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.Serializable;
import java.io.StringReader;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

/**
 * the JSON object, compiles an Java Object to JSON Object, parse JSON String to JSON object or reverse.
 * @author Silverwzw
 */
@SuppressWarnings("serial")
public abstract class JSON implements Cloneable,Serializable,Iterable<Entry<String,JSON>> {

	protected Object data;
	protected JSON() {}
	
	public final static class OperationNotDefinedException extends RuntimeException {
		OperationNotDefinedException(String s) {super(s);};
	}

	public final static class IndentStringException extends RuntimeException {}
	public final static class JsonStringFormatException extends Exception {
		JsonStringFormatException(LexicalException e) {super(e);}
		JsonStringFormatException(ParsingException e) {super(e);}
	}
	public final static class LexicalException extends Exception {}
	public final static class ParsingException extends Exception {}
	/**
	 * the version of the JSON class
	 * @return
	 * the version of the JSON class as String
	 */
	public static String version() {
		return "0.2";
	}
	/**
	 * read a ASCII file contains one standard JSON string, objects in json will be parsed as HashMap<String,JSON>
	 * @param file
	 * the JSON file
	 * @return
	 * the JSON object represented by the given JSON string
	 * @throws JsonStringFormatException
	 * @throws IOException 
	 */
	public final static JSON parse(File file) throws JsonStringFormatException, IOException {
		FileReader fr = null;
				
		fr = new FileReader(file);
		return parse(fr);
	}
	/**
	 * read a a standard JSON string from Reader r, objects in json will be parsed as HashMap<String,JSON>
	 * @param r
	 * the Reader
	 * @return
	 * the JSON object represented by the given JSON string
	 * @throws JsonStringFormatException
	 * @throws IOException 
	 */
	public final static JSON parse(Reader r) throws JsonStringFormatException, IOException {
		ArrayList<JsonToken> tokens;
		JSON root;
		
		try {
			tokens = JsonToken.getTokenStream(r);
			root = parseTokenStream(tokens,0,tokens.size());
		} catch (LexicalException e) {
			throw new JsonStringFormatException(e);
		} catch (ParsingException e) {
			throw new JsonStringFormatException(e);
		}
		return root; 
	}
	/**
	 * read a a standard JSON string from InputStream is, objects in json will be parsed as HashMap<String,JSON>
	 * @param is
	 * the InputStream
	 * @return
	 * the JSON object represented by the given JSON string
	 * @throws JsonStringFormatException
	 * @throws IOException 
	 */
	public final static JSON parse(InputStream is) throws JsonStringFormatException, IOException {
		return parse(new InputStreamReader(is)); 
	}
	/**
	 * parse a standard JSON string to a JSON object, objects in json will be parsed as HashMap<String,JSON>
	 * @param json_str
	 * the JSON string to be parse
	 * @return
	 * the JSON object represented by the given JSON string
	 * @throws JsonStringFormatException
	 */
	public final static JSON parse(String json_str) throws JsonStringFormatException{
		try {
			return parse(new StringReader(json_str));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	/**
	 * compile a Java object to JSON object, explicitly specify the type of the Java object<br /><br />
	 * * usually used when compiling an Java object as an instance of its ancestor class.
	 * @param obj
	 * the Object to be compile
	 * @param objClass
	 * the class explicitly specified
	 * @return
	 * corresponding JSON object
	 */
	public final static JSON compileAs(Object obj, Class<?> objClass){
		if (obj == null) {
			return new JsonNull();
		}
		if (!objClass.isInstance(obj)) {
			throw new ClassCastException();
		}
		if (objClass.isInstance(JSON.class)) {
			JSON json = null;
			try {
				json = (JSON) objClass.newInstance();
			} catch (InstantiationException e) {
				assert false : "InstantiationException when compile a JSON Object to JSON";
			} catch (IllegalAccessException e) {
				assert false : "IllegalAccessException when compile a JSON Object to JSON";
			}
			json.data = ((JSON)obj).data;
			return json;
		} else if (objClass.isArray() || implementsInterface(objClass,List.class)){
			return new JsonArray(obj);
		} else if (implementsInterface(objClass, Map.class)) {
			return new JsonMap((Map<?,?>) obj);
		} else if (objClass == Integer.class || objClass == Short.class || objClass == Long.class || objClass == Double.class || objClass == Float.class) {
			return new JsonNumber(obj);
		} else if (objClass == Boolean.class) {
			return new JsonBoolean((Boolean)obj);
		} else if (objClass == String.class) {
			return new JsonString((String)obj);
		}
		Field[] fields = objClass.getFields();
		HashMap<String, JSON> hm = new HashMap<String, JSON>();
		for (Field field : fields) {
			try {
				hm.put(field.getName(),JSON.auto(field.get(obj)));
				if(field.getName().equals("list")) {
					System.out.println(field.get(obj).getClass());
					System.out.println(implementsInterface(field.get(obj).getClass(),List.class));
					System.out.println(implementsInterface(ArrayList.class,List.class));
				}
			} catch (IllegalArgumentException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				hm.put(field.getName(),new JsonNull());
			}
		}
		JsonMap jsmp = new JsonMap();
		jsmp.data = hm;
		return jsmp;
	}
	/**
	 * compile an Java object to JSON object, automatically determine the type of that Java object<br /><br />
	 * Equivalent to JSON.compileAs(obj, obj.getClass());
	 * @param obj
	 * the object to be compiled
	 * @return
	 * corresponding JSON object
	 */
	public final static JSON auto(Object obj) {
		if (obj == null) {
			return new JsonNull();
		}
		return compileAs(obj, obj.getClass());
	}
	static String ReflectString(String d) {
		int index;
		d = d
			.replaceAll("\\\\", "\\\\\\\\")
			.replaceAll("\n", "\\\\n")
			.replaceAll("\t","\\\\t")
			.replaceAll("/","\\\\/")
			.replaceAll("\b","\\\\b")
			.replaceAll("\f", "\\\\f")
			.replaceAll("\r", "\\\\r");
		index = findUniChar(d);
		while (index > 0) {
			d = d.substring(0,index) + uniCharReflectString(d.charAt(index)) + d.substring(index + 1, d.length());
			index = findUniChar(d);
		}
		return "\"" + d + "\"";	
	}
	/**
	 * @deprecated
	 * @param functionBody
	 * the javascript representation of the function
	 * @return
	 */
	public final static JSON JSFunction(String functionBody) {
		return new JsonFunction(functionBody);
	}
	protected static boolean implementsInterface(Class<?> objClass, Class<?> interf) {
		Class<?>[] inters = objClass.getInterfaces();
		for (int i=0; i < inters.length; i++) {
			if (inters[i] == interf) {
				return true;
			}
		}
		return false;
	}
	/**
	 * get the content of the JSON object, 
	 * @return
	 * * HashMap<String,JSON> => if the JSON object is a representation of an object<br />
	 * * JSON[] => if the JSON object is a representation of an array<br />
	 * * Integer,Integer,Long,Float,Double => if the JSON object is a representation of a Short,Integer,Long,Float,Double<br />
	 * * String => if the JSON object is a representation of String<br />
	 * * Boolean => if the JSON object is a representation of boolean<br />
	 * * null => if the JSON object is a representation of null
	 */
	public abstract Object toObject();
	public abstract int size();
	/**
	 * get a child JSON of a JsonMap by its name
	 * @param name the name of the child JSON entry
	 * @return the child JSON, or null if there's no corresponding child
	 * @throws OperationNotDefinedException if the Object is not an instance of JsonMap
	 */
	@SuppressWarnings("unchecked")
	public JSON get(String name) {
		if (!this.getClass().equals(JsonMap.class)) {
			throw new OperationNotDefinedException("get() is only defined in JsonMap.");
		}
		return ((Map<String,JSON>) data).get(name);
	}
	/**
	 * get a child JSON of a JsonMap by its index
	 * @param index the index of the child JSON entry
	 * @return the child JSON
	 * @throws OperationNotDefinedException if the Object is not an instance of JsonArray
	 */
	public JSON at(int index) {
		if (!this.getClass().equals(JsonArray.class)) {
			throw new OperationNotDefinedException("at() is only defined in JsonArray.");
		}
		return ((JSON[]) data)[index];
	}
	/**
	 * return the JSON String(one line, with no indent) representation of the JSON object
	 * @return
	 * the JSON String
	 */
	public abstract String toString();
	public abstract boolean equals(Object json);
	protected abstract String format(int level, String indentString);
	/**
	 * check whether the JSON object represents a direct value. 
	 * @return
	 * * true => if content is null, boolean, number, String<br />
	 * * false => otherwise
	 */
	public boolean isDirectValue() {
		return false;
	}
	/**
	 * check whether the JSON object represents a container. 
	 * @return
	 * * true => if content is HashMap, Object, Array, ArrayList<br />
	 * * false => otherwise
	 */
	public boolean isContainer() {
		return false;
	}
	/**
	 * @deprecated
	 * check if the JSON object is a representation of a javascript function
	 * @return
	 * * true => if it is a representation of a javascript function<br />
	 * * false => otherwise
	 */
	public boolean isFunction() {
		return false;
	}
	/**
	 * check whether the JSON object represents a null object. 
	 * @return
	 * * true => if content is null<br />
	 * * false => otherwise
	 */
	public boolean isNull() {
		return false;
	}
	/**
	 * return the JSON String (multiple line, with indent) representation of the JSON object
	 * @param indentString
	 * the string used as indent
	 * @return
	 * the JSON String
	 * @throws
	 * com.silverwzw.JSON.IndentStringException
	 */
	public String format(String indentString) {
		if (!indentString.trim().equals("")) {
			throw new IndentStringException();
		}
		return format(0, indentString);
	}
	/**
	 * return the JSON String (multiple line, with indent '\t') representation of the JSON object
	 * @return
	 * the JSON String
	 */
	public String format() {
		return format(0,"\t");
	}
	public abstract Object clone();
	private static String uniCharReflectString(char c) {
		String ret = "\\u";
		ret += hex(c / 0x1000);
		ret += hex(c % 0x1000 / 0x0100);
		ret += hex(c % 0x0100 / 0x0010);
		ret += hex(c % 0x0010);
		return ret;
	}
	private static char hex(int i) {
		assert i >= 0 && i <= 15 : "Int to Hex Char Exception";
		if (i < 10) {
			return (char) ('0' + i);
		} else {
			return (char) ('a' - 10 + i);
		}
	}
	private static int findUniChar(String s) {
		for (int i = 0; i < s.length(); i++) {
			if(127 < s.charAt(i)) {
				return i;
			}
		}
		return -1;
	}
	private static JSON parseTokenStream(ArrayList<JsonToken> tks, int start, int end) throws ParsingException {
		JsonToken first;
		int stack;
		if (start >= end){
			throw new ParsingException();
		}
		first = tks.get(start); 
		if (
				first.typeEquals(JsonToken.BOOLEAN)
				|| first.typeEquals(JsonToken.NUMBER)
				|| first.typeEquals(JsonToken.NULL)
				|| first.typeEquals(JsonToken.STRING)
		) {
			if (end == start +1) {
				return tks.get(start).jsonObj;
			} else {
				throw new ParsingException();
			}
		}
		if (first.typeEquals(JsonToken.LEFT_BRACKET)) {
			if (end < start +2) {
				throw new ParsingException();
			}
			if (tks.get(start +1).typeEquals(JsonToken.RIGHT_BRACKET)) {
				if (end == start + 2) {
					return new JsonArray(new ArrayList<Object>());
				} else {
					throw new ParsingException();
				}
			}
			int currentElementStart, curr;
			ArrayList<JSON> tmpList;
			tmpList = new ArrayList<JSON>();
			currentElementStart = curr = start + 1;
			stack = 0;
			while(curr < end) {
				if (tks.get(curr).typeEquals(JsonToken.LEFT_BRACKET) || tks.get(curr).typeEquals(JsonToken.LEFT_BRACE)) {
					stack++;
				} else if (tks.get(curr).typeEquals(JsonToken.RIGHT_BRACKET) || tks.get(curr).typeEquals(JsonToken.RIGHT_BRACE)) {
					stack--;
					if (stack < 0) {
						if (curr == end - 1 && tks.get(curr).typeEquals(JsonToken.RIGHT_BRACKET)) {
							tmpList.add(parseTokenStream(tks,currentElementStart,curr));
							JSON a[],ret = new JsonArray();
							a = new JSON[tmpList.size()];
							ret.data = tmpList.toArray(a);
							return ret;
						} else {
							throw new ParsingException();
						}
					}
				} else if (stack == 0 && tks.get(curr).typeEquals(JsonToken.COMMA)) {
					tmpList.add(parseTokenStream(tks,currentElementStart,curr));
					currentElementStart = curr + 1;
				}
				curr++;
			}
			throw new ParsingException();
		}
		if (first.typeEquals(JsonToken.LEFT_BRACE)) {
			if (end < start +2) {
				throw new ParsingException();
			}
			if (tks.get(start +1).typeEquals(JsonToken.RIGHT_BRACE)) {
				if (end == start + 2) {
					return new JsonMap(new HashMap<String,Object>());
				} else {
					throw new ParsingException();
				}
			}
			int currentFieldValueStart, curr;
			String currentFieldName;
			HashMap<String,JSON> tmpMap;
			tmpMap = new HashMap<String,JSON>();
			curr = start + 1;
			stack = 0;
			while(curr < end) {
				if ((!tks.get(curr).typeEquals(JsonToken.STRING)) || (!tks.get(curr + 1).typeEquals(JsonToken.COLON))) {
					throw new ParsingException();
				}
				currentFieldName = (String) tks.get(curr).jsonObj.toObject();
				curr += 2;
				currentFieldValueStart = curr;
				
				while(curr < end) {
					if (tks.get(curr).typeEquals(JsonToken.LEFT_BRACKET) || tks.get(curr).typeEquals(JsonToken.LEFT_BRACE)) {
						stack++;
					} else if (tks.get(curr).typeEquals(JsonToken.RIGHT_BRACKET) || tks.get(curr).typeEquals(JsonToken.RIGHT_BRACE)) {
						stack--;
						if (stack < 0 && tks.get(curr).typeEquals(JsonToken.RIGHT_BRACE)) {
							if (curr == end - 1) {
								tmpMap.put(currentFieldName,parseTokenStream(tks,currentFieldValueStart,curr));
								JsonMap ret = new JsonMap();
								ret.data = tmpMap;
								return ret;
							} else {
								throw new ParsingException();
							}
						}
					} else if (stack == 0 && tks.get(curr).typeEquals(JsonToken.COMMA)) {
						tmpMap.put(currentFieldName,parseTokenStream(tks,currentFieldValueStart,curr));
						curr++;
						break;
					}
					curr++;
				}
			}
			throw new ParsingException();
		}
		throw new ParsingException();
	}
}

@SuppressWarnings("serial")
abstract class JsonDirectValue extends JSON {
	public Object toObject() {
		return data;
	}
	public boolean isDirectValue() {
		return true;
	}
	public String format(int level, String indentString) {
		String indent = "";
		for (int i = 0; i < level; i++) {
			indent += indentString;
		}
		return indent + this.toString();
	}
	public Iterator<Entry<String,JSON>> iterator() {
		return new JsonDummyIter();
	}
	public final int size() {
		return 1;
	}
}

@SuppressWarnings("serial")
abstract class JsonContainer extends JSON {
	public boolean isContainer() {
		return true;
	}
	protected abstract String getJsonString(boolean doFormat, int level, String indentString);
	public String toString() {
		return getJsonString(false,0,"");
	}
	public String format(int level, String indentString) {
		return getJsonString(true,level,indentString);
	}
}

@SuppressWarnings("serial")
final class JsonNull extends JsonDirectValue {
	JsonNull() {
		data = null;
	}
	public String toString() {
		return "null";
	}
	public boolean equals(Object json) {
		return json.getClass() == JsonNull.class;
	}
	public boolean isNull() {
		return true;
	}
	public Object clone() {
		return new JsonNull();
	}
}


@SuppressWarnings("serial")
final class JsonFunction extends JsonDirectValue {
	JsonFunction(String functionBody) {
		data = new String(functionBody);
	}
	public Object toObject() {
		return this;
	}
	public boolean equals(Object obj) {
		if (obj.getClass() != JsonFunction.class) {
			return false;
		}
		return ((String)data).equals((String)((JsonFunction)obj).data);
	}
	public String toString() {
		return (String)data;
	}
	public boolean isFunction() {
		return true;
	}
	public Object clone() {
		return new JsonFunction((String) data);
	}
}

@SuppressWarnings("serial")
final class JsonString extends JsonDirectValue {
	JsonString(String string) {
		data = new String(string);
	}
	public String toString() {
		return JSON.ReflectString((String)data);
	}
	public boolean equals(Object json) {
		if (json.getClass() != JsonString.class) {
			return false;
		}
		return data.equals(((JsonString)json).data);
	}
	public Object clone() {
		return new JsonString((String)data);
	}
}

@SuppressWarnings("serial")
final class JsonBoolean extends JsonDirectValue {
	JsonBoolean(boolean bool) {
		data = bool;
	}
	public String toString() {
		return data.toString();
	}
	public boolean equals(Object json) {
		if (json.getClass() != JsonBoolean.class) {
			return false;
		}
		return !(((Boolean)((JsonBoolean)json).data)^(Boolean)data);
	}
	public Object clone() {
		return new JsonBoolean((boolean)(Boolean)data);
	}
}

@SuppressWarnings("serial")
final class JsonNumber extends JsonDirectValue {
	JsonNumber(Object number) {
		assert (number.getClass() == Double.class)
			|| (number.getClass() == Long.class)
			|| (number.getClass() == Integer.class)
			|| (number.getClass() == Float.class)
			|| (number.getClass() == Short.class)
			: "Compiling a non-Number object to JsonNumber";
		if (number.getClass() == Short.class) {
			number = (Integer)(int)(short)(Short)number;
		}
		data = number;
	}
	public String toString() {
		return data.toString();
	}
	public boolean equals(Object json){
		Object o1,o2;
		int i=0, type=0;
		double d=0;
		float f=0;
		long l=0;
		if (json.getClass() != JsonNumber.class) {
			return false;
		}
		o1 = ((JSON)json).data; 
		o2 = data;
		if (o1.getClass() == o2.getClass()) {
			return o1.equals(o2);
		}
		if (o1.getClass() == Integer.class) {
			i = (int)(Integer)o1;
			type += 1;
		}
		if (o2.getClass() == Integer.class) {
			i = (int)(Integer)o2;
			type += 1;
		}
		if (o1.getClass() == Long.class) {
			l = (long)(Long)o1;
			type += 2;
		}
		if (o2.getClass() == Long.class) {
			l = (long)(Long)o2;
			type += 2;
		}
		if (o1.getClass() == Float.class) {
			f = (float)(Float)o1;
			type += 4;
		}
		if (o2.getClass() == Float.class) {
			f = (float)(Float)o2;
			type += 4;
		}
		if (o1.getClass() == Double.class) {
			d = (double)(Double)o1;
			type += 8;
		}
		if (o2.getClass() == Double.class) {
			d = (double)(Double)o2;
			type += 8;
		}
		if (type == 1 + 2) {
			return i == l;
		}
		if (type == 1 + 4) {
			return i == f;
		}
		if (type == 1 + 8) {
			return i == d;
		}
		if (type == 2 + 4) {
			return l == f;
		}
		if (type == 2 + 8) {
			return l == d;
		}
		if (type == 4 + 8) {
			return f == d;
		}
		assert false : "This default branch shouldn't get executed";
		return false;
	}
	public Object clone() {
		if (data.getClass() == Integer.class) {
			int i;
			i = (int)(Integer)data;
			return new JsonNumber(i);
		}
		if (data.getClass() == Float.class) {
			float f;
			f = (float)(Float)data;
			return new JsonNumber(f);
		}
		if (data.getClass() == Double.class) {
			double d;
			d = (double)(Double)data;
			return new JsonNumber(d);
		}
		if (data.getClass() == Long.class) {
			long l;
			l = (long)(Long)data;
			return new JsonNumber(l);
		}
		assert false : "This default branch shouldn't get executed";
		return new JsonNull();
	}
}

@SuppressWarnings("serial")
final class JsonArray extends JsonContainer {
	JsonArray(){};
	JsonArray(Object arr) {
		assert arr.getClass().isArray()||JSON.implementsInterface(arr.getClass(),List.class) : "Compiling a non-Array object to JsonArray";
		if (arr.getClass().isArray()) {
			JSON[] JSONArr;
			JSONArr = new JSON[Array.getLength(arr)];
			for (int i = 0; i < Array.getLength(arr); i++) {
				JSONArr[i] = JSON.auto(Array.get(arr, i));
			}
			data = JSONArr;
		} else {
			List<?> list;
			JSON[] JSONArr;
			list = (List<?>) arr;
			JSONArr = new JSON[list.size()];
			for (int i = 0; i < list.size(); i++) {
				JSONArr[i] = JSON.auto(list.get(i));
			}
			data = JSONArr;
		}
	}
	protected String getJsonString(boolean doFormat, int level, String indentString) {
		String str = "", indent = "";
		if (doFormat) {
			for (int i = 0; i < level; i++) {
				indent += indentString;
			}
		}
		for (int i = 0; i < Array.getLength(data); i++) {
			if (doFormat) {
				str += ((JSON[])data)[i].format(level+1, indentString) + (i == Array.getLength(data)-1?"\n":",\n");
			}
			else {
				str += ((JSON[])data)[i].toString() + (i == Array.getLength(data)-1?"":",");;
			}
		}
		if (str.trim().equals("")) {
			return indent + "[]";
		} else {
			return indent + (doFormat?"[\n":"[") + str +indent + "]";
		}
	}
	public boolean equals(Object obj) {
		if (obj.getClass() != JsonArray.class) {
			return false;
		}
		if (Array.getLength(data) != Array.getLength(((JsonArray)obj).data)) {
			return false;
		}
		for (int i = 0; i < Array.getLength(data); i++) {
			if (!((JSON)Array.get(data, i)).equals((JSON)Array.get(((JsonArray)obj).data,i))) {
				return false;
			}
		}
		return true;
	}
	public Object toObject() {
		Object[] obj;
		obj = new Object[Array.getLength(data)];
		for (int i = 0; i < obj.length; i++) {
			obj[i] = ((JSON)Array.get(data, i)).toObject();
		}
		return obj;
	}
	public Object clone() {
		JSON cloneList[];
		cloneList = new JSON[Array.getLength(data)];
		for(int i = 0; i < cloneList.length; i++) {
			cloneList[i] = (JSON)((JSON)Array.get(data, i)).clone();
		}
		JSON jsarray = new JsonArray();
		jsarray.data = cloneList;
		return jsarray;
	}
	public Iterator<Entry<String,JSON>> iterator() {
		return new JsonArrayIter(this);
	}
	public final int size() {
		return Array.getLength(data);
	}
}

@SuppressWarnings("serial")
final class JsonMap extends JsonContainer {
	JsonMap() {}
	JsonMap(Map<?,?> map) {
		HashMap<String,JSON> d;
		d = new HashMap<String,JSON>();
		Iterator<?> it = map.keySet().iterator();
		while(it.hasNext()) {
			String k;
			k = it.next().toString();
			d.put(k, JSON.auto(map.get(k)));
		}
		data = d;
	}
	@SuppressWarnings("unchecked")
	protected String getJsonString(boolean doFormat, int level, String indentString) {
		HashMap<String,JSON> d;
		String s = "", indent = "";
		Iterator<String> it;
		
		if (doFormat) {
			for (int i = 0; i < level; i++) {
				indent += indentString; 
			}
		}
		
		d = (HashMap<String,JSON>)data;
		it = d.keySet().iterator();
				
		while(it.hasNext()) {
			String k;
			k = it.next();
			if (doFormat) {
				s += indent + indentString + JSON.ReflectString(k) + ":\n" + d.get(k).format(level + 2, indentString) + (it.hasNext()?",\n":"\n");
			} else {
				s += JSON.ReflectString(k) + ":" + d.get(k).toString() + (it.hasNext()?",":"");
			}
		}
		if (s.trim().equals("")) {
			return indent + "{}";
		} else {
			return indent + (doFormat?"{\n":"{") + s + indent + "}";
		}
	}
	@SuppressWarnings("unchecked")
	public boolean equals(Object obj) {
		if (obj.getClass() != JsonMap.class) {
			return false;
		}
		HashMap<String,JSON> hm1,hm2;
		hm1 = (HashMap<String,JSON>)data;
		hm2 = (HashMap<String,JSON>)((JsonMap)obj).data;
		if (hm1.keySet().size() != hm2.keySet().size()) {
			return false;
		}
		Iterator<String> it;
		it = hm1.keySet().iterator();
		while(it.hasNext()) {
			String k;
			k = it.next();
			if (!hm2.containsKey(k)){
				return false;
			}
			if(!hm1.get(k).equals(hm2.get(k))) {
				return false;
			}
		}
		return true;
	}
	@SuppressWarnings("unchecked")
	public Object toObject() {
		HashMap<String,JSON> hm;
		HashMap<String,Object> hmr;
		hm = (HashMap<String,JSON>)data;
		hmr = new HashMap<String,Object>();
		Iterator<String> it;
		it = hm.keySet().iterator();
		while(it.hasNext()) {
			String k;
			k = it.next();
			hmr.put(k, hm.get(k).toObject());
		}
		return hmr;
	}
	@SuppressWarnings("unchecked")
	public Object clone() {
		HashMap<String, JSON> hm1,hm2;
		hm1 = (HashMap<String,JSON>) data;
		hm2 = new HashMap<String,JSON>();
		for (Entry<String,JSON> e : hm1.entrySet()) {
			hm2.put(e.getKey(), (JSON) e.getValue().clone());
		}
		JsonMap jsmap = new JsonMap();
		jsmap.data = hm2;
		return jsmap;
	}
	@SuppressWarnings("unchecked")
	public Iterator<Entry<String,JSON>> iterator() {
		return ((HashMap<String,JSON>) data).entrySet().iterator();
	}
	@SuppressWarnings("unchecked")
	public final int size() {
		return ((HashMap<String,JSON>) data).size();
	}
}

final class JsonEntryForArray implements Entry<String, JSON> {
	private JSON _value, _refer;
	private int _index;
	JsonEntryForArray(JSON value, JsonArray refer, int index) {
		_index = index;
		_value = value;
		_refer = refer;
	}
	public String getKey() {
		return "";
	}
	public JSON getValue() {
		return _value;
	}
	public JSON setValue(JSON value) {
		JSON v,old;
		v = (JSON)value.clone();
		_value = v;
		old = (JSON)Array.get(_refer.data, _index);
		Array.set(_refer.data,_index,v);
		return old;
	}
}

final class JsonDummyIter implements Iterator<Entry<String,JSON>> {
	public boolean hasNext() {
		return false;
	}
	public Entry<String,JSON> next() {
		throw new NoSuchElementException();
	}
	public void remove() {
		throw new UnsupportedOperationException();
	}
}

final class JsonArrayIter implements Iterator<Entry<String,JSON>> {
	private int index;
	private JsonArray _refer;
	private boolean removeAllowed;
	JsonArrayIter(JsonArray refer) {
		index = -1;
		_refer = refer;
		removeAllowed = false;
	}
	public boolean hasNext() {
		return index + 1 < Array.getLength(_refer.data);
	}
	public Entry<String,JSON> next() {
		if (index < Array.getLength(_refer.data)) {
			index++;
			removeAllowed = true;
			return new JsonEntryForArray((JSON)Array.get(_refer.data, index),_refer,index);
		} else {
			throw new NoSuchElementException();
		}
	}
	public void remove() {
		JSON jsarr[];
		
		if (!removeAllowed) {
			throw new IllegalStateException();
		} else {
			removeAllowed = false;
		}
		
		jsarr = new JSON[Array.getLength(_refer.data) - 1];
		
		for (int i = 0, j = 0; j < jsarr.length ;i++,j++) {
			if (i == index) {
				j--;
			} else {
				jsarr[j] = (JSON) Array.get(_refer.data, i);
			}
		}
		
		index--;
		_refer.data = jsarr;
	}
}

final class JsonToken {
	int tokenType;
	JSON jsonObj;
	static JsonToken STRING = JsonToken.string(null);
	static JsonToken NUMBER = JsonToken.number(null);
	static JsonToken BOOLEAN = JsonToken.bool(null);
	static JsonToken NULL = JsonToken.empty();
	static JsonToken LEFT_BRACE = JsonToken.leftBrace();
	static JsonToken LEFT_BRACKET = JsonToken.leftBracket();
	static JsonToken RIGHT_BRACKET = JsonToken.rightBracket();
	static JsonToken RIGHT_BRACE = JsonToken.rightBrace();
	static JsonToken COLON = JsonToken.colon();
	static JsonToken COMMA = JsonToken.comma();
	private JsonToken(int i) {
		tokenType = i;
		jsonObj = null;
	}
	private JsonToken(int i, JSON js) {
		tokenType = i;
		jsonObj = js;
	}
	public boolean typeEquals(Object obj) {
		if (obj.getClass() == Integer.class) {
			return tokenType == (int)(Integer)obj;
		} else if (obj.getClass() == JsonToken.class) {
			return tokenType == ((JsonToken)obj).tokenType;
		}
		return false;
	}
	static JsonToken string(JsonString js) {
		return new JsonToken(0,js);
	}
	static JsonToken number(JsonNumber js) {
		return new JsonToken(1,js);
	}
	static JsonToken empty() {
		return new JsonToken(3,new JsonNull());
	}
	static JsonToken bool(JsonBoolean js) {
		return new JsonToken(5,js);
	}
	static JsonToken leftBrace() {
		return new JsonToken(6);
	}
	static JsonToken rightBrace() {
		return new JsonToken(7);
	}
	static JsonToken leftBracket() {
		return new JsonToken(8);
	}
	static JsonToken rightBracket() {
		return new JsonToken(9);
	}
	static JsonToken colon() {
		return new JsonToken(10);
	}
	static JsonToken comma() {
		return new JsonToken(11);
	}
	public static ArrayList<JsonToken> getTokenStream(Reader json_reader) throws JSON.LexicalException, IOException {
		ArrayList<JsonToken> tokenStream = new ArrayList<JsonToken>();
		int i;
		i = eatSpaces(json_reader);
		while(i != -1) {
			switch (i) {
				case '[':
					tokenStream.add(JsonToken.LEFT_BRACKET);
					i = eatSpaces(json_reader);
					break;
				case '{':
					tokenStream.add(JsonToken.LEFT_BRACE);
					i = eatSpaces(json_reader);
					break;
				case '}':
					tokenStream.add(JsonToken.RIGHT_BRACE);
					i = eatSpaces(json_reader);
					break;
				case ']':
					tokenStream.add(JsonToken.RIGHT_BRACKET);
					i = eatSpaces(json_reader);
					break;
				case ':':
					tokenStream.add(JsonToken.COLON);
					i = eatSpaces(json_reader);
					break;
				case ',':
					tokenStream.add(JsonToken.COMMA);
					i = eatSpaces(json_reader);
					break;
				case '"':
				case '\'':
					tokenStream.add(JsonToken.string(eatString(json_reader, i)));
					i = eatSpaces(json_reader);
					break;
				case '+':
				case '-':
				case '.':
				case '0':
				case '1':
				case '2':
				case '3':
				case '4':
				case '5':
				case '6':
				case '7':
				case '8':
				case '9':
					Object[] o;
					o = eatNumber(json_reader, i);
					tokenStream.add(JsonToken.number((JsonNumber)o[0]));
					i = (Integer) o[1];
					if (isSpace(i)) {
						i = eatSpaces(json_reader);
					}
					break;
				case 'n':
					if (json_reader.read() != 'u' || json_reader.read()!= 'l' || json_reader.read() != 'l') {
						throw new JSON.LexicalException();
					}
					tokenStream.add(JsonToken.empty());
					i = eatSpaces(json_reader);
					break;
				case 't':
					if (json_reader.read() != 'r' || json_reader.read()!= 'u' || json_reader.read() != 'e') {
						throw new JSON.LexicalException();
					}
					tokenStream.add(JsonToken.bool(new JsonBoolean(true)));
					i = eatSpaces(json_reader);
					break;
				case 'f':
					if (json_reader.read() != 'a' || json_reader.read()!= 'l' || json_reader.read() != 's'|| json_reader.read() != 'e') {
						throw new JSON.LexicalException();
					}
					tokenStream.add(JsonToken.bool(new JsonBoolean(false)));
					i = eatSpaces(json_reader);
					break;
				case '\t':
				case '\r':
				case ' ':
				case '\n':
					i = eatSpaces(json_reader);
					break;
				default:
					throw new JSON.LexicalException();
			}
		}
		return tokenStream;
	}
	private static int eatSpaces(Reader r) throws IOException {
		int in;
		in = r.read();
		while (isSpace(in)) {
			in = r.read();
		}
		return in;
	}
	private static Object[] eatNumber(Reader r, int tmpc) throws JSON.LexicalException, IOException {
		double frac = 0, esp = 0.1;
		int i = 0, exp = 0;
		boolean positive = true, epositive = true;
		//eat sign
		if (tmpc == '-') {
			positive = false;
			tmpc = r.read(); 
		} else if (tmpc == '+') {
			tmpc = r.read();
		}
		
		//eat int
		if (tmpc == '0') {
			tmpc = r.read();
			if (tmpc <= '9' && tmpc >= '0' ) {
				throw new JSON.LexicalException();
			}
		} else {
			while (tmpc <= '9' && tmpc >= '0'){
				i *= 10;
				i += tmpc - '0';
				tmpc = r.read();
			}
		}
		//eat frac
		if (tmpc == '.') {
			tmpc = r.read();
			while(tmpc <= '9' && tmpc >= '0') {
				frac += esp * (tmpc - '0');
				esp /= 10;
				tmpc = r.read();
			}
		}
		//eat exp
		if (tmpc == 'e' || tmpc == 'E') {
			tmpc = r.read();
			if (tmpc == '-') {
				epositive = false;
				tmpc = r.read();
			} else if (tmpc == '+') {
				tmpc = r.read();
			}
			if (tmpc == '0') {
				tmpc = r.read();
				if (tmpc <= '9' && tmpc >= '0') {
					throw new JSON.LexicalException();
				}
			} else {
				while (tmpc >= '0' && tmpc <= '9') {
					exp *= 10;
					exp += tmpc - '0';
					tmpc = r.read();
				}
			}
		}
		Object[] o;
		o = new Object[2];
		o[0] = new JsonNumber((positive?1:-1)*(i+frac)*Math.pow(10, (epositive?1:-1)*exp));
		o[1] = tmpc;
		return o;
	}
	private static JsonString eatString(Reader r, int strQuote) throws JSON.LexicalException, IOException {
		String realString="";
		int charCode;
		int in;
		in = r.read();
		while (in != strQuote) {
			if (in == '\\') {
				int in2;
				in2 = r.read();
				switch (in2) {
					case 'u':
						charCode = getUnicode(r);
						if (charCode < 0) {
							throw new JSON.LexicalException();
						}
						realString += (char) charCode;
						break;
					case '"':
					case '\'':
					case '/':
					case '\\':
						realString += (char) in2;
						break;
					case 'n':
						realString += '\n';
						break;
					case 'r':
						realString += '\r';
						break;
					case 'b':
						realString += '\b';
						break;
					case 't':
						realString += '\t';
						break;
					default:
						throw new JSON.LexicalException();
				}
			} else if (in != -1){
				realString += (char)in;
			} else {
				throw new JSON.LexicalException();
			}
			in = r.read();
		}
		return new JsonString(realString);
	}
	private static boolean isSpace(int c) {
		return c ==' ' || c == '\t' || c == '\n' || c == '\r';
	}
	private static int hex(int c) {
		if (c >= '0' && c <= '9') {
			return c - '0';
		} else if (c >= 'a' && c <= 'f') {
			return c - 'a' + 10;
		} else if (c >= 'A' && c <= 'F') {
			return c - 'A' + 10;
		} else {
			return -1;
		}
	}
	private static int getUnicode(Reader r) throws IOException {
		int i,h;
		int charCode = 0;
		for (i = 0; i < 4; i++) {
			h = hex(r.read());
			if (h < 0) {
				return -1;
			}
			charCode += h * (0x1000 >> i);
		}
		return charCode;
	}
}