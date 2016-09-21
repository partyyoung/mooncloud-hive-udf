package net.mooncloud.hadoop.hive.ql.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.io.Text;
import org.json.JSONArray;
import org.json.JSONException;

public class MapAggregation {

	public static Map aggregateMap(Map<Object, Object> aObj,
			Map<Object, Object> bObj) throws UDFArgumentTypeException {

		Map<String, Object> b = new LinkedHashMap<String, Object>(bObj.size());
		for (Entry<Object, Object> entry : bObj.entrySet()) {
			b.put(String.valueOf(entry.getKey()), entry.getValue());
		}
		bObj.clear();

		Map r = new LinkedHashMap(aObj.size());
		for (Entry<Object, Object> entry : aObj.entrySet()) {
			String akey = String.valueOf(entry.getKey());
			Object avalue = entry.getValue();
			Object bvalue = b.get(akey);
			HashSet<Object> rvalue = new HashSet<Object>();
			if (avalue != null) {
				if (avalue instanceof Collection)
					rvalue.addAll((Collection) avalue);
				else {
					try {
						JSONArray jsonObj = new JSONArray(avalue.toString());
						for (int j = 0; j < jsonObj.length(); j++)
							rvalue.add(jsonObj.getString(j));
					} catch (JSONException e) {
						e.printStackTrace();
					}
				}
			}
			if (bvalue != null) {
				if (bvalue instanceof Collection)
					rvalue.addAll((Collection) bvalue);
				else {
					try {
						JSONArray jsonObj = new JSONArray(bvalue.toString());
						for (int j = 0; j < jsonObj.length(); j++)
							rvalue.add(jsonObj.getString(j));
					} catch (JSONException e) {
						e.printStackTrace();
					}
				}
			}
			r.put(akey, rvalue);
			b.remove(akey);
		}
		aObj.clear();
		r.putAll(b);
		return aggregateMap(r);
	}

	public static Map aggregateMap(Map<Object, Object> aObj)
			throws UDFArgumentTypeException {
		Map<String, Object> a = new LinkedHashMap<String, Object>(aObj.size());
		for (Entry<Object, Object> entry : aObj.entrySet()) {
			a.put(String.valueOf(entry.getKey()), entry.getValue());
		}
		aObj.clear();

		Map r = new LinkedHashMap(a.size());
		ArrayList<String> aKeyList = new ArrayList<String>(a.keySet());
		// Iterator<Object> aKeyListIter = aKeyList.iterator();
		while (aKeyList.size() > 0) {
			// while (aKeyListIter.hasNext()) {
			String aKey = aKeyList.get(0);// aKeyListIter.next();
			Object avalue = a.get(aKey);
			aKeyList.remove(aKey);
			a.remove(aKey);
			ArrayList<Object> aValueList = new ArrayList<Object>();
			HashSet<Object> aValueSet = new HashSet<Object>();
			aValueSet.add(aKey);
			if (avalue != null) {
				if (avalue instanceof Collection)
					aValueList.addAll((Collection) avalue);
				else {
					try {
						JSONArray jsonObj = new JSONArray(avalue.toString());
						for (int j = 0; j < jsonObj.length(); j++)
							aValueList.add(jsonObj.getString(j));
					} catch (JSONException e) {
						e.printStackTrace();
					}
				}
			}
			aValueSet.addAll(aValueList);

			// 开始合并
			// Iterator<Object> aValueListIter = aValueList.iterator();
			for (int j = 0; j < aValueList.size(); j++) {
				String aKeyValue = String.valueOf(aValueList.get(j));// aValueListIter.next();
				Object aKeyValueValue = a.get(aKeyValue);
				// throw new UDFArgumentTypeException(0, aKey.getClass() + "="
				// + aKey + ":" + aKeyValue + "=" + aKeyValueValue);
				if (aKeyValueValue != null) {
					if (aKeyValueValue instanceof Collection) {
						Collection t = (Collection) aKeyValueValue;
						t.removeAll(aValueSet);
						aValueList.addAll(t);
						aValueSet.addAll(t);
					} else {
						try {
							HashSet<String> t = new HashSet<String>();
							JSONArray jsonObj = new JSONArray(
									aKeyValueValue.toString());
							for (int k = 0; k < jsonObj.length(); k++)
								t.add(jsonObj.getString(k));
							t.removeAll(aValueSet);
							aValueList.addAll(t);
							aValueSet.addAll(t);
						} catch (JSONException e) {
							e.printStackTrace();
						}
					}
				}
				aKeyList.remove(aKeyValue);
				a.remove(aKeyValue);
			}
			r.put(new Text(aKey), aValueSet);
		}
		return r;
	}

	public static void main(String[] args) throws Exception,
			UDFArgumentTypeException {
		Map<Object, Object> a = new LinkedHashMap<Object, Object>();
		HashSet<Object> aValueSet = new HashSet<Object>();
		// aValueSet.add("[2,3,4,5]");
		// aValueSet.add("6");
		a.put("1", "[1,2,3,4,5]");
		Map<Object, Object> b = new LinkedHashMap<Object, Object>();
		a.put("2", "[3,1]");
		Map<Object, Object> c = new LinkedHashMap<Object, Object>();
		c.put("4", "[3,1]");
		c.put("6", "['3',1]");
		c.put("3", "[6,1]");
		System.out.println(a);
		System.out.println(aggregateMap(a, c));
	}
}
