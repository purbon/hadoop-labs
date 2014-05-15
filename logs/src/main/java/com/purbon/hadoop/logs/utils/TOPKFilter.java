package com.purbon.hadoop.logs.utils;

import java.util.TreeMap;


public class TOPKFilter<K extends Comparable<K>, V> extends TreeMap<K, V> {

	private static final long serialVersionUID = 6841983967479825663L;

	private int topK = 0;
	
	public TOPKFilter(int topK) {
		super();
		this.topK = topK;
	}

	@Override
	public V put(K key, V value) {
		if (size() > topK && (firstKey().compareTo(key) < 0))
			remove(firstKey());
		if (size() <= topK)
			super.put(key, value);
		return value;
	}
	
	
}
