package com.impetus.core;

public interface KunderaEntity<T, K> {

	T find(K key);
	void save();
	void update();
	void delete();
	
}
