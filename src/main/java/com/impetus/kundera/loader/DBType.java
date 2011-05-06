package com.impetus.kundera.loader;

import java.util.HashMap;
import java.util.Map;

/**
 * 
 * @author impetus
 *
 */
public enum DBType {
	HBASE,	MONGODB, CASSANDRA;
	static Map<String, DBType> coll=new HashMap<String,DBType>();

	/**
	 * Static initialisation.
	 */
	static{
		coll.put(HBASE.name(), HBASE);
		coll.put(CASSANDRA.name(), CASSANDRA);
		coll.put(MONGODB.name(), MONGODB);
	}
}
