package com.impetus.kundera.loader;

import java.util.HashMap;
import java.util.Map;

import com.impetus.kundera.Client;

/**
 * 
 * @author impetus
 *
 */
public enum ClientType {
	HBASE,	PELOPS,	MONGODB, THRIFT;
	static Map<String, ClientType> coll=new HashMap<String,ClientType>();

	/**
	 * Static initialisation.
	 */
	static{
		coll.put(HBASE.name(), HBASE);
		coll.put(PELOPS.name(), PELOPS);
		coll.put(THRIFT.name(), THRIFT);
		coll.put(MONGODB.name(), MONGODB);
	}
	/**
	 * Returns value of clientType
	 * @param clientType  client type
	 * @return  clientType enum value.
	 */
	public static ClientType getValue(String clientType){
		if(clientType ==null) {
			throw new EnumConstantNotPresentException(null, clientType);
		}
		return coll.get(clientType);
	}
}
