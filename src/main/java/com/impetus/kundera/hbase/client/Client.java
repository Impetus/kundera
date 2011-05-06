/**
 * 
 */
package com.impetus.kundera.hbase.client;

import java.io.IOException;
import java.util.List;

import com.impetus.kundera.metadata.EntityMetadata.Column;
import com.impetus.kundera.proxy.EnhancedEntity;

/**
 * HBase client for interaction with HBase.
 * @author impetus
 */
public interface Client {

	/**
	 * 
	 * @param columnFamily
	 * @param rowKey
	 * @param columns
	 */
	void write(String tableName, String columnFamily, String rowKey, List<Column> columns, EnhancedEntity e)  throws IOException;
	
	/**
	 * 
	 * @param columnFamily
	 * @param rowKey
	 * @return
	 */
	HBaseData read(String tableName, String columnFamily, String rowKey, String...columnNames)  throws IOException;
	
}
