/**
 * 
 */
package com.impetus.kundera.hbase.admin;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.impetus.kundera.hbase.client.HBaseData;
import com.impetus.kundera.metadata.EntityMetadata.Column;
import com.impetus.kundera.proxy.EnhancedEntity;


/**
 * Data handler for HBase  queries.
 * @author impetus
 */
public interface DataHandler {
	
	
	/**
	 * Creates a HBase table.
	 * @param tableName  table name.
	 * @param colFamily column family.
	 */
	void createTable(String tableName,String... colFamily)  throws IOException;
	
	/**
	 * @param tableName 
	 * @param columnFamily
	 * @param rowKey
	 * @param columns
	 * @throws IOException
	 */
	void loadData(String tableName, String columnFamily, String rowKey,List<Column> columns, EnhancedEntity e)  throws IOException;
	
	/**
	 * Populates data for give column family, column name,  and HBase table name.
	 * @param tableName        HBase table name
	 * @param columnFamily   column family name
	 * @param columnName    column name
	 * @param rowKey              HBase row key
	 */
	HBaseData populateData(String tableName, String columnFamily, String[] columnName, String rowKey) throws IOException ;

	 /**
     * Shutdown.
     */
    void shutdown();
    }
