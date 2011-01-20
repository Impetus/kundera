package com.impetus.kundera.hbase.client;

import java.io.IOException;

import org.apache.hadoop.hbase.client.HTable;

public interface Reader {

	/**
	 * Populates HBase data for given family name.
	 * @param hTable      				HBase table 
	 * @param columnFamily        HBase column family    
	 * @param columnName	        HBase column name.
	 * @param rowKey                   HBase row key.
	 * @return      HBase data wrapper containing all column names along with values.
	 */
	HBaseData LoadData(HTable hTable, String columnFamily, String[] columnName, String rowKey) throws IOException;
	
	/**
	 * 
	 * @param hTable
	 * @param qualifiers
	 * @return
	 */
	HBaseData loadAll(HTable hTable, String...qualifiers) throws IOException;

}
