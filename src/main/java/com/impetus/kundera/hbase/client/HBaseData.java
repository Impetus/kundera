package com.impetus.kundera.hbase.client;

import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;

import com.impetus.kundera.DataWrapper;

/**
 * @author impetus
 */
public class HBaseData  implements DataWrapper{
	
	private String columnFamily;
	private String rowKey;
	private List<KeyValue> columns;
	
	/**
	 * constructor with fields.
	 * @param columnFamily HBase column family
	 * @param rowKey            Row key
	 */
	public HBaseData(String columnFamily, String rowKey) {
		this.columnFamily = columnFamily;
		this.rowKey = rowKey;
	}
	
	/**
	 * Getter column family
	 * @return columnFamily column family
	 */
	public String getColumnFamily() {
		return columnFamily;
	}
	

	/**
	 * Getter for row key
	 * @return   rowKey
	 */
	public String getRowKey() {
		return rowKey;
	}
	
	/**
	 * Getter for list of columns.
	 * @return list of columns
	 */
	public List<KeyValue> getColumns() {
		return Collections.unmodifiableList(columns);
	}

	/**
	 * @param columns
	 */
	public void setColumns(List<KeyValue> columns) {
		this.columns = columns;
	}

}
