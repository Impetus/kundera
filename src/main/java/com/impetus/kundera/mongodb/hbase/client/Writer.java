package com.impetus.kundera.hbase.client;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.HTable;

import com.impetus.kundera.metadata.EntityMetadata.Column;
import com.impetus.kundera.proxy.EnhancedEntity;

/**
 * HBase data writer.
 * @author impetus
 */
public interface Writer {

	/**
	 * 
	 * @param columnFamily
	 * @param rowKey
	 * @param columns
	 */
	void writeColumns(HTable htable, String columnFamily, String rowKey, List<Column> columns, EnhancedEntity e) throws IOException;
}
