/**
 * 
 */
package com.impetus.kundera.hbase.client.service;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.impetus.kundera.hbase.client.Writer;
import com.impetus.kundera.metadata.EntityMetadata.Column;
import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.proxy.EnhancedEntity;

/**
 * @author impetus
 *
 */
public class HBaseWriter implements Writer {

	@Override
	public void writeColumns(HTable htable, String columnFamily, String rowKey,List<Column> columns, EnhancedEntity e) throws IOException {
		Put p = new Put(Bytes.toBytes(rowKey));
		
		for(Column col:columns) {
//		Set<String> keys = columnValues.keySet();
//		Iterator<String> iter = keys.iterator();
//	    while(iter.hasNext()){
			String qualifier = col.getName();
	    	try {
	    		PropertyAccessorHelper.getObject(e.getEntity(), col.getField());
				p.add(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier), PropertyAccessorHelper.get(e.getEntity(), col.getField()));
			} catch (PropertyAccessException e1) {
				throw new IOException(e1.getMessage());
			}
	    }
	    htable.put(p);
	}
	
}
