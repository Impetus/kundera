/*******************************************************************************
 * * Copyright 2012 Impetus Infotech.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 ******************************************************************************/
package com.impetus.client.hbase;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;

import com.impetus.kundera.DataWrapper;

/**
 * The Class HBaseData.
 * 
 * @author impetus
 */
public class HBaseData implements DataWrapper
{

    /** The column family. */
    private String columnFamily;

    /** The row key. */
    private byte[] rowKey;

    /** The columns. */
//    private List<KeyValue> columns;
    
    private Map<String, byte[]> columns = new HashMap<String, byte[]>();

    /**
     * constructor with fields.
     * 
     * @param columnFamily
     *            HBase column family
     * @param rowKey
     *            Row key
     */
    public HBaseData(String columnFamily, byte[] rowKey)
    {
        this.columnFamily = columnFamily;
        this.rowKey = rowKey;
    }

    /**
     * Instantiates a new h base data.
     * 
     * @param rowKey
     *            the row key
     */
    public HBaseData(byte[] rowKey)
    {
        this.rowKey = rowKey;
    }

    /**
     * Getter column family.
     * 
     * @return columnFamily column family
     */
    public String getColumnFamily()
    {
        return columnFamily;
    }

    /**
     * Getter for row key.
     * 
     * @return rowKey
     */
    public byte[] getRowKey()
    {
        return rowKey;
    }

    public byte[] getColumnValue(String qualifier )
    {
        return columns.get(qualifier);
    }
    
    public Map<String, byte[]> getColumns()
    {
        return columns;
    }

    public void setColumns(List<KeyValue> columns)
    {
        for(KeyValue column : columns)
        {
            putColumn(column.getQualifier(), column.getValue());
        }
    }

    private void putColumn(byte[] qualifier, byte[] qualifierValue)
    {
        this.columns.put(Bytes.toString(qualifier), qualifierValue);
    }
    
    
}
