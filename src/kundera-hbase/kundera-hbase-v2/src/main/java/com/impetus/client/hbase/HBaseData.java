/*******************************************************************************
 * * Copyright 2015 Impetus Infotech.
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.util.Bytes;

import com.impetus.kundera.DataWrapper;

/**
 * @author Pragalbh Garg
 *
 */
public class HBaseData implements DataWrapper {

    /** The column family. */
//    private String columnFamily;

    private String tableName;
    /** The row key. */
    private byte[] rowKey;

    private Map<String, byte[]> columns = new HashMap<String, byte[]>();

    /**
     * Instantiates a new h base data.
     * 
     * @param tableName
     *            the table name
     * @param rowKey
     *            the row key
     */
    public HBaseData(String tableName, byte[] rowKey) {
        this.tableName = tableName;
        this.rowKey = rowKey;
    }

    /**
     * Instantiates a new h base data.
     * 
     * @param rowKey
     *            the row key
     */
    public HBaseData(byte[] rowKey) {
        this.rowKey = rowKey;
    }

    /* (non-Javadoc)
     * @see com.impetus.kundera.DataWrapper#getColumnFamily()
     */
    @Override
    public String getColumnFamily() {
        return tableName;
    }

    /* (non-Javadoc)
     * @see com.impetus.kundera.DataWrapper#getRowKey()
     */
    @Override
    public byte[] getRowKey()
    {
        return rowKey;
    }

    /**
     * Gets the column value.
     * 
     * @param qualifier
     *            the qualifier
     * @return the column value
     */
    public byte[] getColumnValue(String qualifier)
    {
        return columns.get(qualifier);
    }

    /**
     * Gets the columns.
     * 
     * @return the columns
     */
    public Map<String, byte[]> getColumns()
    {
        return columns;
    }

    /**
     * Sets the columns.
     * 
     * @param columns
     *            the new columns
     */
    public void setColumns(List<Cell> columns)
    {
        for (Cell column : columns)
        {
            putColumn(CellUtil.cloneFamily(column), CellUtil.cloneQualifier(column), CellUtil.cloneValue(column));
        }
    }

    /**
     * Put column.
     * 
     * @param family
     *            the family
     * @param qualifier
     *            the qualifier
     * @param qualifierValue
     *            the qualifier value
     */
    private void putColumn(byte[] family, byte[] qualifier, byte[] qualifierValue)
    {
        this.columns.put(Bytes.toString(family) + ":" + Bytes.toString(qualifier), qualifierValue);
    }

}
