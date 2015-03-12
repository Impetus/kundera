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
package com.impetus.client.hbase.service;

import java.io.IOException;
import java.util.Map;

import javax.persistence.PersistenceException;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.hbase.Writer;
import com.impetus.client.hbase.admin.HBaseCell;
import com.impetus.client.hbase.admin.HBaseRow;
import com.impetus.client.hbase.utils.HBaseUtils;

/**
 * @author Pragalbh Garg
 * 
 */
public class HBaseWriter implements Writer
{
    /** the log used by this class. */
    private static Logger log = LoggerFactory.getLogger(HBaseWriter.class);

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.client.hbase.Writer#writeRow(org.apache.hadoop.hbase.client
     * .Table, com.impetus.client.hbase.admin.HBaseRow)
     */
    @Override
    public void writeRow(Table hTable, HBaseRow hbaseRow) throws IOException
    {

        Put p = preparePut(hbaseRow);
        hTable.put(p);
    }

    /**
     * Prepare put.
     * 
     * @param hbaseRow
     *            the hbase row
     * @return the put
     */
    public Put preparePut(HBaseRow hbaseRow)
    {
        Put p = new Put(HBaseUtils.getBytes(hbaseRow.getRowKey()));
        for (HBaseCell hbaseCell : hbaseRow.getRowCells())
        {
            String colFamily = hbaseCell.getColumnFamily();
            String colQualifier = hbaseCell.getColumnName();
            Object colValue = hbaseCell.getValue();
            if (colFamily != null && colQualifier != null && colValue != null)
                p.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(colQualifier), HBaseUtils.getBytes(colValue));
        }
        return p;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.client.hbase.Writer#writeColumns(org.apache.hadoop.hbase.
     * client.Table, java.lang.Object, java.util.Map, java.lang.String)
     */
    @Override
    public void writeColumns(Table htable, Object rowKey, Map<String, Object> columns, String columnFamilyName)
            throws IOException
    {

        Put p = new Put(HBaseUtils.getBytes(rowKey));

        boolean isPresent = false;
        for (String columnName : columns.keySet())
        {
            p.addColumn(columnFamilyName.getBytes(), Bytes.toBytes(columnName), HBaseUtils.getBytes(columns.get(columnName)));
            isPresent = true;
        }

        if (isPresent)
        {
            htable.put(p);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.client.hbase.Writer#delete(org.apache.hadoop.hbase.client
     * .Table, java.lang.Object)
     */
    @Override
    public void delete(Table hTable, Object rowKey)
    {
        try
        {
            byte[] rowBytes = HBaseUtils.getBytes(rowKey);
            Delete delete = new Delete(rowBytes);
            hTable.delete(delete);
        }
        catch (IOException e)
        {
            log.error("Error while delete on hbase for : " + rowKey);
            throw new PersistenceException(e);
        }
    }
}