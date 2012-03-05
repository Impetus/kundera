/*******************************************************************************
 * * Copyright 2011 Impetus Infotech.
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
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.PersistenceException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.impetus.client.hbase.Writer;
import com.impetus.kundera.Constants;
import com.impetus.kundera.db.RelationHolder;
import com.impetus.kundera.metadata.MetadataUtils;
import com.impetus.kundera.metadata.model.Column;
import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessorHelper;

/**
 * The Class HBaseWriter.
 * 
 * @author impetus
 */
public class HBaseWriter implements Writer
{
    /** the log used by this class. */
    private static Log log = LogFactory.getLog(HBaseWriter.class);

    @Override
    public void writeColumns(HTable htable, String columnFamily, String rowKey, List<Column> columns,
            Object columnFamilyObj) throws IOException
    {
        Put p = new Put(Bytes.toBytes(rowKey));

        for (Column column : columns)
        {
            String qualifier = column.getName();
            try
            {

                p.add(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier),
                        PropertyAccessorHelper.get(columnFamilyObj, column.getField()));
            }
            catch (PropertyAccessException e1)
            {
                throw new IOException(e1.getMessage());
            }
        }
        htable.put(p);
    }

    @Override
    public void writeColumn(HTable htable, String columnFamily, String rowKey, Column column, Object columnObj)
            throws IOException
    {
        Put p = new Put(Bytes.toBytes(rowKey));

        p.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column.getName()), Bytes.toBytes(columnObj.toString()));

        htable.put(p);
    }

    @Override
    public void writeColumns(HTable htable, String rowKey, List<Column> columns, Object entity) throws IOException
    {
        Put p = new Put(Bytes.toBytes(rowKey));

        for (Column column : columns)
        {
            String qualifier = column.getName();
            try
            {

            	p.add(Bytes.toBytes(qualifier), Bytes.toBytes(qualifier), System.currentTimeMillis(), PropertyAccessorHelper.get(entity, column.getField()));
//                p.add(Bytes.toBytes(qualifier), System.currentTimeMillis(),
//                        PropertyAccessorHelper.get(entity, column.getField()));
            

            }
            catch (PropertyAccessException e1)
            {
                throw new IOException(e1.getMessage());
            }
        }
        htable.put(p);
    }

    @Override
    public void writeColumns(HTable htable, String rowKey, Map<String, String> columns) throws IOException
    {

        Put p = new Put(Bytes.toBytes(rowKey));

        for (String columnName : columns.keySet())
        {
            p.add(Bytes.toBytes(Constants.JOIN_COLUMNS_FAMILY_NAME), Bytes.toBytes(columnName), columns.get(columnName)
                    .getBytes());
        }
        htable.put(p);
    }

    @Override
    public void writeRelations(HTable htable, String rowKey, boolean containsEmbeddedObjectsOnly,
            List<RelationHolder> relations) throws IOException
    {
        Put p = new Put(Bytes.toBytes(rowKey));

        for (RelationHolder r : relations)
        {
            if (r != null)
            {
                if (containsEmbeddedObjectsOnly)
                {
                    p.add(Bytes.toBytes(r.getRelationName()), Bytes.toBytes(r.getRelationName()),
                            Bytes.toBytes(r.getRelationValue()));
                }
                else
                {
                	p.add(Bytes.toBytes(r.getRelationName()), Bytes.toBytes(r.getRelationName()), System.currentTimeMillis(), Bytes.toBytes(r.getRelationValue()));
//                    p.add(Bytes.toBytes(r.getRelationName()), System.currentTimeMillis(),
//                            Bytes.toBytes(r.getRelationValue()));
                }

            }
        }

        htable.put(p);
    }

    // TODO: Scope of performance improvement in this code
    @Override
    public void writeForeignKeys(HTable hTable, String rowKey, Map<String, Set<String>> foreignKeyMap)
            throws IOException
    {
        Put p = new Put(Bytes.toBytes(rowKey));

        // Checking if foreign key column family exists
        Get g = new Get(Bytes.toBytes(rowKey));
        Result r = hTable.get(g);

        for (Map.Entry<String, Set<String>> entry : foreignKeyMap.entrySet())
        {
            String property = entry.getKey(); // Foreign key name
            Set<String> foreignKeys = entry.getValue();
            String keys = MetadataUtils.serializeKeys(foreignKeys);

            // Check if there was any existing foreign key value, if yes, append
            // it
            byte[] value = r.getValue(Bytes.toBytes(Constants.FOREIGN_KEY_EMBEDDED_COLUMN_NAME),
                    Bytes.toBytes(property));
            String existingForeignKey = Bytes.toString(value);

            if (existingForeignKey == null || existingForeignKey.isEmpty())
            {
                p.add(Bytes.toBytes(Constants.FOREIGN_KEY_EMBEDDED_COLUMN_NAME), Bytes.toBytes(property),
                        Bytes.toBytes(keys));
            }
            else
            {
                p.add(Bytes.toBytes(Constants.FOREIGN_KEY_EMBEDDED_COLUMN_NAME), Bytes.toBytes(property),
                        Bytes.toBytes(existingForeignKey + Constants.FOREIGN_KEY_SEPARATOR + keys));
            }

        }

        hTable.put(p);
    }

    /**
     * Support for delete over HBase.
     */
    /* (non-Javadoc)
     * @see com.impetus.client.hbase.Writer#delete(org.apache.hadoop.hbase.client.HTable, java.lang.String, java.lang.String)
     */
    public void delete(HTable hTable, String rowKey, String columnFamily)
    {   	
        try
        {
        	byte[] rowBytes = Bytes.toBytes(rowKey);
        	Delete delete = new Delete(rowBytes);
        	
            hTable.delete(delete);
        }
        catch (IOException e)
        {
            log.error("Error while delete on hbase for : " + rowKey);
            throw new PersistenceException(e.getMessage());
        }
    }

}