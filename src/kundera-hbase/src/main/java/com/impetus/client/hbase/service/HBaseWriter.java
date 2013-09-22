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
package com.impetus.client.hbase.service;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.PersistenceException;
import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.SingularAttribute;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.hbase.Writer;
import com.impetus.client.hbase.admin.HBaseDataHandler.HBaseDataWrapper;
import com.impetus.client.hbase.utils.HBaseUtils;
import com.impetus.kundera.Constants;
import com.impetus.kundera.db.RelationHolder;
import com.impetus.kundera.metadata.MetadataUtils;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessorHelper;

/**
 * The Class HBaseWriter responsible for all sort of get and put commands to be
 * executed on a hTable.
 * 
 * @author vivek.mishra
 */
public class HBaseWriter implements Writer
{
    /** the log used by this class. */
    private static Logger log = LoggerFactory.getLogger(HBaseWriter.class);

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.client.hbase.Writer#writeColumns(org.apache.hadoop.hbase.
     * client.HTable, java.lang.String, java.lang.Object, java.util.Set,
     * java.lang.Object)
     */
    @Override
    public void writeColumns(HTableInterface htable, String columnFamily, Object rowKey,
            Map<String, Attribute> columns,

            Map<String, Object> values, Object columnFamilyObj) throws IOException
    {
        Put p = preparePut(columnFamily, rowKey, columns, values);
        htable.put(p);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.client.hbase.Writer#writeColumn(org.apache.hadoop.hbase.client
     * .HTable, java.lang.String, java.lang.Object,
     * javax.persistence.metamodel.Attribute, java.lang.Object)
     */
    @Override
    public void writeColumn(HTableInterface htable, String columnFamily, Object rowKey, Attribute column,
            Object columnObj) throws IOException
    {
        Put p = new Put(HBaseUtils.getBytes(rowKey));
        p.add(Bytes.toBytes(columnFamily), Bytes.toBytes(((AbstractAttribute) column).getJPAColumnName()),
                Bytes.toBytes(columnObj.toString()));

        htable.put(p);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.client.hbase.Writer#writeColumns(org.apache.hadoop.hbase.
     * client.HTable, java.lang.Object, java.util.Set, java.lang.Object)
     */
    @Override
    public void writeColumns(HTableInterface htable, Object rowKey, Map<String, Attribute> columns, Object entity,
            String columnFamilyName) throws IOException
    {
        Put p = new Put(HBaseUtils.getBytes(rowKey));

        boolean present = false;
        for (String columnName : columns.keySet())
        {
            Attribute column = columns.get(columnName);
            if (!column.isCollection() && !((SingularAttribute) column).isId())
            {
                String qualifier = columnName;
                try
                {
                    byte[] qualValInBytes = Bytes.toBytes(qualifier);
                    Object value = PropertyAccessorHelper.getObject(entity, (Field) column.getJavaMember());
                    if (value != null)
                    {
                        p.add(columnFamilyName.getBytes(), qualValInBytes, System.currentTimeMillis(),
                                HBaseUtils.getBytes(value));
                        present = true;
                    }
                }
                catch (PropertyAccessException e1)
                {
                    throw new IOException(e1);
                }
            }
        }
        if (present)
        {
            htable.put(p);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.client.hbase.Writer#writeColumns(org.apache.hadoop.hbase.
     * client.HTable, java.lang.Object, java.util.Map)
     */
    @Override
    public void writeColumns(HTableInterface htable, Object rowKey, Map<String, Object> columns, String columnFamilyName)
            throws IOException
    {

        Put p = new Put(HBaseUtils.getBytes(rowKey));

        boolean isPresent = false;
        for (String columnName : columns.keySet())
        {
            p.add(columnFamilyName.getBytes(), Bytes.toBytes(columnName), HBaseUtils.getBytes(columns.get(columnName)));
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
     * com.impetus.client.hbase.Writer#writeRelations(org.apache.hadoop.hbase
     * .client.HTable, java.lang.Object, boolean, java.util.List)
     */
    @Override
    public void writeRelations(HTableInterface htable, Object rowKey, boolean containsEmbeddedObjectsOnly,
            List<RelationHolder> relations, String columnFamilyName) throws IOException
    {
        Put p = new Put(HBaseUtils.getBytes(rowKey));

        boolean isPresent = false;
        for (RelationHolder r : relations)
        {
            if (r != null)
            {
                if (containsEmbeddedObjectsOnly)
                {
                    p.add(Bytes.toBytes(r.getRelationName()), Bytes.toBytes(r.getRelationName()),
                            PropertyAccessorHelper.getBytes(r.getRelationValue()));
                    isPresent = true;
                }
                else
                {
                    p.add(columnFamilyName.getBytes(), Bytes.toBytes(r.getRelationName()), System.currentTimeMillis(),
                            PropertyAccessorHelper.getBytes(r.getRelationValue()));
                    isPresent = true;
                }
            }
        }

        if (isPresent)
        {
            htable.put(p);
        }
    }

    // TODO: Scope of performance improvement in this code
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.client.hbase.Writer#writeForeignKeys(org.apache.hadoop.hbase
     * .client.HTable, java.lang.String, java.util.Map)
     */
    @Override
    public void writeForeignKeys(HTableInterface hTable, String rowKey, Map<String, Set<String>> foreignKeyMap)
            throws IOException
    {
        Put p = new Put(Bytes.toBytes(rowKey));

        // Checking if foreign key column family exists
        Get g = new Get(Bytes.toBytes(rowKey));
        Result r = hTable.get(g);

        boolean isPresent = false;

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
                isPresent = true;
            }
            else
            {
                p.add(Bytes.toBytes(Constants.FOREIGN_KEY_EMBEDDED_COLUMN_NAME), Bytes.toBytes(property),
                        Bytes.toBytes(existingForeignKey + Constants.FOREIGN_KEY_SEPARATOR + keys));
                isPresent = true;
            }
        }
        if (isPresent)
        {
            hTable.put(p);
        }
    }

    /**
     * Support for delete over HBase.
     * 
     * @param hTable
     *            the h table
     * @param rowKey
     *            the row key
     * @param columnFamily
     *            the column family
     */
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.client.hbase.Writer#delete(org.apache.hadoop.hbase.client
     * .HTable, java.lang.String, java.lang.String)
     */
    public void delete(HTableInterface hTable, Object rowKey, String columnFamily)
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

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.client.hbase.Writer#persistRows(java.util.Map)
     */
    @Override
    public void persistRows(Map<HTableInterface, List<HBaseDataWrapper>> rows) throws IOException
    {
        List<Put> dataSet = new ArrayList<Put>(rows.size());
        for (HTableInterface hTable : rows.keySet())
        {
            List<HBaseDataWrapper> row = rows.get(hTable);
            for (HBaseDataWrapper data : row)
            {
                dataSet.add(preparePut(data.getColumnFamily(), data.getRowKey(), data.getColumns(), data.getValues()));
            }
            hTable.put(dataSet);
            dataSet.clear();
        }
    }

    /**
     * Prepare put.
     * 
     * @param columnFamily
     *            the column family
     * @param rowKey
     *            the row key
     * @param columns
     *            the columns
     * @param values TODO
     * @param columnFamilyObj
     *            the column family obj
     * @return the put
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    private Put preparePut(String columnFamily, Object rowKey, Map<String, Attribute> columns, Map<String, Object> values)
            throws IOException
    {
        Put p = new Put(HBaseUtils.getBytes(rowKey));
        for (String columnName : columns.keySet())
        {
            Attribute column = columns.get(columnName);
            if (!column.isCollection() && !((SingularAttribute) column).isId())
            {
                String qualifier = columnName;
                try
                {
                    //Object o = PropertyAccessorHelper.getObject(columnFamilyObj, (Field) column.getJavaMember());
                    Object o = values.get(columnName);
                    byte[] value = HBaseUtils.getBytes(o);
                    if (value != null && columnFamily != null)
                    {
                        p.add(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier), value);
                    }
                }
                catch (PropertyAccessException e1)
                {
                    throw new IOException(e1);
                }
            }
        }
        return p;
    }
}