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
package com.impetus.client.cassandra.pelops;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.StringTokenizer;

import javax.persistence.PersistenceException;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.CounterColumn;
import org.apache.cassandra.thrift.CounterSuperColumn;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.SuperColumn;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.scale7.cassandra.pelops.Bytes;
import org.scale7.cassandra.pelops.Pelops;
import org.scale7.cassandra.pelops.Selector;

import com.impetus.client.cassandra.pelops.PelopsDataHandler.ThriftRow;
import com.impetus.kundera.Constants;
import com.impetus.kundera.KunderaException;
import com.impetus.kundera.cache.ElementCollectionCacheManager;
import com.impetus.kundera.client.EnhanceEntity;
import com.impetus.kundera.db.DataRow;
import com.impetus.kundera.db.SearchResult;
import com.impetus.kundera.metadata.MetadataUtils;
import com.impetus.kundera.metadata.model.EmbeddedColumn;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.Relation;
import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessor;
import com.impetus.kundera.property.PropertyAccessorFactory;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.query.QueryHandlerException;
import com.impetus.kundera.query.KunderaQuery.FilterClause;

/**
 * Provides Pelops utility methods for data held in Column family based stores.
 * 
 * @author amresh.singh
 */
final class PelopsDataHandler
{
    /** The timestamp. */
    private long timestamp = System.currentTimeMillis();

    /** The log. */
    private static Log log = LogFactory.getLog(PelopsDataHandler.class);

    /**
     * From thrift row.
     * 
     * @param selector
     *            the selector
     * @param clazz
     *            the clazz
     * @param m
     *            the m
     * @param rowKey
     *            the row key
     * @param relationNames
     *            the relation names
     * @param isWrapReq
     *            the is wrap req
     * @return the object
     * @throws Exception
     *             the exception
     */
    Object fromThriftRow(Selector selector, Class<?> clazz, EntityMetadata m, String rowKey,
            List<String> relationNames, boolean isWrapReq, ConsistencyLevel consistencyLevel) throws Exception
    {
        List<String> superColumnNames = m.getEmbeddedColumnFieldNames();
        Object e = null;

        if (!superColumnNames.isEmpty())
        {
            if (m.isCounterColumnType())
            {
                List<CounterSuperColumn> thriftCounterSuperColumns = new ArrayList<CounterSuperColumn>();
                List<ByteBuffer> rowKeys = new ArrayList<ByteBuffer>(1);
                rowKeys.add(ByteBufferUtil.bytes(rowKey));
                Map<ByteBuffer, List<ColumnOrSuperColumn>> thriftColumnOrSuperColumns = selector
                        .getColumnOrSuperColumnsFromRows(new ColumnParent(m.getTableName()), rowKeys,
                                Selector.newColumnsPredicateAll(true, 10000), consistencyLevel);
                getThriftCounterSuperColumn(thriftCounterSuperColumns, thriftColumnOrSuperColumns);
                if (thriftCounterSuperColumns != null)
                {
                    e = fromCounterSuperColumnThriftRow(clazz, m, new ThriftRow(rowKey, m.getTableName(), null, null,
                            null, thriftCounterSuperColumns), relationNames, isWrapReq);
                }
            }
            else
            {
                List<SuperColumn> thriftSuperColumns = selector.getSuperColumnsFromRow(m.getTableName(), rowKey,
                        Selector.newColumnsPredicateAll(true, 10000), consistencyLevel);
                e = fromSuperColumnThriftRow(clazz, m, new ThriftRow(rowKey, m.getTableName(), null,
                        thriftSuperColumns, null, null), relationNames, isWrapReq);
            }
        }
        else
        {
            List<ByteBuffer> rowKeys = new ArrayList<ByteBuffer>(1);

            ByteBuffer rKeyAsByte = ByteBufferUtil.bytes(rowKey);
            rowKeys.add(ByteBufferUtil.bytes(rowKey));

            Map<ByteBuffer, List<ColumnOrSuperColumn>> columnOrSuperColumnsFromRow = selector
                    .getColumnOrSuperColumnsFromRows(new ColumnParent(m.getTableName()), rowKeys,
                            Selector.newColumnsPredicateAll(true, 10000), consistencyLevel);

            List<ColumnOrSuperColumn> colList = columnOrSuperColumnsFromRow.get(rKeyAsByte);
            if (m.isCounterColumnType())
            {
                List<CounterColumn> thriftColumns = new ArrayList<CounterColumn>(colList.size());
                for (ColumnOrSuperColumn col : colList)
                {
                    if (col.super_column == null)
                    {
                        thriftColumns.add(col.getCounter_column());
                    }
                    else
                    {
                        thriftColumns.addAll(col.getCounter_super_column().getColumns());
                    }

                }

                e = fromCounterColumnThriftRow(clazz, m, new ThriftRow(rowKey, m.getTableName(), null, null,
                        thriftColumns, null), relationNames, isWrapReq);
            }
            else
            {
                List<Column> thriftColumns = new ArrayList<Column>(colList.size());
                for (ColumnOrSuperColumn col : colList)
                {
                    if (col.super_column == null)
                    {
                        thriftColumns.add(col.getColumn());
                    }
                    else
                    {
                        thriftColumns.addAll(col.getSuper_column().getColumns());
                    }

                }

                e = fromColumnThriftRow(clazz, m, new ThriftRow(rowKey, m.getTableName(), thriftColumns, null, null,
                        null), relationNames, isWrapReq);
            }
        }
        return e;
    }

    /**
     * @param thriftCounterSuperColumns
     * @param thriftColumnOrSuperColumns
     */
    private void getThriftCounterSuperColumn(List<CounterSuperColumn> thriftCounterSuperColumns,
            Map<ByteBuffer, List<ColumnOrSuperColumn>> thriftColumnOrSuperColumns)
    {
        for (Map.Entry<ByteBuffer, List<ColumnOrSuperColumn>> entry : thriftColumnOrSuperColumns.entrySet())
        {
            for (ColumnOrSuperColumn col : entry.getValue())
            {
                thriftCounterSuperColumns.add(col.getCounter_super_column());
            }
        }
    }

    /**
     * From thrift row.
     * 
     * @param selector
     *            the selector
     * @param clazz
     *            the clazz
     * @param m
     *            the m
     * @param relationNames
     *            the relation names
     * @param isWrapReq
     *            the is wrap req
     * @param rowIds
     *            the row ids
     * @return the list
     * @throws Exception
     *             the exception
     */
    List<Object> fromThriftRow(Selector selector, Class<?> clazz, EntityMetadata m, List<String> relationNames,
            boolean isWrapReq, ConsistencyLevel consistencyLevel, Object... rowIds) throws Exception
    {
        List<Object> entities = new ArrayList<Object>();
        if (rowIds != null)
        {
            for (Object rowKey : rowIds)
            {
                Object e = fromThriftRow(selector, clazz, m, rowKey.toString(), relationNames, isWrapReq,
                        consistencyLevel);
                if (e != null)
                {
                    entities.add(e);
                }
            }
        }
        return entities;
    }

    /**
     * From thrift row.
     * 
     * @param <E>
     *            the element type
     * @param clazz
     *            the clazz
     * @param m
     *            the m
     * @param tr
     *            the cr
     * @return the e
     * @throws Exception
     *             the exception
     */
    // TODO: this is a duplicate code snippet and we need to refactor this.
    <E> E fromThriftRow(Class<E> clazz, EntityMetadata m, DataRow<SuperColumn> tr) throws Exception
    {

        // Instantiate a new instance
        E e = null;

        // Set row-key. Note:
        // PropertyAccessorHelper.setId(e, m, tr.getId());

        // Get a name->field map for super-columns
        Map<String, Field> columnNameToFieldMap = new HashMap<String, Field>();
        Map<String, Field> superColumnNameToFieldMap = new HashMap<String, Field>();
        MetadataUtils.populateColumnAndSuperColumnMaps(m, columnNameToFieldMap, superColumnNameToFieldMap);

        Collection embeddedCollection = null;
        Field embeddedCollectionField = null;
        for (SuperColumn sc : tr.getColumns())
        {
            if (e == null)
            {
                // Instantiate a new instance
                e = clazz.newInstance();

                // Set row-key. Note:
                PropertyAccessorHelper.setId(e, m, tr.getId());
            }

            String scName = PropertyAccessorFactory.STRING.fromBytes(String.class, sc.getName());
            String scNamePrefix = null;

            if (scName.indexOf(Constants.EMBEDDED_COLUMN_NAME_DELIMITER) != -1)
            {
                scNamePrefix = MetadataUtils.getEmbeddedCollectionPrefix(scName);
                embeddedCollectionField = superColumnNameToFieldMap.get(scNamePrefix);
                embeddedCollection = MetadataUtils.getEmbeddedCollectionInstance(embeddedCollectionField);

                Object embeddedObject = populateEmbeddedObject(sc, m);
                embeddedCollection.add(embeddedObject);
                PropertyAccessorHelper.set(e, embeddedCollectionField, embeddedCollection);
            }
            else
            {
                boolean intoRelations = false;
                if (scName.equals(Constants.FOREIGN_KEY_EMBEDDED_COLUMN_NAME))
                {
                    intoRelations = true;
                }

                for (Column column : sc.getColumns())
                {
                    String name = PropertyAccessorFactory.STRING.fromBytes(String.class, column.getName());
                    byte[] value = column.getValue();

                    if (value == null)
                    {
                        continue;
                    }

                    if (intoRelations)
                    {
                        Relation relation = m.getRelation(name);

                        String foreignKeys = PropertyAccessorFactory.STRING.fromBytes(String.class, value);
                        Set<String> keys = MetadataUtils.deserializeKeys(foreignKeys);
                    }
                    else
                    {
                        // set value of the field in the bean
                        Field field = columnNameToFieldMap.get(name);
                        Object embeddedObject = PropertyAccessorHelper.getObject(e, scName);
                        PropertyAccessorHelper.set(embeddedObject, field, value);
                    }
                }
            }
        }
        return e;
    }

    /**
     * Fetches data held in Thrift row columns and populates to Entity objects.
     * 
     * @param clazz
     *            the clazz
     * @param m
     *            the m
     * @param thriftRow
     *            the cr
     * @param relationNames
     *            the relation names
     * @param isWrapperReq
     *            the is wrapper req
     * @return the e
     * @throws Exception
     *             the exception
     */
    Object fromColumnThriftRow(Class<?> clazz, EntityMetadata m, ThriftRow thriftRow, List<String> relationNames,
            boolean isWrapperReq) throws Exception
    {

        // Instantiate a new instance
        Object entity = null;
        Map<String, Object> relations = new HashMap<String, Object>();

        // Set row-key.
        // PropertyAccessorHelper.setId(entity, m, thriftRow.getId());
        // PropertyAccessorHelper.set(entity, m.getIdColumn().getField(),
        // thriftRow.getId());

        // Iterate through each column
        // if (m.isCounterColumnType())
        // {
        // for (CounterColumn c : thriftRow.getCounterColumns())
        // {
        // if (entity == null)
        // {
        // entity = clazz.newInstance();
        // // Set row-key
        // PropertyAccessorHelper.setId(entity, m, thriftRow.getId());
        // }
        //
        // String thriftColumnName =
        // PropertyAccessorFactory.STRING.fromBytes(String.class, c.getName());
        // Long thriftColumnValue = c.getValue();
        //
        // if (null == thriftColumnValue)
        // {
        // continue;
        // }
        //
        // // Check if this is a property, or a column representing foreign
        // // keys
        // com.impetus.kundera.metadata.model.Column column =
        // m.getColumn(thriftColumnName);
        // if (column != null)
        // {
        // try
        // {
        // if ((column.getField().getType().equals(Integer.class) ||
        // column.getField().getType()
        // .equals(int.class))
        // && thriftColumnValue != null)
        // {
        // PropertyAccessorHelper.set(entity, column.getField(),
        // thriftColumnValue.intValue());
        // }
        // else
        // {
        // PropertyAccessorHelper.set(entity, column.getField(),
        // thriftColumnValue);
        // }
        // }
        // catch (PropertyAccessException pae)
        // {
        // log.warn(pae.getMessage());
        // }
        // }
        // else
        // {
        // if (relationNames != null && !relationNames.isEmpty() &&
        // relationNames.contains(thriftColumnName))
        // {
        // // relations = new HashMap<String, Object>();
        // String value = thriftColumnValue.toString();
        // relations.put(thriftColumnName, value);
        // // prepare EnhanceEntity and return it
        // }
        // }
        // }
        // }
        // else
        // {
        for (Column c : thriftRow.getColumns())
        {
            if (entity == null)
            {
                entity = clazz.newInstance();
                // Set row-key
                PropertyAccessorHelper.setId(entity, m, thriftRow.getId());
            }

            String thriftColumnName = PropertyAccessorFactory.STRING.fromBytes(String.class, c.getName());
            byte[] thriftColumnValue = c.getValue();

            if (null == thriftColumnValue)
            {
                continue;
            }

            // Check if this is a property, or a column representing foreign
            // keys
            com.impetus.kundera.metadata.model.Column column = m.getColumn(thriftColumnName);
            if (column != null)
            {
                try
                {
                    PropertyAccessorHelper.set(entity, column.getField(), thriftColumnValue);
                }
                catch (PropertyAccessException pae)
                {
                    log.warn(pae.getMessage());
                }
            }
            else
            {
                if (relationNames != null && !relationNames.isEmpty() && relationNames.contains(thriftColumnName))
                {
                    // relations = new HashMap<String, Object>();
                    String value = PropertyAccessorFactory.STRING.fromBytes(String.class, thriftColumnValue);
                    relations.put(thriftColumnName, value);
                    // prepare EnhanceEntity and return it
                }
            }
        }

        return isWrapperReq && relations != null && !relations.isEmpty() ? new EnhanceEntity(entity, thriftRow.getId(),
                relations) : entity;
        // return new EnhanceEntity(entity, thriftRow.getId(), relations);
    }

    public Object fromCounterColumnThriftRow(Class<?> clazz, EntityMetadata m, ThriftRow thriftRow,
            List<String> relationNames, boolean isWrapperReq) throws Exception
    {
        // Instantiate a new instance
        Object entity = null;
        Map<String, Object> relations = new HashMap<String, Object>();

        // Set row-key.
        // PropertyAccessorHelper.setId(entity, m, thriftRow.getId());
        // PropertyAccessorHelper.set(entity, m.getIdColumn().getField(),
        // thriftRow.getId());

        // Iterate through each column
        for (CounterColumn c : thriftRow.getCounterColumns())
        {
            if (entity == null)
            {
                entity = clazz.newInstance();
                // Set row-key
                PropertyAccessorHelper.setId(entity, m, thriftRow.getId());
            }

            String thriftColumnName = PropertyAccessorFactory.STRING.fromBytes(String.class, c.getName());
            Long thriftColumnValue = c.getValue();

            if (null == thriftColumnValue)
            {
                continue;
            }

            // Check if this is a property, or a column representing foreign
            // keys
            com.impetus.kundera.metadata.model.Column column = m.getColumn(thriftColumnName);
            if (column != null)
            {
                try
                {
                    if ((column.getField().getType().equals(Integer.class) || column.getField().getType()
                            .equals(int.class))
                            && thriftColumnValue != null)
                    {
                        PropertyAccessorHelper.set(entity, column.getField(), thriftColumnValue.intValue());
                    }
                    else
                    {
                        PropertyAccessorHelper.set(entity, column.getField(), thriftColumnValue);
                    }
                }
                catch (PropertyAccessException pae)
                {
                    log.warn(pae.getMessage());
                }
            }
            else
            {
                if (relationNames != null && !relationNames.isEmpty() && relationNames.contains(thriftColumnName))
                {
                    // relations = new HashMap<String, Object>();
                    String value = thriftColumnValue.toString();
                    relations.put(thriftColumnName, value);
                    // prepare EnhanceEntity and return it
                }
            }
        }

        return isWrapperReq && relations != null && !relations.isEmpty() ? new EnhanceEntity(entity, thriftRow.getId(),
                relations) : entity;
    }

    /**
     * Fetches data held in Thrift row super columns and populates to Entity
     * objects.
     * 
     * @param clazz
     *            the clazz
     * @param m
     *            the m
     * @param tr
     *            the tr
     * @param relationNames
     *            the relation names
     * @param isWrapReq
     *            the is wrap req
     * @return the object
     * @throws Exception
     *             the exception
     */
    Object fromSuperColumnThriftRow(Class clazz, EntityMetadata m, ThriftRow tr, List<String> relationNames,
            boolean isWrapReq) throws Exception
    {

        // Instantiate a new instance
        Object entity = null;

        // Map to hold property-name=>foreign-entity relations
        Map<String, Set<String>> foreignKeysMap = new HashMap<String, Set<String>>();

        // Get a name->field map for super-columns
        Map<String, Field> columnNameToFieldMap = new HashMap<String, Field>();
        Map<String, Field> superColumnNameToFieldMap = new HashMap<String, Field>();
        MetadataUtils.populateColumnAndSuperColumnMaps(m, columnNameToFieldMap, superColumnNameToFieldMap);

        // Add all super columns to entity
        Collection embeddedCollection = null;
        Field embeddedCollectionField = null;
        Map<String, Object> relations = new HashMap<String, Object>();

        for (SuperColumn sc : tr.getSuperColumns())
        {
            if (entity == null)
            {
                entity = clazz.newInstance();
                // Set row-key
                PropertyAccessorHelper.setId(entity, m, tr.getId());
            }
            String scName = PropertyAccessorFactory.STRING.fromBytes(String.class, sc.getName());
            String scNamePrefix = null;

            // If this super column is variable in number (name#sequence
            // format)
            if (scName.indexOf(Constants.EMBEDDED_COLUMN_NAME_DELIMITER) != -1)
            {
                scNamePrefix = MetadataUtils.getEmbeddedCollectionPrefix(scName);
                embeddedCollectionField = superColumnNameToFieldMap.get(scNamePrefix);

                if (embeddedCollection == null)
                {
                    embeddedCollection = MetadataUtils.getEmbeddedCollectionInstance(embeddedCollectionField);
                }

                Object embeddedObject = MetadataUtils.getEmbeddedGenericObjectInstance(embeddedCollectionField);

                for (Column column : sc.getColumns())
                {
                    String name = PropertyAccessorFactory.STRING.fromBytes(String.class, column.getName());
                    byte[] value = column.getValue();
                    if (value == null)
                    {
                        continue;
                    }

                    Field columnField = columnNameToFieldMap.get(name);
                    if (columnField != null)
                    {
                        PropertyAccessorHelper.set(embeddedObject, columnField, value);
                    }
                    else if (relationNames != null && !relationNames.isEmpty() && relationNames.contains(name))
                    {
                        String valueAsStr = PropertyAccessorFactory.STRING.fromBytes(String.class, value);
                        relations.put(name, valueAsStr);
                    }
                }
                embeddedCollection.add(embeddedObject);

                // Add this embedded object to cache
                ElementCollectionCacheManager.getInstance().addElementCollectionCacheMapping(tr.getId(),
                        embeddedObject, scName);
            }
            else
            {
                // For embedded super columns, create embedded entities and
                // add them to parent entity
                Field superColumnField = superColumnNameToFieldMap.get(scName);
                Object superColumnObj = null;
                if (superColumnField != null
                        || (relationNames != null && !relationNames.isEmpty() && relationNames.contains(scName)))
                {

                    Class superColumnClass = superColumnField != null ? superColumnField.getType() : null;
                    superColumnObj = superColumnClass != null ? superColumnClass.newInstance() : null;
                    for (Column column : sc.getColumns())
                    {
                        String name = PropertyAccessorFactory.STRING.fromBytes(String.class, column.getName());
                        byte[] value = column.getValue();
                        Field columnField = columnNameToFieldMap.get(name);
                        if (columnField != null)
                        {
                            try
                            {
                                PropertyAccessorHelper.set(superColumnObj, columnField, value);
                            }
                            catch (PropertyAccessException e)
                            {
                                // This is an entity column to be retrieved
                                // in a
                                // super column family. It's stored as a
                                // super
                                // column that would
                                // have just one column with the same name
                                log.debug(e.getMessage()
                                        + ". Possible case of entity column in a super column family. Will be treated as a super column.");
                                com.impetus.kundera.metadata.model.Column col = m.getColumn(name);
                                if (col != null)
                                {
                                    superColumnObj = Bytes.toUTF8(value);
                                }
                            }

                        }
                        else
                        {
                            String valueAsStr = PropertyAccessorFactory.STRING.fromBytes(String.class, value);
                            relations.put(name, valueAsStr);
                        }
                    }
                }

                if (superColumnField != null)
                {
                    PropertyAccessorHelper.set(entity, superColumnField, superColumnObj);
                }
            }
        }

        // }

        if (embeddedCollection != null && !embeddedCollection.isEmpty())
        {
            PropertyAccessorHelper.set(entity, embeddedCollectionField, embeddedCollection);
        }

        // EnhancedEntity e = EntityResolver.getEnhancedEntity(entity,
        // tr.getId(), foreignKeysMap);
        return isWrapReq && relations != null && !relations.isEmpty() ? new EnhanceEntity(entity, tr.getId(), relations)
                : entity;
        // return new EnhanceEntity(entity, tr.getId(), relations);
    }

    public Object fromCounterSuperColumnThriftRow(Class clazz, EntityMetadata m, ThriftRow tr,
            List<String> relationNames, boolean isWrapReq) throws Exception
    {

        // Instantiate a new instance
        Object entity = null;

        // Map to hold property-name=>foreign-entity relations
        Map<String, Set<String>> foreignKeysMap = new HashMap<String, Set<String>>();

        // Get a name->field map for super-columns
        Map<String, Field> columnNameToFieldMap = new HashMap<String, Field>();
        Map<String, Field> superColumnNameToFieldMap = new HashMap<String, Field>();
        MetadataUtils.populateColumnAndSuperColumnMaps(m, columnNameToFieldMap, superColumnNameToFieldMap);

        // Add all super columns to entity
        Collection embeddedCollection = null;
        Field embeddedCollectionField = null;
        Map<String, Object> relations = new HashMap<String, Object>();

        for (CounterSuperColumn sc : tr.getCounterSuperColumns())
        {
            if (entity == null)
            {
                entity = clazz.newInstance();
                // Set row-key
                PropertyAccessorHelper.setId(entity, m, tr.getId());
            }
            String scName = PropertyAccessorFactory.STRING.fromBytes(String.class, sc.getName());
            String scNamePrefix = null;

            // If this super column is variable in number (name#sequence
            // format)
            if (scName.indexOf(Constants.EMBEDDED_COLUMN_NAME_DELIMITER) != -1)
            {
                scNamePrefix = MetadataUtils.getEmbeddedCollectionPrefix(scName);
                embeddedCollectionField = superColumnNameToFieldMap.get(scNamePrefix);

                if (embeddedCollection == null)
                {
                    embeddedCollection = MetadataUtils.getEmbeddedCollectionInstance(embeddedCollectionField);
                }

                Object embeddedObject = MetadataUtils.getEmbeddedGenericObjectInstance(embeddedCollectionField);

                for (CounterColumn column : sc.getColumns())
                {
                    String name = PropertyAccessorFactory.STRING.fromBytes(String.class, column.getName());
                    Long value = column.getValue();
                    if (value == null)
                    {
                        continue;
                    }

                    Field columnField = columnNameToFieldMap.get(name);
                    if (columnField != null)
                    {
                        if ((columnField.getType().equals(Integer.class) || columnField.getType().equals(int.class))
                                && value != null)
                        {
                            int colValue = value.intValue();
                            PropertyAccessorHelper.set(embeddedObject, columnField, colValue);
                        }
                        else
                        {
                            PropertyAccessorHelper.set(embeddedObject, columnField, value);
                        }
                        // PropertyAccessorHelper.set(embeddedObject,
                        // columnField, value);
                    }
                    else if (relationNames != null && !relationNames.isEmpty() && relationNames.contains(name))
                    {
                        String valueAsStr = value.toString();
                        relations.put(name, valueAsStr);
                    }
                }
                embeddedCollection.add(embeddedObject);

                // Add this embedded object to cache
                ElementCollectionCacheManager.getInstance().addElementCollectionCacheMapping(tr.getId(),
                        embeddedObject, scName);
            }
            else
            {
                // For embedded super columns, create embedded entities and
                // add them to parent entity
                Field superColumnField = superColumnNameToFieldMap.get(scName);
                Object superColumnObj = null;
                if (superColumnField != null
                        || (relationNames != null && !relationNames.isEmpty() && relationNames.contains(scName)))
                {
                    Class superColumnClass = superColumnField != null ? superColumnField.getType() : null;
                    superColumnObj = PropertyAccessorHelper.getObject(superColumnClass);

                    for (CounterColumn column : sc.getColumns())
                    {
                        String name = PropertyAccessorFactory.STRING.fromBytes(String.class, column.getName());
                        Long value = column.getValue();
                        Field columnField = columnNameToFieldMap.get(name);
                        if (columnField != null)
                        {
                            try
                            {
                                if ((columnField.getType().equals(Integer.class) || columnField.getType().equals(
                                        int.class))
                                        && value != null)
                                {
                                    int colValue = value.intValue();
                                    PropertyAccessorHelper.set(superColumnObj, columnField, colValue);
                                }
                                else
                                {
                                    PropertyAccessorHelper.set(superColumnObj, columnField, value);
                                }

                            }
                            catch (PropertyAccessException e)
                            {
                                // This is an entity column to be retrieved
                                // in a
                                // super column family. It's stored as a
                                // super
                                // column that would
                                // have just one column with the same name
                                log.debug(e.getMessage()
                                        + ". Possible case of entity column in a super column family. Will be treated as a super column.");
                                com.impetus.kundera.metadata.model.Column col = m.getColumn(name);
                                if (col != null)
                                {
                                    superColumnObj = value;
                                }
                            }

                        }
                        else
                        {
                            String valueAsStr = value.toString();
                            relations.put(name, valueAsStr);
                        }
                    }
                }

                if (superColumnField != null)
                {
                    PropertyAccessorHelper.set(entity, superColumnField, superColumnObj);
                }
            }
        }

        if (embeddedCollection != null && !embeddedCollection.isEmpty())
        {
            PropertyAccessorHelper.set(entity, embeddedCollectionField, embeddedCollection);
        }

        // EnhancedEntity e = EntityResolver.getEnhancedEntity(entity,
        // tr.getId(), foreignKeysMap);
        return isWrapReq && relations != null && !relations.isEmpty() ? new EnhanceEntity(entity, tr.getId(), relations)
                : entity;
    }

    /**
     * Populate embedded object.
     * 
     * @param sc
     *            the sc
     * @param m
     *            the m
     * @return the object
     * @throws Exception
     *             the exception
     */
    Object populateEmbeddedObject(SuperColumn sc, EntityMetadata m) throws Exception
    {
        Field embeddedCollectionField = null;
        Object embeddedObject = null;
        String scName = PropertyAccessorFactory.STRING.fromBytes(String.class, sc.getName());
        String scNamePrefix = null;

        // Get a name->field map for super-columns
        Map<String, Field> columnNameToFieldMap = new HashMap<String, Field>();
        Map<String, Field> superColumnNameToFieldMap = new HashMap<String, Field>();
        MetadataUtils.populateColumnAndSuperColumnMaps(m, columnNameToFieldMap, superColumnNameToFieldMap);

        // If this super column is variable in number (name#sequence format)
        if (scName.indexOf(Constants.EMBEDDED_COLUMN_NAME_DELIMITER) != -1)
        {
            StringTokenizer st = new StringTokenizer(scName, Constants.EMBEDDED_COLUMN_NAME_DELIMITER);
            if (st.hasMoreTokens())
            {
                scNamePrefix = st.nextToken();
            }

            embeddedCollectionField = superColumnNameToFieldMap.get(scNamePrefix);
            Class<?> embeddedClass = PropertyAccessorHelper.getGenericClass(embeddedCollectionField);

            // must have a default no-argument constructor
            try
            {
                embeddedClass.getConstructor();
            }
            catch (NoSuchMethodException nsme)
            {
                throw new PersistenceException(embeddedClass.getName()
                        + " is @Embeddable and must have a default no-argument constructor.");
            }
            embeddedObject = embeddedClass.newInstance();

            for (Column column : sc.getColumns())
            {
                String name = PropertyAccessorFactory.STRING.fromBytes(String.class, column.getName());
                byte[] value = column.getValue();
                if (value == null)
                {
                    continue;
                }
                Field columnField = columnNameToFieldMap.get(name);
                PropertyAccessorHelper.set(embeddedObject, columnField, value);
            }

        }
        else
        {
            Field superColumnField = superColumnNameToFieldMap.get(scName);
            Class superColumnClass = superColumnField.getType();
            embeddedObject = superColumnClass.newInstance();

            for (Column column : sc.getColumns())
            {
                String name = PropertyAccessorFactory.STRING.fromBytes(String.class, column.getName());
                byte[] value = column.getValue();

                if (value == null)
                {
                    continue;
                }
                // set value of the field in the bean
                Field columnField = columnNameToFieldMap.get(name);
                PropertyAccessorHelper.set(embeddedObject, columnField, value);

            }
        }
        return embeddedObject;
    }

    /**
     * Helper method to convert @Entity to ThriftRow.
     * 
     * @param e
     *            the e
     * @param id
     *            the id
     * @param m
     *            the m
     * @param columnFamily
     *            the colmun family
     * @return the base data accessor. thrift row
     * @throws Exception
     *             the exception
     */
    ThriftRow toThriftRow(Object e, String id, EntityMetadata m, String columnFamily) throws Exception
    {
        // timestamp to use in thrift column objects
        // long timestamp = System.currentTimeMillis();

        ThriftRow tr = new ThriftRow();

        tr.setColumnFamilyName(columnFamily); // column-family name
        tr.setId(id); // Id

        timestamp = getTimestamp();
        // Add super columns to thrift row
        if (m.isCounterColumnType())
        {
            addCounterSuperColumnsToThriftRow(timestamp, tr, m, e, id);
        }
        else
        {
            addSuperColumnsToThriftRow(timestamp, tr, m, e, id);
        }

        // Add columns to thrift row, only if there is no super column
        if (m.getEmbeddedColumnsAsList().isEmpty())
        {
            if (m.isCounterColumnType())
            {
                addCounterColumnsToThriftRow(timestamp, tr, m, e);
            }
            else
            {
                addColumnsToThriftRow(timestamp, tr, m, e);
            }

        }

        // Add relations entities as Foreign keys to a new super column created
        // internally
        // addRelationshipsToThriftRow(timestamp, tr, e, m);

        return tr;
    }

    List<ThriftRow> toIndexThriftRow(Object e, EntityMetadata m, String columnFamily)
    {
        List<ThriftRow> indexThriftRows = new ArrayList<PelopsDataHandler.ThriftRow>();

        byte[] value = PropertyAccessorHelper.get(e, m.getIdColumn().getField());

        for (EmbeddedColumn embeddedColumn : m.getEmbeddedColumnsAsList())
        {
            Object embeddedObject = PropertyAccessorHelper.getObject(e, embeddedColumn.getField());

            if (embeddedObject instanceof Collection)
            {
                ElementCollectionCacheManager ecCacheHandler = ElementCollectionCacheManager.getInstance();

                for (Object obj : (Collection) embeddedObject)
                {
                    for (com.impetus.kundera.metadata.model.Column column : embeddedColumn.getColumns())
                    {

                        // Column Value
                        String id = Bytes.toUTF8(value);
                        String superColumnName = ecCacheHandler.getElementCollectionObjectName(id, obj);
                        byte[] indexColumnValue = (id + Constants.INDEX_TABLE_EC_DELIMITER + superColumnName)
                                .getBytes();

                        ThriftRow tr = constructIndexTableThriftRow(columnFamily, embeddedColumn, obj, column,
                                indexColumnValue);

                        indexThriftRows.add(tr);
                    }
                }
            }
            else
            {
                for (com.impetus.kundera.metadata.model.Column column : embeddedColumn.getColumns())
                {

                    ThriftRow tr = constructIndexTableThriftRow(columnFamily, embeddedColumn, embeddedObject, column,
                            value);

                    indexThriftRows.add(tr);
                }
            }

        }

        return indexThriftRows;
    }

    /**
     * Constructs Thrift Tow (each record) for Index Table
     * 
     * @param columnFamily
     *            Column family Name for Index Table
     * @param embeddedColumn
     *            Instance of {@link EmbeddedColumn}
     * @param obj
     *            Embedded Object instance
     * @param column
     *            Instance of {@link Column}
     * @param indexColumnValue
     *            Name of Index Column
     * @return Instance of {@link ThriftRow}
     */
    private ThriftRow constructIndexTableThriftRow(String columnFamily, EmbeddedColumn embeddedColumn, Object obj,
            com.impetus.kundera.metadata.model.Column column, byte[] indexColumnValue)
    {
        // Column Name
        Field columnField = column.getField();
        byte[] indexColumnName = PropertyAccessorHelper.get(obj, columnField);

        // Construct Index Table Thrift Row
        ThriftRow tr = new ThriftRow();
        tr.setColumnFamilyName(columnFamily); // Index column-family name
        tr.setId(embeddedColumn.getField().getName() + Constants.INDEX_TABLE_ROW_KEY_DELIMITER
                + column.getField().getName()); // Id

        Column thriftColumn = new Column();
        thriftColumn.setName(indexColumnName);
        thriftColumn.setValue(indexColumnValue);
        thriftColumn.setTimestamp(timestamp);

        tr.addColumn(thriftColumn);
        return tr;
    }

    /**
     * @param columnFamilyName
     * @param m
     * @param filterClauseQueue
     * @return
     */
    public List<SearchResult> getSearchResults(String columnFamilyName, EntityMetadata m,
            Queue<FilterClause> filterClauseQueue, String persistenceUnit, ConsistencyLevel consistencyLevel)
    {
        Selector selector = Pelops.createSelector(PelopsUtils.generatePoolName(persistenceUnit));

        List<SearchResult> searchResults = new ArrayList<SearchResult>();

        for (FilterClause o : filterClauseQueue)
        {
            SearchResult searchResult = new SearchResult();

            FilterClause clause = ((FilterClause) o);
            String rowKey = clause.getProperty();
            String columnName = clause.getValue();
            String condition = clause.getCondition();
            log.debug("rowKey:" + rowKey + ";columnName:" + columnName + ";condition:" + condition);

            // TODO: Second check unnecessary but unavoidable as filter clause
            // property is incorrectly passed as column name

            // Search based on Primary key
            if (rowKey.equals(m.getIdColumn().getField().getName()) || rowKey.equals(m.getIdColumn().getName()))
            {

                searchResult.setPrimaryKey(columnName);

            }
            else
            {
                // Search results in the form of thrift columns
                List<Column> thriftColumns = new ArrayList<Column>();

                // EQUAL Operator
                if (condition.equals("="))
                {
                    Column thriftColumn = selector.getColumnFromRow(columnFamilyName, rowKey, columnName,
                            consistencyLevel);
                    thriftColumns.add(thriftColumn);
                }

                // LIKE operation
                else if (condition.equalsIgnoreCase("LIKE"))
                {

                    searchColumnsInRange(columnFamilyName, consistencyLevel, selector, rowKey, columnName,
                            thriftColumns, columnName.getBytes(), new byte[0]);
                }

                // Greater than operator
                else if (condition.equals(">"))
                {
                    searchColumnsInRange(columnFamilyName, consistencyLevel, selector, rowKey, columnName,
                            thriftColumns, columnName.getBytes(), new byte[0]);
                }

                // Less than Operator
                else if (condition.equals("<"))
                {
                    searchColumnsInRange(columnFamilyName, consistencyLevel, selector, rowKey, columnName,
                            thriftColumns, new byte[0], columnName.getBytes());
                }

                // Greater than-equals to operator
                else if (condition.equals(">="))
                {
                    searchColumnsInRange(columnFamilyName, consistencyLevel, selector, rowKey, columnName,
                            thriftColumns, columnName.getBytes(), new byte[0]);
                }

                // Less than equal to operator
                else if (condition.equals("<="))
                {
                    searchColumnsInRange(columnFamilyName, consistencyLevel, selector, rowKey, columnName,
                            thriftColumns, new byte[0], columnName.getBytes());
                }
                else
                {
                    throw new QueryHandlerException(condition
                            + " comparison operator not supported currently for Cassandra Inverted Index");
                }

                // Construct search results out of these thrift columns
                for (Column thriftColumn : thriftColumns)
                {
                    byte[] columnValue = thriftColumn.getValue();
                    String columnValueStr = Bytes.toUTF8(columnValue);

                    PropertyAccessor<?> accessor = PropertyAccessorFactory.getPropertyAccessor(m.getIdColumn()
                            .getField());
                    Object value = null;

                    if (columnValueStr.indexOf(Constants.INDEX_TABLE_EC_DELIMITER) > 0)
                    {
                        String pk = columnValueStr.substring(0,
                                columnValueStr.indexOf(Constants.INDEX_TABLE_EC_DELIMITER));
                        String ecName = columnValueStr.substring(
                                columnValueStr.indexOf(Constants.INDEX_TABLE_EC_DELIMITER)
                                        + Constants.INDEX_TABLE_EC_DELIMITER.length(), columnValueStr.length());

                        searchResult.setPrimaryKey(pk);
                        searchResult.setEmbeddedColumnName(rowKey.substring(0,
                                rowKey.indexOf(Constants.INDEX_TABLE_ROW_KEY_DELIMITER)));
                        searchResult.addEmbeddedColumnValue(ecName);

                    }
                    else
                    {
                        value = accessor.fromBytes(m.getIdColumn().getField().getClass(), columnValue);
                        searchResult.setPrimaryKey(value);
                    }
                    searchResults.add(searchResult);
                }

            }

        }
        return searchResults;
    }

    /**
     * Searches <code>searchString</code> into <code>columnFamilyName</code>
     * (usually a wide row column family) for a given <code>rowKey</code> from
     * start to finish columns. Adds matching thrift columns into
     * <code>thriftColumns</code>
     * 
     * @param columnFamilyName
     * @param consistencyLevel
     * @param selector
     * @param rowKey
     * @param searchString
     * @param thriftColumns
     */
    private void searchColumnsInRange(String columnFamilyName, ConsistencyLevel consistencyLevel, Selector selector,
            String rowKey, String searchString, List<Column> thriftColumns, byte[] start, byte[] finish)
    {
        SlicePredicate colPredicate = new SlicePredicate();
        SliceRange sliceRange = new SliceRange();
        sliceRange.setStart(start);
        sliceRange.setFinish(finish);
        colPredicate.setSlice_range(sliceRange);
        List<Column> allThriftColumns = selector.getColumnsFromRow(columnFamilyName, rowKey, colPredicate,
                consistencyLevel);

        for (Column column : allThriftColumns)
        {
            String colName = Bytes.toUTF8(column.getName());
            // String colValue = Bytes.toUTF8(column.getValue());
            if (colName.indexOf(searchString) >= 0)
            {
                thriftColumns.add(column);
            }
        }
    }

    private void addCounterColumnsToThriftRow(long timestamp2, ThriftRow tr, EntityMetadata m, Object e)
    {

        List<CounterColumn> counterColumns = new ArrayList<CounterColumn>();

        // Iterate through each column-meta and populate that with field values
        for (com.impetus.kundera.metadata.model.Column column : m.getColumnsAsList())
        {
            Field field = column.getField();
            if (field.getType().isAssignableFrom(Set.class) || field.getType().isAssignableFrom(Collection.class))
            {
            }
            else
            {
                String name = column.getName();
                try
                {
                    String value = PropertyAccessorHelper.getString(e, field);

                    if (value != null)
                    {
                        CounterColumn col = new CounterColumn();
                        col.setName(PropertyAccessorFactory.STRING.toBytes(name));
                        col.setValue(new Long(value));
                        counterColumns.add(col);
                    }
                    else
                    {
                        log.debug("skipping column :" + name + " as value is not provided!");
                    }
                }
                catch (PropertyAccessException exp)
                {
                    log.warn(exp.getMessage());
                }
            }
        }
        tr.setCounterColumns(counterColumns);
    }

    private void addCounterSuperColumnsToThriftRow(long timestamp2, ThriftRow tr, EntityMetadata m, Object e, String id)
    {

        // Iterate through Super columns
        for (EmbeddedColumn superColumn : m.getEmbeddedColumnsAsList())
        {
            Field superColumnField = superColumn.getField();
            Object superColumnObject = PropertyAccessorHelper.getObject(e, superColumnField);

            // If Embedded object is a Collection, there will be variable number
            // of super columns one for each object in collection.
            // Key for each super column will be of the format "<Embedded object
            // field name>#<Unique sequence number>

            // On the other hand, if embedded object is not a Collection, it
            // would simply be embedded as ONE super column.
            String superColumnName = null;
            if (superColumnObject == null)
            {
                continue;
            }
            if (superColumnObject instanceof Collection)
            {

                ElementCollectionCacheManager ecCacheHandler = ElementCollectionCacheManager.getInstance();

                // Check whether it's first time insert or updation
                if (ecCacheHandler.isCacheEmpty())
                { // First time insert
                    int count = 0;
                    for (Object obj : (Collection) superColumnObject)
                    {
                        superColumnName = superColumn.getName() + Constants.EMBEDDED_COLUMN_NAME_DELIMITER + count;
                        CounterSuperColumn thriftSuperColumn = buildThriftCounterSuperColumn(superColumnName,
                                superColumn, obj);
                        tr.addCounterSuperColumn(thriftSuperColumn);

                        count++;
                    }
                }
                else
                {
                    // Updation, Check whether this object is already in cache,
                    // which means we already have a super column
                    // Otherwise we need to generate a fresh embedded column
                    // name
                    int lastEmbeddedObjectCount = ecCacheHandler.getLastElementCollectionObjectCount(id);
                    for (Object obj : (Collection) superColumnObject)
                    {
                        superColumnName = ecCacheHandler.getElementCollectionObjectName(id, obj);
                        if (superColumnName == null)
                        { // Fresh row
                            superColumnName = superColumn.getName() + Constants.EMBEDDED_COLUMN_NAME_DELIMITER
                                    + (++lastEmbeddedObjectCount);
                        }
                        CounterSuperColumn thriftSuperColumn = buildThriftCounterSuperColumn(superColumnName,
                                superColumn, obj);
                        tr.addCounterSuperColumn(thriftSuperColumn);
                    }
                }

            }
            else
            {
                superColumnName = superColumn.getName();
                CounterSuperColumn thriftSuperColumn = buildThriftCounterSuperColumn(superColumnName, superColumn,
                        superColumnObject);
                tr.addCounterSuperColumn(thriftSuperColumn);
            }

        }
    }

    private CounterSuperColumn buildThriftCounterSuperColumn(String superColumnName, EmbeddedColumn superColumn,
            Object counterSuperColumnObject)
    {

        List<CounterColumn> thriftColumns = new ArrayList<CounterColumn>();
        for (com.impetus.kundera.metadata.model.Column column : superColumn.getColumns())
        {
            Field field = column.getField();
            String name = column.getName();
            String value = null;
            try
            {
                value = PropertyAccessorHelper.getString(counterSuperColumnObject, field);

            }
            catch (PropertyAccessException exp)
            {
                // This is an entity column to be persisted in a super column
                // family. It will be stored as a super column that would
                // have just one column with the same name
                log.info(exp.getMessage()
                        + ". Possible case of entity column in a super column family. Will be treated as a super column.");
                value = counterSuperColumnObject.toString();
            }
            if (null != value)
            {
                try
                {
                    CounterColumn thriftColumn = new CounterColumn();
                    thriftColumn.setName(PropertyAccessorFactory.STRING.toBytes(name));
                    thriftColumn.setValue(Long.parseLong(value));
                    thriftColumns.add(thriftColumn);
                }
                catch (NumberFormatException nfe)
                {
                    log.error("For counter column arguments should be numeric type, error caused by :"
                            + nfe.getMessage());
                    throw new KunderaException("For counter column,arguments should be numeric type", nfe);
                }
            }
        }
        CounterSuperColumn thriftSuperColumn = new CounterSuperColumn();
        thriftSuperColumn.setName(PropertyAccessorFactory.STRING.toBytes(superColumnName));
        thriftSuperColumn.setColumns(thriftColumns);

        return thriftSuperColumn;

    }

    /**
     * Adds the columns to thrift row.
     * 
     * @param timestamp
     *            the timestamp
     * @param tr
     *            the tr
     * @param m
     *            the m
     * @param e
     *            the e
     * @throws Exception
     *             the exception
     */
    private void addColumnsToThriftRow(long timestamp, ThriftRow tr, EntityMetadata m, Object e) throws Exception
    {
        List<Column> columns = new ArrayList<Column>();

        // Iterate through each column-meta and populate that with field values
        for (com.impetus.kundera.metadata.model.Column column : m.getColumnsAsList())
        {
            Field field = column.getField();
            if (field.getType().isAssignableFrom(Set.class) || field.getType().isAssignableFrom(Collection.class))
            {
            }
            else
            {
                String name = column.getName();
                try
                {
                    byte[] value = PropertyAccessorHelper.get(e, field);

                    if (value != null)
                    {
                        Column col = new Column();
                        col.setName(PropertyAccessorFactory.STRING.toBytes(name));
                        col.setValue(value);
                        col.setTimestamp(timestamp);
                        columns.add(col);
                    }
                    else
                    {
                        log.debug("skipping column :" + name + " as value is not provided!");
                    }
                }
                catch (PropertyAccessException exp)
                {
                    log.warn(exp.getMessage());
                }
            }
        }
        tr.setColumns(columns);

    }

    /**
     * Adds the super columns to thrift row.
     * 
     * @param timestamp
     *            the timestamp
     * @param client
     *            the client
     * @param tr
     *            the tr
     * @param m
     *            the m
     * @param e
     *            the e
     * @param id
     *            the id
     * @throws Exception
     *             the exception
     */
    private void addSuperColumnsToThriftRow(long timestamp, ThriftRow tr, EntityMetadata m, Object e, String id)
            throws Exception
    {
        // Iterate through Super columns
        for (EmbeddedColumn superColumn : m.getEmbeddedColumnsAsList())
        {
            Field superColumnField = superColumn.getField();
            Object superColumnObject = PropertyAccessorHelper.getObject(e, superColumnField);

            // If Embedded object is a Collection, there will be variable number
            // of super columns one for each object in collection.
            // Key for each super column will be of the format "<Embedded object
            // field name>#<Unique sequence number>

            // On the other hand, if embedded object is not a Collection, it
            // would simply be embedded as ONE super column.
            String superColumnName = null;
            if (superColumnObject == null)
            {
                continue;
            }
            if (superColumnObject instanceof Collection)
            {

                ElementCollectionCacheManager ecCacheHandler = ElementCollectionCacheManager.getInstance();

                // Check whether it's first time insert or updation
                if (ecCacheHandler.isCacheEmpty())
                { // First time insert
                    int count = 0;
                    for (Object obj : (Collection) superColumnObject)
                    {
                        superColumnName = superColumn.getName() + Constants.EMBEDDED_COLUMN_NAME_DELIMITER + count;
                        SuperColumn thriftSuperColumn = buildThriftSuperColumn(superColumnName, timestamp, superColumn,
                                obj);
                        tr.addSuperColumn(thriftSuperColumn);
                        ecCacheHandler.addElementCollectionCacheMapping(id, obj, superColumnName);

                        count++;
                    }
                }
                else
                {
                    // Updation, Check whether this object is already in cache,
                    // which means we already have a super column
                    // Otherwise we need to generate a fresh embedded column
                    // name
                    int lastEmbeddedObjectCount = ecCacheHandler.getLastElementCollectionObjectCount(id);
                    for (Object obj : (Collection) superColumnObject)
                    {
                        superColumnName = ecCacheHandler.getElementCollectionObjectName(id, obj);
                        if (superColumnName == null)
                        { // Fresh row
                            superColumnName = superColumn.getName() + Constants.EMBEDDED_COLUMN_NAME_DELIMITER
                                    + (++lastEmbeddedObjectCount);
                        }
                        SuperColumn thriftSuperColumn = buildThriftSuperColumn(superColumnName, timestamp, superColumn,
                                obj);
                        tr.addSuperColumn(thriftSuperColumn);
                        ecCacheHandler.addElementCollectionCacheMapping(id, obj, superColumnName);
                    }

                    // TODO: Why are we not clearing EC Cache as in
                    // HBaseDataHandler
                    // Clear embedded collection cache for GC
                    // ecCacheHandler.clearCache();
                }

            }
            else
            {
                superColumnName = superColumn.getName();
                SuperColumn thriftSuperColumn = buildThriftSuperColumn(superColumnName, timestamp, superColumn,
                        superColumnObject);
                tr.addSuperColumn(thriftSuperColumn);
            }

        }

    }

    /**
     * Builds the thrift super column.
     * 
     * @param superColumnName
     *            the super column name
     * @param timestamp
     *            the timestamp
     * @param superColumn
     *            the super column
     * @param superColumnObject
     *            the super column object
     * @return the super column
     * @throws PropertyAccessException
     *             the property access exception
     */
    private SuperColumn buildThriftSuperColumn(String superColumnName, long timestamp, EmbeddedColumn superColumn,
            Object superColumnObject) throws PropertyAccessException
    {
        List<Column> thriftColumns = new ArrayList<Column>();
        for (com.impetus.kundera.metadata.model.Column column : superColumn.getColumns())
        {
            Field field = column.getField();
            String name = column.getName();
            byte[] value = null;
            try
            {
                value = PropertyAccessorHelper.get(superColumnObject, field);

            }
            catch (PropertyAccessException exp)
            {
                // This is an entity column to be persisted in a super column
                // family. It will be stored as a super column that would
                // have just one column with the same name
                log.info(exp.getMessage()
                        + ". Possible case of entity column in a super column family. Will be treated as a super column.");
                value = superColumnObject.toString().getBytes();
            }
            if (null != value)
            {
                Column thriftColumn = new Column();
                thriftColumn.setName(PropertyAccessorFactory.STRING.toBytes(name));
                thriftColumn.setValue(value);
                thriftColumn.setTimestamp(timestamp);
                thriftColumns.add(thriftColumn);
            }
        }
        SuperColumn thriftSuperColumn = new SuperColumn();
        thriftSuperColumn.setName(PropertyAccessorFactory.STRING.toBytes(superColumnName));
        thriftSuperColumn.setColumns(thriftColumns);

        return thriftSuperColumn;
    }

    /**
     * Gets the foreign keys from join table.
     * 
     * @param <E>
     *            the element type
     * @param inverseJoinColumnName
     *            the inverse join column name
     * @param columns
     *            the columns
     * @return the foreign keys from join table
     */
    <E> List<E> getForeignKeysFromJoinTable(String inverseJoinColumnName, List<Column> columns)
    {
        List<E> foreignKeys = new ArrayList<E>();

        if (columns == null || columns.isEmpty())
        {
            return foreignKeys;
        }

        for (Column c : columns)
        {
            try
            {
                // Thrift Column name
                String thriftColumnName = PropertyAccessorFactory.STRING.fromBytes(String.class, c.getName());

                // Thrift Column Value
                byte[] thriftColumnValue = c.getValue();
                if (null == thriftColumnValue)
                {
                    continue;
                }

                if (thriftColumnName != null && thriftColumnName.startsWith(inverseJoinColumnName))
                {
                    String val = PropertyAccessorFactory.STRING.fromBytes(String.class, thriftColumnValue);
                    foreignKeys.add((E) val);
                }
            }
            catch (PropertyAccessException e)
            {
                continue;
            }

        }
        return foreignKeys;
    }

    /**
     * Utility class that represents a row in Cassandra DB.
     * 
     * @author animesh.kumar
     */
    class ThriftRow
    {

        /** Id of the row. */
        private String id;

        /** name of the family. */
        private String columnFamilyName;

        /** list of thrift columns from the row. */
        private List<Column> columns;

        /** list of thrift super columns columns from the row. */
        private List<SuperColumn> superColumns;

        /** list of thrift counter columns from the row. */
        private List<CounterColumn> counterColumns;

        /** list of thrift counter super columns columns from the row. */
        private List<CounterSuperColumn> counterSuperColumns;

        /**
         * default constructor.
         */
        ThriftRow()
        {
            columns = new ArrayList<Column>();
            superColumns = new ArrayList<SuperColumn>();
            counterColumns = new ArrayList<CounterColumn>();
            counterSuperColumns = new ArrayList<CounterSuperColumn>();
        }

        /**
         * The Constructor.
         * 
         * @param id
         *            the id
         * @param columnFamilyName
         *            the column family name
         * @param columns
         *            the columns
         * @param superColumns
         *            the super columns
         */
        ThriftRow(String id, String columnFamilyName, List<Column> columns, List<SuperColumn> superColumns,
                List<CounterColumn> counterColumns, List<CounterSuperColumn> counterSuperColumns)
        {
            this.id = id;
            this.columnFamilyName = columnFamilyName;
            if (columns != null)
            {
                this.columns = columns;
            }

            if (superColumns != null)
            {
                this.superColumns = superColumns;
            }
            if (counterColumns != null)
            {
                this.counterColumns = counterColumns;
            }

            if (counterSuperColumns != null)
            {
                this.counterSuperColumns = counterSuperColumns;
            }
        }

        /**
         * Gets the id.
         * 
         * @return the id
         */
        String getId()
        {
            return id;
        }

        /**
         * Sets the id.
         * 
         * @param id
         *            the key to set
         */
        void setId(String id)
        {
            this.id = id;
        }

        /**
         * Gets the column family name.
         * 
         * @return the columnFamilyName
         */
        String getColumnFamilyName()
        {
            return columnFamilyName;
        }

        /**
         * Sets the column family name.
         * 
         * @param columnFamilyName
         *            the columnFamilyName to set
         */
        void setColumnFamilyName(String columnFamilyName)
        {
            this.columnFamilyName = columnFamilyName;
        }

        /**
         * Gets the columns.
         * 
         * @return the columns
         */
        List<Column> getColumns()
        {
            return columns;
        }

        /**
         * Sets the columns.
         * 
         * @param columns
         *            the columns to set
         */
        void setColumns(List<Column> columns)
        {
            this.columns = columns;
        }

        /**
         * Adds the column.
         * 
         * @param column
         *            the column
         */
        void addColumn(Column column)
        {
            columns.add(column);
        }

        /**
         * Gets the super columns.
         * 
         * @return the superColumns
         */
        List<SuperColumn> getSuperColumns()
        {
            return superColumns;
        }

        /**
         * Sets the super columns.
         * 
         * @param superColumns
         *            the superColumns to set
         */
        void setSuperColumns(List<SuperColumn> superColumns)
        {
            this.superColumns = superColumns;
        }

        /**
         * Adds the super column.
         * 
         * @param superColumn
         *            the super column
         */
        void addSuperColumn(SuperColumn superColumn)
        {
            this.superColumns.add(superColumn);
        }

        /**
         * @return the counterColumns
         */
        public List<CounterColumn> getCounterColumns()
        {
            return counterColumns;
        }

        /**
         * @param counterColumns
         *            the counterColumns to set
         */
        public void setCounterColumns(List<CounterColumn> counterColumns)
        {
            this.counterColumns = counterColumns;
        }

        /**
         * Adds the counter column.
         * 
         * @param counter
         *            column the column
         */
        void addCounterColumn(CounterColumn column)
        {
            counterColumns.add(column);
        }

        /**
         * @return the counterSuperColumns
         */
        public List<CounterSuperColumn> getCounterSuperColumns()
        {
            return counterSuperColumns;
        }

        /**
         * @param counterSuperColumns
         *            the counterSuperColumns to set
         */
        public void setCounterSuperColumns(List<CounterSuperColumn> counterSuperColumns)
        {
            this.counterSuperColumns = counterSuperColumns;
        }

        /**
         * Adds the counter super column.
         * 
         * @param countersuperColumn
         *            the super column
         */
        void addCounterSuperColumn(CounterSuperColumn superColumn)
        {
            this.counterSuperColumns.add(superColumn);
        }
    }

    /**
     * Gets the timestamp.
     * 
     * @return the timestamp
     */
    long getTimestamp()
    {
        return System.currentTimeMillis();
    }

}