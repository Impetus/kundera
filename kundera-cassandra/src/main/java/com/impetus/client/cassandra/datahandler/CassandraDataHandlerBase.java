/**
 * Copyright 2012 Impetus Infotech.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.impetus.client.cassandra.datahandler;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import javax.persistence.PersistenceException;
import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.Attribute.PersistentAttributeType;
import javax.persistence.metamodel.EmbeddableType;
import javax.persistence.metamodel.EntityType;
import javax.persistence.metamodel.PluralAttribute;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.CounterColumn;
import org.apache.cassandra.thrift.CounterSuperColumn;
import org.apache.cassandra.thrift.SuperColumn;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.client.cassandra.common.CassandraUtilities;
import com.impetus.client.cassandra.thrift.ThriftDataResultHelper;
import com.impetus.client.cassandra.thrift.ThriftRow;
import com.impetus.kundera.Constants;
import com.impetus.kundera.KunderaException;
import com.impetus.kundera.cache.ElementCollectionCacheManager;
import com.impetus.kundera.client.EnhanceEntity;
import com.impetus.kundera.db.DataRow;
import com.impetus.kundera.metadata.MetadataUtils;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.EntityMetadata.Type;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.Relation;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessorFactory;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.property.accessor.LongAccessor;

/**
 * Base class for all Cassandra Data Handlers.
 *
 * @author amresh.singh
 */
public abstract class CassandraDataHandlerBase
{

    /** The log. */
    private static Log log = LogFactory.getLog(CassandraDataHandlerBase.class);

    /** The thrift translator. */
    protected ThriftDataResultHelper thriftTranslator = new ThriftDataResultHelper();

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

    public <E> E fromThriftRow(Class<E> clazz, EntityMetadata m, DataRow<SuperColumn> tr) throws Exception
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
                    if (column != null)
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
        }
        return e;
    }

    /**
     * From thrift row.
     *
     * @param clazz the clazz
     * @param m the m
     * @param relationNames the relation names
     * @param isWrapReq the is wrap req
     * @param consistencyLevel the consistency level
     * @param rowIds the row ids
     * @return the list
     * @throws Exception the exception
     */
    public List<Object> fromThriftRow(Class<?> clazz, EntityMetadata m, List<String> relationNames, boolean isWrapReq,
            ConsistencyLevel consistencyLevel, Object... rowIds) throws Exception
    {
        List<Object> entities = new ArrayList<Object>();
        if (rowIds != null)
        {
            for (Object rowKey : rowIds)
            {
                Object e = fromThriftRow(clazz, m, rowKey.toString(), relationNames, isWrapReq, consistencyLevel);
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
     * @param clazz the clazz
     * @param m the m
     * @param rowKey the row key
     * @param relationNames the relation names
     * @param isWrapReq the is wrap req
     * @param consistencyLevel the consistency level
     * @return the object
     * @throws Exception the exception
     */
    public abstract Object fromThriftRow(Class<?> clazz, EntityMetadata m, String rowKey, List<String> relationNames,
            boolean isWrapReq, ConsistencyLevel consistencyLevel) throws Exception;

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
    private Object populateEmbeddedObject(SuperColumn sc, EntityMetadata m) throws Exception
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
    public ThriftRow toThriftRow(Object e, String id, EntityMetadata m, String columnFamily) throws Exception
    {
        // timestamp to use in thrift column objects
        // long timestamp = System.currentTimeMillis();

        ThriftRow tr = new ThriftRow();

        tr.setColumnFamilyName(columnFamily); // column-family name
        tr.setId(id); // Id

        long timestamp = System.currentTimeMillis();
        // Add super columns to thrift row
        onColumnOrSuperColumnThriftRow(timestamp, tr, m, e, id);
        return tr;
    }

    /**
     * To index thrift row.
     *
     * @param e the e
     * @param m the m
     * @param columnFamily the column family
     * @return the list
     */
    public List<ThriftRow> toIndexThriftRow(Object e, EntityMetadata m, String columnFamily)
    {
        List<ThriftRow> indexThriftRows = new ArrayList<ThriftRow>();

        // byte[] value = PropertyAccessorHelper.get(e,
        // m.getIdColumn().getField());
        byte[] value = PropertyAccessorHelper.get(e, (Field) m.getIdAttribute().getJavaMember());

        MetamodelImpl metaModel = (MetamodelImpl) KunderaMetadata.INSTANCE.getApplicationMetadata().getMetamodel(
                m.getPersistenceUnit());

        Map<String, EmbeddableType> embeddables = metaModel.getEmbeddables(m.getEntityClazz());

        EntityType entityType = metaModel.entity(m.getEntityClazz());

        for (String key : embeddables.keySet())
        {
            EmbeddableType embeddedColumn = embeddables.get(key);

            Object embeddedObject = PropertyAccessorHelper.getObject(e, (Field) entityType.getAttribute(key)
                    .getJavaMember());
            if (embeddedObject == null)
            {
                continue;
            }

            if (embeddedObject instanceof Collection)
            {
                ElementCollectionCacheManager ecCacheHandler = ElementCollectionCacheManager.getInstance();

                for (Object obj : (Collection) embeddedObject)
                {
                    // for (com.impetus.kundera.metadata.model.Column column :
                    // embeddedColumn.getColumns())
                    for (Object column : embeddedColumn.getAttributes())
                    {

                        // Column Value
                        String id = CassandraUtilities.toUTF8(value);

                        String superColumnName = ecCacheHandler.getElementCollectionObjectName(id, obj);
                        byte[] indexColumnValue = (id + Constants.INDEX_TABLE_EC_DELIMITER + superColumnName)
                                .getBytes();

                        ThriftRow tr = constructIndexTableThriftRow(columnFamily, key, obj, (Attribute) column,
                                indexColumnValue);
                        if (tr != null)
                        {
                            indexThriftRows.add(tr);
                        }

                    }
                }
            }
            else
            {
                // for (com.impetus.kundera.metadata.model.Column column :
                // embeddedColumn.getColumns())
                // {
                for (Object column : embeddedColumn.getAttributes())
                {
                    // ThriftRow tr = constructIndexTableThriftRow(columnFamily,
                    // embeddedColumn, embeddedObject, column,
                    // value);
                    ThriftRow tr = constructIndexTableThriftRow(columnFamily, key, embeddedObject, (Attribute) column,
                            value);
                    if (tr != null)
                    {
                        indexThriftRows.add(tr);
                    }

                }
            }

        }

        return indexThriftRows;
    }

    /**
     * Constructs Thrift Tow (each record) for Index Table.
     *
     * @param columnFamily Column family Name for Index Table
     * @param embeddedFieldName the embedded field name
     * @param obj Embedded Object instance
     * @param column Instance of {@link Column}
     * @param indexColumnValue Name of Index Column
     * @return Instance of {@link ThriftRow}
     */
    private ThriftRow constructIndexTableThriftRow(String columnFamily, String embeddedFieldName, Object obj,
            Attribute column, byte[] indexColumnValue)
    {
        // Column Name
        Field columnField = (Field) column.getJavaMember();
        byte[] indexColumnName = PropertyAccessorHelper.get(obj, columnField);

        ThriftRow tr = null;
        if (indexColumnName != null)
        {
            // Construct Index Table Thrift Row
            tr = new ThriftRow();
            tr.setColumnFamilyName(columnFamily); // Index column-family name
            tr.setId(embeddedFieldName + Constants.INDEX_TABLE_ROW_KEY_DELIMITER + column.getName()); // Id

            Column thriftColumn = new Column();
            thriftColumn.setName(indexColumnName);
            thriftColumn.setValue(indexColumnValue);
            thriftColumn.setTimestamp(System.currentTimeMillis());

            tr.addColumn(thriftColumn);
        }
        return tr;
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
    public <E> List<E> getForeignKeysFromJoinTable(String inverseJoinColumnName, List<Column> columns)
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
     * Populate entity.
     *
     * @param tr the tr
     * @param m the m
     * @param relationNames the relation names
     * @param isWrapReq the is wrap req
     * @return the object
     */
    public Object populateEntity(ThriftRow tr, EntityMetadata m, List<String> relationNames, boolean isWrapReq)
    {
        Map<String, Object> relations = new HashMap<String, Object>();
        Object entity = null;
        try
        {
            // entity =m.getEntityClazz().newInstance();

            EntityType entityType = KunderaMetadata.INSTANCE.getApplicationMetadata()
                    .getMetamodel(m.getPersistenceUnit()).entity(m.getEntityClazz());

            for (Column column : tr.getColumns())
            {
                entity = initialize(tr, m, entity);
                onColumn(column, m, entity, entityType, relationNames, isWrapReq, relations);
            }

            // Add all super columns to entity
            Collection embeddedCollection = null;
            Field embeddedCollectionField = null;

            boolean mappingProcessed = false;
            Map<String, Field> columnNameToFieldMap = new HashMap<String, Field>();
            Map<String, Field> superColumnNameToFieldMap = new HashMap<String, Field>();

            for (SuperColumn superColumn : tr.getSuperColumns())
            {
                entity = initialize(tr, m, entity);

                String scName = PropertyAccessorFactory.STRING.fromBytes(String.class, superColumn.getName());
                String scNamePrefix = null;

                // Map to hold property-name=>foreign-entity relations
                Map<String, Set<String>> foreignKeysMap = new HashMap<String, Set<String>>();

                // Get a name->field map for super-columns
                if (!mappingProcessed)
                {
                    MetadataUtils.populateColumnAndSuperColumnMaps(m, columnNameToFieldMap, superColumnNameToFieldMap);
                }

                if (scName.indexOf(Constants.EMBEDDED_COLUMN_NAME_DELIMITER) != -1)
                {
                    scNamePrefix = MetadataUtils.getEmbeddedCollectionPrefix(scName);
                    embeddedCollectionField = superColumnNameToFieldMap.get(scNamePrefix);

                    if (embeddedCollection == null)
                    {
                        embeddedCollection = MetadataUtils.getEmbeddedCollectionInstance(embeddedCollectionField);
                    }

                    Object embeddedObject = MetadataUtils.getEmbeddedGenericObjectInstance(embeddedCollectionField);

                    scrollOverSuperColumn(m, relationNames, isWrapReq, relations, entityType, superColumn,
                            embeddedObject);
                    embeddedCollection.add(embeddedObject);

                    // Add this embedded object to cache
                    ElementCollectionCacheManager.getInstance().addElementCollectionCacheMapping(tr.getId(),
                            embeddedObject, scName);
                }
                else
                {
                    if (superColumnNameToFieldMap.containsKey(scName))
                    {
                        Field field = superColumnNameToFieldMap.get(scName);
                        Object embeddedObj = field.getType().newInstance();
                        // column
                        scrollOverSuperColumn(m, relationNames, isWrapReq, relations, entityType, superColumn,
                                embeddedObj, columnNameToFieldMap);
                        PropertyAccessorHelper.set(entity, field, embeddedObj);
                    }
                    else
                    {
                        scrollOverSuperColumn(m, relationNames, isWrapReq, relations, entityType, superColumn, entity);
                    }

                    scrollOverSuperColumn(m, relationNames, isWrapReq, relations, entityType, superColumn, entity);
                }
            }

            mappingProcessed = false;

            for (CounterColumn counterColumn : tr.getCounterColumns())
            {
                entity = initialize(tr, m, entity);
                onCounterColumn(counterColumn, m, entity, entityType, relationNames, isWrapReq, relations);
            }

            for (CounterSuperColumn counterSuperColumn : tr.getCounterSuperColumns())
            {
                entity = initialize(tr, m, entity);
                String scName = PropertyAccessorFactory.STRING.fromBytes(String.class, counterSuperColumn.getName());
                String scNamePrefix = null;

                // Map to hold property-name=>foreign-entity relations
                Map<String, Set<String>> foreignKeysMap = new HashMap<String, Set<String>>();

                // Get a name->field map for super-columns
                // Get a name->field map for super-columns
                if (!mappingProcessed)
                {
                    MetadataUtils.populateColumnAndSuperColumnMaps(m, columnNameToFieldMap, superColumnNameToFieldMap);
                    mappingProcessed = true;
                }

                if (scName.indexOf(Constants.EMBEDDED_COLUMN_NAME_DELIMITER) != -1)
                {
                    scNamePrefix = MetadataUtils.getEmbeddedCollectionPrefix(scName);
                    embeddedCollectionField = superColumnNameToFieldMap.get(scNamePrefix);

                    if (embeddedCollection == null)
                    {
                        embeddedCollection = MetadataUtils.getEmbeddedCollectionInstance(embeddedCollectionField);
                    }

                    Object embeddedObject = MetadataUtils.getEmbeddedGenericObjectInstance(embeddedCollectionField);

                    scrollOverCounterSuperColumn(m, relationNames, isWrapReq, relations, entityType,
                            counterSuperColumn, embeddedObject, columnNameToFieldMap);
                    embeddedCollection.add(embeddedObject);

                    // Add this embedded object to cache
                    ElementCollectionCacheManager.getInstance().addElementCollectionCacheMapping(tr.getId(),
                            embeddedObject, scName);
                }
                else
                {
                    if (superColumnNameToFieldMap.containsKey(scName))
                    {
                        Field field = superColumnNameToFieldMap.get(scName);
                        Object embeddedObj = field.getType().newInstance();
                        // column
                        scrollOverCounterSuperColumn(m, relationNames, isWrapReq, relations, entityType,
                                counterSuperColumn, embeddedObj, columnNameToFieldMap);
                        PropertyAccessorHelper.set(entity, field, embeddedObj);
                    }
                    else
                    {
                        scrollOverCounterSuperColumn(m, relationNames, isWrapReq, relations, entityType,
                                counterSuperColumn, entity);
                    }
                }
            }

            if (embeddedCollection != null && !embeddedCollection.isEmpty())
            {
                PropertyAccessorHelper.set(entity, embeddedCollectionField, embeddedCollection);
            }

        }
        catch (InstantiationException iex)
        {
            log.error("Eror while retrieving data, Caused by:" + iex.getMessage());
            throw new PersistenceException(iex);
        }
        catch (IllegalAccessException iaex)
        {
            log.error("Eror while retrieving data, Caused by:" + iaex.getMessage());
            throw new PersistenceException(iaex);
        }

        return isWrapReq && relations != null && !relations.isEmpty() ? new EnhanceEntity(entity, tr.getId(), relations)
                : entity;

    }

    /**
     * Initialize.
     *
     * @param tr the tr
     * @param m the m
     * @param entity the entity
     * @return the object
     * @throws InstantiationException the instantiation exception
     * @throws IllegalAccessException the illegal access exception
     */
    private Object initialize(ThriftRow tr, EntityMetadata m, Object entity) throws InstantiationException,
            IllegalAccessException
    {
        if (entity == null)
        {
            entity = m.getEntityClazz().newInstance();
            PropertyAccessorHelper.setId(entity, m, tr.getId());
        }

        return entity;
    }

    /**
     * Scroll over super column.
     *
     * @param m the m
     * @param relationNames the relation names
     * @param isWrapReq the is wrap req
     * @param relations the relations
     * @param entityType the entity type
     * @param superColumn the super column
     * @param embeddedObject the embedded object
     */
    private void scrollOverSuperColumn(EntityMetadata m, List<String> relationNames, boolean isWrapReq,
            Map<String, Object> relations, EntityType entityType, SuperColumn superColumn, Object embeddedObject)
    {
        for (Column column : superColumn.getColumns())
        {
            onColumn(column, m, embeddedObject, entityType, relationNames, isWrapReq, relations);
        }
    }

    /**
     * Scroll over counter super column.
     *
     * @param m the m
     * @param relationNames the relation names
     * @param isWrapReq the is wrap req
     * @param relations the relations
     * @param entityType the entity type
     * @param superColumn the super column
     * @param embeddedObject the embedded object
     */
    private void scrollOverCounterSuperColumn(EntityMetadata m, List<String> relationNames, boolean isWrapReq,
            Map<String, Object> relations, EntityType entityType, CounterSuperColumn superColumn, Object embeddedObject)
    {
        for (CounterColumn column : superColumn.getColumns())
        {
            onCounterColumn(column, m, embeddedObject, entityType, relationNames, isWrapReq, relations);
        }
    }

    /**
     * Scroll over counter super column.
     *
     * @param m the m
     * @param relationNames the relation names
     * @param isWrapReq the is wrap req
     * @param relations the relations
     * @param entityType the entity type
     * @param superColumn the super column
     * @param embeddedObject the embedded object
     * @param superColumnFieldMap the super column field map
     */
    private void scrollOverCounterSuperColumn(EntityMetadata m, List<String> relationNames, boolean isWrapReq,
            Map<String, Object> relations, EntityType entityType, CounterSuperColumn superColumn,
            Object embeddedObject, Map<String, Field> superColumnFieldMap)
    {
        for (CounterColumn column : superColumn.getColumns())
        {
            String thriftColumnName = PropertyAccessorFactory.STRING.fromBytes(String.class, column.getName());
            String thriftColumnValue = new Long(column.getValue()).toString();
            PropertyAccessorHelper.set(embeddedObject, superColumnFieldMap.get(thriftColumnName), thriftColumnValue);
        }
    }

    /**
     * Scroll over super column.
     *
     * @param m the m
     * @param relationNames the relation names
     * @param isWrapReq the is wrap req
     * @param relations the relations
     * @param entityType the entity type
     * @param superColumn the super column
     * @param embeddedObject the embedded object
     * @param superColumnFieldMap the super column field map
     */
    private void scrollOverSuperColumn(EntityMetadata m, List<String> relationNames, boolean isWrapReq,
            Map<String, Object> relations, EntityType entityType, SuperColumn superColumn, Object embeddedObject,
            Map<String, Field> superColumnFieldMap)
    {
        for (Column column : superColumn.getColumns())
        {
            String thriftColumnName = PropertyAccessorFactory.STRING.fromBytes(String.class, column.getName());
            byte[] thriftColumnValue = column.getValue();
            PropertyAccessorHelper.set(embeddedObject, superColumnFieldMap.get(thriftColumnName), thriftColumnValue);
        }
    }

    /**
     * On column.
     *
     * @param column the column
     * @param m the m
     * @param entity the entity
     * @param entityType the entity type
     * @param relationNames the relation names
     * @param isWrapReq the is wrap req
     * @param relations the relations
     */
    private void onColumn(Column column, EntityMetadata m, Object entity, EntityType entityType,
            List<String> relationNames, boolean isWrapReq, Map<String, Object> relations)
    {
        String thriftColumnName = PropertyAccessorFactory.STRING.fromBytes(String.class, column.getName());
        byte[] thriftColumnValue = column.getValue();
        populateViaThrift(m, entity, entityType, relationNames, relations, thriftColumnName, thriftColumnValue);
    }

    /**
     * On counter column.
     *
     * @param column the column
     * @param m the m
     * @param entity the entity
     * @param entityType the entity type
     * @param relationNames the relation names
     * @param isWrapReq the is wrap req
     * @param relations the relations
     */
    private void onCounterColumn(CounterColumn column, EntityMetadata m, Object entity, EntityType entityType,
            List<String> relationNames, boolean isWrapReq, Map<String, Object> relations)
    {
        String thriftColumnName = PropertyAccessorFactory.STRING.fromBytes(String.class, column.getName());
        String thriftColumnValue = new Long(column.getValue()).toString();
        populateViaThrift(m, entity, entityType, relationNames, relations, thriftColumnName, thriftColumnValue);
    }

    /**
     * Populate via thrift.
     *
     * @param m the m
     * @param entity the entity
     * @param entityType the entity type
     * @param relationNames the relation names
     * @param relations the relations
     * @param thriftColumnName the thrift column name
     * @param thriftColumnValue the thrift column value
     */
    private void populateViaThrift(EntityMetadata m, Object entity, EntityType entityType, List<String> relationNames,
            Map<String, Object> relations, String thriftColumnName, Object thriftColumnValue)
    {
        if (thriftColumnValue != null)
        {
            String fieldName = m.getFieldName(thriftColumnName);
            Attribute attribute = fieldName != null ? entityType.getAttribute(fieldName) : null;

            if (attribute != null)
            {
                try
                {
                    if (thriftColumnValue.getClass().isAssignableFrom(String.class))
                    {
                        PropertyAccessorHelper.set(entity, (Field) attribute.getJavaMember(),
                                (String) thriftColumnValue);
                    }
                    else
                    {
                        PropertyAccessorHelper.set(entity, (Field) attribute.getJavaMember(),
                                (byte[]) thriftColumnValue);
                    }
                }
                catch (PropertyAccessException pae)
                {
                    log.warn(pae.getMessage());
                }
            }
        }
        else
        {
            // populate relation.
            if (relationNames != null && !relationNames.isEmpty() && relationNames.contains(thriftColumnName))
            {
                // relations = new HashMap<String, Object>();
                String value = PropertyAccessorFactory.STRING.fromBytes(String.class, (byte[]) thriftColumnValue);
                relations.put(thriftColumnName, value);
                // prepare EnhanceEntity and return it
            }

        }
    }

    /**
     * On column or super column thrift row.
     *
     * @param timestamp2 the timestamp2
     * @param tr the tr
     * @param m the m
     * @param e the e
     * @param id the id
     */
    private void onColumnOrSuperColumnThriftRow(long timestamp2, ThriftRow tr, EntityMetadata m, Object e, String id)
    {

        // Iterate through Super columns

        MetamodelImpl metaModel = (MetamodelImpl) KunderaMetadata.INSTANCE.getApplicationMetadata().getMetamodel(
                m.getPersistenceUnit());

        EntityType entityType = metaModel.entity(m.getEntityClazz());

        Set<Attribute> attributes = entityType.getAttributes();
        for (Attribute attribute : attributes)
        {
            if (!attribute.getName().equals(m.getIdAttribute().getName()))
            {
                Field field = (Field) ((Attribute) attribute).getJavaMember();
                byte[] name = PropertyAccessorFactory.STRING
                        .toBytes(((AbstractAttribute) attribute).getJPAColumnName());
                Object value = getColumnValue(m, e, field);

                // if attribute is embeddable.

                if (metaModel.isEmbeddable(attribute.isCollection() ? ((PluralAttribute) attribute)
                        .getBindableJavaType() : attribute.getJavaType()))
                {
                    onEmbeddable(timestamp2, tr, m, e, id, attribute);
                }
                else
                {
                    if (m.getType().equals(Type.SUPER_COLUMN_FAMILY))
                    {

                        prepareSuperColumn(tr, m, value, name, timestamp2);
                    }
                    else
                    {
                        prepareColumn(tr, m, value, name, timestamp2);
                    }
                }
            }
        }

    }

    private Object getColumnValue(EntityMetadata m, Object e, Field field)
    {
        Object value;
        if(!m.isCounterColumnType())
         {
            value = PropertyAccessorHelper.get(e, field);
         } else
         {
             value = PropertyAccessorHelper.getString(e, field);
         }
        return value;
    }

    /**
     * Prepare column.
     *
     * @param tr the tr
     * @param m the m
     * @param value the value
     * @param name the name
     * @param timestamp the timestamp
     */
    private void prepareColumn(ThriftRow tr, EntityMetadata m, Object value, byte[] name, long timestamp)
    {
        if (value != null)
        {
            if (m.isCounterColumnType())
            {
                CounterColumn counterColumn = prepareCounterColumn((String)value, name);
                tr.addCounterColumn(counterColumn);
            }
            else
            {
                Column column = prepareColumn((byte[])value, name, timestamp);
                tr.addColumn(column);
            }
        }
    }

    /**
     * Prepare super column.
     *
     * @param tr the tr
     * @param m the m
     * @param value the value
     * @param name the name
     * @param timestamp the timestamp
     */
    private void prepareSuperColumn(ThriftRow tr, EntityMetadata m, Object value, byte[] name, long timestamp)
    {
        if (value != null)
        {
            if (m.isCounterColumnType())
            {
                CounterSuperColumn counterSuper = new CounterSuperColumn();
                counterSuper.setName(name);
                CounterColumn counterColumn = prepareCounterColumn((String)value, name);
                List<CounterColumn> subCounterColumn = new ArrayList<CounterColumn>();
                subCounterColumn.add(counterColumn);
                counterSuper.setColumns(subCounterColumn);
                tr.addCounterSuperColumn(counterSuper);
            }
            else
            {
                SuperColumn superCol = new SuperColumn();
                superCol.setName(name);
                Column column = prepareColumn((byte[])value, name, timestamp);
                List<Column> subColumn = new ArrayList<Column>();
                subColumn.add(column);
                superCol.setColumns(subColumn);
                tr.addSuperColumn(superCol);

            }
        }
    }

    /**
     * Prepare column.
     *
     * @param value the value
     * @param name the name
     * @param timestamp the timestamp
     * @return the column
     */
    private Column prepareColumn(byte[] value, byte[] name, long timestamp)
    {
        Column column = new Column();
        column.setName(name);
        column.setValue(value);
        column.setTimestamp(timestamp);
        return column;
    }

    /**
     * Prepare counter column.
     *
     * @param value the value
     * @param name the name
     * @return the counter column
     */
    private CounterColumn prepareCounterColumn(String value, byte[] name)
    {
        CounterColumn counterColumn = new CounterColumn();
        counterColumn.setName(name);
        LongAccessor accessor = new LongAccessor();
        counterColumn.setValue(accessor.fromString(LongAccessor.class, value));
        return counterColumn;
    }

    /**
     * On embeddable.
     *
     * @param timestamp2 the timestamp2
     * @param tr the tr
     * @param m the m
     * @param e the e
     * @param id the id
     * @param embeddableAttrib the embeddable attrib
     */
    private void onEmbeddable(long timestamp2, ThriftRow tr, EntityMetadata m, Object e, String id,
            Attribute embeddableAttrib)
    {

        MetamodelImpl metaModel = (MetamodelImpl) KunderaMetadata.INSTANCE.getApplicationMetadata().getMetamodel(
                m.getPersistenceUnit());

        Map<String, EmbeddableType> embeddables = metaModel.getEmbeddables(m.getEntityClazz());

        for (String key : embeddables.keySet())
        {
            EmbeddableType superColumn = embeddables.get(key);
            Field superColumnField = (Field) embeddableAttrib.getJavaMember();
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
                        // superColumnName = superColumn.getName() +
                        // Constants.EMBEDDED_COLUMN_NAME_DELIMITER + count;
                        superColumnName = ((AbstractAttribute) embeddableAttrib).getJPAColumnName()
                                + Constants.EMBEDDED_COLUMN_NAME_DELIMITER + count;

                        if (m.isCounterColumnType())
                        {
                            CounterSuperColumn thriftSuperColumn = buildThriftCounterSuperColumn(superColumnName,
                                    superColumn, obj);
                            tr.addCounterSuperColumn(thriftSuperColumn);
                        }
                        else
                        {
                            SuperColumn thriftSuperColumn = buildThriftSuperColumn(superColumnName, timestamp2,
                                    superColumn, obj);
                            tr.addSuperColumn(thriftSuperColumn);
                        }
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
                            superColumnName = ((AbstractAttribute) embeddableAttrib).getJPAColumnName()
                                    + Constants.EMBEDDED_COLUMN_NAME_DELIMITER + (++lastEmbeddedObjectCount);
                        }
                        buildThriftSuperColumn(timestamp2, tr, m, id, superColumn, superColumnName, obj);
                        ecCacheHandler.addElementCollectionCacheMapping(id, obj, superColumnName);
                    }
                }

            }
            else
            {
                superColumnName = ((AbstractAttribute) embeddableAttrib).getJPAColumnName();
                buildThriftSuperColumn(timestamp2, tr, m, id, superColumn, superColumnName, superColumnObject);
            }

        }

    }

    /**
     * Builds the thrift super column.
     *
     * @param timestamp2 the timestamp2
     * @param tr the tr
     * @param m the m
     * @param id the id
     * @param superColumn the super column
     * @param superColumnName the super column name
     * @param obj the obj
     */
    private void buildThriftSuperColumn(long timestamp2, ThriftRow tr, EntityMetadata m, String id,
            EmbeddableType superColumn, String superColumnName, Object obj)
    {
        if (m.isCounterColumnType())
        {
            CounterSuperColumn thriftSuperColumn = buildThriftCounterSuperColumn(superColumnName, superColumn, obj);
            tr.addCounterSuperColumn(thriftSuperColumn);
        }
        else
        {
            SuperColumn thriftSuperColumn = buildThriftSuperColumn(superColumnName, timestamp2, superColumn, obj);
            tr.addSuperColumn(thriftSuperColumn);
        }
    }

    /**
     * Builds the thrift counter super column.
     *
     * @param superColumnName the super column name
     * @param superColumn the super column
     * @param counterSuperColumnObject the counter super column object
     * @return the counter super column
     */
    private CounterSuperColumn buildThriftCounterSuperColumn(String superColumnName, EmbeddableType superColumn,
            Object counterSuperColumnObject)
    {

        Iterator<Attribute> iter = superColumn.getAttributes().iterator();

        List<CounterColumn> thriftColumns = new ArrayList<CounterColumn>();

        while (iter.hasNext())
        {
            Attribute column = iter.next();
            Field field = (Field) column.getJavaMember();
            String name = ((AbstractAttribute) column).getJPAColumnName();
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
    private SuperColumn buildThriftSuperColumn(String superColumnName, long timestamp, EmbeddableType superColumn,
            Object superColumnObject) throws PropertyAccessException
    {
        List<Column> thriftColumns = new ArrayList<Column>();

        Iterator<Attribute> iter = superColumn.getAttributes().iterator();

        while (iter.hasNext())
        {
            AbstractAttribute column = (AbstractAttribute) iter.next();

            //
            // for (com.impetus.kundera.metadata.model.Column column :
            // superColumn.getColumns())
            // {
            Field field = (Field) column.getJavaMember();
            String name = column.getJPAColumnName();
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

}
