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
import com.impetus.kundera.metadata.model.EmbeddedColumn;
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
 * Base class for all Cassandra Data Handlers
 * 
 * @author amresh.singh
 */
public abstract class CassandraDataHandlerBase
{

    /** The log. */
    private static Log log = LogFactory.getLog(CassandraDataHandlerBase.class);
    
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
    public Object fromColumnThriftRow(Class<?> clazz, EntityMetadata m, ThriftRow thriftRow,
            List<String> relationNames, boolean isWrapperReq) throws Exception
    {

        // Instantiate a new instance
        Object entity = null;
        Map<String, Object> relations = new HashMap<String, Object>();

        for (Column c : thriftRow.getColumns())
        {

            String thriftColumnName = PropertyAccessorFactory.STRING.fromBytes(String.class, c.getName());
            byte[] thriftColumnValue = c.getValue();

            if (null == thriftColumnValue)
            {
                continue;
            }

            // Check if this is a property, or a column representing foreign
            // keys
            // com.impetus.kundera.metadata.model.Column column =
            // m.getColumn(thriftColumnName);

            EntityType entityType = KunderaMetadata.INSTANCE.getApplicationMetadata()
                    .getMetamodel(m.getPersistenceUnit()).entity(clazz);

            String fieldName = m.getFieldName(thriftColumnName);

            Attribute column = fieldName != null ? entityType.getAttribute(fieldName) : null;

            // entityType.getAttribute(arg0)
            if (column != null)
            {
                if (entity == null)
                {
                    entity = clazz.newInstance();
                    // Set row-key
                    PropertyAccessorHelper.setId(entity, m, thriftRow.getId());
                }

                try
                {
                    // PropertyAccessorHelper.set(entity, column.getField(),
                    // thriftColumnValue);
                    PropertyAccessorHelper.set(entity, (Field) column.getJavaMember(), thriftColumnValue);
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
            // com.impetus.kundera.metadata.model.Column column =
            // m.getColumn(thriftColumnName);
            EntityType entityType = KunderaMetadata.INSTANCE.getApplicationMetadata()
                    .getMetamodel(m.getPersistenceUnit()).entity(clazz);

            String fieldName = m.getFieldName(thriftColumnName);
            Attribute column = entityType.getAttribute(fieldName);

            if (column != null)
            {
                try
                {
                    if ((column.getJavaType().equals(Integer.class) || column.getJavaType().equals(int.class))
                            && thriftColumnValue != null)
                    {
                        PropertyAccessorHelper
                                .set(entity, (Field) column.getJavaMember(), thriftColumnValue.intValue());
                    }
                    else
                    {
                        PropertyAccessorHelper.set(entity, (Field) column.getJavaMember(), thriftColumnValue);
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
    public Object fromSuperColumnThriftRow(Class clazz, EntityMetadata m, ThriftRow tr, List<String> relationNames,
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
                                // com.impetus.kundera.metadata.model.Column col
                                // = m.getColumn(name);
                                EntityType entityType = KunderaMetadata.INSTANCE.getApplicationMetadata()
                                        .getMetamodel(m.getPersistenceUnit()).entity(clazz);

                                String fieldName = m.getFieldName(name);
                                Attribute col = entityType.getAttribute(fieldName);
                                if (col != null)
                                {
                                    superColumnObj = CassandraUtilities.toUTF8(value);
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

                    for (CounterColumn column : sc.getColumns())
                    {
                        String name = PropertyAccessorFactory.STRING.fromBytes(String.class, column.getName());
                        Long value = column.getValue();
                        Field columnField = columnNameToFieldMap.get(name);
                        columnField = columnField == null ? columnNameToFieldMap.get(superColumnField.getName())
                                : columnField;
                        if (columnField != null)
                        {
                            try
                            {
                                if ((columnField.getType().equals(Integer.class) || columnField.getType().equals(
                                        int.class))
                                        && value != null)
                                {
                                    int colValue = value.intValue();
                                    superColumnObj = populateColumnValue(superColumnClass, colValue, columnField);
                                    /*
                                     * if(superColumnClass.isPrimitive()) {
                                     * superColumnObj = colValue; } else {
                                     * superColumnObj =
                                     * PropertyAccessorHelper.getObject
                                     * (superColumnClass);
                                     * PropertyAccessorHelper
                                     * .set(superColumnObj, columnField,
                                     * colValue); }
                                     */}
                                else
                                {
                                    superColumnObj = populateColumnValue(superColumnClass, value, columnField);
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
                                // com.impetus.kundera.metadata.model.Column col
                                // = m.getColumn(name);
                                EntityType entityType = KunderaMetadata.INSTANCE.getApplicationMetadata()
                                        .getMetamodel(m.getPersistenceUnit()).entity(clazz);

                                String fieldName = m.getFieldName(name);
                                Attribute col = entityType.getAttribute(fieldName);
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
     * @param superColumnClass
     * @param value
     * @param columnField
     * @return
     */
    private Object populateColumnValue(Class superColumnClass, Object value, Field columnField)
    {
        Object superColumnObj;
        if (superColumnClass.isPrimitive())
        {
            superColumnObj = value;
        }
        else
        {
            superColumnObj = PropertyAccessorHelper.getObject(superColumnClass);
            PropertyAccessorHelper.set(superColumnObj, columnField, value);
        }
        return superColumnObj;
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

        // }

        // Add relations entities as Foreign keys to a new super column created
        // internally
        // addRelationshipsToThriftRow(timestamp, tr, e, m);

        return tr;
    }

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

  
    protected Object populateEntity(ThriftRow tr,EntityMetadata m,List<String> relationNames,boolean isWrapReq)
    {
        Map<String, Object> relations = new HashMap<String, Object>();
        Object entity = null;
        try
        {
            entity =m.getEntityClazz().newInstance();
        
        PropertyAccessorHelper.setId(entity, m, tr.getId());

        EntityType entityType = KunderaMetadata.INSTANCE.getApplicationMetadata().getMetamodel(m.getPersistenceUnit()).entity(m.getEntityClazz());

        for(Column column : tr.getColumns())
        {
            onColumn(column, m, entity, entityType,relationNames,isWrapReq,relations);
        }
        
        // Add all super columns to entity
        Collection embeddedCollection = null;
        Field embeddedCollectionField = null;

        for(SuperColumn superColumn : tr.getSuperColumns())
        {
            String scName = PropertyAccessorFactory.STRING.fromBytes(String.class, superColumn.getName());
            String scNamePrefix = null;

            // Map to hold property-name=>foreign-entity relations
            Map<String, Set<String>> foreignKeysMap = new HashMap<String, Set<String>>();

            // Get a name->field map for super-columns
            Map<String, Field> columnNameToFieldMap = new HashMap<String, Field>();
            Map<String, Field> superColumnNameToFieldMap = new HashMap<String, Field>();
            MetadataUtils.populateColumnAndSuperColumnMaps(m, columnNameToFieldMap, superColumnNameToFieldMap);


                if (scName.indexOf(Constants.EMBEDDED_COLUMN_NAME_DELIMITER) != -1)
                {
                    scNamePrefix = MetadataUtils.getEmbeddedCollectionPrefix(scName);
                    embeddedCollectionField = superColumnNameToFieldMap.get(scNamePrefix);

                    if (embeddedCollection == null)
                    {
                        embeddedCollection = MetadataUtils.getEmbeddedCollectionInstance(embeddedCollectionField);
                    }

                    Object embeddedObject = MetadataUtils.getEmbeddedGenericObjectInstance(embeddedCollectionField);

                    scrollOverSuperColumn(m, relationNames, isWrapReq, relations, entityType, superColumn,embeddedObject);
                    embeddedCollection.add(embeddedObject);

                    // Add this embedded object to cache
                    ElementCollectionCacheManager.getInstance().addElementCollectionCacheMapping(tr.getId(),
                            embeddedObject, scName);
                }
                else
                {
                    scrollOverSuperColumn(m, relationNames, isWrapReq, relations, entityType, superColumn, entity);
                }
        }

        
        for(CounterColumn counterColumn : tr.getCounterColumns())
        {
            onCounterColumn(counterColumn, m, entity, entityType,relationNames,isWrapReq,relations);
        }
        
        for(CounterSuperColumn counterSuperColumn : tr.getCounterSuperColumns())
        {
            String scName = PropertyAccessorFactory.STRING.fromBytes(String.class, counterSuperColumn.getName());
            String scNamePrefix = null;

            // Map to hold property-name=>foreign-entity relations
            Map<String, Set<String>> foreignKeysMap = new HashMap<String, Set<String>>();

            // Get a name->field map for super-columns
            Map<String, Field> columnNameToFieldMap = new HashMap<String, Field>();
            Map<String, Field> superColumnNameToFieldMap = new HashMap<String, Field>();
            MetadataUtils.populateColumnAndSuperColumnMaps(m, columnNameToFieldMap, superColumnNameToFieldMap);


                if (scName.indexOf(Constants.EMBEDDED_COLUMN_NAME_DELIMITER) != -1)
                {
                    scNamePrefix = MetadataUtils.getEmbeddedCollectionPrefix(scName);
                    embeddedCollectionField = superColumnNameToFieldMap.get(scNamePrefix);

                    if (embeddedCollection == null)
                    {
                        embeddedCollection = MetadataUtils.getEmbeddedCollectionInstance(embeddedCollectionField);
                    }

                    Object embeddedObject = MetadataUtils.getEmbeddedGenericObjectInstance(embeddedCollectionField);

                    scrollOverCounterSuperColumn(m, relationNames, isWrapReq, relations, entityType, counterSuperColumn,embeddedObject);
                    embeddedCollection.add(embeddedObject);

                    // Add this embedded object to cache
                    ElementCollectionCacheManager.getInstance().addElementCollectionCacheMapping(tr.getId(),
                            embeddedObject, scName);
                }
                else
                {
                    scrollOverCounterSuperColumn(m, relationNames, isWrapReq, relations, entityType, counterSuperColumn, entity);
                }            
        }
        
        
        if (embeddedCollection != null && !embeddedCollection.isEmpty())
        {
            PropertyAccessorHelper.set(entity, embeddedCollectionField, embeddedCollection);
        }

        }catch(InstantiationException iex)
        {
            log.error("Eror while retrieving data, Caused by:" + iex.getMessage());
            throw new PersistenceException(iex);
        }
        catch (IllegalAccessException iaex)
        {
            log.error("Eror while retrieving data, Caused by:" + iaex.getMessage());
            throw new PersistenceException(iaex);
        }
        
        return isWrapReq && relations != null && !relations.isEmpty() ? new EnhanceEntity(entity, tr.getId(),
                relations) : entity;

    }

    private void scrollOverSuperColumn(EntityMetadata m, List<String> relationNames, boolean isWrapReq,
            Map<String, Object> relations, EntityType entityType, SuperColumn superColumn, Object embeddedObject)
    {
        for (Column column : superColumn.getColumns())
        {
            onColumn(column, m, embeddedObject, entityType, relationNames, isWrapReq, relations);
        }
    }


    private void scrollOverCounterSuperColumn(EntityMetadata m, List<String> relationNames, boolean isWrapReq,
            Map<String, Object> relations, EntityType entityType, CounterSuperColumn superColumn, Object embeddedObject)
    {
        for (CounterColumn column : superColumn.getColumns())
        {
            onCounterColumn(column, m, embeddedObject, entityType, relationNames, isWrapReq, relations);
        }
    }

    
    private void onColumn(Column column, EntityMetadata m, Object entity, EntityType entityType,List<String> relationNames,boolean isWrapReq,Map<String, Object> relations)
    {
//        Map<String, Object> relations = new HashMap<String, Object>();

            String thriftColumnName = PropertyAccessorFactory.STRING.fromBytes(String.class, column.getName());
            byte[] thriftColumnValue = column.getValue();
            populateViaThrift(m, entity, entityType, relationNames, relations, thriftColumnName, thriftColumnValue);
    }

    private void onCounterColumn(CounterColumn column, EntityMetadata m, Object entity, EntityType entityType,List<String> relationNames,boolean isWrapReq,Map<String, Object> relations)
    {
//        Map<String, Object> relations = new HashMap<String, Object>();

            String thriftColumnName = PropertyAccessorFactory.STRING.fromBytes(String.class, column.getName());
            LongAccessor accessor = new LongAccessor();
            byte[] thriftColumnValue = accessor.toBytes(column.getValue());
            populateViaThrift(m, entity, entityType, relationNames, relations, thriftColumnName, thriftColumnValue);
    }

    private void populateViaThrift(EntityMetadata m, Object entity, EntityType entityType, List<String> relationNames,
            Map<String, Object> relations, String thriftColumnName, byte[] thriftColumnValue)
    {
        if (thriftColumnValue != null)
        {
            String fieldName = m.getFieldName(thriftColumnName);
            Attribute attribute = fieldName != null ? entityType.getAttribute(fieldName) : null;

            if (attribute != null)
            {
                try
                {
                    PropertyAccessorHelper.set(entity, (Field) attribute.getJavaMember(), thriftColumnValue);
                }
                catch (PropertyAccessException pae)
                {
                    log.warn(pae.getMessage());
                }
            }
        } else
        {
            // populate relation.
            if (relationNames != null && !relationNames.isEmpty() && relationNames.contains(thriftColumnName))
            {
                // relations = new HashMap<String, Object>();
                String value = PropertyAccessorFactory.STRING.fromBytes(String.class, thriftColumnValue);
                relations.put(thriftColumnName, value);
                // prepare EnhanceEntity and return it
            }

        
      }
    }

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
                String value = PropertyAccessorHelper.getString(e, field);
                byte[] name = PropertyAccessorFactory.STRING.toBytes(((AbstractAttribute)attribute).getJPAColumnName());

                // if attribute is embeddable.

                if (metaModel.isEmbeddable(attribute.isCollection() ? ((PluralAttribute) attribute)
                        .getBindableJavaType() : attribute.getJavaType()))
                {
                    onEmbeddable(timestamp2, tr, m, e, id, attribute);
                } else if(m.getType().equals(Type.SUPER_COLUMN_FAMILY))
                {
                    
                    prepareSuperColumn(tr, m, value, name);
                } else 
                {
                    prepareColumn(tr, m, value, name);
                }
                

            }
        }

    }

    private void prepareColumn(ThriftRow tr, EntityMetadata m, String value, byte[] name)
    {
        if (m.isCounterColumnType())
        {
            CounterColumn counterColumn = prepareCounterColumn(value, name);
            tr.addCounterColumn(counterColumn);
        }
        else
        {
            Column column = prepareColumn(value, name);
            tr.addColumn(column);
        }
    }

    private void prepareSuperColumn(ThriftRow tr, EntityMetadata m, String value, byte[] name)
    {
        if(m.isCounterColumnType())
        {
            CounterSuperColumn counterSuper = new CounterSuperColumn();
            counterSuper.setName(name);
            CounterColumn counterColumn = prepareCounterColumn(value, name);
            List<CounterColumn> subCounterColumn = new ArrayList<CounterColumn>();
            subCounterColumn.add(counterColumn);
            counterSuper.setColumns(subCounterColumn);
            tr.addCounterSuperColumn(counterSuper);
        } else
        {
            SuperColumn superCol = new SuperColumn();
            superCol.setName(name);
            Column column = prepareColumn(value, name);
            List<Column> subColumn = new ArrayList<Column>();
            subColumn.add(column);
            superCol.setColumns(subColumn);
            tr.addSuperColumn(superCol);
            
        }
    }

    private Column prepareColumn(String value, byte[] name)
    {
        Column column = new Column();
        column.setName(name);
        column.setValue(value.getBytes());
        return column;
    }

    private CounterColumn prepareCounterColumn(String value, byte[] name)
    {
        CounterColumn counterColumn = new CounterColumn();
        counterColumn.setName(name);
        counterColumn.setValue(new Long(value));
        return counterColumn;
    }

    /**
     * @param timestamp2
     * @param tr
     * @param m
     * @param e
     * @param id
     * @param attribute
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

                        if(m.isCounterColumnType())
                        {
                        CounterSuperColumn thriftSuperColumn = buildThriftCounterSuperColumn(superColumnName,
                                superColumn, obj);
                        tr.addCounterSuperColumn(thriftSuperColumn);
                        } else
                        {
                            SuperColumn thriftSuperColumn = buildThriftSuperColumn(superColumnName, timestamp2, superColumn,
                                    obj);
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

    private void buildThriftSuperColumn(long timestamp2, ThriftRow tr, EntityMetadata m, String id,
            EmbeddableType superColumn, String superColumnName, Object obj)
    {
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
    }

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
