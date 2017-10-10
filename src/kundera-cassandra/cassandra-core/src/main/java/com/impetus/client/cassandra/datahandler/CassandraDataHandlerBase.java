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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
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

import org.apache.cassandra.cql3.FieldIdentifier;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.serializers.ListSerializer;
import org.apache.cassandra.serializers.MapSerializer;
import org.apache.cassandra.serializers.SetSerializer;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.serializers.UTF8Serializer;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.CounterColumn;
import org.apache.cassandra.thrift.CounterSuperColumn;
import org.apache.cassandra.thrift.CqlMetadata;
import org.apache.cassandra.thrift.SuperColumn;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.cassandra.CassandraClientBase;
import com.impetus.client.cassandra.common.CassandraConstants;
import com.impetus.client.cassandra.schemamanager.CassandraDataTranslator;
import com.impetus.client.cassandra.thrift.ThriftDataResultHelper;
import com.impetus.client.cassandra.thrift.ThriftRow;
import com.impetus.kundera.Constants;
import com.impetus.kundera.KunderaException;
import com.impetus.kundera.cache.ElementCollectionCacheManager;
import com.impetus.kundera.client.EnhanceEntity;
import com.impetus.kundera.db.DataRow;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.MetadataUtils;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.EntityMetadata.Type;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.metadata.model.type.AbstractManagedType;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessor;
import com.impetus.kundera.property.PropertyAccessorFactory;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.property.accessor.LongAccessor;
import com.impetus.kundera.utils.KunderaCoreUtils;
import com.impetus.kundera.utils.TimestampGenerator;

/**
 * Base class for all Cassandra Data Handlers.
 * 
 * @author amresh.singh
 */
public abstract class CassandraDataHandlerBase
{

    /** The Constant SYS_SUM. */
    private static final String SYS_SUM = "system.sum";

    /** The Constant SYS_AVG. */
    private static final String SYS_AVG = "system.avg";

    /** The Constant SYS_MAX. */
    private static final String SYS_MAX = "system.max";

    /** The Constant SYS_MIN. */
    private static final String SYS_MIN = "system.min";

    /** The Constant SYS_COUNT. */
    private static final String SYS_COUNT = "system.count";

    /** The Constant SUM. */
    private static final String SUM = "sum";

    /** The Constant AVG. */
    private static final String AVG = "avg";

    /** The Constant MAX. */
    private static final String MAX = "max";

    /** The Constant MIN. */
    private static final String MIN = "min";

    /** The Constant COUNT. */
    private static final String COUNT = "count";

    /** The log. */
    private static Logger log = LoggerFactory.getLogger(CassandraDataHandlerBase.class);

    /** The thrift translator. */
    protected final ThriftDataResultHelper thriftTranslator = new ThriftDataResultHelper();

    /** The client base. */
    private final CassandraClientBase clientBase;

    /** The kundera metadata. */
    protected final KunderaMetadata kunderaMetadata;

    /** The generator. */
    protected final TimestampGenerator generator;

    /**
     * Instantiates a new cassandra data handler base.
     * 
     * @param clientBase
     *            the client base
     * @param kunderaMetadata
     *            the kundera metadata
     * @param generator
     *            the generator
     */
    public CassandraDataHandlerBase(final CassandraClientBase clientBase, final KunderaMetadata kunderaMetadata,
            final TimestampGenerator generator)
    {
        this.generator = generator;
        this.clientBase = clientBase;
        this.kunderaMetadata = kunderaMetadata;
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

    public <E> E fromThriftRow(Class<E> clazz, EntityMetadata m, DataRow<SuperColumn> tr) throws Exception
    {

        // Instantiate a new instance
        E e = null;

        // Set row-key. Note:

        // Get a name->field map for super-columns
        Map<String, Field> columnNameToFieldMap = new HashMap<String, Field>();
        Map<String, Field> superColumnNameToFieldMap = new HashMap<String, Field>();
        MetadataUtils.populateColumnAndSuperColumnMaps(m, columnNameToFieldMap, superColumnNameToFieldMap,
                kunderaMetadata);

        Collection embeddedCollection = null;
        Field embeddedCollectionField = null;
        for (SuperColumn sc : tr.getColumns())
        {
            if (e == null)
            {
                // Instantiate a new instance
                e = clazz.newInstance();

                // Set row-key.
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

        if (log.isInfoEnabled())
        {
            log.info("Returning entity {} for class {}", e, clazz);
        }
        return e;
    }

    /**
     * From thrift row.
     * 
     * @param clazz
     *            the clazz
     * @param m
     *            the m
     * @param relationNames
     *            the relation names
     * @param isWrapReq
     *            the is wrap req
     * @param consistencyLevel
     *            the consistency level
     * @param rowIds
     *            the row ids
     * @return the list
     * @throws Exception
     *             the exception
     */
    public List<Object> fromThriftRow(Class<?> clazz, EntityMetadata m, List<String> relationNames, boolean isWrapReq,
            ConsistencyLevel consistencyLevel, Object... rowIds) throws Exception
    {
        List<Object> entities = new ArrayList<Object>();
        if (rowIds != null)
        {
            for (Object rowKey : rowIds)
            {
                Object e = fromThriftRow(clazz, m, rowKey, relationNames, isWrapReq, consistencyLevel);
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
     * @param consistencyLevel
     *            the consistency level
     * @return the object
     * @throws Exception
     *             the exception
     */
    public abstract Object fromThriftRow(Class<?> clazz, EntityMetadata m, Object rowKey, List<String> relationNames,
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
        MetadataUtils.populateColumnAndSuperColumnMaps(m, columnNameToFieldMap, superColumnNameToFieldMap,
                kunderaMetadata);

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
     * @param columnTTLs
     *            TODO
     * @return the base data accessor. thrift row
     * @throws Exception
     *             the exception
     */
    public Collection<ThriftRow> toThriftRow(Object e, Object id, EntityMetadata m, String columnFamily,
            Object columnTTLs) throws Exception
    {
        // timestamp to use in thrift column objects
        long timestamp = generator.getTimestamp();
        // Add super columns to thrift row
        return onColumnOrSuperColumnThriftRow(/* tr, */m, e, id, timestamp, columnTTLs);
    }

    /**
     * Gets the thrift row.
     * 
     * @param id
     *            the id
     * @param columnFamily
     *            the column family
     * @param thriftRows
     *            the thrift rows
     * @return the thrift row
     */
    private ThriftRow getThriftRow(Object id, String columnFamily, Map<String, ThriftRow> thriftRows)
    {
        ThriftRow tr = thriftRows.get(columnFamily);
        if (tr == null)
        {
            tr = new ThriftRow();
            tr.setColumnFamilyName(columnFamily); // column-family name
            tr.setId(id); // Id
            thriftRows.put(columnFamily, tr);
        }
        return tr;
    }

    /**
     * To index thrift row.
     * 
     * @param e
     *            the e
     * @param m
     *            the m
     * @param columnFamily
     *            the column family
     * @return the list
     */
    public List<ThriftRow> toIndexThriftRow(Object e, EntityMetadata m, String columnFamily)
    {
        List<ThriftRow> indexThriftRows = new ArrayList<ThriftRow>();

        byte[] rowKey = getThriftColumnValue(e, m.getIdAttribute());

        MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                m.getPersistenceUnit());

        // Add thrift rows for embeddables
        Map<String, EmbeddableType> embeddables = metaModel.getEmbeddables(m.getEntityClazz());
        EntityType entityType = metaModel.entity(m.getEntityClazz());

        for (String embeddedFieldName : embeddables.keySet())
        {
            EmbeddableType embeddedColumn = embeddables.get(embeddedFieldName);

            // Index embeddable only when specified by user
            Field embeddedField = (Field) entityType.getAttribute(embeddedFieldName).getJavaMember();
            if (!MetadataUtils.isEmbeddedAtributeIndexable(embeddedField))
            {
                continue;
            }

            Object embeddedObject = PropertyAccessorHelper.getObject(e,
                    (Field) entityType.getAttribute(embeddedFieldName).getJavaMember());
            if (embeddedObject == null)
            {
                continue;
            }
            if (embeddedObject instanceof Collection)
            {
                ElementCollectionCacheManager ecCacheHandler = ElementCollectionCacheManager.getInstance();

                for (Object obj : (Collection) embeddedObject)
                {
                    for (Object column : embeddedColumn.getAttributes())
                    {

                        Attribute columnAttribute = (Attribute) column;
                        String columnName = columnAttribute.getName();
                        if (!MetadataUtils.isColumnInEmbeddableIndexable(embeddedField, columnName))
                        {
                            continue;
                        }
                        // Column Value
                        String id = (String) CassandraDataTranslator.decompose(
                                ((AbstractAttribute) m.getIdAttribute()).getBindableJavaType(), rowKey, false);
                        String superColumnName = ecCacheHandler.getElementCollectionObjectName(id, obj);

                        ThriftRow tr = constructIndexTableThriftRow(columnFamily, embeddedFieldName, obj,
                                columnAttribute, rowKey, superColumnName);
                        if (tr != null)
                        {
                            indexThriftRows.add(tr);
                        }
                    }
                }
            }
            else
            {
                for (Object column : embeddedColumn.getAttributes())
                {
                    Attribute columnAttribute = (Attribute) column;
                    String columnName = columnAttribute.getName();
                    Class<?> columnClass = ((AbstractAttribute) columnAttribute).getBindableJavaType();
                    if (!MetadataUtils.isColumnInEmbeddableIndexable(embeddedField, columnName)
                            || columnClass.equals(byte[].class))
                    {
                        continue;
                    }

                    // No EC Name
                    ThriftRow tr = constructIndexTableThriftRow(columnFamily, embeddedFieldName, embeddedObject,
                            (Attribute) column, rowKey, "");
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
     * @param columnFamily
     *            Column family Name for Index Table
     * @param embeddedFieldName
     *            the embedded field name
     * @param obj
     *            Embedded Object instance
     * @param column
     *            Instance of {@link Column}
     * @param rowKey
     *            Name of Index Column
     * @param ecValue
     *            Name of embeddable object in Element Collection cache (usually
     *            in the form of <element collection field name>#<integer
     *            counter>
     * @return Instance of {@link ThriftRow}
     */
    private ThriftRow constructIndexTableThriftRow(String columnFamily, String embeddedFieldName, Object obj,
            Attribute column, byte[] rowKey, String ecValue)
    {
        // Column Name
        Field columnField = (Field) column.getJavaMember();
        byte[] indexColumnName = PropertyAccessorHelper.get(obj, columnField);

        ThriftRow tr = null;
        if (indexColumnName != null && indexColumnName.length != 0 && rowKey != null)
        {
            // Construct Index Table Thrift Row
            tr = new ThriftRow();
            tr.setColumnFamilyName(columnFamily); // Index column-family name
            tr.setId(embeddedFieldName + Constants.INDEX_TABLE_ROW_KEY_DELIMITER + column.getName()); // Id

            SuperColumn thriftSuperColumn = new SuperColumn();
            thriftSuperColumn.setName(indexColumnName);

            Column thriftColumn = new Column();
            thriftColumn.setName(rowKey);
            thriftColumn.setValue(ecValue.getBytes());
            thriftColumn.setTimestamp(generator.getTimestamp());

            thriftSuperColumn.addToColumns(thriftColumn);

            tr.addSuperColumn(thriftSuperColumn);
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
     * @param columnJavaType
     *            the column java type
     * @return the foreign keys from join table
     */
    public <E> List<Object> getForeignKeysFromJoinTable(String inverseJoinColumnName, List<Column> columns,
            Class columnJavaType)
    {
        List<Object> foreignKeys = new ArrayList<Object>();

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
                    Object val = PropertyAccessorHelper.getObject(columnJavaType, thriftColumnValue);
                    foreignKeys.add(val);
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
     * @param tr
     *            the tr
     * @param m
     *            the m
     * @param entity
     *            the entity
     * @param relationNames
     *            the relation names
     * @param isWrapReq
     *            the is wrap req
     * @return the object
     */
    public Object populateEntity(ThriftRow tr, EntityMetadata m, Object entity, List<String> relationNames,
            boolean isWrapReq)
    {
        Map<String, Object> relations = new HashMap<String, Object>();
        try
        {
            boolean isCql3Enabled = clientBase.isCql3Enabled(m);
            MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                    m.getPersistenceUnit());
            EntityType entityType = metaModel.entity(m.getEntityClazz());

            for (Column column : tr.getColumns())
            {
                if (column != null)
                {
                    String thriftColumnName = PropertyAccessorFactory.STRING.fromBytes(String.class, column.getName());
                    if ((CassandraConstants.CQL_KEY.equalsIgnoreCase(thriftColumnName) || ((AbstractAttribute) m
                            .getIdAttribute()).getJPAColumnName().equals(thriftColumnName)) && tr.getId() == null)
                    {
                        entity = KunderaCoreUtils.initialize(m, entity, null);
                        setId(m, entity, column.getValue(), isCql3Enabled);
                    }
                    else
                    {
                        entity = onColumn(column, m, entity, entityType, relationNames, isWrapReq, relations,
                                isCql3Enabled);

                        String discriminatorColumn = ((AbstractManagedType) entityType).getDiscriminatorColumn();
                        String discriminatorValue = ((AbstractManagedType) entityType).getDiscriminatorValue();

                        if (thriftColumnName != null
                                && thriftColumnName.equals(discriminatorColumn)
                                && column.getValue() != null
                                && !PropertyAccessorFactory.STRING.fromBytes(String.class, column.getValue()).equals(
                                        discriminatorValue))
                        {
                            entity = null;
                            break;
                        }
                    }
                }
            }

            // Add all super columns to entity
            Collection embeddedCollection = null;
            Field embeddedCollectionField = null;

            boolean mappingProcessed = false;
            Map<String, Field> columnNameToFieldMap = new HashMap<String, Field>();
            Map<String, Field> superColumnNameToFieldMap = new HashMap<String, Field>();

            for (SuperColumn superColumn : tr.getSuperColumns())
            {
                if (superColumn != null)
                {

                    entity = KunderaCoreUtils.initialize(m, entity, tr.getId());
                    String scName = PropertyAccessorFactory.STRING.fromBytes(String.class, superColumn.getName());
                    String scNamePrefix = null;

                    // Map to hold property-name=>foreign-entity relations
                    Map<String, Set<String>> foreignKeysMap = new HashMap<String, Set<String>>();

                    // Get a name->field map for super-columns
                    if (!mappingProcessed)
                    {
                        MetadataUtils.populateColumnAndSuperColumnMaps(m, columnNameToFieldMap,
                                superColumnNameToFieldMap, kunderaMetadata);
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

                        scrollOverSuperColumn(m, relationNames, isWrapReq, relations, entityType, superColumn,
                                embeddedObject, columnNameToFieldMap);

                        Collection collection = PropertyAccessorHelper.getCollectionInstance(embeddedCollectionField);
                        collection.add(embeddedObject);

                        PropertyAccessorHelper.set(entity, embeddedCollectionField, collection);

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

                            Object embeddedObj = PropertyAccessorHelper.getObject(entity, field);
                            if (embeddedObj == null)
                            {
                                embeddedObj = field.getType().newInstance();
                            }
                            // column
                            scrollOverSuperColumn(m, relationNames, isWrapReq, relations, entityType, superColumn,
                                    embeddedObj, columnNameToFieldMap);
                            PropertyAccessorHelper.set(entity, field, embeddedObj);
                        }
                        else
                        {
                            scrollOverSuperColumn(m, relationNames, isWrapReq, relations, entityType, superColumn,
                                    entity, isCql3Enabled);
                        }

                    }
                }
            }

            mappingProcessed = false;

            for (CounterColumn counterColumn : tr.getCounterColumns())
            {
                if (counterColumn != null)
                {
                    entity = KunderaCoreUtils.initialize(m, entity, tr.getId());
                    onCounterColumn(counterColumn, m, entity, entityType, relationNames, isWrapReq, relations,
                            isCql3Enabled);
                }
            }

            for (CounterSuperColumn counterSuperColumn : tr.getCounterSuperColumns())
            {
                if (counterSuperColumn != null)
                {
                    entity = KunderaCoreUtils.initialize(m, entity, tr.getId());
                    String scName = PropertyAccessorFactory.STRING
                            .fromBytes(String.class, counterSuperColumn.getName());
                    String scNamePrefix = null;

                    // Map to hold property-name=>foreign-entity relations
                    Map<String, Set<String>> foreignKeysMap = new HashMap<String, Set<String>>();

                    // Get a name->field map for super-columns
                    if (!mappingProcessed)
                    {
                        MetadataUtils.populateColumnAndSuperColumnMaps(m, columnNameToFieldMap,
                                superColumnNameToFieldMap, kunderaMetadata);
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
                                    counterSuperColumn, entity, isCql3Enabled);
                        }
                    }
                }
            }

            if (embeddedCollection != null && !embeddedCollection.isEmpty())
            {
                PropertyAccessorHelper.set(entity, embeddedCollectionField, embeddedCollection);
            }

        }
        catch (Exception e)
        {
            log.error("Eror while retrieving data, Caused by: .", e);
            throw new PersistenceException(e);
        }

        if (entity != null && tr.getId() != null)
        {
            PropertyAccessorHelper.setId(entity, m, tr.getId());
        }
        return isWrapReq && relations != null && !relations.isEmpty() ? new EnhanceEntity(entity,
                PropertyAccessorHelper.getId(entity, m), relations) : entity;
    }

    /**
     * Sets the id.
     * 
     * @param m
     *            the m
     * @param entity
     *            the entity
     * @param columnValue
     *            the column value
     * @param isCql3Enabled
     *            the is cql3 enabled
     */
    private void setId(EntityMetadata m, Object entity, Object columnValue, boolean isCql3Enabled)
    {
        if (isCql3Enabled && !m.getType().equals(Type.SUPER_COLUMN_FAMILY))
        {
            setFieldValueViaCQL(entity, columnValue, m.getIdAttribute());
        }
        else
        {
            setFieldValue(entity, columnValue, m.getIdAttribute());
        }
    }

    /**
     * Scroll over super column.
     * 
     * @param m
     *            the m
     * @param relationNames
     *            the relation names
     * @param isWrapReq
     *            the is wrap req
     * @param relations
     *            the relations
     * @param entityType
     *            the entity type
     * @param superColumn
     *            the super column
     * @param embeddedObject
     *            the embedded object
     * @param isCql3Enabled
     *            the is cql3 enabled
     * @throws InstantiationException
     *             the instantiation exception
     * @throws IllegalAccessException
     *             the illegal access exception
     */
    private void scrollOverSuperColumn(EntityMetadata m, List<String> relationNames, boolean isWrapReq,
            Map<String, Object> relations, EntityType entityType, SuperColumn superColumn, Object embeddedObject,
            boolean isCql3Enabled) throws InstantiationException, IllegalAccessException
    {
        for (Column column : superColumn.getColumns())
        {
            embeddedObject = onColumn(column, m, embeddedObject, entityType, relationNames, isWrapReq, relations,
                    isCql3Enabled);
        }
    }

    /**
     * Scroll over counter super column.
     * 
     * @param m
     *            the m
     * @param relationNames
     *            the relation names
     * @param isWrapReq
     *            the is wrap req
     * @param relations
     *            the relations
     * @param entityType
     *            the entity type
     * @param superColumn
     *            the super column
     * @param embeddedObject
     *            the embedded object
     * @param isCql3Enabled
     *            the is cql3 enabled
     * @throws InstantiationException
     *             the instantiation exception
     * @throws IllegalAccessException
     *             the illegal access exception
     */
    private void scrollOverCounterSuperColumn(EntityMetadata m, List<String> relationNames, boolean isWrapReq,
            Map<String, Object> relations, EntityType entityType, CounterSuperColumn superColumn,
            Object embeddedObject, boolean isCql3Enabled) throws InstantiationException, IllegalAccessException
    {
        for (CounterColumn column : superColumn.getColumns())
        {
            onCounterColumn(column, m, embeddedObject, entityType, relationNames, isWrapReq, relations, isCql3Enabled);
        }
    }

    /**
     * Scroll over counter super column.
     * 
     * @param m
     *            the m
     * @param relationNames
     *            the relation names
     * @param isWrapReq
     *            the is wrap req
     * @param relations
     *            the relations
     * @param entityType
     *            the entity type
     * @param superColumn
     *            the super column
     * @param embeddedObject
     *            the embedded object
     * @param superColumnFieldMap
     *            the super column field map
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
     * @param m
     *            the m
     * @param relationNames
     *            the relation names
     * @param isWrapReq
     *            the is wrap req
     * @param relations
     *            the relations
     * @param entityType
     *            the entity type
     * @param superColumn
     *            the super column
     * @param embeddedObject
     *            the embedded object
     * @param superColumnFieldMap
     *            the super column field map
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
     * @param column
     *            the column
     * @param m
     *            the m
     * @param entity
     *            the entity
     * @param entityType
     *            the entity type
     * @param relationNames
     *            the relation names
     * @param isWrapReq
     *            the is wrap req
     * @param relations
     *            the relations
     * @param isCql3Enabled
     *            the is cql3 enabled
     * @return the object
     * @throws InstantiationException
     *             the instantiation exception
     * @throws IllegalAccessException
     *             the illegal access exception
     */
    private Object onColumn(Column column, EntityMetadata m, Object entity, EntityType entityType,
            List<String> relationNames, boolean isWrapReq, Map<String, Object> relations, boolean isCql3Enabled)
            throws InstantiationException, IllegalAccessException
    {
        String thriftColumnName = PropertyAccessorFactory.STRING.fromBytes(String.class, column.getName());
        byte[] thriftColumnValue = column.getValue();

        String discriminatorColumn = ((AbstractManagedType) entityType).getDiscriminatorColumn();
        String discriminatorValue = ((AbstractManagedType) entityType).getDiscriminatorValue();

        if (!thriftColumnName.equals(discriminatorColumn))
        {
            if (m.isCounterColumnType())
            {
                if (thriftColumnName.equals(((AbstractAttribute) m.getIdAttribute()).getJPAColumnName()))
                {
                    return populateViaThrift(m, entity, entityType, relationNames, relations, thriftColumnName,
                            column.getValue(), isCql3Enabled);
                }
                else
                {
                    LongAccessor accessor = new LongAccessor();
                    Long value = accessor.fromBytes(Long.class, column.getValue());
                    return populateViaThrift(m, entity, entityType, relationNames, relations, thriftColumnName,
                            value != null ? value.toString() : null, isCql3Enabled);
                }
            }

            return populateViaThrift(m, entity, entityType, relationNames, relations, thriftColumnName,
                    thriftColumnValue, isCql3Enabled);
        }
        return entity;
    }

    /**
     * On counter column.
     * 
     * @param column
     *            the column
     * @param m
     *            the m
     * @param entity
     *            the entity
     * @param entityType
     *            the entity type
     * @param relationNames
     *            the relation names
     * @param isWrapReq
     *            the is wrap req
     * @param relations
     *            the relations
     * @param isCql3Enabled
     *            the is cql3 enabled
     * @throws InstantiationException
     *             the instantiation exception
     * @throws IllegalAccessException
     *             the illegal access exception
     */
    private void onCounterColumn(CounterColumn column, EntityMetadata m, Object entity, EntityType entityType,
            List<String> relationNames, boolean isWrapReq, Map<String, Object> relations, boolean isCql3Enabled)
            throws InstantiationException, IllegalAccessException
    {
        String thriftColumnName = PropertyAccessorFactory.STRING.fromBytes(String.class, column.getName());
        String thriftColumnValue = new Long(column.getValue()).toString();
        populateViaThrift(m, entity, entityType, relationNames, relations, thriftColumnName, thriftColumnValue,
                isCql3Enabled);
    }

    /**
     * Populate via thrift.
     * 
     * @param m
     *            the m
     * @param entity
     *            the entity
     * @param entityType
     *            the entity type
     * @param relationNames
     *            the relation names
     * @param relations
     *            the relations
     * @param thriftColumnName
     *            the thrift column name
     * @param thriftColumnValue
     *            the thrift column value
     * @param isCql3Enabled
     *            the is cql3 enabled
     * @return the object
     * @throws InstantiationException
     *             the instantiation exception
     * @throws IllegalAccessException
     *             the illegal access exception
     */
    private Object populateViaThrift(EntityMetadata m, Object entity, EntityType entityType,
            List<String> relationNames, Map<String, Object> relations, String thriftColumnName,
            Object thriftColumnValue, boolean isCql3Enabled) throws InstantiationException, IllegalAccessException
    {
        if (relationNames == null || !relationNames.contains(thriftColumnName))
        {
            if (thriftColumnValue != null)
            {
                MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                        m.getPersistenceUnit());
                String fieldName = m.getFieldName(thriftColumnName);
                Attribute attribute = fieldName != null ? entityType.getAttribute(fieldName) : null;

                if (attribute != null)
                {
                    entity = KunderaCoreUtils.initialize(m, entity, null);
                    if (!attribute.isAssociation())
                    {
                        String idColumnName = ((AbstractAttribute) m.getIdAttribute()).getJPAColumnName();
                        if (!metaModel.isEmbeddable(m.getIdAttribute().getBindableJavaType())
                                && thriftColumnName.equals(idColumnName))
                        {
                            setId(m, entity, thriftColumnValue, isCql3Enabled);
                            PropertyAccessorHelper.setId(entity, m, (byte[]) thriftColumnValue);
                        }
                        // remove super column family check and populate
                        // embedded

                        if (isCql3Enabled && !m.isCounterColumnType())
                        {
                            if (metaModel.isEmbeddable(((AbstractAttribute) attribute).getBindableJavaType()))
                            {
                                if (attribute.isCollection())
                                {
                                    // element collection
                                    entity = setElementCollection(entity, thriftColumnValue, metaModel, attribute);
                                }
                                else
                                {
                                    entity = setUdtValue(entity, thriftColumnValue, metaModel, attribute);
                                }
                            }
                            else
                            {
                                setFieldValueViaCQL(entity, thriftColumnValue, attribute);
                            }
                        }
                        else
                        {
                            setFieldValue(entity, thriftColumnValue, attribute);
                        }
                    }
                }
                else if (metaModel.isEmbeddable(((AbstractAttribute) m.getIdAttribute()).getBindableJavaType())
                        && !isAggregate(thriftColumnName))
                {
                    entity = populateCompositeId(m, entity, thriftColumnName, thriftColumnValue, metaModel,
                            m.getIdAttribute(), m.getEntityClazz());
                }
                else if (clientBase.getCqlMetadata() != null)
                {
                    if (entity == null)
                    {
                        entity = new HashMap();
                    }
                    if (entity instanceof HashMap)
                    {
                        composeAndAdd((HashMap) entity, clientBase.getCqlMetadata(), thriftColumnValue,
                                thriftColumnName);
                    }
                }
            }
        }
        else
        {
            // populate relation.
            if (relationNames != null && relationNames.contains(thriftColumnName) && thriftColumnValue != null)
            {

                String fieldName = m.getFieldName(thriftColumnName);
                Attribute attribute = fieldName != null ? entityType.getAttribute(fieldName) : null;

                EntityMetadata relationMetadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata,
                        ((AbstractAttribute) attribute).getBindableJavaType());
                Object value;
                if (isCql3Enabled && !m.getType().equals(Type.SUPER_COLUMN_FAMILY))
                {
                    value = getFieldValueViaCQL(thriftColumnValue, relationMetadata.getIdAttribute());
                }
                else
                {
                    value = PropertyAccessorHelper.getObject(relationMetadata.getIdAttribute().getJavaType(),
                            (byte[]) thriftColumnValue);
                }
                relations.put(thriftColumnName, value);

                if (entity == null)
                {
                    entity = KunderaCoreUtils.initialize(m, entity, null);
                }
                // prepare EnhanceEntity and return it
            }

        }
        return entity;
    }

    /**
     * Checks if is aggregate.
     * 
     * @param thriftColumnName
     *            the thrift column name
     * @return true, if is aggregate
     */
    private boolean isAggregate(String thriftColumnName)
    {
        if (thriftColumnName.startsWith(SYS_COUNT) || thriftColumnName.startsWith(SYS_MIN)
                || thriftColumnName.startsWith(SYS_MAX) || thriftColumnName.startsWith(SYS_AVG)
                || thriftColumnName.startsWith(SYS_SUM))
        {
            return true;
        }
        else if (thriftColumnName.startsWith(COUNT) || thriftColumnName.startsWith(MIN)
                || thriftColumnName.startsWith(MAX) || thriftColumnName.startsWith(AVG)
                || thriftColumnName.startsWith(SUM))
        {
            return true;
        }
        return false;
    }

    /**
     * Compose and add.
     * 
     * @param entity
     *            the entity
     * @param cqlMetadata
     *            the cql metadata
     * @param thriftColumnValue
     *            the thrift column value
     * @param thriftColumnName
     *            the thrift column name
     */
    private void composeAndAdd(HashMap entity, CqlMetadata cqlMetadata, Object thriftColumnValue,
            String thriftColumnName)
    {
        byte[] columnName = thriftColumnName.getBytes();

        Map<ByteBuffer, String> schemaTypes = this.clientBase.getCqlMetadata().getValue_types();
        AbstractType<?> type = null;
        try
        {
            type = TypeParser.parse(schemaTypes.get(ByteBuffer.wrap((byte[]) columnName)));
        }
        catch (SyntaxException | ConfigurationException e)
        {
            log.error(e.getMessage());
            throw new KunderaException("Error while parsing CQL Type " + e);
        }

        entity.put(thriftColumnName, type.compose(ByteBuffer.wrap((byte[]) thriftColumnValue)));
    }

    /**
     * Sets the udt value.
     * 
     * @param entity
     *            the entity
     * @param thriftColumnValue
     *            the thrift column value
     * @param metaModel
     *            the meta model
     * @param attribute
     *            the attribute
     * @return the object
     */
    private Object setUdtValue(Object entity, Object thriftColumnValue, MetamodelImpl metaModel, Attribute attribute)
    {
        List<FieldIdentifier> fieldNames = new ArrayList<FieldIdentifier>();
        List<AbstractType<?>> fieldTypes = new ArrayList<AbstractType<?>>();

        String val = null;

        // get from cqlMetadata, details of types and names (for maintaining
        // order)
        Map<ByteBuffer, String> schemaTypes = this.clientBase.getCqlMetadata().getValue_types();
        for (Map.Entry<ByteBuffer, String> schemaType : schemaTypes.entrySet())
        {
            UTF8Serializer utf8Serializer = UTF8Serializer.instance;
            String key = utf8Serializer.deserialize((schemaType.getKey()));
            if (key.equals(((AbstractAttribute) attribute).getJavaMember().getName()))
            {
                val = schemaType.getValue();
                break;
            }
        }

        UserType userType = null;

        try
        {
            userType = UserType.getInstance(new TypeParser(val.substring(val.indexOf("UserType(") + 8, val.length())));
        }
        catch (ConfigurationException | SyntaxException e)
        {
            log.error(e.getMessage());
            throw new KunderaException("Error while getting instance of UserType " + e);
        }

        fieldNames = userType.fieldNames();
        fieldTypes = userType.allTypes();

        Field field = (Field) ((AbstractAttribute) attribute).getJavaMember();
        Class embeddedClass = ((AbstractAttribute) attribute).getBindableJavaType();

        Object embeddedObject = KunderaCoreUtils.createNewInstance(embeddedClass);

        Object finalValue = populateEmbeddedRecursive((ByteBuffer.wrap((byte[]) thriftColumnValue)), fieldTypes,
                fieldNames, embeddedObject, metaModel);

        PropertyAccessorHelper.set(entity, field, finalValue);

        return entity;
    }

    /**
     * Populate embedded recursive.
     * 
     * @param value
     *            the value
     * @param types
     *            the types
     * @param fieldNames
     *            the field names
     * @param entity
     *            the entity
     * @param metaModel
     *            the meta model
     * @return the object
     */
    public Object populateEmbeddedRecursive(ByteBuffer value, List<AbstractType<?>> types, List<FieldIdentifier> fieldNames,
            Object entity, MetamodelImpl metaModel)
    {
        ByteBuffer input = value.duplicate();
        EmbeddableType emb = metaModel.embeddable(entity.getClass());

        for (int i = 0; i < types.size(); i++)
        {

            if (!input.hasRemaining())
                return entity;

            AbstractType<?> type = types.get(i);
            String name = fieldNames.get(i).toString(); 		 // JPA name,
                                                                 // convert to
                                                                 // column name

            Field fieldToSet = null;
            AbstractAttribute attribute = null;
            // change this if possible
            for (Object attr : emb.getAttributes())
            {
                if (((AbstractAttribute) attr).getJPAColumnName().equals(name))
                {
                    attribute = (AbstractAttribute) attr;
                    break;
                }
            }
            fieldToSet = (Field) attribute.getJavaMember();
            Class embeddedClass = attribute.getBindableJavaType();

            int size = input.getInt();
            if (size < 0)
            {
                continue;
            }

            ByteBuffer field = ByteBufferUtil.readBytes(input, size);

            if (type.getClass().getSimpleName().equals("UserType"))
            {
                List<FieldIdentifier> subFieldNames = ((UserType) type).fieldNames();// ok
                List<AbstractType<?>> subfieldTypes = ((UserType) type).fieldTypes();

                // create entity with type_name and populate fields, set entity
                // in parent object after exit
                Object embeddedObjectChild = KunderaCoreUtils.createNewInstance(embeddedClass);

                Object processedEntity = populateEmbeddedRecursive(field, subfieldTypes, subFieldNames,
                        embeddedObjectChild, metaModel);
                PropertyAccessorHelper.set(entity, fieldToSet, processedEntity);
            }
            else
            {
                boolean flag = true;

                if (type.getClass().getSimpleName().equals("MapType"))
                {
                    if (((MapType) type).getValuesType().getClass().getSimpleName().equals("UserType"))
                    {
                        flag = false;
                        // create instance of embedded object (UserType)
                        setElementCollectionMap((MapType) type, field, entity, fieldToSet, metaModel, embeddedClass,
                                false);
                    }
                }
                else if (type.getClass().getSimpleName().equals("ListType"))
                {
                    if (((ListType) type).getElementsType().getClass().getSimpleName().equals("UserType"))
                    {
                        flag = false;
                        setElementCollectionList((ListType) type, field, entity, fieldToSet, metaModel, embeddedClass,
                                false);
                    }
                }
                else if (type.getClass().getSimpleName().equals("SetType"))
                {
                    if (((SetType) type).getElementsType().getClass().getSimpleName().equals("UserType"))
                    {
                        flag = false;
                        setElementCollectionSet((SetType) type, field, entity, fieldToSet, metaModel, embeddedClass,
                                false);
                    }
                }
                if (flag)
                {
                    TypeSerializer serializer = type.getSerializer();
                    serializer.validate(field);

                    Object finalValue = serializer.deserialize(field);
                    if (type.getClass().getSimpleName().equals("UTF8Type"))
                    {
                        PropertyAccessorHelper.set(entity, fieldToSet, ((String) finalValue).getBytes());
                    }
                    else
                    {
                        PropertyAccessorHelper.set(entity, fieldToSet, finalValue);
                    }
                }

            }

        }
        return entity;
    }

    /**
     * Sets the field value.
     * 
     * @param entity
     *            the entity
     * @param thriftColumnValue
     *            the thrift column value
     * @param attribute
     *            the attribute
     */
    private void setFieldValue(Object entity, Object thriftColumnValue, Attribute attribute)
    {
        if (attribute != null)
        {
            try
            {
                if (thriftColumnValue.getClass().isAssignableFrom(String.class))
                {
                    PropertyAccessorHelper.set(entity, (Field) attribute.getJavaMember(), (String) thriftColumnValue);
                }
                else if (CassandraDataTranslator.isCassandraDataTypeClass(((AbstractAttribute) attribute)
                        .getBindableJavaType()))
                {
                    Object decomposed = null;
                    try
                    {
                        Class<?> clazz = ((AbstractAttribute) attribute).getBindableJavaType();
                        decomposed = CassandraDataTranslator.decompose(clazz, thriftColumnValue, false);
                    }
                    catch (Exception e)
                    {
                        String tableName = entity.getClass().getSimpleName();
                        String fieldName = attribute.getName();
                        String msg = "Decomposing failed for `" + tableName + "`.`" + fieldName
                                + "`, did you set the correct type in your entity class?";
                        log.error(msg, e);
                        throw new KunderaException(msg, e);
                    }
                    PropertyAccessorHelper.set(entity, (Field) attribute.getJavaMember(), decomposed);
                }
                else
                {
                    PropertyAccessorHelper.set(entity, (Field) attribute.getJavaMember(), (byte[]) thriftColumnValue);
                }

            }
            catch (PropertyAccessException pae)
            {
                log.warn("Error while setting field value, Caused by: .", pae);
            }
        }
    }

    /**
     * Sets the field value via cql.
     * 
     * @param entity
     *            the entity
     * @param thriftColumnValue
     *            the thrift column value
     * @param attribute
     *            the attribute
     */
    private void setFieldValueViaCQL(Object entity, Object thriftColumnValue, Attribute attribute)
    {
        if (attribute != null)
        {
            try
            {
                if (attribute.isCollection())
                {
                    setCollectionValue(entity, thriftColumnValue, attribute);
                }
                else if (CassandraDataTranslator.isCassandraDataTypeClass(((AbstractAttribute) attribute)
                        .getBindableJavaType()))
                {
                    PropertyAccessorHelper.set(entity, (Field) attribute.getJavaMember(), CassandraDataTranslator
                            .decompose(((AbstractAttribute) attribute).getBindableJavaType(), thriftColumnValue, true));
                }
                else
                {
                    PropertyAccessorHelper.set(entity, (Field) attribute.getJavaMember(), (byte[]) thriftColumnValue);
                }
            }
            catch (PropertyAccessException pae)
            {
                log.warn("Error while setting field{} value via CQL, Caused by: .", attribute.getName(), pae);
            }
        }
    }

    /**
     * Sets the element collection.
     * 
     * @param entity
     *            the entity
     * @param thriftColumnValue
     *            the thrift column value
     * @param metaModel
     *            the meta model
     * @param attribute
     *            the attribute
     * @return the object
     */
    private Object setElementCollection(Object entity, Object thriftColumnValue, MetamodelImpl metaModel,
            Attribute attribute)
    {
        String cqlColumnMetadata = null;
        Map<ByteBuffer, String> schemaTypes = this.clientBase.getCqlMetadata().getValue_types();
        for (Map.Entry<ByteBuffer, String> schemaType : schemaTypes.entrySet())
        {

            String key = UTF8Serializer.instance.deserialize((schemaType.getKey()));
            if (key.equals(((AbstractAttribute) attribute).getJPAColumnName()))
            {
                cqlColumnMetadata = schemaType.getValue();
                break;
            }
        }

        Field field = (Field) ((AbstractAttribute) attribute).getJavaMember();
        Class embeddedClass = ((AbstractAttribute) attribute).getBindableJavaType();

        if (List.class.isAssignableFrom(((Field) attribute.getJavaMember()).getType()))
        {
            ListType listType = null;
            try
            {
                listType = ListType.getInstance(new TypeParser(cqlColumnMetadata.substring(
                        cqlColumnMetadata.indexOf("("), cqlColumnMetadata.length())));
            }
            catch (ConfigurationException | SyntaxException e)
            {
                log.error(e.getMessage());
                throw new KunderaException("Error while getting instance of ListType " + e);
            }
            return setElementCollectionList(listType, ByteBuffer.wrap((byte[]) thriftColumnValue), entity, field,
                    metaModel, embeddedClass, true);

        }
        else if (Set.class.isAssignableFrom(((Field) attribute.getJavaMember()).getType()))
        {
            SetType setType = null;
            try
            {
                setType = SetType.getInstance(new TypeParser(cqlColumnMetadata.substring(
                        cqlColumnMetadata.indexOf("("), cqlColumnMetadata.length())));
            }
            catch (ConfigurationException | SyntaxException e)
            {
                log.error(e.getMessage());
                throw new KunderaException("Error while getting instance of SetType " + e);
            }
            return setElementCollectionSet(setType, ByteBuffer.wrap((byte[]) thriftColumnValue), entity, field,
                    metaModel, embeddedClass, true);

        }
        else if (Map.class.isAssignableFrom(((Field) attribute.getJavaMember()).getType()))
        {
            MapType mapType = null;
            try
            {
                mapType = MapType.getInstance(new TypeParser(cqlColumnMetadata.substring(
                        cqlColumnMetadata.indexOf("("), cqlColumnMetadata.length())));
            }
            catch (ConfigurationException | SyntaxException e)
            {
                log.error(e.getMessage());
                throw new KunderaException("Error while getting instance of MapType " + e);
            }
            return setElementCollectionMap(mapType, ByteBuffer.wrap((byte[]) thriftColumnValue), entity, field,
                    metaModel, embeddedClass, true);
        }

        return entity;

    }

    /**
     * Sets the element collection map.
     * 
     * @param mapType
     *            the cql column metadata
     * @param thriftColumnValue
     *            the thrift column value
     * @param entity
     *            the entity
     * @param field
     *            the field
     * @param metaModel
     *            the meta model
     * @param embeddedClass
     *            the embedded class
     * @param useNativeProtocol2
     *            the use native protocol2
     * @return the object
     */
    private Object setElementCollectionMap(MapType mapType, ByteBuffer thriftColumnValue, Object entity, Field field,
            MetamodelImpl metaModel, Class embeddedClass, boolean useNativeProtocol2)
    {

        Map result = new HashMap();
        MapSerializer mapSerializer = mapType.getSerializer();
        Map outputCollection = new HashMap();
        if (useNativeProtocol2)
        {
            outputCollection.putAll(mapSerializer.deserializeForNativeProtocol(thriftColumnValue, ProtocolVersion.V2));
        }
        else
        {
            outputCollection.putAll((Map) mapSerializer.deserialize(thriftColumnValue));
        }

        UserType usertype = (UserType) mapType.getValuesType();

        for (Object key : outputCollection.keySet())
        {
            Object embeddedObject = KunderaCoreUtils.createNewInstance(embeddedClass);
            Object value = populateEmbeddedRecursive((ByteBuffer) outputCollection.get(key), usertype.allTypes(),
                    usertype.fieldNames(), embeddedObject, metaModel);
            result.put(key, value);
        }
        PropertyAccessorHelper.set(entity, field, result);
        return entity;
    }

    /**
     * Sets the element collection set.
     * 
     * @param setType
     *            the cql column metadata
     * @param thriftColumnValue
     *            the thrift column value
     * @param entity
     *            the entity
     * @param field
     *            the field
     * @param metaModel
     *            the meta model
     * @param embeddedClass
     *            the embedded class
     * @param useNativeProtocol2
     *            the use native protocol2
     * @return the object
     */
    private Object setElementCollectionSet(SetType setType, ByteBuffer thriftColumnValue, Object entity, Field field,
            MetamodelImpl metaModel, Class embeddedClass, boolean useNativeProtocol2)
    {

        SetSerializer setSerializer = setType.getSerializer();
        Collection outputCollection = new ArrayList();
        if (useNativeProtocol2)
        {
            outputCollection.addAll((Collection) setSerializer.deserializeForNativeProtocol(thriftColumnValue, ProtocolVersion.V2));
        }
        else
        {
            outputCollection.addAll((Collection) setSerializer.deserialize(thriftColumnValue));
        }

        UserType usertype = (UserType) setType.getElementsType();
        Collection result = new HashSet();
        Iterator collectionItems = outputCollection.iterator();
        while (collectionItems.hasNext())
        {
            Object embeddedObject = KunderaCoreUtils.createNewInstance(embeddedClass);
            Object value = populateEmbeddedRecursive((ByteBuffer) collectionItems.next(), usertype.allTypes(),
                    usertype.fieldNames(), embeddedObject, metaModel);
            result.add(value);
        }
        PropertyAccessorHelper.set(entity, field, result);
        return entity;
    }

    /**
     * Sets the element collection list.
     * 
     * @param listType
     *            the cql column metadata
     * @param thriftColumnValue
     *            the thrift column value
     * @param entity
     *            the entity
     * @param field
     *            the field
     * @param metaModel
     *            the meta model
     * @param embeddedClass
     *            the embedded class
     * @param useNativeProtocol2
     *            the use native protocol2
     * @return the object
     */
    private Object setElementCollectionList(ListType listType, ByteBuffer thriftColumnValue, Object entity,
            Field field, MetamodelImpl metaModel, Class embeddedClass, boolean useNativeProtocol2)
    {

        ListSerializer listSerializer = listType.getSerializer();
        Collection outputCollection = new ArrayList();
        if (useNativeProtocol2)
        {
            outputCollection.addAll((Collection) listSerializer.deserializeForNativeProtocol(thriftColumnValue, ProtocolVersion.V2));
        }
        else
        {
            outputCollection.addAll((Collection) listSerializer.deserialize(thriftColumnValue));
        }

        UserType usertype = (UserType) listType.getElementsType();
        Collection result = new ArrayList();
        Iterator collectionItems = outputCollection.iterator();
        while (collectionItems.hasNext())
        {
            Object embeddedObject = KunderaCoreUtils.createNewInstance(embeddedClass);
            Object value = populateEmbeddedRecursive((ByteBuffer) collectionItems.next(), usertype.allTypes(),
                    usertype.fieldNames(), embeddedObject, metaModel);
            result.add(value);
        }
        PropertyAccessorHelper.set(entity, field, result);
        return entity;
    }

    /**
     * Gets the field value via cql.
     * 
     * @param thriftColumnValue
     *            the thrift column value
     * @param attribute
     *            the attribute
     * @return the field value via cql
     */
    private Object getFieldValueViaCQL(Object thriftColumnValue, Attribute attribute)
    {
        PropertyAccessor<?> accessor = PropertyAccessorFactory.getPropertyAccessor((Field) attribute.getJavaMember());
        Object objValue;
        try
        {
            if (CassandraDataTranslator.isCassandraDataTypeClass(((AbstractAttribute) attribute).getBindableJavaType()))
            {
                objValue = CassandraDataTranslator.decompose(((AbstractAttribute) attribute).getBindableJavaType(),
                        thriftColumnValue, true);
                return objValue;
            }
            else
            {
                objValue = accessor.fromBytes(((AbstractAttribute) attribute).getBindableJavaType(),
                        (byte[]) thriftColumnValue);
                return objValue;
            }
        }
        catch (PropertyAccessException pae)
        {
            log.warn("Error while setting field{} value via CQL, Caused by: .", attribute.getName(), pae);
        }
        return null;
    }

    /**
     * On column or super column thrift row.
     * 
     * @param m
     *            the m
     * @param e
     *            the e
     * @param id
     *            the id
     * @param timestamp
     *            the timestamp2
     * @param columnTTLs
     *            TODO
     * @return the collection
     */

    private Collection<ThriftRow> onColumnOrSuperColumnThriftRow(EntityMetadata m, Object e, Object id, long timestamp,
            Object columnTTLs)
    {
        // Iterate through Super columns

        Map<String, ThriftRow> thriftRows = new HashMap<String, ThriftRow>();

        MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                m.getPersistenceUnit());

        EntityType entityType = metaModel.entity(m.getEntityClazz());

        Set<Attribute> attributes = entityType.getAttributes();
        for (Attribute attribute : attributes)
        {
            String tableName = ((AbstractAttribute) attribute).getTableName() != null ? ((AbstractAttribute) attribute)
                    .getTableName() : m.getTableName();

            ThriftRow tr = getThriftRow(id, tableName, thriftRows);
            if (!attribute.getName().equals(m.getIdAttribute().getName()) && !attribute.isAssociation())
            {
                Field field = (Field) ((Attribute) attribute).getJavaMember();
                byte[] name = ByteBufferUtil.bytes(((AbstractAttribute) attribute).getJPAColumnName()).array();

                // if attribute is embeddable.
                if (metaModel.isEmbeddable(attribute.isCollection() ? ((PluralAttribute) attribute)
                        .getBindableJavaType() : attribute.getJavaType()))
                {
                    Map<String, Object> thriftSuperColumns = onEmbeddable(timestamp, tr, m, e, id, attribute);
                    if (thriftSuperColumns != null)
                    {
                        for (String columnFamilyName : thriftSuperColumns.keySet())
                        {
                            ThriftRow thriftRow = getThriftRow(id, columnFamilyName, thriftRows);
                            if (m.isCounterColumnType())
                            {
                                thriftRow.addCounterSuperColumn((CounterSuperColumn) thriftSuperColumns
                                        .get(columnFamilyName));
                            }
                            else
                            {
                                thriftRow.addSuperColumn((SuperColumn) thriftSuperColumns.get(columnFamilyName));
                            }
                        }
                    }
                }
                else
                {
                    Object value = getColumnValue(m, e, attribute);

                    if (m.getType().equals(Type.SUPER_COLUMN_FAMILY))
                    {
                        prepareSuperColumn(tr, m, value, name, timestamp);
                    }
                    else
                    {
                        int ttl = getTTLForColumn(columnTTLs, attribute);
                        prepareColumn(tr, m, value, name, timestamp, ttl);
                    }
                }
            }
        }

        // Add discriminator column.
        onDiscriminatorColumn(thriftRows.get(m.getTableName()), timestamp, entityType);
        return thriftRows.values();
    }

    /**
     * On discriminator column.
     * 
     * @param tr
     *            the tr
     * @param timestamp
     *            the timestamp
     * @param entityType
     *            the entity type
     */
    private void onDiscriminatorColumn(ThriftRow tr, long timestamp, EntityType entityType)
    {
        String discrColumn = ((AbstractManagedType) entityType).getDiscriminatorColumn();
        String discrValue = ((AbstractManagedType) entityType).getDiscriminatorValue();

        // No need to check for empty or blank, as considering it as valid name
        // for nosql!
        if (discrColumn != null && discrValue != null)
        {
            Column column = prepareColumn(PropertyAccessorHelper.getBytes(discrValue),
                    PropertyAccessorHelper.getBytes(discrColumn), timestamp, 0);
            tr.addColumn(column);
        }
    }

    /**
     * Determined TTL for a given column.
     * 
     * @param columnTTLs
     *            the column tt ls
     * @param attribute
     *            the attribute
     * @return the TTL for column
     */
    private int getTTLForColumn(Object columnTTLs, Attribute attribute)
    {
        Integer ttl = null;
        if (columnTTLs != null)
        {
            if (columnTTLs instanceof Map)
            {
                ttl = (Integer) (columnTTLs == null ? 0 : ((Map) columnTTLs).get(((AbstractAttribute) attribute)
                        .getJPAColumnName()));
            }
            else if (columnTTLs instanceof Integer)
            {
                ttl = (Integer) columnTTLs;
            }
        }
        return ttl == null ? 0 : ttl;
    }

    /**
     * Gets the column value.
     * 
     * @param m
     *            the m
     * @param e
     *            the e
     * @param attribute
     *            the attribute
     * @return the column value
     */
    private Object getColumnValue(EntityMetadata m, Object e, Attribute attribute)
    {
        Field field = (Field) ((Attribute) attribute).getJavaMember();
        Object value;
        if (!m.isCounterColumnType())
        {
            value = getThriftColumnValue(e, attribute);
        }
        else
        {
            value = PropertyAccessorHelper.getString(e, field);
        }
        return value;
    }

    /**
     * Gets the thrift column value.
     * 
     * @param e
     *            the e
     * @param attribute
     *            the attribute
     * @return the thrift column value
     */
    protected byte[] getThriftColumnValue(Object e, Attribute attribute)
    {
        byte[] value = null;
        Field field = (Field) ((Attribute) attribute).getJavaMember();
        try
        {
            if (attribute != null && field.get(e) != null)
            {

                if (CassandraDataTranslator.isCassandraDataTypeClass(((AbstractAttribute) attribute)
                        .getBindableJavaType()))
                {
                    value = CassandraDataTranslator.compose(((AbstractAttribute) attribute).getBindableJavaType(),
                            field.get(e), false);
                }
                else
                {
                    value = PropertyAccessorHelper.get(e, field);
                }
            }
        }
        catch (IllegalArgumentException iae)
        {
            log.error("Error while persisting data, Caused by: .", iae);
            throw new IllegalArgumentException(iae);
        }
        catch (IllegalAccessException iace)
        {
            log.error("Error while persisting data, Caused by: .", iace);
        }
        return value;
    }

    /**
     * Prepare column.
     * 
     * @param tr
     *            the tr
     * @param m
     *            the m
     * @param value
     *            the value
     * @param name
     *            the name
     * @param timestamp
     *            the timestamp
     * @param ttl
     *            TODO
     */
    private void prepareColumn(ThriftRow tr, EntityMetadata m, Object value, byte[] name, long timestamp, int ttl)
    {
        if (value != null)
        {
            if (m.isCounterColumnType())
            {
                CounterColumn counterColumn = prepareCounterColumn((String) value, name);
                tr.addCounterColumn(counterColumn);
            }
            else
            {
                Column column = prepareColumn((byte[]) value, name, timestamp, ttl);
                tr.addColumn(column);
            }
        }
    }

    /**
     * Prepare super column.
     * 
     * @param tr
     *            the tr
     * @param m
     *            the m
     * @param value
     *            the value
     * @param name
     *            the name
     * @param timestamp
     *            the timestamp
     */
    private void prepareSuperColumn(ThriftRow tr, EntityMetadata m, Object value, byte[] name, long timestamp)
    {
        if (value != null)
        {
            if (m.isCounterColumnType())
            {
                CounterSuperColumn counterSuper = new CounterSuperColumn();
                counterSuper.setName(name);
                CounterColumn counterColumn = prepareCounterColumn((String) value, name);
                List<CounterColumn> subCounterColumn = new ArrayList<CounterColumn>();
                subCounterColumn.add(counterColumn);
                counterSuper.setColumns(subCounterColumn);
                tr.addCounterSuperColumn(counterSuper);
            }
            else
            {
                SuperColumn superCol = new SuperColumn();
                superCol.setName(name);
                Column column = prepareColumn((byte[]) value, name, timestamp, 0);
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
     * @param value
     *            the value
     * @param name
     *            the name
     * @param timestamp
     *            the timestamp
     * @param ttl
     *            TODO
     * @return the column
     */
    private Column prepareColumn(byte[] value, byte[] name, long timestamp, int ttl)
    {
        Column column = new Column();
        column.setName(name);
        column.setValue(value);
        column.setTimestamp(timestamp);
        if (ttl != 0)
        {
            column.setTtl(ttl);
        }
        return column;
    }

    /**
     * Prepare counter column.
     * 
     * @param value
     *            the value
     * @param name
     *            the name
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
     * @param timestamp2
     *            the timestamp2
     * @param tr
     *            the tr
     * @param m
     *            the m
     * @param e
     *            the e
     * @param id
     *            the id
     * @param embeddableAttrib
     *            the embeddable attrib
     * @return the map
     */
    private Map<String, Object> onEmbeddable(long timestamp2, ThriftRow tr, EntityMetadata m, Object e, Object id,
            Attribute embeddableAttrib)
    {

        MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                m.getPersistenceUnit());

        EmbeddableType superColumn = metaModel.embeddable(((AbstractAttribute) embeddableAttrib).getBindableJavaType());

        Field superColumnField = (Field) embeddableAttrib.getJavaMember();
        Object superColumnObject = PropertyAccessorHelper.getObject(e, superColumnField);

        // If Embedded object is a Collection, there will be variable number
        // of super columns one for each object in collection.
        // Key for each super column will be of the format "<Embedded object
        // field name>#<Unique sequence number>

        // On the other hand, if embedded object is not a Collection, it
        // would simply be embedded as ONE super column.
        String superColumnName = null;
        if (superColumnObject != null && superColumnObject instanceof Collection)
        {

            ElementCollectionCacheManager ecCacheHandler = ElementCollectionCacheManager.getInstance();

            // Check whether it's first time insert or updation
            if (ecCacheHandler.isCacheEmpty())
            { // First time insert
                int count = 0;
                for (Object obj : (Collection) superColumnObject)
                {
                    superColumnName = ((AbstractAttribute) embeddableAttrib).getJPAColumnName()
                            + Constants.EMBEDDED_COLUMN_NAME_DELIMITER + count;

                    if (m.isCounterColumnType())
                    {
                        CounterSuperColumn thriftSuperColumn = (CounterSuperColumn) buildThriftCounterSuperColumn(
                                m.getTableName(), superColumnName, superColumn, obj).get(m.getTableName());
                        tr.addCounterSuperColumn(thriftSuperColumn);
                    }
                    else
                    {
                        SuperColumn thriftSuperColumn = (SuperColumn) buildThriftSuperColumn(m.getTableName(),
                                superColumnName, timestamp2, superColumn, obj).get(m.getTableName());
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
                    Map<String, Object> thriftSuperColumn = buildThriftSuperColumn(timestamp2, m, id, superColumn,
                            superColumnName, obj);
                    if (m.isCounterColumnType())
                    {
                        tr.addCounterSuperColumn((CounterSuperColumn) thriftSuperColumn.get(m.getTableName()));
                    }
                    else
                    {
                        tr.addSuperColumn((SuperColumn) thriftSuperColumn.get(m.getTableName()));
                    }
                    ecCacheHandler.addElementCollectionCacheMapping(id, obj, superColumnName);
                }
            }
        }
        else if (superColumnObject != null)
        {
            superColumnName = ((AbstractAttribute) embeddableAttrib).getJPAColumnName();
            return buildThriftSuperColumn(timestamp2, m, id, superColumn, superColumnName, superColumnObject);
        }
        return null;
    }

    /**
     * Builds the thrift super column.
     * 
     * @param timestamp2
     *            the timestamp2
     * @param m
     *            the m
     * @param id
     *            the id
     * @param superColumn
     *            the super column
     * @param superColumnName
     *            the super column name
     * @param obj
     *            the obj
     * @return the map
     */
    private Map<String, Object> buildThriftSuperColumn(long timestamp2, EntityMetadata m, Object id,
            EmbeddableType superColumn, String superColumnName, Object obj)
    {
        Map<String, Object> thriftSuperColumns = null;
        if (m.isCounterColumnType())
        {
            thriftSuperColumns = buildThriftCounterSuperColumn(m.getTableName(), superColumnName, superColumn, obj);
        }
        else
        {
            thriftSuperColumns = buildThriftSuperColumn(m.getTableName(), superColumnName, timestamp2, superColumn, obj);
        }
        return thriftSuperColumns;
    }

    /**
     * Builds the thrift counter super column.
     * 
     * @param tableName
     *            the table name
     * @param superColumnName
     *            the super column name
     * @param superColumn
     *            the super column
     * @param counterSuperColumnObject
     *            the counter super column object
     * @return the counter super column
     */
    private Map<String, Object> buildThriftCounterSuperColumn(String tableName, String superColumnName,
            EmbeddableType superColumn, Object counterSuperColumnObject)
    {

        Map<String, Object> thriftCounterSuperColumns = new HashMap<String, Object>();

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
                if (log.isInfoEnabled())
                {
                    log.info(exp.getMessage()
                            + ". Possible case of entity column in a super column family. Will be treated as a super column.");
                }

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

                    tableName = ((AbstractAttribute) column).getTableName() != null ? ((AbstractAttribute) column)
                            .getTableName() : tableName;
                    CounterSuperColumn thriftSuperColumn = (CounterSuperColumn) thriftCounterSuperColumns
                            .get(tableName);
                    if (thriftSuperColumn == null)
                    {
                        thriftSuperColumn = new CounterSuperColumn();
                        thriftSuperColumn.setName(PropertyAccessorFactory.STRING.toBytes(superColumnName));
                        thriftCounterSuperColumns.put(tableName, thriftSuperColumn);
                    }
                    thriftSuperColumn.addToColumns(thriftColumn);
                }
                catch (NumberFormatException nfe)
                {
                    log.error("For counter column arguments should be numeric type, Caused by: .", nfe);
                    throw new KunderaException(nfe);
                }
            }
        }
        return thriftCounterSuperColumns;
    }

    /**
     * Builds the thrift super column.
     * 
     * @param tableName
     *            the table name
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
    private Map<String, Object> buildThriftSuperColumn(String tableName, String superColumnName, long timestamp,
            EmbeddableType superColumn, Object superColumnObject) throws PropertyAccessException
    {
        Map<String, Object> tableToSuperColumns = new HashMap<String, Object>();

        List<Column> thriftColumns = new ArrayList<Column>();

        Iterator<Attribute> iter = superColumn.getAttributes().iterator();

        while (iter.hasNext())
        {
            AbstractAttribute column = (AbstractAttribute) iter.next();

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
                if (log.isInfoEnabled())
                {
                    log.info(exp.getMessage()
                            + ". Possible case of entity column in a super column family. Will be treated as a super column.");
                }
                value = superColumnObject.toString().getBytes();
            }
            if (null != value)
            {
                Column thriftColumn = new Column();
                thriftColumn.setName(PropertyAccessorFactory.STRING.toBytes(name));
                thriftColumn.setValue(value);
                thriftColumn.setTimestamp(timestamp);
                thriftColumns.add(thriftColumn);
                String columnFamilyName = ((AbstractAttribute) column).getTableName() != null ? ((AbstractAttribute) column)
                        .getTableName() : tableName;
                SuperColumn thriftSuperColumn = (SuperColumn) tableToSuperColumns.get(tableName);
                if (thriftSuperColumn == null)
                {
                    thriftSuperColumn = new SuperColumn();
                    thriftSuperColumn.setName(PropertyAccessorFactory.STRING.toBytes(superColumnName));
                    tableToSuperColumns.put(columnFamilyName, thriftSuperColumn);
                }
                thriftSuperColumn.addToColumns(thriftColumn);
            }
        }
        return tableToSuperColumns;
    }

    /**
     * Populate composite id.
     * 
     * @param m
     *            the m
     * @param entity
     *            the entity
     * @param thriftColumnName
     *            the thrift column name
     * @param thriftColumnValue
     *            the thrift column value
     * @param metaModel
     *            the meta model
     * @param attribute
     *            the attribute
     * @param entityClazz
     *            the entity clazz
     * @return the object
     * @throws InstantiationException
     *             the instantiation exception
     * @throws IllegalAccessException
     *             the illegal access exception
     */
    private Object populateCompositeId(EntityMetadata m, Object entity, String thriftColumnName,
            Object thriftColumnValue, MetamodelImpl metaModel, Attribute attribute, Class entityClazz)
            throws InstantiationException, IllegalAccessException
    {
        Class javaType = ((AbstractAttribute) attribute).getBindableJavaType();

        if (metaModel.isEmbeddable(javaType))
        {
            EmbeddableType compoundKey = metaModel.embeddable(javaType);
            Object compoundKeyObject = null;
            try
            {
                Set<Attribute> attributes = compoundKey.getAttributes();
                entity = KunderaCoreUtils.initialize(entityClazz, entity);

                for (Attribute compoundAttribute : attributes)
                {
                    compoundKeyObject = compoundKeyObject == null ? getCompoundKey(attribute, entity)
                            : compoundKeyObject;

                    if (metaModel.isEmbeddable(((AbstractAttribute) compoundAttribute).getBindableJavaType()))
                    {
                        Object compoundObject = populateCompositeId(m, compoundKeyObject, thriftColumnName,
                                thriftColumnValue, metaModel, compoundAttribute, javaType);
                        PropertyAccessorHelper.set(entity, (Field) attribute.getJavaMember(), compoundObject);
                    }
                    else if (((AbstractAttribute) compoundAttribute).getJPAColumnName().equals(thriftColumnName))
                    {
                        setFieldValueViaCQL(compoundKeyObject, thriftColumnValue, compoundAttribute);
                        PropertyAccessorHelper.set(entity, (Field) attribute.getJavaMember(), compoundKeyObject);
                        break;
                    }
                }
            }
            catch (IllegalArgumentException iaex)
            {
                // ignore as it might not represented within entity.
                // No need for any logger message
            }
            catch (Exception e)
            {
                log.error("Error while retrieving data, Caused by: .", e);
                throw new PersistenceException(e);
            }
        }
        return entity;
    }

    /**
     * Gets the compound key.
     * 
     * @param attribute
     *            the attribute
     * @param entity
     *            the entity
     * @return the compound key
     * @throws InstantiationException
     *             the instantiation exception
     * @throws IllegalAccessException
     *             the illegal access exception
     */
    private Object getCompoundKey(Attribute attribute, Object entity) throws InstantiationException,
            IllegalAccessException
    {
        Object compoundKeyObject = null;
        if (entity != null)
        {
            compoundKeyObject = PropertyAccessorHelper.getObject(entity, (Field) attribute.getJavaMember());
            if (compoundKeyObject == null)
            {
                compoundKeyObject = ((AbstractAttribute) attribute).getBindableJavaType().newInstance();
            }
        }
        return compoundKeyObject;
    }

    /**
     * Populates collection field(s) into entity.
     * 
     * @param entity
     *            the entity
     * @param thriftColumnValue
     *            the thrift column value
     * @param attribute
     *            the attribute
     */
    private void setCollectionValue(Object entity, Object thriftColumnValue, Attribute attribute)
    {
        try
        {
            ByteBuffer valueByteBuffer = ByteBuffer.wrap((byte[]) thriftColumnValue);
            if (Collection.class.isAssignableFrom(((Field) attribute.getJavaMember()).getType()))
            {

                Class<?> genericClass = PropertyAccessorHelper.getGenericClass((Field) attribute.getJavaMember());

                PropertyAccessorHelper.set(entity, (Field) attribute.getJavaMember(), CassandraDataTranslator
                        .decompose(((Field) attribute.getJavaMember()).getType(), valueByteBuffer, genericClass, true));
            }

            else if (((Field) attribute.getJavaMember()).getType().isAssignableFrom(Map.class))
            {

                List<Class<?>> mapGenericClasses = PropertyAccessorHelper.getGenericClasses((Field) attribute
                        .getJavaMember());

                PropertyAccessorHelper.set(entity, (Field) attribute.getJavaMember(), CassandraDataTranslator
                        .decompose(((Field) attribute.getJavaMember()).getType(), valueByteBuffer, mapGenericClasses,
                                true));
            }
        }
        catch (Exception e)
        {
            log.error("Error while setting field{} value via CQL, Caused by: .", attribute.getName(), e);
            throw new PersistenceException(e);
        }
    }

}
