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
package com.impetus.client.cassandra;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javassist.Modifier;

import javax.persistence.PersistenceException;
import javax.persistence.Transient;
import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.EmbeddableType;
import javax.persistence.metamodel.EntityType;
import javax.persistence.metamodel.ManagedType;
import javax.persistence.metamodel.SingularAttribute;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.serializers.CollectionSerializer;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnDef;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.CounterColumn;
import org.apache.cassandra.thrift.CounterSuperColumn;
import org.apache.cassandra.thrift.CqlMetadata;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.CqlRow;
import org.apache.cassandra.thrift.IndexClause;
import org.apache.cassandra.thrift.IndexExpression;
import org.apache.cassandra.thrift.IndexType;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.SuperColumn;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.cassandra.common.CassandraConstants;
import com.impetus.client.cassandra.common.CassandraUtilities;
import com.impetus.client.cassandra.config.CassandraPropertyReader;
import com.impetus.client.cassandra.datahandler.CassandraDataHandler;
import com.impetus.client.cassandra.schemamanager.CassandraDataTranslator;
import com.impetus.client.cassandra.schemamanager.CassandraValidationClassMapper;
import com.impetus.client.cassandra.thrift.CQLTranslator;
import com.impetus.client.cassandra.thrift.CQLTranslator.TranslationType;
import com.impetus.client.cassandra.thrift.ThriftDataResultHelper;
import com.impetus.client.cassandra.thrift.ThriftRow;
import com.impetus.kundera.Constants;
import com.impetus.kundera.KunderaException;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.ClientBase;
import com.impetus.kundera.client.ClientPropertiesSetter;
import com.impetus.kundera.client.EnhanceEntity;
import com.impetus.kundera.db.DataRow;
import com.impetus.kundera.db.RelationHolder;
import com.impetus.kundera.db.SearchResult;
import com.impetus.kundera.graph.Node;
import com.impetus.kundera.lifecycle.states.RemovedState;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.EntityMetadata.Type;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;
import com.impetus.kundera.metadata.model.annotation.DefaultEntityAnnotationProcessor;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.metadata.model.type.AbstractManagedType;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.persistence.context.jointable.JoinTableData;
import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessor;
import com.impetus.kundera.property.PropertyAccessorFactory;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.property.accessor.StringAccessor;
import com.impetus.kundera.utils.KunderaCoreUtils;
import com.impetus.kundera.utils.TimestampGenerator;
import com.impetus.kundera.query.KunderaQuery;

/**
 * Base Class for all Cassandra Clients Contains methods that are applicable to (but not specific to) different
 * Cassandra clients.
 * 
 * @author amresh.singh
 */
public abstract class CassandraClientBase extends ClientBase implements ClientPropertiesSetter {

    /** log for this class. */
    private static Logger log = LoggerFactory.getLogger(CassandraClientBase.class);

    /** The cql version. */
    private String cqlVersion = CassandraConstants.CQL_VERSION_2_0;

    /** The consistency level. */
    protected ConsistencyLevel consistencyLevel = ConsistencyLevel.ONE;

    /** The ttl per request. */
    private boolean ttlPerRequest = false;

    /** The ttl per session. */
    private boolean ttlPerSession = false;

    /** The ttl values. */
    private Map<String, Object> ttlValues = new HashMap<String, Object>();

    /** The closed. */
    private volatile boolean closed = false;

    /** list of nodes for batch processing. */
    private List<Node> nodes = new ArrayList<Node>();

    /** batch size. */
    private int batchSize;

    /** The cql client. */
    protected final CQLClient cqlClient;

    /** The generator. */
    protected final TimestampGenerator generator;

    /** The cql metadata. */
    private CqlMetadata cqlMetadata;

    /**
     * constructor using fields.
     * 
     * @param persistenceUnit
     *            the persistence unit
     * @param externalProperties
     *            the external properties
     * @param kunderaMetadata
     *            the kundera metadata
     * @param generator
     *            the generator
     */
    protected CassandraClientBase(String persistenceUnit, Map<String, Object> externalProperties,
        final KunderaMetadata kunderaMetadata, final TimestampGenerator generator) {
        super(kunderaMetadata, externalProperties, persistenceUnit);
        this.cqlClient = new CQLClient();
        this.generator = generator;
        setBatchSize(persistenceUnit, this.externalProperties);
        populateCqlVersion(externalProperties);
    }

    /**
     * Populates foreign key as column.
     * 
     * @param rlName
     *            relation name
     * @param rlValue
     *            relation value
     * @param timestamp
     *            the timestamp
     * @return the column
     * @throws PropertyAccessException
     *             the property access exception
     */
    protected Column populateFkey(String rlName, Object rlValue, long timestamp) throws PropertyAccessException {
        Column col = new Column();
        col.setName(PropertyAccessorFactory.STRING.toBytes(rlName));
        col.setValue(PropertyAccessorHelper.getBytes(rlValue));
        col.setTimestamp(timestamp);
        return col;
    }

    /**
     * On counter column.
     * 
     * @param m
     *            the m
     * @param isRelation
     *            the is relation
     * @param relations
     *            the relations
     * @param ks
     *            the ks
     * @return the list
     */
    protected List<Object> onCounterColumn(EntityMetadata m, boolean isRelation, List<String> relations,
        List<KeySlice> ks) {
        List<Object> entities;

        if (m.getType().isSuperColumnFamilyMetadata()) {
            if (log.isInfoEnabled()) {
                log.info("On counter column for super column family of entity {}.", m.getEntityClazz());
            }

            // TODO:: change it. remove column or super column helper
            Map<byte[], List<CounterSuperColumn>> results = new HashMap<byte[], List<CounterSuperColumn>>();

            List<CounterSuperColumn> counterColumns = null;
            for (KeySlice slice : ks) {
                counterColumns = new ArrayList<CounterSuperColumn>(slice.getColumnsSize());
                for (ColumnOrSuperColumn column : slice.columns) {
                    counterColumns.add(column.counter_super_column);
                }

                results.put(slice.getKey(), counterColumns);
            }

            entities = new ArrayList<Object>(results.size());

            for (byte[] key : results.keySet()) {
                Object e = null;
                Object id = PropertyAccessorHelper.getObject(m.getIdAttribute().getJavaType(), key);
                List<CounterSuperColumn> counterSuperColumns = results.get(key);
                ThriftRow tr = new ThriftRow(id, m.getTableName(), new ArrayList<Column>(0),
                    new ArrayList<SuperColumn>(0), new ArrayList<CounterColumn>(0), counterSuperColumns);
                e = getDataHandler().populateEntity(tr, m, KunderaCoreUtils.getEntity(e), relations, isRelation);
                entities.add(e);
            }
        } else {
            if (log.isInfoEnabled()) {
                log.info("On counter column for column family of entity {}", m.getEntityClazz());
            }

            Map<byte[], List<CounterColumn>> results = new HashMap<byte[], List<CounterColumn>>();

            List<CounterColumn> counterColumns = null;

            for (KeySlice slice : ks) {
                counterColumns = new ArrayList<CounterColumn>(slice.getColumnsSize());
                for (ColumnOrSuperColumn column : slice.columns) {
                    counterColumns.add(column.counter_column);
                }

                results.put(slice.getKey(), counterColumns);
            }

            entities = new ArrayList<Object>(results.size());

            for (byte[] key : results.keySet()) {
                Object e = null;
                Object id = PropertyAccessorHelper.getObject(m.getIdAttribute().getJavaType(), key);

                List<CounterColumn> columns = results.get(key);
                ThriftRow tr = new ThriftRow(id, m.getTableName(), new ArrayList<Column>(0),
                    new ArrayList<SuperColumn>(0), columns, new ArrayList<CounterSuperColumn>(0));
                e = getDataHandler().populateEntity(tr, m, KunderaCoreUtils.getEntity(e), relations, isRelation);

                if (e != null) {
                    entities.add(e);
                }
            }
        }
        return entities;
    }

    /**
     * Compute entity via columns.
     * 
     * @param m
     *            the m
     * @param isRelation
     *            the is relation
     * @param relations
     *            the relations
     * @param entities
     *            the entities
     * @param qResults
     *            the q results
     */
    protected void computeEntityViaColumns(EntityMetadata m, boolean isRelation, List<String> relations,
        List<Object> entities, Map<ByteBuffer, List<Column>> qResults) {
        MetamodelImpl metaModel =
            (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(m.getPersistenceUnit());

        EntityType entityType = metaModel.entity(m.getEntityClazz());

        List<AbstractManagedType> subManagedType = ((AbstractManagedType) entityType).getSubManagedType();

        for (ByteBuffer key : qResults.keySet()) {
            onColumn(m, isRelation, relations, entities, qResults.get(key), subManagedType, key);
        }
    }

    /**
     * On column.
     * 
     * @param m
     *            the m
     * @param isRelation
     *            the is relation
     * @param relations
     *            the relations
     * @param entities
     *            the entities
     * @param columns
     *            the columns
     * @param subManagedType
     *            the sub managed type
     * @param key
     *            the key
     */
    protected void onColumn(EntityMetadata m, boolean isRelation, List<String> relations, List<Object> entities,
        List<Column> columns, List<AbstractManagedType> subManagedType, ByteBuffer key) {
        if (!columns.isEmpty()) {
            Object id = PropertyAccessorHelper.getObject(m.getIdAttribute().getJavaType(), key.array());
            ThriftRow tr = new ThriftRow(id, m.getTableName(), columns, new ArrayList<SuperColumn>(0),
                new ArrayList<CounterColumn>(0), new ArrayList<CounterSuperColumn>(0));
            Object o = null;

            if (!subManagedType.isEmpty()) {
                for (AbstractManagedType subEntity : subManagedType) {
                    EntityMetadata subEntityMetadata =
                        KunderaMetadataManager.getEntityMetadata(kunderaMetadata, subEntity.getJavaType());

                    o = getDataHandler().populateEntity(tr, subEntityMetadata, KunderaCoreUtils.getEntity(o),
                        subEntityMetadata.getRelationNames(), isRelation);
                    if (o != null) {
                        break;
                    }
                }
            } else {
                o = getDataHandler().populateEntity(tr, m, KunderaCoreUtils.getEntity(o), relations, isRelation);
            }

            if (log.isInfoEnabled()) {
                log.info("Populating data for entity of clazz {} and row key {}.", m.getEntityClazz(), tr.getId());
            }

            if (o != null) {
                entities.add(o);
            }
        }
    }

    /**
     * Compute entity via super columns.
     * 
     * @param m
     *            the m
     * @param isRelation
     *            the is relation
     * @param relations
     *            the relations
     * @param entities
     *            the entities
     * @param qResults
     *            the q results
     */
    protected void computeEntityViaSuperColumns(EntityMetadata m, boolean isRelation, List<String> relations,
        List<Object> entities, Map<ByteBuffer, List<SuperColumn>> qResults) {
        for (ByteBuffer key : qResults.keySet()) {
            onSuperColumn(m, isRelation, relations, entities, qResults.get(key), key);
        }
    }

    /**
     * On super column.
     * 
     * @param m
     *            the m
     * @param isRelation
     *            the is relation
     * @param relations
     *            the relations
     * @param entities
     *            the entities
     * @param superColumns
     *            the super columns
     * @param key
     *            the key
     */
    protected void onSuperColumn(EntityMetadata m, boolean isRelation, List<String> relations, List<Object> entities,
        List<SuperColumn> superColumns, ByteBuffer key) {
        Object e = null;
        Object id = PropertyAccessorHelper.getObject(m.getIdAttribute().getJavaType(), key.array());

        ThriftRow tr = new ThriftRow(id, m.getTableName(), new ArrayList<Column>(0), superColumns,
            new ArrayList<CounterColumn>(0), new ArrayList<CounterSuperColumn>(0));

        e = getDataHandler().populateEntity(tr, m, KunderaCoreUtils.getEntity(e), relations, isRelation);
        if (log.isInfoEnabled()) {
            log.info("Populating data for super column family of clazz {} and row key {}.", m.getEntityClazz(),
                tr.getId());
        }

        if (e != null) {
            entities.add(e);
        }
    }

    /**
     * Adds relation foreign key values as thrift column/ value to thrift row.
     * 
     * @param metadata
     *            the metadata
     * @param tf
     *            the tf
     * @param relations
     *            the relations
     */
    protected void addRelationsToThriftRow(EntityMetadata metadata, ThriftRow tf, List<RelationHolder> relations) {
        if (relations != null) {
            long timestamp = generator.getTimestamp();
            MetamodelImpl metaModel =
                (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(metadata.getPersistenceUnit());
            for (RelationHolder rh : relations) {
                String linkName = rh.getRelationName();
                Object linkValue = rh.getRelationValue();

                if (linkName != null && linkValue != null) {
                    if (metaModel.getEmbeddables(metadata.getEntityClazz()).isEmpty()) {
                        if (metadata.isCounterColumnType()) {
                            CounterColumn col = populateCounterFkey(linkName, linkValue);
                            tf.addCounterColumn(col);
                        } else {
                            Column col = populateFkey(linkName, linkValue, timestamp);
                            tf.addColumn(col);
                        }

                    } else {
                        if (metadata.isCounterColumnType()) {
                            CounterSuperColumn counterSuperColumn = new CounterSuperColumn();
                            counterSuperColumn.setName(linkName.getBytes());
                            CounterColumn column = populateCounterFkey(linkName, linkValue);
                            counterSuperColumn.addToColumns(column);
                            tf.addCounterSuperColumn(counterSuperColumn);
                        } else {
                            SuperColumn superColumn = new SuperColumn();
                            superColumn.setName(linkName.getBytes());
                            Column column = populateFkey(linkName, linkValue, timestamp);
                            superColumn.addToColumns(column);
                            tf.addSuperColumn(superColumn);
                        }
                    }
                }
            }
        }
    }

    /**
     * Populate counter fkey.
     * 
     * @param rlName
     *            the rl name
     * @param rlValue
     *            the rl value
     * @return the counter column
     */
    private CounterColumn populateCounterFkey(String rlName, Object rlValue) {
        CounterColumn counterCol = new CounterColumn();
        counterCol.setName(PropertyAccessorFactory.STRING.toBytes(rlName));
        counterCol.setValue((Long) rlValue);
        return counterCol;
    }

    /**
     * Deletes record for given primary key from counter column family.
     * 
     * @param pKey
     *            the key
     * @param tableName
     *            the table name
     * @param metadata
     *            the metadata
     * @param consistencyLevel
     *            the consistency level
     */
    protected void deleteRecordFromCounterColumnFamily(Object pKey, String tableName, EntityMetadata metadata,
        ConsistencyLevel consistencyLevel) {
        ColumnPath path = new ColumnPath(tableName);

        Cassandra.Client conn = null;
        Object pooledConnection = null;
        try {
            pooledConnection = getConnection();
            conn = (org.apache.cassandra.thrift.Cassandra.Client) getConnection(pooledConnection);

            if (log.isInfoEnabled()) {
                log.info("Removing data for counter column family {}.", tableName);
            }

            conn.remove_counter((CassandraUtilities.toBytes(pKey, metadata.getIdAttribute().getJavaType())), path,
                consistencyLevel);

        } catch (Exception e) {
            log.error("Error during executing delete, Caused by: .", e);
            throw new PersistenceException(e);
        } finally {
            releaseConnection(pooledConnection);
        }
    }

    /**
     * Creates secondary indexes on columns if not already created.
     * 
     * @param m
     *            the m
     * @param tableName
     *            Column family name
     * @param columns
     *            List of columns
     * @param columnType
     *            the column type
     */
    protected void createIndexesOnColumns(EntityMetadata m, String tableName, List<Column> columns, Class columnType) {
        Object pooledConnection = null;
        try {
            Cassandra.Client api = null;
            pooledConnection = getConnection();
            api = (org.apache.cassandra.thrift.Cassandra.Client) getConnection(pooledConnection);
            KsDef ksDef = api.describe_keyspace(m.getSchema());
            List<CfDef> cfDefs = ksDef.getCf_defs();

            // Column family definition on which secondary index creation is
            // required
            CfDef columnFamilyDefToUpdate = null;
            boolean isUpdatable = false;
            for (CfDef cfDef : cfDefs) {
                if (cfDef.getName().equals(tableName)) {
                    columnFamilyDefToUpdate = cfDef;
                    break;
                }
            }

            if (columnFamilyDefToUpdate == null) {
                log.error("Join table {} not available.", tableName);
                throw new PersistenceException("table" + tableName + " not found!");
            }
            // create a column family, in case it is not already available.

            // Get list of indexes already created
            List<ColumnDef> columnMetadataList = columnFamilyDefToUpdate.getColumn_metadata();
            List<String> indexList = new ArrayList<String>();

            if (columnMetadataList != null) {
                for (ColumnDef columnDef : columnMetadataList) {
                    indexList.add(new StringAccessor().fromBytes(String.class, columnDef.getName()));
                }
                // need to set them to null else it is giving problem on update
                // column family and trying to add again existing indexes.
                // columnFamilyDefToUpdate.column_metadata = null;
            }

            // Iterate over all columns for creating secondary index on them
            for (Column column : columns) {

                ColumnDef columnDef = new ColumnDef();

                columnDef.setName(column.getName());
                columnDef.setValidation_class(CassandraValidationClassMapper.getValidationClass(columnType, false));
                columnDef.setIndex_type(IndexType.KEYS);

                // Add secondary index only if it's not already created
                // (if already created, it would be there in column family
                // definition)
                if (!indexList.contains(new StringAccessor().fromBytes(String.class, column.getName()))) {
                    isUpdatable = true;
                    columnFamilyDefToUpdate.addToColumn_metadata(columnDef);
                }
            }

            // Finally, update column family with modified column family
            // definition
            if (isUpdatable) {
                columnFamilyDefToUpdate.setKey_validation_class(CassandraValidationClassMapper
                    .getValidationClass(m.getIdAttribute().getJavaType(), isCql3Enabled(m)));
                api.system_update_column_family(columnFamilyDefToUpdate);
            }

        } catch (Exception e) {
            log.warn("Could not create secondary index on column family {}, Caused by: . ", tableName, e);

        } finally {
            releaseConnection(pooledConnection);
        }
    }

    /**
     * Finds an entiry from database.
     * 
     * @param entityClass
     *            the entity class
     * @param rowId
     *            the row id
     * @return the object
     */
    public Object find(Class entityClass, Object rowId) {
        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, entityClass);
        List<String> relationNames = entityMetadata.getRelationNames();
        return find(entityClass, entityMetadata, rowId, relationNames);
    }

    /**
     * Finds a {@link List} of entities from database.
     * 
     * @param <E>
     *            the element type
     * @param entityClass
     *            the entity class
     * @param columnsToSelect
     *            TODO
     * @param rowIds
     *            the row ids
     * @return the list
     */
    public <E> List<E> findAll(Class<E> entityClass, String[] columnsToSelect, Object... rowIds) {
        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, entityClass);
        List<E> results = new ArrayList<E>();
        results = find(entityClass, entityMetadata.getRelationNames(),
            entityMetadata.getRelationNames() != null && !entityMetadata.getRelationNames().isEmpty(), entityMetadata,
            rowIds);
        return results.isEmpty() ? null : results;
    }

    /**
     * Find.
     * 
     * @param clazz
     *            the clazz
     * @param metadata
     *            the metadata
     * @param rowId
     *            the row id
     * @param relationNames
     *            the relation names
     * @return the object
     */
    private final Object find(Class<?> clazz, EntityMetadata metadata, Object rowId, List<String> relationNames) {

        List<Object> result = null;
        try {
            MetamodelImpl metaModel =
                (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(metadata.getPersistenceUnit());

            EntityType entityType = metaModel.entity(clazz);

            List<ManagedType> subTypes = ((AbstractManagedType) entityType).getSubManagedType();

            if (!subTypes.isEmpty()) {
                for (ManagedType subEntity : subTypes) {
                    EntityMetadata subEntityMetadata =
                        KunderaMetadataManager.getEntityMetadata(kunderaMetadata, subEntity.getJavaType());
                    result = populate(clazz, subEntityMetadata, rowId, subEntityMetadata.getRelationNames(), metaModel);
                    if (result != null && !result.isEmpty()) {
                        break;
                    }
                }
            } else {
                result = populate(clazz, metadata, rowId, relationNames, metaModel);
            }
        } catch (Exception e) {
            log.error("Error while retrieving records from database for entity {} and key {}, Caused by: .", clazz,
                rowId, e);

            throw new PersistenceException(e);
        }

        return result != null && !result.isEmpty() ? result.get(0) : null;
    }

    /**
     * Populate.
     * 
     * @param clazz
     *            the clazz
     * @param metadata
     *            the metadata
     * @param rowId
     *            the row id
     * @param relationNames
     *            the relation names
     * @param metaModel
     *            the meta model
     * @return the list
     */
    private List<Object> populate(Class<?> clazz, EntityMetadata metadata, Object rowId, List<String> relationNames,
        MetamodelImpl metaModel) {
        List<Object> result;
        if (isCql3Enabled(metadata)) {
            result = cqlClient.find(metaModel, metadata, rowId, relationNames);
        } else {
            result = (List<Object>) find(clazz, relationNames, relationNames != null, metadata, rowId);
        }
        return result;
    }

    /**
     * Returns true in case of, composite Id and if cql3 opted and not a embedded entity.
     * 
     * @param metadata
     *            the metadata
     * @return true, if is cql3 enabled
     */
    public boolean isCql3Enabled(EntityMetadata metadata) {
        if (metadata != null) {

            MetamodelImpl metaModel =
                (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(metadata.getPersistenceUnit());

            if (metaModel.isEmbeddable(metadata.getIdAttribute().getBindableJavaType())) {
                return true;
            }
            // added for embeddables support on cql3
            AbstractManagedType managedType = (AbstractManagedType) metaModel.entity(metadata.getEntityClazz());
            if (managedType.hasEmbeddableAttribute()) {
                return getCqlVersion().equalsIgnoreCase(CassandraConstants.CQL_VERSION_3_0);
            }

            if (getCqlVersion().equalsIgnoreCase(CassandraConstants.CQL_VERSION_3_0)
                && metadata.getType().equals(Type.SUPER_COLUMN_FAMILY)) {
                log.warn(
                    "Super Columns not supported by cql, Any operation on supercolumn family will be executed using thrift, returning false.");
                return false;
            }
            return getCqlVersion().equalsIgnoreCase(CassandraConstants.CQL_VERSION_3_0);
        }
        return getCqlVersion().equalsIgnoreCase(CassandraConstants.CQL_VERSION_3_0);
    }

    /**
     * Returns true in case of, composite Id and if cql3 opted and not a embedded entity.
     * 
     * @return true, if is cql3 enabled
     */
    public boolean isCql3Enabled() {
        return isCql3Enabled(null);
    }

    /**
     * Find.
     * 
     * @param <E>
     *            the element type
     * @param entityClass
     *            the entity class
     * @param superColumnMap
     *            the super column map
     * @param dataHandler
     *            the data handler
     * @return the list
     */
    public <E> List<E> find(Class<E> entityClass, Map<String, String> superColumnMap,
        CassandraDataHandler dataHandler) {
        List<E> entities = null;
        String entityId = null;
        try {
            EntityMetadata entityMetadata =
                KunderaMetadataManager.getEntityMetadata(kunderaMetadata, getPersistenceUnit(), entityClass);
            entities = new ArrayList<E>();
            for (String superColumnName : superColumnMap.keySet()) {
                entityId = superColumnMap.get(superColumnName);
                List<SuperColumn> superColumnList =
                    loadSuperColumns(entityMetadata.getSchema(), entityMetadata.getTableName(), entityId,
                        new String[] { superColumnName.substring(0, superColumnName.indexOf("|")) });
                E e = (E) dataHandler.fromThriftRow(entityMetadata.getEntityClazz(), entityMetadata,
                    new DataRow<SuperColumn>(entityId, entityMetadata.getTableName(), superColumnList));
                if (e != null) {
                    entities.add(e);
                }
            }
        } catch (Exception e) {
            log.error("Error while retrieving records from database for entity {} and key {}, Caused by: . ",
                entityClass, entityId, e);
            throw new KunderaException(e);
        }
        return entities;
    }

    /**
     * Executes Select CQL Query.
     * 
     * @param clazz
     *            the clazz
     * @param relationalField
     *            the relational field
     * @param dataHandler
     *            the data handler
     * @param isNative
     *            the is native
     * @param cqlQuery
     *            the cql query
     * @return the list
     */
    public List executeSelectQuery(Class clazz, List<String> relationalField, CassandraDataHandler dataHandler,
        boolean isNative, String cqlQuery) {
        if (log.isDebugEnabled()) {
            log.debug("Executing cql query {}.", cqlQuery);
        }

        List entities = new ArrayList<Object>();

        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, clazz);

        MetamodelImpl metaModel =
            (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(entityMetadata.getPersistenceUnit());

        EntityType entityType = metaModel.entity(entityMetadata.getEntityClazz());

        List<AbstractManagedType> subManagedType = ((AbstractManagedType) entityType).getSubManagedType();

        if (subManagedType.isEmpty()) {
            entities.addAll(cqlClient.executeQuery(clazz, relationalField, dataHandler, true, isNative, cqlQuery));
        } else {
            for (AbstractManagedType subEntity : subManagedType) {
                EntityMetadata subEntityMetadata =
                    KunderaMetadataManager.getEntityMetadata(kunderaMetadata, subEntity.getJavaType());

                entities.addAll(cqlClient.executeQuery(subEntityMetadata.getEntityClazz(), relationalField, dataHandler,
                    true, isNative, cqlQuery));
            }
        }
        return entities;
    }

    /**
     * Execute scalar query.
     * 
     * @param cqlQuery
     *            the cql query
     * @return the list
     */
    public List executeScalarQuery(String cqlQuery) {
        CqlResult cqlResult = null;
        List results = new ArrayList();
        try {
            if (log.isDebugEnabled()) {
                log.debug("Executing query {}.", cqlQuery);
            }
            cqlResult = (CqlResult) executeCQLQuery(cqlQuery, true);

            if (cqlResult != null && (cqlResult.getRows() != null || cqlResult.getRowsSize() > 0)) {
                results = new ArrayList<Object>(cqlResult.getRowsSize());
                Iterator<CqlRow> iter = cqlResult.getRowsIterator();
                while (iter.hasNext()) {
                    Map<String, Object> entity = new HashMap<String, Object>();

                    CqlRow row = iter.next();
                    for (Column column : row.getColumns()) {
                        if (column != null) {
                            String thriftColumnName =
                                PropertyAccessorFactory.STRING.fromBytes(String.class, column.getName());

                            if (column.getValue() == null) {
                                entity.put(thriftColumnName, null);
                            } else {
                                entity.put(thriftColumnName,
                                    composeColumnValue(cqlResult.getSchema(), column.getValue(), column.getName()));
                            }
                        }
                    }
                    results.add(entity);
                }
            }
        } catch (Exception e) {
            log.error("Error while executing native CQL query Caused by {}.", e);
            throw new PersistenceException(e);
        }
        return results;
    }

    /**
     * Compose column value.
     * 
     * @param cqlMetadata
     *            the cql metadata
     * @param thriftColumnValue
     *            the thrift column value
     * @param thriftColumnName
     *            the thrift column name
     * @return the object
     */
    private Object composeColumnValue(CqlMetadata cqlMetadata, byte[] thriftColumnValue, byte[] thriftColumnName) {
        Map<ByteBuffer, String> schemaTypes = cqlMetadata.getValue_types();
        AbstractType<?> type = null;
        try {
            type = TypeParser.parse(schemaTypes.get(ByteBuffer.wrap(thriftColumnName)));
        } catch (SyntaxException | ConfigurationException ex) {
            log.error(ex.getMessage());
            throw new KunderaException("Error while deserializing column value " + ex);
        }
        if (type.isCollection()) {
            return ((CollectionSerializer) type.getSerializer())
                .deserializeForNativeProtocol(ByteBuffer.wrap(thriftColumnValue), ProtocolVersion.V2);
        }
        return type.compose(ByteBuffer.wrap(thriftColumnValue));
    }

    /**
     * Executes Update/ Delete CQL query.
     * 
     * @param cqlQuery
     *            the cql query
     * @return the int
     */
    public int executeUpdateDeleteQuery(String cqlQuery) {
        if (log.isDebugEnabled()) {
            log.debug("Executing cql query {}.", cqlQuery);
        }
        try {
            CqlResult result = (CqlResult) executeCQLQuery(cqlQuery, true);
            return result.getNum();
        } catch (Exception e) {
            log.error("Error while executing updated query: {}, Caused by: . ", cqlQuery, e);
            return 0;
        }

    }

    /**
     * Gets the external properties.
     * 
     * @return the external properties
     */
    public Map<String, Object> getExternalProperties() {
        return externalProperties;
    }

    /**
     * Populate entities from key slices.
     * 
     * @param m
     *            the m
     * @param isWrapReq
     *            the is wrap req
     * @param relations
     *            the relations
     * @param keys
     *            the keys
     * @param dataHandler
     *            the data handler
     * @return the list
     * @throws Exception
     *             the exception
     */
    protected List populateEntitiesFromKeySlices(EntityMetadata m, boolean isWrapReq, List<String> relations,
        List<KeySlice> keys, CassandraDataHandler dataHandler) throws Exception {
        List results;
        MetamodelImpl metaModel =
            (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(m.getPersistenceUnit());

        Set<String> superColumnAttribs = metaModel.getEmbeddables(m.getEntityClazz()).keySet();
        results = new ArrayList(keys.size());

        ThriftDataResultHelper dataGenerator = new ThriftDataResultHelper();
        for (KeySlice key : keys) {
            List<ColumnOrSuperColumn> columns = key.getColumns();

            byte[] rowKey = key.getKey();

            Object id = PropertyAccessorHelper.getObject(m.getIdAttribute().getJavaType(), rowKey);

            Object e = null;
            Map<ByteBuffer, List<ColumnOrSuperColumn>> data = new HashMap<ByteBuffer, List<ColumnOrSuperColumn>>(1);
            data.put(ByteBuffer.wrap(rowKey), columns);
            ThriftRow tr = new ThriftRow();
            tr.setId(id);
            tr.setColumnFamilyName(m.getTableName());
            tr = dataGenerator.translateToThriftRow(data, m.isCounterColumnType(), m.getType(), tr);

            e = dataHandler.populateEntity(tr, m, KunderaCoreUtils.getEntity(e), relations, isWrapReq);

            if (e != null) {
                results.add(e);
            }
        }
        return results;
    }

    /**
     * Return insert query string for given entity.
     * 
     * @param entityMetadata
     *            the entity metadata
     * @param entity
     *            the entity
     * @param cassandra_client
     *            the cassandra_client
     * @param rlHolders
     *            the rl holders
     * @param ttlColumns
     *            TTL values for each columns
     * @return the list
     */
    protected List<String> createInsertQuery(EntityMetadata entityMetadata, Object entity,
        Cassandra.Client cassandra_client, List<RelationHolder> rlHolders, Object ttlColumns) {
        List<String> insert_Queries = new ArrayList<String>();
        CQLTranslator translator = new CQLTranslator();
        HashMap<TranslationType, Map<String, StringBuilder>> translation = translator.prepareColumnOrColumnValues(
            entity, entityMetadata, TranslationType.ALL, externalProperties, kunderaMetadata);

        Map<String, StringBuilder> columnNamesMap = translation.get(TranslationType.COLUMN);
        Map<String, StringBuilder> columnValuesMap = translation.get(TranslationType.VALUE);

        for (String tableName : columnNamesMap.keySet()) {
            String insert_Query = translator.INSERT_QUERY;

            insert_Query = StringUtils.replace(insert_Query, CQLTranslator.COLUMN_FAMILY,
                translator.ensureCase(new StringBuilder(), tableName, false).toString());
            String columnNames = columnNamesMap.get(tableName).toString();
            String columnValues = columnValuesMap.get(tableName).toString();

            StringBuilder columnNameBuilder = new StringBuilder(columnNames);
            StringBuilder columnValueBuilder = new StringBuilder(columnValues);

            for (RelationHolder rl : rlHolders) {
                columnValueBuilder =
                    onRelationColumns(columnNames, columnValues, columnNameBuilder, columnValueBuilder, rl);

                columnNameBuilder.append(",");
                columnValueBuilder.append(",");
                translator.appendColumnName(columnNameBuilder, rl.getRelationName());
                translator.appendValue(columnValueBuilder, rl.getRelationValue().getClass(), rl.getRelationValue(),
                    true, false);

            }

            insert_Query =
                StringUtils.replace(insert_Query, CQLTranslator.COLUMN_VALUES, columnValueBuilder.toString());
            insert_Query = StringUtils.replace(insert_Query, CQLTranslator.COLUMNS, columnNameBuilder.toString());

            if (log.isDebugEnabled()) {
                log.debug("Returning cql query {}.", insert_Query);
            }

            if (ttlColumns != null && ttlColumns instanceof Integer) {
                int ttl = ((Integer) ttlColumns).intValue();
                if (ttl != 0) {
                    insert_Query = insert_Query + " USING TTL " + ttl;
                }
            }
            insert_Queries.add(insert_Query);
        }
        return insert_Queries;
    }

    /**
     * On relation columns.
     * 
     * @param columnNames
     *            the column names
     * @param columnValues
     *            the column values
     * @param columnNameBuilder
     *            the column name builder
     * @param columnValueBuilder
     *            the column value builder
     * @param rl
     *            the rl
     * @return To remove redundant columns in insert query
     */
    private StringBuilder onRelationColumns(String columnNames, String columnValues, StringBuilder columnNameBuilder,
        StringBuilder columnValueBuilder, RelationHolder rl) {
        int relnameIndx = columnNameBuilder.indexOf("\"" + rl.getRelationName() + "\"");
        if (relnameIndx != -1 && rl.getRelationValue() != null) {

            List<String> cNameArray = Arrays.asList(columnNames.split(","));
            List<String> cValueArray = new ArrayList<String>(Arrays.asList(columnValues.split(",")));
            int cValueIndex = cNameArray.indexOf("\"" + rl.getRelationName() + "\"");

            if (cValueArray.get(cValueIndex).equals("null")) {
                columnNameBuilder.delete(relnameIndx - 1, relnameIndx + rl.getRelationName().length() + 2);
                cValueArray.remove(cValueIndex);
                columnValueBuilder =
                    new StringBuilder(cValueArray.toString().substring(1, cValueArray.toString().length() - 1));

            }

        }
        return columnValueBuilder;
    }

    /**
     * Return update query string for given entity intended for counter column family.
     * 
     * @param entityMetadata
     *            the entity metadata
     * @param entity
     *            the entity
     * @param cassandra_client
     *            the cassandra_client
     * @param rlHolders
     *            the rl holders
     * @return the list
     */
    protected List<String> createUpdateQueryForCounter(EntityMetadata entityMetadata, Object entity,
        Cassandra.Client cassandra_client, List<RelationHolder> rlHolders) {
        Map<String, String> builders = new HashMap<String, String>();

        CQLTranslator translator = new CQLTranslator();

        Object rowId = PropertyAccessorHelper.getId(entity, entityMetadata);
        MetamodelImpl metaModel =
            (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(entityMetadata.getPersistenceUnit());

        EntityType entityType = metaModel.entity(entityMetadata.getEntityClazz());

        Set<Attribute> attributes = entityType.getAttributes();

        for (Attribute attrib : attributes) {
            if (!entityMetadata.getIdAttribute().getName().equals(attrib.getName())
                && !metaModel.isEmbeddable(attrib.getJavaType()) && !attrib.isAssociation()) {
                String tableName = ((AbstractAttribute) attrib).getTableName() != null
                    ? ((AbstractAttribute) attrib).getTableName() : entityMetadata.getTableName();

                String queryString = builders.get(tableName);
                StringBuilder builder;
                if (queryString == null) {
                    builder = new StringBuilder();
                } else {
                    builder = new StringBuilder(queryString);
                }
                translator.buildSetClauseForCounters(builder, ((AbstractAttribute) attrib).getJPAColumnName(),
                    PropertyAccessorHelper.getObject(entity, attrib.getName()));
                builders.put(tableName, builder.toString());
            }
        }
        for (RelationHolder rl : rlHolders) {
            translator.buildSetClauseForCounters(new StringBuilder(builders.get(entityMetadata.getTableName())),
                rl.getRelationName(), rl.getRelationValue());
        }

        for (String tableName : builders.keySet()) {
            StringBuilder builder = new StringBuilder(builders.get(tableName));

            String update_Query = translator.UPDATE_QUERY;

            update_Query = StringUtils.replace(update_Query, CQLTranslator.COLUMN_FAMILY,
                translator.ensureCase(new StringBuilder(), tableName, false).toString());

            // strip last "," clause.
            builder.delete(builder.lastIndexOf(CQLTranslator.COMMA_STR), builder.length());

            builder.append(CQLTranslator.ADD_WHERE_CLAUSE);
            onWhereClause(entityMetadata, rowId, translator, builder, metaModel, entityMetadata.getIdAttribute());

            // strip last "AND" clause.
            builder.delete(builder.lastIndexOf(CQLTranslator.AND_CLAUSE), builder.length());

            StringBuilder queryBuilder = new StringBuilder(update_Query);
            queryBuilder.append(CQLTranslator.ADD_SET_CLAUSE);
            queryBuilder.append(builder);

            if (log.isDebugEnabled()) {
                log.debug("Returning update query {}.", queryBuilder.toString());
            }

            builders.put(tableName, queryBuilder.toString());
        }
        return new ArrayList(builders.values());
    }

    /**
     * Gets the persist queries.
     * 
     * @param entityMetadata
     *            the entity metadata
     * @param entity
     *            the entity
     * @param conn
     *            the conn
     * @param rlHolders
     *            the rl holders
     * @param ttlColumns
     *            the ttl columns
     * @return the persist queries
     */
    protected List<String> getPersistQueries(EntityMetadata entityMetadata, Object entity,
        org.apache.cassandra.thrift.Cassandra.Client conn, List<RelationHolder> rlHolders, Object ttlColumns) {
        List<String> queries;
        if (entityMetadata.isCounterColumnType()) {
            queries = createUpdateQueryForCounter(entityMetadata, entity, conn, rlHolders);
        } else {
            queries = createInsertQuery(entityMetadata, entity, conn, rlHolders, ttlColumns);
        }
        return queries;
    }

    /**
     * Gets the cql version.
     * 
     * @return the cqlVersion
     */
    protected String getCqlVersion() {
        return this.cqlVersion;
    }

    /**
     * Sets the cql version.
     * 
     * @param cqlVersion
     *            the cqlVersion to set
     */
    public void setCqlVersion(String cqlVersion) {
        this.cqlVersion = cqlVersion;
    }

    /**
     * Sets the consistency level.
     * 
     * @param cLevel
     *            the new consistency level
     */
    public void setConsistencyLevel(ConsistencyLevel cLevel) {
        if (cLevel != null) {
            this.consistencyLevel = cLevel;
        } else {
            log.warn("Invalid consistency level {null} provided, default level will be used.");
        }
    }

    /**
     * Close.
     */
    public void close() {
        clear();
        setCqlVersion(CassandraConstants.CQL_VERSION_2_0);
        closed = true;
        externalProperties = null;
    }

    /**
     * Checks if is open.
     * 
     * @return true, if is open
     */
    protected final boolean isOpen() {
        return !closed;
    }

    /**
     * Gets the consistency level.
     * 
     * @return the consistency level
     */
    public ConsistencyLevel getConsistencyLevel() {
        return consistencyLevel;
    }

    /**
     * On delete query.
     * 
     * @param metadata
     *            the metadata
     * @param tableName
     *            TODO
     * @param metaModel
     *            the meta model
     * @param keyObject
     *            the compound key object
     * @return the string
     */
    protected String onDeleteQuery(EntityMetadata metadata, String tableName, MetamodelImpl metaModel,
        Object keyObject) {
        CQLTranslator translator = new CQLTranslator();
        String deleteQuery = CQLTranslator.DELETE_QUERY;

        deleteQuery = StringUtils.replace(deleteQuery, CQLTranslator.COLUMN_FAMILY,
            translator.ensureCase(new StringBuilder(), tableName, false).toString());

        StringBuilder deleteQueryBuilder = new StringBuilder(deleteQuery);

        deleteQueryBuilder.append(CQLTranslator.ADD_WHERE_CLAUSE);
        onWhereClause(metadata, keyObject, translator, deleteQueryBuilder, metaModel, metadata.getIdAttribute());

        // strip last "AND" clause.
        deleteQueryBuilder.delete(deleteQueryBuilder.lastIndexOf(CQLTranslator.AND_CLAUSE),
            deleteQueryBuilder.length());

        if (log.isDebugEnabled()) {
            log.debug("Returning delete query {}.", deleteQueryBuilder.toString());
        }
        return deleteQueryBuilder.toString();
    }

    /**
     * On where clause.
     * 
     * @param metadata
     *            the metadata
     * @param key
     *            the compound key object
     * @param translator
     *            the translator
     * @param queryBuilder
     *            the query builder
     * @param metaModel
     *            the meta model
     * @param attribute
     *            the attribute
     */
    protected void onWhereClause(EntityMetadata metadata, Object key, CQLTranslator translator,
        StringBuilder queryBuilder, MetamodelImpl metaModel, SingularAttribute attribute) {
        // SingularAttribute idAttribute = metadata.getIdAttribute();
        if (metaModel.isEmbeddable(attribute.getBindableJavaType())) {
            Field[] fields = attribute.getBindableJavaType().getDeclaredFields();
            EmbeddableType compoundKey = metaModel.embeddable(attribute.getBindableJavaType());

            for (Field field : fields) {
                if (field != null && !Modifier.isStatic(field.getModifiers())
                    && !Modifier.isTransient(field.getModifiers()) && !field.isAnnotationPresent(Transient.class)) {
                    attribute = (SingularAttribute) compoundKey.getAttribute(field.getName());
                    Object valueObject = PropertyAccessorHelper.getObject(key, field);
                    if (metaModel.isEmbeddable(((AbstractAttribute) attribute).getBindableJavaType())) {
                        onWhereClause(metadata, valueObject, translator, queryBuilder, metaModel, attribute);
                    } else {
                        String columnName = ((AbstractAttribute) attribute).getJPAColumnName();
                        translator.buildWhereClause(queryBuilder, field.getType(), columnName, valueObject,
                            CQLTranslator.EQ_CLAUSE, false);
                    }
                }
            }
        } else {
            translator.buildWhereClause(
                queryBuilder, ((AbstractAttribute) attribute).getBindableJavaType(), CassandraUtilities
                    .getIdColumnName(kunderaMetadata, metadata, getExternalProperties(), isCql3Enabled(metadata)),
                key, translator.EQ_CLAUSE, false);
        }
    }

    /**
     * Find.
     * 
     * @param entityClass
     *            the entity class
     * @param relationNames
     *            the relation names
     * @param isWrapReq
     *            the is wrap req
     * @param metadata
     *            the metadata
     * @param rowIds
     *            the row ids
     * @return the list
     */
    public abstract List find(Class entityClass, List<String> relationNames, boolean isWrapReq, EntityMetadata metadata,
        Object... rowIds);

    /**
     * Load super columns.
     * 
     * @param keyspace
     *            the keyspace
     * @param columnFamily
     *            the column family
     * @param rowId
     *            the row id
     * @param superColumnNames
     *            the super column names
     * @return the list
     */
    protected abstract List<SuperColumn> loadSuperColumns(String keyspace, String columnFamily, String rowId,
        String... superColumnNames);

    /**
     * Query related methods.
     * 
     * @param clazz
     *            the clazz
     * @param relationalField
     *            the relational field
     * @param isNative
     *            the is native
     * @param cqlQuery
     *            the cql query
     * @return the list
     */
    public abstract List executeQuery(Class clazz, List<String> relationalField, boolean isNative, String cqlQuery);

    // XXX
    public List executeQuery(Class clazz, List<String> relationalField, boolean isNative, String cqlQuery,
        final List<KunderaQuery.BindParameter> parameters) {
        throw new KunderaException("not implemented");
    }

    /**
     * Find.
     * 
     * @param ixClause
     *            the ix clause
     * @param m
     *            the m
     * @param isRelation
     *            the is relation
     * @param relations
     *            the relations
     * @param maxResult
     *            the max result
     * @param columns
     *            the columns
     * @return the list
     */
    public abstract List find(List<IndexClause> ixClause, EntityMetadata m, boolean isRelation, List<String> relations,
        int maxResult, List<String> columns);

    /**
     * Find by range.
     * 
     * @param muinVal
     *            the muin val
     * @param maxVal
     *            the max val
     * @param m
     *            the m
     * @param isWrapReq
     *            the is wrap req
     * @param relations
     *            the relations
     * @param columns
     *            the columns
     * @param conditions
     *            the conditions
     * @param maxResults
     *            the max results
     * @return the list
     * @throws Exception
     *             the exception
     */
    public abstract List findByRange(byte[] muinVal, byte[] maxVal, EntityMetadata m, boolean isWrapReq,
        List<String> relations, List<String> columns, List<IndexExpression> conditions, int maxResults)
        throws Exception;

    /**
     * Search in inverted index.
     * 
     * @param columnFamilyName
     *            the column family name
     * @param m
     *            the m
     * @param indexClauseMap
     *            the index clause map
     * @return the list
     */
    public abstract List<SearchResult> searchInInvertedIndex(String columnFamilyName, EntityMetadata m,
        Map<Boolean, List<IndexClause>> indexClauseMap);

    /**
     * Find.
     * 
     * @param m
     *            the m
     * @param relationNames
     *            the relation names
     * @param conditions
     *            the conditions
     * @param maxResult
     *            the max result
     * @param columns
     *            the columns
     * @return the list
     */
    public abstract List<EnhanceEntity> find(EntityMetadata m, List<String> relationNames, List<IndexClause> conditions,
        int maxResult, List<String> columns);

    /**
     * Gets the data handler.
     * 
     * @return the data handler
     */
    protected abstract CassandraDataHandler getDataHandler();

    /**
     * Delete.
     * 
     * @param entity
     *            the entity
     * @param pKey
     *            the key
     */
    protected abstract void delete(Object entity, Object pKey);

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.persistence.api.Batcher#addBatch(com.impetus.kundera .graph.Node)
     */
    /**
     * Adds the batch.
     * 
     * @param node
     *            the node
     */
    public void addBatch(Node node) {

        if (node != null) {
            nodes.add(node);
        }

        onBatchLimit();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.persistence.api.Batcher#getBatchSize()
     */
    /**
     * Gets the batch size.
     * 
     * @return the batch size
     */
    public int getBatchSize() {
        return batchSize;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.persistence.api.Batcher#clear()
     */
    /**
     * Clear.
     */
    public void clear() {
        if (nodes != null) {
            nodes.clear();
            nodes = new ArrayList<Node>();
        }

        if (ttlPerSession) {
            ttlValues.clear();
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.persistence.api.Batcher#executeBatch()
     */
    /**
     * Execute batch.
     * 
     * @return the int
     */
    public int executeBatch() {
        Cassandra.Client conn = null;
        Object pooledConnection = null;

        /**
         * Key -> Entity Class Value -> Map containing Row ID as Key and Mutation List as Value
         */
        Map<Class<?>, Map<ByteBuffer, Map<String, List<Mutation>>>> batchMutationMap =
            new HashMap<Class<?>, Map<ByteBuffer, Map<String, List<Mutation>>>>();

        int recordsExecuted = 0;
        boolean setCounter = true;
        String batchQuery = CQLTranslator.BATCH_QUERY;
        batchQuery = StringUtils.replace(batchQuery, CQLTranslator.STATEMENT, "");
        StringBuilder batchQueryBuilder = new StringBuilder(batchQuery);
        try {
            boolean isCql3Enabled = false;
            for (Node node : nodes) {
                if (node.isDirty()) {
                    node.handlePreEvent();
                    Object entity = node.getData();
                    Object id = node.getEntityId();
                    EntityMetadata metadata =
                        KunderaMetadataManager.getEntityMetadata(kunderaMetadata, node.getDataClass());
                    if (metadata.isCounterColumnType() && setCounter) {
                        batchQueryBuilder = new StringBuilder(StringUtils.replace(batchQueryBuilder.toString(),
                            CQLTranslator.BEGIN_BATCH, CQLTranslator.BEGIN_COUNTER_BATCH));
                        setCounter = false;
                    }
                    persistenceUnit = metadata.getPersistenceUnit();
                    isUpdate = node.isUpdate();

                    MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata()
                        .getMetamodel(metadata.getPersistenceUnit());

                    // delete can not be executed in batch

                    if (isCql3Enabled(metadata)) {
                        isCql3Enabled = true;
                        List<RelationHolder> relationHolders = getRelationHolders(node);
                        if (node.isInState(RemovedState.class)) {
                            String query;
                            query = onDeleteQuery(metadata, metadata.getTableName(), metaModel, id);
                            batchQueryBuilder.append(Constants.SPACE);
                            batchQueryBuilder.append(query);
                        } else {
                            List<String> insertQueries = getPersistQueries(metadata, entity, conn, relationHolders,
                                getTtlValues().get(metadata.getTableName()));
                            for (String query : insertQueries) {
                                batchQueryBuilder.append(Constants.SPACE);
                                batchQueryBuilder.append(query);
                            }
                        }
                    } else {
                        if (node.isInState(RemovedState.class)) {
                            delete(entity, id);
                        } else {
                            List<RelationHolder> relationHolders = getRelationHolders(node);
                            Map<ByteBuffer, Map<String, List<Mutation>>> mutationMap =
                                new HashMap<ByteBuffer, Map<String, List<Mutation>>>();
                            mutationMap = prepareMutation(metadata, entity, id, relationHolders, mutationMap);

                            recordsExecuted += mutationMap.size();
                            if (!batchMutationMap.containsKey(metadata.getEntityClazz())) {
                                batchMutationMap.put(metadata.getEntityClazz(), mutationMap);
                            } else {
                                batchMutationMap.get(metadata.getEntityClazz()).putAll(mutationMap);
                            }

                            indexNode(node, metadata);
                        }
                    }
                    node.handlePostEvent();
                }
            }

            // Write Mutation map to database

            if (!batchMutationMap.isEmpty()) {
                pooledConnection = getConnection();
                conn = (org.apache.cassandra.thrift.Cassandra.Client) getConnection(pooledConnection);

                for (Class<?> entityClass : batchMutationMap.keySet()) {
                    conn.batch_mutate(batchMutationMap.get(entityClass), consistencyLevel);
                }
            }

            if (!nodes.isEmpty() && isCql3Enabled) {
                batchQueryBuilder.append(CQLTranslator.APPLY_BATCH);
                executeCQLQuery(batchQueryBuilder.toString(), isCql3Enabled);
            }
        } catch (Exception e) {
            log.error("Error while persisting record. Caused by: .", e);
            throw new KunderaException(e);
        } finally {
            clear();
            if (pooledConnection != null) {
                releaseConnection(pooledConnection);
            }
        }

        return recordsExecuted;
    }

    /**
     * Prepare mutation.
     * 
     * @param entityMetadata
     *            the entity metadata
     * @param entity
     *            the entity
     * @param id
     *            the id
     * @param relationHolders
     *            the relation holders
     * @param mutationMap
     *            the mutation map
     * @return the map
     */
    protected Map<ByteBuffer, Map<String, List<Mutation>>> prepareMutation(EntityMetadata entityMetadata, Object entity,
        Object id, List<RelationHolder> relationHolders, Map<ByteBuffer, Map<String, List<Mutation>>> mutationMap) {

        if (!isOpen()) {
            throw new PersistenceException("ThriftClient is closed.");
        }

        // check for counter column
        if (isUpdate && entityMetadata.isCounterColumnType()) {
            log.warn("Invalid operation! {} is not possible over counter column of entity {}.", "Merge",
                entityMetadata.getEntityClazz());
            throw new UnsupportedOperationException("Invalid operation! Merge is not possible over counter column.");
        }

        Collection<ThriftRow> tfRows = null;
        try {
            String columnFamily = entityMetadata.getTableName();
            tfRows = getDataHandler().toThriftRow(entity, id, entityMetadata, columnFamily,
                getTtlValues().get(columnFamily));
        } catch (Exception e) {
            log.error("Error during persisting record for entity {}, Caused by: .", entityMetadata.getEntityClazz(),
                entityMetadata.getTableName(), e);
            throw new KunderaException(e);
        }

        Map<String, List<Mutation>> columnFamilyValues = new HashMap<String, List<Mutation>>();

        for (ThriftRow tf : tfRows) {
            if (tf.getColumnFamilyName().equals(entityMetadata.getTableName())) {
                addRelationsToThriftRow(entityMetadata, tf, relationHolders);
            }

            String columnFamily = tf.getColumnFamilyName();
            // Create Insertion List
            List<Mutation> mutationList = new ArrayList<Mutation>();

            /*********** Handling for counter column family ************/

            if (entityMetadata.isCounterColumnType()) {
                List<CounterColumn> thriftCounterColumns = tf.getCounterColumns();
                List<CounterSuperColumn> thriftCounterSuperColumns = tf.getCounterSuperColumns();

                if (thriftCounterColumns != null && !thriftCounterColumns.isEmpty()) {
                    for (CounterColumn column : thriftCounterColumns) {
                        Mutation mut = new Mutation();
                        mut.setColumn_or_supercolumn(new ColumnOrSuperColumn().setCounter_column(column));
                        mutationList.add(mut);
                    }
                }

                if (thriftCounterSuperColumns != null && !thriftCounterSuperColumns.isEmpty()) {
                    for (CounterSuperColumn sc : thriftCounterSuperColumns) {
                        Mutation mut = new Mutation();
                        mut.setColumn_or_supercolumn(new ColumnOrSuperColumn().setCounter_super_column(sc));
                        mutationList.add(mut);
                    }
                }
            } else
            /********* Handling for column family and super column family *********/
            {
                List<Column> thriftColumns = tf.getColumns();
                List<SuperColumn> thriftSuperColumns = tf.getSuperColumns();

                // Populate Insertion list for columns
                if (thriftColumns != null && !thriftColumns.isEmpty()) {
                    for (Column column : thriftColumns) {
                        Mutation mut = new Mutation();
                        mut.setColumn_or_supercolumn(new ColumnOrSuperColumn().setColumn(column));
                        mutationList.add(mut);
                    }
                }

                // Populate Insertion list for super columns
                if (thriftSuperColumns != null && !thriftSuperColumns.isEmpty()) {
                    for (SuperColumn superColumn : thriftSuperColumns) {
                        Mutation mut = new Mutation();
                        mut.setColumn_or_supercolumn(new ColumnOrSuperColumn().setSuper_column(superColumn));
                        mutationList.add(mut);
                    }
                }
            }
            columnFamilyValues.put(columnFamily, mutationList);
        }
        // Create Mutation Map

        ByteBuffer b = CassandraUtilities.toBytes(id, entityMetadata.getIdAttribute().getBindableJavaType());
        mutationMap.put(b, columnFamilyValues);

        return mutationMap;
    }

    /**
     * Check on batch limit.
     */
    private void onBatchLimit() {
        if (batchSize > 0 && batchSize == nodes.size()) {
            executeBatch();
            nodes.clear();
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.ClientPropertiesSetter#populateClientProperties
     * (com.impetus.kundera.client.Client, java.util.Map)
     */
    @Override
    public void populateClientProperties(Client client, Map<String, Object> properties) {
        new CassandraClientProperties().populateClientProperties(client, properties);
    }

    /**
     * Returns raw cassandra client from thrift connection pool.
     * 
     * @param schema
     *            schema or keyspace.
     * @return raw cassandra client.
     */
    public Cassandra.Client getRawClient(final String schema) {
        Cassandra.Client client = null;
        Object pooledConnection;
        pooledConnection = getConnection();
        client = (org.apache.cassandra.thrift.Cassandra.Client) getConnection(pooledConnection);
        try {
            client.set_cql_version(getCqlVersion());
        } catch (Exception e) {
            log.error("Error during borrowing a connection , Caused by: {}.", e);
            throw new KunderaException(e);
        } finally {
            releaseConnection(pooledConnection);
        }
        return client;

    }

    /**
     * Executes query string using cql3.
     * 
     * @param cqlQuery
     *            the cql query
     * @param isCql3Enabled
     *            the is cql3 enabled
     * @return the object
     */
    protected Object executeCQLQuery(String cqlQuery, boolean isCql3Enabled) {
        Cassandra.Client conn = null;
        Object pooledConnection = null;
        pooledConnection = getConnection();
        conn = (org.apache.cassandra.thrift.Cassandra.Client) getConnection(pooledConnection);
        try {
            if (isCql3Enabled || isCql3Enabled()) {
                return execute(cqlQuery, conn);
            }
            KunderaCoreUtils.printQuery(cqlQuery, showQuery);
            if (log.isDebugEnabled()) {
                log.debug("Executing cql query {}.", cqlQuery);
            }
            return conn.execute_cql_query(ByteBufferUtil.bytes(cqlQuery), org.apache.cassandra.thrift.Compression.NONE);
        } catch (Exception ex) {
            if (log.isErrorEnabled()) {
                log.error("Error during executing query {}, Caused by: {} .", cqlQuery, ex);
            }
            throw new PersistenceException(ex);
        } finally {
            releaseConnection(pooledConnection);
        }
    }

    /**
     * Find List of objects based on value {@columnValue} of column {@columnName}.
     * 
     * @param m
     *            the m
     * @param columnName
     *            the column name
     * @param columnValue
     *            the column value
     * @param clazz
     *            the clazz
     * @param dataHandler
     *            the data handler
     * @return the list
     */
    protected List<Object> findByRelationQuery(EntityMetadata m, String columnName, Object columnValue, Class clazz,
        CassandraDataHandler dataHandler) {
        return cqlClient.findByRelationQuery(m, columnName, columnValue, clazz, dataHandler);
    }

    /**
     * Sets the batch size.
     * 
     * @param persistenceUnit
     *            the persistence unit
     * @param puProperties
     *            the pu properties
     */
    private void setBatchSize(String persistenceUnit, Map<String, Object> puProperties) {
        String batch_Size = null;

        PersistenceUnitMetadata puMetadata =
            KunderaMetadataManager.getPersistenceUnitMetadata(kunderaMetadata, persistenceUnit);

        String externalBatchSize =
            puProperties != null ? (String) puProperties.get(PersistenceProperties.KUNDERA_BATCH_SIZE) : null;

        Integer intbatch = null;
        if (puMetadata.getBatchSize() > 0) {
            intbatch = new Integer(puMetadata.getBatchSize());
        }
        batch_Size =
            (String) (externalBatchSize != null ? externalBatchSize : intbatch != null ? intbatch.toString() : null);

        setBatchSize(batch_Size);
    }

    /**
     * Sets the batch size.
     * 
     * @param batch_Size
     *            the new batch size
     */
    void setBatchSize(String batch_Size) {
        if (!StringUtils.isBlank(batch_Size)) {
            batchSize = Integer.valueOf(batch_Size);
            if (batchSize == 0) {
                throw new IllegalArgumentException("kundera.batch.size property must be numeric and > 0.");
            }
        }
    }

    /**
     * Populate cql version.
     * 
     * @param externalProperties
     *            the external properties
     */
    private void populateCqlVersion(Map<String, Object> externalProperties) {
        String cqlVersion =
            externalProperties != null ? (String) externalProperties.get(CassandraConstants.CQL_VERSION) : null;
        if (cqlVersion == null || !(cqlVersion != null && (cqlVersion.equals(CassandraConstants.CQL_VERSION_2_0)
            || cqlVersion.equals(CassandraConstants.CQL_VERSION_3_0)))) {
            cqlVersion = (CassandraPropertyReader.csmd != null ? CassandraPropertyReader.csmd.getCqlVersion()
                : CassandraConstants.CQL_VERSION_2_0);
        }

        if (cqlVersion.equals(CassandraConstants.CQL_VERSION_3_0)) {
            setCqlVersion(CassandraConstants.CQL_VERSION_3_0);
        } else {
            setCqlVersion(CassandraConstants.CQL_VERSION_2_0);
        }
    }

    /**
     * Gets the connection.
     * 
     * @return the connection
     */
    protected abstract Object getConnection();

    /**
     * Gets the connection.
     * 
     * @param connection
     *            the connection
     * @return the connection
     */
    protected abstract Object getConnection(Object connection);

    /**
     * Release connection.
     * 
     * @param conn
     *            the conn
     */
    protected abstract void releaseConnection(Object conn);

    /**
     * Use CqlClient class for crud when cql enable.
     * 
     * 
     * @author Kuldeep Mishra
     * 
     */
    protected class CQLClient {

        /**
         * Persist.
         * 
         * @param entityMetadata
         *            the entity metadata
         * @param entity
         *            the entity
         * @param conn
         *            the conn
         * @param rlHolders
         *            the rl holders
         * @param ttlColumns
         *            the ttl columns
         * @throws UnsupportedEncodingException
         *             the unsupported encoding exception
         * @throws InvalidRequestException
         *             the invalid request exception
         * @throws TException
         *             the t exception
         * @throws UnavailableException
         *             the unavailable exception
         * @throws TimedOutException
         *             the timed out exception
         * @throws SchemaDisagreementException
         *             the schema disagreement exception
         */
        public void persist(EntityMetadata entityMetadata, Object entity,
            org.apache.cassandra.thrift.Cassandra.Client conn, List<RelationHolder> rlHolders, Object ttlColumns)
            throws UnsupportedEncodingException, InvalidRequestException, TException, UnavailableException,
            TimedOutException, SchemaDisagreementException {
            List<String> queries = getPersistQueries(entityMetadata, entity, conn, rlHolders, ttlColumns);

            for (String query : queries) {
                execute(query, conn);

            }
        }

        /**
         * Execute query and Return list of Objects.
         * 
         * @param clazz
         *            the clazz
         * @param relationalField
         *            the relational field
         * @param dataHandler
         *            the data handler
         * @param isCql3Enabled
         *            the is cql3 enabled
         * @param isNative
         *            the is native
         * @param cqlQuery
         *            the cql query
         * @return the list
         */
        public List executeQuery(Class clazz, List<String> relationalField, CassandraDataHandler dataHandler,
            boolean isCql3Enabled, boolean isNative, String cqlQuery) {
            EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, clazz);

            CqlResult result = null;
            List returnedEntities = new ArrayList();
            try {
                if (log.isDebugEnabled()) {
                    log.debug("Executing query {}.", cqlQuery);
                }
                result = (CqlResult) executeCQLQuery(cqlQuery, isCql3Enabled);

                setCqlMetadata(result.getSchema());

                if (result != null && (result.getRows() != null || result.getRowsSize() > 0)) {
                    returnedEntities = new ArrayList<Object>(result.getRowsSize());
                    Iterator<CqlRow> iter = result.getRowsIterator();
                    while (iter.hasNext()) {
                        Object e = null;

                        CqlRow row = iter.next();
                        Object rowKey = null;

                        ThriftRow thriftRow = null;
                        thriftRow = new ThriftRow(rowKey, entityMetadata.getTableName(), row.getColumns(),
                            new ArrayList<SuperColumn>(0), new ArrayList<CounterColumn>(0),
                            new ArrayList<CounterSuperColumn>(0));
                        // send cqlmetadata

                        e = dataHandler.populateEntity(thriftRow, entityMetadata, KunderaCoreUtils.getEntity(e),
                            relationalField, relationalField != null && !relationalField.isEmpty());

                        e = populateSecondaryTableData(relationalField, dataHandler, isCql3Enabled, entityMetadata, e);

                        if (e != null) {
                            returnedEntities.add(e);
                        } else if (isNative) {
                            returnedEntities.add(row.getColumns().get(0));
                        }
                    }
                }
            } catch (Exception e) {
                log.error("Error while executing native CQL query Caused by {}.", e);
                throw new PersistenceException(e);
            }
            return returnedEntities;
        }

        /**
         * Populate secondary table data.
         * 
         * @param relationalField
         *            the relational field
         * @param dataHandler
         *            the data handler
         * @param isCql3Enabled
         *            the is cql3 enabled
         * @param entityMetadata
         *            the entity metadata
         * @param e
         *            the e
         * @return the object
         */
        private Object populateSecondaryTableData(List<String> relationalField, CassandraDataHandler dataHandler,
            boolean isCql3Enabled, EntityMetadata entityMetadata, Object e) {
            CqlResult result;
            // For secondary tables.
            MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata()
                .getMetamodel(entityMetadata.getPersistenceUnit());
            if (!metaModel.isEmbeddable(entityMetadata.getIdAttribute().getBindableJavaType())) {
                AbstractManagedType managedType =
                    (AbstractManagedType) metaModel.entity(entityMetadata.getEntityClazz());
                List<String> secondaryTables =
                    ((DefaultEntityAnnotationProcessor) managedType.getEntityAnnotation()).getSecondaryTablesName();

                CQLTranslator translator = new CQLTranslator();

                for (String tableName : secondaryTables) {
                    // Building query.
                    StringBuilder queryBuilder = new StringBuilder("select * from \"" + tableName + "\" where ");
                    Attribute attribute = entityMetadata.getIdAttribute();
                    translator.buildWhereClause(queryBuilder,
                        ((AbstractAttribute) entityMetadata.getIdAttribute()).getBindableJavaType(),
                        CassandraUtilities.getIdColumnName(kunderaMetadata, entityMetadata, getExternalProperties(),
                            isCql3Enabled(entityMetadata)),
                        PropertyAccessorHelper.getId(e, entityMetadata), translator.EQ_CLAUSE, false);
                    // strip last "AND" clause.
                    queryBuilder.delete(queryBuilder.lastIndexOf(CQLTranslator.AND_CLAUSE), queryBuilder.length());

                    // Executing.
                    result = (CqlResult) executeCQLQuery(queryBuilder.toString(), isCql3Enabled);

                    if (result != null && (result.getRows() != null || result.getRowsSize() > 0)) {
                        Iterator<CqlRow> iterator = result.getRowsIterator();
                        while (iterator.hasNext()) {
                            CqlRow cqlRow = iterator.next();

                            ThriftRow tr = null;
                            tr = new ThriftRow(null, entityMetadata.getTableName(), cqlRow.getColumns(),
                                new ArrayList<SuperColumn>(0), new ArrayList<CounterColumn>(0),
                                new ArrayList<CounterSuperColumn>(0));

                            e = dataHandler.populateEntity(tr, entityMetadata, KunderaCoreUtils.getEntity(e),
                                relationalField, relationalField != null && !relationalField.isEmpty());
                            break;
                        }
                    }
                }
            }
            return e;
        }

        /**
         * Finds entity on the basis of rowid and return list of objects.
         * 
         * @param metaModel
         *            the meta model
         * @param metadata
         *            the metadata
         * @param rowId
         *            the row id
         * @param relationNames
         *            the relation names
         * @return the list
         */
        public List<Object> find(MetamodelImpl metaModel, EntityMetadata metadata, Object rowId,
            List<String> relationNames) {
            CQLTranslator translator = new CQLTranslator();

            String tableName = metadata.getTableName();
            String select_Query = translator.SELECTALL_QUERY;
            select_Query = StringUtils.replace(select_Query, CQLTranslator.COLUMN_FAMILY,
                translator.ensureCase(new StringBuilder(), tableName, false).toString());
            StringBuilder builder = new StringBuilder(select_Query);
            builder.append(CQLTranslator.ADD_WHERE_CLAUSE);
            onWhereClause(metadata, rowId, translator, builder, metaModel, metadata.getIdAttribute());

            // strip last "AND" clause.
            builder.delete(builder.lastIndexOf(CQLTranslator.AND_CLAUSE), builder.length());
            return CassandraClientBase.this.executeQuery(metadata.getEntityClazz(), relationNames, false,
                builder.toString());
        }

        /**
         * Find List of objects based on value {@columnValue} of column {@columnName}.
         * 
         * @param m
         *            the m
         * @param columnName
         *            the column name
         * @param columnValue
         *            the column value
         * @param clazz
         *            the clazz
         * @param dataHandler
         *            the data handler
         * @return the list
         */
        protected List<Object> findByRelationQuery(EntityMetadata m, String columnName, Object columnValue, Class clazz,
            CassandraDataHandler dataHandler) {
            CQLTranslator translator = new CQLTranslator();
            String selectQuery = translator.SELECTALL_QUERY;
            selectQuery = StringUtils.replace(selectQuery, CQLTranslator.COLUMN_FAMILY,
                translator.ensureCase(new StringBuilder(), m.getTableName(), false).toString());

            StringBuilder selectQueryBuilder = new StringBuilder(selectQuery);
            selectQueryBuilder.append(CQLTranslator.ADD_WHERE_CLAUSE);

            translator.buildWhereClause(selectQueryBuilder, columnValue.getClass(), columnName, columnValue,
                CQLTranslator.EQ_CLAUSE, false);
            selectQueryBuilder.delete(selectQueryBuilder.lastIndexOf(CQLTranslator.AND_CLAUSE),
                selectQueryBuilder.length());
            return this.executeQuery(clazz, m.getRelationNames(), dataHandler, true, false,
                selectQueryBuilder.toString());
        }
    }

    /**
     * Checks if is ttl per request.
     * 
     * @return the ttlPerRequest
     */
    public boolean isTtlPerRequest() {
        return ttlPerRequest;
    }

    /**
     * Sets the ttl per request.
     * 
     * @param ttlPerRequest
     *            the ttlPerRequest to set
     */
    public void setTtlPerRequest(boolean ttlPerRequest) {
        this.ttlPerRequest = ttlPerRequest;
    }

    /**
     * Checks if is ttl per session.
     * 
     * @return the ttlPerSession
     */
    public boolean isTtlPerSession() {
        return ttlPerSession;
    }

    /**
     * Sets the ttl per session.
     * 
     * @param ttlPerSession
     *            the ttlPerSession to set
     */
    public void setTtlPerSession(boolean ttlPerSession) {
        this.ttlPerSession = ttlPerSession;
    }

    /**
     * Gets the ttl values.
     * 
     * @return the ttlValues
     */
    public Map<String, Object> getTtlValues() {
        return ttlValues;
    }

    /**
     * Sets the ttl values.
     * 
     * @param ttlValues
     *            the ttlValues to set
     */
    public void setTtlValues(Map<String, Object> ttlValues) {
        this.ttlValues = ttlValues;
    }

    /**
     * Finds a {@link List} of entities from database.
     * 
     * @param entityClass
     *            the entity class
     * @param relationNames
     *            the relation names
     * @param isWrapReq
     *            the is wrap req
     * @param metadata
     *            the metadata
     * @param rowIds
     *            the row ids
     * @return the list
     */
    public final List findByRowKeys(Class entityClass, List<String> relationNames, boolean isWrapReq,
        EntityMetadata metadata, Object... rowIds) {
        List entities = null;

        MetamodelImpl metaModel =
            (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(metadata.getPersistenceUnit());

        EntityType entityType = metaModel.entity(metadata.getEntityClazz());

        List<AbstractManagedType> subManagedType = ((AbstractManagedType) entityType).getSubManagedType();

        try {
            if (!subManagedType.isEmpty()) {
                for (AbstractManagedType subEntity : subManagedType) {
                    EntityMetadata subEntityMetadata =
                        KunderaMetadataManager.getEntityMetadata(kunderaMetadata, subEntity.getJavaType());
                    entities = getDataHandler().fromThriftRow(entityClass, subEntityMetadata,
                        subEntityMetadata.getRelationNames(), isWrapReq, getConsistencyLevel(), rowIds);

                    if (entities != null && !entities.isEmpty()) {
                        break;
                    }
                }
            } else {
                entities = getDataHandler().fromThriftRow(entityClass, metadata, relationNames, isWrapReq,
                    getConsistencyLevel(), rowIds);
            }
        } catch (Exception e) {
            log.error("Error while retrieving records for entity {}, row keys {}", entityClass, rowIds);
            throw new KunderaException(e);
        }
        return entities;
    }

    // XXX
    /**
     * Execute with bind parameters
     * 
     * @param <T>
     *            the generic type
     * @param query
     *            the query
     * @param connection
     *            the connection
     * @return the t
     */
    public <T> T execute(final String query, final Object connection,
        final List<KunderaQuery.BindParameter> parameters) {
        throw new KunderaException("not implemented");
    }

    /**
     * Execute.
     * 
     * @param <T>
     *            the generic type
     * @param query
     *            the query
     * @param connection
     *            the connection
     * @return the t
     */
    public <T> T execute(final String query, Object connection) {
        try {
            org.apache.cassandra.thrift.Cassandra.Client conn =
                (org.apache.cassandra.thrift.Cassandra.Client) connection;
            conn.set_cql_version(CassandraConstants.CQL_VERSION_3_0);
            KunderaCoreUtils.printQuery(query, showQuery);
            return (T) conn.execute_cql3_query(ByteBuffer.wrap(query.getBytes(Constants.CHARSET_UTF8)),
                Compression.NONE, getConsistencyLevel());
        } catch (Exception e) {
            log.error("Error while executing query {}", query);
            throw new KunderaException(e);
        }
    }

    /**
     * Persist join table by cql.
     * 
     * @param joinTableData
     *            the join table data
     * @param conn
     *            the conn
     */
    protected void persistJoinTableByCql(JoinTableData joinTableData, Cassandra.Client conn) {
        String joinTableName = joinTableData.getJoinTableName();
        String invJoinColumnName = joinTableData.getInverseJoinColumnName();
        Map<Object, Set<Object>> joinTableRecords = joinTableData.getJoinTableRecords();

        EntityMetadata entityMetadata =
            KunderaMetadataManager.getEntityMetadata(kunderaMetadata, joinTableData.getEntityClass());

        // need to bring in an insert query for this
        // add columns & execute query
        CQLTranslator translator = new CQLTranslator();

        String batch_Query = CQLTranslator.BATCH_QUERY;

        String insert_Query = translator.INSERT_QUERY;

        StringBuilder builder = new StringBuilder();
        builder.append(CQLTranslator.DEFAULT_KEY_NAME);
        builder.append(CQLTranslator.COMMA_STR);
        builder.append(translator.ensureCase(new StringBuilder(), joinTableData.getJoinColumnName(), false));
        builder.append(CQLTranslator.COMMA_STR);
        builder.append(translator.ensureCase(new StringBuilder(), joinTableData.getInverseJoinColumnName(), false));

        insert_Query = StringUtils.replace(insert_Query, CQLTranslator.COLUMN_FAMILY,
            translator.ensureCase(new StringBuilder(), joinTableName, false).toString());

        insert_Query = StringUtils.replace(insert_Query, CQLTranslator.COLUMNS, builder.toString());

        StringBuilder columnValueBuilder = new StringBuilder();

        StringBuilder statements = new StringBuilder();

        // insert query for each row key

        for (Object key : joinTableRecords.keySet()) {
            PropertyAccessor accessor =
                PropertyAccessorFactory.getPropertyAccessor((Field) entityMetadata.getIdAttribute().getJavaMember());

            Set<Object> values = joinTableRecords.get(key); // join column value

            for (Object value : values) {
                if (value != null) {
                    String insertQuery = insert_Query;
                    columnValueBuilder.append(CQLTranslator.QUOTE_STR);
                    columnValueBuilder.append(
                        PropertyAccessorHelper.getString(key) + "\001" + PropertyAccessorHelper.getString(value));
                    columnValueBuilder.append(CQLTranslator.QUOTE_STR);
                    columnValueBuilder.append(CQLTranslator.COMMA_STR);
                    translator.appendValue(columnValueBuilder, key.getClass(), key, true, false);
                    columnValueBuilder.append(CQLTranslator.COMMA_STR);
                    translator.appendValue(columnValueBuilder, value.getClass(), value, true, false);

                    insertQuery =
                        StringUtils.replace(insertQuery, CQLTranslator.COLUMN_VALUES, columnValueBuilder.toString());
                    statements.append(insertQuery);
                    statements.append(" ");
                }
            }
        }

        if (!StringUtils.isBlank(statements.toString())) {
            batch_Query = StringUtils.replace(batch_Query, CQLTranslator.STATEMENT, statements.toString());
            StringBuilder batchBuilder = new StringBuilder();
            batchBuilder.append(batch_Query);
            batchBuilder.append(CQLTranslator.APPLY_BATCH);
            execute(batchBuilder.toString(), conn);
        }

    }

    /**
     * Find inverse join column values for join column.
     * 
     * @param <E>
     *            the element type
     * @param schemaName
     *            the schema name
     * @param tableName
     *            the table name
     * @param pKeyColumnName
     *            the key column name
     * @param columnName
     *            the column name
     * @param pKeyColumnValue
     *            the key column value
     * @param columnJavaType
     *            the column java type
     * @return the columns by id using cql
     */
    protected <E> List<E> getColumnsByIdUsingCql(String schemaName, String tableName, String pKeyColumnName,
        String columnName, Object pKeyColumnValue, Class columnJavaType) {
        // select columnName from tableName where pKeyColumnName =
        // pKeyColumnValue
        List results = new ArrayList();
        CQLTranslator translator = new CQLTranslator();
        String selectQuery = translator.SELECT_QUERY;
        selectQuery = StringUtils.replace(selectQuery, CQLTranslator.COLUMN_FAMILY,
            translator.ensureCase(new StringBuilder(), tableName, false).toString());
        selectQuery = StringUtils.replace(selectQuery, CQLTranslator.COLUMNS,
            translator.ensureCase(new StringBuilder(), columnName, false).toString());

        StringBuilder selectQueryBuilder = new StringBuilder(selectQuery);

        selectQueryBuilder.append(CQLTranslator.ADD_WHERE_CLAUSE);

        translator.buildWhereClause(selectQueryBuilder, columnJavaType, pKeyColumnName, pKeyColumnValue,
            CQLTranslator.EQ_CLAUSE, false);
        selectQueryBuilder.delete(selectQueryBuilder.lastIndexOf(CQLTranslator.AND_CLAUSE),
            selectQueryBuilder.length());

        CqlResult cqlResult = execute(selectQueryBuilder.toString(), getRawClient(schemaName));

        Iterator<CqlRow> rowIter = cqlResult.getRows().iterator();
        while (rowIter.hasNext()) {
            CqlRow row = rowIter.next();

            if (!row.getColumns().isEmpty()) {
                Column column = row.getColumns().get(0);
                Object columnValue = CassandraDataTranslator.decompose(columnJavaType, column.getValue(), true);
                results.add(columnValue);
            }
        }
        return results;
    }

    /**
     * Find join column values for inverse join column.
     * 
     * @param <E>
     *            the element type
     * @param schemaName
     *            the schema name
     * @param tableName
     *            the table name
     * @param pKeyName
     *            the key name
     * @param columnName
     *            the column name
     * @param columnValue
     *            the column value
     * @param entityClazz
     *            the entity clazz
     * @return the list
     */
    protected <E> List<E> findIdsByColumnUsingCql(String schemaName, String tableName, String pKeyName,
        String columnName, Object columnValue, Class entityClazz) {
        EntityMetadata metadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, entityClazz);

        return getColumnsByIdUsingCql(schemaName, tableName, columnName,
            ((AbstractAttribute) metadata.getIdAttribute()).getJPAColumnName(), columnValue,
            metadata.getIdAttribute().getBindableJavaType());
    }

    /**
     * Gets the cql metadata.
     * 
     * @return the cql metadata
     */
    public CqlMetadata getCqlMetadata() {
        return cqlMetadata;
    }

    /**
     * Sets the cql metadata.
     * 
     * @param cqlMetadata
     *            the new cql metadata
     */
    public void setCqlMetadata(CqlMetadata cqlMetadata) {
        this.cqlMetadata = cqlMetadata;
    }
}
