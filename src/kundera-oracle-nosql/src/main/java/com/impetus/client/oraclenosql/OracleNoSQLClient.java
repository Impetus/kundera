/**
 * Copyright 2013 Impetus Infotech.
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
package com.impetus.client.oraclenosql;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.TimeUnit;

import javax.persistence.GenerationType;
import javax.persistence.PersistenceException;
import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.EmbeddableType;
import javax.persistence.metamodel.EntityType;

import oracle.kv.Consistency;
import oracle.kv.Durability;
import oracle.kv.DurabilityException;
import oracle.kv.FaultException;
import oracle.kv.KVStore;
import oracle.kv.Key;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldValue;
import oracle.kv.table.Index;
import oracle.kv.table.IndexKey;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.RecordValue;
import oracle.kv.table.ReturnRow.Choice;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import oracle.kv.table.TableOpExecutionException;
import oracle.kv.table.TableOperation;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.oraclenosql.config.OracleNoSQLClientProperties;
import com.impetus.client.oraclenosql.query.OracleNoSQLQuery;
import com.impetus.client.oraclenosql.query.OracleNoSQLQueryInterpreter;
import com.impetus.kundera.KunderaException;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.ClientBase;
import com.impetus.kundera.client.ClientPropertiesSetter;
import com.impetus.kundera.client.EnhanceEntity;
import com.impetus.kundera.db.RelationHolder;
import com.impetus.kundera.generator.Generator;
import com.impetus.kundera.graph.Node;
import com.impetus.kundera.index.IndexManager;
import com.impetus.kundera.lifecycle.states.RemovedState;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;
import com.impetus.kundera.metadata.model.Relation;
import com.impetus.kundera.metadata.model.Relation.ForeignKey;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.metadata.model.type.AbstractManagedType;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.api.Batcher;
import com.impetus.kundera.persistence.context.jointable.JoinTableData;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.query.KunderaQuery.FilterClause;
import com.impetus.kundera.query.QueryHandlerException;
import com.impetus.kundera.utils.KunderaCoreUtils;

/**
 * Implementation of {@link Client} interface for Oracle NoSQL database.
 * 
 * @author vivek.mishra
 */
public class OracleNoSQLClient extends ClientBase implements Client<OracleNoSQLQuery>, Batcher, ClientPropertiesSetter
{

    /** The Constant SEPERATOR. */
    private static final String SEPERATOR = "_";

    private static final String JOIN_TABLE_PRIMARY_KEY_NAME = "key";

    /** The kvstore db. */
    private KVStore kvStore;

    /** The factory. */
    private OracleNoSQLClientFactory factory;

    /** The handler. */
    private OracleNoSQLDataHandler handler;

    /** The reader. */
    private EntityReader reader;

    /** list of nodes for batch processing. */
    private List<Node> nodes = new ArrayList<Node>();

    /** batch size. */
    private int batchSize;

    // Configuration Parameter
    /** The timeout. */
    private int timeout = OracleNOSQLConstants.DEFAULT_WRITE_TIMEOUT_SECONDS;

    /** The durability. */
    private Durability durability = OracleNOSQLConstants.DEFAULT_DURABILITY;

    /** The time unit. */
    private TimeUnit timeUnit = OracleNOSQLConstants.DEFAULT_TIME_UNIT;

    /** The consistency. */
    private Consistency consistency = OracleNOSQLConstants.DEFAULT_CONSISTENCY;

    /** The table api. */
    private TableAPI tableAPI;

    /** The log. */
    private static Logger log = LoggerFactory.getLogger(OracleNoSQLClient.class);

    /**
     * Instantiates a new oracle no sqldb client.
     * 
     * @param factory
     *            the factory
     * @param reader
     *            the reader
     * @param indexManager
     *            the index manager
     * @param kvStore
     *            the kv store
     * @param puProperties
     *            the pu properties
     * @param persistenceUnit
     *            the persistence unit
     * @param kunderaMetadata
     *            the kundera metadata
     */
    OracleNoSQLClient(final OracleNoSQLClientFactory factory, EntityReader reader, IndexManager indexManager,
            final KVStore kvStore, Map<String, Object> puProperties, String persistenceUnit,
            final KunderaMetadata kunderaMetadata)
    {
        super(kunderaMetadata, puProperties, persistenceUnit);
        this.factory = factory;
        this.kvStore = kvStore;
        // this.handler = new OracleNoSQLDataHandler(this, kvStore,
        // persistenceUnit);
        this.reader = reader;
        this.indexManager = indexManager;
        this.clientMetadata = factory.getClientMetadata();
        this.tableAPI = kvStore.getTableAPI();
        setBatchSize(persistenceUnit, puProperties);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#find(java.lang.Class,
     * java.lang.Object)
     */

    @Override
    public Object find(Class entityClass, Object key)
    {
        return find(entityClass, key, null);
    }

    /**
     * Find by id.
     * 
     * @param entityClass
     *            entity class
     * @param key
     *            primary key
     * @param columnsToSelect
     *            columns to select
     * @return the object
     */
    private Object find(Class entityClass, Object key, List<String> columnsToSelect)
    {
        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, entityClass);
        MetamodelImpl metamodel = (MetamodelImpl) KunderaMetadataManager.getMetamodel(kunderaMetadata,
                entityMetadata.getPersistenceUnit());

        String idColumnName = ((AbstractAttribute) entityMetadata.getIdAttribute()).getJPAColumnName();
        Table schemaTable = tableAPI.getTable(entityMetadata.getTableName());

        PrimaryKey rowKey = schemaTable.createPrimaryKey();
        if (metamodel.isEmbeddable(entityMetadata.getIdAttribute().getBindableJavaType()))
        {
            readEmbeddable(key, columnsToSelect, entityMetadata, metamodel, schemaTable, rowKey,
                    entityMetadata.getIdAttribute());
        }
        else
        {
            if (eligibleToFetch(columnsToSelect, idColumnName))
            {
                NoSqlDBUtils.add(schemaTable.getField(idColumnName), rowKey, key, idColumnName);
            }
        }
        KunderaCoreUtils.printQuery("Fetch data from " + entityMetadata.getTableName() + " for PK " + key, showQuery);
        if (log.isDebugEnabled())
        {
            log.debug("Fetching data from " + entityMetadata.getTableName() + " for PK " + key);
        }

        // Object entity = null;
        List entities = new ArrayList();
        Map<String, Object> relationMap = initialize(entityMetadata);

        try
        {
            Iterator<Row> rowsIter = tableAPI.tableIterator(rowKey, null, null);
            // iterator and build entity
            entities = scrollAndPopulate(key, entityMetadata, metamodel, schemaTable, rowsIter, relationMap,
                    columnsToSelect);
        }
        catch (Exception e)
        {
            log.error("Error while finding data for Key " + key + ", Caused By :" + e + ".");
            throw new PersistenceException(e);
        }

        return entities.isEmpty() ? null : entities.get(0);
    }

    /**
     * Read embeddable.
     * 
     * @param key
     *            the key
     * @param columnsToSelect
     *            the columns to select
     * @param entityMetadata
     *            the entity metadata
     * @param metamodel
     *            the metamodel
     * @param schemaTable
     *            the schema table
     * @param value
     *            the value
     * @param attribute
     *            the attribute
     */
    private void readEmbeddable(Object key, List<String> columnsToSelect, EntityMetadata entityMetadata,
            MetamodelImpl metamodel, Table schemaTable, RecordValue value, Attribute attribute)
    {
        EmbeddableType embeddableId = metamodel.embeddable(((AbstractAttribute) attribute).getBindableJavaType());
        Set<Attribute> embeddedAttributes = embeddableId.getAttributes();

        for (Attribute embeddedAttrib : embeddedAttributes)
        {
            String columnName = ((AbstractAttribute) embeddedAttrib).getJPAColumnName();
            Object embeddedColumn = PropertyAccessorHelper.getObject(key, (Field) embeddedAttrib.getJavaMember());

            // either null or empty or contains that column
            if (eligibleToFetch(columnsToSelect, columnName))
            {
                NoSqlDBUtils.add(schemaTable.getField(columnName), value, embeddedColumn, columnName);
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#close()
     */
    @Override
    public void close()
    {
        this.handler = null;
        this.reader = null;

        nodes.clear();
    }

    /**
     * Delete by primary key.
     * 
     * @param entity
     *            the entity
     * @param pKey
     *            the key
     */
    @Override
    public void delete(Object entity, Object pKey)
    {
        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, entity.getClass());
        MetamodelImpl metamodel = (MetamodelImpl) KunderaMetadataManager.getMetamodel(kunderaMetadata,
                entityMetadata.getPersistenceUnit());

        String idColumnName = ((AbstractAttribute) entityMetadata.getIdAttribute()).getJPAColumnName();

        Table schemaTable = tableAPI.getTable(entityMetadata.getTableName());

        PrimaryKey key = schemaTable.createPrimaryKey();

        if (metamodel.isEmbeddable(entityMetadata.getIdAttribute().getBindableJavaType()))
        {
            EmbeddableType embeddableId = metamodel.embeddable(entityMetadata.getIdAttribute().getBindableJavaType());
            Set<Attribute> embeddedAttributes = embeddableId.getAttributes();

            for (Attribute embeddedAttrib : embeddedAttributes)
            {
                String columnName = ((AbstractAttribute) embeddedAttrib).getJPAColumnName();
                Object embeddedColumn = PropertyAccessorHelper.getObject(pKey, (Field) embeddedAttrib.getJavaMember());

                NoSqlDBUtils.add(schemaTable.getField(columnName), key, embeddedColumn, columnName);
            }
        }
        else
        {
            NoSqlDBUtils.add(schemaTable.getField(idColumnName), key, pKey, idColumnName);
        }

        tableAPI.delete(key, null, null);
        KunderaCoreUtils.printQuery("Delete data from " + entityMetadata.getTableName() + " for PK " + pKey, showQuery);
    }

    /**
     * On persist.
     * 
     * @param entityMetadata
     *            the entity metadata
     * @param entity
     *            the entity
     * @param id
     *            the id
     * @param rlHolders
     *            the rl holders
     */
    @Override
    protected void onPersist(EntityMetadata entityMetadata, Object entity, Object id, List<RelationHolder> rlHolders)
    {
        KunderaCoreUtils.printQuery(
                "Persist data into " + entityMetadata.getSchema() + "." + entityMetadata.getTableName() + " for " + id,
                showQuery);
        Row row = createRow(entityMetadata, entity, id, rlHolders);
        // TODO:: handle case for putDate??????

        tableAPI.put(row, null, null);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.client.Client#persistJoinTable(com.impetus.kundera
     * .persistence.context.jointable.JoinTableData)
     */
    @Override
    public void persistJoinTable(JoinTableData joinTableData)
    {
        String joinTableName = joinTableData.getJoinTableName();
        String joinColumnName = joinTableData.getJoinColumnName();
        String invJoinColumnName = joinTableData.getInverseJoinColumnName();
        Map<Key, List<TableOperation>> operations = new HashMap<Key, List<TableOperation>>();

        Table schemaTable = tableAPI.getTable(joinTableName);
        Row row = schemaTable.createRow();

        // String primaryKey = joinColumnName + SEPERATOR + invJoinColumnName;

        KunderaCoreUtils.printQuery("Persist Join Table:" + joinTableName, showQuery);
        Map<Object, Set<Object>> joinTableRecords = joinTableData.getJoinTableRecords();

        try
        {
            for (Object pk : joinTableRecords.keySet())
            {
                // here pk is join column name and it's values would become
                // inverse join columns

                NoSqlDBUtils.add(schemaTable.getField(joinColumnName), row, pk, joinColumnName);
                Set<Object> values = joinTableRecords.get(pk);
                for (Object childId : values)
                {
                    // what if join or inverse join column is null? Not handling
                    // as ideally it should be handled in core itself!

                    NoSqlDBUtils.add(schemaTable.getField(invJoinColumnName), row, childId, invJoinColumnName);

                    NoSqlDBUtils.add(schemaTable.getField(JOIN_TABLE_PRIMARY_KEY_NAME), row,
                            pk.toString() + SEPERATOR + childId.toString(), JOIN_TABLE_PRIMARY_KEY_NAME);

                    addOps(operations, schemaTable, row);
                }
            }

            execute(operations);
        }
        finally
        {
            operations.clear();
            operations = null;
        }
    }

    /**
     * Adds the ops.
     * 
     * @param operations
     *            the operations
     * @param schemaTable
     *            the schema table
     * @param row
     *            the row
     */
    private void addOps(Map<Key, List<TableOperation>> operations, Table schemaTable, Row row)
    {
        Key key = ((TableImpl) schemaTable).createKey(row, false);

        TableOperation ops = tableAPI.getTableOperationFactory().createPut(row, Choice.NONE, true);

        if (operations.containsKey(key))
        {
            operations.get(key).add(ops);

        }
        else
        {
            List<TableOperation> operation = new ArrayList<TableOperation>();
            operation.add(ops);
            operations.put(key, operation);
        }
    }

    /**
     * Execute.
     * 
     * @param batches
     *            the batches
     */
    private void execute(Map<Key, List<TableOperation>> batches)
    {
        if (batches != null && !batches.isEmpty())
        {
            try
            {
                for (List<TableOperation> batch : batches.values())
                {
                    tableAPI.execute(batch, null);
                }
            }
            catch (DurabilityException e)
            {
                log.error("Error while executing operations in OracleNOSQL, Caused by:" + e + ".");
                throw new PersistenceException("Error while Persisting data using batch", e);
            }
            catch (TableOpExecutionException e)
            {
                log.error("Error while executing operations in OracleNOSQL, Caused by:" + e + ".");
                throw new PersistenceException("Error while Persisting data using batch", e);
            }
            catch (FaultException e)
            {
                log.error("Error while executing operations in OracleNOSQL, Caused by:" + e + ".");
                throw new PersistenceException("Error while Persisting data using batch", e);
            }
            finally
            {
                batches.clear();
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#findAll(java.lang.Class,
     * java.lang.String[], java.lang.Object[])
     */
    @Override
    public <E> List<E> findAll(Class<E> entityClass, String[] columnsToSelect, Object... keys)
    {
        List<E> results = new ArrayList<E>();

        if (columnsToSelect == null)
        {
            columnsToSelect = new String[0];
        }

        if (keys == null)
        {
            EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, entityClass);
            MetamodelImpl metamodel = (MetamodelImpl) KunderaMetadataManager.getMetamodel(kunderaMetadata,
                    entityMetadata.getPersistenceUnit());

            EntityType entityType = metamodel.entity(entityMetadata.getEntityClazz());

            Table schemaTable = tableAPI.getTable(entityMetadata.getTableName());
            // KunderaCoreUtils.showQuery("Get all records for " +
            // entityMetadata.getTableName(), showQuery);
            Iterator<Row> rowsIter = tableAPI.tableIterator(schemaTable.createPrimaryKey(), null, null);

            Map<String, Object> relationMap = initialize(entityMetadata);

            try
            {
                results = scrollAndPopulate(null, entityMetadata, metamodel, schemaTable, rowsIter, relationMap,
                        Arrays.asList(columnsToSelect));
            }
            catch (Exception e)
            {
                log.error("Error while finding records , Caused By :" + e + ".");
                throw new PersistenceException(e);
            }
        }
        else
        {

            for (Object key : keys)
            {
                results.add((E) find(entityClass, key, Arrays.asList(columnsToSelect)));
            }
        }

        return results;
    }

    /**
     * On JPQL query execution.
     * 
     * @param <E>
     *            the element type
     * @param entityClass
     *            entity class.
     * @param interpreter
     *            query interpreter
     * @param primaryKeys
     *            set of primary keys. Empty if
     * @return the list
     */
    public <E> List<E> executeQuery(Class<E> entityClass, OracleNoSQLQueryInterpreter interpreter,
            Set<Object> primaryKeys)
    {

        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, entityClass);
        MetamodelImpl metamodel = (MetamodelImpl) KunderaMetadataManager.getMetamodel(kunderaMetadata,
                entityMetadata.getPersistenceUnit());

        EntityType entityType = metamodel.entity(entityMetadata.getEntityClazz());

        List<E> results = new ArrayList<E>();

        if (interpreter.getClauseQueue().isEmpty()) // Select all query
        {
            // Select Query with where clause (requires search within inverted
            // index)
            return findAll(entityClass, interpreter.getSelectColumns(), null);
        }
        else if (interpreter.isFindById() && interpreter.getClauseQueue().size() == 1)
        {
            Object value = null;
            Object clause = interpreter.getClauseQueue().peek();
            if (clause.getClass().isAssignableFrom(FilterClause.class))
            {
                value = interpreter.getRowKey();
            }
            else
            {
                throw new QueryHandlerException(
                        "Query with multiple AND/OR clause is not supported with oracle nosql db");
            }
            if (value != null)
            {
                Object output = find(entityClass, value, Arrays.asList(interpreter.getSelectColumns()));
                if (output != null)
                {
                    results.add((E) output);
                }
            }
        }
        else if (interpreter.getClauseQueue().size() >= 1) // query over index
        // keys.
        {
            return onIndexSearch(interpreter, entityMetadata, metamodel, results,
                    Arrays.asList(interpreter.getSelectColumns()));
        }
        return results;

        // throw new
        // UnsupportedOperationException("Query with where clause is not yet
        // supported");
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#find(java.lang.Class,
     * java.util.Map)
     */
    @Override
    public <E> List<E> find(Class<E> entityClass, Map<String, String> embeddedColumnMap)
    {
        throw new UnsupportedOperationException("This operation is not supported for OracleNoSQL.");
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#getColumnsById(java.lang.String,
     * java.lang.String, java.lang.String, java.lang.String, java.lang.Object,
     * java.lang.Class)
     */
    @Override
    public <E> List<E> getColumnsById(String schemaName, String tableName, String pKeyColumnName, String columnName,
            Object pKeyColumnValue, Class columnJavaType)
    {
        // search using index on pkey column
        List<E> foreignKeys = new ArrayList<E>();

        Table schemaTable = tableAPI.getTable(tableName);
        Index index = schemaTable.getIndex(pKeyColumnName);
        IndexKey indexKey = index.createIndexKey();

        // StringBuilder indexNamebuilder = new StringBuilder();
        NoSqlDBUtils.add(schemaTable.getField(pKeyColumnName), indexKey, pKeyColumnValue, pKeyColumnName);
        KunderaCoreUtils.printQuery(
                "Get columns by id from:" + tableName + " for column:" + columnName + " where value:" + pKeyColumnValue,
                showQuery);
        Iterator<Row> rowsIter = tableAPI.tableIterator(indexKey, null, null);

        while (rowsIter.hasNext())
        {
            Row row = rowsIter.next();
            FieldDef fieldMetadata = schemaTable.getField(columnName);
            FieldValue value = row.get(columnName);
            foreignKeys.add((E) NoSqlDBUtils.get(fieldMetadata, value, null));
        }

        return foreignKeys;

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#findIdsByColumn(java.lang.String,
     * java.lang.String, java.lang.String, java.lang.String, java.lang.Object,
     * java.lang.Class)
     */
    @Override
    public Object[] findIdsByColumn(String schemaName, String tableName, String pKeyName, String columnName,
            Object columnValue, Class entityClazz)
    {
        return getColumnsById(schemaName, tableName, columnName, pKeyName, columnValue, entityClazz).toArray();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#deleteByColumn(java.lang.String,
     * java.lang.String, java.lang.String, java.lang.Object)
     */
    @Override
    public void deleteByColumn(String schemaName, String tableName, String columnName, Object columnValue)
    {
        Table schemaTable = tableAPI.getTable(tableName);
        List<String> primaryKeys = schemaTable.getPrimaryKey();

        Object[] foundRecords = findIdsByColumn(schemaName, tableName, primaryKeys.get(0), columnName, columnValue,
                null);
        KunderaCoreUtils.printQuery("Delete columns by id from:" + tableName, showQuery);
        if (foundRecords != null)
        {
            for (Object key : foundRecords)
            {
                PrimaryKey primaryKey = schemaTable.createPrimaryKey();
                NoSqlDBUtils.add(schemaTable.getField(primaryKeys.get(0)), primaryKey, key, primaryKeys.get(0));
                KunderaCoreUtils.printQuery("  Delete for id:" + key, showQuery);
                tableAPI.delete(primaryKey, null, null);
            }
        }

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#findByRelation(java.lang.String,
     * java.lang.Object, java.lang.Class)
     */
    /**
     * Find by relational column name and value.
     * 
     * @param colName
     *            the col name
     * @param colValue
     *            the col value
     * @param entityClazz
     *            the entity clazz
     * @return the list
     */
    @Override
    public List<Object> findByRelation(String colName, Object colValue, Class entityClazz)
    {
        // find by relational value !

        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, entityClazz);
        MetamodelImpl metamodel = (MetamodelImpl) KunderaMetadataManager.getMetamodel(kunderaMetadata,
                entityMetadata.getPersistenceUnit());

        EntityType entityType = metamodel.entity(entityMetadata.getEntityClazz());

        Table schemaTable = tableAPI.getTable(entityMetadata.getTableName());

        Iterator<Row> rowsIter = null;
        if (schemaTable.getPrimaryKey().contains(colName))
        {
            PrimaryKey rowKey = schemaTable.createPrimaryKey();
            NoSqlDBUtils.add(schemaTable.getField(colName), rowKey, colValue, colName);
            rowsIter = tableAPI.tableIterator(rowKey, null, null);
        }
        else
        {
            Index index = schemaTable.getIndex(colName);
            IndexKey indexKey = index.createIndexKey();
            NoSqlDBUtils.add(schemaTable.getField(colName), indexKey, colValue, colName);
            rowsIter = tableAPI.tableIterator(indexKey, null, null);
        }

        try
        {
            Map<String, Object> relationMap = initialize(entityMetadata);

            return scrollAndPopulate(null, entityMetadata, metamodel, schemaTable, rowsIter, relationMap, null);
        }
        catch (Exception e)
        {
            log.error("Error while finding data for Key " + colName + ", Caused By :" + e + ".");
            throw new PersistenceException(e);
        }

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#getReader()
     */
    @Override
    public EntityReader getReader()
    {
        return reader;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#getQueryImplementor()
     */
    @Override
    public Class<OracleNoSQLQuery> getQueryImplementor()
    {
        return OracleNoSQLQuery.class;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.persistence.api.Batcher#addBatch(com.impetus.kundera
     * .graph.Node)
     */
    @Override
    public void addBatch(Node node)
    {
        if (node != null)
        {
            nodes.add(node);
        }
        onBatchLimit();
    }

    /**
     * Gets the handler.
     * 
     * @return the handler
     */
    public OracleNoSQLDataHandler getHandler()
    {
        return handler;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.persistence.api.Batcher#executeBatch()
     */
    @Override
    public int executeBatch()
    {

        Map<Key, List<TableOperation>> operations = new HashMap<Key, List<TableOperation>>();
        for (Node node : nodes)
        {
            if (node.isDirty())
            {
                node.handlePreEvent();
                // delete can not be executed in batch
                if (node.isInState(RemovedState.class))
                {
                    delete(node.getData(), node.getEntityId());
                }
                else
                {
                    EntityMetadata metadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata,
                            node.getDataClass());
                    List<RelationHolder> relationHolders = getRelationHolders(node);
                    // KunderaCoreUtils.showQuery("Execute batch for" +
                    // metadata.getSchema() + "." + metadata.getTableName(),
                    // showQuery);
                    Row row = createRow(metadata, node.getData(), node.getEntityId(), relationHolders);

                    Table schemaTable = tableAPI.getTable(metadata.getTableName());

                    addOps(operations, schemaTable, row);
                }
                node.handlePostEvent();
            }
        }

        execute(operations);
        return nodes.size();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.persistence.api.Batcher#getBatchSize()
     */
    @Override
    public int getBatchSize()
    {
        return batchSize;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.persistence.api.Batcher#clear()
     */
    @Override
    public void clear()
    {
        if (nodes != null)
        {
            nodes.clear();
            nodes = new ArrayList<Node>();
        }
    }

    /**
     * Check on batch limit.
     */
    private void onBatchLimit()
    {
        if (batchSize > 0 && batchSize == nodes.size())
        {
            executeBatch();
            nodes.clear();
        }
    }

    /**
     * Sets the batch size.
     * 
     * @param persistenceUnit
     *            the persistence unit
     * @param puProperties
     *            the pu properties
     */
    private void setBatchSize(String persistenceUnit, Map<String, Object> puProperties)
    {
        String batch_Size = null;
        if (puProperties != null)
        {
            batch_Size = puProperties != null ? (String) puProperties.get(PersistenceProperties.KUNDERA_BATCH_SIZE)
                    : null;
            if (batch_Size != null)
            {
                setBatchSize(Integer.valueOf(batch_Size));
            }
        }
        else if (batch_Size == null)
        {
            PersistenceUnitMetadata puMetadata = KunderaMetadataManager.getPersistenceUnitMetadata(kunderaMetadata,
                    persistenceUnit);
            setBatchSize(puMetadata.getBatchSize());
        }
    }

    /**
     * Sets the batch size.
     * 
     * @param batch_Size
     *            the new batch size
     */
    public void setBatchSize(int batch_Size)
    {
        this.batchSize = batch_Size;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.ClientPropertiesSetter#
     * populateClientProperties (com.impetus.kundera.client.Client,
     * java.util.Map)
     */
    @Override
    public void populateClientProperties(Client client, Map<String, Object> properties)
    {

        new OracleNoSQLClientProperties().populateClientProperties(client, properties);
    }

    /**
     * Sets the timeout.
     * 
     * @param timeout
     *            the timeout to set
     */
    public void setTimeout(int timeout)
    {
        this.timeout = timeout;
    }

    /**
     * Sets the durability.
     * 
     * @param durability
     *            the durability to set
     */
    public void setDurability(Durability durability)
    {
        this.durability = durability;
    }

    /**
     * Sets the time unit.
     * 
     * @param timeUnit
     *            the timeUnit to set
     */
    public void setTimeUnit(TimeUnit timeUnit)
    {
        this.timeUnit = timeUnit;
    }

    /**
     * Sets the consistency.
     * 
     * @param consistency
     *            the consistency to set
     */
    public void setConsistency(Consistency consistency)
    {
        this.consistency = consistency;
    }

    /**
     * Gets the timeout.
     * 
     * @return the timeout
     */
    public int getTimeout()
    {
        return timeout;
    }

    /**
     * Gets the durability.
     * 
     * @return the durability
     */
    public Durability getDurability()
    {
        return durability;
    }

    /**
     * Gets the time unit.
     * 
     * @return the timeUnit
     */
    public TimeUnit getTimeUnit()
    {
        return timeUnit;
    }

    /**
     * Gets the consistency.
     * 
     * @return the consistency
     */
    public Consistency getConsistency()
    {
        return consistency;
    }

    /**
     * Iterate and store attributes.
     * 
     * @param entity
     *            JPA entity.
     * @param metamodel
     *            JPA meta model.
     * @param row
     *            kv row.
     * @param attributes
     *            JPA attributes.
     * @param schemaTable
     *            the schema table
     * @param metadata
     *            the metadata
     */
    private void process(Object entity, MetamodelImpl metamodel, Row row, Set<Attribute> attributes, Table schemaTable,
            EntityMetadata metadata)
    {

        for (Attribute attribute : attributes)
        {
            // by pass association.
            if (!attribute.isAssociation())
            {
                // in case of embeddable id.
                if (attribute.equals(metadata.getIdAttribute())
                        && metamodel.isEmbeddable(((AbstractAttribute) attribute).getBindableJavaType()))
                {
                    processEmbeddableAttribute(entity, metamodel, row, schemaTable, metadata, attribute);
                }
                else
                {
                    if (metamodel.isEmbeddable(((AbstractAttribute) attribute).getBindableJavaType()))
                    {
                        processEmbeddableAttribute(entity, metamodel, row, schemaTable, metadata, attribute);
                    }
                    else
                    {
                        setField(row, schemaTable, entity, attribute);
                    }
                }
            }
        }
    }

    /**
     * Process embeddable attribute.
     * 
     * @param entity
     *            the entity
     * @param metamodel
     *            the metamodel
     * @param row
     *            the row
     * @param schemaTable
     *            the schema table
     * @param metadata
     *            the metadata
     * @param attribute
     *            the attribute
     */
    private void processEmbeddableAttribute(Object entity, MetamodelImpl metamodel, Row row, Table schemaTable,
            EntityMetadata metadata, Attribute attribute)
    {
        // process on embeddables.
        EmbeddableType embeddable = metamodel.embeddable(((AbstractAttribute) attribute).getBindableJavaType());
        Set<Attribute> embeddedAttributes = embeddable.getAttributes();
        Object embeddedObject = PropertyAccessorHelper.getObject(entity, (Field) attribute.getJavaMember());

        for (Attribute embeddedAttrib : embeddedAttributes)
        {
            setField(row, schemaTable, embeddedObject, embeddedAttrib);
        }
    }

    /**
     * Process relational attributes.
     * 
     * @param rlHolders
     *            relation holders
     * @param row
     *            kv row object
     * @param schemaTable
     *            the schema table
     */
    private void onRelationalAttributes(List<RelationHolder> rlHolders, Row row, Table schemaTable)
    {
        // Iterate over relations
        if (rlHolders != null && !rlHolders.isEmpty())
        {
            for (RelationHolder rh : rlHolders)
            {
                String relationName = rh.getRelationName();
                Object valueObj = rh.getRelationValue();

                if (!StringUtils.isEmpty(relationName) && valueObj != null)
                {
                    if (valueObj != null)
                    {
                        NoSqlDBUtils.add(schemaTable.getField(relationName), row, valueObj, relationName);
                        KunderaCoreUtils.printQuery(
                                "Add relation: relation name:" + relationName + "relation value:" + valueObj,
                                showQuery);
                    }
                }
            }
        }
    }

    /**
     * Process discriminator columns.
     * 
     * @param row
     *            kv row object.
     * @param entityType
     *            metamodel attribute.
     * @param schemaTable
     *            the schema table
     */
    private void addDiscriminatorColumn(Row row, EntityType entityType, Table schemaTable)
    {
        String discrColumn = ((AbstractManagedType) entityType).getDiscriminatorColumn();
        String discrValue = ((AbstractManagedType) entityType).getDiscriminatorValue();

        // No need to check for empty or blank, as considering it as valid name
        // for nosql!
        if (discrColumn != null && discrValue != null)
        {
            // Key
            // Key key = Key.createKey(majorKeyComponent, discrColumn);

            byte[] valueInBytes = PropertyAccessorHelper.getBytes(discrValue);
            NoSqlDBUtils.add(schemaTable.getField(discrColumn), row, discrValue, discrColumn);

        }
    }

    /**
     * setter field.
     * 
     * @param row
     *            the row
     * @param schemaTable
     *            the schema table
     * @param embeddedObject
     *            the embedded object
     * @param embeddedAttrib
     *            the embedded attrib
     */
    private void setField(Row row, Table schemaTable, Object embeddedObject, Attribute embeddedAttrib)
    {
        Field field = (Field) embeddedAttrib.getJavaMember();
        FieldDef fieldDef = schemaTable.getField(((AbstractAttribute) embeddedAttrib).getJPAColumnName());

        Object valueObj = PropertyAccessorHelper.getObject(embeddedObject, field);

        if (valueObj != null)
            NoSqlDBUtils.add(fieldDef, row, valueObj, ((AbstractAttribute) embeddedAttrib).getJPAColumnName());

    }

    /**
     * Populate id.
     * 
     * @param entityMetadata
     *            the entity metadata
     * @param schemaTable
     *            the schema table
     * @param entity
     *            the entity
     * @param row
     *            the row
     */
    private void populateId(EntityMetadata entityMetadata, Table schemaTable, Object entity, Row row)
    {
        FieldDef fieldMetadata;
        FieldValue value;
        String idColumnName = ((AbstractAttribute) entityMetadata.getIdAttribute()).getJPAColumnName();

        fieldMetadata = schemaTable.getField(idColumnName);

        value = row.get(idColumnName);

        NoSqlDBUtils.get(fieldMetadata, value, entity, (Field) entityMetadata.getIdAttribute().getJavaMember());
    }

    /**
     * On embeddable id.
     * 
     * @param entityMetadata
     *            the entity metadata
     * @param metaModel
     *            the meta model
     * @param schemaTable
     *            the schema table
     * @param entity
     *            the entity
     * @param row
     *            the row
     * @throws InstantiationException
     *             the instantiation exception
     * @throws IllegalAccessException
     *             the illegal access exception
     */
    private void onEmbeddableId(EntityMetadata entityMetadata, MetamodelImpl metaModel, Table schemaTable,
            Object entity, Row row) throws InstantiationException, IllegalAccessException
    {
        FieldDef fieldMetadata;
        FieldValue value;
        EmbeddableType embeddableType = metaModel.embeddable(entityMetadata.getIdAttribute().getBindableJavaType());
        Set<Attribute> embeddedAttributes = embeddableType.getAttributes();

        Object embeddedObject = entityMetadata.getIdAttribute().getBindableJavaType().newInstance();
        for (Attribute attrib : embeddedAttributes)
        {

            String columnName = ((AbstractAttribute) attrib).getJPAColumnName();

            fieldMetadata = schemaTable.getField(columnName);
            value = row.get(columnName);
            NoSqlDBUtils.get(fieldMetadata, value, embeddedObject, (Field) attrib.getJavaMember());
        }

        PropertyAccessorHelper.set(entity, (Field) entityMetadata.getIdAttribute().getJavaMember(), embeddedObject);
    }

    /**
     * Initialize.
     * 
     * @param entityMetadata
     *            the entity metadata
     * @return the map
     */
    private Map<String, Object> initialize(EntityMetadata entityMetadata)
    {
        Map<String, Object> relationMap = null;
        if (entityMetadata.getRelationNames() != null && !entityMetadata.getRelationNames().isEmpty())
        {
            relationMap = new HashMap<String, Object>();
        }
        return relationMap;
    }

    /**
     * Scroll and populate.
     * 
     * @param key
     *            the key
     * @param entityMetadata
     *            the entity metadata
     * @param metaModel
     *            the meta model
     * @param schemaTable
     *            the schema table
     * @param rowsIter
     *            the rows iter
     * @param relationMap
     *            the relation map
     * @param columnsToSelect
     *            the columns to select
     * @return the list
     * @throws InstantiationException
     *             the instantiation exception
     * @throws IllegalAccessException
     *             the illegal access exception
     */
    private List scrollAndPopulate(Object key, EntityMetadata entityMetadata, MetamodelImpl metaModel,
            Table schemaTable, Iterator<Row> rowsIter, Map<String, Object> relationMap, List<String> columnsToSelect)
                    throws InstantiationException, IllegalAccessException
    {
        List results = new ArrayList();
        Object entity = null;
        EntityType entityType = metaModel.entity(entityMetadata.getEntityClazz());
        // here
        while (rowsIter.hasNext())
        {
            relationMap = new HashMap<String, Object>();
            entity = initializeEntity(key, entityMetadata);

            Row row = rowsIter.next();

            List<String> fields = row.getTable().getFields();
            FieldDef fieldMetadata = null;
            FieldValue value = null;
            String idColumnName = ((AbstractAttribute) entityMetadata.getIdAttribute()).getJPAColumnName();
            if (/* eligibleToFetch(columnsToSelect, idColumnName) && */!metaModel
                    .isEmbeddable(entityMetadata.getIdAttribute().getBindableJavaType()))
            {
                populateId(entityMetadata, schemaTable, entity, row);
            }
            else
            {
                onEmbeddableId(entityMetadata, metaModel, schemaTable, entity, row);
            }

            Iterator<String> fieldIter = fields.iterator();

            Set<Attribute> attributes = entityType.getAttributes();
            for (Attribute attribute : attributes)
            {
                String jpaColumnName = ((AbstractAttribute) attribute).getJPAColumnName();
                if (eligibleToFetch(columnsToSelect, jpaColumnName)
                        && !attribute.getName().equals(entityMetadata.getIdAttribute().getName()))
                {
                    if (metaModel.isEmbeddable(((AbstractAttribute) attribute).getBindableJavaType()))
                    {
                        // readEmbeddable(value, columnsToSelect,
                        // entityMetadata, metaModel, schemaTable, value,
                        // attribute);
                        EmbeddableType embeddableId = metaModel
                                .embeddable(((AbstractAttribute) attribute).getBindableJavaType());
                        Set<Attribute> embeddedAttributes = embeddableId.getAttributes();
                        Object embeddedObject = ((AbstractAttribute) attribute).getBindableJavaType().newInstance();
                        for (Attribute embeddedAttrib : embeddedAttributes)
                        {
                            String embeddedColumnName = ((AbstractAttribute) embeddedAttrib).getJPAColumnName();

                            fieldMetadata = schemaTable.getField(embeddedColumnName);
                            value = row.get(embeddedColumnName);
                            NoSqlDBUtils.get(fieldMetadata, value, embeddedObject,
                                    (Field) embeddedAttrib.getJavaMember());
                        }
                        PropertyAccessorHelper.set(entity, (Field) attribute.getJavaMember(), embeddedObject);

                    }
                    else
                    {
                        fieldMetadata = schemaTable.getField(jpaColumnName);
                        value = row.get(jpaColumnName);

                        if (!attribute.isAssociation() && value != null)
                        {
                            NoSqlDBUtils.get(fieldMetadata, value, entity, (Field) attribute.getJavaMember());
                        }
                        else if (attribute.isAssociation() && value != null)
                        {
                            Relation relation = entityMetadata.getRelation(attribute.getName());

                            if (relation != null)
                            {
                                EntityMetadata associationMetadata = KunderaMetadataManager
                                        .getEntityMetadata(kunderaMetadata, relation.getTargetEntity());
                                if (!relation.getType().equals(ForeignKey.MANY_TO_MANY))
                                {
                                    relationMap.put(jpaColumnName, NoSqlDBUtils.get(fieldMetadata, value,
                                            (Field) associationMetadata.getIdAttribute().getJavaMember()));
                                }
                            }
                        }
                    }
                }
            }

            if (entity != null)
            {
                results.add(
                        relationMap.isEmpty() ? entity
                                : new EnhanceEntity(entity,
                                        key != null ? key : PropertyAccessorHelper.getId(entity, entityMetadata),
                                        relationMap));
            }
        }
        return results;
    }

    /**
     * Initialize entity.
     * 
     * @param key
     *            the key
     * @param entityMetadata
     *            the entity metadata
     * @return the object
     * @throws InstantiationException
     *             the instantiation exception
     * @throws IllegalAccessException
     *             the illegal access exception
     */
    private Object initializeEntity(Object key, EntityMetadata entityMetadata)
            throws InstantiationException, IllegalAccessException
    {
        Object entity = null;
        entity = entityMetadata.getEntityClazz().newInstance();
        if (key != null)
        {
            PropertyAccessorHelper.setId(entity, entityMetadata, key);
        }
        return entity;
    }

    /**
     * On index search.
     * 
     * @param <E>
     *            the element type
     * @param interpreter
     *            the interpreter
     * @param entityMetadata
     *            the entity metadata
     * @param metamodel
     *            the metamodel
     * @param results
     *            the results
     * @param columnsToSelect
     *            the columns to select
     * @return the list
     */
    private <E> List<E> onIndexSearch(OracleNoSQLQueryInterpreter interpreter, EntityMetadata entityMetadata,
            MetamodelImpl metamodel, List<E> results, List<String> columnsToSelect)
    {
        Map<String, List> indexes = new HashMap<String, List>();
        StringBuilder indexNamebuilder = new StringBuilder();

        for (Object clause : interpreter.getClauseQueue())
        {
            if (clause.getClass().isAssignableFrom(FilterClause.class))
            {
                String fieldName = null;

                String clauseName = ((FilterClause) clause).getProperty();
                StringTokenizer stringTokenizer = new StringTokenizer(clauseName, ".");
                // if need to select embedded columns
                if (stringTokenizer.countTokens() > 1)
                {
                    fieldName = stringTokenizer.nextToken();
                }

                fieldName = stringTokenizer.nextToken();
                Object value = ((FilterClause) clause).getValue();
                if (!indexes.containsKey(fieldName))
                {
                    indexNamebuilder.append(fieldName);
                    indexNamebuilder.append(",");
                }
                indexes.put(fieldName, (List) value);
            }
            else
            {
                if (clause.toString().equalsIgnoreCase("OR"))
                {
                    throw new QueryHandlerException("OR clause is not supported with oracle nosql db");
                }
            }
        }

        // prepare index name and value.
        Table schemaTable = tableAPI.getTable(entityMetadata.getTableName());
        String indexKeyName = indexNamebuilder.deleteCharAt(indexNamebuilder.length() - 1).toString();
        Index index = schemaTable.getIndex(entityMetadata.getIndexProperties().get(indexKeyName).getName());
        IndexKey indexKey = index.createIndexKey();

        // StringBuilder indexNamebuilder = new StringBuilder();
        for (String indexName : indexes.keySet())
        {
            NoSqlDBUtils.add(schemaTable.getField(indexName), indexKey, indexes.get(indexName).get(0), indexName);
        }

        Iterator<Row> rowsIter = tableAPI.tableIterator(indexKey, null, null);

        Map<String, Object> relationMap = initialize(entityMetadata);

        try
        {
            results = scrollAndPopulate(null, entityMetadata, metamodel, schemaTable, rowsIter, relationMap,
                    columnsToSelect);
            KunderaCoreUtils.printQueryWithFilterClause(interpreter.getClauseQueue(), entityMetadata.getTableName());
        }
        catch (Exception e)
        {
            log.error("Error while finding records , Caused By :" + e + ".");
            throw new PersistenceException(e);
        }

        return results;
    }

    /**
     * Creates the row.
     * 
     * @param entityMetadata
     *            the entity metadata
     * @param entity
     *            the entity
     * @param id
     *            the id
     * @param rlHolders
     *            the rl holders
     * @return the row
     */
    private Row createRow(EntityMetadata entityMetadata, Object entity, Object id, List<RelationHolder> rlHolders)
    {
        String schema = entityMetadata.getSchema(); // Irrelevant for this
        // datastore
        String table = entityMetadata.getTableName();

        MetamodelImpl metamodel = (MetamodelImpl) KunderaMetadataManager.getMetamodel(kunderaMetadata,
                entityMetadata.getPersistenceUnit());

        Table schemaTable = null;

        try
        {
            schemaTable = tableAPI.getTable(table);

            if (schemaTable == null)
            {
                log.error("No table found for " + table);
                throw new KunderaException("No table found for " + table);
            }
        }
        catch (FaultException ex)
        {
            log.error("Error while getting table " + table + ". Caused By: ", ex);
            throw new KunderaException("Error while getting table " + table + ". Caused By: ", ex);
        }

        Row row = schemaTable.createRow();

        if (log.isDebugEnabled())
        {
            log.debug("Persisting data into " + schema + "." + table + " for " + id);
        }

        EntityType entityType = metamodel.entity(entityMetadata.getEntityClazz());

        Set<Attribute> attributes = entityType.getAttributes();

        // process entity attributes.
        process(entity, metamodel, row, attributes, schemaTable, entityMetadata);
        // on relational attributes.
        onRelationalAttributes(rlHolders, row, schemaTable);
        // add discriminator column(if present)
        addDiscriminatorColumn(row, entityType, schemaTable);
        return row;
    }

    /**
     * Eligible to fetch.
     * 
     * @param columnsToSelect
     *            the columns to select
     * @param columnName
     *            the column name
     * @return true, if successful
     */
    private boolean eligibleToFetch(List<String> columnsToSelect, String columnName)
    {
        return (columnsToSelect != null && !columnsToSelect.isEmpty() && columnsToSelect.contains(columnName))
                || (columnsToSelect == null || columnsToSelect.isEmpty());
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#getIdGenerator()
     */
    @Override
    public Generator getIdGenerator()
    {
        throw new UnsupportedOperationException(
                GenerationType.class.getSimpleName() + " Strategies not supported by this client : OracleNoSQLClient");
    }

}
