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
package com.impetus.kundera.client.cassandra.dsdriver;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.PersistenceException;
import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.EmbeddableType;
import javax.persistence.metamodel.EntityType;

import org.apache.cassandra.thrift.IndexClause;
import org.apache.cassandra.thrift.IndexExpression;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.SuperColumn;
import org.apache.commons.lang.StringUtils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.impetus.client.cassandra.CassandraClientBase;
import com.impetus.client.cassandra.common.CassandraConstants;
import com.impetus.client.cassandra.datahandler.CassandraDataHandler;
import com.impetus.client.cassandra.thrift.CQLTranslator;
import com.impetus.kundera.KunderaException;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.EnhanceEntity;
import com.impetus.kundera.db.RelationHolder;
import com.impetus.kundera.db.SearchResult;
import com.impetus.kundera.generator.Generator;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.annotation.DefaultEntityAnnotationProcessor;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.metadata.model.type.AbstractManagedType;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.api.Batcher;
import com.impetus.kundera.persistence.context.jointable.JoinTableData;
import com.impetus.kundera.property.PropertyAccessor;
import com.impetus.kundera.property.PropertyAccessorFactory;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.utils.KunderaCoreUtils;
import com.impetus.kundera.utils.TimestampGenerator;
import com.impetus.kundera.query.KunderaQuery;

/**
 * Kundera powered datastax java driver based client.
 * 
 * @author vivek.mishra
 * 
 */
public class DSClient extends CassandraClientBase implements Client<DSCassQuery>, Batcher {

    /** log for this class. */
    private static Logger log = LoggerFactory.getLogger(DSClient.class);

    /** The factory. */
    private DSClientFactory factory;

    /** The reader. */
    private EntityReader reader;

    /**
     * Instantiates a new DS client.
     * 
     * @param factory
     *            the factory
     * @param persistenceUnit
     *            the persistence unit
     * @param externalProperties
     *            the external properties
     * @param kunderaMetadata
     *            the kundera metadata
     * @param reader
     *            the reader
     * @param generator
     *            the generator
     */
    public DSClient(DSClientFactory factory, String persistenceUnit, Map<String, Object> externalProperties,
        KunderaMetadata kunderaMetadata, EntityReader reader, final TimestampGenerator generator) {
        super(persistenceUnit, externalProperties, kunderaMetadata, generator);
        this.factory = factory;
        this.reader = reader;
        this.clientMetadata = factory.getClientMetadata();
        this.setCqlVersion(CassandraConstants.CQL_VERSION_3_0);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.ClientBase#onPersist(com.impetus.kundera.metadata .model.EntityMetadata,
     * java.lang.Object, java.lang.Object, java.util.List)
     */
    @Override
    protected void onPersist(EntityMetadata entityMetadata, Object entity, Object id, List<RelationHolder> rlHolders) {
        // Insert, update is fine
        try {
            cqlClient.persist(entityMetadata, entity, null, rlHolders,
                getTtlValues().get(entityMetadata.getTableName()));
        } catch (InvalidRequestException e) {
            log.error("Error while persisting record, Caused by: .", e);
            throw new KunderaException(e);
        } catch (TException e) {
            log.error("Error while persisting record, Caused by: .", e);
            throw new KunderaException(e);
        } catch (UnsupportedEncodingException e) {
            log.error("Error while persisting record, Caused by: .", e);
            throw new KunderaException(e);
        }

    }

    /**
     * Finds an entity from database.
     * 
     * @param entityClass
     *            the entity class
     * @param rowId
     *            the row id
     * @return the object
     */
    @Override
    public Object find(Class entityClass, Object rowId) {
        EntityMetadata metadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, entityClass);
        StringBuilder builder = createSelectQuery(rowId, metadata, metadata.getTableName());
        ResultSet rSet = this.execute(builder.toString(), null);
        List results = iterateAndReturn(rSet, metadata);
        return results.isEmpty() ? null : results.get(0);
    }

    /**
     * Creates the select query.
     * 
     * @param rowId
     *            the row id
     * @param metadata
     *            the metadata
     * @param tableName
     *            the table name
     * @return the string builder
     */
    private StringBuilder createSelectQuery(Object rowId, EntityMetadata metadata, String tableName) {
        MetamodelImpl metaModel =
            (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(metadata.getPersistenceUnit());

        CQLTranslator translator = new CQLTranslator();

        String select_Query = translator.SELECTALL_QUERY;
        select_Query = StringUtils.replace(select_Query, CQLTranslator.COLUMN_FAMILY,
            translator.ensureCase(new StringBuilder(), tableName, false).toString());
        StringBuilder builder = new StringBuilder(select_Query);

        builder.append(CQLTranslator.ADD_WHERE_CLAUSE);
        onWhereClause(metadata, rowId, translator, builder, metaModel, metadata.getIdAttribute());
        builder.delete(builder.lastIndexOf(CQLTranslator.AND_CLAUSE), builder.length());

        return builder;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.client.cassandra.CassandraClientBase#findAll(java.lang.Class, java.lang.String[],
     * java.lang.Object[])
     */
    @Override
    public final <E> List<E> findAll(Class<E> entityClass, String[] columnsToSelect, Object... rowIds) {
        // TODO: need to think about selected column case.
        // Bring in IN Clause in place of each read.

        List results = new ArrayList<E>();
        if (rowIds != null) {
            for (Object rowId : rowIds) {
                Object result = find(entityClass, rowId);
                if (result != null) {
                    results.add(result);
                }
            }
        }
        return results;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#find(java.lang.Class, java.util.Map)
     */
    @Override
    public <E> List<E> find(Class<E> entityClass, Map<String, String> embeddedColumnMap) {
        throw new UnsupportedOperationException(
            "Support for super columns is not available with DS java driver. Either use Thrift or pelops for the same");
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#persistJoinTable(com.impetus.kundera
     * .persistence.context.jointable.JoinTableData)
     */
    @Override
    public void persistJoinTable(JoinTableData joinTableData) {
        // TODO:: Add support for Many-to-Many join tables.
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

        // insert query for each row key and
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
            execute(batchBuilder.toString(), null);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#getColumnsById(java.lang.String, java.lang.String, java.lang.String,
     * java.lang.String, java.lang.Object, java.lang.Class)
     */
    @Override
    public <E> List<E> getColumnsById(String schemaName, String tableName, String pKeyColumnName, String columnName,
        Object pKeyColumnValue, Class columnJavaType) {
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

        ResultSet rSet = execute(selectQueryBuilder.toString(), null);

        Iterator<Row> rowIter = rSet.iterator();
        while (rowIter.hasNext()) {
            Row row = rowIter.next();
            DataType dataType = row.getColumnDefinitions().getType(columnName);
            Object columnValue =
                DSClientUtilities.assign(row, null, null, dataType.getName(), null, columnName, null, null);
            results.add(columnValue);
        }
        return results;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#findIdsByColumn(java.lang.String, java.lang.String, java.lang.String,
     * java.lang.String, java.lang.Object, java.lang.Class)
     */
    @Override
    public Object[] findIdsByColumn(String schemaName, String tableName, String pKeyName, String columnName,
        Object columnValue, Class entityClazz) {
        EntityMetadata metadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, entityClazz);

        return getColumnsById(schemaName, tableName, columnName,
            ((AbstractAttribute) metadata.getIdAttribute()).getJPAColumnName(), columnValue,
            metadata.getIdAttribute().getBindableJavaType()).toArray();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#deleteByColumn(java.lang.String, java.lang.String, java.lang.String,
     * java.lang.Object)
     */
    @Override
    public void deleteByColumn(String schemaName, String tableName, String columnName, Object columnValue) {
        Session session = factory.getConnection();
        String rowKeyName = null;
        CQLTranslator translator = new CQLTranslator();
        try {
            List<ColumnMetadata> primaryKeys = session.getCluster().getMetadata().getKeyspace("\"" + schemaName + "\"")
                .getTable("\"" + tableName + "\"").getPrimaryKey();
            rowKeyName = primaryKeys.get(0).getName();
        } finally {
            // factory.releaseConnection(session);
        }

        List rowKeys =
            getColumnsById(schemaName, tableName, columnName, rowKeyName, columnValue, columnValue.getClass());
        for (Object rowKey : rowKeys) {
            if (rowKey != null) {
                String deleteQuery = CQLTranslator.DELETE_QUERY;

                deleteQuery = StringUtils.replace(deleteQuery, CQLTranslator.COLUMN_FAMILY,
                    translator.ensureCase(new StringBuilder(), tableName, false).toString());
                StringBuilder deleteQueryBuilder = new StringBuilder(deleteQuery);
                deleteQueryBuilder.append(CQLTranslator.ADD_WHERE_CLAUSE);
                deleteQueryBuilder = translator.ensureCase(deleteQueryBuilder, rowKeyName, false);
                deleteQueryBuilder.append(CQLTranslator.EQ_CLAUSE);
                translator.appendValue(deleteQueryBuilder, rowKey.getClass(), rowKey, false, false);
                this.execute(deleteQueryBuilder.toString(), null);
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#findByRelation(java.lang.String, java.lang.Object, java.lang.Class)
     */
    @Override
    public List<Object> findByRelation(String colName, Object colValue, Class entityClazz) {
        EntityMetadata m = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, entityClazz);
        CQLTranslator translator = new CQLTranslator();
        String selectQuery = translator.SELECTALL_QUERY;
        selectQuery = StringUtils.replace(selectQuery, CQLTranslator.COLUMN_FAMILY,
            translator.ensureCase(new StringBuilder(), m.getTableName(), false).toString());

        StringBuilder selectQueryBuilder = new StringBuilder(selectQuery);
        selectQueryBuilder.append(CQLTranslator.ADD_WHERE_CLAUSE);

        translator.buildWhereClause(selectQueryBuilder, colValue.getClass(), colName, colValue, CQLTranslator.EQ_CLAUSE,
            false);
        selectQueryBuilder.delete(selectQueryBuilder.lastIndexOf(CQLTranslator.AND_CLAUSE),
            selectQueryBuilder.length());

        ResultSet rSet = (ResultSet) this.execute(selectQueryBuilder.toString(), null);

        return iterateAndReturn(rSet, m);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#getReader()
     */
    @Override
    public EntityReader getReader() {
        return this.reader;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#getQueryImplementor()
     */
    @Override
    public Class<DSCassQuery> getQueryImplementor() {
        return DSCassQuery.class;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.client.cassandra.CassandraClientBase#find(java.lang.Class, java.util.List, boolean,
     * com.impetus.kundera.metadata.model.EntityMetadata, java.lang.Object[])
     */
    @Override
    public List find(Class entityClass, List<String> relationNames, boolean isWrapReq, EntityMetadata metadata,
        Object... rowIds) {
        return findAll(entityClass, null, rowIds);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.client.cassandra.CassandraClientBase#loadSuperColumns(java .lang.String, java.lang.String,
     * java.lang.String, java.lang.String[])
     */
    @Override
    protected List<SuperColumn> loadSuperColumns(String keyspace, String columnFamily, String rowId,
        String... superColumnNames) {
        throw new UnsupportedOperationException(
            "Support for super columns is not available with DS java driver. Either use Thrift or pelops for the same");
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.client.cassandra.CassandraClientBase#executeQuery(java.lang .Class, java.util.List, boolean,
     * java.lang.String)
     */
    @Override
    public List executeQuery(Class clazz, List<String> relationalField, boolean isNative, String cqlQuery) {
        ResultSet rSet = (ResultSet) this.execute(cqlQuery, null);
        if (clazz == null) {
		// XXX
            if (isNative)
                return iterateAndReturnNative(rSet);
            return iterateAndReturn(rSet);
        }
        EntityMetadata metadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, clazz);
        return iterateAndReturn(rSet, metadata);
    }

    // XXX
    @Override
    public List executeQuery(final Class clazz, final List<String> relationalField, final boolean isNative,
        final String cqlQuery, final List<KunderaQuery.BindParameter> parameters) {
        ResultSet rSet = (ResultSet) this.execute(cqlQuery, null, parameters);

        if (clazz == null) {
            if (isNative)
                return iterateAndReturnNative(rSet);
            return iterateAndReturn(rSet);
        }
        EntityMetadata metadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, clazz);
        return iterateAndReturn(rSet, metadata);
    }

    public ResultSet executeStatement(Statement st) {

        Session session = factory.getConnection();
        return session.execute(st);
    }

    /**
     * XXX
     *
     * Iterate and return for native queries. Returns a List&lt;Object&gt; or a List&lt;Object[]&gt;
     * 
     * @param rSet
     *            the r set
     * @return the list
     */
    private List iterateAndReturnNative(final ResultSet rSet) {
        final Iterator<Row> rowIter = rSet.iterator();
        final List results = new ArrayList();
        final List item = new ArrayList<>();

        final boolean isSingle = (rSet.getColumnDefinitions().size() == 1);

        while (rowIter.hasNext()) {
            final Row row = rowIter.next();

            final ColumnDefinitions columnDefs = row.getColumnDefinitions();

            final Iterator<Definition> columnDefIter = columnDefs.iterator();

            item.clear();

            while (columnDefIter.hasNext()) {
                final Definition columnDef = columnDefIter.next();

                item.add(DSClientUtilities.assign(row, null, null, columnDef.getType().getName(), null,
                    columnDef.getName(), null, null));
            }

            if (isSingle) {
                if (!item.isEmpty())
                    results.add(item.get(0));
                else
                    results.add(null);
            } else
                results.add(item.toArray(new Object[item.size()]));
        }

        return results;
    }

    /**
     * Iterate and return.
     * 
     * @param rSet
     *            the r set
     * @return the list
     */
    private List iterateAndReturn(ResultSet rSet) {
        Iterator<Row> rowIter = rSet.iterator();
        List results = new ArrayList();

        while (rowIter.hasNext()) {
            Row row = rowIter.next();
            ColumnDefinitions columnDefs = row.getColumnDefinitions();
            Iterator<Definition> columnDefIter = columnDefs.iterator();
            Map rowData = new HashMap();
            while (columnDefIter.hasNext()) {
                Definition columnDef = columnDefIter.next();
                rowData.put(columnDef.getName(), DSClientUtilities.assign(row, null, null,
                    columnDef.getType().getName(), null, columnDef.getName(), null, null));
            }
            results.add(rowData);
        }
        return results;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.client.cassandra.CassandraClientBase#find(java.util.List,
     * com.impetus.kundera.metadata.model.EntityMetadata, boolean, java.util.List, int, java.util.List)
     */
    @Override
    public List find(List<IndexClause> ixClause, EntityMetadata m, boolean isRelation, List<String> relations,
        int maxResult, List<String> columns) {
        throw new UnsupportedOperationException("Support available only for thrift/pelops.");
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.client.cassandra.CassandraClientBase#findByRange(byte[], byte[],
     * com.impetus.kundera.metadata.model.EntityMetadata, boolean, java.util.List, java.util.List, java.util.List, int)
     */
    @Override
    public List findByRange(byte[] muinVal, byte[] maxVal, EntityMetadata m, boolean isWrapReq, List<String> relations,
        List<String> columns, List<IndexExpression> conditions, int maxResults) throws Exception {
        throw new UnsupportedOperationException("Support available only for thrift/pelops.");
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.client.cassandra.CassandraClientBase#searchInInvertedIndex (java.lang.String,
     * com.impetus.kundera.metadata.model.EntityMetadata, java.util.Map)
     */
    @Override
    public List<SearchResult> searchInInvertedIndex(String columnFamilyName, EntityMetadata m,
        Map<Boolean, List<IndexClause>> indexClauseMap) {
        throw new UnsupportedOperationException("Support available only for thrift/pelops.");
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.client.cassandra.CassandraClientBase#find(com.impetus.kundera .metadata.model.EntityMetadata,
     * java.util.List, java.util.List, int, java.util.List)
     */
    @Override
    public List<EnhanceEntity> find(EntityMetadata m, List<String> relationNames, List<IndexClause> conditions,
        int maxResult, List<String> columns) {
        throw new UnsupportedOperationException("Support available only for thrift/pelops.");
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.client.cassandra.CassandraClientBase#getDataHandler()
     */
    @Override
    protected CassandraDataHandler getDataHandler() {
        throw new UnsupportedOperationException("Support available only for thrift/pelops.");
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.client.cassandra.CassandraClientBase#delete(java.lang.Object, java.lang.Object)
     */
    @Override
    public void delete(Object entity, Object pKey) {
        EntityMetadata m = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, entity.getClass());

        MetamodelImpl metaModel =
            (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(m.getPersistenceUnit());

        AbstractManagedType managedType = (AbstractManagedType) metaModel.entity(m.getEntityClazz());

        // For secondary tables.
        List<String> secondaryTables =
            ((DefaultEntityAnnotationProcessor) managedType.getEntityAnnotation()).getSecondaryTablesName();
        secondaryTables.add(m.getTableName());

        for (String tableName : secondaryTables) {
            this.execute(onDeleteQuery(m, tableName, metaModel, pKey), null);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.client.cassandra.CassandraClientBase#getConnection()
     */
    @Override
    protected Object getConnection() {
        // do nothing returning null.
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.client.cassandra.CassandraClientBase#getConnection(java.lang .Object)
     */
    @Override
    protected Object getConnection(Object connection) {
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.client.cassandra.CassandraClientBase#releaseConnection(java .lang.Object)
     */
    @Override
    protected void releaseConnection(Object conn) {
        // do nothing
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.client.cassandra.CassandraClientBase#execute(java.lang.String , java.lang.Object)
     */
    @Override
    public <T> T execute(final String query, Object connection) {

        Session session = factory.getConnection();
        try {
            Statement queryStmt = new SimpleStatement(query);
            KunderaCoreUtils.printQuery(query, showQuery);
            queryStmt.setConsistencyLevel(ConsistencyLevel.valueOf(this.consistencyLevel.name()));
            return (T) session.execute(queryStmt);
        } catch (Exception e) {
            log.error("Error while executing query {}.", query);
            throw new KunderaException(e);
        } finally {
            // factory.releaseConnection(session);
        }
    }

    @Override
    public <T> T execute(final String query, Object connection, final List<KunderaQuery.BindParameter> parameters) {

        Session session = factory.getConnection();

        try {
            final PreparedStatement preparedStatement = session.prepare(query);
            final BoundStatement boundStatement = preparedStatement.bind();

            for (KunderaQuery.BindParameter value : parameters) {

                log.info("binding [" + value.getIndex() + ":" + value.getName() + "] to [" + value.getValue() + "]");

                if (value.getValue() != null) {

                    if (value.getValue() instanceof String) {
                        if (value.isNamed())
                            boundStatement.setString(value.getName(), (String) value.getValue());
                        else
                            boundStatement.setString(value.getIndex() - 1, (String) value.getValue());
                    } else if (value.getValue() instanceof Integer) {
                        if (value.isNamed())
                            boundStatement.setInt(value.getName(), (Integer) value.getValue());
                        else
                            boundStatement.setInt(value.getIndex() - 1, (Integer) value.getValue());
                    } else if (value.getValue() instanceof Long) {
                        if (value.isNamed())
                            boundStatement.setLong(value.getName(), (Long) value.getValue());
                        else
                            boundStatement.setLong(value.getIndex() - 1, (Long) value.getValue());
                    } else if (value.getValue() instanceof java.util.UUID) {
                        if (value.isNamed())
                            boundStatement.setUUID(value.getName(), (java.util.UUID) value.getValue());
                        else
                            boundStatement.setUUID(value.getIndex() - 1, (java.util.UUID) value.getValue());
                    } else if (value.getValue() instanceof List) {
                        if (value.isNamed())
                            boundStatement.setList(value.getName(), (List) value.getValue());
                        else
                            boundStatement.setList(value.getIndex() - 1, (List) value.getValue());
                    } else {
                        throw new IllegalArgumentException("bind parameter type [" + value.getValue().getClass()
                            + "] is not supported, " + value.getIndex() + ":" + value.getName());
                    }
                } else {
                    throw new IllegalArgumentException("setting null bind parameters is not supported");
                }
            }

            KunderaCoreUtils.printQuery(query, showQuery);

            boundStatement.setConsistencyLevel(ConsistencyLevel.valueOf(this.consistencyLevel.name()));

            return (T) session.execute(boundStatement);
        } catch (Exception e) {
            log.error("Error while executing query {}.", query);
            throw new KunderaException(e);
        } finally {
            // factory.releaseConnection(session);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.client.cassandra.CassandraClientBase#executeUpdateDeleteQuery (java.lang.String)
     */
    public int executeUpdateDeleteQuery(String cqlQuery) {
        Session session = null;
        try {
            if (log.isInfoEnabled()) {
                log.info("Executing cql query {}.", cqlQuery);
            }
            session = factory.getConnection();
            KunderaCoreUtils.printQuery(cqlQuery, showQuery);
            session.execute(cqlQuery);
        } finally {
            // factory.releaseConnection(session);
        }
        // TODO: can't find a way to return number of updated records.
        return 0;

    }

    /**
     * Iterate and return.
     * 
     * @param rSet
     *            the r set
     * @param entityClazz
     *            the entity clazz
     * @param metadata
     *            the metadata
     * @return the list
     */
    private List iterateAndReturn(ResultSet rSet, EntityMetadata metadata) {
        MetamodelImpl metaModel =
            (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(metadata.getPersistenceUnit());
        EntityType entityType = metaModel.entity(metadata.getEntityClazz());

        Iterator<Row> rowIter = rSet.iterator();
        List results = new ArrayList();

        Map<String, Object> relationalValues = new HashMap<String, Object>();

        while (rowIter.hasNext()) {
            Object entity = null;
            Row row = rowIter.next();
            populateObjectFromRow(metadata, metaModel, entityType, results, relationalValues, entity, row);
        }
        return results;
    }

    void populateObjectFromRow(EntityMetadata metadata, MetamodelImpl metaModel, EntityType entityType, List results,
        Map<String, Object> relationalValues, Object entity, Row row) {
        ColumnDefinitions columnDefs = row.getColumnDefinitions();
        Iterator<Definition> columnDefIter = columnDefs.iterator();

        entity = iteratorColumns(metadata, metaModel, entityType, relationalValues, entity, row, columnDefIter);

        if (entity != null && entity.getClass().isAssignableFrom(metadata.getEntityClazz())) {
            Object rowKey = PropertyAccessorHelper.getId(entity, metadata);

            // populate secondary tables data if there is any.
            populateSecondaryTableData(rowKey, entity, metaModel, metadata);

            if (!relationalValues.isEmpty()) {
                results.add(new EnhanceEntity(entity, rowKey, relationalValues));
            } else {
                results.add(entity);
            }
        } else if (entity != null) {
            results.add(entity);
        }
    }

    /**
     * Populates data form secondary tables of entity for given row key.
     * 
     * @param rowId
     *            the row id
     * @param entity
     *            the entity
     * @param metaModel
     *            the meta model
     * @param metadata
     *            the metadata
     */
    private void populateSecondaryTableData(Object rowId, Object entity, MetamodelImpl metaModel,
        EntityMetadata metadata) {
        AbstractManagedType managedType = (AbstractManagedType) metaModel.entity(metadata.getEntityClazz());
        List<String> secondaryTables =
            ((DefaultEntityAnnotationProcessor) managedType.getEntityAnnotation()).getSecondaryTablesName();

        for (String tableName : secondaryTables) {
            StringBuilder builder = createSelectQuery(rowId, metadata, tableName);
            ResultSet rSet = this.execute(builder.toString(), null);

            Iterator<Row> rowIter = rSet.iterator();

            Row row = rowIter.next();
            ColumnDefinitions columnDefs = row.getColumnDefinitions();
            Iterator<Definition> columnDefIter = columnDefs.iterator();

            entity = iteratorColumns(metadata, metaModel, metaModel.entity(metadata.getEntityClazz()),
                new HashMap<String, Object>(), entity, row, columnDefIter);
        }
    }

    /**
     * Iterator columns.
     * 
     * @param metadata
     *            the metadata
     * @param metamodel
     *            the metamodel
     * @param entityType
     *            the entity type
     * @param relationalValues
     *            the relational values
     * @param entity
     *            the entity
     * @param row
     *            the row
     * @param columnDefIter
     *            the column def iter
     * @return the object
     */
    private Object iteratorColumns(EntityMetadata metadata, MetamodelImpl metamodel, EntityType entityType,
        Map<String, Object> relationalValues, Object entity, Row row, Iterator<Definition> columnDefIter) {
        while (columnDefIter.hasNext()) {
            Definition columnDef = columnDefIter.next();
            final String columnName = columnDef.getName(); // column name

            DataType dataType = columnDef.getType(); // data type

            if (metadata.getRelationNames() != null && metadata.getRelationNames().contains(columnName)
                && !columnName.equals(((AbstractAttribute) metadata.getIdAttribute()).getJPAColumnName())) {
                Object relationalValue = DSClientUtilities.assign(row, null, metadata, dataType.getName(), entityType,
                    columnName, null, metamodel);
                relationalValues.put(columnName, relationalValue);
            } else {
                String fieldName = columnName.equals(((AbstractAttribute) metadata.getIdAttribute()).getJPAColumnName())
                    ? metadata.getIdAttribute().getName() : metadata.getFieldName(columnName);
                Attribute attribute = fieldName != null ? entityType.getAttribute(fieldName) : null;

                if (attribute != null) {
                    if (!attribute.isAssociation()) {
                        entity = DSClientUtilities.assign(row, entity, metadata, dataType.getName(), entityType,
                            columnName, null, metamodel);
                    }
                } else if (metamodel.isEmbeddable(metadata.getIdAttribute().getBindableJavaType())) {
                    entity = populateCompositeId(metadata, entity, columnName, row, metamodel,
                        metadata.getIdAttribute(), metadata.getEntityClazz(), dataType);
                } else {
                    entity = DSClientUtilities.assign(row, entity, metadata, dataType.getName(), entityType, columnName,
                        null, metamodel);
                }
            }
        }
        return entity;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.client.cassandra.CassandraClientBase#close()
     */
    @Override
    public void close() {
        super.close();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.ClientBase#getPersistenceUnit()
     */
    @Override
    public String getPersistenceUnit() {
        return super.getPersistenceUnit();
    }

    /**
     * Populate composite id.
     * 
     * @param metadata
     *            the metadata
     * @param entity
     *            the entity
     * @param columnName
     *            the column name
     * @param row
     *            the row
     * @param metaModel
     *            the meta model
     * @param attribute
     *            the attribute
     * @param entityClazz
     *            the entity clazz
     * @param dataType
     *            the data type
     * @return the object
     */
    private Object populateCompositeId(EntityMetadata metadata, Object entity, String columnName, Row row,
        MetamodelImpl metaModel, Attribute attribute, Class<?> entityClazz, DataType dataType) {
        Class javaType = ((AbstractAttribute) attribute).getBindableJavaType();

        if (metaModel.isEmbeddable(javaType)) {
            EmbeddableType compoundKey = metaModel.embeddable(javaType);
            Object compoundKeyObject = null;
            try {
                Set<Attribute> attributes = compoundKey.getAttributes();
                entity = KunderaCoreUtils.initialize(entityClazz, entity);

                for (Attribute compoundAttribute : attributes) {
                    compoundKeyObject =
                        compoundKeyObject == null ? getCompoundKey(attribute, entity) : compoundKeyObject;

                    if (metaModel.isEmbeddable(((AbstractAttribute) compoundAttribute).getBindableJavaType())) {
                        Object compoundObject = populateCompositeId(metadata, compoundKeyObject, columnName, row,
                            metaModel, compoundAttribute, javaType, dataType);
                        PropertyAccessorHelper.set(entity, (Field) attribute.getJavaMember(), compoundObject);
                    } else if (((AbstractAttribute) compoundAttribute).getJPAColumnName().equals(columnName)) {
                        DSClientUtilities.assign(row, compoundKeyObject, null, dataType.getName(), null, columnName,
                            (Field) compoundAttribute.getJavaMember(), metaModel);
                        PropertyAccessorHelper.set(entity, (Field) attribute.getJavaMember(), compoundKeyObject);
                        break;
                    }
                }
            } catch (IllegalArgumentException iaex) {
                // ignore as it might not represented within entity.
                // No need for any logger message
            } catch (Exception e) {
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
    private Object getCompoundKey(Attribute attribute, Object entity)
        throws InstantiationException, IllegalAccessException {
        Object compoundKeyObject = null;
        if (entity != null) {
            compoundKeyObject = PropertyAccessorHelper.getObject(entity, (Field) attribute.getJavaMember());
            if (compoundKeyObject == null) {
                compoundKeyObject = ((AbstractAttribute) attribute).getBindableJavaType().newInstance();
            }
        }

        return compoundKeyObject;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#getIdGenerator()
     */
    @Override
    public Generator getIdGenerator() {
        return (Generator) KunderaCoreUtils.createNewInstance(DSIdGenerator.class);
    }
}
