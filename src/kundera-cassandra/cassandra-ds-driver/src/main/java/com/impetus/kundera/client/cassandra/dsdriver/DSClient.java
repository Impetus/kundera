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
import java.util.UUID;

import javax.persistence.PersistenceException;
import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.EmbeddableType;
import javax.persistence.metamodel.EntityType;
import javax.persistence.metamodel.Metamodel;

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
import com.impetus.client.cassandra.CassandraClientBase;
import com.impetus.client.cassandra.common.CassandraConstants;
import com.impetus.client.cassandra.common.CassandraUtilities;
import com.impetus.client.cassandra.datahandler.CassandraDataHandler;
import com.impetus.client.cassandra.query.CassQuery;
import com.impetus.client.cassandra.thrift.CQLTranslator;
import com.impetus.kundera.KunderaException;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.EnhanceEntity;
import com.impetus.kundera.db.RelationHolder;
import com.impetus.kundera.db.SearchResult;
import com.impetus.kundera.generator.AutoGenerator;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.EntityMetadata.Type;
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

/**
 * Kundera powered datastax java driver based client.
 * 
 * @author vivek.mishra
 * 
 */
public class DSClient extends CassandraClientBase implements Client<CassQuery>, Batcher, AutoGenerator
{

    /** log for this class. */
    private static Logger log = LoggerFactory.getLogger(DSClient.class);

    private DSClientFactory factory;

    private EntityReader reader;

    private Session session;

    public DSClient(DSClientFactory factory, String persistenceUnit, Map<String, Object> externalProperties,
            KunderaMetadata kunderaMetadata, EntityReader reader, final TimestampGenerator generator)
    {
        super(persistenceUnit, externalProperties, kunderaMetadata, generator);
        this.factory = factory;
        this.reader = reader;
        this.clientMetadata = factory.getClientMetadata();
        this.setCqlVersion(CassandraConstants.CQL_VERSION_3_0);
    }

    @Override
    protected void onPersist(EntityMetadata entityMetadata, Object entity, Object id, List<RelationHolder> rlHolders)
    {

        // Insert, update is fine
        try
        {
            cqlClient.persist(entityMetadata, entity, null, rlHolders, getTtlValues()
                    .get(entityMetadata.getTableName()));
        }
        catch (InvalidRequestException e)
        {
            log.error("Error while persisting record, Caused by: .", e);
            throw new KunderaException(e);
        }
        catch (TException e)
        {
            log.error("Error while persisting record, Caused by: .", e);
            throw new KunderaException(e);
        }
        catch (UnsupportedEncodingException e)
        {
            log.error("Error while persisting record, Caused by: .", e);
            throw new KunderaException(e);
        }

    }

    /**
     * Finds an entity from database
     */
    @Override
    public Object find(Class entityClass, Object rowId)
    {
        EntityMetadata metadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, entityClass);
        StringBuilder builder = createSelectQuery(rowId, metadata, metadata.getTableName());
        ResultSet rSet = this.execute(builder.toString(), null);
        List results = iterateAndReturn(rSet, entityClass, metadata);
        return results.isEmpty() ? null : results.get(0);
    }

    private StringBuilder createSelectQuery(Object rowId, EntityMetadata metadata, String tableName)
    {
        MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                metadata.getPersistenceUnit());

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

    @Override
    public final <E> List<E> findAll(Class<E> entityClass, String[] columnsToSelect, Object... rowIds)
    {
        // TODO: need to think about selected column case.
        // Bring in IN Clause in place of each read.

        List results = new ArrayList<E>();
        if (rowIds != null)
        {
            for (Object rowId : rowIds)
            {
                Object result = find(entityClass, rowId);
                if (result != null)
                {
                    results.add(result);
                }
            }
        }

        return results;
    }

    @Override
    public Object generate()
    {
        final String generatedId = "Select now() from system.schema_columns";
        ResultSet rSet = this.execute(generatedId, null);

        UUID uuid = rSet.iterator().next().getUUID(0);
        return uuid;
        // return uuid.timestamp();
    }

    @Override
    public <E> List<E> find(Class<E> entityClass, Map<String, String> embeddedColumnMap)
    {
        throw new UnsupportedOperationException(
                "Support for super columns is not available with DS java driver. Either use Thrift or pelops for the same");
    }

    @Override
    public void persistJoinTable(JoinTableData joinTableData)
    {
        // TODO:: Add support for Many-to-Many join tables.
        String joinTableName = joinTableData.getJoinTableName();
        String invJoinColumnName = joinTableData.getInverseJoinColumnName();
        Map<Object, Set<Object>> joinTableRecords = joinTableData.getJoinTableRecords();

        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata,
                joinTableData.getEntityClass());

        // need to bring in an insert query for this
        // add columns & execute query
        //
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

        // if
        // insert query for for each row key and

        for (Object key : joinTableRecords.keySet())
        {
            PropertyAccessor accessor = PropertyAccessorFactory.getPropertyAccessor((Field) entityMetadata
                    .getIdAttribute().getJavaMember());

            Set<Object> values = joinTableRecords.get(key); // join column value

            for (Object value : values)
            {
                if (value != null)
                {
                    String insertQuery = insert_Query;
                    columnValueBuilder.append(CQLTranslator.QUOTE_STR);
                    columnValueBuilder.append(PropertyAccessorHelper.getString(key) + "\001"
                            + PropertyAccessorHelper.getString(value));
                    columnValueBuilder.append(CQLTranslator.QUOTE_STR);
                    columnValueBuilder.append(CQLTranslator.COMMA_STR);
                    translator.appendValue(columnValueBuilder, key.getClass(), key, true, false);
                    columnValueBuilder.append(CQLTranslator.COMMA_STR);
                    translator.appendValue(columnValueBuilder, value.getClass(), value, true, false);

                    insertQuery = StringUtils.replace(insertQuery, CQLTranslator.COLUMN_VALUES,
                            columnValueBuilder.toString());
                    statements.append(insertQuery);
                    statements.append(" ");
                }

            }

        }

        if (!StringUtils.isBlank(statements.toString()))
        {
            batch_Query = StringUtils.replace(batch_Query, CQLTranslator.STATEMENT, statements.toString());
            StringBuilder batchBuilder = new StringBuilder();
            batchBuilder.append(batch_Query);
            batchBuilder.append(CQLTranslator.APPLY_BATCH);
            execute(batchBuilder.toString(), null);
        }
    }

    @Override
    public <E> List<E> getColumnsById(String schemaName, String tableName, String pKeyColumnName, String columnName,
            Object pKeyColumnValue, Class columnJavaType)
    {
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
        selectQueryBuilder
                .delete(selectQueryBuilder.lastIndexOf(CQLTranslator.AND_CLAUSE), selectQueryBuilder.length());

        ResultSet rSet = execute(selectQueryBuilder.toString(), null);

        Iterator<Row> rowIter = rSet.iterator();
        while (rowIter.hasNext())
        {
            Row row = rowIter.next();
            DataType dataType = row.getColumnDefinitions().getType(columnName);
            Object columnValue = DSClientUtilities.assign(row, null, null, dataType.getName(), null, columnName, null);
            results.add(columnValue);
        }
        return results;
    }

    @Override
    public Object[] findIdsByColumn(String schemaName, String tableName, String pKeyName, String columnName,
            Object columnValue, Class entityClazz)
    {
        EntityMetadata metadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, entityClazz);

        return getColumnsById(schemaName, tableName, columnName,
                ((AbstractAttribute) metadata.getIdAttribute()).getJPAColumnName(), columnValue,
                metadata.getIdAttribute().getBindableJavaType()).toArray();
    }

    @Override
    public void deleteByColumn(String schemaName, String tableName, String columnName, Object columnValue)
    {

        session = session == null ? factory.getConnection() : this.session;
        String rowKeyName = null;
        CQLTranslator translator = new CQLTranslator();
        try
        {
            List<ColumnMetadata> primaryKeys = session.getCluster().getMetadata().getKeyspace("\"" + schemaName + "\"")
                    .getTable("\"" + tableName + "\"").getPrimaryKey();
            rowKeyName = primaryKeys.get(0).getName();
        }
        finally
        {
            // factory.releaseConnection(session);
        }

        List rowKeys = getColumnsById(schemaName, tableName, columnName, rowKeyName, columnValue,
                columnValue.getClass());
        for (Object rowKey : rowKeys)
        {
            if (rowKey != null)
            {
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

    @Override
    public List<Object> findByRelation(String colName, Object colValue, Class entityClazz)
    {
        EntityMetadata m = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, entityClazz);
        CQLTranslator translator = new CQLTranslator();
        String selectQuery = translator.SELECTALL_QUERY;
        selectQuery = StringUtils.replace(selectQuery, CQLTranslator.COLUMN_FAMILY,
                translator.ensureCase(new StringBuilder(), m.getTableName(), false).toString());

        StringBuilder selectQueryBuilder = new StringBuilder(selectQuery);
        selectQueryBuilder.append(CQLTranslator.ADD_WHERE_CLAUSE);

        translator.buildWhereClause(selectQueryBuilder, colValue.getClass(), colName, colValue,
                CQLTranslator.EQ_CLAUSE, false);
        selectQueryBuilder
                .delete(selectQueryBuilder.lastIndexOf(CQLTranslator.AND_CLAUSE), selectQueryBuilder.length());

        ResultSet rSet = (ResultSet) this.execute(selectQueryBuilder.toString(), null);

        return iterateAndReturn(rSet, entityClazz, m);
    }

    @Override
    public EntityReader getReader()
    {
        return this.reader;
    }

    @Override
    public Class<CassQuery> getQueryImplementor()
    {
        return CassQuery.class;
    }

    @Override
    public List find(Class entityClass, List<String> relationNames, boolean isWrapReq, EntityMetadata metadata,
            Object... rowIds)
    {
        return findAll(entityClass, null, rowIds);
    }

    @Override
    protected List<SuperColumn> loadSuperColumns(String keyspace, String columnFamily, String rowId,
            String... superColumnNames)
    {
        throw new UnsupportedOperationException(
                "Support for super columns is not available with DS java driver. Either use Thrift or pelops for the same");
    }

    @Override
    public List executeQuery(Class clazz, List<String> relationalField, boolean isNative, String cqlQuery)
    {
        ResultSet rSet = (ResultSet) this.execute(cqlQuery, null);
        EntityMetadata metadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, clazz);
        return iterateAndReturn(rSet, clazz, metadata);
    }

    @Override
    public List find(List<IndexClause> ixClause, EntityMetadata m, boolean isRelation, List<String> relations,
            int maxResult, List<String> columns)
    {
        throw new UnsupportedOperationException("Support available only for thrift/pelops.");
    }

    @Override
    public List findByRange(byte[] muinVal, byte[] maxVal, EntityMetadata m, boolean isWrapReq, List<String> relations,
            List<String> columns, List<IndexExpression> conditions, int maxResults) throws Exception
    {
        throw new UnsupportedOperationException("Support available only for thrift/pelops.");
    }

    @Override
    public List<SearchResult> searchInInvertedIndex(String columnFamilyName, EntityMetadata m,
            Map<Boolean, List<IndexClause>> indexClauseMap)
    {
        throw new UnsupportedOperationException("Support available only for thrift/pelops.");
    }

    @Override
    public List<EnhanceEntity> find(EntityMetadata m, List<String> relationNames, List<IndexClause> conditions,
            int maxResult, List<String> columns)
    {
        throw new UnsupportedOperationException("Support available only for thrift/pelops.");
    }

    @Override
    protected CassandraDataHandler getDataHandler()
    {
        throw new UnsupportedOperationException("Support available only for thrift/pelops.");
    }

    @Override
    public void delete(Object entity, Object pKey)
    {
        EntityMetadata m = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, entity.getClass());

        MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                m.getPersistenceUnit());

        AbstractManagedType managedType = (AbstractManagedType) metaModel.entity(m.getEntityClazz());

        // For secondary tables.
        List<String> secondaryTables = ((DefaultEntityAnnotationProcessor) managedType.getEntityAnnotation())
                .getSecondaryTablesName();
        secondaryTables.add(m.getTableName());

        for (String tableName : secondaryTables)
        {
            this.execute(onDeleteQuery(m, tableName, metaModel, pKey), null);
        }
    }

    @Override
    protected Object getConnection()
    {
        // do nothing returning null.
        return null;
    }

    @Override
    protected Object getConnection(Object connection)
    {
        return null;
    }

    @Override
    protected void releaseConnection(Object conn)
    {
        // do nothing
    }

    @Override
    protected <T> T execute(final String query, Object connection)
    {

        session = session == null ? factory.getConnection() : this.session;
        try
        {
            Statement queryStmt = new SimpleStatement(query);
            KunderaCoreUtils.showQuery(query, showQuery);
            queryStmt.setConsistencyLevel(ConsistencyLevel.valueOf(this.consistencyLevel.name()));
            return (T) session.execute(queryStmt);
        }
        catch (Exception e)
        {
            log.error("Error while executing query {}.", query);
            throw new KunderaException(e);
        }
        finally
        {
            // factory.releaseConnection(session);
        }
    }

    public int executeUpdateDeleteQuery(String cqlQuery)
    {
        Session session = null;
        try
        {
            if (log.isInfoEnabled())
            {
                log.info("Executing cql query {}.", cqlQuery);
            }
            session = session == null ? factory.getConnection() : this.session;
            KunderaCoreUtils.showQuery(cqlQuery, showQuery);
            session.execute(cqlQuery);
        }
        finally
        {
            // factory.releaseConnection(session);
        }
        // TODO: can't find a way to return number of updated records.
        return 0;

    }

    private List iterateAndReturn(ResultSet rSet, Class entityClazz, EntityMetadata metadata)
    {
        MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                metadata.getPersistenceUnit());
        EntityType entityType = metaModel.entity(metadata.getEntityClazz());

        Iterator<Row> rowIter = rSet.iterator();
        List results = new ArrayList();

        Map<String, Object> relationalValues = new HashMap<String, Object>();
        // Map<String, Field> compositeColumns = new HashMap<String, Field>();

        // Object compositeKeyInstance = null;
        // boolean isCompositeKey = false;
        // if
        // (metaModel.isEmbeddable(metadata.getIdAttribute().getBindableJavaType()))
        // {
        // isCompositeKey = true;
        // EmbeddableType compositeKey =
        // metaModel.embeddable(metadata.getIdAttribute().getBindableJavaType());
        // Iterator<Attribute> attributes =
        // compositeKey.getAttributes().iterator();
        // while (attributes.hasNext())
        // {
        // Attribute attribute = attributes.next();
        // String columnName = ((AbstractAttribute)
        // attribute).getJPAColumnName();
        // compositeColumns.put(columnName, (Field) attribute.getJavaMember());
        // }
        // }

        while (rowIter.hasNext())
        {
            Object entity = null;
            Row row = rowIter.next();
            ColumnDefinitions columnDefs = row.getColumnDefinitions();
            Iterator<Definition> columnDefIter = columnDefs.iterator();

            // if (isCompositeKey)
            // {
            // compositeKeyInstance = getCompositeKeyInstance(metadata);
            // }
            entity = iteratorColumns(metadata, metaModel, entityType, relationalValues, entity, row, columnDefIter);

            // if (compositeKeyInstance != null)
            // {
            // // compositeKeyInstance = getCompositeKeyInstance(metadata);
            // entity = CassandraUtilities.initialize(metadata, entity,
            // compositeKeyInstance);
            // }

            if (entity != null && entity.getClass().isAssignableFrom(metadata.getEntityClazz()))
            {
                Object rowKey = PropertyAccessorHelper.getId(entity, metadata);

                // populate secondary tables data if there is any.
                populateSecondaryTableData(rowKey, entity, metaModel, metadata);

                if (!relationalValues.isEmpty())
                {
                    results.add(new EnhanceEntity(entity, rowKey, relationalValues));
                }
                else
                {
                    results.add(entity);
                }
            }
            else if (entity != null)
            {
                results.add(entity);
            }
        }

        return results;
    }

    /**
     * 
     * Populates data form secondary tables of entity for given row key.
     * 
     * @param rowId
     * @param entity
     * @param metaModel
     * @param metadata
     * @return
     */
    private void populateSecondaryTableData(Object rowId, Object entity, MetamodelImpl metaModel,
            EntityMetadata metadata)
    {
        AbstractManagedType managedType = (AbstractManagedType) metaModel.entity(metadata.getEntityClazz());
        List<String> secondaryTables = ((DefaultEntityAnnotationProcessor) managedType.getEntityAnnotation())
                .getSecondaryTablesName();

        for (String tableName : secondaryTables)
        {
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

    private Object iteratorColumns(EntityMetadata metadata, MetamodelImpl metamodel, EntityType entityType,
            Map<String, Object> relationalValues, Object entity, Row row, Iterator<Definition> columnDefIter)
    {
        while (columnDefIter.hasNext())
        {
            Definition columnDef = columnDefIter.next();
            final String columnName = columnDef.getName(); // column name

            DataType dataType = columnDef.getType(); // data type

            if (metadata.getRelationNames() != null && metadata.getRelationNames().contains(columnName)
                    && !columnName.equals(((AbstractAttribute) metadata.getIdAttribute()).getJPAColumnName()))
            {
                Object relationalValue = DSClientUtilities.assign(row, null, metadata, dataType.getName(), entityType,
                        columnName, null);
                relationalValues.put(columnName, relationalValue);
            }
            else
            {
                String fieldName = metadata.getFieldName(columnName);
                Attribute attribute = fieldName != null ? entityType.getAttribute(fieldName) : null;

                if (attribute != null)
                {
                    if (!attribute.isAssociation())
                    {
                        entity = DSClientUtilities.assign(row, entity, metadata, dataType.getName(), entityType,
                                columnName, null);
                    }
                }
                else
                {
                    entity = populateCompositeId(metadata, entity, columnName, row, metamodel,
                            metadata.getIdAttribute(), metadata.getEntityClazz(), dataType);
                }
            }

        }
        return entity;
    }

    @Override
    public void close()
    {
        super.close();
        if (this.session != null)
        {
            factory.releaseConnection(this.session);
        }
    }

    @Override
    public String getPersistenceUnit()
    {
        return super.getPersistenceUnit();
    }

    /**
     * 
     * @param metadata
     * @param entity
     * @param columnName
     * @param row
     * @param metaModel
     * @param attribute
     * @param entityClazz
     * @param dataType
     * @return
     */
    private Object populateCompositeId(EntityMetadata metadata, Object entity, String columnName, Row row,
            MetamodelImpl metaModel, Attribute attribute, Class<?> entityClazz, DataType dataType)
    {
        Class javaType = ((AbstractAttribute) attribute).getBindableJavaType();

        if (metaModel.isEmbeddable(javaType))
        {
            EmbeddableType compoundKey = metaModel.embeddable(javaType);
            Object compoundKeyObject = null;
            try
            {
                Set<Attribute> attributes = compoundKey.getAttributes();
                entity = CassandraUtilities.initialize(entityClazz, entity);

                for (Attribute compoundAttribute : attributes)
                {
                    compoundKeyObject = compoundKeyObject == null ? getCompoundKey(attribute, entity)
                            : compoundKeyObject;

                    if (metaModel.isEmbeddable(((AbstractAttribute) compoundAttribute).getBindableJavaType()))
                    {
                        Object compoundObject = populateCompositeId(metadata, compoundKeyObject, columnName, row,
                                metaModel, compoundAttribute, javaType, dataType);
                        PropertyAccessorHelper.set(entity, (Field) attribute.getJavaMember(), compoundObject);
                    }
                    else if (((AbstractAttribute) compoundAttribute).getJPAColumnName().equals(columnName))
                    {
                        DSClientUtilities.assign(row, compoundKeyObject, null, dataType.getName(), null, columnName,
                                (Field) compoundAttribute.getJavaMember());
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
     * 
     * @param attribute
     * @param entity
     * @return
     * @throws InstantiationException
     * @throws IllegalAccessException
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
}
