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
import java.util.UUID;

import javax.persistence.PersistenceException;
import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.EmbeddableType;
import javax.persistence.metamodel.EntityType;

import org.apache.cassandra.thrift.IndexClause;
import org.apache.cassandra.thrift.IndexExpression;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.SuperColumn;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.commons.lang.StringUtils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Query;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
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
import com.impetus.kundera.generator.TableGenerator;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.TableGeneratorDiscriptor;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.api.Batcher;
import com.impetus.kundera.persistence.context.jointable.JoinTableData;
import com.impetus.kundera.property.PropertyAccessorHelper;

/**
 * Kundera powered data stax java driver based client.
 * 
 * @author vivek.mishra
 * 
 */
public class DSClient extends CassandraClientBase implements Client<CassQuery>, Batcher, TableGenerator
{

    /** log for this class. */
    private static Logger log = LoggerFactory.getLogger(DSClient.class);

    private DSClientFactory factory;

    private EntityReader reader;

    public DSClient(DSClientFactory factory, String persistenceUnit, Map<String, Object> externalProperties,
            KunderaMetadata kunderaMetadata, EntityReader reader)
    {
        super(persistenceUnit, externalProperties, kunderaMetadata);
        this.factory = factory;
        this.reader = reader;
        this.clientMetadata = factory.getClientMetadata();
        this.setCqlVersion(CassandraConstants.CQL_VERSION_3_0);
    }

    @Override
    protected void onPersist(EntityMetadata entityMetadata, Object entity, Object id, List<RelationHolder> rlHolders)
    {

        // Insert, update, delete is fine
        // find by id, find all, find and execute batch

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
        catch (UnavailableException e)
        {
            log.error("Error while persisting record, Caused by: .", e);
            throw new KunderaException(e);
        }
        catch (TimedOutException e)
        {
            log.error("Error while persisting record, Caused by: .", e);
            throw new KunderaException(e);
        }
        catch (SchemaDisagreementException e)
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
        MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                metadata.getPersistenceUnit());

        CQLTranslator translator = new CQLTranslator();

        String tableName = metadata.getTableName();
        String select_Query = translator.SELECTALL_QUERY;
        select_Query = StringUtils.replace(select_Query, CQLTranslator.COLUMN_FAMILY,
                translator.ensureCase(new StringBuilder(), tableName, false).toString());
        StringBuilder builder = new StringBuilder(select_Query);
        onWhereClause(metadata, rowId, translator, builder, metaModel);
        ResultSet rSet = this.execute(builder.toString(), null);
        List results = iterateAndReturn(rSet, entityClass, metadata);
        return results.isEmpty() ? null : results.get(0);
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
    public Object generate(TableGeneratorDiscriptor discriptor)
    {
        final String generatedId = "Select now() from system.schema_columns";
        ResultSet rSet = this.execute(generatedId, null);

        UUID uuid = rSet.iterator().next().getUUID(0);
        return uuid.timestamp();
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

        // TODO Auto-generated method stub

    }

    @Override
    public <E> List<E> getColumnsById(String schemaName, String tableName, String pKeyColumnName, String columnName,
            Object pKeyColumnValue, Class columnJavaType)
    {
        // TODO:: Add support for Many-to-Many join tables.

        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Object[] findIdsByColumn(String schemaName, String tableName, String pKeyName, String columnName,
            Object columnValue, Class entityClazz)
    {
        // TODO:: Add support for Many-to-Many join tables.

        return null;
    }

    @Override
    public void deleteByColumn(String schemaName, String tableName, String columnName, Object columnValue)
    {
        CQLTranslator translator = new CQLTranslator();
        String deleteQuery = CQLTranslator.DELETE_QUERY;

        deleteQuery = StringUtils.replace(deleteQuery, CQLTranslator.COLUMN_FAMILY,
                translator.ensureCase(new StringBuilder(), tableName, false).toString());

        StringBuilder deleteQueryBuilder = new StringBuilder(deleteQuery);
        deleteQueryBuilder.append(CQLTranslator.ADD_WHERE_CLAUSE);
        deleteQueryBuilder = translator.ensureCase(deleteQueryBuilder, columnName, false);
        deleteQueryBuilder.append(CQLTranslator.EQ_CLAUSE);
        translator.appendValue(deleteQueryBuilder, columnValue.getClass(), columnValue, false, false);
        this.execute(deleteQueryBuilder.toString(), null);
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
        // TODO:: Handle secondary table support case
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

        this.execute(onDeleteQuery(m, m.getTableName(), metaModel, pKey), null);
    }

    @Override
    protected Object getConnection()
    {
        throw new UnsupportedOperationException("Support available only for thrift/pelops.");
    }

    @Override
    protected Object getConnection(Object connection)
    {
        throw new UnsupportedOperationException("Support available only for thrift/pelops.");
    }

    @Override
    protected void releaseConnection(Object conn)
    {
        throw new UnsupportedOperationException("Support available only for thrift/pelops.");
    }

    @Override
    protected <T> T execute(final String query, Object connection)
    {
        Session session=factory.getConnection();
        try
        {
            Query queryStmt = new SimpleStatement(query);
            queryStmt.setConsistencyLevel(ConsistencyLevel.valueOf(this.consistencyLevel.name()));
            return (T) session.execute(queryStmt);
        }
        catch (Exception e)
        {
            log.error("Error while executing query {}", query);
            throw new KunderaException(e);
        }finally
        {
            factory.releaseConnection(session);
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
        session = factory.getConnection();
        session.execute(cqlQuery);
        }finally
        {
            factory.releaseConnection(session);
        }
        // TODO: can't find a way to return number of updated records.
        return 0;

    }

    private List iterateAndReturn(ResultSet rSet, Class entityClazz, EntityMetadata metadata)
    {
        // TODO:: Handle secondary table support case

        MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                metadata.getPersistenceUnit());
        EntityType entityType = metaModel.entity(metadata.getEntityClazz());

        Iterator<Row> rowIter = rSet.iterator();
        List results = new ArrayList();

        Map<String, Object> relationalValues = new HashMap<String, Object>();
        Map<String, Object> compositeValues = new HashMap<String, Object>();
        Map<String, Field> compositeColumns = new HashMap<String, Field>();

        Object compositeKeyInstance = null;
        if (metaModel.isEmbeddable(metadata.getIdAttribute().getBindableJavaType()))
        {
            compositeKeyInstance = getCompositeKeyInstance(metadata);
            EmbeddableType compositeKey = metaModel.embeddable(metadata.getIdAttribute().getBindableJavaType());
            Iterator<Attribute> attributes = compositeKey.getAttributes().iterator();
            while (attributes.hasNext())
            {
                Attribute attribute = attributes.next();
                String columnName = ((AbstractAttribute) attribute).getJPAColumnName();
                compositeColumns.put(columnName, (Field) attribute.getJavaMember());
            }
        }

        while (rowIter.hasNext())
        {
            Object entity = null;
            Row row = rowIter.next();
            ColumnDefinitions columnDefs = row.getColumnDefinitions();
            Iterator<Definition> columnDefIter = columnDefs.iterator();

            entity = iteratorColumns(metadata, entityType, relationalValues, compositeColumns, compositeKeyInstance,
                    entity, row, columnDefIter);

            if (compositeKeyInstance != null)
            {
                entity = CassandraUtilities.initialize(metadata, entity, compositeKeyInstance);
            }

            if (!relationalValues.isEmpty())
            {
                results.add(new EnhanceEntity(entity, PropertyAccessorHelper.getId(entity, metadata), relationalValues));
            }
            else
            {
                results.add(entity);
            }
        }

        return results;
    }

    private Object iteratorColumns(EntityMetadata metadata, EntityType entityType,
            Map<String, Object> relationalValues, Map<String, Field> compositeColumns, Object compositeKeyInstance,
            Object entity, Row row, Iterator<Definition> columnDefIter)
    {
        while (columnDefIter.hasNext())
        {
            Definition columnDef = columnDefIter.next();
            final String columnName = columnDef.getName(); // column name

            DataType dataType = columnDef.getType(); // data type

            if (metadata.getRelationNames() != null && metadata.getRelationNames().contains(columnName))
            {
                Object relationalValue = DSClientUtilities.assign(row, null, metadata, dataType.getName(), entityType,
                        columnName, null);
                relationalValues.put(columnName, relationalValue);
            }
            else if (compositeColumns.containsKey(columnName))
            {
                Object compositeKeyAttributeValue = DSClientUtilities.assign(row, null, metadata, dataType.getName(),
                        entityType, columnName, compositeColumns.get(columnName));
                PropertyAccessorHelper.set(compositeKeyInstance, compositeColumns.get(columnName),
                        compositeKeyAttributeValue);
            }
            else
            {
                entity = DSClientUtilities.assign(row, entity, metadata, dataType.getName(), entityType, columnName,
                        null);
            }

        }
        return entity;
    }

    @Override
    public String getPersistenceUnit()
    {
        return super.getPersistenceUnit();
    }

    /**
     * @param metadata
     * @return
     */
    private Object getCompositeKeyInstance(EntityMetadata metadata)
    {
        Object compositeKeyInstance = null;
        try
        {
            compositeKeyInstance = metadata.getIdAttribute().getBindableJavaType().newInstance();
        }
        catch (Exception e)
        {
            throw new PersistenceException("Error occured while instantiating entity.", e);
        }

        return compositeKeyInstance;
    }
}
