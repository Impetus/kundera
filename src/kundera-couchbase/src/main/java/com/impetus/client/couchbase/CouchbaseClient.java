/*******************************************************************************
 *  * Copyright 2017 Impetus Infotech.
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
package com.impetus.client.couchbase;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.persistence.metamodel.EntityType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.N1qlParams;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.N1qlQueryRow;
import com.couchbase.client.java.query.Statement;
import com.couchbase.client.java.query.consistency.ScanConsistency;
import com.impetus.client.couchbase.query.CouchbaseQuery;
import com.impetus.kundera.KunderaException;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.ClientBase;
import com.impetus.kundera.db.RelationHolder;
import com.impetus.kundera.generator.Generator;
import com.impetus.kundera.index.IndexManager;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.ClientMetadata;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.context.jointable.JoinTableData;

/**
 * The Class CouchbaseClient.
 * 
 * @author devender.yadav
 */
public class CouchbaseClient extends ClientBase implements Client<CouchbaseQuery>
{

    /** The Constant LOGGER. */
    private static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseClient.class);

    /** The bucket. */
    private Bucket bucket;

    /** The reader. */
    private EntityReader reader;

    /** The handler. */
    private DefaultCouchbaseDataHandler handler;

    /**
     * Instantiates a new couchbase client.
     *
     * @param kunderaMetadata
     *            the kundera metadata
     * @param indexManager
     *            the index manager
     * @param reader
     *            the reader
     * @param properties
     *            the properties
     * @param persistenceUnit
     *            the persistence unit
     * @param bucket
     *            the bucket
     * @param clientMetadata
     *            the client metadata
     */
    protected CouchbaseClient(KunderaMetadata kunderaMetadata, IndexManager indexManager, EntityReader reader,
            Map<String, Object> properties, String persistenceUnit, Bucket bucket, ClientMetadata clientMetadata)
    {
        super(kunderaMetadata, properties, persistenceUnit);
        this.reader = reader;
        this.bucket = bucket;
        this.indexManager = indexManager;
        this.clientMetadata = clientMetadata;
        handler = new DefaultCouchbaseDataHandler();
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

        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, entityClass);
        JsonDocument doc;
        String id = generateJsonDocId(entityMetadata.getTableName(), key.toString());
        doc = bucket.get(id);
        LOGGER.debug("Found result for ID : " + key.toString() + " in the " + bucket.name() + " Bucket");

        if (doc == null)
        {
            return null;
        }
        else
        {
            MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata()
                    .getMetamodel(entityMetadata.getPersistenceUnit());
            EntityType entityType = metaModel.entity(entityMetadata.getEntityClazz());
            return handler.getEntityFromDocument(entityClass, doc.content(), entityType);
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
        return null;
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
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#close()
     */
    @Override
    public void close()
    {
        externalProperties = null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.client.Client#persistJoinTable(com.impetus.kundera.
     * persistence.context.jointable.JoinTableData )
     */
    @Override
    public void persistJoinTable(JoinTableData joinTableData)
    {

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
        return null;
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
        return null;
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

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#findByRelation(java.lang.String,
     * java.lang.Object, java.lang.Class)
     */
    @Override
    public List<Object> findByRelation(String colName, Object colValue, Class entityClazz)
    {
        return null;
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
    public Class<CouchbaseQuery> getQueryImplementor()
    {
        return CouchbaseQuery.class;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#getIdGenerator()
     */
    @Override
    public Generator getIdGenerator()
    {
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.ClientBase#onPersist(com.impetus.kundera.
     * metadata.model.EntityMetadata, java.lang.Object, java.lang.Object,
     * java.util.List)
     */
    @Override
    protected void onPersist(EntityMetadata entityMetadata, Object entity, Object id, List<RelationHolder> rlHolders)
    {
        JsonDocument doc = handler.getDocumentFromEntity(entityMetadata, entity, kunderaMetadata);

        if (!isUpdate)
        {
            bucket.insert(doc);
            LOGGER.debug("Inserted document with ID : " + doc.id() + " in the " + bucket.name() + " Bucket");
        }
        else
        {
            bucket.upsert(doc);
            LOGGER.debug("Updated document with ID : " + doc.id() + " in the " + bucket.name() + " Bucket");
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.ClientBase#delete(java.lang.Object,
     * java.lang.Object)
     */
    @Override
    protected void delete(Object entity, Object pKey)
    {
        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, entity.getClass());
        String id = generateJsonDocId(entityMetadata.getTableName(), pKey.toString());
        bucket.remove(id);
        LOGGER.debug("Deleted document with ID : " + id + " from the " + bucket.name() + " Bucket");
    }

    /**
     * Generate json doc id.
     *
     * @param tableName
     *            the table name
     * @param pKey
     *            the key
     * @return the string
     */
    private String generateJsonDocId(String tableName, String pKey)
    {
        return tableName + "_" + pKey;
    }

    /**
     * Execute query.
     *
     * @param stmt
     *            the statement
     * @param em
     *            the entity manager
     * @return the list
     */
    public List executeQuery(Statement stmt, EntityMetadata em)
    {
        N1qlQuery query = N1qlQuery.simple(stmt, N1qlParams.build().consistency(ScanConsistency.REQUEST_PLUS));
        N1qlQueryResult list = bucket.query(query);
        LOGGER.debug("Executed query : " + query.toString() + " on the " + bucket.name() + " Bucket");
        validateQueryResults(stmt.toString(), list);

        List records = new ArrayList<>();
        for (N1qlQueryRow row : list)
        {
            MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata()
                    .getMetamodel(em.getPersistenceUnit());
            EntityType entityType = metaModel.entity(em.getEntityClazz());

            JsonObject jsonObj = row.value().containsKey(em.getSchema()) ? row.value().getObject(em.getSchema())
                    : row.value();

            records.add(handler.getEntityFromDocument(em.getEntityClazz(), jsonObj, entityType));
        }

        return records;

    }

    /**
     * Execute native query.
     *
     * @param n1qlQuery
     *            the n1ql query
     * @param em
     *            the entity manager
     * @return the list
     */
    public List executeNativeQuery(String n1qlQuery, EntityMetadata em)
    {
        N1qlQueryResult result = bucket
                .query(N1qlQuery.simple(n1qlQuery, N1qlParams.build().consistency(ScanConsistency.REQUEST_PLUS)));
        LOGGER.debug("Executed query : " + n1qlQuery + " on the " + bucket.name() + " Bucket");

        validateQueryResults(n1qlQuery, result);
        return result.allRows();

    }

    /**
     * Validate query results.
     *
     * @param query
     *            the query
     * @param result
     *            the result
     */
    private void validateQueryResults(String query, N1qlQueryResult result)
    {
        LOGGER.debug("Query output status: " + result.finalSuccess());

        if (!result.finalSuccess())
        {
            StringBuilder errorBuilder = new StringBuilder();
            for (JsonObject obj : result.errors())
            {
                errorBuilder.append(obj.toString());
                errorBuilder.append("\n");
            }
            errorBuilder.deleteCharAt(errorBuilder.length() - 1);
            String errors = errorBuilder.toString();
            LOGGER.error(errors);
            throw new KunderaException("Not able to execute query/statement:" + query + ". More details : " + errors);
        }
    }

}
