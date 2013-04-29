/*******************************************************************************
 * * Copyright 2012 Impetus Infotech.
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
package com.impetus.client.mongodb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.bson.types.ObjectId;

import com.impetus.client.mongodb.query.MongoDBQuery;
import com.impetus.client.mongodb.utils.MongoDBUtils;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.ClientBase;
import com.impetus.kundera.client.ClientPropertiesSetter;
import com.impetus.kundera.db.RelationHolder;
import com.impetus.kundera.generator.AutoGenerator;
import com.impetus.kundera.graph.Node;
import com.impetus.kundera.index.IndexManager;
import com.impetus.kundera.lifecycle.states.RemovedState;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.api.Batcher;
import com.impetus.kundera.persistence.context.jointable.JoinTableData;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBEncoder;
import com.mongodb.DBObject;
import com.mongodb.DefaultDBEncoder;
import com.mongodb.WriteConcern;

/**
 * CLient class for MongoDB database.
 * 
 * @author impetusopensource
 */
public class MongoDBClient extends ClientBase implements Client<MongoDBQuery>, Batcher, ClientPropertiesSetter,
        AutoGenerator
{
    /** The mongo db. */
    private DB mongoDb;

    /** The reader. */
    private EntityReader reader;

    /** The data handler. */
    private MongoDBDataHandler handler;

    /** The log. */
    private static Log log = LogFactory.getLog(MongoDBClient.class);

    private List<Node> nodes = new ArrayList<Node>();

    private int batchSize;

    private WriteConcern writeConcern = null;

    private DBEncoder encoder = DefaultDBEncoder.FACTORY.create();

    private Map<String, Object> puProperties;

    /**
     * Instantiates a new mongo db client.
     * 
     * @param mongo
     *            the mongo
     * @param mgr
     *            the mgr
     * @param reader
     *            the reader
     * @param puProperties
     */
    public MongoDBClient(Object mongo, IndexManager mgr, EntityReader reader, String persistenceUnit,
            Map<String, Object> puProperties)
    {
        // TODO: This could be a constly call, see how connection pooling is
        // relevant here
        this.mongoDb = (DB) mongo;
        this.indexManager = mgr;
        this.reader = reader;
        this.persistenceUnit = persistenceUnit;
        this.puProperties = puProperties;
        handler = new MongoDBDataHandler();

        populateBatchSize(persistenceUnit, this.puProperties);

    }

    @Override
    public void persistJoinTable(JoinTableData joinTableData)
    {
        String joinTableName = joinTableData.getJoinTableName();
        String joinColumnName = joinTableData.getJoinColumnName();
        String invJoinColumnName = joinTableData.getInverseJoinColumnName();
        Map<Object, Set<Object>> joinTableRecords = joinTableData.getJoinTableRecords();

        DBCollection dbCollection = mongoDb.getCollection(joinTableName);
        List<DBObject> documents = new ArrayList<DBObject>();

        for (Object key : joinTableRecords.keySet())
        {
            Set<Object> values = joinTableRecords.get(key);
            Object joinColumnValue = key;

            for (Object childId : values)
            {
                DBObject dbObj = new BasicDBObject();
                dbObj.put(joinColumnName, MongoDBUtils.populateValue(joinColumnValue, joinColumnValue.getClass()));
                dbObj.put(invJoinColumnName, MongoDBUtils.populateValue(childId, childId.getClass()));
                documents.add(dbObj);
            }
        }
        dbCollection.insert(documents.toArray(new BasicDBObject[0]), getWriteConcern(), encoder);
    }

    @Override
    public <E> List<E> getColumnsById(String schemaName, String joinTableName, String joinColumnName,
            String inverseJoinColumnName, Object parentId, Class columnJavaType)
    {
        List<E> foreignKeys = new ArrayList<E>();

        DBCollection dbCollection = mongoDb.getCollection(joinTableName);
        BasicDBObject query = new BasicDBObject();

        query.put(joinColumnName, MongoDBUtils.populateValue(parentId, parentId.getClass()));

        DBCursor cursor = dbCollection.find(query);
        DBObject fetchedDocument = null;

        while (cursor.hasNext())
        {
            fetchedDocument = cursor.next();
            Object foreignKey = fetchedDocument.get(inverseJoinColumnName);
            foreignKey = MongoDBUtils.getTranslatedObject(foreignKey, foreignKey.getClass(), columnJavaType);
            foreignKeys.add((E) foreignKey);
        }
        return foreignKeys;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#findIdsByColumn(java.lang.String,
     * java.lang.String, java.lang.String, java.lang.Object, java.lang.Class)
     */
    @Override
    public Object[] findIdsByColumn(String schemaName, String tableName, String pKeyName, String columnName,
            Object columnValue, Class entityClazz)
    {
        EntityMetadata metadata = KunderaMetadataManager.getEntityMetadata(entityClazz);

        List<Object> primaryKeys = new ArrayList<Object>();

        DBCollection dbCollection = mongoDb.getCollection(tableName);
        BasicDBObject query = new BasicDBObject();

        query.put(columnName, MongoDBUtils.populateValue(columnValue, columnValue.getClass()));

        DBCursor cursor = dbCollection.find(query);
        DBObject fetchedDocument = null;

        while (cursor.hasNext())
        {
            fetchedDocument = cursor.next();
            Object primaryKey = fetchedDocument.get(pKeyName);
            primaryKey = MongoDBUtils.getTranslatedObject(primaryKey, primaryKey.getClass(), metadata.getIdAttribute()
                    .getJavaType());
            primaryKeys.add(primaryKey);
        }

        if (primaryKeys != null && !primaryKeys.isEmpty())
        {
            return primaryKeys.toArray(new Object[0]);
        }
        return null;

    }

    /*
     * (non-Javadoc)
     * 
     * @seecom.impetus.kundera.Client#loadColumns(com.impetus.kundera.ejb.
     * EntityManager, java.lang.Class, java.lang.String, java.lang.String,
     * java.lang.String, com.impetus.kundera.metadata.EntityMetadata)
     */
    @Override
    public Object find(Class entityClass, Object key)
    {
        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(entityClass);

        List<String> relationNames = entityMetadata.getRelationNames();

        if (log.isDebugEnabled())
        {
            log.debug("Fetching data from " + entityMetadata.getTableName() + " for PK " + key);
        }

        DBCollection dbCollection = mongoDb.getCollection(entityMetadata.getTableName());

        BasicDBObject query = new BasicDBObject();

        MetamodelImpl metaModel = (MetamodelImpl) KunderaMetadata.INSTANCE.getApplicationMetadata().getMetamodel(
                entityMetadata.getPersistenceUnit());

        if (metaModel.isEmbeddable(entityMetadata.getIdAttribute().getBindableJavaType()))
        {
            MongoDBUtils.populateCompoundKey(query, entityMetadata, metaModel, key);
        }
        else
        {
            query.put("_id", MongoDBUtils.populateValue(key, key.getClass()));
        }

        // DBCursor cursor = dbCollection.findOne(query);
        DBObject fetchedDocument = dbCollection.findOne(query);
        ;

        /*
         * if (cursor.hasNext()) { fetchedDocument = cursor.next(); } else {
         * return null; }
         */

        if (fetchedDocument != null)
        {
            Object enhancedEntity = handler.getEntityFromDocument(entityMetadata.getEntityClazz(), entityMetadata,
                    fetchedDocument, relationNames);

            return enhancedEntity;
        }

        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#findAll(java.lang.Class,
     * java.lang.Object[])
     */
    @Override
    public <E> List<E> findAll(Class<E> entityClass, String[] columnsToSelect, Object... keys)
    {
        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(entityClass);

        log.debug("Fetching data from " + entityMetadata.getTableName() + " for Keys " + keys);

        DBCollection dbCollection = mongoDb.getCollection(entityMetadata.getTableName());

        BasicDBObject query = new BasicDBObject();

        query.put("_id", new BasicDBObject("$in", keys));

        DBCursor cursor = dbCollection.find(query);

        List entities = new ArrayList<E>();
        while (cursor.hasNext())
        {
            DBObject fetchedDocument = cursor.next();
            Object entity = handler.getEntityFromDocument(entityMetadata.getEntityClazz(), entityMetadata,
                    fetchedDocument, entityMetadata.getRelationNames());
            entities.add(entity);
        }
        return entities;
    }

    /**
     * Loads columns from multiple rows restricting results to conditions stored
     * in <code>filterClauseQueue</code>.
     * 
     * @param <E>
     *            the element type
     * @param entityMetadata
     *            the entity metadata
     * @param mongoQuery
     *            the mongo query
     * @param result
     *            the result
     * @param relationNames
     *            the relation names
     * @param orderBy
     *            the order by
     * @param maxResult
     * @param keys
     * @return the list
     * @throws Exception
     *             the exception
     */
    public <E> List<E> loadData(EntityMetadata entityMetadata, BasicDBObject mongoQuery, List<String> relationNames,
            BasicDBObject orderBy, int maxResult, BasicDBObject keys, String... results) throws Exception
    {
        String documentName = entityMetadata.getTableName();
        Class clazz = entityMetadata.getEntityClazz();

        DBCollection dbCollection = mongoDb.getCollection(documentName);
        List entities = new ArrayList<E>();

        if (results != null && results.length > 0)
        {
            for (int i = 1; i < results.length; i++)
            {
                String result = results[i];

                // If User wants search on a column within a particular super
                // column,
                // fetch that embedded object collection only
                // otherwise retrieve whole entity
                // TODO: improve code
                if (result != null && result.indexOf(".") >= 0)
                {
                    // TODO i need to discuss with Amresh before modifying it.
                    entities.addAll(handler.getEmbeddedObjectList(dbCollection, entityMetadata, documentName,
                            mongoQuery, result, orderBy, maxResult, keys));
                    return entities;
                }
            }
        }
        log.debug("Fetching data from " + documentName + " for Filter " + mongoQuery.toString());

        DBCursor cursor = orderBy != null ? dbCollection.find(mongoQuery, keys).sort(orderBy) : dbCollection.find(
                mongoQuery, keys).limit(maxResult);
        while (cursor.hasNext())
        {
            DBObject fetchedDocument = cursor.next();
            Object entity = handler.getEntityFromDocument(clazz, entityMetadata, fetchedDocument, relationNames);
            entities.add(entity);
        }
        return entities;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#delete(java.lang.Object,
     * java.lang.Object)
     */
    @Override
    public void delete(Object entity, Object pKey)
    {
        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(entity.getClass());

        DBCollection dbCollection = mongoDb.getCollection(entityMetadata.getTableName());

        // Find the DBObject to remove first
        BasicDBObject query = new BasicDBObject();

        MetamodelImpl metaModel = (MetamodelImpl) KunderaMetadata.INSTANCE.getApplicationMetadata().getMetamodel(
                entityMetadata.getPersistenceUnit());

        if (metaModel.isEmbeddable(entityMetadata.getIdAttribute().getBindableJavaType()))
        {
            MongoDBUtils.populateCompoundKey(query, entityMetadata, metaModel, pKey);
        }
        else
        {

            query.put("_id", MongoDBUtils.populateValue(pKey, pKey.getClass()));
        }
        dbCollection.remove(query, getWriteConcern(), encoder);
        getIndexManager().remove(entityMetadata, entity, pKey.toString());

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#close()
     */
    @Override
    public void close()
    {
        // TODO Once pool is implemented this code should not be there.
        // Workaround for pool
        this.indexManager.flush();
        puProperties = null;
    }

    /**
     * Creates the index.
     * 
     * @param collectionName
     *            the collection name
     * @param columnList
     *            the column list
     * @param order
     *            the order
     */
    public void createIndex(String collectionName, List<String> columnList, int order)
    {
        DBCollection coll = mongoDb.getCollection(collectionName);

        List<DBObject> indexes = coll.getIndexInfo(); // List of all current
        // indexes on collection
        Set<String> indexNames = new HashSet<String>(); // List of all current
        // index names
        for (DBObject index : indexes)
        {
            BasicDBObject obj = (BasicDBObject) index.get("key");
            Set<String> set = obj.keySet(); // Set containing index name which
            // is key
            indexNames.addAll(set);
        }

        // Create index if not already created
        for (String columnName : columnList)
        {
            if (!indexNames.contains(columnName))
            {
                coll.createIndex(new BasicDBObject(columnName, order));
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#find(java.lang.Class,
     * java.util.Map)
     */
    @Override
    public <E> List<E> find(Class<E> entityClass, Map<String, String> col)
    {
        throw new NotImplementedException("Not yet implemented");
    }

    /**
     * Method to find entity for given association name and association value.
     * 
     * @param colName
     *            the col name
     * @param colValue
     *            the col value
     * @param m
     *            the m
     * @return the list
     */
    public List<Object> findByRelation(String colName, Object colValue, Class entityClazz)
    {
        EntityMetadata m = KunderaMetadataManager.getEntityMetadata(entityClazz);
        // you got column name and column value.
        DBCollection dbCollection = mongoDb.getCollection(m.getTableName());

        BasicDBObject query = new BasicDBObject();

        query.put(colName, MongoDBUtils.populateValue(colValue, colValue.getClass()));

        DBCursor cursor = dbCollection.find(query);
        DBObject fetchedDocument = null;
        List<Object> results = new ArrayList<Object>();
        while (cursor.hasNext())
        {
            fetchedDocument = cursor.next();
            Object entity = handler.getEntityFromDocument(m.getEntityClazz(), m, fetchedDocument, null);
            results.add(entity);
        }

        return results.isEmpty() ? null : results;
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
     * @see com.impetus.kundera.client.Client#deleteByColumn(java.lang.String,
     * java.lang.String, java.lang.Object)
     */
    public void deleteByColumn(String schemaName, String tableName, String columnName, Object columnValue)
    {
        DBCollection dbCollection = mongoDb.getCollection(tableName);
        BasicDBObject query = new BasicDBObject();
        query.put(columnName, columnValue);
        dbCollection.remove(query, getWriteConcern(), encoder);

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#getQueryImplementor()
     */
    @Override
    public Class<MongoDBQuery> getQueryImplementor()
    {
        return MongoDBQuery.class;
    }

    @Override
    protected void onPersist(EntityMetadata entityMetadata, Object entity, Object id, List<RelationHolder> rlHolders)
    {
        Map<String, List<DBObject>> collections = new HashMap<String, List<DBObject>>();
        collections = onPersist(collections, entity, id, entityMetadata, rlHolders, isUpdate);
        onFlushCollection(collections);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.persistence.api.Batcher#addBatch(com.impetus.kundera
     * .graph.Node)
     */
    public void addBatch(Node node)
    {
        if (node != null)
        {
            nodes.add(node);
        }

        onBatchLimit();
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
            nodes = null;
            nodes = new ArrayList<Node>();
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.persistence.api.Batcher#executeBatch()
     */
    @Override
    public int executeBatch()
    {
        Map<String, List<DBObject>> collections = new HashMap<String, List<DBObject>>();
        for (Node node : nodes)
        {
            if (node.isDirty())
            {
                // delete can not be executed in batch
                if (node.isInState(RemovedState.class))
                {
                    delete(node.getData(), node.getEntityId());
                }
                else
                {

                    List<RelationHolder> relationHolders = getRelationHolders(node);
                    EntityMetadata metadata = KunderaMetadataManager.getEntityMetadata(node.getDataClass());
                    collections = onPersist(collections, node.getData(), node.getEntityId(), metadata, relationHolders,
                            node.isUpdate());
                    indexNode(node, metadata);
                }
            }
        }

        if (!collections.isEmpty())
        {
            onFlushCollection(collections);
        }
        return collections.size();
    }

    /**
     * On collections flush.
     * 
     * @param collections
     *            collection containing records to be inserted in mongo db.
     */
    private void onFlushCollection(Map<String, List<DBObject>> collections)
    {
        for (String tableName : collections.keySet())
        {
            DBCollection dbCollection = mongoDb.getCollection(tableName);
            dbCollection.insert(collections.get(tableName).toArray(new DBObject[0]), getWriteConcern(), encoder);
        }
    }

    /**
     * Executes on list of entities to be persisted.
     * 
     * @param collections
     *            collection containing list of db objects.
     * @param entity
     *            entity in question.
     * @param id
     *            entity id.
     * @param metadata
     *            entity metadata
     * @param relationHolders
     *            relation holders.
     * @param isUpdate
     *            if it is an update
     * @return collection of DB objects.
     */
    private Map<String, List<DBObject>> onPersist(Map<String, List<DBObject>> collections, Object entity, Object id,
            EntityMetadata metadata, List<RelationHolder> relationHolders, boolean isUpdate)
    {
        persistenceUnit = metadata.getPersistenceUnit();
        String documentName = metadata.getTableName();
        DBObject document = null;
        document = new BasicDBObject();

        document = handler.getDocumentFromEntity(document, metadata, entity, relationHolders);

        if (isUpdate)
        {
            BasicDBObject query = new BasicDBObject();

            MetamodelImpl metaModel = (MetamodelImpl) KunderaMetadata.INSTANCE.getApplicationMetadata().getMetamodel(
                    metadata.getPersistenceUnit());

            if (metaModel.isEmbeddable(metadata.getIdAttribute().getBindableJavaType()))
            {
                MongoDBUtils.populateCompoundKey(query, metadata, metaModel, id);
            }
            else
            {
                query.put("_id", MongoDBUtils.populateValue(id, id.getClass()));
            }
            DBCollection dbCollection = mongoDb.getCollection(documentName);
            dbCollection.findAndModify(query, document);
        }
        else
        {
            // a db collection can have multiple records..
            // and we can have a collection of records as well.
            List<DBObject> dbStatements = null;
            if (collections.containsKey(documentName))
            {
                dbStatements = collections.get(documentName);
                dbStatements.add(document);
            }
            else
            {
                dbStatements = new ArrayList<DBObject>();
                dbStatements.add(document);
                collections.put(documentName, dbStatements);

            }
        }

        return collections;
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

    @Override
    public void populateClientProperties(Client client, Map<String, Object> properties)
    {
        new MongoDBClientProperties().populateClientProperties(client, properties);
    }

    /**
     * @param mongoDb
     *            the mongoDb to set
     */
    public void setMongoDb(DB mongoDb)
    {
        this.mongoDb = mongoDb;
    }

    /**
     * @param handler
     *            the handler to set
     */
    public void setHandler(MongoDBDataHandler handler)
    {
        this.handler = handler;
    }

    /**
     * @param log
     *            the log to set
     */
    public static void setLog(Log log)
    {
        MongoDBClient.log = log;
    }

    /**
     * @param nodes
     *            the nodes to set
     */
    public void setNodes(List<Node> nodes)
    {
        this.nodes = nodes;
    }

    /**
     * @param writeConcern
     *            the writeConcern to set
     */
    public void setWriteConcern(WriteConcern writeConcern)
    {
        this.writeConcern = writeConcern;
    }

    /**
     * @param encoder
     *            the encoder to set
     */
    public void setEncoder(DBEncoder encoder)
    {
        this.encoder = encoder;
    }

    /**
     * @return the encoder
     */
    public DBEncoder getEncoder()
    {
        return encoder;
    }

    /**
     * @return the writeConcern
     */
    public WriteConcern getWriteConcern()
    {
        if (writeConcern == null)
        {
            return mongoDb.getWriteConcern();
        }
        return writeConcern;
    }

    /**
     * @param batchSize
     *            the batchSize to set
     */
    public void setBatchSize(int batchSize)
    {
        this.batchSize = batchSize;
    }

    /**
     * @param persistenceUnit
     * @param puProperties
     */
    private void populateBatchSize(String persistenceUnit, Map<String, Object> puProperties)
    {
        String batch_Size = puProperties != null ? (String) puProperties.get(PersistenceProperties.KUNDERA_BATCH_SIZE)
                : null;
        if (batch_Size != null)
        {
            batchSize = Integer.valueOf(batch_Size);
            if (batchSize == 0)
            {
                throw new IllegalArgumentException("kundera.batch.size property must be numeric and > 0");
            }
        }
        else
        {
            PersistenceUnitMetadata puMetadata = KunderaMetadataManager.getPersistenceUnitMetadata(persistenceUnit);
            batchSize = puMetadata.getBatchSize();
        }
    }

    @Override
    public Object generate()
    {
        // return auto generated id used by mongodb.
        return new ObjectId();
    }

    /**
     * Method to execute mongo jscripts.
     * 
     * @param script
     *            jscript in string format
     * 
     * @return result object.
     */
    public Object executeScript(String script)
    {
        return mongoDb.eval(script);
    }
}
