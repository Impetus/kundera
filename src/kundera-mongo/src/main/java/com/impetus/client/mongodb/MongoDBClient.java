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

import javax.persistence.metamodel.EntityType;

import org.apache.commons.lang.NotImplementedException;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.mongodb.query.MongoDBQuery;
import com.impetus.client.mongodb.utils.MongoDBUtils;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.ClientBase;
import com.impetus.kundera.client.ClientPropertiesSetter;
import com.impetus.kundera.client.EnhanceEntity;
import com.impetus.kundera.db.RelationHolder;
import com.impetus.kundera.generator.AutoGenerator;
import com.impetus.kundera.graph.Node;
import com.impetus.kundera.index.IndexManager;
import com.impetus.kundera.lifecycle.states.RemovedState;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.ClientMetadata;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;
import com.impetus.kundera.metadata.model.annotation.DefaultEntityAnnotationProcessor;
import com.impetus.kundera.metadata.model.type.AbstractManagedType;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.api.Batcher;
import com.impetus.kundera.persistence.context.jointable.JoinTableData;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.utils.KunderaCoreUtils;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBEncoder;
import com.mongodb.DBObject;
import com.mongodb.DefaultDBEncoder;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;
import com.mongodb.util.JSON;
import com.mongodb.util.JSONParseException;

/**
 * Client class for MongoDB database.
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
    private DefaultMongoDBDataHandler handler;

    /** The log. */
    private static Logger log = LoggerFactory.getLogger(MongoDBClient.class);

    private List<Node> nodes = new ArrayList<Node>();

    private int batchSize;

    private WriteConcern writeConcern = null;

    private DBEncoder encoder = DefaultDBEncoder.FACTORY.create();

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
            Map<String, Object> externalProperties, ClientMetadata clientMetadata, final KunderaMetadata kunderaMetadata)
    {
        // TODO: This could be a constantly called, see how connection pooling
        // is
        // relevant here
        super(kunderaMetadata, externalProperties, persistenceUnit);
        this.mongoDb = (DB) mongo;
        this.indexManager = mgr;
        this.reader = reader;
        handler = new DefaultMongoDBDataHandler();
        this.clientMetadata = clientMetadata;
        populateBatchSize(persistenceUnit, this.externalProperties);
    }

    @Override
    public void persistJoinTable(JoinTableData joinTableData)
    {
        String joinTableName = joinTableData.getJoinTableName();
        String joinColumnName = joinTableData.getJoinColumnName();
        String invJoinColumnName = joinTableData.getInverseJoinColumnName();
        Map<Object, Set<Object>> joinTableRecords = joinTableData.getJoinTableRecords();

        DBCollection dbCollection = mongoDb.getCollection(joinTableName);
        KunderaCoreUtils.printQuery("Persist join table:" + joinTableName, showQuery);
        List<DBObject> documents = new ArrayList<DBObject>();

        for (Object key : joinTableRecords.keySet())
        {
            Set<Object> values = joinTableRecords.get(key);
            Object joinColumnValue = key;

            for (Object childId : values)
            {
                DBObject dbObj = new BasicDBObject();
                dbObj.put("_id", joinColumnValue.toString() + childId);
                dbObj.put(joinColumnName, MongoDBUtils.populateValue(joinColumnValue, joinColumnValue.getClass()));
                dbObj.put(invJoinColumnName, MongoDBUtils.populateValue(childId, childId.getClass()));
                documents.add(dbObj);
                KunderaCoreUtils.printQuery("id:" + joinColumnValue.toString() + childId + "   " + joinColumnName + ":"
                        + joinColumnValue + "   " + invJoinColumnName + ":" + childId, showQuery);
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
        KunderaCoreUtils.printQuery("Find by Id:" + query, showQuery);
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
        EntityMetadata metadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, entityClazz);

        List<Object> primaryKeys = new ArrayList<Object>();

        DBCollection dbCollection = mongoDb.getCollection(tableName);
        BasicDBObject query = new BasicDBObject();

        query.put(columnName, MongoDBUtils.populateValue(columnValue, columnValue.getClass()));

        DBCursor cursor = dbCollection.find(query);
        KunderaCoreUtils.printQuery("Find id by column:" + query, showQuery);
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
        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, entityClass);

        List<String> relationNames = entityMetadata.getRelationNames();

        BasicDBObject query = new BasicDBObject();

        MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                entityMetadata.getPersistenceUnit());

        AbstractManagedType managedType = (AbstractManagedType) metaModel.entity(entityMetadata.getEntityClazz());

        if (metaModel.isEmbeddable(entityMetadata.getIdAttribute().getBindableJavaType()))
        {
            MongoDBUtils.populateCompoundKey(query, entityMetadata, metaModel, key);
        }
        else
        {
            query.put("_id", MongoDBUtils.populateValue(key, key.getClass()));
        }

        // For secondary tables.
        List<String> secondaryTables = ((DefaultEntityAnnotationProcessor) managedType.getEntityAnnotation())
                .getSecondaryTablesName();
        secondaryTables.add(entityMetadata.getTableName());

        Object enhancedEntity = null;
        Map<String, Object> relationValue = null;
        // Here you need to fetch by sub managed type.

        EntityType entityType = metaModel.entity(entityMetadata.getEntityClazz());

        for (String tableName : secondaryTables)
        {
            DBCollection dbCollection = mongoDb.getCollection(tableName);
            KunderaCoreUtils.printQuery("Find document:" + query, showQuery);
            DBObject fetchedDocument = dbCollection.findOne(query);

            if (fetchedDocument != null)
            {
                List<AbstractManagedType> subManagedType = ((AbstractManagedType) entityType).getSubManagedType();

                EntityMetadata subEntityMetadata = null;
                if (!subManagedType.isEmpty())
                {
                    for (AbstractManagedType subEntity : subManagedType)
                    {
                        String discColumn = subEntity.getDiscriminatorColumn();
                        String disColValue = subEntity.getDiscriminatorValue();
                        Object value = fetchedDocument.get(discColumn);
                        if (value != null && value.toString().equals(disColValue))
                        {
                            subEntityMetadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata,
                                    subEntity.getJavaType());
                            break;
                        }
                    }

                    enhancedEntity = instantiateEntity(subEntityMetadata.getEntityClazz(), enhancedEntity);
                    relationValue = handler.getEntityFromDocument(subEntityMetadata.getEntityClazz(), enhancedEntity,
                            subEntityMetadata, fetchedDocument, subEntityMetadata.getRelationNames(), relationValue,
                            kunderaMetadata);
                }
                else
                {
                    enhancedEntity = instantiateEntity(entityClass, enhancedEntity);
                    relationValue = handler.getEntityFromDocument(entityMetadata.getEntityClazz(), enhancedEntity,
                            entityMetadata, fetchedDocument, relationNames, relationValue, kunderaMetadata);
                }

            }
        }

        if (relationValue != null && !relationValue.isEmpty())
        {
            EnhanceEntity entity = new EnhanceEntity(enhancedEntity, PropertyAccessorHelper.getId(enhancedEntity,
                    entityMetadata), relationValue);
            return entity;
        }
        else
        {
            return enhancedEntity;
        }
    }

    private Object instantiateEntity(Class entityClass, Object entity)
    {
        try
        {
            if (entity == null)
            {
                return entityClass.newInstance();
            }
            return entity;
        }
        catch (InstantiationException e)
        {
            log.error("Error while instantiating " + entityClass + ", Caused by: ", e);
        }
        catch (IllegalAccessException e)
        {
            log.error("Error while instantiating " + entityClass + ", Caused by: ", e);
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
        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, entityClass);

        log.debug("Fetching data from " + entityMetadata.getTableName() + " for Keys " + keys);

        DBCollection dbCollection = mongoDb.getCollection(entityMetadata.getTableName());

        BasicDBObject query = new BasicDBObject();

        query.put("_id", new BasicDBObject("$in", keys));

        DBCursor cursor = dbCollection.find(query);
        KunderaCoreUtils.printQuery("Find collection:" + query, showQuery);
        List entities = new ArrayList<E>();
        while (cursor.hasNext())
        {
            DBObject fetchedDocument = cursor.next();

            populateEntity(entityMetadata, entities, fetchedDocument);
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
            BasicDBObject orderBy, int maxResult, int firstResult, BasicDBObject keys, String... results)
            throws Exception
    {
        String documentName = entityMetadata.getTableName();
        Class clazz = entityMetadata.getEntityClazz();

        List entities = new ArrayList<E>();

        boolean isCountQuery = false;
        if (results != null && results.length > 1)
        {
            if (results[0].toLowerCase().indexOf("count(") == 0)
            {
                isCountQuery = true;
            }
        }

        Object object = getDBCursorInstance(mongoQuery, orderBy, maxResult, firstResult, keys, documentName,
                isCountQuery);

        DBCursor cursor = null;

        if (object instanceof Long)
        {
            List<Long> lst = new ArrayList<Long>();
            lst.add((Long) object);
            return (List<E>) lst;
        }
        else
        {
            cursor = (DBCursor) object;
        }

        if (results != null && results.length > 0)
        {
            DBCollection dbCollection = mongoDb.getCollection(documentName);
            KunderaCoreUtils.printQuery("Find document: " + mongoQuery, showQuery);
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
                            mongoQuery, result, orderBy, maxResult, firstResult, keys, kunderaMetadata));
                    return entities;
                }
            }
        }
        log.debug("Fetching data from " + documentName + " for Filter " + mongoQuery.toString());

        while (cursor.hasNext())
        {
            DBObject fetchedDocument = cursor.next();

            populateEntity(entityMetadata, entities, fetchedDocument);
        }
        return entities;
    }

    public Object getDBCursorInstance(BasicDBObject mongoQuery, BasicDBObject orderBy, int maxResult, int firstResult,
            BasicDBObject keys, String documentName, boolean isCountQuery)
    {
        DBCollection dbCollection = mongoDb.getCollection(documentName);

        DBCursor cursor = null;
        if (isCountQuery)
            return dbCollection.count(mongoQuery);
        else
            cursor = orderBy != null ? dbCollection.find(mongoQuery, keys).sort(orderBy).limit(maxResult)
                    .skip(firstResult) : dbCollection.find(mongoQuery, keys).limit(maxResult).skip(firstResult);
        return cursor;
    }

    /*
     * (non-Javadoc) object
     * 
     * @see com.impetus.kundera.client.Client#delete(java.lang.Object,
     * java.lang.Object)
     */
    @Override
    public void delete(Object entity, Object pKey)
    {
        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, entity.getClass());

        // Find the DBObject to remove first
        BasicDBObject query = new BasicDBObject();

        MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                entityMetadata.getPersistenceUnit());

        if (metaModel.isEmbeddable(entityMetadata.getIdAttribute().getBindableJavaType()))
        {
            MongoDBUtils.populateCompoundKey(query, entityMetadata, metaModel, pKey);
        }
        else
        {

            query.put("_id", MongoDBUtils.populateValue(pKey, pKey.getClass()));
        }

        AbstractManagedType managedType = (AbstractManagedType) metaModel.entity(entityMetadata.getEntityClazz());
        // For secondary tables.
        List<String> secondaryTables = ((DefaultEntityAnnotationProcessor) managedType.getEntityAnnotation())
                .getSecondaryTablesName();
        secondaryTables.add(entityMetadata.getTableName());

        for (String collectionName : secondaryTables)
        {
            KunderaCoreUtils.printQuery("Drop existing collection:" + query, showQuery);
            DBCollection dbCollection = mongoDb.getCollection(collectionName);
            dbCollection.remove(query, getWriteConcern(), encoder);
        }

        getIndexManager().remove(entityMetadata, entity, pKey);
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
        externalProperties = null;
        clear();
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
                KunderaCoreUtils.printQuery("Create index on:" + columnName, showQuery);
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
        EntityMetadata m = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, entityClazz);
        // you got column name and column value.
        DBCollection dbCollection = mongoDb.getCollection(m.getTableName());

        BasicDBObject query = new BasicDBObject();

        query.put(colName, MongoDBUtils.populateValue(colValue, colValue.getClass()));
        KunderaCoreUtils.printQuery("Find by relation:" + query, showQuery);
        DBCursor cursor = dbCollection.find(query);
        DBObject fetchedDocument = null;
        List<Object> results = new ArrayList<Object>();
        while (cursor.hasNext())
        {
            fetchedDocument = cursor.next();
            populateEntity(m, results, fetchedDocument);
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
        KunderaCoreUtils.printQuery("Delete column:" + query, showQuery);
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
                node.handlePreEvent();
                // delete can not be executed in batch
                if (node.isInState(RemovedState.class))
                {
                    delete(node.getData(), node.getEntityId());
                }
                else
                {
                    List<RelationHolder> relationHolders = getRelationHolders(node);
                    EntityMetadata metadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata,
                            node.getDataClass());
                    collections = onPersist(collections, node.getData(), node.getEntityId(), metadata, relationHolders,
                            node.isUpdate());
                    indexNode(node, metadata);
                }
                node.handlePostEvent();
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
            KunderaCoreUtils.printQuery("Persist collection:" + tableName, showQuery);
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
        // String documentName = metadata.getTableName();
        Map<String, DBObject> documents = handler.getDocumentFromEntity(metadata, entity, relationHolders,
                kunderaMetadata);
        if (isUpdate)
        {
            for (String documentName : documents.keySet())
            {
                BasicDBObject query = new BasicDBObject();

                MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
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
                KunderaCoreUtils.printQuery("Persist collection:" + documentName, showQuery);
                DBObject obj = dbCollection.findOne(query);
                if (obj != null)
                {
                    obj.putAll(documents.get(documentName));

                    dbCollection.save(obj);
                }
                else
                {
                    dbCollection.save(documents.get(documentName));
                }
            }
        }
        else
        {
            for (String documentName : documents.keySet())
            {
                // a db collection can have multiple records..
                // and we can have a collection of records as well.
                List<DBObject> dbStatements = null;
                if (collections.containsKey(documentName))
                {
                    dbStatements = collections.get(documentName);
                    dbStatements.add(documents.get(documentName));
                }
                else
                {
                    dbStatements = new ArrayList<DBObject>();
                    dbStatements.add(documents.get(documentName));
                    collections.put(documentName, dbStatements);
                }
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
    public void setHandler(DefaultMongoDBDataHandler handler)
    {
        this.handler = handler;
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
            PersistenceUnitMetadata puMetadata = KunderaMetadataManager.getPersistenceUnitMetadata(kunderaMetadata,
                    persistenceUnit);
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
    @Override
    public Object executeScript(String script)
    {
        Object result = mongoDb.eval(script);
        KunderaCoreUtils.printQuery("Execute mongo jscripts:" + script, showQuery);
        return result;
    }

    /**
     * @param jsonClause
     * @param entityMetadata
     * @return
     */
    public List executeQuery(String jsonClause, EntityMetadata entityMetadata)
    {
        List entities = new ArrayList();
        try
        {
            DBCursor cursor = parseAndScroll(jsonClause, entityMetadata.getTableName());

            while (cursor.hasNext())
            {
                DBObject fetchedDocument = cursor.next();

                populateEntity(entityMetadata, entities, fetchedDocument);
            }
            return entities;

        }
        catch (JSONParseException jex)
        {
            entities = executeNativeQuery(jsonClause, entityMetadata);
            List result = new ArrayList();
            if (entities.get(0) instanceof EnhanceEntity)
            {
                for (Object obj : entities)
                {
                    result.add(((EnhanceEntity) obj).getEntity());
                }
                return result;
            }
            return entities;
        }
    }

    public List executeNativeQuery(String jsonClause, EntityMetadata entityMetadata)
    {
        List entities = new ArrayList();
        String[] tempArray = jsonClause.split("\\.");
        String tempClause = tempArray[tempArray.length - 1];

        if (tempClause.contains("findOne(") || tempClause.contains("findAndModify("))
        {
            DBObject obj = (BasicDBObject) executeScript(jsonClause);
            populateEntity(entityMetadata, entities, obj);
            return entities;

        }
        else if (tempClause.contains("find(") || jsonClause.contains("aggregate("))
        {
            jsonClause = jsonClause.concat(".toArray()");
            BasicDBList list = (BasicDBList) executeScript(jsonClause);
            for (Object obj : list)
            {
                populateEntity(entityMetadata, entities, (DBObject) obj);
            }
            return entities;

        }
        else if (tempClause.contains("count(") || tempClause.contains("dataSize(")
                || tempClause.contains("storageSize(") || tempClause.contains("totalIndexSize(")
                || tempClause.contains("totalSize("))
        {
            Long count = ((Double) executeScript(jsonClause)).longValue();
            entities.add(count);
            return entities;

        }
        else if (tempClause.contains("distinct("))
        {
            BasicDBList list = (BasicDBList) executeScript(jsonClause);
            for (Object obj : list)
            {
                entities.add(obj);
            }
            return entities;

        }
        else
        {
            BasicDBList list = (BasicDBList) executeScript(jsonClause);
            for (Object obj : list)
            {
                entities.add(obj);
            }
            return entities;
        }
    }

    /**
     * @param jsonClause
     * @param collectionName
     * @return
     * @throws JSONParseException
     */
    private DBCursor parseAndScroll(String jsonClause, String collectionName) throws JSONParseException
    {
        BasicDBObject clause = (BasicDBObject) JSON.parse(jsonClause);
        DBCursor cursor = mongoDb.getCollection(collectionName).find(clause);
        return cursor;
    }

    /**
     * @param entityMetadata
     * @param entities
     * @param fetchedDocument
     */
    private void populateEntity(EntityMetadata entityMetadata, List entities, DBObject fetchedDocument)
    {
        Map<String, Object> relationValue = null;
        if (fetchedDocument != null)
        {
            MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                    entityMetadata.getPersistenceUnit());

            EntityType entityType = metaModel.entity(entityMetadata.getEntityClazz());

            List<AbstractManagedType> subManagedType = ((AbstractManagedType) entityType).getSubManagedType();

            EntityMetadata subEntityMetadata = null;
            Object enhancedEntity = null;
            if (!subManagedType.isEmpty())
            {
                for (AbstractManagedType subEntity : subManagedType)
                {
                    String discColumn = subEntity.getDiscriminatorColumn();
                    String disColValue = subEntity.getDiscriminatorValue();
                    Object value = fetchedDocument.get(discColumn);
                    if (value != null && value.toString().equals(disColValue))
                    {
                        subEntityMetadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata,
                                subEntity.getJavaType());
                        break;
                    }
                }
                enhancedEntity = instantiateEntity(subEntityMetadata.getEntityClazz(), enhancedEntity);
                relationValue = handler.getEntityFromDocument(subEntityMetadata.getEntityClazz(), enhancedEntity,
                        subEntityMetadata, fetchedDocument, subEntityMetadata.getRelationNames(), relationValue,
                        kunderaMetadata);

            }
            else
            {
                enhancedEntity = instantiateEntity(entityMetadata.getEntityClazz(), enhancedEntity);
                relationValue = handler.getEntityFromDocument(entityMetadata.getEntityClazz(), enhancedEntity,
                        entityMetadata, fetchedDocument, entityMetadata.getRelationNames(), relationValue,
                        kunderaMetadata);
            }

            if (relationValue != null && !relationValue.isEmpty())
            {
                enhancedEntity = new EnhanceEntity(enhancedEntity, PropertyAccessorHelper.getId(enhancedEntity,
                        entityMetadata), relationValue);
            }

            if (enhancedEntity != null)
            {
                entities.add(enhancedEntity);
            }
        }
    }

    public int handleUpdateFunctions(BasicDBObject query, BasicDBObject update, String collName)
    {
        DBCollection collection = mongoDb.getCollection(collName);
        KunderaCoreUtils.printQuery("Update collection:" + query, showQuery);
        WriteResult result = collection.update(query, update);
        if (result.getError() != null || result.getN() <= 0)
            return -1;
        return result.getN();
    }

}