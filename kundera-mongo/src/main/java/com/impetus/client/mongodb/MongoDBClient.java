/*******************************************************************************
 * * Copyright 2011 Impetus Infotech.
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
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import javax.persistence.PersistenceException;
import javax.persistence.Query;

import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.client.mongodb.query.MongoDBQuery;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.db.RelationHolder;
import com.impetus.kundera.index.IndexManager;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.persistence.handler.impl.EntitySaveGraph;
import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.proxy.EnhancedEntity;
import com.impetus.kundera.query.KunderaQuery;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

/**
 * CLient class for MongoDB database.
 * 
 * @author impetusopensource
 */
public class MongoDBClient implements Client
{

    /** The is connected. */
    // private boolean isConnected;

    /** The mongo db. */
    private DB mongoDb;

    /** The data handler. */
    // private MongoDBDataHandler dataHandler;

    /** The index manager. */
    private IndexManager indexManager;

    private String persistenceUnit;

    /** The log. */
    private static Log log = LogFactory.getLog(MongoDBClient.class);

    public MongoDBClient(Object mongo, IndexManager mgr)
    {
        // TODO: This could be a constly call, see how connection pooling is
        // relevant here
        this.mongoDb = (DB) mongo;
        this.indexManager = mgr;

        // this.dataHandler = new MongoDBDataHandler(this);

    }

    @Override
    @Deprecated
    public void persist(EnhancedEntity enhancedEntity) throws Exception
    {
        throw new PersistenceException("Not Implemented");
    }

    @Override
    public String persist(EntitySaveGraph entityGraph, EntityMetadata entityMetadata)
    {
        Object entity = entityGraph.getParentEntity();
        String id = entityGraph.getParentId();

        try
        {
            onPersist(entityMetadata, entity, id, RelationHolder.addRelation(entityGraph, entityGraph.getRevFKeyName(),
                    entityGraph.getRevFKeyValue()));

            if (entityGraph.getRevParentClass() != null)
            {
                getIndexManager().write(entityMetadata, entity, entityGraph.getRevFKeyValue(),
                        entityGraph.getRevParentClass());
            }
            else
            {
                getIndexManager().write(entityMetadata, entity);
            }
        }
        catch (PropertyAccessException e)
        {
            log.error(e.getMessage());
            throw new PersistenceException(e.getMessage());
        }
        catch (Exception e)
        {
            log.error(e.getMessage());
            throw new PersistenceException(e.getMessage());
        }

        return null;
    }

    @Override
    public void persist(Object childEntity, EntitySaveGraph entitySaveGraph, EntityMetadata metadata)
    {
        String rlName = entitySaveGraph.getfKeyName();
        String rlValue = entitySaveGraph.getParentId();
        String id = entitySaveGraph.getChildId();

        try
        {
            onPersist(metadata, childEntity, id, RelationHolder.addRelation(entitySaveGraph, rlName, rlValue));
            onIndex(childEntity, entitySaveGraph, metadata, rlValue);
        }
        catch (PropertyAccessException e)
        {
            e.printStackTrace();
            log.error(e.getMessage());
            throw new PersistenceException(e.getMessage());
        }
        catch (Exception e)
        {
            log.error(e.getMessage());
            throw new PersistenceException(e.getMessage());
        }
    }

    @Override
    public void persistJoinTable(String joinTableName, String joinColumnName, String inverseJoinColumnName,
            EntityMetadata relMetadata, EntitySaveGraph objectGraph)
    {
        DBCollection dbCollection = mongoDb.getCollection(joinTableName);

        List<BasicDBObject> documents = new ArrayList<BasicDBObject>();

        String parentId = objectGraph.getParentId();
        try
        {
            if (Collection.class.isAssignableFrom(objectGraph.getChildEntity().getClass()))
            {
                Collection children = (Collection) objectGraph.getChildEntity();

                for (Object child : children)
                {

                    addColumnsToJoinTable(joinColumnName, inverseJoinColumnName, relMetadata, documents, parentId,
                            child);

                }

            }
            else
            {
                Object child = objectGraph.getChildEntity();

                addColumnsToJoinTable(joinColumnName, inverseJoinColumnName, relMetadata, documents, parentId, child);

            }
        }
        catch (PropertyAccessException e)
        {
            e.printStackTrace();
        }

        dbCollection.insert(documents.toArray(new BasicDBObject[0]));
    }

    @Override
    public <E> List<E> getForeignKeysFromJoinTable(String joinTableName, String joinColumnName,
            String inverseJoinColumnName, EntityMetadata relMetadata, EntitySaveGraph objectGraph)
    {

        String parentId = objectGraph.getParentId();
        List<E> foreignKeys = new ArrayList<E>();

        DBCollection dbCollection = mongoDb.getCollection(joinTableName);
        BasicDBObject query = new BasicDBObject();

        query.put(joinColumnName, parentId);

        DBCursor cursor = dbCollection.find(query);
        DBObject fetchedDocument = null;

        while (cursor.hasNext())
        {
            fetchedDocument = cursor.next();
            String foreignKey = (String) fetchedDocument.get(inverseJoinColumnName);
            foreignKeys.add((E) foreignKey);
        }
        return foreignKeys;
    }

    private void addColumnsToJoinTable(String joinColumnName, String inverseJoinColumnName, EntityMetadata relMetadata,
            List<BasicDBObject> documents, String parentId, Object child) throws PropertyAccessException
    {
        String childId = PropertyAccessorHelper.getId(child, relMetadata);
        BasicDBObject dbObj = new BasicDBObject();
        dbObj.put(joinColumnName, parentId);
        dbObj.put(inverseJoinColumnName, childId);

        documents.add(dbObj);
    }

    /**
     * On index.
     * 
     * @param childEntity
     *            the child entity
     * @param entitySaveGraph
     *            the entity save graph
     * @param metadata
     *            the metadata
     * @param rlValue
     *            the rl value
     */
    private void onIndex(Object childEntity, EntitySaveGraph entitySaveGraph, EntityMetadata metadata, String rlValue)
    {
        if (!entitySaveGraph.isSharedPrimaryKey())
        {
            getIndexManager().write(metadata, childEntity, rlValue, entitySaveGraph.getParentEntity().getClass());
        }
        else
        {
            getIndexManager().write(metadata, childEntity);
        }
    }

    /**
     * @param entityMetadata
     * @param id
     * @throws Exception
     * @throws PropertyAccessException
     */
    private void onPersist(EntityMetadata entityMetadata, Object entity, String id, List<RelationHolder> relations)
            throws Exception, PropertyAccessException
    {
        String dbName = entityMetadata.getSchema();
        String documentName = entityMetadata.getTableName();

        log.debug("Persisting data into " + dbName + "." + documentName + " for " + id);
        DBCollection dbCollection = mongoDb.getCollection(documentName);

        BasicDBObject document = new MongoDBDataHandler(this, getPersistenceUnit()).getDocumentFromEntity(
                entityMetadata, entity, relations);
        dbCollection.insert(document);

    }

    /*
     * (non-Javadoc)
     * 
     * @seecom.impetus.kundera.Client#loadColumns(com.impetus.kundera.ejb.
     * EntityManager, java.lang.Class, java.lang.String, java.lang.String,
     * java.lang.String, com.impetus.kundera.metadata.EntityMetadata)
     */
    @Override
    public <E> E find(Class<E> entityClass, String key, List<String> relationNames) throws Exception
    {
        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(getPersistenceUnit(), entityClass);

        log.debug("Fetching data from " + entityMetadata.getTableName() + " for PK " + key);

        DBCollection dbCollection = mongoDb.getCollection(entityMetadata.getTableName());

        BasicDBObject query = new BasicDBObject();
        query.put(entityMetadata.getIdColumn().getName(), key);

        DBCursor cursor = dbCollection.find(query);
        DBObject fetchedDocument = null;

        if (cursor.hasNext())
        {
            fetchedDocument = cursor.next();
        }
        else
        {
            return null;
        }

        Object enhancedEntity = new MongoDBDataHandler(this, getPersistenceUnit()).getEntityFromDocument(
                entityMetadata.getEntityClazz(), entityMetadata, fetchedDocument, null);

        return (E) enhancedEntity;
    }

    @Override
    public <E> List<E> find(Class<E> entityClass, String... keys) throws Exception
    {
        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(getPersistenceUnit(), entityClass);

        log.debug("Fetching data from " + entityMetadata.getTableName() + " for Keys " + keys);

        DBCollection dbCollection = mongoDb.getCollection(entityMetadata.getTableName());

        BasicDBObject query = new BasicDBObject();
        query.put(entityMetadata.getIdColumn().getName(), new BasicDBObject("$in", keys));

        DBCursor cursor = dbCollection.find(query);

        List entities = new ArrayList<E>();
        while (cursor.hasNext())
        {
            DBObject fetchedDocument = cursor.next();
            Object entity = new MongoDBDataHandler(this, getPersistenceUnit()).getEntityFromDocument(
                    entityMetadata.getEntityClazz(), entityMetadata, fetchedDocument, null);
            entities.add(entity);
        }
        return entities;
    }

    /**
     * Loads columns from multiple rows restricting results to conditions stored
     * in <code>filterClauseQueue</code>.
     * 
     * @param m
     *            the m
     * @param filterClauseQueue
     *            the filter clause queue
     * @return the list
     * @throws Exception
     *             the exception
     */
    public <E> List<E> loadData(EntityMetadata entityMetadata, KunderaQuery query, List<String> relationNames) throws Exception
    {
//        MongoDBQuery mongoDBQuery = (MongoDBQuery) query;

        // TODO Resolve the workaround
//        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(getPersistenceUnit(), mongoDBQuery
//                .getKunderaQuery().getEntityClass());

        String documentName = entityMetadata.getTableName();
        String dbName = entityMetadata.getSchema();
        Class clazz = entityMetadata.getEntityClazz();

        DBCollection dbCollection = mongoDb.getCollection(documentName);

        Queue filterClauseQueue = query.getFilterClauseQueue();
        String result = query.getResult();

        List entities = new ArrayList<E>();

        // If User wants search on a column within a particular super column,
        // fetch that embedded object collection only
        // otherwise retrieve whole entity
        // TODO: improve code
        if (result.indexOf(".") >= 0)
        {
            //TODO i need to discuss with Amresh before modifying it.
            entities.addAll(new MongoDBDataHandler(this, getPersistenceUnit()).getEmbeddedObjectList(dbCollection,
                    entityMetadata, documentName, query));

        }
        else
        {
            log.debug("Fetching data from " + documentName + " for Filter " + filterClauseQueue);

            BasicDBObject mongoQuery = new MongoDBDataHandler(this, getPersistenceUnit()).createMongoQuery(
                    entityMetadata, filterClauseQueue);

            DBCursor cursor = dbCollection.find(mongoQuery);

            while (cursor.hasNext())
            {
                DBObject fetchedDocument = cursor.next();
                Object entity = new MongoDBDataHandler(this, getPersistenceUnit()).getEntityFromDocument(clazz,
                        entityMetadata, fetchedDocument, relationNames);
                entities.add(entity);
            }
        }

        return entities;
    }

    @Override
    public void delete(Object entity, Object pKey, EntityMetadata entityMetadata) throws Exception
    {
        DBCollection dbCollection = mongoDb.getCollection(entityMetadata.getTableName());

        // Find the DBObject to remove first
        BasicDBObject query = new BasicDBObject();
        query.put(entityMetadata.getIdColumn().getName(), pKey.toString());

        dbCollection.remove(query);
        getIndexManager().remove(entityMetadata, entity, pKey.toString());

    }

    @Override
    public void close()
    {
        // TODO Once pool is implemented this code should not be there.
        // Workaround for pool
        this.indexManager.flush();
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

    @Override
    public <E> List<E> find(Class<E> entityClass, Map<String, String> col) throws Exception
    {
        throw new NotImplementedException("Not yet implemented");
    }

    @Override
    public String getPersistenceUnit()
    {
        return persistenceUnit;
    }

    @Override
    public IndexManager getIndexManager()
    {
        return indexManager;
    }

    @Override
    public void setPersistenceUnit(String persistenceUnit)
    {
        this.persistenceUnit = persistenceUnit;

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#find(java.lang.Class,
     * com.impetus.kundera.metadata.model.EntityMetadata, java.lang.String)
     */
    @Override
    public Object find(Class<?> clazz, EntityMetadata entityMetadata, String rowId, List<String> relationNames)
    {

        log.debug("Fetching data from " + entityMetadata.getTableName() + " for PK " + rowId);

        DBCollection dbCollection = mongoDb.getCollection(entityMetadata.getTableName());

        BasicDBObject query = new BasicDBObject();
        query.put(entityMetadata.getIdColumn().getName(), rowId);

        DBCursor cursor = dbCollection.find(query);
        DBObject fetchedDocument = null;

        if (cursor.hasNext())
        {
            fetchedDocument = cursor.next();
        }
        else
        {
            return null;
        }

        Object entity = new MongoDBDataHandler(this, getPersistenceUnit()).getEntityFromDocument(
                entityMetadata.getEntityClazz(), entityMetadata, fetchedDocument, null);

        return entity;
    }

    /**
     * Method to find entity for given association name and association value.
     */
  public List<Object> find(String colName, String colValue, EntityMetadata m)
    {
       // you got column name and column value.
      DBCollection dbCollection = mongoDb.getCollection(m.getTableName());

      BasicDBObject query = new BasicDBObject();

      query.put(colName, colValue);

      DBCursor cursor = dbCollection.find(query);
      DBObject fetchedDocument = null;
      MongoDBDataHandler handler = new MongoDBDataHandler(this, getPersistenceUnit());
      List<Object> results = new ArrayList<Object>();
      while (cursor.hasNext())
      {
          fetchedDocument = cursor.next();
          Object entity = handler.getEntityFromDocument(m.getEntityClazz(), m, fetchedDocument, null);
          results.add(entity);
      }
      


      return results.isEmpty()?null:results;
    }

}