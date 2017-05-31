/*******************************************************************************
 *  * Copyright 2015 Impetus Infotech.
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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.EntityType;

import org.apache.commons.lang.NotImplementedException;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.mongodb.query.MongoDBQuery;
import com.impetus.client.mongodb.query.gfs.KunderaGridFS;
import com.impetus.client.mongodb.utils.MongoDBUtils;
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
import com.impetus.kundera.metadata.model.ClientMetadata;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;
import com.impetus.kundera.metadata.model.Relation;
import com.impetus.kundera.metadata.model.annotation.DefaultEntityAnnotationProcessor;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.metadata.model.type.AbstractManagedType;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.api.Batcher;
import com.impetus.kundera.persistence.context.jointable.JoinTableData;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.utils.KunderaCoreUtils;
import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.BulkWriteException;
import com.mongodb.BulkWriteOperation;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBEncoder;
import com.mongodb.DBObject;
import com.mongodb.DefaultDBEncoder;
import com.mongodb.MapReduceCommand;
import com.mongodb.MapReduceOutput;
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.gridfs.GridFSInputFile;
import com.mongodb.util.JSON;
import com.mongodb.util.JSONParseException;

/**
 * Client class for MongoDB database.
 * 
 * @author devender.yadav
 */
public class MongoDBClient extends ClientBase implements Client<MongoDBQuery>, Batcher, ClientPropertiesSetter

{
    /** The mongo db. */
    private DB mongoDb;

    /** The reader. */
    private EntityReader reader;

    /** The data handler. */
    private DefaultMongoDBDataHandler handler;

    /** The log. */
    private static Logger log = LoggerFactory.getLogger(MongoDBClient.class);

    /** The nodes. */
    private List<Node> nodes = new ArrayList<Node>();

    /** The batch size. */
    private int batchSize;

    /** The ordered bulk operation. */
    private boolean orderedBulkOperation;

    /**
     * Checks if is ordered bulk operation.
     * 
     * @return true, if is ordered bulk operation
     */
    public boolean isOrderedBulkOperation()
    {
        return orderedBulkOperation;
    }

    /**
     * Sets the ordered bulk operation.
     * 
     * @param orderedBulkOperation
     *            the new ordered bulk operation
     */
    public void setOrderedBulkOperation(boolean orderedBulkOperation)
    {
        this.orderedBulkOperation = orderedBulkOperation;
    }

    /** The write concern. */
    private WriteConcern writeConcern = null;

    /** The encoder. */
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
     * @param persistenceUnit
     *            the persistence unit
     * @param externalProperties
     *            the external properties
     * @param clientMetadata
     *            the client metadata
     * @param kunderaMetadata
     *            the kundera metadata
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
        Map<Object, Set<Object>> joinTableRecords = joinTableData.getJoinTableRecords();

        DBCollection dbCollection = mongoDb.getCollection(joinTableName);
        KunderaCoreUtils.printQuery("Persist join table:" + joinTableName, showQuery);

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
                KunderaCoreUtils.printQuery("id:" + joinColumnValue.toString() + childId + "   " + joinColumnName + ":"
                        + joinColumnValue + "   " + invJoinColumnName + ":" + childId, showQuery);

                dbCollection.save(dbObj, getWriteConcern());
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#getColumnsById(java.lang.String,
     * java.lang.String, java.lang.String, java.lang.String, java.lang.Object,
     * java.lang.Class)
     */
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

        MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                entityMetadata.getPersistenceUnit());

        AbstractManagedType managedType = (AbstractManagedType) metaModel.entity(entityMetadata.getEntityClazz());

        return managedType.hasLobAttribute() ? findGFSEntity(entityMetadata, entityClass, key) : find(entityClass, key,
                entityMetadata, metaModel, managedType);
    }

    /**
     * Find.
     *
     * @param entityClass
     *            the entity class
     * @param key
     *            the key
     * @param entityMetadata
     *            the entity metadata
     * @param metaModel
     *            the meta model
     * @param managedType
     *            the managed type
     * @return the object
     */
    private Object find(Class entityClass, Object key, EntityMetadata entityMetadata, MetamodelImpl metaModel,
            AbstractManagedType managedType)
    {
        List<String> relationNames = entityMetadata.getRelationNames();

        BasicDBObject query = new BasicDBObject();

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

    /**
     * Find GFS entity.
     *
     * @param entityMetadata
     *            the entity metadata
     * @param entityClass
     *            the entity class
     * @param key
     *            the key
     * @return the object
     */
    private Object findGFSEntity(EntityMetadata entityMetadata, Class entityClass, Object key)
    {
        GridFSDBFile outputFile = findGridFSDBFile(entityMetadata, key);
        return outputFile != null ? handler.getEntityFromGFSDBFile(entityMetadata.getEntityClazz(),
                instantiateEntity(entityClass, null), entityMetadata, outputFile, kunderaMetadata) : null;
    }

    /**
     * Find GRIDFSDBFile.
     *
     * @param entityMetadata
     *            the entity metadata
     * @param key
     *            the key
     * @return the grid fsdb file
     */
    private GridFSDBFile findGridFSDBFile(EntityMetadata entityMetadata, Object key)
    {
        String id = ((AbstractAttribute) entityMetadata.getIdAttribute()).getJPAColumnName();
        DBObject query = new BasicDBObject("metadata." + id, key);
        KunderaGridFS gfs = new KunderaGridFS(mongoDb, entityMetadata.getTableName());
        return gfs.findOne(query);
    }

    /**
     * Instantiate entity.
     *
     * @param entityClass
     *            the entity class
     * @param entity
     *            the entity
     * @return the object
     */
    private Object instantiateEntity(Class entityClass, Object entity)
    {
        if (entity == null)
        {
            return KunderaCoreUtils.createNewInstance(entityClass);
        }
        return entity;
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

    public <E> List<E> aggregate(EntityMetadata entityMetadata, BasicDBObject mongoQuery, BasicDBList lookup,
            BasicDBObject aggregation, BasicDBObject orderBy, int maxResult) throws Exception
    {
        String collectionName = entityMetadata.getTableName();

        MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                entityMetadata.getPersistenceUnit());
        AbstractManagedType managedType = (AbstractManagedType) metaModel.entity(entityMetadata.getEntityClazz());
        boolean hasLob = managedType.hasLobAttribute();

        List<DBObject> pipeline = new LinkedList<DBObject>();
        addLookupAndMatchToPipeline(lookup, mongoQuery, pipeline);
        if (aggregation != null)
        {
            pipeline.add(new BasicDBObject("$group", aggregation));
        }
        if (orderBy != null && aggregation != null)
        {
            addSortToPipeline(orderBy, aggregation, hasLob, pipeline);
        }
        if (maxResult > 0)
        {
            pipeline.add(new BasicDBObject("$limit", maxResult));
        }

        Iterable<DBObject> aggregationResults;

        if (hasLob)
        {
            // KunderaGridFS gridFS = new KunderaGridFS(mongoDb,
            // collectionName);
            // AggregationOutput output = gridFS.aggregate(pipeline);
            // aggregationResults = output.results();
            throw new KunderaException("Aggregation not supported for MongoDB with GridFS.");

        }
        else
        {
            AggregationOutput output = mongoDb.getCollection(collectionName).aggregate(pipeline);
            aggregationResults = output.results();
        }

        return (List<E>) extractAggregationValues(aggregationResults, aggregation);
    }

    private void addLookupAndMatchToPipeline(BasicDBList lookup, BasicDBObject mongoQuery, List<DBObject> pipeline)
    {
        BasicDBObject matchBeforeLookup = new BasicDBObject();
        BasicDBObject matchAfterLookup = new BasicDBObject();

        for (String key : mongoQuery.keySet())
        {
            if (key.contains("."))
            {
                matchAfterLookup.append(key, mongoQuery.get(key));
            }
            else
            {
                matchBeforeLookup.append(key, mongoQuery.get(key));
            }
        }

        if (matchBeforeLookup.size() > 0)
        {
            pipeline.add(new BasicDBObject("$match", matchBeforeLookup));
        }
        for (Object lookupItem : lookup)
        {
            pipeline.add((DBObject) lookupItem);
        }
        if (matchAfterLookup.size() > 0)
        {
            pipeline.add(new BasicDBObject("$match", matchAfterLookup));
        }
    }

    private void addSortToPipeline(BasicDBObject orderBy, BasicDBObject aggregation, boolean hasLob,
            List<DBObject> pipeline)
    {
        BasicDBObject actual = new BasicDBObject();
        for (String key : orderBy.keySet())
        {
            if (aggregation.containsField(key))
            {
                actual.put(key, orderBy.get(key));
            }
            else if (aggregation.containsField("_id"))
            {
                if (((BasicDBObject) aggregation.get("_id")).containsField(key))
                {
                    actual.put("_id", orderBy.get(key));
                }
                else if (hasLob && key.startsWith("metadata."))
                {
                    // check the key without the "metadata." prefix for GridFS
                    if (((BasicDBObject) aggregation.get("_id")).containsField(key.substring(9)))
                    {
                        actual.put("_id", orderBy.get(key));
                    }
                }
            }
        }

        if (actual.size() > 0)
        {
            pipeline.add(new BasicDBObject("$sort", actual));
        }
    }

    private List extractAggregationValues(Iterable<DBObject> documents, BasicDBObject aggregation)
    {
        List results = new LinkedList();

        if (aggregation != null)
        {
            if (aggregation.containsField("_id") && aggregation.get("_id") == null)
            {
                aggregation.removeField("_id");
            }
        }

        for (DBObject document : documents)
        {
            if (document.containsField("_id") && document.get("_id") == null)
            {
                document.removeField("_id");
            }

            extractAggregationValues(document, results, aggregation != null ? aggregation : (BasicDBObject) document);
        }

        return results;
    }

    private void extractAggregationValues(DBObject document, List results, DBObject keyMap)
    {
        if (document.keySet().size() == 1)
        {
            String key = document.keySet().iterator().next();
            Object value = document.get(key);

            // special case for count
            if (key.equals("count"))
            {
                value = Long.parseLong(value.toString());
            }

            if (value instanceof DBObject)
            {
                extractAggregationValues((DBObject) value, results, (DBObject) keyMap.get(key));
            }
            else
            {
                results.add(value);
            }
        }
        else if (document.keySet().size() > 1)
        {
            List<Object> values = new ArrayList<Object>(document.keySet().size());
            for (String key : keyMap.keySet())
            {
                Object value = document.get(key);

                // special case for count
                if (key.equals("count"))
                {
                    value = Long.parseLong(value.toString());
                }

                if (value instanceof DBObject)
                {
                    extractAggregationValues((DBObject) value, values, (DBObject) keyMap.get(key));
                }
                else
                {
                    values.add(value);
                }
            }
            results.add(values.toArray());
        }
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
     * @param relationNames
     *            the relation names
     * @param orderBy
     *            the order by
     * @param maxResult
     *            the max result
     * @param firstResult
     *            the first result
     * @param isCountQuery
     *            the is count query
     * @param keys
     *            the keys
     * @param results
     *            the results
     * @return the list
     * @throws Exception
     *             the exception
     */
    public <E> List<E> loadData(EntityMetadata entityMetadata, BasicDBObject mongoQuery, List<String> relationNames,
            BasicDBObject orderBy, int maxResult, int firstResult, boolean isCountQuery, BasicDBObject keys,
            String... results) throws Exception
    {
        MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                entityMetadata.getPersistenceUnit());
        AbstractManagedType managedType = (AbstractManagedType) metaModel.entity(entityMetadata.getEntityClazz());
        boolean hasLob = managedType.hasLobAttribute();
        return (List<E>) (!hasLob ? loadQueryData(entityMetadata, mongoQuery, orderBy, maxResult, firstResult,
                isCountQuery, keys, results) : loadQueryDataGFS(entityMetadata, mongoQuery, orderBy, maxResult,
                firstResult, isCountQuery));
    }

    /**
     * Load query data gfs.
     * 
     * @param <E>
     *            the element type
     * @param entityMetadata
     *            the entity metadata
     * @param mongoQuery
     *            the mongo query
     * @param orderBy
     *            the order by
     * @param maxResult
     *            the max result
     * @param firstResult
     *            the first result
     * @param isCountQuery
     *            the is count query
     * @return the list
     */
    private <E> List<E> loadQueryDataGFS(EntityMetadata entityMetadata, BasicDBObject mongoQuery,
            BasicDBObject orderBy, int maxResult, int firstResult, boolean isCountQuery)
    {
        List<GridFSDBFile> gfsDBfiles = getGFSDBFiles(mongoQuery, orderBy, entityMetadata.getTableName(), maxResult,
                firstResult);

        if (isCountQuery)
        {
            return (List<E>) Collections.singletonList(gfsDBfiles.size());
        }

        List entities = new ArrayList<E>();
        for (GridFSDBFile file : gfsDBfiles)
        {
            populateGFSEntity(entityMetadata, entities, file);
        }
        return entities;
    }

    /**
     * Load query data.
     * 
     * @param <E>
     *            the element type
     * @param entityMetadata
     *            the entity metadata
     * @param mongoQuery
     *            the mongo query
     * @param orderBy
     *            the order by
     * @param maxResult
     *            the max result
     * @param firstResult
     *            the first result
     * @param isCountQuery
     *            the is count query
     * @param keys
     *            the keys
     * @param results
     *            the results
     * @return the list
     * @throws InstantiationException
     *             the instantiation exception
     * @throws IllegalAccessException
     *             the illegal access exception
     */
    private <E> List<E> loadQueryData(EntityMetadata entityMetadata, BasicDBObject mongoQuery, BasicDBObject orderBy,
            int maxResult, int firstResult, boolean isCountQuery, BasicDBObject keys, String... results)
            throws InstantiationException, IllegalAccessException
    {
        String documentName = entityMetadata.getTableName();

        List entities = new ArrayList<E>();

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

    /**
     * Populate gfs entity.
     * 
     * @param entityMetadata
     *            the entity metadata
     * @param entities
     *            the entities
     * @param gfsDBFile
     *            the gfs db file
     */
    private void populateGFSEntity(EntityMetadata entityMetadata, List entities, GridFSDBFile gfsDBFile)
    {
        Object entity = instantiateEntity(entityMetadata.getEntityClazz(), null);
        handler.getEntityFromGFSDBFile(entityMetadata.getEntityClazz(), entity, entityMetadata, gfsDBFile,
                kunderaMetadata);
        entities.add(entity);
    }

    /**
     * Gets the DB cursor instance.
     * 
     * @param mongoQuery
     *            the mongo query
     * @param orderBy
     *            the order by
     * @param maxResult
     *            the max result
     * @param firstResult
     *            the first result
     * @param keys
     *            the keys
     * @param documentName
     *            the document name
     * @param isCountQuery
     *            the is count query
     * @return the DB cursor instance
     */
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

    /**
     * Gets the GFSDB files.
     * 
     * @param mongoQuery
     *            the mongo query
     * @param sort
     *            the sort
     * @param collectionName
     *            the collection name
     * @param maxResult
     *            the max result
     * @param firstResult
     *            the first result
     * @return the GFSDB files
     */
    private List<GridFSDBFile> getGFSDBFiles(BasicDBObject mongoQuery, BasicDBObject sort, String collectionName,
            int maxResult, int firstResult)
    {
        KunderaGridFS gfs = new KunderaGridFS(mongoDb, collectionName);
        return gfs.find(mongoQuery, sort, firstResult, maxResult);
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
        MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                entityMetadata.getPersistenceUnit());
        AbstractManagedType managedType = (AbstractManagedType) metaModel.entity(entityMetadata.getEntityClazz());

        DBObject query = new BasicDBObject();

        if (managedType.hasLobAttribute())
        {
            KunderaGridFS gfs = new KunderaGridFS(mongoDb, entityMetadata.getTableName());
            String id = ((AbstractAttribute) entityMetadata.getIdAttribute()).getJPAColumnName();
            query.put("metadata." + id, pKey);
            gfs.remove(query);
        }

        else
        {
            if (metaModel.isEmbeddable(entityMetadata.getIdAttribute().getBindableJavaType()))
            {
                MongoDBUtils.populateCompoundKey(query, entityMetadata, metaModel, pKey);
            }
            else
            {
                query.put("_id", MongoDBUtils.populateValue(pKey, pKey.getClass()));
            }
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
     * @param entityClazz
     *            the entity clazz
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

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.client.ClientBase#onPersist(com.impetus.kundera.metadata
     * .model.EntityMetadata, java.lang.Object, java.lang.Object,
     * java.util.List)
     */
    @Override
    protected void onPersist(EntityMetadata entityMetadata, Object entity, Object id, List<RelationHolder> rlHolders)
    {
        MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                entityMetadata.getPersistenceUnit());

        AbstractManagedType managedType = (AbstractManagedType) metaModel.entity(entityMetadata.getEntityClazz());

        if (managedType.hasLobAttribute())
        {
            onPersistGFS(entity, id, entityMetadata, isUpdate);
        }

        else
        {
            Map<String, List<DBObject>> collections = new HashMap<String, List<DBObject>>();
            collections = onPersist(collections, entity, id, entityMetadata, rlHolders, isUpdate);
            onFlushCollection(collections);
        }
    }

    /**
     * Save GRID FS file.
     * 
     * @param gfsInputFile
     *            the gfs input file
     * @param m
     *            the m
     */
    private void saveGridFSFile(GridFSInputFile gfsInputFile, EntityMetadata m)
    {
        try
        {
            DBCollection coll = mongoDb.getCollection(m.getTableName() + MongoDBUtils.FILES);
            createUniqueIndexGFS(coll, ((AbstractAttribute) m.getIdAttribute()).getJPAColumnName());
            gfsInputFile.save();
            log.info("Input GridFS file: " + gfsInputFile.getFilename() + " is saved successfully in "
                    + m.getTableName() + MongoDBUtils.CHUNKS + " and metadata in " + m.getTableName()
                    + MongoDBUtils.FILES);
        }
        catch (MongoException e)
        {
            log.error("Error in saving GridFS file in " + m.getTableName() + MongoDBUtils.FILES + " or "
                    + m.getTableName() + MongoDBUtils.CHUNKS + " collections.");
            throw new KunderaException("Error in saving GridFS file in " + m.getTableName() + MongoDBUtils.FILES
                    + " or " + m.getTableName() + MongoDBUtils.CHUNKS + " collections. Caused By: ", e);
        }
        try
        {
            gfsInputFile.validate();
            log.info("Input GridFS file: " + gfsInputFile.getFilename() + " is validated.");
        }
        catch (MongoException e)
        {
            log.error("Error in validating GridFS file in " + m.getTableName() + MongoDBUtils.FILES + " collection.");
            throw new KunderaException("Error in validating GridFS file in " + m.getTableName() + MongoDBUtils.FILES
                    + " collection. Caused By: ", e);
        }
    }

    /**
     * On persist GFS.
     * 
     * @param entity
     *            the entity
     * @param entityId
     *            the entityId
     * @param entityMetadata
     *            the entity metadata
     * @param isUpdate
     *            the is update
     */
    private void onPersistGFS(Object entity, Object entityId, EntityMetadata entityMetadata, boolean isUpdate)
    {
        KunderaGridFS gfs = new KunderaGridFS(mongoDb, entityMetadata.getTableName());
        if (!isUpdate)
        {
            GridFSInputFile gfsInputFile = handler.getGFSInputFileFromEntity(gfs, entityMetadata, entity,
                    kunderaMetadata, isUpdate);
            saveGridFSFile(gfsInputFile, entityMetadata);
        }
        else
        {
            Object val = handler.getLobFromGFSEntity(gfs, entityMetadata, entity, kunderaMetadata);
            String md5 = MongoDBUtils.calculateMD5(val);
            GridFSDBFile outputFile = findGridFSDBFile(entityMetadata, entityId);

            // checking MD5 of the file to be updated with the file saved in DB
            if (md5.equals(outputFile.getMD5()))
            {
                DBObject metadata = handler.getMetadataFromGFSEntity(gfs, entityMetadata, entity, kunderaMetadata);
                outputFile.setMetaData(metadata);
                outputFile.save();
            }
            else
            {
                // GFSInput file is created corresponding to the entity to be
                // merged with a new ObjectID()
                GridFSInputFile gfsInputFile = handler.getGFSInputFileFromEntity(gfs, entityMetadata, entity,
                        kunderaMetadata, isUpdate);
                ObjectId updatedId = (ObjectId) gfsInputFile.getId();
                DBObject metadata = gfsInputFile.getMetaData();

                // updated file is saved in DB
                saveGridFSFile(gfsInputFile, entityMetadata);

                // last version of file is deleted
                DBObject query = new BasicDBObject("_id", outputFile.getId());
                gfs.remove(query);

                // newly added file is found using its _id
                outputFile = gfs.findOne(updatedId);

                // Id of entity (which is saved in metadata) is updated to its
                // actual Id
                metadata.put(((AbstractAttribute) entityMetadata.getIdAttribute()).getJPAColumnName(), entityId);
                outputFile.setMetaData(metadata);

                // output file is updated
                outputFile.save();
            }
        }
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
        Map<String, BulkWriteOperation> bulkWriteOperationMap = new HashMap<String, BulkWriteOperation>();
        int size = 0;
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
                    Map<String, DBObject> documents = handler.getDocumentFromEntity(metadata, node.getData(),
                            relationHolders, kunderaMetadata);
                    for (String tableName : documents.keySet())
                    {
                        if (!bulkWriteOperationMap.containsKey(tableName))
                        {
                            DBCollection collection = mongoDb.getCollection(tableName);
                            BulkWriteOperation builder = null;
                            if (isOrderedBulkOperation())
                            {
                                builder = collection.initializeOrderedBulkOperation();
                            }
                            else
                            {
                                builder = collection.initializeUnorderedBulkOperation();
                            }
                            bulkWriteOperationMap.put(tableName, builder);
                        }

                        if (!node.isUpdate())
                        {
                            bulkWriteOperationMap.get(tableName).insert(documents.get(tableName));
                        }

                        else
                        {
                            bulkWriteOperationMap.get(tableName).find(new BasicDBObject("_id", node.getEntityId()))
                                    .upsert().replaceOne(documents.get(tableName));
                        }
                        size++;
                    }
                    indexNode(node, metadata);
                }
                node.handlePostEvent();
            }
        }
        onFlushBatch(bulkWriteOperationMap);
        return size;
    }

    /**
     * On flush batch.
     * 
     * @param bulkWriteOperationMap
     *            the bulk write operation map
     */
    private void onFlushBatch(Map<String, BulkWriteOperation> bulkWriteOperationMap)
    {
        if (!bulkWriteOperationMap.isEmpty())
        {
            for (BulkWriteOperation builder : bulkWriteOperationMap.values())
            {
                try
                {
                    builder.execute(getWriteConcern());
                }
                catch (BulkWriteException bwex)
                {
                    log.error("Batch insertion is not performed due to error in write command. Caused By: ", bwex);
                    throw new KunderaException(
                            "Batch insertion is not performed due to error in write command. Caused By: ", bwex);
                }
                catch (MongoException mex)
                {
                    log.error("Batch insertion is not performed. Caused By: ", mex);
                    throw new KunderaException("Batch insertion is not performed. Caused By: ", mex);
                }
            }
        }
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
            try
            {
                dbCollection.insert(collections.get(tableName).toArray(new DBObject[0]), getWriteConcern(), encoder);
            }
            catch (MongoException ex)
            {
                throw new KunderaException("document is not inserted in " + dbCollection.getFullName()
                        + " collection. Caused By:", ex);
            }
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

                dbCollection.save(documents.get(documentName), getWriteConcern());
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

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.client.ClientPropertiesSetter#populateClientProperties
     * (com.impetus.kundera.client.Client, java.util.Map)
     */
    @Override
    public void populateClientProperties(Client client, Map<String, Object> properties)
    {
        new MongoDBClientProperties().populateClientProperties(client, properties);
    }

    /**
     * Sets the mongo db.
     * 
     * @param mongoDb
     *            the mongoDb to set
     */
    public void setMongoDb(DB mongoDb)
    {
        this.mongoDb = mongoDb;
    }

    /**
     * Sets the handler.
     * 
     * @param handler
     *            the handler to set
     */
    public void setHandler(DefaultMongoDBDataHandler handler)
    {
        this.handler = handler;
    }

    /**
     * Sets the nodes.
     * 
     * @param nodes
     *            the nodes to set
     */
    public void setNodes(List<Node> nodes)
    {
        this.nodes = nodes;
    }

    /**
     * Sets the write concern.
     * 
     * @param writeConcern
     *            the writeConcern to set
     */
    public void setWriteConcern(WriteConcern writeConcern)
    {
        this.writeConcern = writeConcern;
    }

    /**
     * Sets the encoder.
     * 
     * @param encoder
     *            the encoder to set
     */
    public void setEncoder(DBEncoder encoder)
    {
        this.encoder = encoder;
    }

    /**
     * Gets the encoder.
     * 
     * @return the encoder
     */
    public DBEncoder getEncoder()
    {
        return encoder;
    }

    /**
     * Gets the write concern.
     * 
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
     * Sets the batch size.
     * 
     * @param batchSize
     *            the batchSize to set
     */
    public void setBatchSize(int batchSize)
    {
        this.batchSize = batchSize;
    }

    /**
     * Populate batch size.
     * 
     * @param persistenceUnit
     *            the persistence unit
     * @param puProperties
     *            the pu properties
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

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#getIdGenerator()
     */
    @Override
    public Generator getIdGenerator()
    {
        return (Generator) KunderaCoreUtils.createNewInstance(MongoDBIdGenerator.class);
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
     * Execute query.
     * 
     * @param jsonClause
     *            the json clause
     * @param entityMetadata
     *            the entity metadata
     * @return the list
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
            if (entities.size() > 0 && (entities.get(0) instanceof EnhanceEntity))
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

    /**
     * Execute native query.
     * 
     * @param jsonClause
     *            the json clause
     * @param entityMetadata
     *            the entity metadata
     * @return the list
     */
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
        else if (jsonClause.contains("mapReduce("))
        {
            final MapReduceCommand command = parseMapReduceCommand(jsonClause);
            final MapReduceOutput output = mongoDb.getCollection(command.getInput()).mapReduce(command);

            final BasicDBList list = new BasicDBList();
            for (final DBObject item : output.results())
            {
                list.add(item);
            }
            return list;
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
     * Parses the map reduce command.
     * 
     * @param jsonClause
     *            the json clause
     * @return the map reduce command
     */
    private MapReduceCommand parseMapReduceCommand(String jsonClause)
    {
        String collectionName = jsonClause.replaceFirst("(?ms).*?\\.\\s*(.+?)\\s*\\.\\s*mapReduce\\s*\\(.*", "$1");
        if (collectionName.contains("getCollection"))
        {
            collectionName = collectionName
                    .replaceFirst(".*getCollection\\s*\\(\\s*(['\"])([^'\"]+)\\1\\s*\\).*", "$2");
        }

        DBCollection collection = mongoDb.getCollection(collectionName);

        String body = jsonClause.replaceFirst("^(?ms).*?mapReduce\\s*\\(\\s*(.*)\\s*\\)\\s*;?\\s*$", "$1");
        String mapFunction = findCommaSeparatedArgument(body, 0).trim();
        String reduceFunction = findCommaSeparatedArgument(body, 1).trim();

        String query = findCommaSeparatedArgument(body, 2).trim();
        DBObject parameters = (DBObject) JSON.parse(query);
        DBObject mongoQuery;
        if (parameters.containsField("query"))
        {
            mongoQuery = (DBObject) parameters.get("query");
        }
        else
        {
            mongoQuery = new BasicDBObject();
        }

        return new MapReduceCommand(collection, mapFunction, reduceFunction, null, MapReduceCommand.OutputType.INLINE,
                mongoQuery);
    }

    /**
     * Find comma separated argument.
     * 
     * @param functionBody
     *            the function body
     * @param index
     *            the index
     * @return the string
     */
    private String findCommaSeparatedArgument(String functionBody, int index)
    {
        int start = 0;
        int found = -1;
        int brackets = 0;
        int pos = 0;
        int length = functionBody.length();

        while (found < index && pos < length)
        {
            char ch = functionBody.charAt(pos);
            switch (ch)
            {
            case ',':
                if (brackets == 0)
                {
                    found++;

                    if (found < index)
                    {
                        start = pos + 1;
                    }
                }
                break;
            case '(':
            case '[':
            case '{':
                brackets++;
                break;
            case ')':
            case ']':
            case '}':
                brackets--;
                break;
            }

            pos++;
        }

        if (found == index)
        {
            return functionBody.substring(start, pos - 1);
        }
        else if (pos == length)
        {
            return functionBody.substring(start);
        }
        else
        {
            return "";
        }
    }

    /**
     * Parses the and scroll.
     * 
     * @param jsonClause
     *            the json clause
     * @param collectionName
     *            the collection name
     * @return the DB cursor
     * @throws JSONParseException
     *             the JSON parse exception
     */
    private DBCursor parseAndScroll(String jsonClause, String collectionName) throws JSONParseException
    {
        BasicDBObject clause = (BasicDBObject) JSON.parse(jsonClause);
        DBCursor cursor = mongoDb.getCollection(collectionName).find(clause);
        return cursor;
    }

    /**
     * Populate entity.
     * 
     * @param entityMetadata
     *            the entity metadata
     * @param entities
     *            the entities
     * @param fetchedDocument
     *            the fetched document
     */
    private void populateEntity(EntityMetadata entityMetadata, List entities, DBObject fetchedDocument)
    {

        // handler.getEntityFromGFSDBFile(entityClazz, entity, m, outputFile,
        // kunderaMetadata)
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

    /**
     * Handle update functions.
     * 
     * @param query
     *            the query
     * @param update
     *            the update
     * @param collName
     *            the coll name
     * @return the int
     */
    public int handleUpdateFunctions(BasicDBObject query, BasicDBObject update, String collName)
    {
        DBCollection collection = mongoDb.getCollection(collName);
        KunderaCoreUtils.printQuery("Update collection:" + query, showQuery);
        WriteResult result = null;
        try
        {
            result = collection.update(query, update);
        }
        catch (MongoException ex)
        {
            return -1;
        }
        if (result.getN() <= 0)
            return -1;
        return result.getN();
    }

    /**
     * Creates the unique index gfs.
     * 
     * @param coll
     *            the coll
     * @param id
     *            the id
     */
    private void createUniqueIndexGFS(DBCollection coll, String id)
    {
        try
        {
            coll.createIndex(new BasicDBObject("metadata." + id, 1), new BasicDBObject("unique", true));
        }
        catch (MongoException ex)
        {
            throw new KunderaException("Error in creating unique indexes in " + coll.getFullName() + " collection on "
                    + id + " field");
        }
    }

}