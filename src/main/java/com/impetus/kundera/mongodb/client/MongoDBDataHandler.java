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
package com.impetus.kundera.mongodb.client;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import javax.persistence.PersistenceException;
import javax.persistence.Query;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.kundera.ejb.EntityManagerImpl;
import com.impetus.kundera.metadata.model.Column;
import com.impetus.kundera.metadata.model.EmbeddedColumn;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.Relation;
import com.impetus.kundera.mongodb.DocumentObjectMapper;
import com.impetus.kundera.mongodb.query.MongoDBQuery;
import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.proxy.EnhancedEntity;
import com.impetus.kundera.query.KunderaQuery.FilterClause;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

/**
 * Provides utility methods for handling data held in MongoDB
 * 
 * @author amresh.singh
 */
public class MongoDBDataHandler
{
    private static Log log = LogFactory.getLog(MongoDBDataHandler.class);

    public Object getEntityFromDocument(EntityManagerImpl em, Class<?> entityClass, EntityMetadata m, DBObject document)
    {
        Object entity = null;
        try
        {
            entity = entityClass.newInstance();

            // Populate entity columns
            List<Column> columns = m.getColumnsAsList();
            for (Column column : columns)
            {
                PropertyAccessorHelper.set(entity, column.getField(), document.get(column.getName()));
            }

            // Populate primary key column
            PropertyAccessorHelper.set(entity, m.getIdColumn().getField(), document.get(m.getIdColumn().getField().getName()));

            // Populate @Embedded objects and collections
            List<EmbeddedColumn> superColumns = m.getEmbeddedColumnsAsList();
            for (EmbeddedColumn superColumn : superColumns)
            {
                Field superColumnField = superColumn.getField();
                // Can be a BasicDBObject or a list of it.
                Object embeddedDocumentObject = document.get(superColumnField.getName());

                if (embeddedDocumentObject != null)
                {
                    if (embeddedDocumentObject instanceof BasicDBList)
                    {
                        Class embeddedObjectClass = PropertyAccessorHelper.getGenericClass(superColumnField);
                        Collection embeddedCollection = DocumentObjectMapper.getCollectionFromDocumentList(
                                (BasicDBList) embeddedDocumentObject, superColumnField.getType(), embeddedObjectClass,
                                superColumn.getColumns());
                        PropertyAccessorHelper.set(entity, superColumnField, embeddedCollection);
                    }
                    else if (embeddedDocumentObject instanceof BasicDBObject)
                    {
                        Object embeddedObject = DocumentObjectMapper.getObjectFromDocument(
                                (BasicDBObject) embeddedDocumentObject, superColumn.getField().getType(),
                                superColumn.getColumns());
                        PropertyAccessorHelper.set(entity, superColumnField, embeddedObject);

                    }
                    else
                    {
                        throw new PersistenceException("Can't retrieve embedded object from MONGODB document coz "
                                + "it wasn't stored as BasicDBObject, possible problem in format.");
                    }
                }

            }

            // Check relations and fetch data from foreign keys list column
            List<Relation> relations = m.getRelations();
            for (Relation relation : relations)
            {
                Class<?> embeddedEntityClass = relation.getTargetEntity(); // Embedded
                                                                           // entity
                                                                           // class
                Field embeddedPropertyField = relation.getProperty(); // Mapped
                                                                      // to this
                                                                      // property

                EntityMetadata relMetadata = em.getMetadataManager().getEntityMetadata(embeddedEntityClass);
                BasicDBList relList = (BasicDBList) document.get(embeddedPropertyField.getName());
                ; // List foreign keys

                if (relList != null)
                {
                    if (relation.isUnary())
                    {
                        String foreignKey = (String) relList.get(0);

                        Object embeddedEntity = null;
                        try
                        {
                            embeddedEntity = em.getClient().loadData(em, embeddedEntityClass,
                                    relMetadata.getSchema(), relMetadata.getTableName(), foreignKey, relMetadata);
                        }
                        catch (Exception e)
                        {
                            throw new PersistenceException("Error while fetching relationship entity "
                                    + relMetadata.getTableName() + " from " + m.getTableName());
                        }
                        PropertyAccessorHelper.set(entity, embeddedPropertyField, embeddedEntity);
                    }
                    else if (relation.isCollection())
                    {
                        List<String> foreignKeys = new ArrayList<String>();
                        for (Object o : relList)
                        {
                            foreignKeys.add((String) o);
                        }

                        Collection embeddedEntityList = null; // Collection of
                                                              // embedded
                                                              // entities

                        try
                        {
                            embeddedEntityList = em.getClient().loadData(em, embeddedEntityClass,
                                    relMetadata.getSchema(), relMetadata.getTableName(), relMetadata,
                                    foreignKeys.toArray(new String[0]));
                        }
                        catch (Exception e)
                        {
                            throw new PersistenceException("Error while fetching relationship entity collection  "
                                    + relMetadata.getTableName() + " from " + m.getTableName());
                        }

                        Collection<Object> embeddedObjects = null; // Collection
                                                                   // of
                                                                   // embedded
                                                                   // entities
                        if (relation.getPropertyType().equals(Set.class))
                        {
                            embeddedObjects = new HashSet<Object>();
                        }
                        else if (relation.getPropertyType().equals(List.class))
                        {
                            embeddedObjects = new ArrayList<Object>();
                        }

                        embeddedObjects.addAll(embeddedEntityList);
                        PropertyAccessorHelper.set(entity, embeddedPropertyField, embeddedObjects);

                    }

                }

            }

        }
        catch (InstantiationException e)
        {
            log.error("Error while instantiating " + entityClass + ". Details:" + e.getMessage());
            return entity;
        }
        catch (IllegalAccessException e)
        {
            log.error("Error while Getting entity from Document. Details:" + e.getMessage());
            return entity;
        }
        catch (PropertyAccessException e)
        {
            log.error("Error while Getting entity from Document. Details:" + e.getMessage());
            return entity;
        }
        return entity;
    }

    public BasicDBObject getDocumentFromEntity(EntityManagerImpl em, EntityMetadata m, EnhancedEntity e)
            throws PropertyAccessException
    {
        List<Column> columns = m.getColumnsAsList();
        BasicDBObject dbObj = new BasicDBObject();

        // Populate columns
        for (Column column : columns)
        {
            try
            {
                extractEntityField(e.getEntity(), dbObj, column);
            }
            catch (PropertyAccessException e1)
            {
                log.error("Can't access property " + column.getField().getName());
            }
        }

        // Populate @Embedded objects and collections
        List<EmbeddedColumn> superColumns = m.getEmbeddedColumnsAsList();
        for (EmbeddedColumn superColumn : superColumns)
        {
            Field superColumnField = superColumn.getField();
            Object embeddedObject = PropertyAccessorHelper.getObject(e.getEntity(), superColumnField);

            if (embeddedObject != null)
            {
                if (embeddedObject instanceof Collection)
                {

                    Collection embeddedCollection = (Collection) embeddedObject;

                    dbObj.put(
                            superColumnField.getName(),
                            DocumentObjectMapper.getDocumentListFromCollection(embeddedCollection,
                                    superColumn.getColumns()));
                }
                else
                {
                    dbObj.put(superColumnField.getName(),
                            DocumentObjectMapper.getDocumentFromObject(embeddedObject, superColumn.getColumns()));
                }
            }
        }

        // Check foreign keys and set as list column on document object
        Map<String, Set<String>> foreignKeyMap = e.getForeignKeysMap();
        Set foreignKeyNameSet = foreignKeyMap.keySet();
        for (Object foreignKeyName : foreignKeyNameSet)
        {
            Set valueSet = foreignKeyMap.get(foreignKeyName);
            BasicDBList foreignKeyValueList = new BasicDBList();
            for (Object o : valueSet)
            {
                foreignKeyValueList.add(o);
            }
            dbObj.put((String) foreignKeyName, foreignKeyValueList);
        }

        return dbObj;
    }

    /**
     * @param entity
     * @param dbObj
     * @param column
     * @throws PropertyAccessException
     */
    private void extractEntityField(Object entity, BasicDBObject dbObj, Column column) throws PropertyAccessException
    {
        // A column field may be a collection(not defined as 1-to-M
        // relationship)
        if (column.getField().getType().equals(List.class) || column.getField().getType().equals(Set.class))
        {
            Collection collection = (Collection) PropertyAccessorHelper.getObject(entity, column.getField());
            BasicDBList basicDBList = new BasicDBList();
            for (Object o : collection)
            {
                basicDBList.add(o);
            }
            dbObj.put(column.getName(), basicDBList);
        }
        else
        {
            dbObj.put(column.getName(), PropertyAccessorHelper.getString(entity, column.getField()));
        }
    }

    /**
     * Returns column name from the filter property which is in the form
     * dbName.columnName
     * 
     * @param filterProperty
     * @return
     */
    public String getColumnName(String filterProperty)
    {
        StringTokenizer st = new StringTokenizer(filterProperty, ".");
        String columnName = "";
        while (st.hasMoreTokens())
        {
            columnName = st.nextToken();
        }

        return columnName;
    }

    /**
     * Creates MongoDB Query object from filterClauseQueue
     * 
     * @param filterClauseQueue
     * @return
     */
    public BasicDBObject createMongoQuery(EntityMetadata m, Queue filterClauseQueue)
    {
        BasicDBObject query = new BasicDBObject();
        for (Object object : filterClauseQueue)
        {
            if (object instanceof FilterClause)
            {
                FilterClause filter = (FilterClause) object;
                String property = new MongoDBDataHandler().getColumnName(filter.getProperty());
                String condition = filter.getCondition();
                String value = filter.getValue();

                // Property, if doesn't exist in entity, may be there in a
                // document embedded within it, so we have to check that
                // TODO: Query should actually be in a format
                // documentName.embeddedDocumentName.column, remove below if
                // block once this is decided
                String enclosingDocumentName = getEnclosingDocumentName(m, property);
                if (enclosingDocumentName != null)
                {
                    property = enclosingDocumentName + "." + property;
                }

                if (condition.equals("="))
                {
                    query.append(property, value);
                }
                else if (condition.equalsIgnoreCase("like"))
                {
                    query.append(property, Pattern.compile(value));
                }
                // TODO: Add support for other operators like >, <, >=, <=,
                // order by asc/ desc, limit, skip, count etc
            }
        }
        return query;
    }

    /**
     * @param m
     * @param columnName
     * @param embeddedDocumentName
     * @return
     */
    public String getEnclosingDocumentName(EntityMetadata m, String columnName)
    {
        String enclosingDocumentName = null;
        if (!m.getColumnFieldNames().contains(columnName))
        {

            for (EmbeddedColumn superColumn : m.getEmbeddedColumnsAsList())
            {
                List<Column> columns = superColumn.getColumns();
                for (Column column : columns)
                {
                    if (column.getName().equals(columnName))
                    {
                        enclosingDocumentName = superColumn.getName();
                        break;
                    }
                }
            }

        }
        return enclosingDocumentName;
    }

    /**
     * Retrieves A collection of embedded object within a document that match a
     * criteria specified in <code>query</code> TODO: This code requires a serious
     * overhawl. Currently it assumes that user query is in the form
     * "Select alias.columnName from EntityName alias". However, correct query
     * to be supported is
     * "Select alias.superColumnName.columnName from EntityName alias"
     */
    public List getEmbeddedObjectList(DBCollection dbCollection, EntityMetadata m, String documentName, Query query)
            throws PropertyAccessException
    {
        List list = new ArrayList();// List of embedded object to be returned
        
        //Query parameters, 
        MongoDBQuery mongoDBQuery = (MongoDBQuery) query;
        Queue filterClauseQueue = mongoDBQuery.getFilterClauseQueue();
        String result = mongoDBQuery.getResult();
        
        //Specified after entity alias in query
        String columnName = getColumnName(result);
        
        //Something user didn't specify and we have to derive
        //TODO: User must specify this in query and remove this logic once query format is changed
        String enclosingDocumentName = getEnclosingDocumentName(m, columnName);
        
        //Query for fetching entities based on user specified criteria
        BasicDBObject mongoQuery = createMongoQuery(m, filterClauseQueue);
        DBCursor cursor = dbCollection.find(mongoQuery);

        EmbeddedColumn superColumn = m.getEmbeddedColumn(enclosingDocumentName);
        Field superColumnField = superColumn.getField();
        while (cursor.hasNext())
        {
            DBObject fetchedDocument = cursor.next();
            Object embeddedDocumentObject = fetchedDocument.get(superColumnField.getName());

            if (embeddedDocumentObject != null)
            {
                if (embeddedDocumentObject instanceof BasicDBList)
                {
                    Class embeddedObjectClass = PropertyAccessorHelper.getGenericClass(superColumnField);
                    for (Object dbObj : (BasicDBList) embeddedDocumentObject)
                    {
                        Object embeddedObject = new DocumentObjectMapper().getObjectFromDocument((BasicDBObject) dbObj,
                                embeddedObjectClass, superColumn.getColumns());
                        Object fieldValue = PropertyAccessorHelper.getObject(embeddedObject, columnName);

                        for (Object object : filterClauseQueue)
                        {
                            if (object instanceof FilterClause)
                            {
                                FilterClause filter = (FilterClause) object;
                                String value = filter.getValue();
                                String condition = filter.getCondition();
                                
                                //This is not an ideal and complete implementation. A similar logic exists in 
                                // createMongoQuery method. Need to find a way to combine them
                                if (condition.equals("="))
                                {
                                    if (value.equals(fieldValue))
                                    {
                                        list.add(embeddedObject);
                                    }
                                }
                                else if (condition.equalsIgnoreCase("like"))
                                {
                                    if (fieldValue.toString().indexOf(value) >= 0)
                                    {
                                        list.add(embeddedObject);
                                    }
                                }

                            }
                        }
                    }

                }
                else if (embeddedDocumentObject instanceof BasicDBObject)
                {
                    Object embeddedObject = DocumentObjectMapper.getObjectFromDocument(
                            (BasicDBObject) embeddedDocumentObject, superColumn.getField().getType(),
                            superColumn.getColumns());
                    list.add(embeddedObject);

                }
                else
                {
                    throw new PersistenceException("Can't retrieve embedded object from MONGODB document coz "
                            + "it wasn't stored as BasicDBObject, possible problem in format.");
                }
            }

        }
        return list;
    }
}
