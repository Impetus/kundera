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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import javax.persistence.Embedded;
import javax.persistence.PersistenceException;
import javax.persistence.Query;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.client.mongodb.query.MongoDBQuery;
import com.impetus.kundera.Constants;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.db.RelationHolder;
import com.impetus.kundera.metadata.MetadataUtils;
import com.impetus.kundera.metadata.model.Column;
import com.impetus.kundera.metadata.model.EmbeddedColumn;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.persistence.EntityResolver;
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
    private Client client;

    private String persistenceUnit;

    public MongoDBDataHandler(Client client, String persistenceUnit)
    {
        super();
        this.client = client;
        this.persistenceUnit = persistenceUnit;
    }

    private static Log log = LogFactory.getLog(MongoDBDataHandler.class);

    public Object getEntityFromDocument(Class<?> entityClass, EntityMetadata m, DBObject document)
    {
        // Entity object
        Object entity = null;

        // Map to hold property-name=>foreign-entity relations
//        Map<String, Set<String>> foreignKeysMap = new HashMap<String, Set<String>>();

        try
        {
            entity = entityClass.newInstance();

            // Populate primary key column
            String rowKey = (String) document.get(m.getIdColumn().getName());
            PropertyAccessorHelper.set(entity, m.getIdColumn().getField(), rowKey);

            // Populate entity columns
            List<Column> columns = m.getColumnsAsList();
            for (Column column : columns)
            {
                PropertyAccessorHelper.set(entity, column.getField(), document.get(column.getName()));
            }

            // Populate @Embedded objects and collections
            List<EmbeddedColumn> embeddedColumns = m.getEmbeddedColumnsAsList();
            for (EmbeddedColumn embeddedColumn : embeddedColumns)
            {
                Field embeddedColumnField = embeddedColumn.getField();
                // Can be a BasicDBObject or a list of it.
                Object embeddedDocumentObject = document.get(embeddedColumnField.getName());

                if (embeddedDocumentObject != null)
                {
                    if (embeddedDocumentObject instanceof BasicDBList)
                    {
                        Class embeddedObjectClass = PropertyAccessorHelper.getGenericClass(embeddedColumnField);
                        Collection embeddedCollection = DocumentObjectMapper.getCollectionFromDocumentList(
                                (BasicDBList) embeddedDocumentObject, embeddedColumnField.getType(),
                                embeddedObjectClass, embeddedColumn.getColumns());
                        PropertyAccessorHelper.set(entity, embeddedColumnField, embeddedCollection);
                    }
                    else if (embeddedDocumentObject instanceof BasicDBObject)
                    {
                    	Object embeddedObject = null;
                    	if(embeddedColumnField.isAnnotationPresent(Embedded.class))
                    	{
                        embeddedObject = DocumentObjectMapper.getObjectFromDocument(
                                (BasicDBObject) embeddedDocumentObject, embeddedColumn.getField().getType(),
                                embeddedColumn.getColumns());
                    	} else
                    	{
                            
                    		embeddedObject =((BasicDBObject) embeddedDocumentObject).get(embeddedColumn.getName()) ;
                    		
                    	}
                        PropertyAccessorHelper.set(entity, embeddedColumnField, embeddedObject);

                    }
                    else
                    {
                        throw new PersistenceException("Can't retrieve embedded object from MONGODB document coz "
                                + "it wasn't stored as BasicDBObject, possible problem in format.");
                    }
                }

            }

//            // Check whether there is an embedded document for foreign keys, if
//            // it is there, put data
//            // into foreign keys map
//            Object foreignKeyObj = document.get(Constants.FOREIGN_KEY_EMBEDDED_COLUMN_NAME);
//            if (foreignKeyObj != null && foreignKeyObj instanceof BasicDBObject)
//            {
//                BasicDBObject dbObj = (BasicDBObject) foreignKeyObj;
//
//                Set<String> foreignKeySet = dbObj.keySet();
//                for (String foreignKey : foreignKeySet)
//                {
//                    String foreignKeyValues = (String) dbObj.get(foreignKey); // Foreign
//                                                                              // key
//                                                                              // values
//                                                                              // are
//                                                                              // stored
//                                                                              // as
//                                                                              // list
//
//                    Set<String> foreignKeysSet = MetadataUtils.deserializeKeys(foreignKeyValues);
//
//                    foreignKeysMap.put(foreignKey, foreignKeysSet);
//
//                }
//
//            }

            // Set entity object and foreign key map into enhanced entity and
            // return
//            EnhancedEntity e = EntityResolver.getEnhancedEntity(entity, rowKey, foreignKeysMap);
            return entity;

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

    }

    private Client getClient()
    {
        return client;
    }

    private String getPersistenceUnit()
    {
        return persistenceUnit;
    }

    public BasicDBObject getDocumentFromEntity(EntityMetadata m, Object entity, List<RelationHolder> relations) throws PropertyAccessException
    {
        List<Column> columns = m.getColumnsAsList();
        BasicDBObject dbObj = new BasicDBObject();

        // Populate Row Key        
        dbObj.put("_id", PropertyAccessorHelper.getId(entity, m));  

        // Populate columns
        for (Column column : columns)
        {
            try
            {
                extractEntityField(entity, dbObj, column);
            }
            catch (PropertyAccessException e1)
            {
                log.error("Can't access property " + column.getField().getName());
            }
        }

        // Populate @Embedded objects and collections
        List<EmbeddedColumn> embeddedColumns = m.getEmbeddedColumnsAsList();
        for (EmbeddedColumn embeddedColumn : embeddedColumns)
        {
            Field superColumnField = embeddedColumn.getField();
            Object embeddedObject = PropertyAccessorHelper.getObject(entity, superColumnField);

            if (embeddedObject != null)
            {
                if (embeddedObject instanceof Collection)
                {

                    Collection embeddedCollection = (Collection) embeddedObject;

                    dbObj.put(
                            superColumnField.getName(),
                            DocumentObjectMapper.getDocumentListFromCollection(embeddedCollection,
                                    embeddedColumn.getColumns()));
                }
                else
                {
                	if(superColumnField.isAnnotationPresent(Embedded.class))
                	{
                		dbObj.put(superColumnField.getName(),
                				DocumentObjectMapper.getDocumentFromObject(embeddedObject, embeddedColumn.getColumns()));
                	} else 
                	{
                		dbObj.put(superColumnField.getName(),
                				DocumentObjectMapper.getDocumentFromObject(entity, embeddedColumn.getColumns()));
                		
                	}
                }
            }
        }
        
        //Populate foreign keys
        if(relations != null) {
        	for(RelationHolder rh : relations) {
        		dbObj.put(rh.getRelationName(), rh.getRelationValue());
        	}
        }

        /*// Check foreign keys and set as list column on document object
        Map<String, Set<String>> foreignKeyMap = e.getForeignKeysMap();
        if (foreignKeyMap != null && !foreignKeyMap.isEmpty())
        {

            DBObject foreignKeyObj = new BasicDBObject(); // A document
                                                          // containing all
                                                          // foreign keys as
                                                          // columns

            Set foreignKeyNameSet = foreignKeyMap.keySet();
            for (Object foreignKeyName : foreignKeyNameSet)
            {
                Set<String> valueSet = foreignKeyMap.get(foreignKeyName);

                String foreignKeyValues = MetadataUtils.serializeKeys(valueSet);

                foreignKeyObj.put((String) foreignKeyName, foreignKeyValues);
            }

            dbObj.put(Constants.FOREIGN_KEY_EMBEDDED_COLUMN_NAME, foreignKeyObj);
        }
*/
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
            dbObj.put(column.getName(), PropertyAccessorHelper.getObject(entity, column.getField()).toString());            
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
                String property = new MongoDBDataHandler(getClient(), getPersistenceUnit()).getColumnName(filter
                        .getProperty());
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
     * criteria specified in <code>query</code> TODO: This code requires a
     * serious overhawl. Currently it assumes that user query is in the form
     * "Select alias.columnName from EntityName alias". However, correct query
     * to be supported is
     * "Select alias.superColumnName.columnName from EntityName alias"
     */
    public List getEmbeddedObjectList(DBCollection dbCollection, EntityMetadata m, String documentName, Query query)
            throws PropertyAccessException
    {
        List list = new ArrayList();// List of embedded object to be returned

        // Query parameters,
        MongoDBQuery mongoDBQuery = (MongoDBQuery) query;
        Queue filterClauseQueue = mongoDBQuery.getKunderaQuery().getFilterClauseQueue();
        String result = mongoDBQuery.getKunderaQuery().getResult();

        // Specified after entity alias in query
        String columnName = getColumnName(result);

        // Something user didn't specify and we have to derive
        // TODO: User must specify this in query and remove this logic once
        // query format is changed
        String enclosingDocumentName = getEnclosingDocumentName(m, columnName);

        // Query for fetching entities based on user specified criteria
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

                                // This is not an ideal and complete
                                // implementation. A similar logic exists in
                                // createMongoQuery method. Need to find a way
                                // to combine them
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