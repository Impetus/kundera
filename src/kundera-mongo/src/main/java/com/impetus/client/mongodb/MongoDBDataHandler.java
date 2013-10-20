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

import com.impetus.kundera.db.RelationHolder;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.property.PropertyAccessException;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

import java.util.List;

/**
 * Provides utility methods for handling data held in MongoDB.
 *
 * @author Sergei @bsideup Egorov
 *
 */
public interface MongoDBDataHandler
{
    /**
     * Gets the entity from document.
     *
     * @param entityClass
     *            the entity class
     * @param m
     *            the m
     * @param document
     *            the document
     * @param relations
     *            the relations
     * @return the entity from document
     */
    Object getEntityFromDocument(Class<?> entityClass, EntityMetadata m, DBObject document, List<String> relations);


    /**
     * Retrieves A collection of embedded object within a document that match a
     * criteria specified in <code>query</code> TODO: This code requires a
     * serious overhawl. Currently it assumes that user query is in the form
     * "Select alias.columnName from EntityName alias". However, correct query
     * to be supported is
     * "Select alias.superColumnName.columnName from EntityName alias"
     *
     * @param dbCollection
     *            the db collection
     * @param m
     *            the m
     * @param documentName
     *            the document name
     * @param mongoQuery
     *            the mongo query
     * @param result
     *            the result
     * @param orderBy
     *            the order by
     * @param maxResult
     * @return the embedded object list
     * @throws com.impetus.kundera.property.PropertyAccessException
     *             the property access exception
     */
    List getEmbeddedObjectList(DBCollection dbCollection, EntityMetadata m, String documentName,
                               BasicDBObject mongoQuery, String result, BasicDBObject orderBy, int maxResult, BasicDBObject keys)
            throws PropertyAccessException;


    /**
     * Gets the document from entity.
     *
     * @param m
     *            the m
     * @param entity
     *            the entity
     * @param relations
     *            the relations
     * @return the document from entity
     * @throws PropertyAccessException
     *             the property access exception
     */
    DBObject getDocumentFromEntity(DBObject dbObj, EntityMetadata m, Object entity, List<RelationHolder> relations)
            throws PropertyAccessException;
}
