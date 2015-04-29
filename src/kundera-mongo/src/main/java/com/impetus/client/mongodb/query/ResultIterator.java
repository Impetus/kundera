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
package com.impetus.client.mongodb.query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import javax.persistence.PersistenceException;

import com.impetus.client.mongodb.DefaultMongoDBDataHandler;
import com.impetus.client.mongodb.MongoDBClient;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.EnhanceEntity;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.persistence.PersistenceDelegator;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.query.IResultIterator;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

/**
 * @author kuldeep.mishra .
 * 
 *         Implementation of MongoDB result iteration.
 * 
 * @param <E>
 */
class ResultIterator<E> implements IResultIterator<E>
{
    private DBCursor cursor;

    private EntityMetadata m;

    private MongoDBClient client;

    private int fetchSize;

    private DefaultMongoDBDataHandler handler;

    private PersistenceDelegator persistenceDelegator;

    ResultIterator(MongoDBClient client, EntityMetadata m, BasicDBObject basicDBObject, BasicDBObject orderByClause,
            BasicDBObject keys, PersistenceDelegator pd, int fetchSize)
    {
        this.m = m;
        this.client = client;
        this.fetchSize = fetchSize;
        this.persistenceDelegator = pd;
        this.handler = new DefaultMongoDBDataHandler();
        this.cursor = (DBCursor) client.getDBCursorInstance(basicDBObject, orderByClause, fetchSize, 0, keys,
                m.getTableName(), false);
    }

    @Override
    public boolean hasNext()
    {
        if (cursor != null && fetchSize != 0)
        {
            return cursor.hasNext();
        }
        return false;
    }

    @Override
    public E next()
    {
        if (!cursor.hasNext() || fetchSize == 0)
        {
            throw new NoSuchElementException("Nothing to scroll further for:" + m.getEntityClazz());
        }
        if (cursor.hasNext())
        {
            fetchSize--;
            DBObject document = cursor.next();
            E entityFromDocument = instantiateEntity(m.getEntityClazz(), null);
            Map<String, Object> relationValue = null;
            relationValue = handler.getEntityFromDocument(m.getEntityClazz(), entityFromDocument, m, document,
                    m.getRelationNames(), relationValue, persistenceDelegator.getKunderaMetadata());

            if (relationValue != null && !relationValue.isEmpty())
            {
                entityFromDocument = (E) new EnhanceEntity(entityFromDocument, PropertyAccessorHelper.getId(
                        entityFromDocument, m), relationValue);
            }

            if (!m.isRelationViaJoinTable() && (m.getRelationNames() == null || (m.getRelationNames().isEmpty())))
            {
                return entityFromDocument;
            }
            else
            {
                List<EnhanceEntity> ls = new ArrayList<EnhanceEntity>();
                ls.add((EnhanceEntity) entityFromDocument);
                return setRelationEntities(ls.get(0), client, m);
            }
        }
        return null;
    }

    /**
     * 
     * @param entityClazz
     * @param entity
     * @return
     */
    private E instantiateEntity(Class<?> entityClazz, Object entity)
    {
        try
        {
            if (entity == null)
            {
                return (E) entityClazz.newInstance();
            }
            return (E) entity;
        }
        catch (InstantiationException e)
        {
            throw new PersistenceException("Error while instantiating entity " + entityClazz + ".", e);
        }
        catch (IllegalAccessException e)
        {
            throw new PersistenceException("Error while instantiating entity " + entityClazz + ".", e);
        }
    }

    @Override
    public void remove()
    {
        throw new UnsupportedOperationException("Remove method is not supported over pagination");
    }

    @Override
    public List<E> next(int chunkSize)
    {
        throw new UnsupportedOperationException("Fetch in chunks is not yet supported");
    }

    /**
     * 
     * @param enhanceEntity
     * @param client
     * @param m
     * @return
     */
    private E setRelationEntities(Object enhanceEntity, Client client, EntityMetadata m)
    {
        // Enhance entities can contain or may not contain relation.
        // if it contain a relation means it is a child
        // if it does not then it means it is a parent.
        E result = null;
        if (enhanceEntity != null)
        {
            if (!(enhanceEntity instanceof EnhanceEntity))
            {
                enhanceEntity = new EnhanceEntity(enhanceEntity, PropertyAccessorHelper.getId(enhanceEntity, m), null);
            }
            EnhanceEntity ee = (EnhanceEntity) enhanceEntity;

            result = (E) client.getReader().recursivelyFindEntities(ee.getEntity(), ee.getRelations(), m,
                    persistenceDelegator, false, new HashMap<Object, Object>());
        }
        return result;
    }
}