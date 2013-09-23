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
import java.util.List;
import java.util.NoSuchElementException;

import com.impetus.client.mongodb.MongoDBClient;
import com.impetus.client.mongodb.MongoDBDataHandler;
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

    private MongoDBDataHandler handler;

    private PersistenceDelegator persistenceDelegator;

    public ResultIterator(MongoDBClient client, EntityMetadata m, BasicDBObject basicDBObject,
            BasicDBObject orderByClause, BasicDBObject keys, PersistenceDelegator pd, int fetchSize)
    {
        this.m = m;
        this.client = client;
        this.fetchSize = fetchSize;
        this.persistenceDelegator = pd;
        this.handler = new MongoDBDataHandler();
        onQuery(orderByClause, basicDBObject, keys);
    }

    private void onQuery(BasicDBObject orderByClause, BasicDBObject mongoQuery, BasicDBObject keys)
    {
        try
        {
            cursor = client.getDBCursorInstance(mongoQuery, orderByClause, fetchSize, keys, m.getTableName());
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
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
            E entityFromDocument = (E) handler.getEntityFromDocument(m.getEntityClazz(), m, document,
                    m.getRelationNames());
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

    @Override
    public void remove()
    {
        throw new UnsupportedOperationException("remove method is not supported over pagination");
    }

    @Override
    public List<E> next(int chunkSize)
    {
        throw new UnsupportedOperationException("fetch in chunks is not yet supported");
    }

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
                    persistenceDelegator, false);
        }
        return result;
    }
}