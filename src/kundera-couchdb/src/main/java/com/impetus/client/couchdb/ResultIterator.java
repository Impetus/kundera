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
package com.impetus.client.couchdb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.NoSuchElementException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.KunderaException;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.EnhanceEntity;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.persistence.PersistenceDelegator;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.query.IResultIterator;

/**
 * The Class ResultIterator.
 * 
 * @author kuldeep.mishra .
 * 
 *         Implementation of CouchDB result iteration.
 * @param <E>
 *            the element type
 */
class ResultIterator<E> implements IResultIterator<E>
{
    /** The logger. */
    private static Logger logger = LoggerFactory.getLogger(ResultIterator.class);

    /** The m. */
    private EntityMetadata m;

    /** The client. */
    private CouchDBClient client;

    /** The fetch size. */
    private int fetchSize;

    /** The persistence delegator. */
    private PersistenceDelegator persistenceDelegator;

    /** The _id. */
    private String _id;

    /** The q. */
    private StringBuilder q = new StringBuilder();

    /** The interpreter. */
    private CouchDBQueryInterpreter interpreter;

    /** The skip counter. */
    private int skipCounter = 0;

    /** The count. */
    private int count = 0;

    /** The current object. */
    private E currentObject = null;

    /** The scroll complete. */
    private boolean scrollComplete = false;

    /** The results. */
    private List results = new ArrayList();

    /**
     * Instantiates a new result iterator.
     * 
     * @param client
     *            the client
     * @param m
     *            the m
     * @param pd
     *            the pd
     * @param interpreter
     *            the interpreter
     * @param fetchSize
     *            the fetch size
     */
    public ResultIterator(CouchDBClient client, EntityMetadata m, PersistenceDelegator pd,
            CouchDBQueryInterpreter interpreter, Integer fetchSize)
    {
        this.m = m;
        this.client = client;
        this.fetchSize = fetchSize;
        this.persistenceDelegator = pd;
        this.interpreter = interpreter;
        onQuery();
    }

    /**
     * On query.
     */
    private void onQuery()
    {
        try
        {
            _id = CouchDBConstants.URL_SEPARATOR + m.getSchema() + CouchDBConstants.URL_SEPARATOR + "_design/"
                    + m.getTableName() + "/_view/";
            _id = client.createQuery(interpreter, m, q, _id);

            if (!q.toString().isEmpty())
            {
                q.append("&");
            }
            q.append("limit=" + 1);
        }
        catch (Exception e)
        {

        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Iterator#hasNext()
     */
    @Override
    public boolean hasNext()
    {
        currentObject = fetchObject();
        if (count < fetchSize && fetchSize > 0 && currentObject != null)
        {
            count++;
            return true;
        }
        scrollComplete = true;
        return false;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Iterator#next()
     */
    @Override
    public E next()
    {
        if (fetchSize <= 0 || count > fetchSize || scrollComplete)
        {
            throw new NoSuchElementException("Nothing to scroll further for:" + m.getEntityClazz());
        }
        else
        {
            E objectToReturn = currentObject;
            currentObject = null;
            return objectToReturn;
        }
    }

    /**
     * Fetch object.
     * 
     * @return the e
     */
    private E fetchObject()
    {
        try
        {
            String previousSkip = "&skip=" + (skipCounter - 1);
            String currentSkip = "&skip=" + skipCounter;
            int indexOf = q.indexOf(previousSkip);
            if (indexOf > 0)
            {
                q.replace(indexOf, indexOf + previousSkip.length(), currentSkip);
            }
            else if (skipCounter > 0)
            {
                q.append(currentSkip);
            }
            client.executeQueryAndGetResults(q, _id, m, results, interpreter);
            skipCounter++;
        }
        catch (Exception e)
        {
            logger.error("Error while executing query, caused by {}.", e);
            throw new KunderaException("Error while executing query, caused by : " + e);
        }
        if (results != null && !results.isEmpty())
        {
            Object object = results.get(0);
            results = new ArrayList();
            if (!m.isRelationViaJoinTable() && (m.getRelationNames() == null || (m.getRelationNames().isEmpty())))
            {
                return (E) object;
            }
            else
            {
                return setRelationEntities(object, client, m);
            }
        }
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Iterator#remove()
     */
    @Override
    public void remove()
    {
        throw new UnsupportedOperationException("remove method is not supported over pagination");
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.query.IResultIterator#next(int)
     */
    @Override
    public List<E> next(int chunkSize)
    {
        throw new UnsupportedOperationException("fetch in chunks is not yet supported");
    }

    /**
     * Sets the relation entities.
     * 
     * @param enhanceEntity
     *            the enhance entity
     * @param client
     *            the client
     * @param m
     *            the m
     * @return the e
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