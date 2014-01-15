/**
 * Copyright 2014 Impetus Infotech.
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
package com.impetus.client.rdbms.query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import javax.persistence.PersistenceException;

import org.hibernate.SQLQuery;

import com.impetus.client.rdbms.HibernateClient;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.EnhanceEntity;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.persistence.PersistenceDelegator;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.query.IResultIterator;

/**
 * @author kuldeep.mishra .
 * 
 *         Implementation of RDBMS result iteration.
 * 
 * @param <E>
 */
class ResultIterator<E> implements IResultIterator<E>
{
    private EntityMetadata m;

    private HibernateClient client;

    private int fetchSize;

    private PersistenceDelegator persistenceDelegator;

    private SQLQuery sqlQuery;

    private List result;

    private int firstResultIndex;

    private int maxResultIndex;

    ResultIterator(HibernateClient client, EntityMetadata m, PersistenceDelegator pd, int fetchSize, String query)
    {
        this.m = m;
        this.client = client;
        this.fetchSize = fetchSize;
        this.maxResultIndex = fetchSize;
        this.persistenceDelegator = pd;
        this.sqlQuery = client.getQueryInstance(query, m);
    }

    @Override
    public boolean hasNext()
    {
        if (firstResultIndex < maxResultIndex)
        {
            result = sqlQuery.setFirstResult(firstResultIndex++).setMaxResults(1).list();
        }

        return result != null && !result.isEmpty() && fetchSize != 0;
    }

    @Override
    public E next()
    {
        if (result == null || result.isEmpty() || fetchSize == 0)
        {
            throw new NoSuchElementException("Nothing to scroll further for: " + m.getEntityClazz());
        }

        fetchSize--;

        Object nextResult = result.get(0);

        result = null;

        Class clazz = m.getEntityClazz();
        E entity = null;
        entity = instantiateEntity(clazz, entity);

        boolean noRelationFound = true;
        if (!nextResult.getClass().isAssignableFrom(clazz))
        {
            entity = (E) ((Object[]) nextResult)[0];
            noRelationFound = false;
        }
        else
        {
            entity = (E) nextResult;
        }

        Object id = PropertyAccessorHelper.getId(entity, m);
        entity = (E) new EnhanceEntity(entity, id, noRelationFound ? null : populateRelations(m.getRelationNames(),
                (Object[]) nextResult));

        if (!m.isRelationViaJoinTable() && (m.getRelationNames() == null || (m.getRelationNames().isEmpty())))
        {
            return entity;
        }
        else
        {
            List<EnhanceEntity> ls = new ArrayList<EnhanceEntity>();
            ls.add((EnhanceEntity) entity);
            return setRelationEntities(ls.get(0), client, m);
        }
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
                    persistenceDelegator, false);
        }
        return result;
    }

    /**
     * Populate relations.
     * 
     * @param relations
     *            the relations
     * @param o
     *            the o
     * @return the map
     */
    private Map<String, Object> populateRelations(List<String> relations, Object[] o)
    {
        Map<String, Object> relationVal = new HashMap<String, Object>(relations.size());
        int counter = 1;
        for (String r : relations)
        {
            relationVal.put(r, o[counter++]);
        }
        return relationVal;
    }
}