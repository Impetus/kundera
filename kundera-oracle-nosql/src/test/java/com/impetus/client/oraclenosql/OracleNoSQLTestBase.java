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
package com.impetus.client.oraclenosql;

import java.util.List;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import org.apache.commons.lang.StringUtils;

import com.impetus.kundera.metadata.model.ClientMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;

/**
 * Base class for all test cases
 * 
 * @author amresh.singh
 */
public class OracleNoSQLTestBase
{

    private static final String PERSISTENCE_UNIT = "twikvstore";

    /** The emf. */
    private static EntityManagerFactory emf;

    /** The em. */
    private static EntityManager em;

    protected void setUp() throws Exception
    {
        emf = Persistence.createEntityManagerFactory(PERSISTENCE_UNIT);
        em = emf.createEntityManager();
    }

    protected void tearDown()
    {
        em.close();
        emf.close();
    }

    protected void clearEm()
    {
        em.clear();
    }

    protected void setEmProperty(String key, Object value)
    {
        em.setProperty(key, value);
    }

    protected void persist(Object entity)
    {
        em.persist(entity);
    }

    protected Object find(Class<?> entityClass, Object id)
    {
        return em.find(entityClass, id);
    }

    protected void update(Object entity)
    {
        em.merge(entity);
    }

    protected void delete(Object entity)
    {
        em.remove(entity);
    }

    protected List executeSelectQuery(String jpaQuery)
    {
        Query query = em.createQuery(jpaQuery);
        return query.getResultList();
    }

    protected List executeSelectQuery(String jpaQuery, Map<Object, Object> params)
    {
        Query query = em.createQuery(jpaQuery);
        setParams(params, query);
        return query.getResultList();
    }

    protected int executeDMLQuery(String jpaQuery)
    {
        Query query = em.createQuery(jpaQuery);
        int updateCount = query.executeUpdate();
        return updateCount;
    }

    protected List executeNamedQuery(String namedQuery, Map<Object, Object> params)
    {
        Query query = em.createNamedQuery(namedQuery);
        setParams(params, query);
        return query.getResultList();
    }

    protected void begingTx()
    {
        em.getTransaction().begin();
    }

    protected void commitTx()
    {
        em.getTransaction().commit();
    }

    protected void rollbackTx()
    {
        em.getTransaction().rollback();
    }

    /**
     * @param params
     * @param query
     */
    private void setParams(Map<Object, Object> params, Query query)
    {
        if (params != null && !params.isEmpty())
        {
            for (Object param : params.keySet())
            {
                if (param instanceof Integer)
                {
                    query.setParameter(((Integer) param).intValue(), params.get(param));
                }
                else if (param instanceof String)
                {
                    query.setParameter((String) param, params.get(param));
                }
            }

        }
    }

    protected boolean isLuceneIndexingEnabled()
    {
        if (emf != null)
        {
            ClientMetadata clientMetadata = KunderaMetadata.INSTANCE.getClientMetadata(PERSISTENCE_UNIT);
            String luceneDirectory = clientMetadata.getLuceneIndexDir();
            if (!StringUtils.isEmpty(luceneDirectory))
            {
                return true;
            }
        }
        return false;
    }

}
