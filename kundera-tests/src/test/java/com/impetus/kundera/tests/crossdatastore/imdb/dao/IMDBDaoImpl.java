/**
 * Copyright 2012 Impetus Infotech.
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
package com.impetus.kundera.tests.crossdatastore.imdb.dao;

import java.util.Set;

import javax.persistence.EntityManager;

/**
 * Implementation of {@link IMDBDao} 
 * @author amresh.singh
 */
public class IMDBDaoImpl extends BaseDao {
    
    private String persistenceUnit;

    /**
     * @param persistenceUnit
     */
    public IMDBDaoImpl(String persistenceUnit)
    {
        this.persistenceUnit = persistenceUnit;
    }
    
    public void insert(Object actor)
    {
        em = getEntityManager(persistenceUnit);
        em.getTransaction().begin();
        em.persist(actor);
        em.getTransaction().commit();
        closeEntityManager();
    }    

    public Object find(Class entityClass, Object key)
    {
        em = getEntityManager(persistenceUnit);
        em.clear();
        Object actor = em.find(entityClass, key);
        return actor;
    }

    public void insertActors(Set<?> actors)
    {
        EntityManager em = getEntityManager(persistenceUnit);
        em.getTransaction().begin();
        for (Object actor : actors)
        {
            em.persist(actor);
        }
        em.getTransaction().commit();
        closeEntityManager();
    }

    public void remove(Object pKey, Class clazz)
    {
        em = getEntityManager(persistenceUnit);        
        Object obj = em.find(clazz, pKey);
        em.getTransaction().begin();
        em.remove(obj);
        em.getTransaction().commit();
        closeEntityManager();
    }

    public void merge(Object modifiedObj)
    {
        em = getEntityManager(persistenceUnit);
        em.getTransaction().begin();
        em.merge(modifiedObj);
        em.getTransaction().commit();
        closeEntityManager();
    } 
    

}
