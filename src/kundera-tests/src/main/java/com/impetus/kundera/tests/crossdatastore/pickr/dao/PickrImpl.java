/**
 * Copyright 2012 Impetus Infotech.
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
package com.impetus.kundera.tests.crossdatastore.pickr.dao;

import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

/**
 * Implementation class for Pickr functionality
 * 
 * @author amresh.singh
 */

public class PickrImpl implements Pickr
{

    EntityManagerFactory emf;

    EntityManager em;

    public PickrImpl(String persistenceUnitName)
    {
        if (emf == null)
        {
            emf = Persistence.createEntityManagerFactory(persistenceUnitName);
        }
    }

    @Override
    public void addPhotographer(Object p)
    {
        closeEntityManager();
        EntityManager em = getEntityManager();
        em.persist(p);
        closeEntityManager();
    }

    @Override
    public Object getPhotographer(Class<?> entityClass, Integer photographerId)
    {
        EntityManager em = getEntityManager();
        Object p = em.find(entityClass, photographerId);
        return p;
    }

    @Override
    public List<Object> getAllPhotographers(String className)
    {
        EntityManager em = getEntityManager();
        Query q = em.createQuery("select p from " + className + " p");
        List<Object> photographers = q.getResultList();
        //closeEntityManager();
        return photographers;
    }

    @Override
    public void deletePhotographer(Object p)
    {
        EntityManager em = getEntityManager();
        em.remove(p);
        closeEntityManager();
    }

    @Override
    public void mergePhotographer(Object p)
    {
        EntityManager em = getEntityManager();
        em.merge(p);
        closeEntityManager();
    }

    EntityManager getEntityManager()
    {
        if (em == null)
        {
            em = emf.createEntityManager();
        }
        return em;
    }

    void closeEntityManager()
    {
        if (em != null)
        {
            em.close();
            em = null;
        }
    }

    @Override
    public void close()
    {
        closeEntityManager();
        emf.close();
    }

}