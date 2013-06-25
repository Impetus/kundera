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
package com.impetus.kundera.polyglot.dao;

import java.util.List;
import java.util.Set;

import javax.persistence.EntityManager;
import javax.persistence.Query;

public class PersonAddressDaoImpl extends BaseDao
{
    private String persistenceUnit;

    public PersonAddressDaoImpl(String pu)
    {
        this.persistenceUnit = pu;
    }

    public Query createQuery(String query)
    {
        EntityManager em = getEntityManager(persistenceUnit);
        Query q = em.createQuery(query);
        return q;
    }

    public void insert(Object person)
    {
        em = getEntityManager(persistenceUnit);
        em.persist(person);
        closeEntityManager();
    }

    public void update(Object obj)
    {
        em.merge(obj);
        closeEntityManager();
    }

    public Object findPerson(Class entityClass, Object personId)
    {
        em = getEntityManager(persistenceUnit);
        Object personnel = em.find(entityClass, personId);
        return personnel;
    }

    public void savePersons(Set<?> personnels)
    {
        EntityManager em = getEntityManager(persistenceUnit);

        for (Object personnel : personnels)
        {
            em.persist(personnel);
        }

        closeEntityManager();
    }

    public void remove(Object pKey, Class clazz)
    {
        em = getEntityManager(persistenceUnit);
        Object obj = em.find(clazz, pKey);
        em.remove(obj);
        closeEntityManager();
    }

    public void merge(Object modifiedObj)
    {
        em = getEntityManager(persistenceUnit);
        em.merge(modifiedObj);
        closeEntityManager();

    }

    public List<?> getAllPersons(String className)
    {
        EntityManager em = getEntityManager(persistenceUnit);
        Query q = em.createQuery("select p from " + className + " p");
        List<?> persons = q.getResultList();
        closeEntityManager();
        return persons;
    }

    public Object findPersonByIdColumn(Class entityClass, Object personId)
    {
        EntityManager em = getEntityManager(persistenceUnit);
        String query = "Select p from " + entityClass.getSimpleName() + " p where p.personId = " + personId;
        Query q = em.createQuery(query);
        List persons = q.getResultList();
        //closeEntityManager();
        assert persons != null;
        assert !persons.isEmpty();
        assert persons.size() == 1;

        return persons.get(0);
    }

    public List findPersonByName(Class entityClass, String personName)
    {
        EntityManager em = getEntityManager(persistenceUnit);
        String query = "Select p from " + entityClass.getSimpleName() + " p where p.personName = " + personName;
        Query q = em.createQuery(query);
        List persons = q.getResultList();
        //closeEntityManager();

        return persons;
    }

    public Object findAddressByIdColumn(Class entityClass, Object addressId)
    {
        EntityManager em = getEntityManager(persistenceUnit);
        String query = "Select a from " + entityClass.getSimpleName() + " a where a.addressId = " + addressId;
        Query q = em.createQuery(query);
        List addresses = q.getResultList();
        closeEntityManager();
        assert addresses != null;
        assert !addresses.isEmpty();
        assert addresses.size() == 1;

        return addresses.get(0);
    }

    public List findAddressByStreet(Class entityClass, String street)
    {
        EntityManager em = getEntityManager(persistenceUnit);
        String query = "Select a from " + entityClass.getSimpleName() + " a where a.street = " + street;
        Query q = em.createQuery(query);
        List addresses = q.getResultList();
        closeEntityManager();

        return addresses;
    }

}