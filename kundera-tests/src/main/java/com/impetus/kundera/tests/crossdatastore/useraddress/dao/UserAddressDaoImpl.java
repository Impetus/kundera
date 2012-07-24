package com.impetus.kundera.tests.crossdatastore.useraddress.dao;

import java.util.List;
import java.util.Set;

import javax.persistence.EntityManager;
import javax.persistence.Query;

public class UserAddressDaoImpl extends BaseDao
{
    private String persistenceUnit;

    public UserAddressDaoImpl(String pu)
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

    public Object findPerson(Class entityClass, String personId)
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
        return persons;
    }

}