package com.impetus.kundera.datakeeper.dao;

import java.util.List;

import javax.persistence.EntityManager;

/**
 * @author Kuldeep.Mishra
 * 
 */
public interface DataKeeperDao
{
    EntityManager getEntityManager();

    void closeEntityManager();

    void clearEntityManager();

    void shutDown();

    void insert(Object entity);

    void merge(Object entity);

    void remove(Object entity);

    <T> T findById(Class<T> entityClazz, Object id);

    List<?> findByQuery(String Query);

    List<?> findByQuery(String queryString, String paramater, Object parameterValue);
}
