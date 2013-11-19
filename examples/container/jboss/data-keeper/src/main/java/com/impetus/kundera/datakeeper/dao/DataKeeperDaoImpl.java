package com.impetus.kundera.datakeeper.dao;

import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.PersistenceContext;
import javax.persistence.PersistenceContextType;
import javax.persistence.Query;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.impetus.kundera.datakeeper.service.DataKeeperServiceImpl;

/**
 * @author Kuldeep.Mishra
 * 
 */
// @Transactional(propagation = Propagation.REQUIRED)
public class DataKeeperDaoImpl implements DataKeeperDao
{
    /**
     * logger used for logging statement.
     */
    private static final Logger log = LoggerFactory.getLogger(DataKeeperServiceImpl.class);

    @PersistenceContext(unitName = "cassandra-pu,mongo-pu", type = PersistenceContextType.EXTENDED)
    private EntityManager em;

    public DataKeeperDaoImpl()
    {
    }

    @Override
    public void insert(Object entity)
    {
        em.persist(entity);
        em.clear();
    }

    @Override
    public void merge(Object entity)
    {
        em.merge(entity);
        em.clear();
    }

    @Override
    public void remove(Object entity)
    {
        em.remove(entity);
        em.clear();
    }

    @Override
    public <T> T findById(Class<T> entityClazz, Object id)
    {
        T results = em.find(entityClazz, id);
        return results;
    }

    @Override
    public List<?> findByQuery(String queryString)
    {
        log.info(queryString);
        Query query = em.createQuery(queryString);
        List<?> resultList = query.getResultList();
        return resultList;
    }

    @Override
    public List<?> findByQuery(String queryString, String paramater, Object parameterValue)
    {
        Query query = em.createQuery(queryString);
        query.setParameter(paramater, parameterValue);
        log.info(queryString);
        List<?> resultList = query.getResultList();
        return resultList;
    }

    @Override
    public EntityManager getEntityManager()
    {
        return em;
    }

    @Override
    public void closeEntityManager()
    {
        if (em != null)
        {
            em.close();
        }
    }

    @Override
    public void clearEntityManager()
    {
        if (em != null)
        {
            em.clear();
        }
    }

    @Override
    public void shutDown()
    {
        if (em != null)
        {
            em.close();
        }
    }
}
