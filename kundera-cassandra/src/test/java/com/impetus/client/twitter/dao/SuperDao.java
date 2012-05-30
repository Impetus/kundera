/**
 * 
 */
package com.impetus.client.twitter.dao;

import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

/**
 * The Class SuperDao.
 * 
 * @author impetus
 */
public class SuperDao
{

    /**
     * Inits the.
     * 
     * @param persistenceUnitName
     *            the persistence unit name
     * @return the entity manager
     * @throws Exception
     *             the exception
     */
    protected EntityManagerFactory createEntityManagerFactory(String persistenceUnitName)
    {
        return Persistence.createEntityManagerFactory(persistenceUnitName);

    }
}
