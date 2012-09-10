package com.impetus.kundera.tests.persistence.lazy;

import static org.junit.Assert.fail;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.PersistenceUnitUtil;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class KunderaPersistenceUnitUtilTest
{
    EntityManagerFactory emf;
    EntityManager em;
    PersistenceUnitUtil util;

    @Before
    public void setUp() throws Exception
    {
        emf = Persistence.createEntityManagerFactory("piccandra");
        em = emf.createEntityManager();
        util = emf.getPersistenceUnitUtil();
    }

    @After
    public void tearDown() throws Exception
    {
        em.close();
        emf.close();
    }

    @Test
    public void testIsLoadedObjectString()
    {
        
    }

    @Test
    public void testIsLoadedObject()
    {

    }

    @Test
    public void testGetIdentifier()
    {

    }

}
