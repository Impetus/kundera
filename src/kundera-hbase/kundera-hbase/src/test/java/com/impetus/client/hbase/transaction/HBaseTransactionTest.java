/*******************************************************************************
 * * Copyright 2014 Impetus Infotech.
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
package com.impetus.client.hbase.transaction;

import java.io.IOException;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;
import javax.xml.registry.InvalidRequestException;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.hbase.config.HBaseUser;
/**
 * @author Chhavi Gangwal
 * 
 */
public class HBaseTransactionTest{
	
  
    /** The Constant HBASE_PU. */
    private static final String HBASE_PU = "XmlPropertyTest";

    /** The emf. */
    private EntityManagerFactory emf;

    /** The em. */
    private EntityManager em;

    /** The logger. */
    private static Logger logger = LoggerFactory.getLogger(HBaseTransactionTest.class);

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        emf = Persistence.createEntityManagerFactory(HBASE_PU);
        em = emf.createEntityManager();
    }

    /**
     * On rollback.
     * 
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     * @throws TException
     *             the t exception
     * @throws InvalidRequestException
     *             the invalid request exception
     * @throws UnavailableException
     *             the unavailable exception
     * @throws TimedOutException
     *             the timed out exception
     * @throws SchemaDisagreementException
     *             the schema disagreement exception
     */
    @Test
    public void onRollback() throws Exception
    {
        em.getTransaction().begin();

        Object p1 = prepareData("test1", 10);
        Object p2 = prepareData("test2", 20);
        Object p3 = prepareData("test3", 15);
        em.persist(p1);
        em.persist(p2);
        em.persist(p3);

        // roll back.
        em.getTransaction().rollback();
        HBaseUser p = em.find(HBaseUser.class, "test1");
        Assert.assertNull(p);

        // on commit.

        // Still no record should be flushed as already rollback!
        p = em.find(HBaseUser.class, "test1");
        Assert.assertNull(p);
    }

    /**
     * On commit.
     * 
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     * @throws TException
     *             the t exception
     * @throws InvalidRequestException
     *             the invalid request exception
     * @throws UnavailableException
     *             the unavailable exception
     * @throws TimedOutException
     *             the timed out exception
     * @throws SchemaDisagreementException
     *             the schema disagreement exception
     */
    @Test
    public void onCommit() throws Exception
    {
        // em.setFlushMode(FlushModeType.COMMIT);

        em.getTransaction().begin();

        Object p1 = prepareData("test1", 10);
        Object p2 = prepareData("test2", 20);
        Object p3 = prepareData("test3", 15);
        em.persist(p1);
        em.persist(p2);
        em.persist(p3);

        // on commit.
        em.getTransaction().commit();

        em.getTransaction().begin();

        HBaseUser p = em.find(HBaseUser.class, "test1");
        Assert.assertNotNull(p);

        ((HBaseUser) p2).setAddress("rollback");
        em.merge(p2);

        // roll back, should roll back person name for p2!
        em.getTransaction().rollback();

        p = em.find(HBaseUser.class, "test1");
        Assert.assertNotNull(p);

        p = em.find(HBaseUser.class, "test2");
        Assert.assertNotNull(p);
        Assert.assertEquals("test2", p.getName());
        Assert.assertNotSame("rollback", p.getAddress());

        em.getTransaction().begin();
        em.merge(p2);
        em.getTransaction().commit();
        
        p = em.find(HBaseUser.class, "test2");
        Assert.assertNotNull(p);
        Assert.assertEquals("test2", p.getName());
        Assert.assertEquals("rollback", p.getAddress());
    }

    /**
     * Rollback on error.
     * 
     * @throws Exception
     *             the exception
     */
    @Test
    public void rollbackOnError() throws Exception
    {
        try
        {
            em.getTransaction().begin();
            Object p1 = prepareData("test1", 10);
            Object p2 = prepareData("test2", 20);
            Object p3 = prepareData("test3", 15);
            em.persist(p1);
            em.persist(p2);
            em.persist(p3);
            em.merge(null);
            em.getTransaction().commit();
        }
        catch (Exception ex)
        {
            em.clear();
            HBaseUser user = em.find(HBaseUser.class, "test1");
            Assert.assertNull(user);
            user = em.find(HBaseUser.class, "test2");
            Assert.assertNull(user);
            user = em.find(HBaseUser.class, "test3");
            Assert.assertNull(user);
        }
    }


    /**
     * Roll back with multi transactions.
     */
    @Test
    public void rollBackWithMultiTransactions()
    {
        EntityManager em1 = emf.createEntityManager();
        // em1.setFlushMode(FlushModeType.COMMIT);

        // Begin transaction.
        em1.getTransaction().begin();
        Object p1 = prepareData("test11", 10);
        em1.persist(p1);

        // commit p1.
        em1.getTransaction().commit();

        // another em instance
        EntityManager em2 = emf.createEntityManager();
        // em2.setFlushMode(FlushModeType.COMMIT);

        // begin transaction.
        HBaseUser found = em2.find(HBaseUser.class, "test11");

        em2.getTransaction().begin();
        found.setAddress("merged");
        em2.merge(found);

        // // commit p1 after modification.
        em2.getTransaction().commit();

        // open another entity manager.
        EntityManager em3 = emf.createEntityManager();
        found = em3.find(HBaseUser.class, "test11");
        Assert.assertEquals("merged", found.getAddress());
        found.setAddress("lastemerge");
        try
        {
            em3.merge(found);
            em3.merge(null);
        }
        catch (Exception ex)
        {
            HBaseUser finalFound = em2.find(HBaseUser.class, "test11");
            Assert.assertNotNull(finalFound);
            Assert.assertEquals("merged", finalFound.getAddress());
        }
    }

    /**
     * Prepare data.
     * 
     * @param rowKey
     *            the row key
     * @param age
     *            the age
     * @return the person
     */
    private HBaseUser prepareData(String id,  int age){
    	
        HBaseUser user = new HBaseUser();
        user.setName(id);
        user.setAge(age);
        user.setAddress("D-40");
        return user;
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception{
    	
        EntityManager em = emf.createEntityManager();

        // Delete by query.
        String deleteQuery = "Delete from HBaseUser p";
        Query query = em.createQuery(deleteQuery);
        int updateCount = query.executeUpdate();

        em.close();
        emf.close();
        emf = null;
    }

}
