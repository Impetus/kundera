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

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import junit.framework.Assert;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.impetus.client.hbase.testingutil.HBaseTestingUtils;

/**
 * The Class HBaseTransactionTest.
 * 
 * @author Devender Yadav
 */
public class HBaseTransactionTest
{

    /** The Constant HBASE_PU. */
    private static final String HBASE_PU = "transactionTest";

    /** The Constant SCHEMA. */
    private static final String SCHEMA = "HBaseNew";

    /** The emf. */
    private static EntityManagerFactory emf;

    /** The em. */
    private EntityManager em;

    /**
     * Sets the up before class.
     * 
     * @throws Exception
     *             the exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception
    {
        emf = Persistence.createEntityManagerFactory(HBASE_PU);
    }

    /**
     * Sets the up.
     * 
     * @throws Exception
     *             the exception
     */
    @Before
    public void setUp() throws Exception
    {
        em = emf.createEntityManager();
    }

    /**
     * On commit.
     * 
     * @throws Exception
     *             the exception
     */
    @Test
    public void onCommit() throws Exception
    {
        em.getTransaction().begin();
        persistUsers();
        em.getTransaction().commit();

        em.clear();
        User user = em.find(User.class, "1");
        Assert.assertNotNull(user);
        Assert.assertEquals("1", user.getId());
        Assert.assertEquals("dev", user.getName());
        Assert.assertEquals(20, user.getAge());

        user = em.find(User.class, "2");
        Assert.assertNotNull(user);
        Assert.assertEquals("2", user.getId());
        Assert.assertEquals("amit", user.getName());
        Assert.assertEquals(25, user.getAge());

        user = em.find(User.class, "3");
        Assert.assertNotNull(user);
        Assert.assertEquals("3", user.getId());
        Assert.assertEquals("vivek", user.getName());
        Assert.assertEquals(30, user.getAge());

        em.getTransaction().begin();
        user = em.find(User.class, "1");
        user.setName("pragalbh");
        em.merge(user);
        em.getTransaction().commit();

        Assert.assertEquals("pragalbh", user.getName());

    }

    /**
     * On rollback after persist.
     * 
     * @throws Exception
     *             the exception
     */
    @Test
    public void onRollbackAfterPersist() throws Exception
    {

        em.getTransaction().begin();
        persistUsers();
        em.getTransaction().rollback();
        em.clear();
        User user = em.find(User.class, "1");
        Assert.assertNull(user);
        user = em.find(User.class, "2");
        Assert.assertNull(user);
        user = em.find(User.class, "3");
        Assert.assertNull(user);

    }

    /**
     * On rollback after merge.
     * 
     * @throws Exception
     *             the exception
     */
    @Test
    public void onRollbackAfterMerge() throws Exception
    {

        em.getTransaction().begin();
        persistUsers();
        em.getTransaction().commit();
        em.clear();

        em.getTransaction().begin();
        User user = em.find(User.class, "1");
        user.setName("pragalbh");
        em.merge(user);
        em.getTransaction().rollback();
        em.clear();

        user = em.find(User.class, "1");
        Assert.assertEquals("dev", user.getName());
        Assert.assertNotSame("pragalbh", user.getName());
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
            persistUsers();
            em.merge(null);
            em.getTransaction().commit();
        }
        catch (Exception ex)
        {
            em.clear();
            User user = em.find(User.class, "1");
            Assert.assertNull(user);
            user = em.find(User.class, "2");
            Assert.assertNull(user);
            user = em.find(User.class, "3");
            Assert.assertNull(user);
        }
    }

    /**
     * Roll back with multi transactions.
     */
    @Test
    public void rollBackWithMultiTransactions()
    {
        em.getTransaction().begin();
        persistUsers();
        em.getTransaction().commit();
        em.clear();

        EntityManager em1 = emf.createEntityManager();

        User user = em1.find(User.class, "1");
        em1.getTransaction().begin();
        user.setName("pragalbh");
        em1.merge(user);
        em1.getTransaction().commit();

        EntityManager em2 = emf.createEntityManager();
        user = em2.find(User.class, "1");
        Assert.assertEquals("pragalbh", user.getName());
        user.setName("karthik");
        try
        {
            em2.merge(user);
            em2.merge(null);
        }
        catch (Exception ex)
        {
            EntityManager em3 = emf.createEntityManager();
            User userNew = em3.find(User.class, "1");
            Assert.assertNotNull(userNew);
            Assert.assertEquals("pragalbh", userNew.getName());
        }
    }

    /**
     * Prepare data.
     * 
     * @param id
     *            the id
     * @param name
     *            the name
     * @param age
     *            the age
     * @return the user
     */
    private User prepareData(String id, String name, int age)
    {
        User user = new User();
        user.setId(id);
        user.setName(name);
        user.setAge(age);
        return user;
    }

    /**
     * Persist users.
     */
    private void persistUsers()
    {
        Object user1 = prepareData("1", "dev", 20);
        Object user2 = prepareData("2", "amit", 25);
        Object user3 = prepareData("3", "vivek", 30);
        em.persist(user1);
        em.persist(user2);
        em.persist(user3);
    }

    /**
     * Tear down.
     * 
     * @throws Exception
     *             the exception
     */
    @After
    public void tearDown() throws Exception
    {
        String deleteQuery = "Delete from User u";
        em.createQuery(deleteQuery).executeUpdate();
        em.close();
    }

    /**
     * Tear down after class.
     * 
     * @throws Exception
     *             the exception
     */
    @AfterClass
    public static void tearDownAfterClass() throws Exception
    {
        emf.close();
        emf = null;
        HBaseTestingUtils.dropSchema(SCHEMA);
    }
}
