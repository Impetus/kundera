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
package com.impetus.kundera;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;
import javax.persistence.Persistence;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.client.DummyDatabase;
import com.impetus.kundera.query.Person;

/**
 * Test case for Testing {@link EntityTransaction}
 * 
 * @author amresh.singh
 * 
 */
public class EntityTransactionTest
{

    private EntityManagerFactory emf;

    private EntityManager em;

    @Before
    public void setUp() throws Exception
    {
        emf = Persistence.createEntityManagerFactory("patest");
        em = emf.createEntityManager();
    }

//    @Test
    public void testRollback()
    {

        em.getTransaction().begin();
        Object p1 = prepareData("1", 10);
        Object p2 = prepareData("2", 20);
        Object p3 = prepareData("3", 15);
        em.persist(p1);
        em.persist(p2);
        em.persist(p3);

        // roll back.
        em.getTransaction().rollback();

        em.getTransaction().begin();

        Person p = findById(Person.class, "1", em);
        Assert.assertNull(p);

        em.getTransaction().commit();

        // Still no record should be flushed as already rollback!
        p = findById(Person.class, "1", em);
        Assert.assertNull(p);
    }

    @Test
    public void testCommit()
    {

        em.getTransaction().begin();

        Object p1 = prepareData("1", 10);
        Object p2 = prepareData("2", 20);
        Object p3 = prepareData("3", 15);
        em.persist(p1);
        em.persist(p2);
        em.persist(p3);

        // on commit.
        em.getTransaction().commit();

        Person p = findById(Person.class, "1", em);
        Assert.assertNotNull(p);

        em.getTransaction().begin();

        ((Person) p2).setPersonName("rollback");
        em.merge(p2);

        // roll back, should roll back person name for p2!
        em.getTransaction().rollback();

        p = findById(Person.class, "1", em);
        Assert.assertNotNull(p);

        p = findById(Person.class, "2", em);
        Assert.assertNotNull(p);
        Assert.assertEquals("vivek", p.getPersonName());
        Assert.assertNotSame("rollback", p.getPersonName());
    }

    @Test
    public void testRollbackOnError()
    {
        Person p = null;
        try
        {
            Object p1 = prepareData("1", 10);
            Object p2 = prepareData("2", 20);
            em.persist(p1);
            em.persist(p2);

            p = findById(Person.class, "1", em);
            Assert.assertNotNull(p);

            Object p3 = prepareData("3", 15);
            em.persist(p3);

            // Assert on rollback on error.
            ((Person) p2).setPersonName("rollback");
            em.merge(p2);
            em.merge(null);

            // As this is a runtime exception so rollback should happen and
            // delete out commited data.
        }
        catch (Exception ex)
        {

            p = findById(Person.class, "1", em);
            Assert.assertNull(p);

            p = findById(Person.class, "2", em);
            Assert.assertNull(p);

            p = findById(Person.class, "3", em);
            Assert.assertNull(p);
        }
        em.clear();
        // persist with 1 em
        EntityManager em1 = emf.createEntityManager();
        // em1.setFlushMode(FlushModeType.COMMIT);
        em1.getTransaction().begin();
        Object p3 = prepareData("4", 15);
        em1.persist(p3);
        em1.getTransaction().commit();

        try
        {
            // remove with another em with auto flush.
            EntityManager em2 = emf.createEntityManager();
            Person person = em2.find(Person.class, "4");
            em2.remove(person);
            em2.merge(null);
        }
        catch (Exception ex)
        {
            // Deleted records cannot be rolled back in cassandra!
            // em1.clear();

            p = findById(Person.class, "4", em1);
            Assert.assertNotNull(p);
            Assert.assertEquals("vivek", p.getPersonName());

        }
    }

    /**
     * Roll back with multi transactions.
     */
    @Test
    public void testRollbackWithMultiTransactions()
    {
        EntityManager em1 = emf.createEntityManager();
        // em1.setFlushMode(FlushModeType.COMMIT);

        // Begin transaction.
        em1.getTransaction().begin();
        Object p1 = prepareData("11", 10);
        em1.persist(p1);

        // commit p1.
        em1.getTransaction().commit();

        // another em instance
        EntityManager em2 = emf.createEntityManager();
        // em2.setFlushMode(FlushModeType.COMMIT);

        // begin transaction.
        em2.getTransaction().begin();
        Person found = em2.find(Person.class, "11");
        found.setPersonName("merged");
        em2.merge(found);

        // commit p1 after modification.
        em2.getTransaction().commit();

        // open another entity manager.
        EntityManager em3 = emf.createEntityManager();
        found = em3.find(Person.class, "11");
        found.setPersonName("lastemerge");
        try
        {
            em3.merge(found);
            em3.merge(null);
        }
        catch (Exception ex)
        {
            Person finalFound = em2.find(Person.class, "11");
            Assert.assertNotNull(finalFound);
            Assert.assertEquals("merged", finalFound.getPersonName());
        }
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
        em.close();
        emf.close();
        DummyDatabase.INSTANCE.dropDatabase();
    }

    private Person prepareData(String rowKey, int age)
    {
        Person o = new Person();
        o.setPersonId(rowKey);
        o.setPersonName("vivek");
        o.setAge(age);
        return o;
    }

    private <E extends Object> E findById(Class<E> clazz, Object rowKey, EntityManager em)
    {
        return em.find(clazz, rowKey);
    }

}
