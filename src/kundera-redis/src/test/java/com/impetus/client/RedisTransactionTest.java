package com.impetus.client;

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

import com.impetus.client.entities.PersonRedis;

public class RedisTransactionTest
{

    private static final String ROW_KEY = "1";

    /** The Constant REDIS_PU. */
    private static final String REDIS_PU = "redis_pu";

    /** The emf. */
    private EntityManagerFactory emf;

    /** The em. */
    private EntityManager em;

    /** The logger. */
    private static Logger logger = LoggerFactory.getLogger(RedisTransactionTest.class);

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        emf = Persistence.createEntityManagerFactory(REDIS_PU);
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
        purge();
        em.getTransaction().begin();

        Object p1 = prepareData("1", 10);
        Object p2 = prepareData("2", 20);
        Object p3 = prepareData("3", 15);
        em.persist(p1);
        em.persist(p2);
        em.persist(p3);

        // roll back.
        em.getTransaction().rollback();
        PersonRedis p = em.find(PersonRedis.class, "1");
        Assert.assertNull(p);

        // on commit.

        // Still no record should be flushed as already rollback!
        p = em.find(PersonRedis.class, "1");
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
        purge();
        // em.setFlushMode(FlushModeType.COMMIT);

        em.getTransaction().begin();

        Object p1 = prepareData("1", 10);
        Object p2 = prepareData("2", 20);
        Object p3 = prepareData("3", 15);
        em.persist(p1);
        em.persist(p2);
        em.persist(p3);

        // on commit.
        em.getTransaction().commit();

        em.getTransaction().begin();

        PersonRedis p = em.find(PersonRedis.class, "1");
        Assert.assertNotNull(p);

        ((PersonRedis) p2).setPersonName("rollback");
        em.merge(p2);

        // roll back, should roll back person name for p2!
        em.getTransaction().rollback();

        p = em.find(PersonRedis.class, "1");
        Assert.assertNotNull(p);

        p = em.find(PersonRedis.class, "2");
        Assert.assertNotNull(p);
        Assert.assertEquals("vivek", p.getPersonName());
        Assert.assertNotSame("rollback", p.getPersonName());

        em.getTransaction().begin();
        em.getTransaction().commit();
    }

    /**
     * Rollback on error.
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
    public void rollbackOnError() throws Exception
    {
        purge();
        PersonRedis p = null;
        try
        {
            Object p1 = prepareData("1", 10);
            Object p2 = prepareData("2", 20);
            em.persist(p1);
            em.persist(p2);

            p = em.find(PersonRedis.class, "1");
            Assert.assertNotNull(p);

            Object p3 = prepareData("3", 15);
            em.persist(p3);

            // Assert on rollback on error.
            ((PersonRedis) p2).setPersonName("rollback");
            em.merge(p2);
            em.merge(null);

            // As this is a runtime exception so rollback should happen and
            // delete out commited data.
        }
        catch (Exception ex)
        {
            em.clear();
            p = em.find(PersonRedis.class, "1");
            Assert.assertNull(p);

            p = em.find(PersonRedis.class, "2");
            Assert.assertNull(p);

            p = em.find(PersonRedis.class, "3");
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
            PersonRedis person = em2.find(PersonRedis.class, "4");
            em2.remove(person);
            em2.merge(null);
        }
        catch (Exception ex)
        {
            em1.clear();
            p = em.find(PersonRedis.class, "4");
            Assert.assertNotNull(p);
            Assert.assertEquals("vivek", p.getPersonName());

        }
    }

    /**
     * Roll back with multi transactions.
     */
    @Test
    public void rollBackWithMultiTransactions()
    {
        purge();
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
        PersonRedis found = em2.find(PersonRedis.class, "11");

        em2.getTransaction().begin();
        found.setPersonName("merged");
        em2.merge(found);

        // // commit p1 after modification.
        em2.getTransaction().commit();

        // open another entity manager.
        EntityManager em3 = emf.createEntityManager();
        found = em3.find(PersonRedis.class, "11");
        found.setPersonName("lastemerge");
        try
        {
            em3.merge(found);
            em3.merge(null);
        }
        catch (Exception ex)
        {
            PersonRedis finalFound = em2.find(PersonRedis.class, "11");
            Assert.assertNotNull(finalFound);
            Assert.assertEquals("merged", finalFound.getPersonName());
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
    private PersonRedis prepareData(String rowKey, int age)
    {
        PersonRedis o = new PersonRedis();
        o.setPersonId(rowKey);
        o.setPersonName("vivek");
        o.setAge(age);
        return o;
    }
    
    private void purge()
    {
        // Delete by query.
        String deleteQuery = "Delete from PersonRedis p";
        Query query = em.createQuery(deleteQuery);
        int updateCount = query.executeUpdate();
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
        
        purge();

        em.close();
        emf.close();
        emf = null;
    }

}
