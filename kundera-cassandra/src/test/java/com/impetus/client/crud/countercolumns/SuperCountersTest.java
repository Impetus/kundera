/**
 * 
 */
package com.impetus.client.crud.countercolumns;

import java.io.IOException;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.persistence.CassandraCli;
import com.impetus.kundera.KunderaException;

/**
 * @author impadmin
 * 
 */
public class SuperCountersTest
{
    private static final String id1 = "12";

    private static final String id2 = "15";

    private static final String id3 = "18";

    private EntityManagerFactory emf;

    private static final boolean RUN_IN_EMBEDDED_MODE = true;

    private static final boolean AUTO_MANAGE_SCHEMA = true;

    private String keyspace = "KunderaCounterColumn";

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        if (RUN_IN_EMBEDDED_MODE)
        {
            startServer();
        }

        if (AUTO_MANAGE_SCHEMA)
        {
            createSchema();
        }
        emf = Persistence.createEntityManagerFactory("CassandraCounterTest");
    }

    private void createSchema() throws InvalidRequestException, TException, SchemaDisagreementException
    {
        Client client = CassandraCli.getClient();
        CassandraCli.createKeySpace(keyspace);
        client.set_keyspace(keyspace);

        CfDef cfDef = new CfDef();
        cfDef.keyspace = keyspace;
        cfDef.name = "SuperCounters";
        cfDef.column_type = "Super";
        cfDef.default_validation_class = "CounterColumnType";
        cfDef.comparator_type = "UTF8Type";
        client.system_add_column_family(cfDef);
    }

    private void startServer() throws IOException, TException, InvalidRequestException, UnavailableException,
            TimedOutException, SchemaDisagreementException
    {
        CassandraCli.cassandraSetUp();
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
        if (/* AUTO_MANAGE_SCHEMA && CassandraCli.keyspaceExist(keyspace) */CassandraCli.client != null)
        {
            CassandraCli.client.system_drop_keyspace(keyspace);
        }
        emf.close();
    }

    @Test
    public void testCRUDOnSuperCounter()
    {
        incrSuperCounter();
        findSuperCounter();
        decrSuperCounter();
        selectAllQuery();
        queryGTEQOnId();
        queryLTEQOnId();
        rangeQuery();
        mergeCounter();
        updateNamedQueryOnCounter();
        deleteNamedQueryOnCounter();
        deleteSuperCounter();
    }

    private void deleteNamedQueryOnCounter()
    {
        EntityManager em = emf.createEntityManager();
        String deleteQuery = "Delete From SuperCounters c where c.id <= " + id2;

        Query q = em.createQuery(deleteQuery);
        q.executeUpdate();

        SuperCounters counter2 = new SuperCounters();
        counter2 = em.find(SuperCounters.class, id1);
        Assert.assertNull(counter2);

        SuperCounters counter3 = new SuperCounters();
        counter3 = em.find(SuperCounters.class, id2);
        Assert.assertNull(counter3);

        em.close();

    }

    private void updateNamedQueryOnCounter()
    {
        EntityManager em = emf.createEntityManager();
        String updateQuery = "Update SuperCounters c SET c.counter=23 where c.id=12";
        Query q = em.createQuery(updateQuery);

        try
        {
            q.executeUpdate();
        }
        catch (UnsupportedOperationException uoe)
        {
            Assert.assertEquals(" Merge is not permitted on counter column! ", uoe.getMessage());
        }
        finally
        {
            em.close();
        }

    }

    private void mergeCounter()
    {
        EntityManager em = emf.createEntityManager();
        SuperCounters counter = new SuperCounters();
        counter = em.find(SuperCounters.class, id1);
        Assert.assertNotNull(counter);
        Assert.assertNotNull(counter.getCounter());
        try
        {
            em.merge(counter);
        }
        catch (KunderaException ke)
        {
            Assert.assertEquals("java.lang.UnsupportedOperationException:  Merge is not permitted on counter column! ",
                    ke.getMessage());
        }
        finally
        {
            em.close();
        }

    }

    private void rangeQuery()
    {

        EntityManager em = emf.createEntityManager();
        Query q = em.createQuery("select c from SuperCounters c where c.id between 12 and 15");
        List<SuperCounters> r = q.getResultList();
        Assert.assertNotNull(r);
        Assert.assertEquals(2, r.size());
        int count = 0;
        for (SuperCounters counter : r)
        {
            if (id1.equalsIgnoreCase(counter.getId()))
            {
                count++;
                Assert.assertNotNull(counter);
                Assert.assertNotNull(counter.getCounter());
                Assert.assertEquals(2, counter.getCounter());
                Assert.assertEquals(2223, counter.getlCounter());
                Assert.assertNotNull(counter.getSubCounter());
                Assert.assertEquals(3, counter.getSubCounter().getSubCounter());
            }
            else
            {
                count++;
                Assert.assertNotNull(counter);
                Assert.assertEquals(id2, counter.getId());
                Assert.assertNotNull(counter.getCounter());
                Assert.assertEquals(5, counter.getCounter());
                Assert.assertEquals(2224, counter.getlCounter());
                Assert.assertNotNull(counter.getSubCounter());
                Assert.assertEquals(3, counter.getSubCounter().getSubCounter());
            }
        }
        Assert.assertEquals(2, count);
    }

    private void queryLTEQOnId()
    {

        EntityManager em = emf.createEntityManager();
        Query q = em.createQuery("select c from SuperCounters c where c.id <= 15");
        List<SuperCounters> r = q.getResultList();
        Assert.assertNotNull(r);
        Assert.assertEquals(2, r.size());
        int count = 0;
        for (SuperCounters counter : r)
        {
            if (id1.equalsIgnoreCase(counter.getId()))
            {
                count++;
                Assert.assertNotNull(counter);
                Assert.assertNotNull(counter.getCounter());
                Assert.assertEquals(2, counter.getCounter());
                Assert.assertEquals(2223, counter.getlCounter());
                Assert.assertNotNull(counter.getSubCounter());
                Assert.assertEquals(3, counter.getSubCounter().getSubCounter());
            }
            else
            {
                count++;
                Assert.assertNotNull(counter);
                Assert.assertEquals(id2, counter.getId());
                Assert.assertNotNull(counter.getCounter());
                Assert.assertEquals(5, counter.getCounter());
                Assert.assertEquals(2224, counter.getlCounter());
                Assert.assertNotNull(counter.getSubCounter());
                Assert.assertEquals(3, counter.getSubCounter().getSubCounter());
            }
        }
        Assert.assertEquals(2, count);
    }

    private void queryGTEQOnId()
    {
        EntityManager em = emf.createEntityManager();
        Query q = em.createQuery("select c from SuperCounters c where c.id >= 15");
        List<SuperCounters> r = q.getResultList();
        Assert.assertNotNull(r);
        Assert.assertEquals(2, r.size());
        int count = 0;
        for (SuperCounters counter : r)
        {
            if (id2.equalsIgnoreCase(counter.getId()))
            {
                count++;
                Assert.assertNotNull(counter);
                Assert.assertNotNull(counter.getCounter());
                Assert.assertEquals(5, counter.getCounter());
                Assert.assertEquals(2224, counter.getlCounter());
                Assert.assertNotNull(counter.getSubCounter());
                Assert.assertEquals(3, counter.getSubCounter().getSubCounter());
            }
            else
            {
                count++;
                Assert.assertNotNull(counter);
                Assert.assertEquals(id3, counter.getId());
                Assert.assertNotNull(counter.getCounter());
                Assert.assertEquals(8, counter.getCounter());
                Assert.assertEquals(2225, counter.getlCounter());
                Assert.assertNotNull(counter.getSubCounter());
                Assert.assertEquals(3, counter.getSubCounter().getSubCounter());
            }
        }
        Assert.assertEquals(2, count);
    }

    private void deleteSuperCounter()
    {
        EntityManager em = emf.createEntityManager();
        SuperCounters counter = new SuperCounters();
        counter = em.find(SuperCounters.class, id3);
        Assert.assertNotNull(counter);
        Assert.assertNotNull(counter.getCounter());
        Assert.assertNotNull(counter.getSubCounter());
        Assert.assertEquals(8, counter.getCounter());
        Assert.assertEquals(2225, counter.getlCounter());
        Assert.assertEquals(3, counter.getSubCounter().getSubCounter());
        em.remove(counter);

        EntityManager em1 = emf.createEntityManager();
        counter = em1.find(SuperCounters.class, id3);
        Assert.assertNull(counter);

        em.close();

    }

    private void findSuperCounter()
    {

        EntityManager em = emf.createEntityManager();
        SuperCounters superCounter = new SuperCounters();
        superCounter = em.find(SuperCounters.class, id1);
        Assert.assertNotNull(superCounter);
        Assert.assertNotNull(superCounter.getCounter());
        Assert.assertNotNull(superCounter.getSubCounter());
        Assert.assertEquals(12, superCounter.getCounter());
        Assert.assertEquals(12223, superCounter.getlCounter());
        Assert.assertEquals(23, superCounter.getSubCounter().getSubCounter());

        SuperCounters superCounter1 = new SuperCounters();
        superCounter1 = em.find(SuperCounters.class, id2);
        Assert.assertNotNull(superCounter1);
        Assert.assertNotNull(superCounter1.getCounter());
        Assert.assertNotNull(superCounter1.getSubCounter());
        Assert.assertEquals(15, superCounter1.getCounter());
        Assert.assertEquals(12224, superCounter1.getlCounter());
        Assert.assertEquals(23, superCounter1.getSubCounter().getSubCounter());

        SuperCounters superCounter2 = new SuperCounters();
        superCounter2 = em.find(SuperCounters.class, id3);
        Assert.assertNotNull(superCounter2);
        Assert.assertNotNull(superCounter2.getCounter());
        Assert.assertNotNull(superCounter2.getSubCounter());
        Assert.assertEquals(18, superCounter2.getCounter());
        Assert.assertEquals(12225, superCounter2.getlCounter());
        Assert.assertEquals(23, superCounter2.getSubCounter().getSubCounter());

        em.close();
    }

    private void incrSuperCounter()
    {

        EntityManager em = emf.createEntityManager();
        SuperCounters superCounter = new SuperCounters();
        superCounter.setCounter(12);
        superCounter.setId(id1);
        superCounter.setlCounter(12223);
        SubCounter subCounter = new SubCounter();
        subCounter.setSubCounter(23);
        superCounter.setSubCounter(subCounter);
        em.persist(superCounter);

        SuperCounters superCounter1 = new SuperCounters();
        superCounter1.setCounter(15);
        superCounter1.setId(id2);
        superCounter1.setlCounter(12224);
        SubCounter subCounter1 = new SubCounter();
        subCounter1.setSubCounter(23);
        superCounter1.setSubCounter(subCounter1);
        em.persist(superCounter1);

        SuperCounters superCounter2 = new SuperCounters();
        superCounter2.setCounter(18);
        superCounter2.setId(id3);
        superCounter2.setlCounter(12225);
        SubCounter subCounter2 = new SubCounter();
        subCounter2.setSubCounter(23);
        superCounter2.setSubCounter(subCounter2);
        em.persist(superCounter2);

        em.close();

    }

    private void decrSuperCounter()
    {
        EntityManager em = emf.createEntityManager();
        SuperCounters superCounter = new SuperCounters();
        superCounter.setCounter(-10);
        superCounter.setId(id1);
        superCounter.setlCounter(-10000);
        SubCounter subCounter = new SubCounter();
        subCounter.setSubCounter(-20);
        superCounter.setSubCounter(subCounter);
        em.persist(superCounter);

        SuperCounters superCounter1 = new SuperCounters();
        superCounter1.setCounter(-10);
        superCounter1.setId(id2);
        superCounter1.setlCounter(-10000);
        SubCounter subCounter1 = new SubCounter();
        subCounter1.setSubCounter(-20);
        superCounter1.setSubCounter(subCounter1);
        em.persist(superCounter1);

        SuperCounters superCounter2 = new SuperCounters();
        superCounter2.setCounter(-10);
        superCounter2.setId(id3);
        superCounter2.setlCounter(-10000);
        SubCounter subCounter2 = new SubCounter();
        subCounter2.setSubCounter(-20);
        superCounter2.setSubCounter(subCounter2);
        em.persist(superCounter2);

        em.close();

    }

    public void selectAllQuery()
    {
        EntityManager em = emf.createEntityManager();
        Query q = em.createQuery("select c from SuperCounters c");
        List<SuperCounters> r = q.getResultList();
        Assert.assertNotNull(r);
        Assert.assertEquals(3, r.size());
        int count = 0;
        for (SuperCounters counter : r)
        {
            if (id1.equalsIgnoreCase(counter.getId()))
            {
                count++;
                Assert.assertNotNull(counter);
                Assert.assertNotNull(counter.getCounter());
                Assert.assertEquals(2, counter.getCounter());
                Assert.assertEquals(2223, counter.getlCounter());
                Assert.assertNotNull(counter.getSubCounter());
                Assert.assertEquals(3, counter.getSubCounter().getSubCounter());
            }
            else if (id2.equalsIgnoreCase(counter.getId()))
            {
                count++;
                Assert.assertNotNull(counter);
                Assert.assertNotNull(counter.getCounter());
                Assert.assertEquals(5, counter.getCounter());
                Assert.assertEquals(2224, counter.getlCounter());
                Assert.assertNotNull(counter.getSubCounter());
                Assert.assertEquals(3, counter.getSubCounter().getSubCounter());
            }
            else
            {
                count++;
                Assert.assertNotNull(counter);
                Assert.assertEquals(id3, counter.getId());
                Assert.assertNotNull(counter.getCounter());
                Assert.assertEquals(8, counter.getCounter());
                Assert.assertEquals(2225, counter.getlCounter());
                Assert.assertNotNull(counter.getSubCounter());
                Assert.assertEquals(3, counter.getSubCounter().getSubCounter());
            }
        }
        Assert.assertEquals(3, count);
    }
}
