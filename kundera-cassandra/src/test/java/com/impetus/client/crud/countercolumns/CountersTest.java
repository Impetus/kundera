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

import junit.framework.Assert;

import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.persistence.CassandraCli;
import com.impetus.kundera.KunderaException;

/**
 * @author kuldeep.mishra
 * 
 */
public class CountersTest
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
            // createSchema();
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
        cfDef.name = "counters";
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
        if (AUTO_MANAGE_SCHEMA && CassandraCli.keyspaceExist(keyspace))
        {
            CassandraCli.client.system_drop_keyspace(keyspace);
        }
        emf.close();
    }

    @Test
    public void testCRUDOnCounter()
    {
        incrCounter();
        decrCounter();
        findCounter();
        selectAllQuery();
        queryGTEQOnId();
        queryLTEQOnId();
        rangeQuery();
        mergeCounter();
        updateNamedQueryOnCounter();
        deleteNamedQueryOnCounter();
        deleteCounter();
    }

    public void incrCounter()
    {
        EntityManager em = emf.createEntityManager();
        Counters counter = new Counters();
        counter.setCounter(12);
        counter.setId(id1);
        em.persist(counter);

        Counters counter1 = new Counters();
        counter1.setCounter(15);
        counter1.setId(id2);
        em.persist(counter1);

        Counters counter2 = new Counters();
        counter2.setCounter(18);
        counter2.setId(id3);
        em.persist(counter2);

        em.close();
    }

    private void decrCounter()
    {
        EntityManager em = emf.createEntityManager();
        Counters counter1 = new Counters();
        counter1.setCounter(-10);
        counter1.setId(id1);
        em.persist(counter1);

        Counters counter2 = new Counters();
        counter2.setCounter(-10);
        counter2.setId(id2);
        em.persist(counter2);

        Counters counter3 = new Counters();
        counter3.setCounter(-10);
        counter3.setId(id3);
        em.persist(counter3);

        em.close();
    }

    public void findCounter()
    {
        EntityManager em = emf.createEntityManager();
        Counters counter1 = new Counters();
        counter1 = em.find(Counters.class, id1);
        Assert.assertNotNull(counter1);
        Assert.assertNotNull(counter1.getCounter());
        Assert.assertEquals(2, counter1.getCounter());

        Counters counter2 = new Counters();
        counter2 = em.find(Counters.class, id2);
        Assert.assertNotNull(counter2);
        Assert.assertNotNull(counter2.getCounter());
        Assert.assertEquals(5, counter2.getCounter());

        Counters counter3 = new Counters();
        counter3 = em.find(Counters.class, id3);
        Assert.assertNotNull(counter3);
        Assert.assertNotNull(counter3.getCounter());
        Assert.assertEquals(8, counter3.getCounter());

        em.close();
    }

    public void deleteCounter()
    {
        EntityManager em = emf.createEntityManager();
        Counters counters = new Counters();
        counters = em.find(Counters.class, id3);
        Assert.assertNotNull(counters);
        Assert.assertNotNull(counters.getCounter());
        em.remove(counters);

        EntityManager em1 = emf.createEntityManager();
        counters = em1.find(Counters.class, id3);
        Assert.assertNull(counters);

        em.close();
    }

    public void mergeCounter()
    {
        EntityManager em = emf.createEntityManager();
        Counters counters = new Counters();
        counters = em.find(Counters.class, id1);
        Assert.assertNotNull(counters);
        Assert.assertNotNull(counters.getCounter());
        try
        {
            em.merge(counters);
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

    public void selectAllQuery()
    {
        EntityManager em = emf.createEntityManager();
        Query q = em.createQuery("select c from Counters c");
        List<Counters> r = q.getResultList();
        Assert.assertNotNull(r);
        Assert.assertEquals(3, r.size());
        int counter = 0;
        for (Counters counters : r)
        {
            if (counters.getId().equals(id1))
            {
                counter++;
                Assert.assertNotNull(counters);
                Assert.assertNotNull(counters.getCounter());
                Assert.assertEquals(2, counters.getCounter());
            }
            else if (counters.getId().equals(id2))
            {
                counter++;
                Assert.assertNotNull(counters);
                Assert.assertNotNull(counters.getCounter());
                Assert.assertEquals(5, counters.getCounter());
            }
            else
            {
                counter++;
                Assert.assertNotNull(counters);
                Assert.assertNotNull(counters.getCounter());
                Assert.assertEquals(id3, counters.getId());
                Assert.assertEquals(8, counters.getCounter());
            }

        }
        Assert.assertEquals(3, counter);
        em.close();
    }

    /**
     * 
     */
    private void rangeQuery()
    {
        EntityManager em = emf.createEntityManager();
        Query q = em.createQuery("select c from Counters c where c.id between 12 and 15");
        List<Counters> r = q.getResultList();
        Assert.assertNotNull(r);
        Assert.assertEquals(2, r.size());
        int counter = 0;
        for (Counters counters : r)
        {
            if (counters.getId().equals(id1))
            {
                counter++;
                Assert.assertNotNull(counters);
                Assert.assertNotNull(counters.getCounter());
                Assert.assertEquals(2, counters.getCounter());
            }
            else
            {
                counter++;
                Assert.assertNotNull(counters);
                Assert.assertNotNull(counters.getCounter());
                Assert.assertEquals(id2, counters.getId());
                Assert.assertEquals(5, counters.getCounter());
            }
        }
        Assert.assertEquals(2, counter);
        em.close();
    }

    private void queryGTEQOnId()
    {
        EntityManager em = emf.createEntityManager();
        Query q = em.createQuery("select c from Counters c where c.id >= 15");
        List<Counters> r = q.getResultList();
        Assert.assertNotNull(r);
        Assert.assertEquals(2, r.size());
        int counter = 0;
        for (Counters counters : r)
        {
            if (counters.getId().equals(id2))
            {
                counter++;
                Assert.assertNotNull(counters);
                Assert.assertNotNull(counters.getCounter());
                Assert.assertEquals(5, counters.getCounter());
            }
            else
            {
                counter++;
                Assert.assertNotNull(counters);
                Assert.assertNotNull(counters.getCounter());
                Assert.assertEquals(id3, counters.getId());
                Assert.assertEquals(8, counters.getCounter());
            }

        }
        Assert.assertEquals(2, counter);
        em.close();
    }

    private void queryLTEQOnId()
    {
        EntityManager em = emf.createEntityManager();
        Query q = em.createQuery("select c from Counters c where c.id <= 15");
        List<Counters> r = q.getResultList();
        Assert.assertNotNull(r);
        Assert.assertEquals(2, r.size());
        int counter = 0;
        for (Counters counters : r)
        {
            if (counters.getId().equals(id1))
            {
                counter++;
                Assert.assertNotNull(counters);
                Assert.assertNotNull(counters.getCounter());
                Assert.assertEquals(2, counters.getCounter());
            }
            else
            {
                counter++;
                Assert.assertNotNull(counters);
                Assert.assertNotNull(counters.getCounter());
                Assert.assertEquals(id2, counters.getId());
                Assert.assertEquals(5, counters.getCounter());
            }

        }
        Assert.assertEquals(2, counter);
        em.close();
    }

    private void updateNamedQueryOnCounter()
    {
        EntityManager em = emf.createEntityManager();
        String updateQuery = "Update Counters c SET c.counter=23 where c.id=12";
        Query q = em.createQuery(updateQuery);
        q.executeUpdate();
        Counters counter1 = new Counters();
        counter1 = em.find(Counters.class, id1);
        Assert.assertNotNull(counter1);
        Assert.assertNotNull(counter1.getCounter());
        Assert.assertEquals(2, counter1.getCounter());
    }

    private void deleteNamedQueryOnCounter()
    {
        EntityManager em = emf.createEntityManager();
        String deleteQuery = "Delete From Counters c where c.id <= " + id2;

        Query q = em.createQuery(deleteQuery);
        q.executeUpdate();

        Counters counter2 = new Counters();
        counter2 = em.find(Counters.class, id1);
        Assert.assertNull(counter2);

        Counters counter3 = new Counters();
        counter3 = em.find(Counters.class, id2);
        Assert.assertNull(counter3);

        em.close();
    }
}
