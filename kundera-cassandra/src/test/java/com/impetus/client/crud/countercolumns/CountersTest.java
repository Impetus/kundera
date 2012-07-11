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

/**
 * @author kuldeep.mishra
 * 
 */
public class CountersTest
{
    private EntityManagerFactory emf;

    private EntityManager em;

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
        persistCounter();
        findCounter();
        queryOnCounter();
        mergeCounter();
        deleteCounter();
    }

    public void persistCounter()
    {
        em = emf.createEntityManager();
        Counters counters = new Counters();
        counters.setCounter1(12);
        counters.setId("sk");
        em.persist(counters);

        counters = em.find(Counters.class, "sk");
        Assert.assertNotNull(counters);
        Assert.assertNotNull(counters.getCounter1());

        em.close();
    }

    public void findCounter()
    {
        em = emf.createEntityManager();
        Counters counters = new Counters();
        counters = em.find(Counters.class, "sk");
        Assert.assertNotNull(counters);
        Assert.assertNotNull(counters.getCounter1());
        em.close();
    }

    public void deleteCounter()
    {
        em = emf.createEntityManager();
        Counters counters = new Counters();
        counters = em.find(Counters.class, "sk");
        Assert.assertNotNull(counters);
        Assert.assertNotNull(counters.getCounter1());
        em.remove(counters);

        EntityManager em1 = emf.createEntityManager();
        counters = em1.find(Counters.class, "sk");
        Assert.assertNull(counters);

        em.close();
    }

    public void mergeCounter()
    {
        em = emf.createEntityManager();
        Counters counters = new Counters();
        counters = em.find(Counters.class, "sk");
        Assert.assertNotNull(counters);
        Assert.assertNotNull(counters.getCounter1());
        counters = em.merge(counters);
        Assert.assertNull(counters);
        em.close();
    }

    public void queryOnCounter()
    {
        em = emf.createEntityManager();
        Query q = em.createQuery("select c from Counters c");
        List<Counters> r = q.getResultList();
        Assert.assertNotNull(r);
        for (Counters counters : r)
        {
            Assert.assertNotNull(counters);
            Assert.assertNotNull(counters.getCounter1());
        }
    }
}
