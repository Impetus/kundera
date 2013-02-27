/**
 * 
 */
package com.impetus.client.cassandra.config;

import java.util.HashMap;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.persistence.CassandraCli;

/**
 * @author Kuldeep Mishra
 * 
 */
public class CassandraUserTest
{

    /**
     * 
     */
    private static final String _PU = "CassandraXmlPropertyTest";

    private EntityManagerFactory emf;

    private EntityManager em;

    private Map<String, Object> puProperties = new HashMap<String, Object>();

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        CassandraCli.cassandraSetUp();
        puProperties.put("kundera.ddl.auto.prepare", "create-drop");
        puProperties.put("kundera.keyspace", "KunderaKeyspace");
        puProperties.put("kundera.client.lookup.class", "com.impetus.client.cassandra.pelops.PelopsClientFactory");
        puProperties.put("kundera.nodes", "localhost");
        puProperties.put("kundera.port", "9160");
        puProperties.put("kundera.client.property", "kunderaTest.xml");
        emf = Persistence.createEntityManagerFactory(_PU, puProperties);
        em = emf.createEntityManager();
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
        em.close();
        emf.close();
        puProperties = null;
        CassandraCli.dropKeySpace("KunderaKeyspace");
    }

    @Test
    public void test()
    {
        CassandraUser u = new CassandraUser();
        u.setName("kuldeep");
        u.setAge(24);
        u.setAddress("gzb");
        em.persist(u);

        em.clear();

        CassandraUser user = em.find(CassandraUser.class, "kuldeep");
        Assert.assertNotNull(user);
        Assert.assertEquals(24, user.getAge());
        Assert.assertEquals("gzb", user.getAddress());
    }
}
