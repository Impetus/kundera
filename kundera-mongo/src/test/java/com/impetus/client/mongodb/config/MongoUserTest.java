/**
 * 
 */
package com.impetus.client.mongodb.config;

import java.util.HashMap;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Kuldeep Mishra
 * 
 */
public class MongoUserTest
{

    /**
     * 
     */
    private static final String _PU = "mongoTest";

    private EntityManagerFactory emf;

    private EntityManager em;

    private Map<String, String> puProperties = new HashMap<String, String>();

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        puProperties.put("kundera.ddl.auto.prepare", "create-drop");
        puProperties.put("kundera.keyspace", "KunderaMongoKeyspace");
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
    }

    @Test
    public void test()
    {
        MongoUser u = new MongoUser();
        u.setName("kuldeep");
        u.setAge(24);
        u.setAddress("gzb");
        em.persist(u);

        em.clear();

        MongoUser user = em.find(MongoUser.class, "kuldeep");
        Assert.assertNotNull(user);
        Assert.assertEquals(24, user.getAge());
        Assert.assertEquals("gzb", user.getAddress());
    }
}
