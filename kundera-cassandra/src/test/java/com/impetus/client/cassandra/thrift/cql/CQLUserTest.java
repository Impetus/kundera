/**
 * 
 */
package com.impetus.client.cassandra.thrift.cql;

import java.util.HashMap;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.cassandra.common.CassandraConstants;
import com.impetus.client.persistence.CassandraCli;

/**
 * @author Kuldeep
 * 
 */
public class CQLUserTest
{

    private EntityManagerFactory emf;

    private String persistenceUnit = "cassandra_cql";

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        CassandraCli.cassandraSetUp();
        Map propertyMap = new HashMap();
        propertyMap.put(CassandraConstants.CQL_VERSION, CassandraConstants.CQL_VERSION_3_0);
        emf = Persistence.createEntityManagerFactory(persistenceUnit, propertyMap);
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
        emf.close();
        emf = null;
    }

    @Test
    public void testCRUD()
    {
        EntityManager em = emf.createEntityManager();
        CQLUser user = new CQLUser();
        user.setId(1);
        user.setName("Kuldeep");
        user.setAge(24);

        em.persist(user);

        em.clear();

        CQLUser foundUser = em.find(CQLUser.class, 1);
        Assert.assertNotNull(foundUser);
        Assert.assertEquals(1, foundUser.getId());
        Assert.assertEquals(24, foundUser.getAge());
        Assert.assertEquals("Kuldeep", foundUser.getName());

        foundUser.setName("KK");

        em.merge(foundUser);

        em.clear();

        CQLUser mergedUser = em.find(CQLUser.class, 1);
        Assert.assertNotNull(mergedUser);
        Assert.assertEquals(1, mergedUser.getId());
        Assert.assertEquals(24, mergedUser.getAge());
        Assert.assertEquals("KK", mergedUser.getName());

        em.remove(mergedUser);

        CQLUser deletedUser = em.find(CQLUser.class, 1);
        Assert.assertNull(deletedUser);

        em.clear();

        deletedUser = em.find(CQLUser.class, 1);
        Assert.assertNull(deletedUser);
    }
}
