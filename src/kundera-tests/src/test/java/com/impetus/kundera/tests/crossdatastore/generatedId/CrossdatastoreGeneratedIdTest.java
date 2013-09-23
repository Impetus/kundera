/**
 * 
 */
package com.impetus.kundera.tests.crossdatastore.generatedId;

import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.crud.RDBMSCli;
import com.impetus.kundera.tests.cli.CassandraCli;

/**
 * @author impadmin
 * 
 */
public class CrossdatastoreGeneratedIdTest
{
    private EntityManagerFactory emf;

    private EntityManager em;

    private RDBMSCli cli;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        CassandraCli.cassandraSetUp();
        emf = Persistence.createEntityManagerFactory("secIdxAddCassandra,addMongo");
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
        CassandraCli.dropKeySpace("KunderaTests");
    }

    @Test
    public void test()
    {
        AddressMongoGeneratedId address = new AddressMongoGeneratedId();
        address.setStreet("sector 20, G Block");

        UserCassandraGeneratedId user = new UserCassandraGeneratedId();
        user.setAddress(address);
        user.setPersonName("Kuldeep");

        em.persist(user);

        em.clear();

        List<UserCassandraGeneratedId> result = em.createQuery("Select u from UserCassandraGeneratedId u")
                .getResultList();
        Assert.assertNotNull(result);
        Assert.assertEquals(1, result.size());
        Assert.assertNotNull(result.get(0).getAddress());
        Assert.assertEquals("sector 20, G Block", result.get(0).getAddress().getStreet());
        Assert.assertEquals("Kuldeep", result.get(0).getPersonName());
    }
}
