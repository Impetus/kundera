/**
 * 
 */
package com.impetus.client.crud.compositeType.association;

import java.util.HashSet;
import java.util.Set;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.persistence.CassandraCli;

/**
 * @author impadmin
 * 
 */
public class CassandraUserOTMTest
{

    private static final String _KEYSPACE = "KunderaExamples";

    private EntityManagerFactory emf;

    private EntityManager em;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        CassandraCli.cassandraSetUp();
        emf = Persistence.createEntityManagerFactory("secIdxCassandraTest");
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
        CassandraCli.dropKeySpace(_KEYSPACE);
        emf.close();
    }

    @Test
    public void test()
    {
        em = emf.createEntityManager();

        Set<CassandraAddressUniOTM> addresses = new HashSet<CassandraAddressUniOTM>();

        CassandraAddressUniOTM address1 = new CassandraAddressUniOTM();
        address1.setAddressId("a");
        address1.setStreet("my street");
        CassandraAddressUniOTM address2 = new CassandraAddressUniOTM();
        address2.setAddressId("b");
        address2.setStreet("my new street");

        addresses.add(address1);
        addresses.add(address2);

        CassandraUserUniOTM userUniOTM = new CassandraUserUniOTM();
        userUniOTM.setPersonId("1");
        userUniOTM.setPersonName("kuldeep");
        userUniOTM.setAddresses(addresses);

        em.persist(userUniOTM);

        em.clear();

        CassandraUserUniOTM foundObject = em.find(CassandraUserUniOTM.class, "1");
        Assert.assertNotNull(foundObject);
        Assert.assertEquals(2, foundObject.getAddresses().size());

        em.remove(foundObject);

        foundObject = em.find(CassandraUserUniOTM.class, "1");
        Assert.assertNull(foundObject);
    }
}
