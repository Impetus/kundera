/**
 * 
 */
package com.impetus.client.cassandra.thrift;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.persistence.CassandraCli;

/**
 * @author impadmin
 * 
 */
public class PersonnelListenerDTOTest
{
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
        CassandraCli.dropKeySpace("KunderaExamples");
    }

    @Test
    public void test()
    {
        AddressListenerDTO address = new AddressListenerDTO();
        address.setStreet("New Street");
        address.setAddressId("a1");
        PersonnelListenerDTO personnel = new PersonnelListenerDTO();
        personnel.setPersonId("1");
        personnel.setFirstName("kk");
        personnel.setLastName("Mishra");
        personnel.setAddress(address);

        em.persist(personnel);

        em.clear();

        PersonnelListenerDTO foundPersonnel = em.find(PersonnelListenerDTO.class, "1");
        Assert.assertNotNull(foundPersonnel);
        Assert.assertEquals("kuldeep", foundPersonnel.getFirstName());
        Assert.assertNotNull(foundPersonnel.getAddress());
        Assert.assertEquals("aaaa", foundPersonnel.getAddress().getStreet());
    }
}
