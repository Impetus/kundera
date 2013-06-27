/**
 * 
 */
package com.impetus.client.cassandra.thrift;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.apache.cassandra.thrift.Cassandra;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.persistence.CassandraCli;

/**
 * @author impadmin
 * 
 */
public class PersonIdentityTest
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
        CassandraCli.createKeySpace("CompositeCassandra");
        emf = Persistence.createEntityManagerFactory("composite_pu");
        em = emf.createEntityManager();
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
        CassandraCli.dropKeySpace("CompositeCassandra");
        em.close();
        emf.close();
    }

    @Test
    public void test()
    {
        PhoneId phoneId1 = new PhoneId();
        phoneId1.setPhoneId("A");
        PhoneId phoneId2 = new PhoneId();
        phoneId2.setPhoneId("B");

        Phone phone1 = new Phone();
        phone1.setPhoneId(phoneId1);
        phone1.setPhoneNumber(99533533434l);

        Phone phone2 = new Phone();
        phone2.setPhoneId(phoneId2);
        phone2.setPhoneNumber(9972723678l);

        List<Phone> phones = new ArrayList<Phone>();
        phones.add(phone1);
        phones.add(phone2);

        PersonIdentity identity = new PersonIdentity();
        identity.setPersonId("1");
        identity.setPersonName("KK");
        identity.setPhones(phones);

        em.persist(identity);

        em.clear();

        PersonIdentity foundPerson = em.find(PersonIdentity.class, "1");

        Assert.assertNotNull(foundPerson);
        Assert.assertNotNull(foundPerson.getPhones());
        Assert.assertFalse(foundPerson.getPhones().isEmpty());
        Assert.assertEquals(2,foundPerson.getPhones().size());
        Assert.assertNotNull(foundPerson.getPhones().get(0));
        Assert.assertNotNull(foundPerson.getPhones().get(1));
        Assert.assertNotNull(foundPerson.getPhones().get(0).getPhoneId());
        Assert.assertNotNull(foundPerson.getPhones().get(1).getPhoneId());

        Assert.assertEquals(new Long(99533533434l), foundPerson.getPhones().get(0).getPhoneNumber());
        Assert.assertEquals(new Long(9972723678l), foundPerson.getPhones().get(1).getPhoneNumber());

        Assert.assertEquals("1", foundPerson.getPhones().get(0).getPhoneId().getPersonId());
        
        List<String> phoneIds = new ArrayList<String>();
        phoneIds.add("A");
        phoneIds.add("B");
        Assert.assertTrue(phoneIds.contains(foundPerson.getPhones().get(0).getPhoneId().getPhoneId()));
        Assert.assertEquals("1", foundPerson.getPhones().get(1).getPhoneId().getPersonId());
        Assert.assertNotSame(foundPerson.getPhones().get(0).getPhoneId().getPhoneId(), foundPerson.getPhones().get(1).getPhoneId().getPhoneId());

    }

}
