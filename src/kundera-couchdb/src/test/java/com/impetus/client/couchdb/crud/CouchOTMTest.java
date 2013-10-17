/**
 * 
 */
package com.impetus.client.couchdb.crud;

import java.util.HashSet;
import java.util.Set;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.couchdb.entities.AddressCouchOTM;
import com.impetus.client.couchdb.entities.PersonCouchOTM;

/**
 * @author impadmin
 * 
 */
public class CouchOTMTest
{
    private EntityManagerFactory emf;

    private EntityManager em;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        emf = Persistence.createEntityManagerFactory("couchdb_pu");
        em = getNewEM();
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
        em.close();
        emf.close();
    }

    @Test
    public void testCRUD()
    {
        AddressCouchOTM address1 = new AddressCouchOTM();
        address1.setAddressId("a");
        address1.setStreet("sector 11");

        AddressCouchOTM address2 = new AddressCouchOTM();
        address2.setAddressId("b");
        address2.setStreet("sector 12");

        Set<AddressCouchOTM> addresses = new HashSet<AddressCouchOTM>();
        addresses.add(address1);
        addresses.add(address2);

        PersonCouchOTM person = new PersonCouchOTM();
        person.setPersonId("1");
        person.setPersonName("Kuldeep");
        person.setAddresses(addresses);

        em.persist(person);

        em = getNewEM();

        PersonCouchOTM foundPerson = em.find(PersonCouchOTM.class, 1);
        Assert.assertNotNull(foundPerson);
        Assert.assertNotNull(foundPerson.getAddresses());
        Assert.assertEquals("1", foundPerson.getPersonId());
        Assert.assertEquals("Kuldeep", foundPerson.getPersonName());

        int counter = 0;
        for (AddressCouchOTM address : foundPerson.getAddresses())
        {
            if (address.getAddressId().equals("a"))
            {
                counter++;
                Assert.assertEquals("sector 11", address.getStreet());
            }
            else
            {
                Assert.assertEquals("b", address.getAddressId());
                Assert.assertEquals("sector 12", address.getStreet());
                counter++;
            }
        }

        Assert.assertEquals(2, counter);

        foundPerson.setPersonName("KK");

        em.merge(foundPerson);

        em = getNewEM();

        foundPerson = em.find(PersonCouchOTM.class, 1);
        Assert.assertNotNull(foundPerson);
        Assert.assertNotNull(foundPerson.getAddresses());
        Assert.assertEquals("1", foundPerson.getPersonId());
        Assert.assertEquals("KK", foundPerson.getPersonName());

        counter = 0;
        for (AddressCouchOTM address : foundPerson.getAddresses())
        {
            if (address.getAddressId().equals("a"))
            {
                counter++;
                Assert.assertEquals("sector 11", address.getStreet());
            }
            else
            {
                Assert.assertEquals("b", address.getAddressId());
                Assert.assertEquals("sector 12", address.getStreet());
                counter++;
            }
        }

        Assert.assertEquals(2, counter);

        em.remove(foundPerson);

        foundPerson = em.find(PersonCouchOTM.class, 1);
        Assert.assertNull(foundPerson);
    }

    private EntityManager getNewEM()
    {
        if (em != null && em.isOpen())
        {
            em.close();
        }
        return em = emf.createEntityManager();
    }

}
