package com.impetus.kundera.tests.crossdatastore.transaction;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.tests.cli.CassandraCli;
import com.impetus.kundera.tests.crossdatastore.useraddress.entities.HabitatOToOFKEntity;
import com.impetus.kundera.tests.crossdatastore.useraddress.entities.PersonnelOToOFKEntity;

/**
 * @author vivek.mishra
 * 
 */
public class CrossDataStoreTransactionTest
{

    private EntityManagerFactory emf;

    private EntityManager em;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        emf = Persistence.createEntityManagerFactory("secIdxAddCassandra,addMongo");
        em = emf.createEntityManager();

        // CassandraCli.cassandraSetUp();
        // CassandraCli.createKeySpace("KunderaTests");
    }

    @Test
    public void testRollback()
    {
        PersonnelOToOFKEntity person = new PersonnelOToOFKEntity();
        person.setPersonId("1_p");
        person.setPersonName("crossdata-store");
        HabitatOToOFKEntity address = new HabitatOToOFKEntity();
        address.setAddressId("1_a");
        address.setStreet("my street");
        person.setAddress(address);
        try
        {
            em.persist(person);
        }
        catch (Exception ex)
        {
            HabitatOToOFKEntity found = em.find(HabitatOToOFKEntity.class, "1_a");
            Assert.assertNull(found);
        }
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
        // CassandraCli.dropKeySpace("KunderaExamples");
    }

}
