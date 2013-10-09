/**
 * 
 */
package com.impetus.kundera.tests.entities;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.crud.RDBMSCli;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.tests.cli.CassandraCli;

/**
 * @author Chhavi Gangwal
 * 
 */
public class PersonDetailAnnotationPolyglotTest
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
        
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
  
        CassandraCli.dropKeySpace("KunderaTests");
    }

    @Test
    public void test()
    {
        Map propertyMap = new HashMap();
        propertyMap.put(PersistenceProperties.KUNDERA_DDL_AUTO_PREPARE, "create-drop");
        emf = Persistence.createEntityManagerFactory("noAnnotationAddCassandra,noAnnotationAddMongo", propertyMap);
        em = emf.createEntityManager();
        
        AddressMongoNoAnnotation address = new AddressMongoNoAnnotation();
        address.setAddressId("1");
        address.setStreet("sector 20, G Block");

        UserCassandraNoAnnotation user = new UserCassandraNoAnnotation();
        user.setPersonId(1);
        user.setAddress(address);
        user.setPersonName("Kuldeep");

        em.persist(user);

        em.clear();

        List<UserCassandraNoAnnotation> result = em.createQuery("Select u from UserCassandraNoAnnotation u")
                .getResultList();
        Assert.assertNotNull(result);
        Assert.assertEquals(1, result.size());
        Assert.assertNotNull(result.get(0).getAddress());
        Assert.assertEquals("sector 20, G Block", result.get(0).getAddress().getStreet());
        Assert.assertEquals("Kuldeep", result.get(0).getPersonName());
        
        em.close();
        emf.close();
    }
    
   
    @Test
    public void testAnnotateforCassToMongo()
    {
        
        Map propertyMap = new HashMap();
               
        EntityManagerFactory emfCass = Persistence.createEntityManagerFactory("noAnnotationAddMongo", propertyMap);
        EntityManager emCass = emfCass.createEntityManager();
        
        PersonDetailClassMap p = new PersonDetailClassMap();
        p.setPersonId("1");
        p.setFirstName("Chhavi");
        p.setLastName("Gangwal");
        emCass.persist(p);
        emCass.clear();
        
        PersonDetailClassMap found = emCass.find(PersonDetailClassMap.class, "1");
        Assert.assertNotNull(found);
        Assert.assertEquals("1", found.getPersonId());
        Assert.assertEquals("Chhavi", found.getFirstName());
        Assert.assertEquals("Gangwal", found.getLastName());
        
        emCass.close();
        emfCass.close();
        
        propertyMap.put(PersistenceProperties.KUNDERA_DDL_AUTO_PREPARE, "create-drop");
        EntityManagerFactory emfMongo = Persistence.createEntityManagerFactory("noAnnotationAddCassandra", propertyMap);
        EntityManager emMongo = emfMongo.createEntityManager();

        
        emMongo.persist(found);
        
        PersonDetailClassMap foundMongo = emMongo.find(PersonDetailClassMap.class, "1");
        Assert.assertNotNull(foundMongo);
        Assert.assertEquals("1", foundMongo.getPersonId());
        Assert.assertEquals("Chhavi", foundMongo.getFirstName());
        Assert.assertEquals("Gangwal", foundMongo.getLastName());
        
        emMongo.close();
        emfMongo.close();
    }
}
