/*******************************************************************************
 * * Copyright 2013 Impetus Infotech.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 ******************************************************************************/
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

    /**
     * sets up cassandra client
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

        emf = Persistence.createEntityManagerFactory("addMongoNoAnnotateTest,secIdxAddCassandraNoAnnotateTest");
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

        propertyMap.put(PersistenceProperties.KUNDERA_DDL_AUTO_PREPARE, "create-drop");
        EntityManagerFactory emfCass = Persistence.createEntityManagerFactory("noAnnotationAddCassandra", propertyMap);
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

        EntityManagerFactory emfMongo = Persistence.createEntityManagerFactory("noAnnotationAddMongo", propertyMap);
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

    @Test
    public void testRDBMSPolyglot()
    {

        emf = Persistence.createEntityManagerFactory("cassandraAddressNoAnnotate,rdbmsNoAnnotateTest");
        em = emf.createEntityManager();

        PersonRDBMSPolyglot person = new PersonRDBMSPolyglot();
        person.setPersonId("p1");

        AddressCassandra address = new AddressCassandra();
        address.setAddressId("addr_1");
        address.setStreet("Street");

        person.setAddress(address);

        em.persist(person);
        
        PersonRDBMSPolyglot found = em.find(PersonRDBMSPolyglot.class, "p1");
        Assert.assertNotNull(found);
        Assert.assertEquals("p1", found.getPersonId());
      

        em.close();
        emf.close();

    }

    @Test
    public void testInvalidEntityObject()
    {
        try
        {

            Map<String, Object> puProperties = new HashMap<String, Object>();
            puProperties.put("kundera.ddl.auto.prepare", "create-drop");

            emf = Persistence.createEntityManagerFactory("rdbms,secIdxAddCassandra,piccandra,addMongo,picongo",
                    puProperties);

            em = emf.createEntityManager();

            AddressCassandra address = new AddressCassandra();
            address.setAddressId("addr_1");
            address.setStreet("Street");

            PersonRDBMSPolyglot person = new PersonRDBMSPolyglot();
            person.setAddress(address);

            em.persist(person);

            em.close();
            emf.close();
        }
        catch (Exception iex)
        {
            Assert.assertNotNull(iex.getMessage());
        }

    }

}
