/**
 * Copyright 2012 Impetus Infotech.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.impetus.client.mongodb.config;

import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.crud.entities.PersonMongo;
import com.impetus.client.mongodb.MongoDBClient;
import com.impetus.client.mongodb.MongoDBClientProperties;
import com.impetus.kundera.client.Client;
import com.mongodb.DBEncoder;
import com.mongodb.DefaultDBEncoder;
import com.mongodb.LazyDBEncoder;
import com.mongodb.WriteConcern;

/**
 * Test case for properties set on operation level in MongoDBClient
 * 
 * @author amresh.singh
 */
public class OperationLevelPropertiesTest
{
    String persistenceUnit = "mongoTest";

    EntityManagerFactory emf;

    EntityManager em;

    @Before
    public void setUp() throws Exception
    {
        emf = Persistence.createEntityManagerFactory(persistenceUnit);
        em = emf.createEntityManager();
        
        em.getDelegate();
        
    }

    @Test
    public void executeTest()
    {
        // Get properties from client
        Map<String, Client> clients = (Map<String, Client>) em.getDelegate();
        MongoDBClient client = (MongoDBClient) clients.get(persistenceUnit);

        // Check default values
        WriteConcern wc = client.getWriteConcern();
        DBEncoder encoder = client.getEncoder();
        Assert.assertNotNull(wc);
        Assert.assertFalse(wc.getFsync());
        Assert.assertEquals(0, wc.getW());
        Assert.assertEquals(0, wc.getWtimeout());
        Assert.assertNotNull(encoder);
        Assert.assertTrue(encoder instanceof DefaultDBEncoder);
        
        // Set parameters into EM
        // (See http://api.mongodb.org/java/2.6/com/mongodb/WriteConcern.html)
        WriteConcern wcNew = new WriteConcern(1, 300, true);
        DBEncoder encoderNew = new LazyDBEncoder();
        em.setProperty(MongoDBClientProperties.WRITE_CONCERN, wcNew);
        em.setProperty(MongoDBClientProperties.BATCH_SIZE, 5);
       

        // Check Modified values
        WriteConcern wcModified = client.getWriteConcern();
        DBEncoder encoderModified = client.getEncoder();
        Assert.assertNotNull(wcModified);
        Assert.assertTrue(wcModified.getFsync());
        Assert.assertEquals(1, wcModified.getW());
        Assert.assertEquals(300, wcModified.getWtimeout());
        Assert.assertNotNull(encoderModified);
        Assert.assertEquals(5, client.getBatchSize());
        // Assert.assertTrue(encoderModified instanceof LazyDBEncoder);
        em.clear();
        
        em.setProperty(MongoDBClientProperties.BATCH_SIZE,""+ 2);
        Assert.assertEquals(2, client.getBatchSize());
        
        em.clear();
        // Write Entity to database
        PersonMongo person = new PersonMongo();
        person.setPersonId("1");
        person.setPersonName("Amresh");
        person.setAge(31);
        em.persist(person);
        
        PersonMongo person1 = new PersonMongo();
        person1.setPersonId("2");
        person1.setPersonName("Chhavi");
        person1.setAge(31);
        em.persist(person1);

        // Find entity from database
        PersonMongo p = em.find(PersonMongo.class, "1");
        Assert.assertNotNull(p);

        // Delete entity from database
        em.remove(p);

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

}
