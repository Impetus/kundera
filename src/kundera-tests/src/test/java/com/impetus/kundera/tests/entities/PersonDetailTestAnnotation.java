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
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.tests.cli.CassandraCli;
import com.impetus.kundera.tests.cli.HBaseCli;


public class PersonDetailTestAnnotation
{
    /**
     * 
     */
    private static final String _PU = "secIdxAddCassandra,addMongo,oracle_kvstore,redis,piccandra,picongo";
  

    private EntityManagerFactory emf;

    private EntityManager em;

    private Map<String, Object> mongoProperties = new HashMap<String, Object>();

    private Map<String, Object> cassandraProperties = new HashMap<String, Object>();
    
    private Map<String, Object> properties = new HashMap<String, Object>();

    private Map<String, Map<String, Object>> puPropertiesMap = new HashMap<String, Map<String, Object>>();

    private HBaseCli cli;

    /**
     * setup for cassandra, mongo, oracle-nosql, redis and hbase
     */
    @Before
    public void setUp() throws Exception
    {
        CassandraCli.cassandraSetUp();
      
        KunderaMetadata.INSTANCE.setApplicationMetadata(null);

        mongoProperties.put("kundera.ddl.auto.prepare", "create-drop");

        cassandraProperties.put("kundera.ddl.auto.prepare", "create-drop");
        
        properties.put("kundera.ddl.auto.prepare", "create-drop");

        puPropertiesMap.put("addMongo", mongoProperties);
        puPropertiesMap.put("secIdxAddCassandra", cassandraProperties);
        puPropertiesMap.put("piccandra", properties);
        puPropertiesMap.put("picongo", properties);

        emf = Persistence.createEntityManagerFactory(_PU, puPropertiesMap);
        em = emf.createEntityManager();
    }

    /**
     * closes em
     */
    @After
    public void tearDown() throws Exception
    {
        em.close();
        emf.close();
        mongoProperties = null;
        cassandraProperties = null;
        puPropertiesMap = null;
    }
    
    /**
     * Test method for testing entity class with no table name annotation
     */
    @Test
    public void testNoTableAnnotation()
    {
        PersonDetailNoTable p = new PersonDetailNoTable();
        p.setPersonId("1");
        p.setFirstName("Chhavi");
        p.setLastName("Gangwal");
        em.persist(p);
        em.clear();

        PersonDetailNoTable found = em.find(PersonDetailNoTable.class, "1");
        Assert.assertNotNull(found);
        Assert.assertEquals("1", found.getPersonId());
        Assert.assertEquals("Chhavi", found.getFirstName());
        Assert.assertEquals("Gangwal", found.getLastName());
    }
    
    /**
     * Test method for testing entity classes with no schema definition 
    */
    @Test
    public void testTableNoSchemaAnnotation()
    {
        PersonDetailNoSchema p = new PersonDetailNoSchema();
        p.setPersonId("1");
        p.setFirstName("Chhavi");
        p.setLastName("Gangwal");
        em.persist(p);
        em.clear();

        PersonDetailNoSchema found = em.find(PersonDetailNoSchema.class, "1");
        Assert.assertNotNull(found);
        Assert.assertEquals("1", found.getPersonId());
        Assert.assertEquals("Chhavi", found.getFirstName());
        Assert.assertEquals("Gangwal", found.getLastName());
    }
    
    


}
