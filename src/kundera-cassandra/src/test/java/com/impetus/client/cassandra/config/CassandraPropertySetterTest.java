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
package com.impetus.client.cassandra.config;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.lang.reflect.Field;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.apache.cassandra.thrift.ConsistencyLevel;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.cassandra.CassandraClientBase;
import com.impetus.client.cassandra.common.CassandraConstants;
import com.impetus.client.persistence.CassandraCli;
import com.impetus.kundera.client.Client;

/**
 * @author Chhavi Gangwal
 * 
 */
public class CassandraPropertySetterTest
{

    /**
     * 
     */
    private static final String _PU = "CassandraXmlPropertyTest";

    private EntityManagerFactory emf;

    private EntityManager em;

    private Map<String, Object> puProperties = new HashMap<String, Object>();

    /**
     * creates client connection and keyspace
     */
    @Before
    public void setUp() throws Exception
    {
        CassandraCli.cassandraSetUp();

        puProperties.put("kundera.ddl.auto.prepare", "create-drop");
        puProperties.put("kundera.keyspace", "KunderaKeyspace");
        puProperties.put("kundera.client.lookup.class", "com.impetus.client.cassandra.pelops.PelopsClientFactory");
        puProperties.put("kundera.nodes", "localhost");
        puProperties.put("kundera.port", "9160");
        puProperties.put("kundera.client.property", "kunderaTest.xml");

        emf = Persistence.createEntityManagerFactory(_PU, puProperties);

    }

    /**
     * Drops keyspace and closes connection
     */
    @After
    public void tearDown() throws Exception
    {
        em.close();
        emf.close();
        puProperties = null;
        CassandraCli.dropKeySpace("KunderaKeyspace");
    }

    /**
     * Sets property of cassandra client in form of string
     */
    @Test
    public void testStringPropertyValues()
    {
        try
        {
            Map<String, Object> puPropertiesString = new HashMap<String, Object>();
            Map<String, Object> ttv = new HashMap<String, Object>();

            ttv.put("", "");
            puPropertiesString.put(CassandraConstants.CQL_VERSION, CassandraConstants.CQL_VERSION_2_0);
            puPropertiesString.put("consistency.level", "" + (ConsistencyLevel.QUORUM));
            puPropertiesString.put("ttl.per.request", "" + true);
            puPropertiesString.put("ttl.per.session", "" + false);
            puPropertiesString.put("ttl.values", ttv);

            em = emf.createEntityManager(puPropertiesString);

            Map<String, Client> clients = (Map<String, Client>) em.getDelegate();
            Client client = clients.get(_PU);

            Field f;

            f = ((CassandraClientBase) client).getClass().getSuperclass().getDeclaredField("consistencyLevel"); // NoSuchFieldException
            f.setAccessible(true);
            Assert.assertEquals(f.get((CassandraClientBase) client), ConsistencyLevel.QUORUM);

            f = ((CassandraClientBase) client).getClass().getSuperclass().getDeclaredField("cqlVersion"); // NoSuchFieldException
            f.setAccessible(true);
            Assert.assertEquals(f.get((CassandraClientBase) client), CassandraConstants.CQL_VERSION_2_0);

            Assert.assertTrue(((CassandraClientBase) client).isTtlPerRequest());
            Assert.assertFalse(((CassandraClientBase) client).isTtlPerSession());
            Assert.assertEquals(((CassandraClientBase) client).getTtlValues().size(), ttv.size());

        }
        catch (Exception e)
        {
            System.out.println(e.getMessage());
        }
    }

    /**
     * Sets property of cassandra client in form of object map
     */
    @Test
    public void testObjectPropertyValues()
    {
        try
        {
            Map<String, Object> puPropertiesObj = new HashMap<String, Object>();
            Map<String, Object> ttv = new HashMap<String, Object>();

            ttv.put("test", "1");
            puPropertiesObj.put(CassandraConstants.CQL_VERSION, CassandraConstants.CQL_VERSION_2_0);
            puPropertiesObj.put("consistency.level", ConsistencyLevel.QUORUM);
            puPropertiesObj.put("ttl.per.request", true);
            puPropertiesObj.put("ttl.per.session", false);
            puPropertiesObj.put("ttl.values", ttv);

            em = emf.createEntityManager(puPropertiesObj);

            Map<String, Client> clients = (Map<String, Client>) em.getDelegate();
            Client client = clients.get(_PU);

            Field f;

            f = ((CassandraClientBase) client).getClass().getSuperclass().getDeclaredField("consistencyLevel"); // NoSuchFieldException
            f.setAccessible(true);
            Assert.assertEquals(f.get((CassandraClientBase) client), ConsistencyLevel.QUORUM);

            f = ((CassandraClientBase) client).getClass().getSuperclass().getDeclaredField("cqlVersion"); // NoSuchFieldException
            f.setAccessible(true);
            Assert.assertEquals(f.get((CassandraClientBase) client), CassandraConstants.CQL_VERSION_2_0);

            Assert.assertTrue(((CassandraClientBase) client).isTtlPerRequest());
            Assert.assertFalse(((CassandraClientBase) client).isTtlPerSession());
            Assert.assertEquals(((CassandraClientBase) client).getTtlValues().size(), ttv.size());

        }
        catch (Exception e)
        {
            System.out.println(e.getMessage());
        }
    }

}
