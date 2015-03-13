/*******************************************************************************
 * * Copyright 2015 Impetus Infotech.
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
package com.impetus.client.hbase.configuration;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import junit.framework.Assert;

import org.apache.hadoop.hbase.filter.ColumnPaginationFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.hbase.HBaseClient;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.Client;

/**
 * The Class HbasePropertySetterTest.
 * 
 * @author Chhavi Gangwal
 */
public class HbasePropertySetterTest
{

    /** The Constant HBASE_PU. */
    private static final String HBASE_PU = "XmlPropertyTest";

    /** The emf. */
    private EntityManagerFactory emf;

    /** The em. */
    private EntityManager em;

    /**
     * logger used for logging statement.
     */
    private static final Logger logger = LoggerFactory.getLogger(HbasePropertySetterTest.class);

    /**
     * Sets the up.
     * 
     * @throws Exception
     *             the exception
     */
    @Before
    public void setUp() throws Exception
    {
        Map<String, String> puProperties = new HashMap<String, String>();
        puProperties.put("kundera.ddl.auto.prepare", "create-drop");
        puProperties.put("kundera.keyspace", "KunderaHbaseKeyspace");
        emf = Persistence.createEntityManagerFactory(HBASE_PU, puProperties);

    }

    /**
     * drops the keyspace and closes connection.
     * 
     * @throws Exception
     *             the exception
     */
    @After
    public void tearDown() throws Exception
    {
        emf.close();
    }

    /**
     * Sets property of hbase client in form of String.
     * 
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    @Test
    public void testUsingExternalStringProperty() throws IOException
    {

        Map<String, Object> puPropertiesString = new HashMap<String, Object>();

        puPropertiesString.put(PersistenceProperties.KUNDERA_BATCH_SIZE, "10");

        em = emf.createEntityManager(puPropertiesString);

        Map<String, Client> clients = (Map<String, Client>) em.getDelegate();
        Client client = clients.get(HBASE_PU);

        Assert.assertEquals(((HBaseClient) client).getBatchSize(), 10);

    }

    /**
     * Sets property of hbase client in form of object map.
     * 
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    @Test
    public void testUsingExternalObjectProperty() throws IOException
    {
        try
        {
            Map<String, Object> puPropertiesString = new HashMap<String, Object>();
            Filter filter = new ColumnPaginationFilter(2, 0);
            FilterList filterlist = new FilterList();
            filterlist.addFilter(filter);

            puPropertiesString.put("hbase.filter", filter);
            puPropertiesString.put(PersistenceProperties.KUNDERA_BATCH_SIZE, 10);

            em = emf.createEntityManager(puPropertiesString);

            Map<String, Client> clients = (Map<String, Client>) em.getDelegate();
            Client client = clients.get(HBASE_PU);

            Field f1, f2;

            f1 = ((HBaseClient) client).getClass().getDeclaredField("handler"); // NoSuchFieldException
            f1.setAccessible(true);
            f2 = f1.get((HBaseClient) client).getClass().getDeclaredField("filter");
            f2.setAccessible(true);
            Assert.assertEquals(f2.get(f1.get((HBaseClient) client)).toString(), filterlist.toString());
            Assert.assertEquals(((HBaseClient) client).getBatchSize(), 10);
        }
        catch (NoSuchFieldException nfe)
        {
            Assert.fail();
            logger.error("Error in test, Caused by: .", nfe.getMessage());

        }
        catch (IllegalArgumentException iae)
        {
            Assert.fail();
            logger.error("Error in test, Caused by: .", iae.getMessage());
        }
        catch (IllegalAccessException e)
        {
            Assert.fail();
            logger.error("Error in test, Caused by: .", e.getMessage());
        }
    }
}
