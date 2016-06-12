/*******************************************************************************
 *  * Copyright 2015 Impetus Infotech.
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
package com.impetus.client.couchdb.crud;

import javax.persistence.Persistence;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.impetus.kundera.client.query.AggregationsBaseTest;

/**
 * The Class CouchDBESAggregationTest.
 * 
 * @author: karthikp.manchala
 */
public class CouchDBESAggregationTest extends AggregationsBaseTest
{
    /** The node. */
    private static Node node = null;

    /**
     * Sets the up before class.
     * 
     * @throws Exception
     *             the exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception
    {
        if (!checkIfServerRunning())
        {
            Builder builder = Settings.settingsBuilder();
            builder.put("path.home", "target/data");
            node = new NodeBuilder().settings(builder).node();
        }
    }

    /**
     * Setup.
     * 
     * @throws InterruptedException
     *             the interrupted exception
     */
    @Before
    public void setup() throws InterruptedException
    {

        emf = Persistence.createEntityManagerFactory("couchdbESAggragation_pu");
        em = emf.createEntityManager();
        init();
    }

    /**
     * Test.
     */
    @Test
    public void test()
    {
        testAggregation();
    }

    /**
     * Tear down after class.
     * 
     * @throws Exception
     *             the exception
     */
    @AfterClass
    public static void tearDownAfterClass() throws Exception
    {
        if (node != null)
            node.close();
    }

    /**
     * Tear down.
     */
    @After
    public void tearDown()
    {
        em.createQuery("DELETE FROM Person p").executeUpdate();
        em.close();
        emf.close();
    }

}
