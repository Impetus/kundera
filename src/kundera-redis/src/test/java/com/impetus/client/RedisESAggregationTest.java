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
package com.impetus.client;

import javax.persistence.Persistence;
import javax.persistence.Query;

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
 * The Class RedisESAggregationTest.
 * 
 * @author karthikp.manchala
 */
public class RedisESAggregationTest extends AggregationsBaseTest
{

    /** The Constant REDIS_PU. */
    private static final String REDIS_PU = "redisElasticSearch_pu";

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
        Builder builder = Settings.settingsBuilder();
        builder.put("path.home", "target/data");
        node = new NodeBuilder().settings(builder).node();
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
        node.close();
    }

    /**
     * Sets the up.
     * 
     * @throws Exception
     *             the exception
     */
    @Before
    public void setUp() throws Exception
    {
        emf = Persistence.createEntityManagerFactory(REDIS_PU);
        em = emf.createEntityManager();
        init();
    }

    /**
     * Tear down.
     * 
     * @throws Exception
     *             the exception
     */
    @After
    public void tearDown() throws Exception
    {

        String deleteQuery = "Delete from Person p";
        Query query = em.createQuery(deleteQuery);
        query.executeUpdate();
        em.flush();
        em.clear();
        emf.close();
    }
}