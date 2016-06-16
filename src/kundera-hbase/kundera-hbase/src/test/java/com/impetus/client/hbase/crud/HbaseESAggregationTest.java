///*******************************************************************************
// *  * Copyright 2015 Impetus Infotech.
// *  *
// *  * Licensed under the Apache License, Version 2.0 (the "License");
// *  * you may not use this file except in compliance with the License.
// *  * You may obtain a copy of the License at
// *  *
// *  *      http://www.apache.org/licenses/LICENSE-2.0
// *  *
// *  * Unless required by applicable law or agreed to in writing, software
// *  * distributed under the License is distributed on an "AS IS" BASIS,
// *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// *  * See the License for the specific language governing permissions and
// *  * limitations under the License.
// ******************************************************************************/
///*
// * author: karthikp.manchala
// */
//package com.impetus.client.hbase.crud;
//
//import java.util.List;
//
//import javax.persistence.Persistence;
//
//import org.elasticsearch.common.settings.Settings;
//import org.elasticsearch.common.settings.Settings.Builder;
//import org.elasticsearch.node.Node;
//import org.elasticsearch.node.NodeBuilder;
//import org.junit.After;
//import org.junit.AfterClass;
//import org.junit.Assert;
//import org.junit.Before;
//import org.junit.BeforeClass;
//import org.junit.Test;
//
//import com.impetus.kundera.client.query.AggregationsBaseTest;
//import com.impetus.kundera.query.Person;
//
///**
// * The Class HBaseESAggregationTest.
// * 
// * @
// */
//public class HbaseESAggregationTest extends AggregationsBaseTest
//{
//    /** The node. */
//    private static Node node = null;
//
//    /**
//     * Sets the up before class.
//     * 
//     * @throws Exception
//     *             the exception
//     */
//    @BeforeClass
//    public static void setUpBeforeClass() throws Exception
//    {
//        if (!checkIfServerRunning())
//        {
//            Builder builder = Settings.settingsBuilder();
//            builder.put("path.home", "target/data");
//            node = new NodeBuilder().settings(builder).node();
//        }
//    }
//
//    /**
//     * Setup.
//     * 
//     * @throws InterruptedException
//     *             the interrupted exception
//     */
//    @Before
//    public void setup() throws InterruptedException
//    {
//
//        emf = Persistence.createEntityManagerFactory("hbaseESindexerTest");
//        em = emf.createEntityManager();
//        init();
//    }
//
//    /**
//     * Test.
//     */
//    @Test
//    public void test()
//    {
//        testAggregation();
//    }
//
//    @Test
//    public void indexDeletionTest() throws Exception
//    {
//        init();
//        Thread.sleep(1000);
//        String query = "Select min(p.age) from Person p";
//        List resultList = em.createQuery(query).getResultList();
//        Assert.assertEquals(1, resultList.size());
//        Assert.assertEquals(10.0, resultList.get(0));
//
//        Person person = em.find(Person.class, "2");
//        em.remove(person);
//        Thread.sleep(1000);
//        query = "Select min(p.age) from Person p";
//        resultList = em.createQuery(query).getResultList();
//        Assert.assertEquals(1, resultList.size());
//        Assert.assertEquals(20.0, resultList.get(0));
//
//        person = em.find(Person.class, "1");
//        em.remove(person);
//        Thread.sleep(1000);
//        query = "Select min(p.age) from Person p";
//        resultList = em.createQuery(query).getResultList();
//        Assert.assertEquals(1, resultList.size());
//        Assert.assertEquals(30.0, resultList.get(0));
//    }
//
//    @Override
//    protected void testBetweenWithExpression()
//    {
//        // do nothing.. exception in hbase client for (15 + 5)
//    }
//
//    /**
//     * Tear down after class.
//     * 
//     * @throws Exception
//     *             the exception
//     */
//    @AfterClass
//    public static void tearDownAfterClass() throws Exception
//    {
//        if (node != null)
//            node.close();
//    }
//
//    /**
//     * Tear down.
//     */
//    @After
//    public void tearDown()
//    {
//        em.createQuery("DELETE FROM Person p").executeUpdate();
//        em.close();
//        emf.close();
//    }
//
//}