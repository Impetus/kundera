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
package com.impetus.client.query;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.Persistence;

import junit.framework.Assert;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.impetus.client.hbase.testingutil.HBaseTestingUtils;

/**
 * The Class HBaseESQueryTest.
 * 
 * @author Devender Yadav
 */
public class HBaseESQueryTest extends HBaseQueryBaseTest
{

    /** The property map. */
    private static Map<String, String> propertyMap = new HashMap<String, String>();

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

        propertyMap.put("kundera.indexer.class", "com.impetus.client.es.index.ESIndexer");
        emf = Persistence.createEntityManagerFactory(HBASE_PU, propertyMap);

        if (!checkIfServerRunning())
        {
            ImmutableSettings.Builder builder = ImmutableSettings.settingsBuilder();

            builder.put("path.data", "target/data");
            new NodeBuilder().settings(builder).node();
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.client.query.HBaseQueryBaseTest#setUp()
     */
    @Before
    public void setUp() throws Exception
    {
        em = emf.createEntityManager();
        persistBooks();
        Thread.sleep(2000);
    }

    /**
     * Check if server running.
     * 
     * @return true, if successful
     */
    private static boolean checkIfServerRunning()
    {
        try
        {
            Socket socket = new Socket("127.0.0.1", 9300);
            return socket.getInetAddress() != null;
        }
        catch (UnknownHostException e)
        {
            return false;
        }
        catch (IOException e)
        {
            return false;
        }
    }

    /**
     * Aggregation test.
     * 
     * @throws Exception
     *             the exception
     */
    @Test
    public void aggregationTest() throws Exception
    {

        persistBooks();
        minAggregation();
        minAggregationError();
        multiMinAggregation();
        minAggregationWithWhere();
        multiMinAggregationWithWhere();
        maxAggregation();
        multiMaxAggregation();
        maxAggregationWithWhere();
        multiMaxAggregationWithWhere();
        maxMinSameFieldAggregation();
        maxMinDiffFieldAggregation();
        multiMaxMinAggregation();
        sumAggregation();
        sumAggregationWithWhere();
        multiSumAggregation();
        multiSumAggregationWithWhere();
        avgAggregation();
        avgAggregationWithWhere();
        multiAvgAggregation();
        multiAvgAggregationWithWhere();
        minMaxSumAvgAggregation();

    }

    /**
     * Min aggregation.
     */
    public void minAggregation()
    {
        String query = "Select min(b.pages) from Book b";
        List resultList = em.createQuery(query).getResultList();
        assertES(resultList, 100.0);
    }

    /**
     * Min aggregation error.
     */
    @Test
    public void minAggregationError()
    {
        String query = "Select min(b.title)from Book b";
        try
        {
            List resultList = em.createQuery(query).getResultList();
            Assert.fail();
        }
        catch (Exception ex)
        {
            Assert.assertEquals("Aggregations can not performed over non-numeric fields.", ex.getMessage());
        }
    }

    /**
     * Min aggregation with where.
     */
    private void minAggregationWithWhere()
    {
        String query = "Select min(b.pages) from Book b where b.year>2005";
        List resultList = em.createQuery(query).getResultList();
        assertES(resultList, 300.0);
    }

    /**
     * Multi min aggregation.
     */
    public void multiMinAggregation()
    {
        String query = "Select min(b.pages), min(b.year) from Book b";
        List resultList = em.createQuery(query).getResultList();
        assertES(resultList, 100.0, 2000.0);
    }

    /**
     * Multi min aggregation with where.
     */
    private void multiMinAggregationWithWhere()
    {
        String query = "Select min(b.pages), min(b.year) from Book b where b.pages > 200";
        List resultList = em.createQuery(query).getResultList();
        assertES(resultList, 300.0, 2010.0);
    }

    /**
     * Max aggregation.
     */
    public void maxAggregation()
    {
        String query = "Select max(b.pages) from Book b";
        List resultList = em.createQuery(query).getResultList();
        assertES(resultList, 400.0);
    }

    /**
     * Max aggregation with where.
     */
    private void maxAggregationWithWhere()
    {
        String query = "Select max(b.pages) from Book b where b.year<2010";
        List resultList = em.createQuery(query).getResultList();
        assertES(resultList, 200.0);
    }

    /**
     * Multi max aggregation.
     */
    public void multiMaxAggregation()
    {
        String query = "Select max(b.pages), max(b.year) from Book b";
        List resultList = em.createQuery(query).getResultList();
        assertES(resultList, 400.0, 2015.0);

    }

    /**
     * Multi max aggregation with where.
     */
    private void multiMaxAggregationWithWhere()
    {
        String query = "Select max(b.pages), max(b.year) from Book b where b.pages < 300";
        List resultList = em.createQuery(query).getResultList();
        assertES(resultList, 200.0, 2005.0);
    }

    /**
     * Max min same field aggregation.
     */
    public void maxMinSameFieldAggregation()
    {
        String query = "Select max(b.pages), min(b.pages) from Book b";
        List resultList = em.createQuery(query).getResultList();
        assertES(resultList, 400.0, 100.0);
    }

    /**
     * Max min diff field aggregation.
     */
    public void maxMinDiffFieldAggregation()
    {
        String query = "Select max(b.pages), min(b.year) from Book b";
        List resultList = em.createQuery(query).getResultList();
        assertES(resultList, 400.0, 2000.0);
    }

    /**
     * Multi max min aggregation.
     */
    public void multiMaxMinAggregation()
    {
        String query = "Select max(b.pages), min(b.year), min(b.pages) from Book b";
        List resultList = em.createQuery(query).getResultList();
        assertES(resultList, 400.0, 2000.0, 100.0);
    }

    /**
     * Sum aggregation.
     */
    public void sumAggregation()
    {
        String query = "Select sum(b.pages) from Book b";
        List resultList = em.createQuery(query).getResultList();
        assertES(resultList, 1000.0);
    }

    /**
     * Sum aggregation with where.
     */
    private void sumAggregationWithWhere()
    {
        String query = "Select sum(b.pages) from Book b where b.year>2005";
        List resultList = em.createQuery(query).getResultList();
        assertES(resultList, 700.0);
    }

    /**
     * Multi sum aggregation.
     */
    public void multiSumAggregation()
    {
        String query = "Select sum(b.pages), sum(b.year) from Book b";
        List resultList = em.createQuery(query).getResultList();
        assertES(resultList, 1000.0, 8030.0);
    }

    /**
     * Multi sum aggregation with where.
     */
    private void multiSumAggregationWithWhere()
    {
        String query = "Select sum(b.pages), sum(b.year) from Book b where b.pages > 200";
        List resultList = em.createQuery(query).getResultList();
        assertES(resultList, 700.0, 4025.0);
    }

    /**
     * Avg aggregation.
     */
    public void avgAggregation()
    {
        String query = "Select avg(b.pages) from Book b";
        List resultList = em.createQuery(query).getResultList();
        assertES(resultList, 250.0);
    }

    /**
     * Avg aggregation with where.
     */
    private void avgAggregationWithWhere()
    {
        String query = "Select avg(b.pages) from Book b where b.year>2005";
        List resultList = em.createQuery(query).getResultList();
        assertES(resultList, 350.0);
    }

    /**
     * Multi avg aggregation.
     */
    public void multiAvgAggregation()
    {
        String query = "Select avg(b.pages), avg(b.year) from Book b";
        List resultList = em.createQuery(query).getResultList();
        assertES(resultList, 250.0, 2007.5);
    }

    /**
     * Multi avg aggregation with where.
     */
    private void multiAvgAggregationWithWhere()
    {
        String query = "Select avg(b.pages), avg(b.year) from Book b where b.pages > 200";
        List resultList = em.createQuery(query).getResultList();
        assertES(resultList, 350.0, 2012.5);
    }

    /**
     * Min max sum avg aggregation.
     */
    public void minMaxSumAvgAggregation()
    {
        String query = "Select min(b.pages), max(b.year), sum(b.pages), avg(b.year) from Book b";
        List resultList = em.createQuery(query).getResultList();
        assertES(resultList, 100.0, 2015.0, 1000.0, 2007.5);
    }

    /**
     * Min max sum avg aggregation with where.
     */
    public void minMaxSumAvgAggregationWithWhere()
    {
        String query = "Select min(b.pages), max(b.year), sum(b.pages), avg(b.year) from Book b where b.pages > 100 ";
        List resultList = em.createQuery(query).getResultList();
        assertES(resultList, 200.0, 2015.0, 900.0, 2010.0);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.client.query.HBaseQueryBaseTest#tearDown()
     */
    @After
    public void tearDown() throws Exception
    {
        EntityManager em = emf.createEntityManager();
        deleteBooks();
        em.close();
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
        emf.close();
        emf = null;
        HBaseTestingUtils.dropSchema(SCHEMA);
    }

    /**
     * Assert es.
     * 
     * @param resultList
     *            the result list
     * @param value
     *            the value
     */
    public void assertES(List resultList, double value)
    {
        Assert.assertEquals(1, resultList.size());
        Assert.assertEquals(value, resultList.get(0));
    }

    /**
     * Assert es.
     * 
     * @param resultList
     *            the result list
     * @param value1
     *            the value1
     * @param value2
     *            the value2
     */
    public void assertES(List resultList, double value1, double value2)
    {
        Assert.assertEquals(2, resultList.size());
        Assert.assertEquals(value1, resultList.get(0));
        Assert.assertEquals(value2, resultList.get(1));
    }

    /**
     * Assert es.
     * 
     * @param resultList
     *            the result list
     * @param value1
     *            the value1
     * @param value2
     *            the value2
     * @param value3
     *            the value3
     */
    public void assertES(List resultList, double value1, double value2, double value3)
    {
        Assert.assertEquals(3, resultList.size());
        Assert.assertEquals(value1, resultList.get(0));
        Assert.assertEquals(value2, resultList.get(1));
        Assert.assertEquals(value3, resultList.get(2));
    }

    /**
     * Assert es.
     * 
     * @param resultList
     *            the result list
     * @param value1
     *            the value1
     * @param value2
     *            the value2
     * @param value3
     *            the value3
     * @param value4
     *            the value4
     */
    public void assertES(List resultList, double value1, double value2, double value3, double value4)
    {
        Assert.assertEquals(4, resultList.size());
        Assert.assertEquals(value1, resultList.get(0));
        Assert.assertEquals(value2, resultList.get(1));
        Assert.assertEquals(value3, resultList.get(2));
        Assert.assertEquals(value4, resultList.get(3));
    }

}