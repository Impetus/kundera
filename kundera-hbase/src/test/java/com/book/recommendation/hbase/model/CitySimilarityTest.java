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
package com.book.recommendation.hbase.model;

import java.util.Iterator;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.hbase.HBaseClient;
import com.impetus.client.hbase.junits.HBaseCli;
import com.impetus.kundera.client.Client;

/**
 * 
 * @author Kuldeep.Mishra
 * 
 */
public class CitySimilarityTest
{

    private EntityManagerFactory emf;

    private EntityManager em;

    private HBaseCli cli = new HBaseCli();

    @Before
    public void setUp() throws Exception
    {
        cli.startCluster();
        emf = Persistence.createEntityManagerFactory("hbaseTest");
        em = emf.createEntityManager();
        BookInfo info1 = new BookInfo();
        info1.setBookId("book1");
        info1.setAuthor("Vivek");

        BookInfo info2 = new BookInfo();
        info2.setBookId("book2");
        info2.setAuthor("Amresh");

        CitySimilarity city1 = new CitySimilarity();
        city1.setId("100_1");
        city1.setBookInfo(info1);

        CitySimilarity city2 = new CitySimilarity();
        city2.setId("100_2");
        city2.setBookInfo(info2);

        em.persist(city1);
        em.persist(city2);

        em.flush();
        em.clear();
    }

    @After
    public void tearDown() throws Exception
    {
        em.remove(em.find(CitySimilarity.class, "100_1"));
        em.remove(em.find(CitySimilarity.class, "100_2"));
        em.close();
        emf.close();
        cli.stopCluster(null);
    }

    @Test
    public void testIterator()
    {
        String query2 = "select u from CitySimilarity u";
        com.impetus.kundera.query.Query<CitySimilarity> queryObject = (com.impetus.kundera.query.Query<CitySimilarity>) em
                .createQuery(query2);

        queryObject.setFetchSize(10);

        Iterator<CitySimilarity> resultIterator = queryObject.iterate();
        CitySimilarity cityS = null;
        int counter = 0;
        while (resultIterator.hasNext())
        {
            counter++;
            cityS = resultIterator.next();
            Assert.assertNotNull(cityS);
            Assert.assertNotNull(cityS.getId());
            Assert.assertNotNull(cityS.getBookInfo());
            Assert.assertNotNull(cityS.getBookInfo().getBookId());
        }
        Assert.assertEquals(2, counter);
    }

    @Test
    public void testIteratorWithOneFilter()
    {
        Map<String, Client> clients = (Map<String, Client>) em.getDelegate();

        HBaseClient client = (HBaseClient) clients.get("hbaseTest");

        Filter filter = new PrefixFilter(Bytes.toBytes("100" + "_"));

        client.setFilter(new KeyOnlyFilter());
        client.addFilter("city_similarity", filter);

        String query2 = "select u from CitySimilarity u";
        com.impetus.kundera.query.Query<CitySimilarity> queryObject = (com.impetus.kundera.query.Query<CitySimilarity>) em
                .createQuery(query2);

        queryObject.setFetchSize(10);

        Iterator<CitySimilarity> resultIterator = queryObject.iterate();
        CitySimilarity cityS = null;
        int counter = 0;
        while (resultIterator.hasNext())
        {
            counter++;
            cityS = resultIterator.next();
            Assert.assertNotNull(cityS);
            Assert.assertNotNull(cityS.getId());
            Assert.assertNull(cityS.getBookInfo());
        }
        Assert.assertEquals(2, counter);
    }

    @Test
    public void testIteratorWithTwoFilter()
    {
        Map<String, Client> clients = (Map<String, Client>) em.getDelegate();

        HBaseClient client = (HBaseClient) clients.get("hbaseTest");

        Filter filter = new PrefixFilter(Bytes.toBytes("100" + "_"));

        client.addFilter("city_similarity", filter);
        client.addFilter("bookinfo", new KeyOnlyFilter());

        String query2 = "select u from CitySimilarity u";
        com.impetus.kundera.query.Query<CitySimilarity> queryObject = (com.impetus.kundera.query.Query<CitySimilarity>) em
                .createQuery(query2);

        queryObject.setFetchSize(10);

        Iterator<CitySimilarity> resultIterator = queryObject.iterate();
        CitySimilarity cityS = null;
        int counter = 0;
        while (resultIterator.hasNext())
        {
            counter++;
            cityS = resultIterator.next();
            Assert.assertNotNull(cityS);
            Assert.assertNotNull(cityS.getBookInfo());
            Assert.assertNotNull(cityS.getBookInfo().getBookId());
            Assert.assertNull(cityS.getBookInfo().getTitle());
            Assert.assertNull(cityS.getBookInfo().getAuthor());
            Assert.assertNull(cityS.getBookInfo().getImageurll());
            Assert.assertNull(cityS.getBookInfo().getImageurlm());
            Assert.assertNull(cityS.getBookInfo().getImageurls());
            Assert.assertNull(cityS.getBookInfo().getMd5());
            Assert.assertNull(cityS.getBookInfo().getPublisher());
            Assert.assertNull(cityS.getBookInfo().getYearofpub());
        }
        Assert.assertEquals(2, counter);
    }
}
