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
import java.util.List;
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
 * @author Kuldeep.Mishra
 * 
 */
public class UserAndPasswordTest
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
        for (int i = 1; i < 10; i++)
        {
            UserAndPassword user = new UserAndPassword();
            user.setId(i + "");
            user.setUserName("KK" + i);
            user.setFirstName("Kuldeep" + i);
            user.setLastName("Mishra" + i);
            user.setPassword("xxx" + i);
            em.persist(user);
        }
        em.clear();
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
        for (int i = 1; i < 10; i++)
        {
            em.remove(em.find(UserAndPassword.class, i + ""));
        }
        em.close();
        emf.close();
        cli.stopCluster(null);
    }

    @Test
    public void testWithSameEM()
    {
        try
        {
            String query = "select u from UserAndPassword u where u.userName=KK5";
            com.impetus.kundera.query.Query q = (com.impetus.kundera.query.Query) em
                    .createQuery(query);
            q.setFetchSize(1);
            Iterator<UserAndPassword> results = q.iterate();
            while (results.hasNext())
            {
                UserAndPassword user = results.next();
                Assert.assertNotNull(user);
                Assert.assertEquals("KK5", user.getUserName());
                Assert.assertEquals("Kuldeep5", user.getFirstName());
                Assert.assertEquals("Mishra5", user.getLastName());
                Assert.assertEquals("xxx5", user.getPassword());
                Assert.assertEquals("5", user.getId());
            }

            query = "select u from UserAndPassword u where u.userName=KK6";
            q = (com.impetus.kundera.query.Query) em.createQuery(query);
            q.setFetchSize(1);
            results = q.iterate();
            while (results.hasNext())
            {
                UserAndPassword user = results.next();
                Assert.assertNotNull(user);
                Assert.assertEquals("KK6", user.getUserName());
                Assert.assertEquals("Kuldeep6", user.getFirstName());
                Assert.assertEquals("Mishra6", user.getLastName());
                Assert.assertEquals("xxx6", user.getPassword());
                Assert.assertEquals("6", user.getId());
            }
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
            Assert.fail(ex.getMessage());
        }
    }

    @Test
    public void testWithDifferentEM()
    {
        try
        {
            String query = "select u from UserAndPassword u where u.userName=KK5";
            com.impetus.kundera.query.Query q = (com.impetus.kundera.query.Query) em
                    .createQuery(query);
            q.setFetchSize(1);
            Iterator<UserAndPassword> results = q.iterate();
            while (results.hasNext())
            {
                UserAndPassword user = results.next();
                Assert.assertNotNull(user);
                Assert.assertEquals("KK5", user.getUserName());
                Assert.assertEquals("Kuldeep5", user.getFirstName());
                Assert.assertEquals("Mishra5", user.getLastName());
                Assert.assertEquals("xxx5", user.getPassword());
                Assert.assertEquals("5", user.getId());
            }

            // query = "select u from UserAndPassword u";
            // q = (com.impetus.kundera.query.Query<UserAndPassword>)
            // em.createQuery(query);
            //
            // Map<String, Client> clients = (Map<String, Client>)
            // em.getDelegate();
            // HBaseClient client = (HBaseClient) clients.get("hbaseTest");
            //
            // Filter filter = new PrefixFilter(Bytes.toBytes("KK"));
            //
            // client.setFilter(new KeyOnlyFilter());
            // client.addFilter("city_similarity", filter);
            //
            // q.setFetchSize(4);
            // results = q.iterate();
            // while (results.hasNext())
            // {
            // List<UserAndPassword> users =
            // ((com.impetus.client.hbase.query.ResultIterator<UserAndPassword>)
            // results)
            // .next(2);
            // Assert.assertNotNull(users);
            // Assert.assertEquals(2, users.size());
            // }
            // em.close();

            em = emf.createEntityManager();
            query = "select u from UserAndPassword u where u.userName=KK6";
            q = (com.impetus.kundera.query.Query) em.createQuery(query);
            q.setFetchSize(1);
            results = q.iterate();
            while (results.hasNext())
            {
                UserAndPassword user = results.next();
                Assert.assertNotNull(user);
                Assert.assertEquals("KK6", user.getUserName());
                Assert.assertEquals("Kuldeep6", user.getFirstName());
                Assert.assertEquals("Mishra6", user.getLastName());
                Assert.assertEquals("xxx6", user.getPassword());
                Assert.assertEquals("6", user.getId());
            }
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
            Assert.fail(ex.getMessage());
        }
    }
}
