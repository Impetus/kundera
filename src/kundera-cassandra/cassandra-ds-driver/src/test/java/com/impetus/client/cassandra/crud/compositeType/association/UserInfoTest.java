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
package com.impetus.client.cassandra.crud.compositeType.association;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.cassandra.CassandraClientBase;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.cassandra.persistence.CassandraCli;

/**
 * User info test.
 * 
 * @author vivek.mishra
 * 
 */
public class UserInfoTest
{
    private EntityManagerFactory emf;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        CassandraCli.cassandraSetUp();
        CassandraCli.initClient();
        CassandraCli.dropKeySpace("KunderaExamples");
//        loadData();
        Map<String, String> props = new HashMap<String, String>(1);
//        props.put("kundera.ddl.auto.prepare", "");
        emf = Persistence.createEntityManagerFactory("ds_pu", props);
    }

    @Test
    public void onCRUD()
    {
        EntityManager em = createEM();

        // Persist
        UUID timeLineId = UUID.randomUUID();
        Date currentDate = new Date();
        CassandraCompoundKey key = new CassandraCompoundKey("mevivs", 1, timeLineId);
        CassandraEmbeddedAssociation timeLine = new CassandraEmbeddedAssociation(key);
        timeLine.setTweetBody("my first tweet");
        timeLine.setTweetDate(currentDate);

        UserInfo userInfo = new UserInfo("mevivs_info", "Vivek", "Mishra", 31);
        timeLine.setUserInfo(userInfo);
        em.persist(timeLine);
        em.clear();
        em.close();

        em = createEM();

        // Find
        CassandraEmbeddedAssociation result = em.find(CassandraEmbeddedAssociation.class, key);
        Assert.assertNotNull(result);
        Assert.assertEquals(currentDate, result.getTweetDate());
        Assert.assertEquals(timeLineId, result.getKey().getTimeLineId());
        Assert.assertEquals("Vivek", result.getUserInfo().getFirstName());
        Assert.assertEquals(31, result.getUserInfo().getAge());

        result.getUserInfo().setFirstName("Kuldeep");
        result.getUserInfo().setAge(23);

        em.merge(result);

        em.clear();
        em.close();
        em = createEM();
        // Find
        result = null;
        result = em.find(CassandraEmbeddedAssociation.class, key);
        Assert.assertNotNull(result);
        Assert.assertEquals(currentDate, result.getTweetDate());
        Assert.assertEquals(timeLineId, result.getKey().getTimeLineId());
        Assert.assertEquals("Kuldeep", result.getUserInfo().getFirstName());
        Assert.assertEquals(23, result.getUserInfo().getAge());

        em.remove(result);

        em.clear();
        em.close();

        em = createEM();
        result = em.find(CassandraEmbeddedAssociation.class, key);
        Assert.assertNull(result);

    }

    private EntityManager createEM()
    {
        EntityManager em = emf.createEntityManager();
        Map<String, Client> clients = (Map<String, Client>) em.getDelegate();
        Client client = clients.get("ds_pu");
        ((CassandraClientBase) client).setCqlVersion("3.0.0");
        return em;
    }

    @Test
    public void onQuery()
    {
        EntityManager em = createEM();

        // Persist
        UUID timeLineId = UUID.randomUUID();
        Date currentDate = new Date();
        CassandraCompoundKey key = new CassandraCompoundKey("mevivs", 1, timeLineId);
        CassandraEmbeddedAssociation timeLine = new CassandraEmbeddedAssociation(key);
        timeLine.setTweetBody("my first tweet");
        timeLine.setTweetDate(currentDate);

        UserInfo userInfo = new UserInfo("mevivs_info", "Vivek", "Mishra", 31);
        timeLine.setUserInfo(userInfo);
        em.persist(timeLine);

        em.clear(); // optional,just to clear persistence cache.
        em.flush();

        final String noClause = "Select t from CassandraEmbeddedAssociation t";

        Query query = em.createQuery(noClause);
        List<CassandraEmbeddedAssociation> results = query.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals("Vivek", results.get(0).getUserInfo().getFirstName());
        Assert.assertEquals(31, results.get(0).getUserInfo().getAge());

        em.remove(timeLine);

        em.clear();// optional,just to clear persistence cache.
        em.close();

        em = createEM();
        UserInfo user_Info = em.find(UserInfo.class, "mevivs_info");
        Assert.assertNull(user_Info);
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
        emf.close();
        CassandraCli.dropKeySpace("KunderaExamples");
    }
}
