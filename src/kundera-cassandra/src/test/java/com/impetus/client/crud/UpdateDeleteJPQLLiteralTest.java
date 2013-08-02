/*******************************************************************************
 * * Copyright 2012 Impetus Infotech.
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
package com.impetus.client.crud;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import junit.framework.Assert;

import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.cassandra.common.CassandraConstants;
import com.impetus.client.persistence.CassandraCli;

/**
 * Junit test case for Update/Delete via query test.
 * 
 * @author vivek.mishra
 * 
 */
public class UpdateDeleteJPQLLiteralTest
{

    private static final String SEC_IDX_CASSANDRA_TEST = "CassandraXmlPropertyTest";

    private EntityManagerFactory emf;

    private EntityManager em;

    /**
     * Setup
     * 
     * @throws IOException
     * @throws TException
     * @throws InvalidRequestException
     * @throws UnavailableException
     * @throws TimedOutException
     * @throws SchemaDisagreementException
     */
    @Before
    public void setUp() throws IOException, TException, InvalidRequestException, UnavailableException,
            TimedOutException, SchemaDisagreementException
    {
        CassandraCli.cassandraSetUp();
        CassandraCli.initClient();
        Map propertyMap = new HashMap();
        propertyMap.put(CassandraConstants.CQL_VERSION, CassandraConstants.CQL_VERSION_3_0);
        emf = Persistence.createEntityManagerFactory(SEC_IDX_CASSANDRA_TEST, propertyMap);
        em = emf.createEntityManager();
    }

    /**
     * Test case to demonstrate update via query use case.
     * 
     */
    @Test
    public void test()
    {
        MyTestEntity entity = new MyTestEntity();

        UUID key = UUID.randomUUID();
        entity.setKey(key);
        entity.setUrl("url");
        em.persist(entity);

        // With JPA where clause and update parameter.
        String jpql = "update MyTestEntity c set c.url = :url where c.key =:key";

        Query query = em.createQuery(jpql);
        query.setParameter("url", "firstUpdate");
        query.setParameter("key", key);
        query.executeUpdate();

        em.clear();

        MyTestEntity found = em.find(MyTestEntity.class, key);

        Assert.assertNotNull(found);
        Assert.assertEquals("firstUpdate", found.getUrl());

        // Static where clause with update parameter
        jpql = "update MyTestEntity c set c.url = :url where c.key =" + key.toString();
        query = em.createQuery(jpql);
        query.setParameter("url", "secondUpdate");
        query.executeUpdate();

        found = em.find(MyTestEntity.class, key);

        Assert.assertNotNull(found);
        Assert.assertEquals("secondUpdate", found.getUrl());
    }

    /**
     * JPA String literal test for Select, Update and Delete.
     */
    @Test
    public void jpaStringLiteralTest()
    {
        MyTestEntity entity = new MyTestEntity();

        UUID key = UUID.randomUUID();
        entity.setKey(key);
        entity.setUrl("url");
        em.persist(entity);

        String staticJpaQueryEnclosing = "Select e from MyTestEntity e where e.url = 'url'";
        Query query = em.createQuery(staticJpaQueryEnclosing);

        List<MyTestEntity> result = query.getResultList();

        Assert.assertNotNull(result);
        Assert.assertEquals(1, result.size());
        Assert.assertEquals("url", result.get(0).getUrl());

        String withoutEnclosed = "Select e from MyTestEntity e where e.url = url";
        query = em.createQuery(withoutEnclosed);

        result = query.getResultList();

        Assert.assertNotNull(result);
        Assert.assertEquals(1, result.size());
        Assert.assertEquals("url", result.get(0).getUrl());

        MyTestEntity found = em.find(MyTestEntity.class, key);

        Assert.assertNotNull(found);
        Assert.assertEquals("url", result.get(0).getUrl());

        found.setUrl("url's");

        em.merge(found);

        String dynamicQuery = "Select e from MyTestEntity e where e.url = 'url''s'";

        query = em.createQuery(dynamicQuery);

        result = query.getResultList();

        Assert.assertNotNull(result);
        Assert.assertEquals(1, result.size());
        Assert.assertEquals("url's", result.get(0).getUrl());

        dynamicQuery = "Select e from MyTestEntity e where e.url = :url";
        query = em.createQuery(dynamicQuery);
        query.setParameter("url", "url's");

        result = query.getResultList();

        Assert.assertNotNull(result);
        Assert.assertEquals(1, result.size());
        Assert.assertEquals("url's", result.get(0).getUrl());

        // Update by query
        // With JPA where clause and update parameter.
        String jpql = "update MyTestEntity c set c.url = :url where c.key =" + key.toString();
        query = em.createQuery(jpql);
        query.setParameter("url", "updateUrl");
        query.executeUpdate();

        em.clear();

        found = em.find(MyTestEntity.class, key);

        Assert.assertNotNull(found);
        Assert.assertEquals("updateUrl", found.getUrl());

        jpql = "update MyTestEntity c set c.url = 'enclosedUrl' where c.key = :key";
        query = em.createQuery(jpql);
        query.setParameter("key", key);
        query.executeUpdate();

        found = em.find(MyTestEntity.class, key);

        Assert.assertNotNull(found);
        Assert.assertEquals("enclosedUrl", found.getUrl());

        jpql = "update MyTestEntity c set c.url = 'enclosedUrl''s' where c.key = :key";
        query = em.createQuery(jpql);
        query.setParameter("key", key);
        query.executeUpdate();

        found = em.find(MyTestEntity.class, key);
        Assert.assertEquals("enclosedUrl's", found.getUrl());

        // Delete by non column.

        jpql = "Delete c from MyTestEntity c where c.url = :url";
        query = em.createQuery(jpql);
        query.setParameter("url", "enclosedUrl's");
        query.executeUpdate();

        found = em.find(MyTestEntity.class, key);
        Assert.assertNull(found);

    }

    @After
    public void tearDown()
    {
        CassandraCli.dropKeySpace("KunderaCassandraXmlTest");
        em.close();
        emf.close();
    }
}
