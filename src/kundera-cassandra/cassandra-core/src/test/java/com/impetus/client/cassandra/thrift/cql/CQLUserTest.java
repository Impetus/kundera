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
package com.impetus.client.cassandra.thrift.cql;

import java.util.HashMap;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.cassandra.common.CassandraConstants;
import com.impetus.kundera.client.cassandra.persistence.CassandraCli;

/**
 * @author Kuldeep
 * 
 */
public class CQLUserTest
{

    private EntityManagerFactory emf;

    private String persistenceUnit = "cassandra_cql";

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        CassandraCli.cassandraSetUp();
        Map propertyMap = new HashMap();
        propertyMap.put(CassandraConstants.CQL_VERSION, CassandraConstants.CQL_VERSION_3_0);
        emf = Persistence.createEntityManagerFactory(persistenceUnit, propertyMap);
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
        emf.close();
        emf = null;
    }

    @Test
    public void testCRUD()
    {
        EntityManager em = emf.createEntityManager();
        CQLUser user = new CQLUser();
        user.setId(1);
        user.setName("Kuldeep");
        user.setAge(24);

        em.persist(user);

        em.clear();

        CQLUser foundUser = em.find(CQLUser.class, 1);
        Assert.assertNotNull(foundUser);
        Assert.assertEquals(1, foundUser.getId());
        Assert.assertEquals(24, foundUser.getAge());
        Assert.assertEquals("Kuldeep", foundUser.getName());

        foundUser.setName(null);

        em.merge(foundUser);

        em.clear();

        CQLUser mergedUser = em.find(CQLUser.class, 1);
        Assert.assertNotNull(mergedUser);
        Assert.assertEquals(1, mergedUser.getId());
        Assert.assertEquals(24, mergedUser.getAge());
        Assert.assertEquals(null, mergedUser.getName());

        em.remove(mergedUser);

        CQLUser deletedUser = em.find(CQLUser.class, 1);
        Assert.assertNull(deletedUser);

        em.clear();

        deletedUser = em.find(CQLUser.class, 1);
        Assert.assertNull(deletedUser);
    }
}
