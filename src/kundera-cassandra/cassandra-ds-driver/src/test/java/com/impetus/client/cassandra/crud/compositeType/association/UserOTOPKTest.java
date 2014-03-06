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

import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.client.cassandra.persistence.CassandraCli;

/**
 * One-To-One by primary key test.
 * 
 * @author vivek.mishra
 * 
 */
public class UserOTOPKTest
{
    private String keyspace = "KunderaExamples";

    private EntityManagerFactory emf;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        CassandraCli.cassandraSetUp();
        CassandraCli.initClient();
        emf = Persistence.createEntityManagerFactory("ds_pu");
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
        CassandraCli.dropKeySpace(keyspace);
        emf.close();
    }

    @Test
    public void testOTOPk()
    {
        EntityManager em = emf.createEntityManager();

        AddressOTOPK addressOTOPK = new AddressOTOPK();
        addressOTOPK.setAddressId("xyz");
        addressOTOPK.setStreet("STTRRREEEETTTTT");

        UserOTOPK userOTOPK = new UserOTOPK();
        userOTOPK.setPersonId("1234");
        userOTOPK.setPersonName("Kuldeep");
        userOTOPK.setAddress(addressOTOPK);

        em.persist(userOTOPK);

        em.clear();

        UserOTOPK otopk = em.find(UserOTOPK.class, "1234");
        Assert.assertNotNull(otopk);
        Assert.assertNotNull(otopk.getAddress());
        Assert.assertEquals("Kuldeep", otopk.getPersonName());
        Assert.assertEquals("1234", otopk.getAddress().getPersonId());
        Assert.assertEquals("xyz", otopk.getAddress().getAddressId());
        Assert.assertEquals("STTRRREEEETTTTT", otopk.getAddress().getStreet());

        em.clear();

        Query q = em.createQuery("Select u from UserOTOPK u");
        List<UserOTOPK> users = q.getResultList();
        Assert.assertNotNull(users);
        Assert.assertEquals(1, users.size());
        Assert.assertNotNull(users.get(0));
        Assert.assertNotNull(users.get(0).getAddress());
        Assert.assertEquals("STTRRREEEETTTTT", users.get(0).getAddress().getStreet());
        em.close();

    }
}
