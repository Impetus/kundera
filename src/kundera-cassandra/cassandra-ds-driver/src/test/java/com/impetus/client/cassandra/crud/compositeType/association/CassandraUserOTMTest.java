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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.cassandra.common.CassandraConstants;
import com.impetus.kundera.client.cassandra.persistence.CassandraCli;

/**
 * User One-To-Many junit.
 * @author vivek.mishra
 * 
 */
public class CassandraUserOTMTest
{

    private static final String _KEYSPACE = "KunderaExamples";

    private EntityManagerFactory emf;

    private EntityManager em;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        CassandraCli.cassandraSetUp();
        CassandraCli.createKeySpace(_KEYSPACE);
        Map<String, Object> puPropertiesObj = new HashMap<String, Object>();
   
       puPropertiesObj.put(CassandraConstants.THRIFT_PORT, "9160");
       
       
        emf = Persistence.createEntityManagerFactory("ds_pu",puPropertiesObj);
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
        CassandraCli.dropKeySpace(_KEYSPACE);
        emf.close();
    }

    @Test
    public void test()
    {
        em = emf.createEntityManager();

        Set<CassandraAddressUniOTM> addresses = new HashSet<CassandraAddressUniOTM>();

        CassandraAddressUniOTM address1 = new CassandraAddressUniOTM();
        address1.setAddressId("a");
        address1.setStreet("my street");
        CassandraAddressUniOTM address2 = new CassandraAddressUniOTM();
        address2.setAddressId("b");
        address2.setStreet("my new street");

        addresses.add(address1);
        addresses.add(address2);

        CassandraUserUniOTM userUniOTM = new CassandraUserUniOTM();
        userUniOTM.setPersonId("1");
        userUniOTM.setPersonName("kuldeep");
        userUniOTM.setAddresses(addresses);

        em.persist(userUniOTM);

        em.clear();

        CassandraUserUniOTM foundObject = em.find(CassandraUserUniOTM.class, "1");
        Assert.assertNotNull(foundObject);
        Assert.assertEquals(2, foundObject.getAddresses().size());

        Query q = em.createQuery("Select u from CassandraUserUniOTM u");
        List<CassandraUserUniOTM> users = q.getResultList();
        Assert.assertNotNull(users);
        Assert.assertEquals(1, users.size());
        Assert.assertNotNull(users.get(0));
        Assert.assertEquals("kuldeep", users.get(0).getPersonName());
        Assert.assertEquals("1", users.get(0).getPersonId());
        Assert.assertNotNull(users.get(0).getAddresses());
        Assert.assertEquals(2, users.get(0).getAddresses().size());

        em.remove(foundObject);

        foundObject = em.find(CassandraUserUniOTM.class, "1");
        Assert.assertNull(foundObject);
    }
}
