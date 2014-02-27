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
package com.impetus.client.crud.compositeType;


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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.cassandra.CassandraClientBase;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.cassandra.persistence.CassandraCli;

/**
 * Junit test case for Compound/Composite key.
 * 
 * @author chhavi.gangwal
 * 
 */

public class CassandraCompositeTypeOrderByTest
{

    private static final String _PU = "composite_pu";

    private EntityManagerFactory emf;

    /** The Constant logger. */
    private static final Logger logger = LoggerFactory.getLogger(CassandraCompositeTypeOrderByTest.class);


    private Date currentDate = new Date();

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        CassandraCli.cassandraSetUp();
        Map<String, Object> puProperties = new HashMap<String, Object>();
        puProperties.put("kundera.ddl.auto.prepare", "create-drop");
        

        emf = Persistence.createEntityManagerFactory(_PU, puProperties);
    }

    @Test
    public void onQuery() throws Exception
    {

        EntityManager em = emf.createEntityManager();
        UUID timeLineId = UUID.randomUUID();
        CassandraCompoundKey key = new CassandraCompoundKey("mevivs", 1, timeLineId);

        Map<String, Client> clients = (Map<String, Client>) em.getDelegate();
        Client client = clients.get(_PU);
        ((CassandraClientBase) client).setCqlVersion("3.0.0");

        CassandraPrimeUser user = new CassandraPrimeUser(key);
        user.setTweetBody("my first tweet");
        user.setTweetDate(currentDate);
        em.persist(user);

        CassandraCompoundKey key1 = new CassandraCompoundKey("mevivs1", 12, timeLineId);
        user = new CassandraPrimeUser(key1);
        user.setTweetBody("my second tweet");
        user.setTweetDate(currentDate);
        em.persist(user);

        em.flush(); // optional,just to clear persistence cache.

        em.clear();

        final String noClause = "Select u from CassandraPrimeUser u";

        // query with no clause.
        Query q = em.createQuery(noClause);
        List<CassandraPrimeUser> results = q.getResultList();
        Assert.assertEquals(2, results.size());
        
        em.close();

    }

    /**
     * CompositeUserDataType
     * 
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
        emf.close();
        CassandraCli.dropKeySpace("CompositeCassandra");
    }

}
