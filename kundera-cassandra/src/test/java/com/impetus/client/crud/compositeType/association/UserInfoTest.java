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
package com.impetus.client.crud.compositeType.association;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import junit.framework.Assert;

import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.crud.compositeType.CassandraCompositeTypeTest;
import com.impetus.client.crud.compositeType.CassandraCompoundKey;
import com.impetus.client.crud.compositeType.CassandraEmbeddedAssociation;
import com.impetus.client.persistence.CassandraCli;

/**
 * @author vivek.mishra
 * 
 */
public class UserInfoTest
{

    private EntityManagerFactory emf;

    private static final Log logger = LogFactory.getLog(CassandraCompositeTypeTest.class);

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        CassandraCli.cassandraSetUp();
        CassandraCli.initClient();
        loadData();
        Map<String, String> props = new HashMap<String, String>(1);
        props.put("kundera.ddl.auto.prepare", "");
        emf = Persistence.createEntityManagerFactory("composite_pu",props);
    }


    @Test
    public void onCRUD()
    {
        EntityManager em = emf.createEntityManager();

        // Persist
        UUID timeLineId = UUID.randomUUID();
        Date currentDate = new Date();
        CassandraCompoundKey key = new CassandraCompoundKey("mevivs", 1, timeLineId);
        CassandraEmbeddedAssociation timeLine = new CassandraEmbeddedAssociation(key);
        timeLine.setTweetBody("my first tweet");
        timeLine.setTweetDate(new Date());

        UserInfo userInfo = new UserInfo("mevivs_info", "Vivek", "Mishra", 31);
        timeLine.setUserInfo(userInfo);
        em.persist(timeLine);
        em.clear();

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
        result = em.find(CassandraEmbeddedAssociation.class, key);
        Assert.assertNull(result);

    }

     @Test
    public void onQuery()
    {
        EntityManager em = emf.createEntityManager();

        // Persist
        UUID timeLineId = UUID.randomUUID();
        Date currentDate = new Date();
        CassandraCompoundKey key = new CassandraCompoundKey("mevivs", 1, timeLineId);
        CassandraEmbeddedAssociation timeLine = new CassandraEmbeddedAssociation(key);
        timeLine.setTweetBody("my first tweet");
        timeLine.setTweetDate(new Date());

        UserInfo userInfo = new UserInfo("mevivs_info", "Vivek", "Mishra", 31);
        timeLine.setUserInfo(userInfo);
        em.persist(timeLine);

        em.clear(); // optional,just to clear persistence cache.
        
        final String noClause = "Select t from CassandraEmbeddedAssociation t";
        
        Query query = em.createQuery(noClause);
        List<CassandraEmbeddedAssociation> results = query.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals("Vivek", results.get(0).getUserInfo().getFirstName());
        Assert.assertEquals(31, results.get(0).getUserInfo().getAge());

        em.remove(timeLine);

        em.clear();// optional,just to clear persistence cache.
        
        UserInfo user_Info = em.find(UserInfo.class, "mevivs_info");
        Assert.assertNull(user_Info);
    }


    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
        CassandraCli.dropKeySpace("CompositeCassandra");
        emf.close();
    }

    /**
     * Loads data.
     * 
     * @throws InvalidRequestException   
     * @throws SchemaDisagreementException
     * @throws TException
     */
    private void loadData() throws InvalidRequestException, SchemaDisagreementException, TException
    {
        List<CfDef> cfDefs = new ArrayList<CfDef>();
        CfDef cfDef = new CfDef("CompositeCassandra", "UserInfo");
        cfDef.setKey_validation_class("UTF8Type");
        cfDef.setDefault_validation_class("UTF8Type");
        cfDef.setComparator_type("UTF8Type");
        cfDefs.add(cfDef);
        org.apache.cassandra.thrift.KsDef ksDef = new org.apache.cassandra.thrift.KsDef("CompositeCassandra",
                "org.apache.cassandra.locator.SimpleStrategy", cfDefs);
     
        if (ksDef.strategy_options == null)
        {
            ksDef.strategy_options = new LinkedHashMap<String, String>();
        }
        ksDef.strategy_options.put("replication_factor", "1");
        
        CassandraCli.getClient().system_add_keyspace(ksDef);

        CassandraCli.executeCqlQuery("USE \"CompositeCassandra\"");

        CassandraCli
                .executeCqlQuery("CREATE TABLE \"CompositeUser\" (\"userId\" varchar,\"tweetId\" int,\"timeLineId\" uuid, \"tweetBody\" varchar, \"tweetDate\" timestamp, \"userInfo_id\" varchar,\"first_name\" varchar,\"last_name\" varchar, \"age\" int, PRIMARY KEY (\"userId\", \"tweetId\",\"timeLineId\"))");

    }
}
