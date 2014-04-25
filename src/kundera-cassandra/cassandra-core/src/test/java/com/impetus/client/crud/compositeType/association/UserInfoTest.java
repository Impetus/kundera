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

import java.nio.ByteBuffer;
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
import org.apache.cassandra.thrift.ColumnDef;
import org.apache.cassandra.thrift.IndexType;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.crud.compositeType.CassandraCompoundKey;
import com.impetus.client.crud.compositeType.CassandraEmbeddedAssociation;
import com.impetus.kundera.client.cassandra.persistence.CassandraCli;

/**
 * @author vivek.mishra
 * 
 */
public class UserInfoTest
{
    private static final String _CQL_VERSION = "2.0.0";

    private EntityManagerFactory emf;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        CassandraCli.cassandraSetUp();
        CassandraCli.initClient();
        CassandraCli.dropKeySpace("CompositeCassandra");
        loadData();
        Map<String, String> props = new HashMap<String, String>(1);
        props.put("kundera.ddl.auto.prepare", "");
        emf = Persistence.createEntityManagerFactory("composite_pu", props);
    }

    @Test
    public void onCRUD()
    {
        EntityManager em = createEM(_CQL_VERSION);

        // persist userinfo object only.
        UserInfo userInfo = new UserInfo("mevivs_info", "Vivek", "Mishra", 31, 168);
        em.persist(userInfo);

        em.clear();
        em.close();
        em = createEM(_CQL_VERSION);

        UserInfo foundUser = em.find(UserInfo.class, userInfo.getUserInfoId());
        Assert.assertNotNull(foundUser);
        Assert.assertEquals("Mishra", foundUser.getLastName());
        Assert.assertEquals("Vivek", foundUser.getFirstName());
        Assert.assertEquals(31, foundUser.getAge());
        Assert.assertEquals(0, foundUser.getHeight());

        em.remove(foundUser);

        em.clear();
        em.close();
        em = createEM(_CQL_VERSION);

        UserInfo deletedUser = em.find(UserInfo.class, userInfo.getUserInfoId());
        Assert.assertNull(deletedUser);

        em.clear();
        em.close();
        em = createEM(_CQL_VERSION);

        // Persist
        UUID timeLineId = UUID.randomUUID();
        Date currentDate = new Date();
        CassandraCompoundKey key = new CassandraCompoundKey("mevivs", 1, timeLineId);
        CassandraEmbeddedAssociation timeLine = new CassandraEmbeddedAssociation(key);
        timeLine.setTweetBody("my first tweet");
        timeLine.setTweetDate(currentDate);

        timeLine.setUserInfo(userInfo);
        em.persist(timeLine);
        em.clear();
        em.close();

        em = createEM(_CQL_VERSION);

        // Find
        CassandraEmbeddedAssociation result = em.find(CassandraEmbeddedAssociation.class, key);
        Assert.assertNotNull(result);
        Assert.assertEquals(currentDate, result.getTweetDate());
        Assert.assertEquals(timeLineId, result.getKey().getTimeLineId());
        Assert.assertEquals("Vivek", result.getUserInfo().getFirstName());
        Assert.assertEquals(31, result.getUserInfo().getAge());
        Assert.assertEquals(0, result.getUserInfo().getHeight());

        result.getUserInfo().setFirstName("Kuldeep");
        result.getUserInfo().setAge(23);

        em.merge(result);

        em.clear();
        em.close();
        em = createEM(_CQL_VERSION);
        // Find
        result = null;
        result = em.find(CassandraEmbeddedAssociation.class, key);
        Assert.assertNotNull(result);
        Assert.assertEquals(currentDate, result.getTweetDate());
        Assert.assertEquals(timeLineId, result.getKey().getTimeLineId());
        Assert.assertEquals("Kuldeep", result.getUserInfo().getFirstName());
        Assert.assertEquals(23, result.getUserInfo().getAge());
        Assert.assertEquals(0, result.getUserInfo().getHeight());

        em.remove(result);

        em.clear();
        em.close();
        em = createEM(_CQL_VERSION);

        result = em.find(CassandraEmbeddedAssociation.class, key);
        Assert.assertNull(result);

    }

    private EntityManager createEM(String cqlVersion)
    {
        EntityManager em = emf.createEntityManager();
        // Map<String, Client> clients = (Map<String, Client>) em.getDelegate();
        // Client client = clients.get("composite_pu");
        // ((CassandraClientBase) client).setCqlVersion(cqlVersion);
        return em;
    }

    @Test
    public void onQuery()
    {
        EntityManager em = createEM(_CQL_VERSION);

        // Persist
        UUID timeLineId = UUID.randomUUID();
        Date currentDate = new Date();
        CassandraCompoundKey key = new CassandraCompoundKey("mevivs", 1, timeLineId);
        CassandraEmbeddedAssociation timeLine = new CassandraEmbeddedAssociation(key);
        timeLine.setTweetBody("my first tweet");
        timeLine.setTweetDate(currentDate);

        UserInfo userInfo = new UserInfo("mevivs_info", "Vivek", "Mishra", 31, 170);
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
        Assert.assertEquals(0, results.get(0).getUserInfo().getHeight());

        em.remove(timeLine);

        em.clear();// optional,just to clear persistence cache.
        em.close();

        em = createEM(_CQL_VERSION);
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
        CassandraCli.dropKeySpace("CompositeCassandra");
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
        cfDef.setKey_validation_class("UTF8Type");
        ColumnDef columnDef = new ColumnDef(ByteBuffer.wrap("first_name".getBytes()), "UTF8Type");
        columnDef.index_type = IndexType.KEYS;
        cfDef.addToColumn_metadata(columnDef);
        ColumnDef columnDef1 = new ColumnDef(ByteBuffer.wrap("last_name".getBytes()), "UTF8Type");
        columnDef1.index_type = IndexType.KEYS;
        cfDef.addToColumn_metadata(columnDef1);
        ColumnDef columnDef2 = new ColumnDef(ByteBuffer.wrap("age".getBytes()), "Int32Type");
        columnDef2.index_type = IndexType.KEYS;
        cfDef.addToColumn_metadata(columnDef2);
        // ColumnDef columnDef3 = new
        // ColumnDef(ByteBuffer.wrap("height".getBytes()), "Int32Type");
        // columnDef3.index_type = IndexType.KEYS;
        // cfDef.addToColumn_metadata(columnDef3);
        cfDefs.add(cfDef);
        KsDef ksDef = new KsDef("CompositeCassandra", "org.apache.cassandra.locator.SimpleStrategy", cfDefs);

        if (ksDef.strategy_options == null)
        {
            ksDef.strategy_options = new LinkedHashMap<String, String>();
        }
        ksDef.strategy_options.put("replication_factor", "1");

        CassandraCli.getClient().system_add_keyspace(ksDef);

        CassandraCli.executeCqlQuery("USE \"CompositeCassandra\"", ksDef.getName());

        // \"first_name\" varchar,\"last_name\" varchar, \"age\" int,
        CassandraCli
                .executeCqlQuery(
                        "CREATE TABLE \"CompositeUserAssociation\" (\"userId\" varchar,\"tweetId\" int,\"timeLineId\" uuid, \"tweetBody\" varchar, \"tweetDate\" timestamp, \"userInfo_id\" varchar, PRIMARY KEY (\"userId\", \"tweetId\",\"timeLineId\"))",
                        ksDef.getName());

    }
}
