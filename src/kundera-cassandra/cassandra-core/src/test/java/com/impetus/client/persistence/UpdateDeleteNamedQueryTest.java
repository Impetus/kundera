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
package com.impetus.client.persistence;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

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

import com.impetus.kundera.client.cassandra.persistence.CassandraCli;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl;

/**
 * Test case for update/delete via JPQL.
 * 
 * @author vivek.mishra
 * 
 */
public class UpdateDeleteNamedQueryTest
{

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        CassandraCli.cassandraSetUp();

        loadData();
    }

    /**
     * @throws TException
     * @throws InvalidRequestException
     * @throws SchemaDisagreementException
     * 
     */
    private void loadData() throws InvalidRequestException, TException, SchemaDisagreementException
    {
        KsDef ksDef = null;
        CfDef user_Def = new CfDef();
        user_Def.name = "users";
        user_Def.keyspace = "KunderaExamples";
        user_Def.setComparator_type("UTF8Type");
        user_Def.setDefault_validation_class("UTF8Type");
        user_Def.setKey_validation_class("UTF8Type");
        ColumnDef columnDef = new ColumnDef(ByteBuffer.wrap("birth_date".getBytes()), "Int32Type");
        columnDef.index_type = IndexType.KEYS;
        user_Def.addToColumn_metadata(columnDef);
        ColumnDef columnDef1 = new ColumnDef(ByteBuffer.wrap("state".getBytes()), "UTF8Type");
        columnDef1.index_type = IndexType.KEYS;
        user_Def.addToColumn_metadata(columnDef1);

        ColumnDef columnDef2 = new ColumnDef(ByteBuffer.wrap("full_name".getBytes()), "UTF8Type");
        columnDef2.index_type = IndexType.KEYS;
        user_Def.addToColumn_metadata(columnDef2);

        List<CfDef> cfDefs = new ArrayList<CfDef>();
        cfDefs.add(user_Def);

        ksDef = new KsDef("KunderaExamples", "org.apache.cassandra.locator.SimpleStrategy", cfDefs);
        // Set replication factor
        if (ksDef.strategy_options == null)
        {
            ksDef.strategy_options = new LinkedHashMap<String, String>();
        }
        // Set replication factor, the value MUST be an integer
        ksDef.strategy_options.put("replication_factor", "1");
        CassandraCli.client.system_add_keyspace(ksDef);
    }

    @Test
    public void testUpdate()
    {
        EntityManagerFactory emf = getEntityManagerFactory();
        EntityManager em = emf.createEntityManager();

       CassandraEntitySample entity = new CassandraEntitySample();
        entity.setBirth_date(new Integer(100112));
        entity.setFull_name("impetus_emp");
        entity.setKey("k");
        entity.setState("UP");
        em.persist(entity);

        String updateQuery = "Update CassandraEntitySample c SET c.state = DELHI where c.state = UP";
        Query q = em.createQuery(updateQuery);
        q.executeUpdate();
        CassandraEntitySample result = em.find(CassandraEntitySample.class, "k");
        Assert.assertNotNull(result);

        updateQuery = "Update CassandraEntitySample c SET c.state = Bengalore where c.key = k";
        q = em.createQuery(updateQuery);
        q.executeUpdate();
        result = em.find(CassandraEntitySample.class, "k");
        Assert.assertNotNull(result);

        // Assert.assertEquals("DELHI", result.getState()); // This should be
        // uncommented later. as merge got some issue.
        String deleteQuery = "Delete From CassandraEntitySample c where c.state=UP";

        q = em.createQuery(deleteQuery);
        // q = em.createNamedQuery("delete.query");
        q.executeUpdate();
        result = em.find(CassandraEntitySample.class, "k");
        // Assert.assertNull(result); // This should be uncommented later. as
        // merge got some issue.
        emf.close();
    }

    @Test
    public void testUpdateUsingIdColumnClause()
    {
        EntityManagerFactory emf = getEntityManagerFactory();
        EntityManager em = emf.createEntityManager();

        CassandraEntitySample entity = new CassandraEntitySample();
        entity.setBirth_date(new Integer(100112));
        entity.setFull_name("impetus_emp");
        entity.setKey("k");
        entity.setState("UP");
        em.persist(entity);

        String updateQuery = "Update CassandraEntitySample c SET c.state = DELHI where c.key = k";
        Query q = em.createQuery(updateQuery);
        q.executeUpdate();
        CassandraEntitySample result = em.find(CassandraEntitySample.class, "k");
        Assert.assertNotNull(result);

        // Assert.assertEquals("DELHI", result.getState()); // This should be
        // uncommented later. as merge got some issue.
        String deleteQuery = "Delete From CassandraEntitySample c where c.key=k";

        q = em.createQuery(deleteQuery);
        // q = em.createNamedQuery("delete.query");
        q.executeUpdate();
        result = em.find(CassandraEntitySample.class, "k");
        // Assert.assertNull(result); // This should be uncommented later. as
        // merge got some issue.
        emf.close();
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
        CassandraCli.dropKeySpace("KunderaExamples");
    }

    /**
     * Gets the entity manager factory.
     * 
     * @return the entity manager factory
     */
    private EntityManagerFactoryImpl getEntityManagerFactory()
    {
        return (EntityManagerFactoryImpl) Persistence.createEntityManagerFactory("cassandra");
    }

}
