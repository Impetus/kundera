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
package com.impetus.client.junit;

import java.nio.ByteBuffer;
import java.util.UUID;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import junit.framework.Assert;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.ColumnDef;
import org.apache.cassandra.thrift.IndexType;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.entity.CassandraUUIDEntity;
import com.impetus.client.persistence.CassandraCli;

/**
 * Test case for CRUD operations on Cassandra database using Kundera.
 */
public class TestCassandra
{
    /** The logger. */
    private static Logger logger = Logger.getLogger(TestCassandra.class);

    /**
     * Sets the up.
     * 
     * @throws Exception
     *             the exception
     */
    @Before
    public void setUp() throws Exception
    {
        logger.info("starting server");
        CassandraCli.cassandraSetUp();
        CassandraCli.createKeySpace("UUIDCassandra");
        Cassandra.Client client = CassandraCli.getClient();
        client.set_keyspace("UUIDCassandra");
        CfDef cf_def = new CfDef();
        cf_def.keyspace = "UUIDCassandra";
        cf_def.name = "uuidsample";
        cf_def.setKey_validation_class("UUIDType");
        ColumnDef columnDef2 = new ColumnDef(ByteBuffer.wrap("name".getBytes()), "UTF8Type");
        columnDef2.index_type = IndexType.KEYS;
        cf_def.addToColumn_metadata(columnDef2);
        ColumnDef columnDef3 = new ColumnDef(ByteBuffer.wrap("age".getBytes()), "Int32Type");
        columnDef3.index_type = IndexType.KEYS;
        cf_def.addToColumn_metadata(columnDef3);
        client.system_add_column_family(cf_def);
    }

    @Test
    public void testUUID()
    {
        EntityManagerFactory emf = Persistence.createEntityManagerFactory("cass_pu");
        UUID key = UUID.randomUUID();
        CassandraUUIDEntity entity = new CassandraUUIDEntity();
        entity.setAge(10);
        entity.setName("vivek");
        entity.setUuidKey(key);

        EntityManager em = emf.createEntityManager();
        em.persist(entity);
        CassandraUUIDEntity result = em.find(CassandraUUIDEntity.class, key);
        Assert.assertNotNull(result);
        Assert.assertEquals(key, result.getUuidKey());
        Assert.assertEquals("vivek", result.getName());
        Assert.assertEquals(new Integer(10), result.getAge());
    }

    /**
     * Tear down.
     * 
     * @throws Exception
     *             the exception
     */
    @After
    public void tearDown() throws Exception
    {
        logger.info("destroying");
        CassandraCli.dropKeySpace("UUIDCassandra");
    }
}