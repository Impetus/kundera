/**
 * 
 */
package com.impetus.client.schemamanager.entites;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

import javax.persistence.Persistence;

import junit.framework.Assert;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.cassandra.persistence.CassandraCli;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl;

/**
 * @author Kuldeep Mishra
 * 
 */
public class ActorTest
{
    private Cassandra.Client client;

    private final boolean useLucene = false;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        CassandraCli.cassandraSetUp();
        CassandraCli cli = new CassandraCli();
        client = cli.getClient();
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
        CassandraCli.dropKeySpace("KunderaCoreExmples");

    }

    @Test
    public void test() throws NotFoundException, InvalidRequestException, TException, UnsupportedEncodingException
    {
        getEntityManagerFactory("create");

        Assert.assertTrue(CassandraCli.keyspaceExist("KunderaCoreExmples"));
        Assert.assertTrue(CassandraCli.columnFamilyExist("Actor", "KunderaCoreExmples"));
        org.apache.cassandra.thrift.KsDef ksDef = new KsDef();
        ksDef = client.describe_keyspace("KunderaCoreExmples");
        Assert.assertEquals(2, ksDef.getCf_defs().size());
        for (org.apache.cassandra.thrift.CfDef cfDef : ksDef.getCf_defs())
        {
            if ("Actor".equals(cfDef.getName()))
            {
                Assert.assertEquals("Standard", cfDef.column_type);
            }
        }
    }

    /**
     * Gets the entity manager factory.
     * 
     * @param useLucene
     * @param property
     * 
     * @return the entity manager factory
     */
    private EntityManagerFactoryImpl getEntityManagerFactory(String property)
    {
        Map propertyMap = new HashMap();
        propertyMap.put(PersistenceProperties.KUNDERA_DDL_AUTO_PREPARE, property);
        return (EntityManagerFactoryImpl) Persistence.createEntityManagerFactory("CassandraSchemaOperationTest",
                propertyMap);
    }
}
