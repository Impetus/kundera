package com.impetus.client.oraclenosql.schemamanager;

import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class OracleNoSQLSchemaGenerationTest
{

    private EntityManagerFactory emf;


    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
        emf.close();
    }

    @Test
    public void testCreate()
    {

        KVStore kvStore = KVStoreFactory.getStore(new KVStoreConfig("OracleNoSqlTests", "localhost:5000"));
        TableAPI tableAPI = kvStore.getTableAPI();
        Table table = tableAPI.getTable("ONS_USER");
        Assert.assertNull(table);

        // Schema will be generated during EMF Creation.
        emf = Persistence.createEntityManagerFactory("oracleNosqlSchemaGeneration");
        
        table = tableAPI.getTable("ONS_USER");
        Assert.assertNotNull(table);
        Assert.assertEquals("USER_ID", table.getPrimaryKey().get(0));

        tableAPI.executeSync("DROP TABLE IF EXISTS ONS_USER");
    }

}
