package com.impetus.client.crud;

import java.util.HashMap;
import java.util.Map;

import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.cassandra.common.CassandraConstants;
import com.impetus.kundera.PersistenceProperties;

public class CassandraCqlSecondaryTableTest extends SecondaryTableTestBase
{

    private EntityManagerFactory emf;

    @Before
    public void setUp() throws Exception
    {
        Map propertyMap = new HashMap();
        propertyMap.put(CassandraConstants.CQL_VERSION, CassandraConstants.CQL_VERSION_3_0);
        emf = Persistence.createEntityManagerFactory("secIdxCassandraTest", propertyMap);
    }

    @After
    public void tearDown() throws Exception
    {
        emf.close();
    }

    @Test
    public void test()
    {
//        testCRUD(emf);
    }
}
