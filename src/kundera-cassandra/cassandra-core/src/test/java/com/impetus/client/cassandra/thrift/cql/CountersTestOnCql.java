package com.impetus.client.cassandra.thrift.cql;

import org.junit.After;
import org.junit.Before;

import com.impetus.client.cassandra.common.CassandraConstants;
import com.impetus.client.crud.countercolumns.CountersTest;
import com.impetus.kundera.PersistenceProperties;

public class CountersTestOnCql extends CountersTest
{

    @Before
    public void setUp() throws Exception
    {
        propertyMap.put(CassandraConstants.CQL_VERSION, CassandraConstants.CQL_VERSION_3_0);
        propertyMap.put(PersistenceProperties.KUNDERA_DDL_AUTO_PREPARE, "create");
        super.setUp();
    }

    @After
    public void tearDown() throws Exception
    {
        super.tearDown();
    }
}
