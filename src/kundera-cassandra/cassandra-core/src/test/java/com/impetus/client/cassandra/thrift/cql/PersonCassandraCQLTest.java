package com.impetus.client.cassandra.thrift.cql;

import java.util.HashMap;

import org.junit.After;
import org.junit.Before;

import com.impetus.client.cassandra.common.CassandraConstants;
import com.impetus.kundera.PersistenceProperties;

public class PersonCassandraCQLTest extends com.impetus.client.crud.PersonCassandraTest
{

    @Before
    public void setUp() throws Exception
    {
        propertyMap = new HashMap();
        propertyMap.put(CassandraConstants.CQL_VERSION, CassandraConstants.CQL_VERSION_3_0);
        propertyMap.put(PersistenceProperties.KUNDERA_DDL_AUTO_PREPARE, "create");
        AUTO_MANAGE_SCHEMA = true;
        USE_CQL = true;
        super.setUp();
    }

    @After
    public void tearDown() throws Exception
    {
        super.tearDown();
    }
}
