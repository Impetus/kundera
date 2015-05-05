package com.impetus.client.generatedId;

import org.junit.After;
import org.junit.Before;

import com.impetus.client.cassandra.common.CassandraConstants;

public class CassandraGeneratedIdCqlTest extends CassandraGeneratedIdTest
{

    @Before
    public void setUp() throws Exception
    {
        properties.put(CassandraConstants.CQL_VERSION, CassandraConstants.CQL_VERSION_3_0);
        super.setUp();
    }

    @After
    public void tearDown() throws Exception
    {
        super.tearDown();
    }

}
