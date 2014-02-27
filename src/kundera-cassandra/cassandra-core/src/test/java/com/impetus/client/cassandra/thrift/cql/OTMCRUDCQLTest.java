/**
 * 
 */
package com.impetus.client.cassandra.thrift.cql;

import java.util.HashMap;

import org.junit.After;
import org.junit.Before;

import com.impetus.client.cassandra.common.CassandraConstants;
import com.impetus.client.crud.OTMCRUDTest;

/**
 * @author impadmin
 * 
 */
public class OTMCRUDCQLTest extends OTMCRUDTest
{

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        propertyMap = new HashMap();
        propertyMap.put(CassandraConstants.CQL_VERSION, CassandraConstants.CQL_VERSION_3_0);
        super.setUp();
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
        super.tearDown();
    }
}
