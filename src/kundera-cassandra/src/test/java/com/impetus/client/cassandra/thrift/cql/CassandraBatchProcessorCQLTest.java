/**
 * 
 */
package com.impetus.client.cassandra.thrift.cql;

import java.util.HashMap;

import org.junit.After;
import org.junit.Before;

import com.impetus.client.cassandra.common.CassandraConstants;
import com.impetus.client.crud.batch.CassandraBatchProcessorTest;
import com.impetus.kundera.PersistenceProperties;

/**
 * @author impadmin
 * 
 */
public class CassandraBatchProcessorCQLTest extends CassandraBatchProcessorTest
{

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.client.crud.batch.CassandraBatchProcessorTest#setUp()
     */
    @Before
    public void setUp() throws Exception
    {
        propertyMap = new HashMap();
        propertyMap.put(CassandraConstants.CQL_VERSION, CassandraConstants.CQL_VERSION_3_0);
        propertyMap.put(PersistenceProperties.KUNDERA_DDL_AUTO_PREPARE, "create");
        propertyMap.put("kundera.batch.size", "5");
        AUTO_MANAGE_SCHEMA = false;
        USE_CQL = false;
        super.setUp();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.client.crud.batch.CassandraBatchProcessorTest#tearDown()
     */
    @After
    public void tearDown() throws Exception
    {
        super.tearDown();
    }
}
