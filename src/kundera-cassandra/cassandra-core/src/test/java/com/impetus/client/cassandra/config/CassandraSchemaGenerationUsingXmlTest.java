/**
 * 
 */
package com.impetus.client.cassandra.config;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import junit.framework.Assert;

import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.CqlRow;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.cassandra.common.CassandraConstants;
import com.impetus.kundera.client.cassandra.persistence.CassandraCli;

/**
 * @author Kuldeep Mishra
 * 
 */
public class CassandraSchemaGenerationUsingXmlTest
{
    private EntityManagerFactory emf;

    private String keyspaceName = "KunderaCassandraXmlTest";

    private Logger logger = LoggerFactory.getLogger(CassandraSchemaGenerationUsingXmlTest.class);

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        CassandraCli.cassandraSetUp();
        CassandraCli.createKeySpace(keyspaceName);
        Map propertyMap = new HashMap();
        propertyMap.put(CassandraConstants.CQL_VERSION, CassandraConstants.CQL_VERSION_3_0);
        emf = Persistence.createEntityManagerFactory("CassandraXmlPropertyTest", propertyMap);
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
        emf.close();
        CassandraCli.dropKeySpace(keyspaceName);
    }

    @Test
    public void test()
    {
        try
        {
              CqlResult cqlResult = CassandraCli.client.execute_cql3_query(ByteBuffer
                                    .wrap("Select * from system_schema.keyspaces where keyspace_name = 'KunderaCassandraXmlTest'"
                                            .getBytes()), Compression.NONE, ConsistencyLevel.ONE);

            List<CqlRow> cqlRows = cqlResult.getRows();
            
            StringBuilder builder = new  StringBuilder("{");
            builder.append("\"replication_factor\"");
            builder.append(":");
            builder.append("\"1\"");
            builder.append("}");
            
            for(CqlRow cqlRow : cqlRows)
            {
                Assert.assertEquals("keyspace_name", ByteBufferUtil.string(ByteBuffer.wrap(cqlRow.getColumns().get(0).getName())));
                Assert.assertEquals(keyspaceName, ByteBufferUtil.string(ByteBuffer.wrap(cqlRow.getColumns().get(0).getValue())));
                Assert.assertEquals("durable_writes", ByteBufferUtil.string(ByteBuffer.wrap(cqlRow.getColumns().get(1).getName())));
                Assert.assertEquals(false, Boolean.getBoolean(ByteBufferUtil.string(ByteBuffer.wrap(cqlRow.getColumns().get(1).getValue()))));
                Assert.assertEquals("replication", ByteBufferUtil.string(ByteBuffer.wrap(cqlRow.getColumns().get(2).getName())));
            }
            
            cqlResult = CassandraCli.client.execute_cql3_query(ByteBuffer
                                    .wrap("Select * from system_schema.tables where keyspace_name = 'KunderaCassandraXmlTest'"
                                            .getBytes()), Compression.NONE, ConsistencyLevel.ONE);

            List<CqlRow> columnFamilies = cqlResult.getRows();

            Assert.assertNotNull(columnFamilies);
           
//            Assert.assertEquals(13, columnFamilies.size());

            for (CqlRow cqlRow : cqlResult.getRows())
            {
                Assert.assertNotNull(cqlRow);
                if ("CASSANDRAUSERXYZ".equals(ByteBuffer.wrap(cqlRow.getColumns().get(1).getValue())))
                {                    
                    Assert.assertEquals("comment", ByteBufferUtil.string(ByteBuffer.wrap(cqlRow.getColumns().get(5).getName())));
                    Assert.assertTrue(ByteBufferUtil.string(ByteBuffer.wrap(cqlRow.getColumns().get(5).getValue())).isEmpty());
                    Assert.assertEquals("comparator", ByteBufferUtil.string(ByteBuffer.wrap(cqlRow.getColumns().get(8).getName())));
                    Assert.assertEquals(UTF8Type.class.getName(), ByteBufferUtil.string(ByteBuffer.wrap(cqlRow.getColumns().get(8).getValue())));
                    Assert.assertEquals("default_validator", ByteBufferUtil.string(ByteBuffer.wrap(cqlRow.getColumns().get(10).getName())));
                    Assert.assertEquals(BytesType.class.getName(), Boolean.getBoolean(ByteBufferUtil.string(ByteBuffer.wrap(cqlRow.getColumns().get(10).getValue()))));
                    Assert.assertEquals("max_compaction_threshold", ByteBufferUtil.string(ByteBuffer.wrap(cqlRow.getColumns().get(17).getName())));
                    Assert.assertEquals(16, ByteBufferUtil.toInt(ByteBuffer.wrap(cqlRow.getColumns().get(17).getValue())));
                    Assert.assertEquals("min_compaction_threshold", ByteBufferUtil.string(ByteBuffer.wrap(cqlRow.getColumns().get(18).getName())));
                    Assert.assertEquals(64, ByteBufferUtil.toInt(ByteBuffer.wrap(cqlRow.getColumns().get(18).getValue())));
                    Assert.assertEquals("replicate_on_write", ByteBufferUtil.string(ByteBuffer.wrap(cqlRow.getColumns().get(21).getName())));
                    Assert.assertEquals("true", ByteBufferUtil.string(ByteBuffer.wrap(cqlRow.getColumns().get(21).getValue())));
                    Assert.assertEquals("subcomparator", ByteBufferUtil.string(ByteBuffer.wrap(cqlRow.getColumns().get(22).getName())));
                    Assert.assertNull(ByteBufferUtil.string(ByteBuffer.wrap(cqlRow.getColumns().get(22).getValue())));
                    Assert.assertEquals("type", ByteBufferUtil.string(ByteBuffer.wrap(cqlRow.getColumns().get(23).getName())));
                    Assert.assertEquals("Standard", ByteBufferUtil.string(ByteBuffer.wrap(cqlRow.getColumns().get(23).getValue())));
                }
                else if ("CassandraDefaultUser".equals(ByteBuffer.wrap(cqlRow.getColumns().get(1).getValue())))
                {
                    Assert.assertEquals("comment", ByteBufferUtil.string(ByteBuffer.wrap(cqlRow.getColumns().get(5).getName())));
                    Assert.assertTrue(ByteBufferUtil.string(ByteBuffer.wrap(cqlRow.getColumns().get(5).getValue())).isEmpty());
                    Assert.assertEquals("comparator", ByteBufferUtil.string(ByteBuffer.wrap(cqlRow.getColumns().get(8).getName())));
                    Assert.assertEquals(UTF8Type.class.getName(), ByteBufferUtil.string(ByteBuffer.wrap(cqlRow.getColumns().get(8).getValue())));
                    Assert.assertEquals("default_validator", ByteBufferUtil.string(ByteBuffer.wrap(cqlRow.getColumns().get(10).getName())));
                    Assert.assertEquals(BytesType.class.getName(), Boolean.getBoolean(ByteBufferUtil.string(ByteBuffer.wrap(cqlRow.getColumns().get(10).getValue()))));
                    Assert.assertEquals("max_compaction_threshold", ByteBufferUtil.string(ByteBuffer.wrap(cqlRow.getColumns().get(17).getName())));
                    Assert.assertEquals(4, ByteBufferUtil.toInt(ByteBuffer.wrap(cqlRow.getColumns().get(17).getValue())));
                    Assert.assertEquals("min_compaction_threshold", ByteBufferUtil.string(ByteBuffer.wrap(cqlRow.getColumns().get(18).getName())));
                    Assert.assertEquals(32, ByteBufferUtil.toInt(ByteBuffer.wrap(cqlRow.getColumns().get(18).getValue())));
                    Assert.assertEquals("replicate_on_write", ByteBufferUtil.string(ByteBuffer.wrap(cqlRow.getColumns().get(21).getName())));
                    Assert.assertEquals("true", ByteBufferUtil.string(ByteBuffer.wrap(cqlRow.getColumns().get(21).getValue())));
                    Assert.assertEquals("subcomparator", ByteBufferUtil.string(ByteBuffer.wrap(cqlRow.getColumns().get(22).getName())));
                    Assert.assertNull(ByteBufferUtil.string(ByteBuffer.wrap(cqlRow.getColumns().get(22).getValue())));
                    Assert.assertEquals("type", ByteBufferUtil.string(ByteBuffer.wrap(cqlRow.getColumns().get(23).getName())));
                    Assert.assertEquals("Standard", ByteBufferUtil.string(ByteBuffer.wrap(cqlRow.getColumns().get(23).getValue())));                  
                }
                else if ("CassandraDefaultSuperUser".equals(ByteBuffer.wrap(cqlRow.getColumns().get(1).getValue())))
                {
                    Assert.assertEquals("comment", ByteBufferUtil.string(ByteBuffer.wrap(cqlRow.getColumns().get(5).getName())));
                    Assert.assertTrue(ByteBufferUtil.string(ByteBuffer.wrap(cqlRow.getColumns().get(5).getValue())).isEmpty());
                    Assert.assertEquals("comparator", ByteBufferUtil.string(ByteBuffer.wrap(cqlRow.getColumns().get(8).getName())));
                    Assert.assertEquals(UTF8Type.class.getName(), ByteBufferUtil.string(ByteBuffer.wrap(cqlRow.getColumns().get(8).getValue())));
                    Assert.assertEquals("default_validator", ByteBufferUtil.string(ByteBuffer.wrap(cqlRow.getColumns().get(10).getName())));
                    Assert.assertEquals(BytesType.class.getName(), Boolean.getBoolean(ByteBufferUtil.string(ByteBuffer.wrap(cqlRow.getColumns().get(10).getValue()))));
                    Assert.assertEquals("max_compaction_threshold", ByteBufferUtil.string(ByteBuffer.wrap(cqlRow.getColumns().get(17).getName())));
                    Assert.assertEquals(16, ByteBufferUtil.toInt(ByteBuffer.wrap(cqlRow.getColumns().get(17).getValue())));
                    Assert.assertEquals("min_compaction_threshold", ByteBufferUtil.string(ByteBuffer.wrap(cqlRow.getColumns().get(18).getName())));
                    Assert.assertEquals(64, ByteBufferUtil.toInt(ByteBuffer.wrap(cqlRow.getColumns().get(18).getValue())));
                    Assert.assertEquals("replicate_on_write", ByteBufferUtil.string(ByteBuffer.wrap(cqlRow.getColumns().get(21).getName())));
                    Assert.assertEquals("true", ByteBufferUtil.string(ByteBuffer.wrap(cqlRow.getColumns().get(21).getValue())));
                    Assert.assertEquals("subcomparator", ByteBufferUtil.string(ByteBuffer.wrap(cqlRow.getColumns().get(22).getName())));
                    Assert.assertNull(ByteBufferUtil.string(ByteBuffer.wrap(cqlRow.getColumns().get(22).getValue())));
                    Assert.assertEquals("type", ByteBufferUtil.string(ByteBuffer.wrap(cqlRow.getColumns().get(23).getName())));
                    Assert.assertEquals("Super", ByteBufferUtil.string(ByteBuffer.wrap(cqlRow.getColumns().get(23).getValue())));
                }
                else if ("CASSANDRASUPERUSER".equals(ByteBuffer.wrap(cqlRow.getColumns().get(1).getValue())))
                {
                    Assert.assertEquals("comment", ByteBufferUtil.string(ByteBuffer.wrap(cqlRow.getColumns().get(5).getName())));
                    Assert.assertTrue(ByteBufferUtil.string(ByteBuffer.wrap(cqlRow.getColumns().get(5).getValue())).isEmpty());
                    Assert.assertEquals("comparator", ByteBufferUtil.string(ByteBuffer.wrap(cqlRow.getColumns().get(8).getName())));
                    Assert.assertEquals(UTF8Type.class.getName(), ByteBufferUtil.string(ByteBuffer.wrap(cqlRow.getColumns().get(8).getValue())));
                    Assert.assertEquals("default_validator", ByteBufferUtil.string(ByteBuffer.wrap(cqlRow.getColumns().get(10).getName())));
                    Assert.assertEquals(BytesType.class.getName(), Boolean.getBoolean(ByteBufferUtil.string(ByteBuffer.wrap(cqlRow.getColumns().get(10).getValue()))));
                    Assert.assertEquals("max_compaction_threshold", ByteBufferUtil.string(ByteBuffer.wrap(cqlRow.getColumns().get(17).getName())));
                    Assert.assertEquals(16, ByteBufferUtil.toInt(ByteBuffer.wrap(cqlRow.getColumns().get(17).getValue())));
                    Assert.assertEquals("min_compaction_threshold", ByteBufferUtil.string(ByteBuffer.wrap(cqlRow.getColumns().get(18).getName())));
                    Assert.assertEquals(64, ByteBufferUtil.toInt(ByteBuffer.wrap(cqlRow.getColumns().get(18).getValue())));
                    Assert.assertEquals("replicate_on_write", ByteBufferUtil.string(ByteBuffer.wrap(cqlRow.getColumns().get(21).getName())));
                    Assert.assertEquals("true", ByteBufferUtil.string(ByteBuffer.wrap(cqlRow.getColumns().get(21).getValue())));
                    Assert.assertEquals("subcomparator", ByteBufferUtil.string(ByteBuffer.wrap(cqlRow.getColumns().get(22).getName())));
                    Assert.assertNull(ByteBufferUtil.string(ByteBuffer.wrap(cqlRow.getColumns().get(22).getValue())));
                    Assert.assertEquals("type", ByteBufferUtil.string(ByteBuffer.wrap(cqlRow.getColumns().get(23).getName())));
                    Assert.assertEquals("Super", ByteBufferUtil.string(ByteBuffer.wrap(cqlRow.getColumns().get(23).getValue())));
                }
                else
                {

                }
            }

        }        
        catch (InvalidRequestException ire)
        {
            Assert.fail();
            logger.error("Error in test, Caused by: .", ire.getMessage());
        }
        catch (TException te)
        {
            Assert.fail();
            logger.error("Error in test, caused by: .", te.getMessage());
        }
        catch (CharacterCodingException e)
        {
            Assert.fail();
            logger.error("Error in test, caused by: .", e.getMessage());
        }
    }
}
