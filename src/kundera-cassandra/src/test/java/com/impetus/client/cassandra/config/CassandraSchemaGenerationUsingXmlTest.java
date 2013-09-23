/**
 * 
 */
package com.impetus.client.cassandra.config;

import java.util.HashMap;
import java.util.Map;

import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import junit.framework.Assert;

import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.cassandra.common.CassandraConstants;
import com.impetus.client.persistence.CassandraCli;

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
            KsDef ksDef = CassandraCli.client.describe_keyspace(keyspaceName);

            Assert.assertNotNull(ksDef);
            Assert.assertEquals(keyspaceName, ksDef.getName());
            Assert.assertEquals(SimpleStrategy.class.getName(), ksDef.getStrategy_class());
            Assert.assertEquals("1", ksDef.getStrategy_options().get("replication_factor"));
            Assert.assertTrue(ksDef.isDurable_writes());
            Assert.assertNotNull(ksDef.getCf_defs());
            Assert.assertNotNull(ksDef.getStrategy_options());
            Assert.assertEquals(5, ksDef.getCf_defsSize());

            for (CfDef cfDef : ksDef.getCf_defs())
            {
                Assert.assertNotNull(cfDef);
                if ("CASSANDRAUSERXYZ".equals(cfDef.getName()))
                {
                    Assert.assertEquals("CASSANDRAUSERXYZ", cfDef.getName());
                    Assert.assertEquals(keyspaceName, cfDef.getKeyspace());
                    Assert.assertEquals("Standard", cfDef.getColumn_type());
                    Assert.assertFalse(cfDef.getComment().isEmpty());
                    Assert.assertEquals(UTF8Type.class.getName(), cfDef.getComparator_type());
                    Assert.assertNull(cfDef.getSubcomparator_type());
                    Assert.assertEquals(2, cfDef.getColumn_metadataSize());
                    Assert.assertEquals(BytesType.class.getName(), cfDef.getDefault_validation_class());
                    Assert.assertTrue(cfDef.isReplicate_on_write());
                    Assert.assertEquals(16, cfDef.getMin_compaction_threshold());
                    Assert.assertEquals(64, cfDef.getMax_compaction_threshold());
                }
                else if ("CassandraDefaultUser".equals(cfDef.getName()))
                {
                    Assert.assertEquals(keyspaceName, cfDef.getKeyspace());
                    Assert.assertEquals("Standard", cfDef.getColumn_type());
                    Assert.assertTrue(cfDef.getComment().isEmpty());
                    Assert.assertEquals(UTF8Type.class.getName(), cfDef.getComparator_type());
                    Assert.assertNull(cfDef.getSubcomparator_type());
                    Assert.assertEquals(2, cfDef.getColumn_metadataSize());
                    Assert.assertEquals(BytesType.class.getName(), cfDef.getDefault_validation_class());
                    Assert.assertTrue(cfDef.isReplicate_on_write());
                    Assert.assertEquals(4, cfDef.getMin_compaction_threshold());
                    Assert.assertEquals(32, cfDef.getMax_compaction_threshold());
                }
                else if ("CassandraDefaultSuperUser".equals(cfDef.getName()))
                {
                    Assert.assertEquals(keyspaceName, cfDef.getKeyspace());
                    Assert.assertEquals("Super", cfDef.getColumn_type());
                    Assert.assertTrue(cfDef.getComment().isEmpty());
                    Assert.assertEquals(UTF8Type.class.getName(), cfDef.getComparator_type());
                    Assert.assertNotNull(cfDef.getSubcomparator_type());
                    Assert.assertEquals(UTF8Type.class.getName(), cfDef.getSubcomparator_type());
                    Assert.assertEquals(0, cfDef.getColumn_metadataSize());
                    Assert.assertEquals(BytesType.class.getName(), cfDef.getDefault_validation_class());
                }
                else if ("CASSANDRASUPERUSER".equals(cfDef.getName()))
                {
                    Assert.assertEquals("CASSANDRASUPERUSER", cfDef.getName());
                    Assert.assertEquals(keyspaceName, cfDef.getKeyspace());
                    Assert.assertEquals("Super", cfDef.getColumn_type());
                    Assert.assertFalse(cfDef.getComment().isEmpty());
                    Assert.assertEquals(BytesType.class.getName(), cfDef.getComparator_type());
                    Assert.assertNotNull(cfDef.getSubcomparator_type());
                    Assert.assertEquals(BytesType.class.getName(), cfDef.getSubcomparator_type());
                    Assert.assertEquals(0, cfDef.getColumn_metadataSize());
                    Assert.assertEquals(BytesType.class.getName(), cfDef.getDefault_validation_class());
                }
                else
                {

                }
            }

        }
        catch (NotFoundException nfe)
        {
            Assert.fail();
            logger.error("Error in test, Caused by: .",nfe.getMessage());
        }
        catch (InvalidRequestException ire)
        {
            Assert.fail();
            logger.error("Error in test, Caused by: .",ire.getMessage());
        }
        catch (TException te)
        {
            Assert.fail();
            logger.error("Error in test, caused by: .",te.getMessage());
        }
    }
}
