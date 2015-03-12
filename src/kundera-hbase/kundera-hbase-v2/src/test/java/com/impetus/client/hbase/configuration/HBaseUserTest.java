/**
 * 
 */
package com.impetus.client.hbase.configuration;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import junit.framework.Assert;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.hbase.testingutil.HBaseTestingUtils;

/**
 * @author Kuldeep Mishra
 * 
 */
public class HBaseUserTest
{

    private static final String TABLE = "KunderaHbaseXmlTest:HBASEUSERXYZ";

    private EntityManagerFactory emf;
    
    private static HBaseAdmin admin;
    
    private static Connection connection;

    /**
     * logger used for logging statement.
     */
    private static final Logger logger = LoggerFactory.getLogger(HBaseUserTest.class);

    /**
     * @throws java.lang.Exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception
    {
        connection = ConnectionFactory.createConnection();
        admin = (HBaseAdmin) connection.getAdmin();
    }

    /**
     * @throws java.lang.Exception
     */
    @AfterClass
    public static void tearDownAfterClass() throws Exception
    {
        HBaseTestingUtils.dropSchema("HBaseNew");
    }

    @Test
    public void test() throws IOException
    {
        emf = Persistence.createEntityManagerFactory("XmlPropertyTest");
        try
        {
            HTableDescriptor hTableDescriptor = admin.getTableDescriptor(TABLE.getBytes());
            int count = 0;
            for (HColumnDescriptor columnDescriptor : hTableDescriptor.getColumnFamilies())
            {
                if (columnDescriptor.getNameAsString().equalsIgnoreCase("HBASEUSERXYZ"))
                {
                    Assert.assertEquals(Algorithm.valueOf("GZ"), columnDescriptor.getCompactionCompressionType());
                    Assert.assertEquals(Integer.parseInt("12345678"), columnDescriptor.getTimeToLive());
                    Assert.assertEquals(Algorithm.valueOf("GZ"), columnDescriptor.getCompressionType());
                    Assert.assertEquals(Integer.parseInt("6"), columnDescriptor.getMaxVersions());
                    Assert.assertEquals(Integer.parseInt("3"), columnDescriptor.getMinVersions());
                    count++;
                }
            }
            Assert.assertEquals(1, count);
        }
        catch (TableNotFoundException tnfe)
        {
            logger.error("Error during UserTest, caused by :" + tnfe);
        }
        catch (IOException ie)
        {
            logger.error("Error during UserTest, caused by :" + ie);
        }
        finally
        {
            emf.close();
            Assert.assertTrue(admin.isTableAvailable(TABLE));
        }
    }

    @Test
    public void testUsingExternalProperty() throws IOException
    {
        Map<String, String> puProperties = new HashMap<String, String>();
        puProperties.put("kundera.ddl.auto.prepare", "create-drop");
        puProperties.put("kundera.keyspace", "KunderaHbaseKeyspace");
        emf = Persistence.createEntityManagerFactory("XmlPropertyTest", puProperties);
        Assert.assertTrue(admin.isTableAvailable("KunderaHbaseKeyspace:HBASEUSERXYZ"));
        emf.close();
        Assert.assertFalse(admin.isTableAvailable("KunderaHbaseKeyspace:HBASEUSERXYZ"));
    }
}
