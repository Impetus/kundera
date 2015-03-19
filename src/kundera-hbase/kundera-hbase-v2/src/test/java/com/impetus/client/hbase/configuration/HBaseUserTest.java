/*******************************************************************************
 * * Copyright 2015 Impetus Infotech.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 ******************************************************************************/
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
 * The Class HBaseUserTest.
 * 
 * @author Kuldeep Mishra
 */
public class HBaseUserTest
{

    /** The Constant TABLE. */
    private static final String TABLE = "KunderaHbaseXmlTest:HBASEUSERXYZ";

    /** The emf. */
    private EntityManagerFactory emf;

    /** The admin. */
    private static HBaseAdmin admin;

    /** The connection. */
    private static Connection connection;

    /**
     * logger used for logging statement.
     */
    private static final Logger logger = LoggerFactory.getLogger(HBaseUserTest.class);

    /**
     * Sets the up before class.
     * 
     * @throws Exception
     *             the exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception
    {
        connection = ConnectionFactory.createConnection();
        admin = (HBaseAdmin) connection.getAdmin();
    }

    /**
     * Tear down after class.
     * 
     * @throws Exception
     *             the exception
     */
    @AfterClass
    public static void tearDownAfterClass() throws Exception
    {
        HBaseTestingUtils.dropSchema("HBaseNew");
    }

    /**
     * Test.
     * 
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
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

    /**
     * Test using external property.
     * 
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
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
