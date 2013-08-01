package com.impetus.client.hbase.schemaManager;

import java.io.IOException;

import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.impetus.client.hbase.junits.HBaseCli;

public class HBaseGeneratedIdSchemaTest
{
    private static final String table = "kundera";

    private EntityManagerFactory emf;

    private HBaseCli cli;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception
    {
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception
    {
    }

    @Before
    public void setUp() throws Exception
    {
        cli = new HBaseCli();
        cli.startCluster();
        emf = Persistence.createEntityManagerFactory("hbase_generated_id");
    }

    @After
    public void tearDown() throws Exception
    {
        emf.close();
        cli.dropTable("kundera");
    }

    @Test
    public void test()
    {
        try
        {
            HBaseAdmin admin = HBaseCli.utility.getHBaseAdmin();
            Assert.assertTrue(admin.isTableAvailable(table.getBytes()));
            HTableDescriptor descriptor = admin.getTableDescriptor(table.getBytes());
            Assert.assertNotNull(descriptor.getFamily("HBaseGeneratedIdDefault".getBytes()));
            Assert.assertNotNull(descriptor.getFamily("HBaseGeneratedIdStrategyAuto".getBytes()));
            Assert.assertNotNull(descriptor.getFamily("HBaseGeneratedIdStrategyIdentity".getBytes()));
            Assert.assertNotNull(descriptor.getFamily("HBaseGeneratedIdStrategySequence".getBytes()));
            Assert.assertNotNull(descriptor.getFamily("HBaseGeneratedIdStrategyTable".getBytes()));
            Assert.assertNotNull(descriptor.getFamily("HBaseGeneratedIdWithOutSequenceGenerator".getBytes()));
            Assert.assertNotNull(descriptor.getFamily("HBaseGeneratedIdWithOutTableGenerator".getBytes()));
            Assert.assertNotNull(descriptor.getFamily("HBaseGeneratedIdWithSequenceGenerator".getBytes()));
            Assert.assertNotNull(descriptor.getFamily("HBaseGeneratedIdWithTableGenerator".getBytes()));
            Assert.assertNotNull(descriptor.getFamily("kunderahbase".getBytes()));
            Assert.assertNotNull(descriptor.getFamily("kundera_sequences".getBytes()));
        }
        catch (IOException e)
        {
            Assert.fail();
        }
    }
}
