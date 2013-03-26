package com.impetus.client.hbase.schemaManager;

import java.io.IOException;

import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.impetus.client.hbase.junits.HBaseCli;

public class HBaseGeneratedIdSchemaTest
{
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
    }

    @Test
    public void test()
    {
        try
        {
            HBaseAdmin admin = HBaseCli.utility.getHBaseAdmin();
            Assert.assertTrue(admin.isTableAvailable("HBaseGeneratedIdDefault".getBytes()));
            Assert.assertTrue(admin.isTableAvailable("HBaseGeneratedIdStrategyAuto".getBytes()));
            Assert.assertTrue(admin.isTableAvailable("HBaseGeneratedIdStrategyIdentity".getBytes()));
            Assert.assertTrue(admin.isTableAvailable("HBaseGeneratedIdStrategySequence".getBytes()));
            Assert.assertTrue(admin.isTableAvailable("HBaseGeneratedIdStrategyTable".getBytes()));
            Assert.assertTrue(admin.isTableAvailable("HBaseGeneratedIdWithOutSequenceGenerator".getBytes()));
            Assert.assertTrue(admin.isTableAvailable("HBaseGeneratedIdWithOutTableGenerator".getBytes()));
            Assert.assertTrue(admin.isTableAvailable("HBaseGeneratedIdWithSequenceGenerator".getBytes()));
            Assert.assertTrue(admin.isTableAvailable("HBaseGeneratedIdWithTableGenerator".getBytes()));
            Assert.assertTrue(admin.isTableAvailable("kunderahbase".getBytes()));
            Assert.assertTrue(admin.isTableAvailable("kundera_sequences".getBytes()));
        }
        catch (IOException e)
        {
            Assert.fail();
        }
    }
}
