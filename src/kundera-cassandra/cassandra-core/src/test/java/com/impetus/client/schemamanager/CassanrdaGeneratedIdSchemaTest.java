package com.impetus.client.schemamanager;

import java.util.List;

import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.apache.cassandra.db.marshal.CounterColumnType;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.ColumnDef;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.impetus.kundera.client.cassandra.persistence.CassandraCli;

public class CassanrdaGeneratedIdSchemaTest
{
    private EntityManagerFactory emf;

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
        CassandraCli.cassandraSetUp();
        emf = Persistence.createEntityManagerFactory("cassandra_generated_id");
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
            KsDef ksDef = CassandraCli.client.describe_keyspace("kunderaGeneratedId");
            Assert.assertNotNull(ksDef);
            Assert.assertEquals(19, ksDef.getCf_defsSize());
            int count = 0;
            for (CfDef cfDef : ksDef.cf_defs)
            {
                if (cfDef.getName().equals("kundera_sequences"))
                {
                    Assert.assertTrue(cfDef.getColumn_type().equals("Standard"));
                    Assert.assertTrue(cfDef.getDefault_validation_class().equals(CounterColumnType.class.getName()));
                    count++;
                    continue;
                }
                if (cfDef.getName().equals("kundera"))
                {
                    Assert.assertTrue(cfDef.getColumn_type().equals("Standard"));
                    Assert.assertTrue(cfDef.getDefault_validation_class().equals(CounterColumnType.class.getName()));
                    count++;
                }
                // Mapped super class test classes are created with 4 columns
                // and without @Table annotation. Hence eligible for this pu as
                // well.
                else if (!cfDef.getName().equals("user_account") && !cfDef.getName().equals("social_profile")
                        && !cfDef.getName().equals("TRNX_CREDIT") && !cfDef.getName().equals("DebitTransaction")
                        && !cfDef.getName().equals("PRIMARY_TABLE"))
                {
                    Assert.assertTrue(cfDef.getColumn_type().equals("Standard"));
                    List<ColumnDef> columnDefs = cfDef.getColumn_metadata();
                    Assert.assertEquals(1, columnDefs.size());
                    count++;
                }

            }
            Assert.assertEquals(14, count);
        }
        catch (NotFoundException e)
        {
            // TODO Auto-generated catch block

        }
        catch (InvalidRequestException e)
        {
            // TODO Auto-generated catch block

        }
        catch (TException e)
        {
            // TODO Auto-generated catch block

        }
    }

}
