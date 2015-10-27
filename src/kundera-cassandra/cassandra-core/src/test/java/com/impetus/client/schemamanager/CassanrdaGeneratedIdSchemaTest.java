package com.impetus.client.schemamanager;

import java.util.ArrayList;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.generatedId.entites.CassandraGeneratedIdDefault;
import com.impetus.client.generatedId.entites.CassandraGeneratedIdStrategyAuto;
import com.impetus.client.generatedId.entites.CassandraGeneratedIdStrategyIdentity;
import com.impetus.client.generatedId.entites.CassandraGeneratedIdStrategySequence;
import com.impetus.client.generatedId.entites.CassandraGeneratedIdStrategyTable;
import com.impetus.client.generatedId.entites.CassandraGeneratedIdWithOutSequenceGenerator;
import com.impetus.client.generatedId.entites.CassandraGeneratedIdWithOutTableGenerator;
import com.impetus.client.generatedId.entites.CassandraGeneratedIdWithSequenceGenerator;
import com.impetus.client.generatedId.entites.CassandraGeneratedIdWithTableGenerator;
import com.impetus.client.generatedId.entites.EmployeeAddress;
import com.impetus.client.generatedId.entites.EmployeeInfo;
import com.impetus.kundera.client.cassandra.persistence.CassandraCli;

public class CassanrdaGeneratedIdSchemaTest
{
    private Logger logger = LoggerFactory.getLogger(CassanrdaGeneratedIdSchemaTest.class);
    
    private EntityManagerFactory emf;
    
    private List<String> columnFamilies = new ArrayList<String>();

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
        columnFamilies.add(CassandraGeneratedIdDefault.class.getSimpleName());
        columnFamilies.add(CassandraGeneratedIdStrategyAuto.class.getSimpleName());
        columnFamilies.add(CassandraGeneratedIdStrategyIdentity.class.getSimpleName());
        columnFamilies.add(CassandraGeneratedIdStrategySequence.class.getSimpleName());
        columnFamilies.add(CassandraGeneratedIdStrategyTable.class.getSimpleName());
        columnFamilies.add(CassandraGeneratedIdWithOutSequenceGenerator.class.getSimpleName());
        columnFamilies.add(CassandraGeneratedIdWithOutTableGenerator.class.getSimpleName());
        columnFamilies.add(CassandraGeneratedIdWithSequenceGenerator.class.getSimpleName());
        columnFamilies.add(CassandraGeneratedIdWithTableGenerator.class.getSimpleName());
        columnFamilies.add(EmployeeAddress.class.getSimpleName());
        columnFamilies.add(EmployeeInfo.class.getSimpleName());
        
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
                else if (columnFamilies.contains(cfDef.getName()))
                {
                    Assert.assertTrue(cfDef.getColumn_type().equals("Standard"));
                    List<ColumnDef> columnDefs = cfDef.getColumn_metadata();
                    Assert.assertEquals(1, columnDefs.size());
                    count++;
                }

            }
            Assert.assertEquals(13, count);
        }
        catch (NotFoundException e)
        {
            Assert.fail();
            logger.error("Error in test, Caused by: .", e.getMessage());

        }
        catch (InvalidRequestException e)
        {
            Assert.fail();
            logger.error("Error in test, Caused by: .", e.getMessage());

        }
        catch (TException e)
        {
            Assert.fail();
            logger.error("Error in test, Caused by: .", e.getMessage());

        }
    }

}
