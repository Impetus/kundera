package com.impetus.kundera.client.cassandra.thrift;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import junit.framework.Assert;

import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.ColumnDef;
import org.apache.cassandra.thrift.IndexType;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.cassandra.thrift.ThriftClient;
import com.impetus.client.cassandra.thrift.entities.PersonMToM;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.cassandra.persistence.CassandraCli;
import com.impetus.kundera.persistence.context.jointable.JoinTableData;
import com.impetus.kundera.persistence.context.jointable.JoinTableData.OPERATION;

public class ThriftClientTest
{
    private String persistenceUnit = "thriftClientTest";

    private EntityManagerFactory emf;

    private EntityManager em;

    private ThriftClient client;

    @Before
    public void setUp() throws Exception
    {
        CassandraCli.cassandraSetUp();
        createSchema();
        emf = Persistence.createEntityManagerFactory(persistenceUnit);
        em = emf.createEntityManager();
        Map<String, Client> clients = (Map<String, Client>) em.getDelegate();
        client = (ThriftClient) clients.get(persistenceUnit);
    }

    @After
    public void tearDown() throws Exception
    {
        if (em != null)
        {
            em.close();
        }
        if (emf != null)
        {
            emf.close();
        }
        CassandraCli.dropKeySpace("KunderaExamples");
    }

    @Test
    public void executeTests()
    {
        persistJoinTable();
        getColumnsById();
//        findIdsByColumn();
        //TODO: debug TimedOutException
    }

    private void persistJoinTable()
    {
        JoinTableData joinTableData = new JoinTableData(OPERATION.INSERT, "KunderaExamples", "PERSON_ADDRESS",
                "PERSON_ID", "ADDRESS_ID", PersonMToM.class);

        Set<Object> values1 = new HashSet<Object>();
        values1.add("A");
        values1.add("B");
        Set<Object> values2 = new HashSet<Object>();
        values2.add("C");
        values2.add("D");

        joinTableData.addJoinTableRecord("1", values1);
        joinTableData.addJoinTableRecord("2", values2);

        client.persistJoinTable(joinTableData);

    }

    private void getColumnsById()
    {
        List columns1 = client.getColumnsById("KunderaExamples", "PERSON_ADDRESS", "PERSON_ID", "ADDRESS_ID", "1",
                String.class);
        Assert.assertNotNull(columns1);
        Assert.assertFalse(columns1.isEmpty());
        Assert.assertEquals(2, columns1.size());
        Assert.assertTrue(columns1.contains("A"));
        Assert.assertTrue(columns1.contains("B"));

        List columns2 = client.getColumnsById("KunderaExamples", "PERSON_ADDRESS", "PERSON_ID", "ADDRESS_ID", "2",
                String.class);
        Assert.assertNotNull(columns2);
        Assert.assertFalse(columns2.isEmpty());
        Assert.assertEquals(2, columns2.size());
        Assert.assertTrue(columns2.contains("C"));
        Assert.assertTrue(columns2.contains("D"));

    }

    private void findIdsByColumn()
    {
        Object[] ids1 = client.findIdsByColumn("KunderaExamples", "PERSON_ADDRESS", "PERSON_ID", "ADDRESS_ID", "A",
                PersonMToM.class);
        Assert.assertNotNull(ids1);
        Assert.assertTrue(ids1.length == 1);
        Assert.assertEquals("1", ids1[0]);

        Object[] ids2 = client.findIdsByColumn("KunderaExamples", "PERSON_ADDRESS", "PERSON_ID", "ADDRESS_ID", "B",
                PersonMToM.class);
        Assert.assertNotNull(ids2);
        Assert.assertTrue(ids2.length == 1);
        Assert.assertEquals("1", ids2[0]);

        Object[] ids3 = client.findIdsByColumn("KunderaExamples", "PERSON_ADDRESS", "PERSON_ID", "ADDRESS_ID", "C",
                PersonMToM.class);
        Assert.assertNotNull(ids3);
        Assert.assertTrue(ids3.length == 1);
        Assert.assertEquals("2", ids3[0]);

        Object[] ids4 = client.findIdsByColumn("KunderaExamples", "PERSON_ADDRESS", "PERSON_ID", "ADDRESS_ID", "D",
                PersonMToM.class);
        Assert.assertNotNull(ids4);
        Assert.assertTrue(ids4.length == 1);
        Assert.assertEquals("2", ids4[0]);
    }

    private void createSchema() throws InvalidRequestException, SchemaDisagreementException, TException
    {
        KsDef ksDef = null;
        CfDef user_Def = new CfDef();
        user_Def.name = "PERSON_ADDRESS";
        user_Def.keyspace = "KunderaExamples";
        user_Def.setComparator_type("UTF8Type");
        user_Def.setDefault_validation_class("UTF8Type");
        user_Def.setKey_validation_class("UTF8Type");
        ColumnDef columnDef = new ColumnDef(ByteBuffer.wrap("PERSON_ID".getBytes()), "UTF8Type");
        columnDef.index_type = IndexType.KEYS;
        user_Def.addToColumn_metadata(columnDef);
        ColumnDef columnDef1 = new ColumnDef(ByteBuffer.wrap("ADDRESS_ID".getBytes()), "UTF8Type");
        columnDef1.index_type = IndexType.KEYS;
        user_Def.addToColumn_metadata(columnDef1);

        List<CfDef> cfDefs = new ArrayList<CfDef>();
        cfDefs.add(user_Def);

        try
        {
            ksDef = CassandraCli.client.describe_keyspace("KunderaExamples");
            CassandraCli.client.set_keyspace("KunderaExamples");

            List<CfDef> cfDefn = ksDef.getCf_defs();

            for (CfDef cfDef1 : cfDefn)
            {

                if (cfDef1.getName().equalsIgnoreCase("PERSON"))
                {

                    CassandraCli.client.system_drop_column_family("PERSON");

                }
            }
            CassandraCli.client.system_add_column_family(user_Def);

        }
        catch (NotFoundException e)
        {

            ksDef = new KsDef("KunderaExamples", "org.apache.cassandra.locator.SimpleStrategy", cfDefs);
            // Set replication factor
            if (ksDef.strategy_options == null)
            {
                ksDef.strategy_options = new LinkedHashMap<String, String>();
            }
            // Set replication factor, the value MUST be an integer
            ksDef.strategy_options.put("replication_factor", "1");
            CassandraCli.client.system_add_keyspace(ksDef);
        }

        CassandraCli.client.set_keyspace("KunderaExamples");

    }
}
