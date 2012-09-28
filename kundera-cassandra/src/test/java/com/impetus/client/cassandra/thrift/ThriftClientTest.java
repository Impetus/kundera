package com.impetus.client.cassandra.thrift;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.cassandra.thrift.entities.PersonMToM;
import com.impetus.client.persistence.CassandraCli;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.persistence.context.jointable.JoinTableData;
import com.impetus.kundera.persistence.context.jointable.JoinTableData.OPERATION;

public class ThriftClientTest
{
    String persistenceUnit = "thriftClientTest";

    EntityManagerFactory emf;

    EntityManager em;

    ThriftClient client;

    @Before
    public void setUp() throws Exception
    {
        CassandraCli.cassandraSetUp();
        emf = Persistence.createEntityManagerFactory(persistenceUnit);
        em = emf.createEntityManager();
        Map<String, Client> clients = (Map<String, Client>) em.getDelegate();
        client = (ThriftClient) clients.get(persistenceUnit);
    }

    @After
    public void tearDown() throws Exception
    {
        em.close();
        emf.close();
    }

    @Test
    public void executeTests()
    {
        persistJoinTable();
        getColumnsById();
        findIdsByColumn();
    }

    public void persistJoinTable()
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

    public void getColumnsById()
    {
        List columns1 = client.getColumnsById("KunderaExamples", "PERSON_ADDRESS", "PERSON_ID", "ADDRESS_ID", "1");
        Assert.assertNotNull(columns1);
        Assert.assertFalse(columns1.isEmpty());
        Assert.assertEquals(2, columns1.size());
        Assert.assertTrue(columns1.contains("A"));
        Assert.assertTrue(columns1.contains("B"));

        List columns2 = client.getColumnsById("KunderaExamples", "PERSON_ADDRESS", "PERSON_ID", "ADDRESS_ID", "2");
        Assert.assertNotNull(columns2);
        Assert.assertFalse(columns2.isEmpty());
        Assert.assertEquals(2, columns2.size());
        Assert.assertTrue(columns2.contains("C"));
        Assert.assertTrue(columns2.contains("D"));

    }

    public void findIdsByColumn()
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

    @Test
    public void deleteByColumn()
    {
        // fail("Not yet implemented");
    }

}
