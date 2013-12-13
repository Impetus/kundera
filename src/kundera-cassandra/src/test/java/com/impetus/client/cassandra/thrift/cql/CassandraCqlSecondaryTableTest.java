package com.impetus.client.cassandra.thrift.cql;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.cassandra.common.CassandraConstants;
import com.impetus.client.persistence.CassandraCli;

public class CassandraCqlSecondaryTableTest
{

    private EntityManagerFactory emf;

    @Before
    public void setUp() throws Exception
    {
        CassandraCli.cassandraSetUp();
        CassandraCli.createKeySpace("KunderaExamples");
        loadData();
        Map<String, String> props = new HashMap<String, String>();
        props.put(CassandraConstants.CQL_VERSION, CassandraConstants.CQL_VERSION_3_0);
        emf = Persistence.createEntityManagerFactory("secIdxCassandraTest", props);
    }

    @After
    public void tearDown() throws Exception
    {
        emf.close();
        CassandraCli.dropKeySpace("KunderaExamples");
    }

    @Test
    public void test()
    {
        EntityManager em = emf.createEntityManager();

        SecondaryTableCqlEntity entity = new SecondaryTableCqlEntity();
        entity.setAge(24);
        entity.setObjectId("123");
        entity.setName("Kuldeep");

        em.persist(entity);

        em.clear();

        SecondaryTableCqlEntity foundEntity = em.find(SecondaryTableCqlEntity.class, "123");
        Assert.assertNotNull(foundEntity);
        Assert.assertEquals("Kuldeep", foundEntity.getName());
        Assert.assertEquals(24, foundEntity.getAge());

        foundEntity.setAge(25);
        foundEntity.setName("kk");

        em.merge(foundEntity);

        em.clear();

        foundEntity = em.find(SecondaryTableCqlEntity.class, "123");
        Assert.assertNotNull(foundEntity);
        Assert.assertEquals("kk", foundEntity.getName());
        Assert.assertEquals(25, foundEntity.getAge());

        em.remove(foundEntity);

        foundEntity = em.find(SecondaryTableCqlEntity.class, "123");
        Assert.assertNull(foundEntity);
    }

    /**
     * Load cassandra specific data.
     * 
     * @throws TException
     *             the t exception
     * @throws InvalidRequestException
     *             the invalid request exception
     * @throws UnavailableException
     *             the unavailable exception
     * @throws TimedOutException
     *             the timed out exception
     * @throws SchemaDisagreementException
     *             the schema disagreement exception
     */
    private void loadData() throws TException, InvalidRequestException, UnavailableException, TimedOutException,
            SchemaDisagreementException
    {
        String table1 = "PRIMARY_TABLE";
        String table2 = "SECONDARY_TABLE";
        String keyspace = "KunderaExamples";

        try
        {
            Client client = CassandraCli.getClient();
            KsDef ksDef = client.describe_keyspace(keyspace);
            client.set_keyspace(keyspace);

            List<CfDef> cfDefn = ksDef.getCf_defs();

            for (CfDef cfDef1 : cfDefn)
            {

                if (cfDef1.getName().equalsIgnoreCase(table1))
                {
                    client.system_drop_column_family(table1);
                }
                if (cfDef1.getName().equalsIgnoreCase(table2))
                {
                    client.system_drop_column_family(table2);
                }
            }

            client.execute_cql3_query(ByteBufferUtil.bytes("create table \"PRIMARY_TABLE\"(\"OBJECT_ID\" text PRIMARY KEY, \"NAME\" text)"),
                    org.apache.cassandra.thrift.Compression.NONE, org.apache.cassandra.thrift.ConsistencyLevel.ANY);

            client.execute_cql3_query(ByteBufferUtil.bytes("create table \"SECONDARY_TABLE\"(\"OBJECT_ID\" text PRIMARY KEY, \"AGE\" int)"),
                    org.apache.cassandra.thrift.Compression.NONE, org.apache.cassandra.thrift.ConsistencyLevel.ANY);
        }
        catch (NotFoundException e)
        {
            e.printStackTrace();
        }
    }
}
