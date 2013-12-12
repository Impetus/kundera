package com.impetus.client.crud;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.ColumnDef;
import org.apache.cassandra.thrift.IndexType;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.persistence.CassandraCli;

public class CassandraThriftSecondaryTableTest extends SecondaryTableTestBase
{

    private EntityManagerFactory emf;

    @Before
    public void setUp() throws Exception
    {
        CassandraCli.cassandraSetUp();
        CassandraCli.createKeySpace("KunderaExamples");
        loadData();
        emf = Persistence.createEntityManagerFactory("secIdxCassandraTest");
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
        testCRUD(emf);
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
        KsDef ksDef = null;
        CfDef user_Def1 = new CfDef();
        user_Def1.name = table1;
        user_Def1.keyspace = keyspace;
        user_Def1.column_type = "Super";


        CfDef user_Def2 = new CfDef();
        user_Def2.name = table2;
        user_Def2.keyspace = keyspace;
        user_Def2.column_type = "Super";

        List<CfDef> cfDefs = new ArrayList<CfDef>();
        cfDefs.add(user_Def1);
        cfDefs.add(user_Def2);

        try
        {
            ksDef = CassandraCli.client.describe_keyspace(keyspace);
            CassandraCli.client.set_keyspace(keyspace);

            List<CfDef> cfDefn = ksDef.getCf_defs();

            for (CfDef cfDef1 : cfDefn)
            {

                if (cfDef1.getName().equalsIgnoreCase(table1))
                {
                    CassandraCli.client.system_drop_column_family(table1);
                }
                if (cfDef1.getName().equalsIgnoreCase(table2))
                {
                    CassandraCli.client.system_drop_column_family(table2);
                }
            }
            CassandraCli.client.system_add_column_family(user_Def1);
            CassandraCli.client.system_add_column_family(user_Def2);

        }
        catch (NotFoundException e)
        {

            ksDef = new KsDef(keyspace, "org.apache.cassandra.locator.SimpleStrategy", cfDefs);
            // Set replication factor
            if (ksDef.strategy_options == null)
            {
                ksDef.strategy_options = new LinkedHashMap<String, String>();
            }
            // Set replication factor, the value MUST be an integer
            ksDef.strategy_options.put("replication_factor", "1");
            CassandraCli.client.system_add_keyspace(ksDef);
        }
    }
}
