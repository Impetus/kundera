/**
 * 
 */
package com.impetus.client.crud.compositeType.association;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.ColumnDef;
import org.apache.cassandra.thrift.IndexType;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.persistence.CassandraCli;

/**
 * @author impadmin
 * 
 */
public class CassandraUserOTMTest
{

    private static final String _KEYSPACE = "KunderaExamples";

    private EntityManagerFactory emf;

    private EntityManager em;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        CassandraCli.cassandraSetUp();
        CassandraCli.createKeySpace(_KEYSPACE);
        loadData();
        // HashMap propertyMap = new HashMap();
        // propertyMap.put(CassandraConstants.CQL_VERSION,
        // CassandraConstants.CQL_VERSION_3_0);
        // propertyMap.put(PersistenceProperties.KUNDERA_DDL_AUTO_PREPARE,
        // "create");
        emf = Persistence.createEntityManagerFactory("secIdxCassandraTest");
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
        CassandraCli.dropKeySpace(_KEYSPACE);
        emf.close();
    }

    @Test
    public void test()
    {
        em = emf.createEntityManager();

        Set<CassandraAddressUniOTM> addresses = new HashSet<CassandraAddressUniOTM>();

        CassandraAddressUniOTM address1 = new CassandraAddressUniOTM();
        address1.setAddressId("a");
        address1.setStreet("my street");
        CassandraAddressUniOTM address2 = new CassandraAddressUniOTM();
        address2.setAddressId("b");
        address2.setStreet("my new street");

        addresses.add(address1);
        addresses.add(address2);

        CassandraUserUniOTM userUniOTM = new CassandraUserUniOTM();
        userUniOTM.setPersonId("1");
        userUniOTM.setPersonName("kuldeep");
        userUniOTM.setAddresses(addresses);

        em.persist(userUniOTM);

        em.clear();

        CassandraUserUniOTM foundObject = em.find(CassandraUserUniOTM.class, "1");
        Assert.assertNotNull(foundObject);
        Assert.assertEquals(2, foundObject.getAddresses().size());

        Query q = em.createQuery("Select u from CassandraUserUniOTM u");
        List<CassandraUserUniOTM> users = q.getResultList();
        Assert.assertNotNull(users);
        Assert.assertEquals(1, users.size());
        Assert.assertNotNull(users.get(0));
        Assert.assertEquals("kuldeep", users.get(0).getPersonName());
        Assert.assertEquals("1", users.get(0).getPersonId());
        Assert.assertNotNull(users.get(0).getAddresses());
        Assert.assertEquals(2, users.get(0).getAddresses().size());

        em.remove(foundObject);

        foundObject = em.find(CassandraUserUniOTM.class, "1");
        Assert.assertNull(foundObject);
    }

    /**
     * Loads data.
     * 
     * @throws InvalidRequestException
     * @throws SchemaDisagreementException
     * @throws TException
     */
    private void loadData() throws InvalidRequestException, SchemaDisagreementException, TException
    {
        List<CfDef> cfDefs = new ArrayList<CfDef>();

        CfDef user = new CfDef(_KEYSPACE, "CassandraUserUniOTM");
        user.setKey_validation_class("UTF8Type");
        user.setDefault_validation_class("UTF8Type");
        user.setComparator_type("UTF8Type");
        ColumnDef columnDef = new ColumnDef(ByteBuffer.wrap("PERSON_NAME".getBytes()), "UTF8Type");
        columnDef.index_type = IndexType.KEYS;
        user.addToColumn_metadata(columnDef);

        CfDef address = new CfDef(_KEYSPACE, "CassandraAddressUniOTM");
        address.setKey_validation_class("UTF8Type");
        address.setDefault_validation_class("UTF8Type");
        address.setComparator_type("UTF8Type");
        ColumnDef columnDef1 = new ColumnDef(ByteBuffer.wrap("PERSON_ID".getBytes()), "UTF8Type");
        columnDef1.index_type = IndexType.KEYS;
        ColumnDef columnDef2 = new ColumnDef(ByteBuffer.wrap("STREET".getBytes()), "UTF8Type");
        columnDef2.index_type = IndexType.KEYS;

        address.addToColumn_metadata(columnDef1);
        address.addToColumn_metadata(columnDef2);

        cfDefs.add(user);
        cfDefs.add(address);
        KsDef ksDef = null;
        try
        {
            CassandraCli.initClient();
            ksDef = CassandraCli.client.describe_keyspace(_KEYSPACE);
            CassandraCli.client.set_keyspace(_KEYSPACE);

            List<CfDef> cfDefn = ksDef.getCf_defs();

            for (CfDef cfDef1 : cfDefn)
            {
                if (cfDef1.getName().equals("CassandraUserUniOTM"))
                {
                    CassandraCli.client.system_drop_column_family("CassandraUserUniOTM");
                }
                if (cfDef1.getName().equals("CassandraAddressUniOTM"))
                {
                    CassandraCli.client.system_drop_column_family("CassandraAddressUniOTM");
                }
            }
            CassandraCli.client.system_add_column_family(user);
            CassandraCli.client.system_add_column_family(address);
        }
        catch (NotFoundException e)
        {

            ksDef = new org.apache.cassandra.thrift.KsDef(_KEYSPACE, "org.apache.cassandra.locator.SimpleStrategy",
                    cfDefs);

            if (ksDef.strategy_options == null)
            {
                ksDef.strategy_options = new LinkedHashMap<String, String>();
            }
            ksDef.strategy_options.put("replication_factor", "1");
            CassandraCli.client.system_add_keyspace(ksDef);

            CassandraCli.client.set_keyspace(_KEYSPACE);
        }
        catch (TException e)
        {
            e.printStackTrace();
        }
        catch (InvalidRequestException e)
        {
            e.printStackTrace();
        }
        catch (SchemaDisagreementException e)
        {
            e.printStackTrace();
        }
    }
}
