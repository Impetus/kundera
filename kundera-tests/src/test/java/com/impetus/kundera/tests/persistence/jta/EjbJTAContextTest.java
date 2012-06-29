package com.impetus.kundera.tests.persistence.jta;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.FlushModeType;
import javax.persistence.Persistence;
import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.UserTransaction;

import junit.framework.Assert;

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

import com.impetus.kundera.persistence.jta.KunderaJTAUserTransaction;
import com.impetus.kundera.tests.cli.CassandraCli;
import com.impetus.kundera.tests.crossdatastore.useraddress.entities.HabitatOToOFKEntity;
import com.impetus.kundera.tests.crossdatastore.useraddress.entities.PersonnelOToOFKEntity;

/**
 * @author vivek.mishra
 * 
 */
public class EjbJTAContextTest
{
    private InitialContext initialContext;

    private UserTransaction userTransaction;

    private EntityManagerFactory emf;

    private EntityManager em;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {

//        Properties properties = new Properties();
//        properties.put(Context.INITIAL_CONTEXT_FACTORY, "org.apache.openejb.client.LocalInitialContextFactory");
//        // DO NOT DELETE IT !!!
//        // properties.put("openejb.deployments.classpath.ear", "true");
//        // properties.put("openejb.jndiname.format","{deploymentId}/{interfaceType.annotationName}");
//        // properties.put("openejb.altdd.prefix", "test");
//        // properties.put("openejb.validation.output.level", "VERBOSE");
//        // initialContext = new InitialContext(properties);
//        initialContext = new InitialContext(properties);
//        initialContext.bind("inject", this);
//        initialContext.bind("java:comp/UserTransaction", new KunderaJTAUserTransaction());

        System.setProperty(Context.INITIAL_CONTEXT_FACTORY, "org.apache.naming.java.javaURLContextFactory");
        System.setProperty(Context.URL_PKG_PREFIXES, "org.apache.naming");

        initialContext = new InitialContext();
        initialContext.createSubcontext("java:comp");
        // This is what we need to bind to get handle of JTA's
        // UserTransaction.

        initialContext.bind("java:comp/UserTransaction", new KunderaJTAUserTransaction());

        emf = Persistence.createEntityManagerFactory("secIdxAddCassandraJTA,addMongoJTA");
        em = emf.createEntityManager();
        CassandraCli.cassandraSetUp();
        CassandraCli.createKeySpace("KunderaTests");
        loadData();
    }

    @Test
    public void testPersist() throws NotSupportedException, SystemException, NamingException, SecurityException,
            IllegalStateException, RollbackException, HeuristicMixedException, HeuristicRollbackException
    {

        userTransaction = (UserTransaction) initialContext.lookup("java:comp/UserTransaction");
        em.setFlushMode(FlushModeType.COMMIT);

        userTransaction.begin();
        PersonnelOToOFKEntity person = new PersonnelOToOFKEntity();
        person.setPersonId("1_p");
        person.setPersonName("crossdata-store");
        HabitatOToOFKEntity address = new HabitatOToOFKEntity();
        address.setAddressId("1_a");
        address.setStreet("my street");
        person.setAddress(address);
        try
        {
            em.persist(person);
        }
        catch (Exception ex)
        {
            HabitatOToOFKEntity found = em.find(HabitatOToOFKEntity.class, "1_a");
            Assert.assertNull(found);
        }
        userTransaction.commit();
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
        CassandraCli.dropKeySpace("KunderaTests");
    }
    
    /**
     * Load cassandra specific data.
     *
     * @throws TException the t exception
     * @throws InvalidRequestException the invalid request exception
     * @throws UnavailableException the unavailable exception
     * @throws TimedOutException the timed out exception
     * @throws SchemaDisagreementException the schema disagreement exception
     */
    private void loadData() throws TException, InvalidRequestException, UnavailableException, TimedOutException,
            SchemaDisagreementException
    {

        KsDef ksDef = null;
        CfDef user_Def = new CfDef();
        user_Def.name = "PERSONNEL";
        user_Def.keyspace = "KunderaTests";
        user_Def.setComparator_type("UTF8Type");
        user_Def.setDefault_validation_class("UTF8Type");
        ColumnDef columnDef = new ColumnDef(ByteBuffer.wrap("PERSON_NAME".getBytes()), "UTF8Type");
        columnDef.index_type = IndexType.KEYS;
        user_Def.addToColumn_metadata(columnDef);
        ColumnDef columnDef1 = new ColumnDef(ByteBuffer.wrap("AGE".getBytes()), "UTF8Type");
        columnDef1.index_type = IndexType.KEYS;
        user_Def.addToColumn_metadata(columnDef1);

        List<CfDef> cfDefs = new ArrayList<CfDef>();
        cfDefs.add(user_Def);

        try
        {
            ksDef = CassandraCli.client.describe_keyspace("KunderaTests");
            CassandraCli.client.set_keyspace("KunderaTests");

            List<CfDef> cfDefn = ksDef.getCf_defs();

            for (CfDef cfDef1 : cfDefn)
            {

                if (cfDef1.getName().equalsIgnoreCase("PERSONNEL"))
                {

                    CassandraCli.client.system_drop_column_family("PERSONNEL");

                }
            }
            CassandraCli.client.system_add_column_family(user_Def);

        }
        catch (NotFoundException e)
        {

            ksDef = new KsDef("KunderaTests", "org.apache.cassandra.locator.SimpleStrategy", cfDefs);
            ksDef.setReplication_factor(1);
            CassandraCli.client.system_add_keyspace(ksDef);
        }

        com.impetus.kundera.tests.cli.CassandraCli.client.set_keyspace("KunderaTests");

    }


}
