/*******************************************************************************
 * * Copyright 2012 Impetus Infotech.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 ******************************************************************************/
package com.impetus.kundera.tests.persistence.jta;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
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

import com.impetus.kundera.client.cassandra.persistence.CassandraCli;
import com.impetus.kundera.persistence.jta.KunderaJTAUserTransaction;

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

    private Integer i = 0;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        System.setProperty(Context.INITIAL_CONTEXT_FACTORY, "org.apache.naming.java.javaURLContextFactory");
        System.setProperty(Context.URL_PKG_PREFIXES, "org.apache.naming");

        initialContext = new InitialContext();
        initialContext.createSubcontext("java:comp");
        // This is what we need to bind to get handle of JTA's
        // UserTransaction.

        initialContext.bind("java:comp/UserTransaction", new KunderaJTAUserTransaction());

        CassandraCli.cassandraSetUp();
        CassandraCli.dropKeySpace("KunderaTests");
        CassandraCli.createKeySpace("KunderaTests");
        emf = Persistence.createEntityManagerFactory("secIdxAddCassandraJTA,addMongoJTA");
        em = emf.createEntityManager();
        loadData();
    }

    @Test
    public void testConcurrentPersist() throws NotSupportedException, SystemException, NamingException,
            SecurityException, IllegalStateException, RollbackException, HeuristicMixedException,
            HeuristicRollbackException
    {

        userTransaction = (UserTransaction) initialContext.lookup("java:comp/UserTransaction");

        userTransaction.begin();

        for (i = 0; i < 100; i++)
        {
            Runnable r = onExecute();
            r.run();
        }

        userTransaction.commit();

        userTransaction.begin();
        // As data is commited, hence it should return values with other
        // session.
        for (i = 0; i < 100; i++)
        {
            EntityManager em1 = emf.createEntityManager();
            Assert.assertNotNull(em1.find(PersonnelOToOFKEntityJTA.class, "1_p" + i));
        }
    }

    private Runnable onExecute()
    {
        Runnable r = new Runnable()
        {

            @Override
            public void run()
            {
                PersonnelOToOFKEntityJTA person = new PersonnelOToOFKEntityJTA();
                person.setPersonId("1_p" + i);
                person.setPersonName("crossdata-store");
                HabitatOToOFKEntityJTA address = new HabitatOToOFKEntityJTA();
                address.setAddressId("1_a" + i);
                address.setStreet("my street");
                person.setAddress(address);
                try
                {
                    em.persist(person);
                }
                catch (Exception ex)
                {
                    HabitatOToOFKEntityJTA found = em.find(HabitatOToOFKEntityJTA.class, "1_a" + i);
                    Assert.assertNull(found);
                }

                // As data is not commited, hence it should return null with
                // other session.
                EntityManager em1 = emf.createEntityManager();
                Assert.assertNull(em1.find(PersonnelOToOFKEntityJTA.class, "1_p" + i));
            }
        };
        return r;
    }

    @Test
    public void testPersist() throws NotSupportedException, SystemException, NamingException, SecurityException,
            IllegalStateException, RollbackException, HeuristicMixedException, HeuristicRollbackException
    {
        userTransaction = (UserTransaction) initialContext.lookup("java:comp/UserTransaction");
        em.setFlushMode(FlushModeType.COMMIT);

        userTransaction.begin();
        PersonnelOToOFKEntityJTA person = new PersonnelOToOFKEntityJTA();
        person.setPersonId("1_p");
        person.setPersonName("crossdata-store");
        HabitatOToOFKEntityJTA address = new HabitatOToOFKEntityJTA();
        address.setAddressId("1_a");
        address.setStreet("my street");
        person.setAddress(address);
        try
        {
            em.persist(person);
        }
        catch (Exception ex)
        {
            HabitatOToOFKEntityJTA found = em.find(HabitatOToOFKEntityJTA.class, "1_a");
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
        initialContext.unbind("java:comp/UserTransaction");
        initialContext.destroySubcontext("java:comp");

        // CassandraCli.dropKeySpace("KunderaTests");
        CassandraCli.truncateColumnFamily("KunderaTests", "PERSONNEL");
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

        KsDef ksDef = null;
        CfDef user_Def = new CfDef();
        user_Def.name = "PERSONNEL";
        user_Def.keyspace = "KunderaTests";
        user_Def.setComparator_type("UTF8Type");
        user_Def.setKey_validation_class("UTF8Type");
        ColumnDef columnDef = new ColumnDef(ByteBuffer.wrap("PERSON_NAME".getBytes()), "UTF8Type");
        columnDef.index_type = IndexType.KEYS;
        user_Def.addToColumn_metadata(columnDef);
        ColumnDef columnDef1 = new ColumnDef(ByteBuffer.wrap("ADDRESS_ID".getBytes()), "UTF8Type");
        columnDef1.index_type = IndexType.KEYS;
        user_Def.addToColumn_metadata(columnDef1);

        List<CfDef> cfDefs = new ArrayList<CfDef>();
        cfDefs.add(user_Def);

        try
        {
            ksDef = CassandraCli.client.describe_keyspace("KunderaTests");
            CassandraCli.client.set_keyspace("KunderaTests");
            // if (!CassandraCli.columnFamilyExist("PERSONNEL", "KunderaTests"))
            // {
            // CassandraCli.client.system_add_column_family(user_Def);
            // }
            // else
            // {
            // CassandraCli.truncateColumnFamily("KunderaTests", "PERSONNEL");
            // }

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
            // Set replication factor
            if (ksDef.strategy_options == null)
            {
                ksDef.strategy_options = new LinkedHashMap<String, String>();
            }
            // Set replication factor, the value MUST be an integer
            ksDef.strategy_options.put("replication_factor", "1");
            CassandraCli.client.system_add_keyspace(ksDef);
        }

        CassandraCli.client.set_keyspace("KunderaTests");
    }
}
