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
package com.impetus.client.crud;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

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
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.persistence.CassandraCli;

/**
 * The Class EntityTransactionTest.
 * 
 * @author vivek.mishra
 */
public class EntityTransactionTest extends BaseTest
{

    /** The emf. */
    private static EntityManagerFactory emf;

    /** The em. */
    private static EntityManager em;

    /**
     * Sets the up.
     * 
     * @throws Exception
     *             the exception
     */
    @Before
    public void setUp() throws Exception
    {

        // cassandraSetUp();
        CassandraCli.cassandraSetUp();
        CassandraCli.createKeySpace("KunderaExamples");
        loadData();

        emf = Persistence.createEntityManagerFactory("secIdxCassandraTest");
        em = emf.createEntityManager();
    }

    /**
     * On rollback.
     * 
     * @throws IOException
     *             Signals that an I/O exception has occurred.
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
    @Test
    public void onRollback() throws IOException, TException, InvalidRequestException, UnavailableException,
            TimedOutException, SchemaDisagreementException
    {

        em.getTransaction().begin();
        Object p1 = prepareData("1", 10);
        Object p2 = prepareData("2", 20);
        Object p3 = prepareData("3", 15);
        em.persist(p1);
        em.persist(p2);
        em.persist(p3);

        // roll back.
        em.getTransaction().rollback();

        em.getTransaction().begin();

        PersonCassandra p = findById(PersonCassandra.class, "1", em);
        Assert.assertNull(p);

        // on commit.
        em.getTransaction().commit();

        // Still no record should be flushed as already rollback!
        p = findById(PersonCassandra.class, "1", em);
        Assert.assertNull(p);
    }

    /**
     * On commit.
     * 
     * @throws IOException
     *             Signals that an I/O exception has occurred.
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
    @Test
    public void onCommit() throws IOException, TException, InvalidRequestException, UnavailableException,
            TimedOutException, SchemaDisagreementException
    {

        em.getTransaction().begin();

        Object p1 = prepareData("1", 10);
        Object p2 = prepareData("2", 20);
        Object p3 = prepareData("3", 15);
        em.persist(p1);
        em.persist(p2);
        em.persist(p3);

        // on commit.
        em.getTransaction().commit();

        PersonCassandra p = findById(PersonCassandra.class, "1", em);
        Assert.assertNotNull(p);

        em.getTransaction().begin();

        ((PersonCassandra) p2).setPersonName("rollback");
        em.merge(p2);

        // roll back, should roll back person name for p2!
        em.getTransaction().rollback();

        p = findById(PersonCassandra.class, "1", em);
        Assert.assertNotNull(p);

        p = findById(PersonCassandra.class, "2", em);
        Assert.assertNotNull(p);
        Assert.assertEquals("vivek", p.getPersonName());
        Assert.assertNotSame("rollback", p.getPersonName());

    }

    /**
     * Rollback on error.
     * 
     * @throws IOException
     *             Signals that an I/O exception has occurred.
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
    @Test
    public void rollbackOnError() throws IOException, TException, InvalidRequestException, UnavailableException,
            TimedOutException, SchemaDisagreementException
    {
        PersonCassandra p = null;
        try
        {
            Object p1 = prepareData("1", 10);
            Object p2 = prepareData("2", 20);
            em.persist(p1);
            em.persist(p2);

            p = findById(PersonCassandra.class, "1", em);
            Assert.assertNotNull(p);

            Object p3 = prepareData("3", 15);
            em.persist(p3);

            // Assert on rollback on error.
            ((PersonCassandra) p2).setPersonName("rollback");
            em.merge(p2);
            em.merge(null);

            // As this is a runtime exception so rollback should happen and
            // delete out commited data.
        }
        catch (Exception ex)
        {

            p = findById(PersonCassandra.class, "1", em);
            Assert.assertNull(p);

            p = findById(PersonCassandra.class, "2", em);
            Assert.assertNull(p);

            p = findById(PersonCassandra.class, "3", em);
            Assert.assertNull(p);
        }
        em.clear();
        // persist with 1 em
        EntityManager em1 = emf.createEntityManager();
//        em1.setFlushMode(FlushModeType.COMMIT);
        em1.getTransaction().begin();
        Object p3 = prepareData("4", 15);
        em1.persist(p3);
        em1.getTransaction().commit();

        try
        {
            // remove with another em with auto flush.
            EntityManager em2 = emf.createEntityManager();
            PersonCassandra person = em2.find(PersonCassandra.class, "4");
            em2.remove(person);
            em2.merge(null);
        }
        catch (Exception ex)
        {
            // Deleted records cannot be rolled back in cassandra!
            // em1.clear();

            p = findById(PersonCassandra.class, "4", em1);
            Assert.assertNotNull(p);
            Assert.assertEquals("vivek", p.getPersonName());

        }
    }

    /**
     * Roll back with multi transactions.
     */
    @Test
    public void rollBackWithMultiTransactions()
    {
        EntityManager em1 = emf.createEntityManager();
//        em1.setFlushMode(FlushModeType.COMMIT);

        // Begin transaction.
        em1.getTransaction().begin();
        Object p1 = prepareData("11", 10);
        em1.persist(p1);

        // commit p1.
        em1.getTransaction().commit();

        // another em instance
        EntityManager em2 = emf.createEntityManager();
//        em2.setFlushMode(FlushModeType.COMMIT);

        // begin transaction.
        em2.getTransaction().begin();
        PersonCassandra found = em2.find(PersonCassandra.class, "11");
        found.setPersonName("merged");
        em2.merge(found);

        // commit p1 after modification.
        em2.getTransaction().commit();

        // open another entity manager.
        EntityManager em3 = emf.createEntityManager();
        found = em3.find(PersonCassandra.class, "11");
        found.setPersonName("lastemerge");
        try
        {
            em3.merge(found);
            em3.merge(null);
        }
        catch (Exception ex)
        {
            PersonCassandra finalFound = em2.find(PersonCassandra.class, "11");
            Assert.assertNotNull(finalFound);
            Assert.assertEquals("merged", finalFound.getPersonName());
        }
    }

    /**
     * Tear down.
     * 
     * @throws Exception
     *             the exception
     */
    @After
    public void tearDown() throws Exception
    {/*
      * Delete is working, but as row keys are not deleted from cassandra, so
      * resulting in issue while reading back. // Delete
      * em.remove(em.find(Person.class, "1")); em.remove(em.find(Person.class,
      * "2")); em.remove(em.find(Person.class, "3")); em.close(); emf.close();
      * em = null; emf = null;
      */
        CassandraCli.dropKeySpace("KunderaExamples");
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
        user_Def.name = "PERSON";
        user_Def.keyspace = "KunderaExamples";
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
