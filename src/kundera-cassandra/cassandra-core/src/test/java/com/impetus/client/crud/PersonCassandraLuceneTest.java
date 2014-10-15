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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;
import javax.persistence.TypedQuery;

import junit.framework.Assert;

import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnDef;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.IndexType;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.cassandra.thrift.CQLTranslator;
import com.impetus.kundera.client.cassandra.persistence.CassandraCli;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl;
import com.impetus.kundera.property.PropertyAccessorFactory;
import com.impetus.kundera.utils.LuceneCleanupUtilities;

/**
 * Test case to perform simple CRUD operation.(insert, delete, merge, and
 * select)
 * 
 * @author kuldeep.mishra
 * 
 *         Run this script to create column family in cassandra with indexes.
 *         create column family PERSON with comparator=UTF8Type and
 *         column_metadata=[{column_name: PERSON_NAME, validation_class:
 *         UTF8Type, index_type: KEYS}, {column_name: AGE, validation_class:
 *         IntegerType, index_type: KEYS}];
 * 
 */
public class PersonCassandraLuceneTest extends BaseTest
{
    private static final String LUCENE_IDX_CASSANDRA_TEST = "luceneCassandraTest";

    private static final boolean USE_CQL = false;

    /** The emf. */
    private EntityManagerFactory emf;

    /** The em. */
    private EntityManager em;

    /** The col. */
    private Map<Object, Object> col;

    /**
     * Sets the up.
     * 
     * @throws Exception
     *             the exception
     */
    @Before
    public void setUp() throws Exception
    {
        CassandraCli.cassandraSetUp();
        CassandraCli.createKeySpace("KunderaExamples");
        // loadData();
        Map propertyMap = new HashMap();
        // propertyMap.put(CassandraConstants.CQL_VERSION,
        // CassandraConstants.CQL_VERSION_3_0);
        emf = Persistence.createEntityManagerFactory(LUCENE_IDX_CASSANDRA_TEST, propertyMap);
        em = emf.createEntityManager();
        col = new java.util.HashMap<Object, Object>();
    }

    /**
     * On insert cassandra.
     * 
     * @throws Exception
     *             the exception
     */
    @Test
    public void onInsertCassandra() throws Exception
    {
        Object p1 = prepare("1", 10);
        Object p2 = prepare("2", 20);
        Object p3 = prepare("3", 15);
        em.persist(p1);
        em.persist(p2);
        em.persist(p3);
        col.put("1", p1);
        col.put("2", p2);
        col.put("3", p3);

        em.clear();
        PersonLuceneCassandra p = findById(PersonLuceneCassandra.class, "1", em);
        Assert.assertNotNull(p);
        Assert.assertEquals("vivek", p.getPersonName());

        em.clear();
        String qry = "Select p.personId,p.personName from PersonLuceneCassandra p where p.personId >= 1";
        Query q = em.createQuery(qry);
        List<PersonLuceneCassandra> persons = q.getResultList();

        assertFindByName(em, "PersonLuceneCassandra", PersonLuceneCassandra.class, "vivek", "personName");
        assertFindByNameAndAge(em, "PersonLuceneCassandra", PersonLuceneCassandra.class, "vivek", "10", "personName");
        assertFindByNameAndAgeGTAndLT(em, "PersonLuceneCassandra", PersonLuceneCassandra.class, "vivek", "10", "20",
                "personName");
        assertFindByNameAndAgeBetween(em, "PersonLuceneCassandra", PersonLuceneCassandra.class, "vivek", "10", "15",
                "personName");
        assertFindByRange(em, "PersonLuceneCassandra", PersonLuceneCassandra.class, "1", "2", "personId", false);
        assertFindWithoutWhereClause(em, "PersonLuceneCassandra", PersonLuceneCassandra.class, false);

        // OR clause test case.
        String orClauseQuery = "Select p from PersonLuceneCassandra p where p.personName = 'vivek1' OR p.age = 10";

        q = em.createQuery(orClauseQuery);

        List<PersonLuceneCassandra> results = q.getResultList();

        Assert.assertEquals(1, results.size());
        // perform merge after query.
        for (PersonLuceneCassandra person : persons)
        {
            person.setPersonName("after merge");
            em.merge(person);
        }

        em.clear();

        // select rowid test
        selectIdQuery();

        em.clear();

        p = findById(PersonLuceneCassandra.class, "1", em);
        Assert.assertNotNull(p);
        Assert.assertEquals("after merge", p.getPersonName());

        List<PersonLuceneCassandra> allPersons = em
                .createQuery("select p from PersonLuceneCassandra p where p.personId = :id",
                        PersonLuceneCassandra.class).setParameter("id", 1).getResultList();

        Assert.assertNotNull(allPersons);
        Assert.assertEquals(1, allPersons.size());

        // Delete without WHERE clause.

        String deleteQuery = "DELETE from PersonLuceneCassandra";
        q = em.createQuery(deleteQuery);
        Assert.assertEquals(3, q.executeUpdate());

    }

    /**
     * On merge cassandra.
     * 
     * @throws Exception
     *             the exception
     */
    @Test
    public void onMergeCassandra() throws Exception
    {
        // CassandraCli.cassandraSetUp();
        // loadData();
        Object p1 = prepare("1", 10);
        Object p2 = prepare("2", 20);
        Object p3 = prepare("3", 15);
        em.persist(p1);
        em.persist(p2);
        em.persist(p3);

        em.clear();
        col.put("1", p1);
        col.put("2", p2);
        col.put("3", p3);
        PersonLuceneCassandra p = findById(PersonLuceneCassandra.class, "1", em);
        Assert.assertNotNull(p);
        Assert.assertEquals("vivek", p.getPersonName());
        // modify record.
        p.setPersonName("newvivek");
        em.merge(p);

        assertOnMerge(em, "PersonLuceneCassandra", "vivek", "newvivek", "personName");
    }

    @Test
    public void onDeleteThenInsertCassandra() throws Exception
    {
        // CassandraCli.cassandraSetUp();
        // CassandraCli.initClient();
        // loadData();
        Object p1 = prepare("1", 10);
        Object p2 = prepare("2", 20);
        Object p3 = prepare("3", 15);
        em.persist(p1);
        em.persist(p2);
        em.persist(p3);

        col.put("1", p1);
        col.put("2", p2);
        col.put("3", p3);
        PersonLuceneCassandra p = findById(PersonLuceneCassandra.class, "1", em);
        Assert.assertNotNull(p);
        Assert.assertEquals("vivek", p.getPersonName());
        em.remove(p);
        em.clear();

        TypedQuery<PersonLuceneCassandra> query = em.createQuery("Select p from PersonLuceneCassandra p",
                PersonLuceneCassandra.class);

        List<PersonLuceneCassandra> results = query.getResultList();
        Assert.assertNotNull(query);
        Assert.assertNotNull(results);
        Assert.assertEquals(2, results.size());

        p1 = prepare("1", 10);
        em.persist(p1);

        query = em.createQuery("Select p from PersonLuceneCassandra p", PersonLuceneCassandra.class);

        results = query.getResultList();
        Assert.assertNotNull(query);
        Assert.assertNotNull(results);
        Assert.assertEquals(3, results.size());

    }

    @Test
    public void onRefreshCassandra() throws Exception
    {
        // cassandraSetUp();
        // CassandraCli.cassandraSetUp();
        // CassandraCli.createKeySpace("KunderaExamples");
        // loadData();
        CassandraCli.client.set_keyspace("KunderaExamples");
        Object p1 = prepare("1", 10);
        Object p2 = prepare("2", 20);
        Object p3 = prepare("3", 15);
        em.persist(p1);
        em.persist(p2);
        em.persist(p3);
        col.put("1", p1);
        col.put("2", p2);
        col.put("3", p3);

        // Check for contains
        Object pp1 = prepare("1", 10);
        Object pp2 = prepare("2", 20);
        Object pp3 = prepare("3", 15);
        Assert.assertTrue(em.contains(pp1));
        Assert.assertTrue(em.contains(pp2));
        Assert.assertTrue(em.contains(pp3));

        // Check for detach
        em.detach(pp1);
        em.detach(pp2);
        Assert.assertFalse(em.contains(pp1));
        Assert.assertFalse(em.contains(pp2));
        Assert.assertTrue(em.contains(pp3));

        // Modify value in database directly, refresh and then check PC
        em.clear();
        em = emf.createEntityManager();
        Object o1 = em.find(PersonLuceneCassandra.class, "1");

        if (!USE_CQL)
        {
            // Create Insertion List
            List<Mutation> insertionList = new ArrayList<Mutation>();
            List<Column> columns = new ArrayList<Column>();
            Column column = new Column();
            column.setName(PropertyAccessorFactory.STRING.toBytes("PERSON_NAME"));
            column.setValue(PropertyAccessorFactory.STRING.toBytes("Amry"));
            column.setTimestamp(System.currentTimeMillis());
            columns.add(column);
            Mutation mut = new Mutation();
            mut.setColumn_or_supercolumn(new ColumnOrSuperColumn().setColumn(column));
            insertionList.add(mut);
            // Create Mutation Map
            Map<String, List<Mutation>> columnFamilyValues = new HashMap<String, List<Mutation>>();
            columnFamilyValues.put("PERSON", insertionList);
            Map<ByteBuffer, Map<String, List<Mutation>>> mulationMap = new HashMap<ByteBuffer, Map<String, List<Mutation>>>();
            mulationMap.put(ByteBuffer.wrap("1".getBytes()), columnFamilyValues);
            CassandraCli.client.batch_mutate(mulationMap, ConsistencyLevel.ONE);
        }
        else
        {
            CQLTranslator translator = new CQLTranslator();
            String query = "insert into \"PERSONCASSANDRA\" (key,\"PERSON_NAME\",\"AGE\") values (1,'Amry',10 )";
            CassandraCli.client.execute_cql3_query(ByteBuffer.wrap(query.getBytes()), Compression.NONE,
                    ConsistencyLevel.ONE);
        }

        em.refresh(o1);
        Object oo1 = em.find(PersonLuceneCassandra.class, "1");
        Assert.assertTrue(em.contains(o1));
        Assert.assertEquals("Amry", ((PersonLuceneCassandra) oo1).getPersonName());
    }

    /**
     * On typed create query
     * 
     * @throws TException
     * @throws InvalidRequestException
     * @throws UnavailableException
     * @throws TimedOutException
     * @throws SchemaDisagreementException
     */
    @Test
    public void onTypedQuery() throws TException, InvalidRequestException, UnavailableException, TimedOutException,
            SchemaDisagreementException
    {
        // CassandraCli.createKeySpace("KunderaExamples");
        // loadData();

        Object p1 = prepare("1", 10);
        Object p2 = prepare("2", 20);
        Object p3 = prepare("3", 15);
        em.persist(p1);
        em.persist(p2);
        em.persist(p3);
        TypedQuery<PersonLuceneCassandra> query = em.createQuery("Select p from PersonLuceneCassandra p",
                PersonLuceneCassandra.class);

        List<PersonLuceneCassandra> results = query.getResultList();
        Assert.assertNotNull(query);
        Assert.assertNotNull(results);
        Assert.assertEquals(3, results.size());
    }

    /**
     * On typed create query
     * 
     * @throws TException
     * @throws InvalidRequestException
     * @throws UnavailableException
     * @throws TimedOutException
     * @throws SchemaDisagreementException
     */
    @Test
    public void onGenericTypedQuery() throws TException, InvalidRequestException, UnavailableException,
            TimedOutException, SchemaDisagreementException
    {
        // CassandraCli.createKeySpace("KunderaExamples");
        // loadData();

        Object p1 = prepare("1", 10);
        Object p2 = prepare("2", 20);
        Object p3 = prepare("3", 15);
        em.persist(p1);
        em.persist(p2);
        em.persist(p3);
        TypedQuery<Object> query = em.createQuery("Select p from PersonLuceneCassandra p", Object.class);

        List<Object> results = query.getResultList();
        Assert.assertNotNull(query);
        Assert.assertNotNull(results);
        Assert.assertEquals(3, results.size());
        Assert.assertEquals(PersonLuceneCassandra.class, results.get(0).getClass());
    }

    /**
     * on invalid typed query.
     * 
     * @throws TException
     * @throws InvalidRequestException
     * @throws UnavailableException
     * @throws TimedOutException
     * @throws SchemaDisagreementException
     */
    @Test
    public void onInvalidTypedQuery() throws TException, InvalidRequestException, UnavailableException,
            TimedOutException, SchemaDisagreementException
    {
        // CassandraCli.createKeySpace("KunderaExamples");
        // loadData();

        Object p1 = prepare("1", 10);
        Object p2 = prepare("2", 20);
        Object p3 = prepare("3", 15);
        em.persist(p1);
        em.persist(p2);
        em.persist(p3);

        TypedQuery<Group> query = null;
        try
        {
            query = em.createQuery("Select p from PersonLuceneCassandra p", Group.class);
            Assert.fail("Should have gone to catch block, as it is an invalid scenario!");
        }
        catch (IllegalArgumentException iaex)
        {
            Assert.assertNull(query);
        }
    }

    @Test
    public void onGhostRows() throws TException, InvalidRequestException, UnavailableException, TimedOutException,
            SchemaDisagreementException
    {
        // CassandraCli.createKeySpace("KunderaExamples");
        // loadData();
        Object p1 = prepare("1", 10);
        Object p2 = prepare("2", 20);
        Object p3 = prepare("3", 15);
        em.persist(p1);
        em.persist(p2);
        em.persist(p3);
        em.clear();
        PersonLuceneCassandra person = em.find(PersonLuceneCassandra.class, "1");
        em.remove(person);
        em.clear(); // just to make sure that not to be picked up from cache.
        TypedQuery<PersonLuceneCassandra> query = em.createQuery("Select p from PersonLuceneCassandra p",
                PersonLuceneCassandra.class);

        List<PersonLuceneCassandra> results = query.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(2, results.size());

    }

    private void selectIdQuery()
    {
        String query = "select p.personId from PersonLuceneCassandra p";
        Query q = em.createQuery(query);
        List<PersonLuceneCassandra> results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(3, results.size());
        Assert.assertNotNull(results.get(0).getPersonId());
        Assert.assertNull(results.get(0).getPersonName());

        query = "Select p.personId from PersonLuceneCassandra p where p.personName = vivek"; 
        q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertFalse(results.isEmpty());
        Assert.assertEquals(3, results.size());
        Assert.assertNotNull(results.get(0).getPersonId());
        Assert.assertNull(results.get(0).getPersonName());
        Assert.assertNull(results.get(0).getAge());

        q = em.createQuery("Select p.personId from PersonLuceneCassandra p where p.personName = vivek and p.age > " + 10);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertFalse(results.isEmpty());
        Assert.assertEquals(2, results.size());
        Assert.assertNotNull(results.get(0).getPersonId());
        Assert.assertNull(results.get(0).getPersonName());
        Assert.assertNull(results.get(0).getAge());

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
        emf.close();
        CassandraCli.dropKeySpace("KunderaExamples");
        LuceneCleanupUtilities.cleanLuceneDirectory(((EntityManagerFactoryImpl) emf).getKunderaMetadataInstance()
                .getApplicationMetadata().getPersistenceUnitMetadata(LUCENE_IDX_CASSANDRA_TEST));
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
        user_Def.setKey_validation_class("UTF8Type");
        ColumnDef columnDef = new ColumnDef(ByteBuffer.wrap("PERSON_NAME".getBytes()), "UTF8Type");
        columnDef.index_type = IndexType.KEYS;
        user_Def.addToColumn_metadata(columnDef);
        ColumnDef columnDef1 = new ColumnDef(ByteBuffer.wrap("AGE".getBytes()), "UTF8Type");
        columnDef1.index_type = IndexType.KEYS;
        user_Def.addToColumn_metadata(columnDef1);
        ColumnDef columnDef2 = new ColumnDef(ByteBuffer.wrap("ENUM".getBytes()), "UTF8Type");
        columnDef2.index_type = IndexType.KEYS;
        user_Def.addToColumn_metadata(columnDef2);

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

    /**
     * Prepare data.
     * 
     * @param rowKey
     *            the row key
     * @param age
     *            the age
     * @return the person
     */
    private PersonLuceneCassandra prepare(String rowKey, int age)
    {
        PersonLuceneCassandra o = new PersonLuceneCassandra();
        o.setPersonId(rowKey);
        o.setPersonName("vivek");
        o.setAge(age + "");
        o.setDay(com.impetus.client.crud.PersonLuceneCassandra.Day.MONDAY);
        return o;
    }

    private void assertOnMerge(EntityManager em, String clazz, String oldName, String newName, String fieldName)
    {
        Query q = em.createQuery("Select p from " + clazz + " p where p." + fieldName + " = " + oldName);
        List<PersonLuceneCassandra> results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(2, results.size());

        q = em.createQuery("Select p from " + clazz + " p where p." + fieldName + " = " + newName);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
        Assert.assertNotSame(oldName, getPersonName(results.get(0)));
        Assert.assertEquals(newName, getPersonName(results.get(0)));
    }

    /**
     * Gets the person name.
     * 
     * @param <E>
     *            the element type
     * @param e
     *            the e
     * @param result
     *            the result
     * @return the person name
     */
    private String getPersonName(Object result)
    {

        return ((PersonLuceneCassandra) result).getPersonName();
    }

}