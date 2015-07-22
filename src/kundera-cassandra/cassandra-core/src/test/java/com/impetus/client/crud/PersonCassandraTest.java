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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.NoResultException;
import javax.persistence.Persistence;
import javax.persistence.Query;
import javax.persistence.TypedQuery;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Expression;
import javax.persistence.criteria.Root;

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

import com.impetus.client.cassandra.common.CassandraConstants;
import com.impetus.client.cassandra.thrift.CQLTranslator;
import com.impetus.client.cassandra.thrift.ThriftClient;
import com.impetus.client.crud.PersonCassandra.Day;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.cassandra.persistence.CassandraCli;
import com.impetus.kundera.property.PropertyAccessorFactory;

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
public class PersonCassandraTest extends BaseTest
{

    /** The Constant SEC_IDX_CASSANDRA_TEST. */
    private static final String SEC_IDX_CASSANDRA_TEST = "genericCassandraTest";

    /** The emf. */
    private EntityManagerFactory emf;

    /** The em. */
    private EntityManager entityManager;

    /** The col. */
    private Map<Object, Object> col;

    /** The property map. */
    protected Map propertyMap = null;

    /** The auto manage schema. */
    protected boolean AUTO_MANAGE_SCHEMA = true;

    /** The use cql. */
    protected boolean USE_CQL = false;

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

        if (propertyMap == null)
        {
            propertyMap = new HashMap();
            propertyMap.put(PersistenceProperties.KUNDERA_DDL_AUTO_PREPARE, "create");
        }

        if (AUTO_MANAGE_SCHEMA)
        {
            // loadData();
        }
        emf = Persistence.createEntityManagerFactory(SEC_IDX_CASSANDRA_TEST, propertyMap);
        entityManager = emf.createEntityManager();
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
        Object p1 = prepareData("1", 10);
        Object p2 = prepareData("2", 20);
        Object p3 = prepareData("3", 15);

        Query findQuery = entityManager.createQuery("Select p from PersonCassandra p", PersonCassandra.class);
        List<PersonCassandra> allPersons = findQuery.getResultList();
        Assert.assertNotNull(allPersons);
        Assert.assertTrue(allPersons.isEmpty());

        findQuery = entityManager.createQuery("Select p from PersonCassandra p where p.personName = vivek");
        allPersons = findQuery.getResultList();
        Assert.assertNotNull(allPersons);
        Assert.assertTrue(allPersons.isEmpty());

        findQuery = entityManager.createQuery("Select p.age from PersonCassandra p where p.personName = vivek");
        allPersons = findQuery.getResultList();
        Assert.assertNotNull(allPersons);
        Assert.assertTrue(allPersons.isEmpty());

        entityManager.persist(p1);
        entityManager.persist(p2);
        entityManager.persist(p3);

        PersonCassandra personWithKey = new PersonCassandra();
        personWithKey.setPersonId("111");
        entityManager.persist(personWithKey);
        col.put("1", p1);
        col.put("2", p2);
        col.put("3", p3);

        entityManager.clear();
        PersonCassandra p = findById(PersonCassandra.class, "1", entityManager);
        Assert.assertNotNull(p);
        Assert.assertEquals("vivek", p.getPersonName());
        Assert.assertEquals(Day.thursday, p.getDay());

        entityManager.clear();
        Query q;
        List<PersonCassandra> persons = queryOverRowkey();

        assertFindByName(entityManager, "PersonCassandra", PersonCassandra.class, "vivek", "personName");
        assertFindByNameAndAge(entityManager, "PersonCassandra", PersonCassandra.class, "vivek", "10", "personName");
        assertFindByNameAndAgeGTAndLT(entityManager, "PersonCassandra", PersonCassandra.class, "vivek", "10", "20",
                "personName");
        assertFindByNameAndAgeBetween(entityManager, "PersonCassandra", PersonCassandra.class, "vivek", "10", "15",
                "personName");
        assertFindByRange(entityManager, "PersonCassandra", PersonCassandra.class, "1", "2", "personId", USE_CQL);
        assertFindWithoutWhereClause(entityManager, "PersonCassandra", PersonCassandra.class, USE_CQL);

        // perform merge after query.
        for (PersonCassandra person : persons)
        {
            person.setPersonName("'after merge'");
            person.setDay(null);
            entityManager.merge(person);

        }

        entityManager.clear();

        p = findById(PersonCassandra.class, "1", entityManager);
        Assert.assertNotNull(p);
        Assert.assertEquals("'after merge'", p.getPersonName());
        Assert.assertEquals(new Integer(10), p.getAge());

        String updateQuery = "update PersonCassandra p set p.personName='KK MISHRA' where p.personId=1";
        q = entityManager.createQuery(updateQuery);
        q.executeUpdate();

        entityManager.clear();
        p = findById(PersonCassandra.class, "1", entityManager);
        Assert.assertNotNull(p);
        Assert.assertEquals("KK MISHRA", p.getPersonName());

        // Test single result.
        Query query = entityManager.createQuery("select p from PersonCassandra p");
        query.setMaxResults(1);
        PersonCassandra result = (PersonCassandra) (query.getSingleResult());
        Assert.assertNotNull(result);
        Assert.assertEquals(Month.APRIL, result.getMonth());

        query = entityManager.createQuery("select p from PersonCassandra p where p.personName = vivek");
        try
        {
            result = (PersonCassandra) (query.getSingleResult());
            Assert.fail("Should have gone to catch block!");
        }
        catch (NoResultException nrex)
        {
            Assert.assertNotNull(nrex.getMessage());
        }

        // Test count native query.
        testCountResult();

        testCriteriaCountResult();

        testLightWeightTransactions();

        testINClause();

        // Delete without WHERE clause.
        String deleteQuery = "DELETE from PersonCassandra";
        q = entityManager.createQuery(deleteQuery);
        if (USE_CQL)
        {
            Assert.assertEquals(4, q.executeUpdate());
        }
        else
        {
            Assert.assertEquals(3, q.executeUpdate());
        }

    }

    /**
     * Test light weight transactions.
     */
    private void testLightWeightTransactions()
    {
        CQLTranslator translator = new CQLTranslator();
        if (USE_CQL)
        {
            Map<String, Client> clientMap = (Map<String, Client>) entityManager.getDelegate();
            ThriftClient tc = (ThriftClient) clientMap.get(SEC_IDX_CASSANDRA_TEST);
            tc.setCqlVersion(CassandraConstants.CQL_VERSION_3_0);
        }
        String key = USE_CQL ? "\"personId\"" : "key";

        String query = "INSERT INTO " + translator.ensureCase(new StringBuilder(), "PERSONCASSANDRA", false).toString()
                + " (" + key + ", \"PERSON_NAME\") VALUES ('1', 'Karthik') IF NOT EXISTS";
        Query q = entityManager.createNativeQuery(query, PersonCassandra.class);
        q.executeUpdate();
        entityManager.clear();
        PersonCassandra p = findById(PersonCassandra.class, "1", entityManager);
        Assert.assertNotNull(p);
        Assert.assertEquals("KK MISHRA", p.getPersonName());

        query = "INSERT INTO " + translator.ensureCase(new StringBuilder(), "PERSONCASSANDRA", false).toString() + " ("
                + key + ", \"PERSON_NAME\") VALUES ('4', 'Karthik') IF NOT EXISTS";

        q = entityManager.createNativeQuery(query, PersonCassandra.class);
        q.executeUpdate();
        entityManager.clear();
        p = findById(PersonCassandra.class, "4", entityManager);
        Assert.assertNotNull(p);
        Assert.assertEquals("Karthik", p.getPersonName());

        query = "UPDATE " + translator.ensureCase(new StringBuilder(), "PERSONCASSANDRA", false).toString()
                + " SET \"PERSON_NAME\" = 'Pragalbh' WHERE " + key + " = '4' IF \"PERSON_NAME\" = 'Karthik'";
        q = entityManager.createNativeQuery(query, PersonCassandra.class);
        q.executeUpdate();
        entityManager.clear();
        p = findById(PersonCassandra.class, "4", entityManager);
        Assert.assertNotNull(p);
        Assert.assertEquals("Pragalbh", p.getPersonName());
        String deleteQuery = "DELETE from PersonCassandra p WHERE p.personId = '4'";
        q = entityManager.createQuery(deleteQuery);
        q.executeUpdate();

    }

    /**
     * test IN clause in select query.
     */
    private void testINClause()
    {
        if (USE_CQL)
        {
            Map<String, Client> clientMap = (Map<String, Client>) entityManager.getDelegate();
            ThriftClient tc = (ThriftClient) clientMap.get(SEC_IDX_CASSANDRA_TEST);
            tc.setCqlVersion(CassandraConstants.CQL_VERSION_3_0);

            Query findQuery;
            List<PersonCassandra> allPersons;
            findQuery = entityManager.createQuery("Select p from PersonCassandra p where p.personId IN :idList");
            List<String> idList = new ArrayList<String>();
            idList.add("1");
            idList.add("2");
            idList.add("3");

            findQuery.setParameter("idList", idList);
            allPersons = findQuery.getResultList();
            Assert.assertNotNull(allPersons);
            Assert.assertEquals(3, allPersons.size());

            findQuery = entityManager.createQuery("Select p from PersonCassandra p where p.personId IN ?1");
            findQuery.setParameter(1, idList);
            allPersons = findQuery.getResultList();
            Assert.assertNotNull(allPersons);
            Assert.assertEquals(3, allPersons.size());

            entityManager.close();

            entityManager = emf.createEntityManager();

            findQuery = entityManager.createQuery("Select p from PersonCassandra p where p.personId IN :idList");
            findQuery.setParameter("idList", new ArrayList<String>());
            allPersons = findQuery.getResultList();
            Assert.assertNotNull(allPersons);
            Assert.assertTrue(allPersons.isEmpty());

            findQuery = entityManager.createQuery("Select p from PersonCassandra p where p.personId IN ('1', '2')");
            allPersons = findQuery.getResultList();
            Assert.assertNotNull(allPersons);
            Assert.assertEquals(2, allPersons.size());

            entityManager.close();

            try
            {
                entityManager = emf.createEntityManager();
                findQuery = entityManager.createQuery("Select p from PersonCassandra p where p.age IN (10 , 20)");
                allPersons = findQuery.getResultList();
                Assert.fail();
            }
            catch (Exception e)
            {
                Assert.assertEquals(
                        "javax.persistence.PersistenceException: com.impetus.kundera.KunderaException: InvalidRequestException(why:IN predicates on non-primary-key columns (AGE) is not yet supported)",
                        e.getMessage());
            }

            tc.setCqlVersion(CassandraConstants.CQL_VERSION_2_0);
        }
        else
        {
            Query findQuery;
            List<PersonCassandra> allPersons;
            findQuery = entityManager.createQuery("Select p from PersonCassandra p where p.personName IN :nameList");
            List<String> nameList = new ArrayList<String>();
            nameList.add("vivek");
            nameList.add("kk");

            findQuery.setParameter("nameList", nameList);
            try
            {
                allPersons = findQuery.getResultList();
                Assert.fail();
            }
            catch (Exception e)
            {
                Assert.assertEquals("IN clause is not enabled for thrift, use cql3.", e.getMessage());
            }

        }

    }

    /**
     * Query over rowkey.
     * 
     * @return the list
     */
    private List<PersonCassandra> queryOverRowkey()
    {
        String qry = "Select p.personId,p.personName from PersonCassandra p where p.personId = 1";
        Query q = entityManager.createQuery(qry);
        List<PersonCassandra> persons = q.getResultList();
        Assert.assertNotNull(persons);
        Assert.assertFalse(persons.isEmpty());
        Assert.assertEquals(1, persons.size());

        qry = "Select p.personId,p.personName from PersonCassandra p where p.personId > 1";
        q = entityManager.createQuery(qry);
        persons = q.getResultList();
        Assert.assertNotNull(persons);

        qry = "Select p.personId,p.personName from PersonCassandra p where p.personId < 2";
        q = entityManager.createQuery(qry);
        persons = q.getResultList();
        Assert.assertNotNull(persons);
        Assert.assertFalse(persons.isEmpty());

        qry = "Select p.personId,p.personName from PersonCassandra p where p.personId <= 2";
        q = entityManager.createQuery(qry);
        persons = q.getResultList();
        Assert.assertNotNull(persons);
        Assert.assertFalse(persons.isEmpty());

        qry = "Select p from PersonCassandra p where p.personId >= 1";
        q = entityManager.createQuery(qry);
        persons = q.getResultList();
        Assert.assertNotNull(persons);
        Assert.assertFalse(persons.isEmpty());

        return persons;
    }

    /**
     * Test criteria count result.
     */
    private void testCriteriaCountResult()
    {
        Map<String, Client> clientMap = (Map<String, Client>) entityManager.getDelegate();
        ThriftClient tc = (ThriftClient) clientMap.get(SEC_IDX_CASSANDRA_TEST);
        tc.setCqlVersion(CassandraConstants.CQL_VERSION_3_0);
        CQLTranslator translator = new CQLTranslator();

        CriteriaBuilder criteriaBuilder = entityManager.getCriteriaBuilder();

        CriteriaQuery<Object> personQuery = criteriaBuilder.createQuery(Object.class);
        Root<PersonCassandra> from = personQuery.from(PersonCassandra.class);
        personQuery.select(criteriaBuilder.count((Expression<?>) from.alias("p")));

        Query q = entityManager.createQuery(personQuery);
        List noOfRows = q.getResultList();

        if (USE_CQL)
        {
            Assert.assertEquals(new Long(4), ((Map) noOfRows.get(0)).get("count"));
        }
        else
        {
            Assert.assertEquals(new Long(3), ((Map) noOfRows.get(0)).get("count"));
        }

        entityManager.clear();
        q = entityManager.createNamedQuery("q");
        noOfRows = q.getResultList();
        if (USE_CQL)
        {
            Assert.assertEquals(4, noOfRows.size());
        }
        else
        {
            Assert.assertEquals(3, noOfRows.size());
        }
        tc.setCqlVersion(CassandraConstants.CQL_VERSION_2_0);
    }
    /**
     * Test count result.
     */
    private void testCountResult()
    {
        Map<String, Client> clientMap = (Map<String, Client>) entityManager.getDelegate();
        ThriftClient tc = (ThriftClient) clientMap.get(SEC_IDX_CASSANDRA_TEST);
        tc.setCqlVersion(CassandraConstants.CQL_VERSION_3_0);
        CQLTranslator translator = new CQLTranslator();

        String query = "select count(*) from "
                + translator.ensureCase(new StringBuilder(), "PERSONCASSANDRA", false).toString();
        Query q = entityManager.createNativeQuery(query, PersonCassandra.class);
        List noOfRows = q.getResultList();

        if (USE_CQL)
        {
            Assert.assertEquals(new Long(4), ((Map) noOfRows.get(0)).get("count"));
        }
        else
        {
            Assert.assertEquals(new Long(3), ((Map) noOfRows.get(0)).get("count"));
        }

        entityManager.clear();
        q = entityManager.createNamedQuery("q");
        noOfRows = q.getResultList();
        if (USE_CQL)
        {
            Assert.assertEquals(4, noOfRows.size());
        }
        else
        {
            Assert.assertEquals(3, noOfRows.size());
        }
        tc.setCqlVersion(CassandraConstants.CQL_VERSION_2_0);
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
        Object p1 = prepareData("1", 10);
        Object p2 = prepareData("2", 20);
        Object p3 = prepareData("3", 15);
        entityManager.persist(p1);
        entityManager.persist(p2);
        entityManager.persist(p3);

        entityManager.clear();
        col.put("1", p1);
        col.put("2", p2);
        col.put("3", p3);
        PersonCassandra p = findById(PersonCassandra.class, "1", entityManager);
        Assert.assertNotNull(p);
        Assert.assertEquals("vivek", p.getPersonName());
        Assert.assertEquals(Month.APRIL, p.getMonth());
        // modify record.
        p.setPersonName("newvivek");
        entityManager.merge(p);

        assertOnMerge(entityManager, "PersonCassandra", PersonCassandra.class, "vivek", "newvivek", "personName");
    }

    /**
     * On delete then insert cassandra.
     * 
     * @throws Exception
     *             the exception
     */
    @Test
    public void onDeleteThenInsertCassandra() throws Exception
    {
        Object p1 = prepareData("1", 10);
        Object p2 = prepareData("2", 20);
        Object p3 = prepareData("3", 15);
        entityManager.persist(p1);
        entityManager.persist(p2);
        entityManager.persist(p3);

        col.put("1", p1);
        col.put("2", p2);
        col.put("3", p3);
        PersonCassandra p = findById(PersonCassandra.class, "1", entityManager);
        Assert.assertNotNull(p);
        Assert.assertEquals("vivek", p.getPersonName());
        entityManager.remove(p);
        entityManager.clear();

        TypedQuery<PersonCassandra> query = entityManager.createQuery("Select p from PersonCassandra p",
                PersonCassandra.class);

        List<PersonCassandra> results = query.getResultList();
        Assert.assertNotNull(query);
        Assert.assertNotNull(results);
        Assert.assertEquals(2, results.size());
        Assert.assertEquals(Month.APRIL, results.get(0).getMonth());

        p1 = prepareData("1", 10);
        entityManager.persist(p1);

        query = entityManager.createQuery("Select p from PersonCassandra p", PersonCassandra.class);

        results = query.getResultList();
        Assert.assertNotNull(query);
        Assert.assertNotNull(results);
        Assert.assertEquals(3, results.size());
        Assert.assertEquals(Month.APRIL, results.get(0).getMonth());

    }

    /**
     * On refresh cassandra.
     * 
     * @throws Exception
     *             the exception
     */
    @Test
    public void onRefreshCassandra() throws Exception
    {
        CassandraCli.client.set_keyspace("KunderaExamples");
        Object p1 = prepareData("1", 10);
        Object p2 = prepareData("2", 20);
        Object p3 = prepareData("3", 15);
        entityManager.persist(p1);
        entityManager.persist(p2);
        entityManager.persist(p3);
        col.put("1", p1);
        col.put("2", p2);
        col.put("3", p3);

        // Check for contains
        Object pp1 = prepareData("1", 10);
        Object pp2 = prepareData("2", 20);
        Object pp3 = prepareData("3", 15);
        Assert.assertTrue(entityManager.contains(pp1));
        Assert.assertTrue(entityManager.contains(pp2));
        Assert.assertTrue(entityManager.contains(pp3));

        // Check for detach
        entityManager.detach(pp1);
        entityManager.detach(pp2);
        Assert.assertFalse(entityManager.contains(pp1));
        Assert.assertFalse(entityManager.contains(pp2));
        Assert.assertTrue(entityManager.contains(pp3));

        // Modify value in database directly, refresh and then check PC
        entityManager.clear();
        entityManager = emf.createEntityManager();
        Object o1 = entityManager.find(PersonCassandra.class, "1");

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
            columnFamilyValues.put("PERSONCASSANDRA", insertionList);
            Map<ByteBuffer, Map<String, List<Mutation>>> mulationMap = new HashMap<ByteBuffer, Map<String, List<Mutation>>>();
            mulationMap.put(ByteBuffer.wrap("1".getBytes()), columnFamilyValues);
            CassandraCli.client.batch_mutate(mulationMap, ConsistencyLevel.ONE);
        }
        else
        {
            String query = "insert into \"PERSONCASSANDRA\" (\"personId\",\"PERSON_NAME\",\"AGE\") values ('1','Amry',10 )";
            CassandraCli.client.execute_cql3_query(ByteBuffer.wrap(query.getBytes()), Compression.NONE,
                    ConsistencyLevel.ONE);
        }
        entityManager.refresh(o1);
        Object oo1 = entityManager.find(PersonCassandra.class, "1");
        Assert.assertTrue(entityManager.contains(o1));
        Assert.assertEquals("Amry", ((PersonCassandra) oo1).getPersonName());
    }

    /**
     * On typed create query.
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
    @Test
    public void onTypedQuery() throws TException, InvalidRequestException, UnavailableException, TimedOutException,
            SchemaDisagreementException
    {
        Object p1 = prepareData("1", 10);
        Object p2 = prepareData("2", 20);
        Object p3 = prepareData("3", 15);
        entityManager.persist(p1);
        entityManager.persist(p2);
        entityManager.persist(p3);
        TypedQuery<PersonCassandra> query = entityManager.createQuery("Select p from PersonCassandra p",
                PersonCassandra.class);

        List<PersonCassandra> results = query.getResultList();
        Assert.assertNotNull(query);
        Assert.assertNotNull(results);
        Assert.assertEquals(3, results.size());
        Assert.assertEquals(Month.APRIL, results.get(0).getMonth());
    }

    /**
     * On typed create query.
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
    @Test
    public void onGenericTypedQuery() throws TException, InvalidRequestException, UnavailableException,
            TimedOutException, SchemaDisagreementException
    {
        Object p1 = prepareData("1", 10);
        Object p2 = prepareData("2", 20);
        Object p3 = prepareData("3", 15);
        entityManager.persist(p1);
        entityManager.persist(p2);
        entityManager.persist(p3);
        TypedQuery<Object> query = entityManager.createQuery("Select p from PersonCassandra p", Object.class);

        List<Object> results = query.getResultList();
        Assert.assertNotNull(query);
        Assert.assertNotNull(results);
        Assert.assertEquals(3, results.size());
        Assert.assertEquals(PersonCassandra.class, results.get(0).getClass());
    }

    /**
     * on invalid typed query.
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
    @Test
    public void onInvalidTypedQuery() throws TException, InvalidRequestException, UnavailableException,
            TimedOutException, SchemaDisagreementException
    {
        Object p1 = prepareData("1", 10);
        Object p2 = prepareData("2", 20);
        Object p3 = prepareData("3", 15);
        entityManager.persist(p1);
        entityManager.persist(p2);
        entityManager.persist(p3);

        TypedQuery<Group> query = null;
        try
        {
            query = entityManager.createQuery("Select p from PersonCassandra p", Group.class);
            Assert.fail("Should have gone to catch block, as it is an invalid scenario!");
        }
        catch (IllegalArgumentException iaex)
        {
            Assert.assertNull(query);
        }
    }

    /**
     * On ghost rows.
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
    @Test
    public void onGhostRows() throws TException, InvalidRequestException, UnavailableException, TimedOutException,
            SchemaDisagreementException
    {
        Object p1 = prepareData("1", 10);
        Object p2 = prepareData("2", 20);
        Object p3 = prepareData("3", 15);
        entityManager.persist(p1);
        entityManager.persist(p2);
        entityManager.persist(p3);
        entityManager.clear();
        PersonCassandra person = entityManager.find(PersonCassandra.class, "1");
        entityManager.remove(person);
        entityManager.clear(); // just to make sure that not to be picked up
                               // from cache.
        TypedQuery<PersonCassandra> query = entityManager.createQuery("Select p from PersonCassandra p",
                PersonCassandra.class);

        List<PersonCassandra> results = query.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(2, results.size());

    }

    // @Test
    /**
     * Test with multiple thread.
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
    public void testWithMultipleThread() throws TException, InvalidRequestException, UnavailableException,
            TimedOutException, SchemaDisagreementException
    {
        ExecutorService executor = Executors.newFixedThreadPool(10);

        List<Future> futureList = new ArrayList<Future>();

        for (int i = 1; i <= 1000; i++)
        {
            HandlePersist persist = new HandlePersist(i);
            futureList.add(executor.submit(persist));
        }

        while (!futureList.isEmpty())
        {
            for (int i = 0; i < futureList.size(); i++)
            {
                if (futureList.get(i).isDone())
                {
                    futureList.remove(i);
                }
            }
        }

        String qry = "Select * from \"PERSONCASSANDRA\"";
        Query q = entityManager.createNativeQuery(qry, PersonCassandra.class);
        List<PersonCassandra> persons = q.getResultList();
        Assert.assertNotNull(persons);
        Assert.assertFalse(persons.isEmpty());
        Assert.assertEquals(1000, persons.size());
    }

    /**
     * The Class HandlePersist.
     */
    private class HandlePersist implements Runnable
    {

        /** The i. */
        private int i;

        /**
         * Instantiates a new handle persist.
         * 
         * @param i
         *            the i
         */
        public HandlePersist(int i)
        {
            this.i = i;
        }

        /*
         * (non-Javadoc)
         * 
         * @see java.lang.Runnable#run()
         */
        @Override
        public void run()
        {
            for (int j = 1; j <= 100; j++)
            {
                PersonCassandra foundObject = entityManager.find(PersonCassandra.class, "" + i * 1000);

                if (foundObject != null)
                {
                    Assert.assertNotNull(foundObject.getPersonId());
                    Assert.assertEquals(10 + j - 1, foundObject.getAge().intValue());
                    Assert.assertEquals("vivek" + (j - 1), foundObject.getPersonName());

                    foundObject.setAge(10 + j);
                    foundObject.setPersonName("vivek" + j);
                }
                else
                {
                    foundObject = prepareData("" + i * 1000, 10 + j);
                    foundObject.setPersonName("vivek" + j);
                }
                entityManager.persist(foundObject);
            }
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
    {
        entityManager.close();
        emf.close();
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
        user_Def.name = "PERSONCASSANDRA";
        user_Def.keyspace = "KunderaExamples";
        user_Def.setComparator_type("UTF8Type");
        user_Def.setDefault_validation_class("UTF8Type");
        user_Def.setKey_validation_class("UTF8Type");
        ColumnDef columnDef = new ColumnDef(ByteBuffer.wrap("PERSON_NAME".getBytes()), "UTF8Type");
        columnDef.index_type = IndexType.KEYS;
        user_Def.addToColumn_metadata(columnDef);
        ColumnDef columnDef1 = new ColumnDef(ByteBuffer.wrap("AGE".getBytes()), "Int32Type");
        columnDef1.index_type = IndexType.KEYS;
        user_Def.addToColumn_metadata(columnDef1);
        ColumnDef columnDef2 = new ColumnDef(ByteBuffer.wrap("ENUM".getBytes()), "UTF8Type");
        columnDef2.index_type = IndexType.KEYS;
        user_Def.addToColumn_metadata(columnDef2);
        ColumnDef columnDef3 = new ColumnDef(ByteBuffer.wrap("MONTH_ENUM".getBytes()), "UTF8Type");
        columnDef3.index_type = IndexType.KEYS;
        user_Def.addToColumn_metadata(columnDef3);

        List<CfDef> cfDefs = new ArrayList<CfDef>();
        cfDefs.add(user_Def);

        try
        {
            ksDef = CassandraCli.client.describe_keyspace("KunderaExamples");
            CassandraCli.client.set_keyspace("KunderaExamples");

            List<CfDef> cfDefn = ksDef.getCf_defs();

            for (CfDef cfDef1 : cfDefn)
            {

                if (cfDef1.getName().equalsIgnoreCase("PERSONCASSANDRA"))
                {

                    CassandraCli.client.system_drop_column_family("PERSONCASSANDRA");

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
