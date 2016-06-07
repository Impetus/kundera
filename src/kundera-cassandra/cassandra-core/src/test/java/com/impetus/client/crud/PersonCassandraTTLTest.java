/*******************************************************************************
 *  * Copyright 2013 Impetus Infotech.
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import junit.framework.Assert;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.CqlRow;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.cassandra.common.CassandraConstants;
import com.impetus.kundera.Constants;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.cassandra.persistence.CassandraCli;

/**
 * Test case for verifying Time to Live feature of Cassandra on single entity
 * 
 * @author amresh.singh
 */
public class PersonCassandraTTLTest extends BaseTest
{
    private static final String SEC_IDX_CASSANDRA_TEST = "genericCassandraTest";

    /** The emf. */
    private EntityManagerFactory emf;

    /** The em. */
    private EntityManager em;

    /** The col. */
    private Map<Object, Object> col;

    protected Map propertyMap = null;

    protected boolean AUTO_MANAGE_SCHEMA = true;

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
    }

    @After
    public void tearDown() throws Exception
    {
        if (em != null)
            em.close();
        if (emf != null)
            emf.close();
        CassandraCli.dropKeySpace("KunderaExamples");
    }

    /**
     * Tests whether TTL provided while inserting records are correctly getting
     * applied for CQL 2.0 Common TTL value for entire row is used
     * 
     * @throws Exception
     */

    @Test
    public void testTTLForEntireRowOnCQL2_0() throws Exception
    {
        if (propertyMap == null)
        {
            propertyMap = new HashMap();
            propertyMap.put(PersistenceProperties.KUNDERA_DDL_AUTO_PREPARE, "create");
        }
        emf = Persistence.createEntityManagerFactory(SEC_IDX_CASSANDRA_TEST, propertyMap);
        em = emf.createEntityManager();

        Map<String, Integer> ttlValues = new HashMap<String, Integer>();
        ttlValues.put("PERSONCASSANDRA", new Integer(5));
        em.setProperty("ttl.per.request", true);
        em.setProperty("ttl.values", ttlValues);

        Object p1 = prepareData("1", 10);
        em.persist(p1);
        em.clear();
        PersonCassandra p = findById(PersonCassandra.class, "1", em);

        SlicePredicate predicate = new SlicePredicate();
        predicate.setSlice_range(new SliceRange(ByteBufferUtil.EMPTY_BYTE_BUFFER, ByteBufferUtil.EMPTY_BYTE_BUFFER,
                true, 10000));
        ByteBuffer key = ByteBuffer.wrap("1".getBytes());

        CassandraCli.client.set_keyspace("KunderaExamples");
        List<ColumnOrSuperColumn> columnOrSuperColumns = CassandraCli.client.get_slice(key, new ColumnParent(
                "PERSONCASSANDRA"), predicate, ConsistencyLevel.ONE);

        boolean personNameFound = false;
        boolean ageFound = false;

        for (ColumnOrSuperColumn cosc : columnOrSuperColumns)
        {
            Column column = cosc.column;

            String columnName = new String(column.getName(), Constants.ENCODING);
            if (columnName.equals("PERSON_NAME"))
            {
                Assert.assertEquals(5, column.getTtl());
                personNameFound = true;
            }
            else if (columnName.equals("AGE"))
            {
                Assert.assertEquals(5, column.getTtl());
                ageFound = true;
            }
        }

        Assert.assertTrue(personNameFound && ageFound);
        Thread.sleep(5000);

        columnOrSuperColumns = CassandraCli.client.get_slice(key, new ColumnParent("PERSONCASSANDRA"), predicate,
                ConsistencyLevel.ONE);
        for (ColumnOrSuperColumn cosc : columnOrSuperColumns)
        {
            Column column = cosc.column;

            String columnName = new String(column.getName(), Constants.ENCODING);
            if (columnName.equals("PERSON_NAME"))
            {
                Assert.fail("PERSON_NAME column not deleted even though a TTL of 5 seconds was specified while writing to cassandra.");
            }
            else if (columnName.equals("AGE"))
            {
                Assert.fail("Age column not deleted even though a TTL of 5 seconds was specified while writing to cassandra.");
            }
        }

        String deleteQuery = "DELETE from PersonCassandra";
        Query q = em.createQuery(deleteQuery);
        Assert.assertEquals(0, q.executeUpdate());
    }

    /**
     * Tests whether TTL provided while inserting records are correctly getting
     * applied for CQL 2.0 Different TTL Values for Different columns are used
     * 
     * @throws Exception
     */

    @Test
    public void testDifferentTTLOnCQL2_0() throws Exception
    {
        if (propertyMap == null)
        {
            propertyMap = new HashMap();
            propertyMap.put(PersistenceProperties.KUNDERA_DDL_AUTO_PREPARE, "create");
        }
        emf = Persistence.createEntityManagerFactory(SEC_IDX_CASSANDRA_TEST, propertyMap);
        em = emf.createEntityManager();

        Map<String, Integer> ttlColumns = new HashMap<String, Integer>();
        ttlColumns.put("PERSON_NAME", 5000000);
        ttlColumns.put("AGE", 5);
        Map<String, Map<String, Integer>> ttlValues = new HashMap<String, Map<String, Integer>>();
        ttlValues.put("PERSONCASSANDRA", ttlColumns);
        em.setProperty("ttl.per.request", true);
        em.setProperty("ttl.values", ttlValues);

        Object p1 = prepareData("1", 10);
        em.persist(p1);
        em.clear();
        PersonCassandra p = findById(PersonCassandra.class, "1", em);

        SlicePredicate predicate = new SlicePredicate();
        predicate.setSlice_range(new SliceRange(ByteBufferUtil.EMPTY_BYTE_BUFFER, ByteBufferUtil.EMPTY_BYTE_BUFFER,
                true, 10000));
        ByteBuffer key = ByteBuffer.wrap("1".getBytes());

        CassandraCli.client.set_keyspace("KunderaExamples");
        List<ColumnOrSuperColumn> columnOrSuperColumns = CassandraCli.client.get_slice(key, new ColumnParent(
                "PERSONCASSANDRA"), predicate, ConsistencyLevel.ONE);

        boolean personNameFound = false;
        boolean ageFound = false;

        for (ColumnOrSuperColumn cosc : columnOrSuperColumns)
        {
            Column column = cosc.column;

            String columnName = new String(column.getName(), Constants.ENCODING);
            if (columnName.equals("PERSON_NAME"))
            {
                Assert.assertEquals(5000000, column.getTtl());
                personNameFound = true;
            }
            else if (columnName.equals("AGE"))
            {
                Assert.assertEquals(5, column.getTtl());
                ageFound = true;
            }
        }

        Assert.assertTrue(personNameFound && ageFound);
        Thread.sleep(5000);

        columnOrSuperColumns = CassandraCli.client.get_slice(key, new ColumnParent("PERSONCASSANDRA"), predicate,
                ConsistencyLevel.ONE);
        for (ColumnOrSuperColumn cosc : columnOrSuperColumns)
        {
            Column column = cosc.column;

            String columnName = new String(column.getName(), Constants.ENCODING);
            if (columnName.equals("PERSON_NAME"))
            {
                Assert.assertEquals(5000000, column.getTtl());
                personNameFound = true;
            }
            else if (columnName.equals("AGE"))
            {
                Assert.fail("Age column not deleted even though a TTL of 5 seconds was specified while writing to cassandra.");
            }
        }

        String deleteQuery = "DELETE from PersonCassandra";
        Query q = em.createQuery(deleteQuery);
        Assert.assertEquals(1, q.executeUpdate());
    }

    /**
     * Tests whether TTL provided while inserting records are correctly getting
     * applied using CQL 3 Common TTL value for entire row is used
     * 
     * @throws Exception
     */
    @Test
    public void testTTLonCQL3_0() throws Exception
    {
        propertyMap = new HashMap();
        propertyMap.put(PersistenceProperties.KUNDERA_DDL_AUTO_PREPARE, "create");
        propertyMap.put(CassandraConstants.CQL_VERSION, CassandraConstants.CQL_VERSION_3_0);
        emf = Persistence.createEntityManagerFactory(SEC_IDX_CASSANDRA_TEST, propertyMap);
        em = emf.createEntityManager();

        Map<String, Integer> ttlValues = new HashMap<String, Integer>();
        ttlValues.put("PERSONCASSANDRA", new Integer(5));
        em.setProperty("ttl.per.request", true);
        em.setProperty("ttl.values", ttlValues);

        Object p1 = prepareData("1", 10);
        em.persist(p1);
        em.clear();

        CassandraCli.client.set_keyspace("KunderaExamples");
        CqlResult cqlResult = CassandraCli.client.execute_cql3_query(ByteBuffer
                .wrap("Select ttl(\"PERSON_NAME\"), ttl(\"AGE\") from \"PERSONCASSANDRA\" where \"personId\" = '1'"
                        .getBytes()), Compression.NONE, ConsistencyLevel.ONE);

        List<CqlRow> cqlRows = cqlResult.getRows();
        CqlRow cqlRow = cqlRows.get(0);

        boolean personNameFound = false;
        boolean ageFound = false;

        for (Column column : cqlRow.getColumns())
        {
            String columnName = new String(column.getName(), Constants.ENCODING);
            if (columnName.equals("ttl(PERSON_NAME)"))
            {
                Assert.assertEquals(5, ByteBufferUtil.toInt(ByteBuffer.wrap(column.getValue())));
                personNameFound = true;
            }
            else if (columnName.equals("ttl(AGE)"))
            {
                Assert.assertEquals(5, ByteBufferUtil.toInt(ByteBuffer.wrap(column.getValue())));
                ageFound = true;
            }
        }

        Assert.assertTrue(personNameFound && ageFound);
        Thread.sleep(5000);

        cqlResult = CassandraCli.client.execute_cql3_query(ByteBuffer
                .wrap("Select ttl(\"PERSON_NAME\"), ttl(\"AGE\") from \"PERSONCASSANDRA\" where \"personId\" = '1'"
                        .getBytes()), Compression.NONE, ConsistencyLevel.ONE);

        cqlRows = cqlResult.getRows();

        try
        {
            cqlRow = cqlRows.get(0);
            Assert.fail("PERSON_NAME and AGE column not deleted even though a TTL of 5 seconds was specified while writing to cassandra.");
        }
        catch (IndexOutOfBoundsException ioobe)
        {
            Assert.assertTrue(cqlRows.isEmpty());
        }
        // checking for update query.

        Object p2 = prepareData("2", 10);
        em.persist(p2);
        em.clear();

        ttlValues = new HashMap<String, Integer>();
        ttlValues.put("PERSONCASSANDRA", new Integer(10));
        em.setProperty("ttl.per.request", true);
        em.setProperty("ttl.values", ttlValues);

        Query q = em.createQuery("update PersonCassandra p set p.personName='''KK MISHRA''' where p.personId=2");
        q.executeUpdate();

        cqlResult = CassandraCli.client.execute_cql3_query(ByteBuffer
                .wrap("Select ttl(\"PERSON_NAME\"), ttl(\"AGE\") from \"PERSONCASSANDRA\" where \"personId\" = '2'"
                        .getBytes()), Compression.NONE, ConsistencyLevel.ONE);

        cqlRows = cqlResult.getRows();
        cqlRow = cqlRows.get(0);

        personNameFound = false;
        ageFound = false;

        for (Column column : cqlRow.getColumns())
        {
            String columnName = new String(column.getName(), Constants.ENCODING);
            if (columnName.equals("ttl(PERSON_NAME)"))
            {
                Assert.assertEquals(10, ByteBufferUtil.toInt(ByteBuffer.wrap(column.getValue())));
                personNameFound = true;
            }
            else if (columnName.equals("ttl(AGE)"))
            {
                Assert.assertEquals(null, column.getValue());
                ageFound = true;
            }            
        }

        Assert.assertTrue(personNameFound && ageFound);
        Thread.sleep(10000);

        cqlResult = CassandraCli.client.execute_cql3_query(ByteBuffer
                .wrap("Select ttl(\"PERSON_NAME\"), ttl(\"AGE\") from \"PERSONCASSANDRA\" where \"personId\" = '2'"
                        .getBytes()), Compression.NONE, ConsistencyLevel.ONE);

        cqlRows = cqlResult.getRows();
        cqlRow = cqlRows.get(0);
        for (Column column : cqlRow.getColumns())
        {
            String columnName = new String(column.getName(), Constants.ENCODING);
            if (columnName.equals("ttl(PERSON_NAME)"))
            {
                Assert.assertEquals(null, null);
                personNameFound = true;
            }
            else if (columnName.equals("ttl(AGE)"))
            {
                Assert.assertEquals(null, column.getValue());
                ageFound = true;
            }
            if (columnName.equals("PERSON_NAME"))
            {
                Assert.fail("PERSON_NAME column not deleted even though a TTL of 10 seconds was specified while writing to cassandra.");
            }
        }

        // TTL per session.

        ttlValues = new HashMap<String, Integer>();
        ttlValues.put("PERSONCASSANDRA", new Integer(10));
        em.setProperty("ttl.per.session", true);
        em.setProperty("ttl.values", ttlValues);

        Object p3 = prepareData("3", 10);
        em.persist(p3);
        em.clear();

        cqlResult = CassandraCli.client.execute_cql3_query(ByteBuffer
                .wrap("Select ttl(\"PERSON_NAME\"), ttl(\"AGE\") from \"PERSONCASSANDRA\" where \"personId\" = '3'"
                        .getBytes()), Compression.NONE, ConsistencyLevel.ONE);

        cqlRows = cqlResult.getRows();
        cqlRow = cqlRows.get(0);

        personNameFound = false;
        ageFound = false;

        for (Column column : cqlRow.getColumns())
        {
            String columnName = new String(column.getName(), Constants.ENCODING);
            if (columnName.equals("ttl(PERSON_NAME)"))
            {
                Assert.assertEquals(10, ByteBufferUtil.toInt(ByteBuffer.wrap(column.getValue())));
                personNameFound = true;
            }
            else if (columnName.equals("ttl(AGE)"))
            {
                Assert.assertEquals(10, ByteBufferUtil.toInt(ByteBuffer.wrap(column.getValue())));
                ageFound = true;
            }
        }

        Assert.assertTrue(personNameFound && ageFound);
        Thread.sleep(10000);

        cqlResult = CassandraCli.client.execute_cql3_query(ByteBuffer
                .wrap("Select ttl(\"PERSON_NAME\"), ttl(\"AGE\") from \"PERSONCASSANDRA\" where \"personId\" = '3'"
                        .getBytes()), Compression.NONE, ConsistencyLevel.ONE);

        cqlRows = cqlResult.getRows();
        try
        {
            cqlRow = cqlRows.get(0);
            Assert.fail("PERSON_NAME and AGE column not deleted even though a TTL of 5 seconds was specified while writing to cassandra.");
        }
        catch (IndexOutOfBoundsException ioobe)
        {
            Assert.assertTrue(cqlRows.isEmpty());
        }

        Object p4 = prepareData("4", 10);
        ttlValues = new HashMap<String, Integer>();
        ttlValues.put("PERSONCASSANDRA", new Integer(10));
        em.setProperty("ttl.per.session", true);
        em.setProperty("ttl.values", ttlValues);
        em.persist(p4);
        em.clear();

        cqlResult = CassandraCli.client.execute_cql3_query(ByteBuffer
                .wrap("Select ttl(\"PERSON_NAME\"), ttl(\"AGE\") from \"PERSONCASSANDRA\" where \"personId\" = '4'"
                        .getBytes()), Compression.NONE, ConsistencyLevel.ONE);

        cqlRows = cqlResult.getRows();
        cqlRow = cqlRows.get(0);
        personNameFound = false;
        ageFound = false;

        for (Column column : cqlRow.getColumns())
        {
            String columnName = new String(column.getName(), Constants.ENCODING);
            if (columnName.equals("ttl(PERSON_NAME)"))
            {
                Assert.assertEquals(10, ByteBufferUtil.toInt(ByteBuffer.wrap(column.getValue())));
                personNameFound = true;
            }
            else if (columnName.equals("ttl(AGE)"))
            {
                Assert.assertEquals(10, ByteBufferUtil.toInt(ByteBuffer.wrap(column.getValue())));
                ageFound = true;
            }
        }

        Assert.assertTrue(personNameFound && ageFound);
        Thread.sleep(10000);

        cqlResult = CassandraCli.client.execute_cql3_query(ByteBuffer
                .wrap("Select ttl(\"PERSON_NAME\"), ttl(\"AGE\") from \"PERSONCASSANDRA\" where \"personId\" = '4'"
                        .getBytes()), Compression.NONE, ConsistencyLevel.ONE);

        cqlRows = cqlResult.getRows();
        try
        {
            cqlRow = cqlRows.get(0);
            Assert.fail("PERSON_NAME and AGE column not deleted even though a TTL of 5 seconds was specified while writing to cassandra.");
        }
        catch (IndexOutOfBoundsException ioobe)
        {
            Assert.assertTrue(cqlRows.isEmpty());
        }

        String deleteQuery = "DELETE from PersonCassandra";
        q = em.createQuery(deleteQuery);
        Assert.assertEquals(1, q.executeUpdate());
    }
}