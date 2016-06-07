/**
 * 
 */
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

import com.impetus.client.cassandra.common.CassandraConstants;
import com.impetus.kundera.client.cassandra.persistence.CassandraCli;

/**
 * @author Kuldeep Mishra
 * 
 */
public class CassandraIdQueryTest extends BaseTest
{
    /** The emf. */
    private EntityManagerFactory emf;

    /** The em. */
    private EntityManager em;

    private Map<Object, Object> col;

    private CassandraCli cli;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        CassandraCli.cassandraSetUp();
        CassandraCli.createKeySpace("KunderaExamples");
        Map propertyMap = new HashMap();
        propertyMap.put(CassandraConstants.CQL_VERSION, CassandraConstants.CQL_VERSION_2_0);
        emf = Persistence.createEntityManagerFactory("genericCassandraTest", propertyMap);
        loadData();
        em = emf.createEntityManager();
        col = new java.util.HashMap<Object, Object>();
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
        for (Object val : col.values())
        {
            em.remove(val);
        }
        em.close();
        emf.close();
        CassandraCli.dropKeySpace("KunderaExamples");
    }

    @Test
    public void test() throws TException, InvalidRequestException, UnavailableException, TimedOutException,
            SchemaDisagreementException
    {
        init();
        em.clear();
        findById();
        findByWithOutWhereClause();
        findByIdEQ();
        findByIdLT();
        findByIdLTE();
        findByIdGT();
        findByIdGTE();
        findByIdGTEAndLT();
        findByIdGTAndLTE();
//        findByIdAndAgeGTAndLT();
//        findByIdGTAndAgeGTAndLT();
        findByIdAndAge();
        findByIdAndAgeGT();
        findByIdGTEAndAge();
        findByIdLTEAndAge();
    }

    /**
     * 
     */
    private void findByWithOutWhereClause()
    {
        String qry = "Select p.personName from PersonCassandra p";
        Query q = em.createQuery(qry);
        List<PersonCassandra> persons = q.getResultList();
        Assert.assertNotNull(persons);
        Assert.assertEquals(3, persons.size());
        int count = 0;
        for (PersonCassandra person : persons)
        {
            if (person.getPersonId().equals("2"))
            {
                Assert.assertNull(person.getAge());
                Assert.assertEquals("vivek", person.getPersonName());
                count++;
            }
            else if (person.getPersonId().equals("3"))
            {
                Assert.assertNull(person.getAge());
                Assert.assertEquals("vivek", person.getPersonName());
                count++;
            }
            else
            {
                Assert.assertNull(person.getAge());
                Assert.assertEquals("1", person.getPersonId());
                Assert.assertEquals("vivek", person.getPersonName());
                count++;
            }
        }
        Assert.assertEquals(3, count);

    }

    /**
     * 
     */
    private void findByIdEQ()
    {
        String qry = "Select p.personName from PersonCassandra p where p.personId = 2";
        Query q = em.createQuery(qry);
        List<PersonCassandra> persons = q.getResultList();
        Assert.assertNotNull(persons);
        Assert.assertEquals(1, persons.size());
        for (PersonCassandra person : persons)
        {
            Assert.assertNull(person.getAge());
            Assert.assertEquals("2", person.getPersonId());
            Assert.assertEquals("vivek", person.getPersonName());
        }
    }

    /**
     * 
     */
    private void findByIdLT()
    {
        String qry = "Select p.personName from PersonCassandra p where p.personId < 3";
        Query q = em.createQuery(qry);
        List<PersonCassandra> persons = q.getResultList();
        Assert.assertNotNull(persons);
        Assert.assertEquals(3, persons.size());
        int count = 0;
        for (PersonCassandra person : persons)
        {
            if (person.getPersonId().equals("2"))
            {
                Assert.assertNull(person.getAge());
                Assert.assertEquals("vivek", person.getPersonName());
                count++;
            }
            else if (person.getPersonId().equals("3"))
            {
                Assert.assertNull(person.getAge());
                Assert.assertEquals("vivek", person.getPersonName());
                count++;
            }
            else
            {
                Assert.assertNull(person.getAge());
                Assert.assertEquals("1", person.getPersonId());
                Assert.assertEquals("vivek", person.getPersonName());
                count++;
            }
        }
        Assert.assertEquals(3, count);
    }

    /**
     * 
     */
    private void findByIdLTE()
    {
        String qry = "Select p.personName, p.age from PersonCassandra p where p.personId <= 3";
        Query q = em.createQuery(qry);
        List<PersonCassandra> persons = q.getResultList();
        Assert.assertNotNull(persons);
        Assert.assertEquals(3, persons.size());
        int count = 0;
        for (PersonCassandra person : persons)
        {
            if (person.getPersonId().equals("2"))
            {
                Assert.assertEquals(new Integer("20"), person.getAge());
                Assert.assertEquals("vivek", person.getPersonName());
                count++;
            }
            else if (person.getPersonId().equals("3"))
            {
                Assert.assertEquals(new Integer("15"), person.getAge());
                Assert.assertEquals("vivek", person.getPersonName());
                count++;
            }
            else
            {
                Assert.assertEquals(new Integer("10"), person.getAge());
                Assert.assertEquals("1", person.getPersonId());
                Assert.assertEquals("vivek", person.getPersonName());
                count++;
            }
        }
        Assert.assertEquals(3, count);
    }

    /**
     * 
     */
    private void findByIdGT()
    {
        String qry = "Select p.personName from PersonCassandra p where p.personId > 1";
        Query q = em.createQuery(qry);
        List<PersonCassandra> persons = q.getResultList();
        Assert.assertNotNull(persons);
        Assert.assertEquals(3, persons.size());
        int count = 0;
        for (PersonCassandra person : persons)
        {
            if (person.getPersonId().equals("2"))
            {
                Assert.assertNull(person.getAge());
                Assert.assertEquals("vivek", person.getPersonName());
                count++;
            }
            else if (person.getPersonId().equals("3"))
            {
                Assert.assertNull(person.getAge());
                Assert.assertEquals("vivek", person.getPersonName());
                count++;
            }
            else
            {
                Assert.assertNull(person.getAge());
                Assert.assertEquals("1", person.getPersonId());
                Assert.assertEquals("vivek", person.getPersonName());
                count++;
            }
        }
        Assert.assertEquals(3, count);
    }

    /**
     * 
     */
    private void findByIdGTE()
    {
        String qry = "Select p.personName from PersonCassandra p where p.personId >= 1 ";
        Query q = em.createQuery(qry);
        List<PersonCassandra> persons = q.getResultList();
        Assert.assertNotNull(persons);
        Assert.assertEquals(3, persons.size());
        int count = 0;
        for (PersonCassandra person : persons)
        {
            if (person.getPersonId().equals("2"))
            {
                Assert.assertNull(person.getAge());
                Assert.assertEquals("vivek", person.getPersonName());
                count++;
            }
            else if (person.getPersonId().equals("3"))
            {
                Assert.assertNull(person.getAge());
                Assert.assertEquals("vivek", person.getPersonName());
                count++;
            }
            else
            {
                Assert.assertNull(person.getAge());
                Assert.assertEquals("1", person.getPersonId());
                Assert.assertEquals("vivek", person.getPersonName());
                count++;
            }
        }
        Assert.assertEquals(3, count);
    }

    /**
     * 
     */
    private void findByIdGTEAndLT()
    {
        String qry = "Select p.personName from PersonCassandra p where p.personId >= 1 and p.personId < 3";
        Query q = em.createQuery(qry);
        List<PersonCassandra> persons = q.getResultList();
        Assert.assertNotNull(persons);
        Assert.assertEquals(3, persons.size());
        int count = 0;
        for (PersonCassandra person : persons)
        {
            if (person.getPersonId().equals("2"))
            {
                Assert.assertNull(person.getAge());
                Assert.assertEquals("vivek", person.getPersonName());
                count++;
            }
            else if (person.getPersonId().equals("3"))
            {
                Assert.assertNull(person.getAge());
                Assert.assertEquals("vivek", person.getPersonName());
                count++;
            }
            else
            {
                Assert.assertNull(person.getAge());
                Assert.assertEquals("1", person.getPersonId());
                Assert.assertEquals("vivek", person.getPersonName());
                count++;
            }
        }
        Assert.assertEquals(3, count);
    }

    /**
     * 
     */
    private void findByIdGTAndLTE()
    {
        String qry = "Select p.personName from PersonCassandra p where p.personId > 1 and p.personId <= 2";
        Query q = em.createQuery(qry);
        List<PersonCassandra> persons = q.getResultList();
        Assert.assertNotNull(persons);
        Assert.assertEquals(2, persons.size());
        int count = 0;
        for (PersonCassandra person : persons)
        {
            if (person.getPersonId().equals("2"))
            {
                Assert.assertNull(person.getAge());
                Assert.assertEquals("vivek", person.getPersonName());
                count++;
            }
            else
            {
                Assert.assertEquals("1", person.getPersonId());
                Assert.assertNull(person.getAge());
                Assert.assertEquals("vivek", person.getPersonName());
                count++;
            }
        }
        Assert.assertEquals(2, count);
    }

    /**
     * 
     */
    private void findByIdGTAndAgeGTAndLT()
    {

        String qry = "Select p.personName from PersonCassandra p where p.personId > 1 and p.personName = vivek and p.age >=10 and p.age <= 20";
        Query q = em.createQuery(qry);
        List<PersonCassandra> persons = q.getResultList();
        Assert.assertNotNull(persons);
        Assert.assertEquals(3, persons.size());
        int count = 0;
        for (PersonCassandra person : persons)
        {
            if (person.getPersonId().equals("2"))
            {
                Assert.assertNull(person.getAge());
                Assert.assertEquals("vivek", person.getPersonName());
                count++;
            }
            else if (person.getPersonId().equals("3"))
            {
                Assert.assertNull(person.getAge());
                Assert.assertEquals("vivek", person.getPersonName());
                count++;
            }
            else
            {
                Assert.assertNull(person.getAge());
                Assert.assertEquals("1", person.getPersonId());
                Assert.assertEquals("vivek", person.getPersonName());
                count++;
            }
        }
        Assert.assertEquals(3, count);
    }

    /**
     * 
     */
    private void findById()
    {
        PersonCassandra personHBase = findById(PersonCassandra.class, "1", em);
        Assert.assertNotNull(personHBase);
        Assert.assertEquals("vivek", personHBase.getPersonName());
        Assert.assertEquals(new Integer(10), personHBase.getAge());

        personHBase = findById(PersonCassandra.class, "2", em);
        Assert.assertNotNull(personHBase);
        Assert.assertEquals("vivek", personHBase.getPersonName());
        Assert.assertEquals(new Integer(20), personHBase.getAge());

        personHBase = findById(PersonCassandra.class, "3", em);
        Assert.assertNotNull(personHBase);
        Assert.assertEquals("vivek", personHBase.getPersonName());
        Assert.assertEquals(new Integer(15), personHBase.getAge());
    }

    /**
     * 
     */
    private void findByIdGTEAndAge()
    {
        String qry = "Select p.personName, p.age from PersonCassandra p where p.personId >= 1 and p.age = 10";
        Query q = em.createQuery(qry);
        List<PersonCassandra> persons = q.getResultList();
        Assert.assertNotNull(persons);
        Assert.assertEquals(1, persons.size());
        for (PersonCassandra person : persons)
        {
            Assert.assertEquals(new Integer(10), person.getAge());
            Assert.assertEquals("1", person.getPersonId());
            Assert.assertEquals("vivek", person.getPersonName());
        }
    }

    /**
     * 
     */
    private void findByIdAndAge()
    {
        String qry = "Select p.personName, p.age from PersonCassandra p where p.personId = 1 and p.age = 10";
        Query q = em.createQuery(qry);
        List<PersonCassandra> persons = q.getResultList();
        Assert.assertNotNull(persons);
        Assert.assertEquals(1, persons.size());
        for (PersonCassandra person : persons)
        {
            Assert.assertEquals(new Integer(10), person.getAge());
            Assert.assertEquals("1", person.getPersonId());
            Assert.assertEquals("vivek", person.getPersonName());
            Assert.assertEquals(10, person.getAge().intValue());
            Assert.assertNull(person.getA());
        }
    }

    /**
     * 
     */
    private void findByIdAndAgeGT()
    {
        String qry = "Select p.personName, p.age from PersonCassandra p where p.personId = 1 and p.age > 5";
        Query q = em.createQuery(qry);
        List<PersonCassandra> persons = q.getResultList();
        Assert.assertNotNull(persons);
        Assert.assertEquals(1, persons.size());
        for (PersonCassandra person : persons)
        {
            Assert.assertEquals(new Integer(10), person.getAge());
            Assert.assertEquals("1", person.getPersonId());
            Assert.assertEquals("vivek", person.getPersonName());
            Assert.assertEquals(10, person.getAge().intValue());
            Assert.assertNull(person.getA());
        }
    }

    /**
     * 
     */
    private void findByIdAndAgeGTAndLT()
    {
        String qry = "Select p.personName from PersonCassandra p where p.personId = 1 and p.personName = vivek and p.age >=10 and p.age <= 20";
        Query q = em.createQuery(qry);
        List<PersonCassandra> persons = q.getResultList();
        Assert.assertNotNull(persons);
        Assert.assertEquals(1, persons.size());
        int count = 0;
        for (PersonCassandra person : persons)
        {
            if (person.getPersonId().equals("1"))
            {
                Assert.assertNull(person.getAge());
                Assert.assertEquals("vivek", person.getPersonName());
                count++;
            }
        }
        Assert.assertEquals(1, count);
    }

    /**
     * 
     */
    private void findByIdLTEAndAge()
    {
        String qry = "Select p.personName, p.age from PersonCassandra p where p.personId <= 3 and p.age = 10";
        Query q = em.createQuery(qry);
        List<PersonCassandra> persons = q.getResultList();
        Assert.assertNotNull(persons);
        Assert.assertEquals(1, persons.size());
        for (PersonCassandra person : persons)
        {
            Assert.assertEquals(new Integer(10), person.getAge());
            Assert.assertEquals("1", person.getPersonId());
            Assert.assertEquals("vivek", person.getPersonName());
        }

    }

    private void init() throws TException, InvalidRequestException, UnavailableException, TimedOutException,
            SchemaDisagreementException
    {
        Object p1 = prepareData("1", 10);
        Object p2 = prepareData("2", 20);
        Object p3 = prepareData("3", 15);
        em.persist(p1);
        em.persist(p2);
        em.persist(p3);
        col.put("1", p1);
        col.put("2", p2);
        col.put("3", p3);
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
        ColumnDef columnDef1 = new ColumnDef(ByteBuffer.wrap("AGE".getBytes()), "IntegerType");
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
