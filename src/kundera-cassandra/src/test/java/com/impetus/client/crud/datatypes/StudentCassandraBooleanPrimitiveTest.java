package com.impetus.client.crud.datatypes;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import javax.persistence.EntityManager;
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

import com.impetus.client.crud.datatypes.entities.StudentBooleanPrimitive;
import com.impetus.client.persistence.CassandraCli;
import com.impetus.kundera.query.QueryHandlerException;

public class StudentCassandraBooleanPrimitiveTest extends CassandraBase
{

    private static final String keyspace = "KunderaCassandraDataType";

    @Before
    public void setUp() throws Exception
    {
        super.setUp();
    }

    @After
    public void tearDown() throws Exception
    {
        super.tearDown();
    }

    @Test
    public void testExecuteUseSameEm()
    {
        testPersist(true);
        testFindById(true);
        testMerge(true);
        testFindByQuery(true);
        testNamedQueryUseSameEm(true);
        testDelete(true);
    }

    @Test
    public void testExecute()
    {
        testPersist(false);
        testFindById(false);
        testMerge(false);
        testFindByQuery(false);
        testNamedQuery(false);
        testDelete(false);
    }

    public void testPersist(boolean useSameEm)
    {
        EntityManager em = emf.createEntityManager();

        // Insert max value of boolean
        StudentBooleanPrimitive studentMax = new StudentBooleanPrimitive();
        studentMax.setAge((Short) getMaxValue(short.class));
        studentMax.setId((Boolean) getMaxValue(boolean.class));
        studentMax.setName((String) getMaxValue(String.class));
        em.persist(studentMax);

        // Insert min value of boolean
        StudentBooleanPrimitive studentMin = new StudentBooleanPrimitive();
        studentMin.setAge((Short) getMinValue(short.class));
        studentMin.setId((Boolean) getMinValue(boolean.class));
        studentMin.setName((String) getMinValue(String.class));
        em.persist(studentMin);

        em.close();
    }

    public void testFindById(boolean useSameEm)
    {
        EntityManager em = emf.createEntityManager();

        StudentBooleanPrimitive studentMax = em.find(StudentBooleanPrimitive.class, getMaxValue(boolean.class));
        Assert.assertNotNull(studentMax);
        Assert.assertEquals(getMaxValue(short.class), studentMax.getAge());
        Assert.assertEquals(getMaxValue(String.class), studentMax.getName());

        if (!useSameEm)
        {
            em.close();
            em = emf.createEntityManager();
        }
        StudentBooleanPrimitive studentMin = em.find(StudentBooleanPrimitive.class, getMinValue(boolean.class));
        Assert.assertNotNull(studentMin);
        Assert.assertEquals(getMinValue(short.class), studentMin.getAge());
        Assert.assertEquals(getMinValue(String.class), studentMin.getName());

        em.close();
    }

    public void testMerge(boolean useSameEm)
    {
        EntityManager em = emf.createEntityManager();
        StudentBooleanPrimitive student = em.find(StudentBooleanPrimitive.class, getMaxValue(boolean.class));
        Assert.assertNotNull(student);
        Assert.assertEquals(getMaxValue(short.class), student.getAge());
        Assert.assertEquals(getMaxValue(String.class), student.getName());

        student.setName("Kuldeep");
        em.merge(student);
        if (!useSameEm)
        {
            em.close();
            em = emf.createEntityManager();
        }
        StudentBooleanPrimitive newStudent = em.find(StudentBooleanPrimitive.class, getMaxValue(boolean.class));
        Assert.assertNotNull(newStudent);
        Assert.assertEquals(getMaxValue(short.class), newStudent.getAge());
        Assert.assertEquals("Kuldeep", newStudent.getName());
    }

    public void testFindByQuery(boolean useSameEm)
    {
        findAllQuery();
        findByName();
        findByAge();
        findByNameAndAgeGTAndLT();
        findByNameAndAgeGTEQAndLTEQ();
        findByNameAndAgeGTAndLTEQ();
        findByNameAndAgeWithOrClause();
        findByAgeAndNameGTAndLT();
        findByNameAndAGEBetween();
        findByRange();
    }

    private void findByAgeAndNameGTAndLT()
    {
        EntityManager em;
        String query;
        Query q;
        List<StudentBooleanPrimitive> students;
        int count;
        em = emf.createEntityManager();
        query = "Select s From StudentBooleanPrimitive s where s.age = " + getMinValue(short.class)
                + " and s.name > Amresh and s.name <= " + getMaxValue(String.class);
        q = em.createQuery(query);
        students = q.getResultList();
        Assert.assertNotNull(students);
        Assert.assertEquals(1, students.size());
        count = 0;
        for (StudentBooleanPrimitive student : students)
        {
            Assert.assertEquals(getMinValue(boolean.class), student.getId());
            Assert.assertEquals(getMinValue(short.class), student.getAge());
            Assert.assertEquals(getMinValue(String.class), student.getName());
            count++;

        }
        Assert.assertEquals(1, count);
        em.close();

    }

    private void findByRange()
    {
        EntityManager em;
        String query;
        Query q;
        List<StudentBooleanPrimitive> students;
        em = emf.createEntityManager();
        query = "Select s From StudentBooleanPrimitive s where s.id between ?1 and ?2";
        q = em.createQuery(query);
        q.setParameter(1, getMinValue(boolean.class));
        q.setParameter(2, getMaxValue(boolean.class));
        students = q.getResultList();
        Assert.assertNotNull(students);
        Assert.assertEquals(2, students.size());
        int count = 0;
        for (StudentBooleanPrimitive student : students)
        {
            if (student.getId() == ((Boolean) getMaxValue(boolean.class)).booleanValue())
            {
                Assert.assertEquals(getMaxValue(short.class), student.getAge());
                Assert.assertEquals("Kuldeep", student.getName());
                count++;
            }
            else if (student.getId() == ((Boolean) getMinValue(boolean.class)).booleanValue())
            {
                Assert.assertEquals(getMinValue(short.class), student.getAge());
                Assert.assertEquals(getMinValue(String.class), student.getName());
                count++;
            }
        }
        Assert.assertEquals(2, count);
        em.close();
    }

    private void findByNameAndAgeWithOrClause()
    {
        EntityManager em;
        String query;
        Query q;
        List<StudentBooleanPrimitive> students;
        int count;
        em = emf.createEntityManager();
        query = "Select s From StudentBooleanPrimitive s where s.name = Kuldeep or s.age > " + getMinValue(short.class);
        try
        {
            q = em.createQuery(query);
            students = q.getResultList();
            Assert.assertNotNull(students);
            Assert.assertEquals(1, students.size());
            count = 0;
            for (StudentBooleanPrimitive student : students)
            {
                Assert.assertEquals(getMaxValue(boolean.class), student.getId());
                Assert.assertEquals(getMaxValue(short.class), student.getAge());
                Assert.assertEquals("Kuldeep", student.getName());
                count++;
            }
            Assert.assertEquals(1, count);
            em.close();
        }
        catch (QueryHandlerException qhe)
        {
            Assert.assertEquals("unsupported clause OR for cassandra", qhe.getMessage());
        }
    }

    private void findByNameAndAgeGTAndLTEQ()
    {

        EntityManager em;
        String query;
        Query q;
        List<StudentBooleanPrimitive> students;
        int count;
        em = emf.createEntityManager();
        query = "Select s From StudentBooleanPrimitive s where s.name = Kuldeep and s.age > "
                + getMinValue(short.class) + " and s.age <= " + getMaxValue(short.class);
        q = em.createQuery(query);
        students = q.getResultList();
        Assert.assertNotNull(students);
        Assert.assertEquals(1, students.size());
        count = 0;
        for (StudentBooleanPrimitive student : students)
        {
            Assert.assertEquals(getMaxValue(boolean.class), student.getId());
            Assert.assertEquals(getMaxValue(short.class), student.getAge());
            Assert.assertEquals("Kuldeep", student.getName());
            count++;
        }
        Assert.assertEquals(1, count);
        em.close();
    }

    public void testNamedQueryUseSameEm(boolean useSameEm)
    {
        updateNamed(true);
        deleteNamed(true);
    }

    public void testNamedQuery(boolean useSameEm)
    {
        updateNamed(false);
        deleteNamed(false);
    }

    public void testDelete(boolean useSameEm)
    {
        EntityManager em = emf.createEntityManager();

        StudentBooleanPrimitive studentMax = em.find(StudentBooleanPrimitive.class, getMinValue(boolean.class));
        Assert.assertNotNull(studentMax);
        Assert.assertEquals(getMinValue(short.class), studentMax.getAge());
        Assert.assertEquals("Kuldeep", studentMax.getName());
        em.remove(studentMax);
        if (!useSameEm)
        {
            em.close();
            em = emf.createEntityManager();
        }
        studentMax = em.find(StudentBooleanPrimitive.class, getMinValue(boolean.class));
        Assert.assertNull(studentMax);
        em.close();
    }

    /**
     * 
     */
    private void deleteNamed(boolean useSameEm)
    {

        String deleteQuery = "Delete From StudentBooleanPrimitive s where s.name=Vivek";
        EntityManager em = emf.createEntityManager();
        Query q = em.createQuery(deleteQuery);
        q.executeUpdate();
        if (!useSameEm)
        {
            em.close();
            em = emf.createEntityManager();
        }
        StudentBooleanPrimitive newStudent = em.find(StudentBooleanPrimitive.class, getRandomValue(boolean.class));
        Assert.assertNull(newStudent);
        em.close();
    }

    /**
     * @return
     */
    private void updateNamed(boolean useSameEm)
    {
        EntityManager em = emf.createEntityManager();
        String updateQuery = "Update StudentBooleanPrimitive s SET s.name=Vivek where s.id=true";
        Query q = em.createQuery(updateQuery);
        q.executeUpdate();
        if (!useSameEm)
        {
            em.close();
            em = emf.createEntityManager();
        }
        StudentBooleanPrimitive newStudent = em.find(StudentBooleanPrimitive.class, getMaxValue(boolean.class));
        Assert.assertNotNull(newStudent);
        Assert.assertEquals(getMaxValue(short.class), newStudent.getAge());
        Assert.assertEquals("Vivek", newStudent.getName());
        em.close();
    }

    private void findByNameAndAGEBetween()
    {
        EntityManager em;
        String query;
        Query q;
        List<StudentBooleanPrimitive> students;
        em = emf.createEntityManager();
        query = "Select s From StudentBooleanPrimitive s where s.name = Kuldeep and s.age between "
                + getMinValue(short.class) + " and " + getMaxValue(short.class);
        q = em.createQuery(query);
        students = q.getResultList();
        Assert.assertNotNull(students);
        Assert.assertEquals(2, students.size());
        int count = 0;
        for (StudentBooleanPrimitive student : students)
        {
            if (student.getId() == ((Boolean) getMaxValue(boolean.class)).booleanValue())
            {
                Assert.assertEquals(getMaxValue(short.class), student.getAge());
                Assert.assertEquals("Kuldeep", student.getName());
                count++;
            }
            else if (student.getId() == ((Boolean) getMinValue(boolean.class)).booleanValue())
            {
                Assert.assertEquals(getMinValue(short.class), student.getAge());
                Assert.assertEquals(getMinValue(String.class), student.getName());
                count++;
            }
        }
        Assert.assertEquals(2, count);
        em.close();
    }

    private void findByNameAndAgeGTAndLT()
    {
        EntityManager em;
        String query;
        Query q;
        List<StudentBooleanPrimitive> students;
        em = emf.createEntityManager();
        query = "Select s From StudentBooleanPrimitive s where s.name = Amresh and s.age > " + getMinValue(short.class)
                + " and s.age < " + getMaxValue(short.class);
        q = em.createQuery(query);
        students = q.getResultList();
        Assert.assertNotNull(students);
        Assert.assertTrue(students.isEmpty());

        em.close();
    }

    private void findByNameAndAgeGTEQAndLTEQ()
    {
        EntityManager em;
        String query;
        Query q;
        List<StudentBooleanPrimitive> students;
        int count;
        em = emf.createEntityManager();
        query = "Select s From StudentBooleanPrimitive s where s.name = Kuldeep and s.age >= "
                + getMinValue(short.class) + " and s.age <= " + getMaxValue(short.class);
        q = em.createQuery(query);
        students = q.getResultList();
        Assert.assertNotNull(students);
        Assert.assertEquals(2, students.size());
        count = 0;
        for (StudentBooleanPrimitive student : students)
        {
            if (student.getId() == ((Boolean) getMaxValue(boolean.class)).booleanValue())
            {
                Assert.assertEquals(getMaxValue(short.class), student.getAge());
                Assert.assertEquals("Kuldeep", student.getName());
                count++;
            }
            else
            {
                Assert.assertEquals(getMinValue(boolean.class), student.getId());
                Assert.assertEquals(getMinValue(short.class), student.getAge());
                Assert.assertEquals(getMinValue(String.class), student.getName());
                count++;
            }

        }
        Assert.assertEquals(2, count);
        em.close();

    }

    private void findByAge()
    {
        EntityManager em;
        String query;
        Query q;
        List<StudentBooleanPrimitive> students;
        int count;
        em = emf.createEntityManager();
        query = "Select s From StudentBooleanPrimitive s where s.age = " + getMinValue(short.class);
        q = em.createQuery(query);
        students = q.getResultList();
        Assert.assertNotNull(students);
        Assert.assertEquals(1, students.size());
        count = 0;
        for (StudentBooleanPrimitive student : students)
        {
            Assert.assertEquals(getMinValue(boolean.class), student.getId());
            Assert.assertEquals(getMinValue(short.class), student.getAge());
            Assert.assertEquals(getMinValue(String.class), student.getName());
            count++;
        }
        Assert.assertEquals(1, count);
        em.close();
    }

    /**
     * 
     */
    private void findByName()
    {
        EntityManager em;
        String query;
        Query q;
        List<StudentBooleanPrimitive> students;
        int count;
        em = emf.createEntityManager();
        query = "Select s From StudentBooleanPrimitive s where s.name = Kuldeep";
        q = em.createQuery(query);
        students = q.getResultList();
        Assert.assertNotNull(students);
        Assert.assertEquals(2, students.size());
        count = 0;
        for (StudentBooleanPrimitive student : students)
        {
            if (student.getId() == ((Boolean) getMaxValue(boolean.class)).booleanValue())
            {
                Assert.assertEquals(getMaxValue(short.class), student.getAge());
                Assert.assertEquals("Kuldeep", student.getName());
                count++;
            }
            else
            {
                Assert.assertEquals(getMinValue(boolean.class), student.getId());
                Assert.assertEquals(getMinValue(short.class), student.getAge());
                Assert.assertEquals(getMinValue(String.class), student.getName());
                count++;
            }
        }
        Assert.assertEquals(2, count);
        em.close();
    }

    /**
     * 
     */
    private void findAllQuery()
    {
        EntityManager em = emf.createEntityManager();
        // Selet all query.
        String query = "Select s From StudentBooleanPrimitive s ";
        Query q = em.createQuery(query);
        List<StudentBooleanPrimitive> students = q.getResultList();
        Assert.assertNotNull(students);
        Assert.assertEquals(2, students.size());
        int count = 0;
        for (StudentBooleanPrimitive student : students)
        {
            if (student.getId() == ((Boolean) getMaxValue(boolean.class)).booleanValue())
            {
                Assert.assertEquals(getMaxValue(short.class), student.getAge());
                Assert.assertEquals("Kuldeep", student.getName());
                count++;
            }
            else if (student.getId() == ((Boolean) getMinValue(boolean.class)).booleanValue())
            {
                Assert.assertEquals(getMinValue(short.class), student.getAge());
                Assert.assertEquals(getMinValue(String.class), student.getName());
                count++;
            }
        }
        Assert.assertEquals(2, count);
        em.close();
    }

    public void startCluster()
    {
        try
        {
            CassandraCli.cassandraSetUp();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        catch (TException e)
        {
            e.printStackTrace();
        }
        catch (InvalidRequestException e)
        {
            e.printStackTrace();
        }
        catch (UnavailableException e)
        {
            e.printStackTrace();
        }
        catch (TimedOutException e)
        {
            e.printStackTrace();
        }
        catch (SchemaDisagreementException e)
        {
            e.printStackTrace();
        }
    }

    public void stopCluster()
    {
        // TODO Auto-generated method stub

    }

    public void createSchema()
    {
        try
        {
            KsDef ksDef = null;

            CfDef cfDef = new CfDef();
            cfDef.name = "StudentBooleanPrimitive";
            cfDef.keyspace = keyspace;
            cfDef.setKey_validation_class("BooleanType");
            cfDef.setComparator_type("UTF8Type");
            ColumnDef name = new ColumnDef(ByteBuffer.wrap("NAME".getBytes()), "UTF8Type");
            name.index_type = IndexType.KEYS;
            cfDef.addToColumn_metadata(name);
            ColumnDef age = new ColumnDef(ByteBuffer.wrap("AGE".getBytes()), "Int32Type");
            age.index_type = IndexType.KEYS;
            cfDef.addToColumn_metadata(age);
            List<CfDef> cfDefs = new ArrayList<CfDef>();
            cfDefs.add(cfDef);
            try
            {
                CassandraCli.initClient();
                ksDef = CassandraCli.client.describe_keyspace(keyspace);
                CassandraCli.client.set_keyspace(keyspace);

                List<CfDef> cfDefn = ksDef.getCf_defs();

                for (CfDef cfDef1 : cfDefn)
                {

                    if (cfDef1.getName().equalsIgnoreCase("StudentBooleanPrimitive"))
                    {

                        CassandraCli.client.system_drop_column_family("StudentBooleanPrimitive");

                    }
                }
                CassandraCli.client.system_add_column_family(cfDef);

            }
            catch (NotFoundException e)
            {

                ksDef = new KsDef(keyspace, "org.apache.cassandra.locator.SimpleStrategy", cfDefs);
                // Set replication factor
                if (ksDef.strategy_options == null)
                {
                    ksDef.strategy_options = new LinkedHashMap<String, String>();
                }
                // Set replication factor, the value MUST be an int
                ksDef.strategy_options.put("replication_factor", "1");
                CassandraCli.client.system_add_keyspace(ksDef);
            }

            CassandraCli.client.set_keyspace(keyspace);
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

    public void dropSchema()
    {
        CassandraCli.executeCqlQuery("TRUNCATE \"StudentCassandraBooleanPrimitive\"");
    }

}
