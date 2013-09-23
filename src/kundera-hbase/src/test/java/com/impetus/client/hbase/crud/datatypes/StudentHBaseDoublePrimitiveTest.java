package com.impetus.client.hbase.crud.datatypes;

import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.hbase.crud.datatypes.entities.StudentHBaseDoublePrimitive;
import com.impetus.client.hbase.junits.HBaseCli;

public class StudentHBaseDoublePrimitiveTest extends Base
{
    private static final String table = "StudentHBaseDoublePrimitive";

    private HBaseCli cli;

    private static final String keyspace = "KunderaHbaseDataType";

    private EntityManagerFactory emf;

    @Before
    public void setUp() throws Exception
    {
        if (RUN_IN_EMBEDDED_MODE)
        {
            startCluster();
        }
        if (AUTO_MANAGE_SCHEMA)
        {
            createSchema();
        }
        emf = Persistence.createEntityManagerFactory("HbaseDataTypeTest");
    }

    @After
    public void tearDown() throws Exception
    {
        emf.close();
        if (AUTO_MANAGE_SCHEMA)
        {
            dropSchema();
        }
        if (RUN_IN_EMBEDDED_MODE)
        {
            stopCluster();
        }
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

        // Insert max value of Double
        StudentHBaseDoublePrimitive studentMax = new StudentHBaseDoublePrimitive();
        studentMax.setAge((Short) getMaxValue(short.class));
        studentMax.setId((Double) getMaxValue(Double.class));
        studentMax.setName((String) getMaxValue(String.class));
        em.persist(studentMax);

        // Insert min value of Double
        StudentHBaseDoublePrimitive studentMin = new StudentHBaseDoublePrimitive();
        studentMin.setAge((Short) getPartialValue(short.class));
        studentMin.setId((Double) getMinValue(Double.class));
        studentMin.setName((String) getMinValue(String.class));
        em.persist(studentMin);

        // Insert random value of Double
        StudentHBaseDoublePrimitive student = new StudentHBaseDoublePrimitive();
        student.setAge((Short) getRandomValue(short.class));
        student.setId((Double) getRandomValue(Double.class));
        student.setName((String) getRandomValue(String.class));
        em.persist(student);
        em.close();
    }

    public void testFindById(boolean useSameEm)
    {
        EntityManager em = emf.createEntityManager();

        StudentHBaseDoublePrimitive studentMax = em.find(StudentHBaseDoublePrimitive.class, getMaxValue(Double.class));
        Assert.assertNotNull(studentMax);
        Assert.assertEquals(getMaxValue(short.class), studentMax.getAge());
        Assert.assertEquals(getMaxValue(String.class), studentMax.getName());

        if (!useSameEm)
        {
            em.close();
            em = emf.createEntityManager();
        }
        StudentHBaseDoublePrimitive studentMin = em.find(StudentHBaseDoublePrimitive.class, getMinValue(Double.class));
        Assert.assertNotNull(studentMin);
        Assert.assertEquals(getPartialValue(short.class), studentMin.getAge());
        Assert.assertEquals(getMinValue(String.class), studentMin.getName());

        if (!useSameEm)
        {
            em.close();
            em = emf.createEntityManager();
        }
        StudentHBaseDoublePrimitive student = em.find(StudentHBaseDoublePrimitive.class, getRandomValue(Double.class));
        Assert.assertNotNull(student);
        Assert.assertEquals(getRandomValue(short.class), student.getAge());
        Assert.assertEquals(getRandomValue(String.class), student.getName());
        em.close();
    }

    public void testMerge(boolean useSameEm)
    {
        EntityManager em = emf.createEntityManager();
        StudentHBaseDoublePrimitive student = em.find(StudentHBaseDoublePrimitive.class, getMaxValue(Double.class));
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
        StudentHBaseDoublePrimitive newStudent = em.find(StudentHBaseDoublePrimitive.class, getMaxValue(Double.class));
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
        List<StudentHBaseDoublePrimitive> students;
        int count;
        em = emf.createEntityManager();
        query = "Select s From StudentHBaseDoublePrimitive s where s.age = " + getPartialValue(short.class)
                + " and s.name > Amresh and s.name <= " + getMaxValue(String.class);
        q = em.createQuery(query);
        students = q.getResultList();
        Assert.assertNotNull(students);
        Assert.assertEquals(1, students.size());
        count = 0;
        for (StudentHBaseDoublePrimitive student : students)
        {
            Assert.assertEquals(getMinValue(Double.class), student.getId());
            Assert.assertEquals(getPartialValue(short.class), student.getAge());
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
        List<StudentHBaseDoublePrimitive> students;
        em = emf.createEntityManager();
        query = "Select s From StudentHBaseDoublePrimitive s where s.id between ?1 and ?2";
        q = em.createQuery(query);
        q.setParameter(1, getMinValue(double.class));
        q.setParameter(2, getMaxValue(double.class));
        students = q.getResultList();
        Assert.assertNotNull(students);
        Assert.assertEquals(2, students.size());
        int count = 0;
        for (StudentHBaseDoublePrimitive student : students)
        {
            /*
             * if (student.getId() == ((Double)
             * getMaxValue(Double.class)).doubleValue()) {
             * Assert.assertEquals(getMaxValue(short.class), student.getAge());
             * Assert.assertEquals("Kuldeep", student.getName()); count++; }
             * else
             */if (student.getId() == ((Double) getMinValue(Double.class)).doubleValue())
            {
                Assert.assertEquals(getPartialValue(short.class), student.getAge());
                Assert.assertEquals(getMinValue(String.class), student.getName());
                count++;
            }
            else
            {
                Assert.assertEquals(getRandomValue(Double.class), student.getId());
                Assert.assertEquals(getRandomValue(short.class), student.getAge());
                Assert.assertEquals(getRandomValue(String.class), student.getName());
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
        List<StudentHBaseDoublePrimitive> students;
        int count;
        em = emf.createEntityManager();
        query = "Select s From StudentHBaseDoublePrimitive s where s.name = Kuldeep and s.age > " + getPartialValue(short.class);
        q = em.createQuery(query);
        students = q.getResultList();
        Assert.assertNotNull(students);
        Assert.assertEquals(1, students.size());
        count = 0;
        for (StudentHBaseDoublePrimitive student : students)
        {
            Assert.assertEquals(getMaxValue(Double.class), student.getId());
            Assert.assertEquals(getMaxValue(short.class), student.getAge());
            Assert.assertEquals("Kuldeep", student.getName());
            count++;
        }
        Assert.assertEquals(1, count);
        em.close();
    }

    private void findByNameAndAgeGTAndLTEQ()
    {

        EntityManager em;
        String query;
        Query q;
        List<StudentHBaseDoublePrimitive> students;
        int count;
        em = emf.createEntityManager();
        query = "Select s From StudentHBaseDoublePrimitive s where s.name = Kuldeep and s.age > " + getPartialValue(short.class)
                + " and s.age <= " + getMaxValue(short.class);
        q = em.createQuery(query);
        students = q.getResultList();
        Assert.assertNotNull(students);
        Assert.assertEquals(1, students.size());
        count = 0;
        for (StudentHBaseDoublePrimitive student : students)
        {
            Assert.assertEquals(getMaxValue(Double.class), student.getId());
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

        StudentHBaseDoublePrimitive studentMax = em.find(StudentHBaseDoublePrimitive.class, getMaxValue(Double.class));
        Assert.assertNotNull(studentMax);
        Assert.assertEquals(getMaxValue(short.class), studentMax.getAge());
        Assert.assertEquals("Kuldeep", studentMax.getName());
        em.remove(studentMax);
        if (!useSameEm)
        {
            em.close();
            em = emf.createEntityManager();
        }
        studentMax = em.find(StudentHBaseDoublePrimitive.class, getMaxValue(Double.class));
        Assert.assertNull(studentMax);
        em.close();
    }

    /**
     * 
     */
    private void deleteNamed(boolean useSameEm)
    {

        String deleteQuery = "Delete From StudentHBaseDoublePrimitive s where s.name=Vivek";
        EntityManager em = emf.createEntityManager();
        Query q = em.createQuery(deleteQuery);
        q.executeUpdate();
        if (!useSameEm)
        {
            em.close();
            em = emf.createEntityManager();
        }
        StudentHBaseDoublePrimitive newStudent = em.find(StudentHBaseDoublePrimitive.class, getRandomValue(Double.class));
        Assert.assertNull(newStudent);
        em.close();
    }

    /**
     * @return
     */
    private void updateNamed(boolean useSameEm)
    {
        EntityManager em = emf.createEntityManager();
        String updateQuery = "Update StudentHBaseDoublePrimitive s SET s.name=Vivek where s.name=Amresh";
        Query q = em.createQuery(updateQuery);
        q.executeUpdate();
        if (!useSameEm)
        {
            em.close();
            em = emf.createEntityManager();
        }
        StudentHBaseDoublePrimitive newStudent = em.find(StudentHBaseDoublePrimitive.class, getRandomValue(Double.class));
        Assert.assertNotNull(newStudent);
        Assert.assertEquals(getRandomValue(short.class), newStudent.getAge());
        Assert.assertEquals("Vivek", newStudent.getName());
        em.close();
    }

    private void findByNameAndAGEBetween()
    {
        EntityManager em;
        String query;
        Query q;
        List<StudentHBaseDoublePrimitive> students;
        int count;
        em = emf.createEntityManager();
        query = "Select s From StudentHBaseDoublePrimitive s where s.name = Amresh and s.age between "
                + getPartialValue(short.class) + " and " + getMaxValue(short.class);
        q = em.createQuery(query);
        students = q.getResultList();
        Assert.assertNotNull(students);
        Assert.assertEquals(1, students.size());
        count = 0;
        for (StudentHBaseDoublePrimitive student : students)
        {
            Assert.assertEquals(getRandomValue(Double.class), student.getId());
            Assert.assertEquals(getRandomValue(short.class), student.getAge());
            Assert.assertEquals(getRandomValue(String.class), student.getName());
            count++;

        }
        Assert.assertEquals(1, count);
        em.close();
    }

    private void findByNameAndAgeGTAndLT()
    {
        EntityManager em;
        String query;
        Query q;
        List<StudentHBaseDoublePrimitive> students;
        int count;
        em = emf.createEntityManager();
        query = "Select s From StudentHBaseDoublePrimitive s where s.name = Amresh and s.age > " + getPartialValue(short.class)
                + " and s.age < " + getMaxValue(short.class);
        q = em.createQuery(query);
        students = q.getResultList();
        Assert.assertNotNull(students);
        Assert.assertEquals(1, students.size());
        count = 0;
        for (StudentHBaseDoublePrimitive student : students)
        {
            Assert.assertEquals(getRandomValue(Double.class), student.getId());
            Assert.assertEquals(getRandomValue(short.class), student.getAge());
            Assert.assertEquals(getRandomValue(String.class), student.getName());
            count++;

        }
        Assert.assertEquals(1, count);
        em.close();

    }

    private void findByNameAndAgeGTEQAndLTEQ()
    {
        EntityManager em;
        String query;
        Query q;
        List<StudentHBaseDoublePrimitive> students;
        int count;
        em = emf.createEntityManager();
        query = "Select s From StudentHBaseDoublePrimitive s where s.name = Kuldeep and s.age >= "
                + getPartialValue(short.class) + " and s.age <= " + getMaxValue(short.class);
        q = em.createQuery(query);
        students = q.getResultList();
        Assert.assertNotNull(students);
        Assert.assertEquals(2, students.size());
        count = 0;
        for (StudentHBaseDoublePrimitive student : students)
        {
            if (student.getId() == ((Double) getMaxValue(Double.class)).doubleValue())
            {
                Assert.assertEquals(getMaxValue(short.class), student.getAge());
                Assert.assertEquals("Kuldeep", student.getName());
                count++;
            }
            else
            {
                Assert.assertEquals(getMinValue(Double.class), student.getId());
                Assert.assertEquals(getPartialValue(short.class), student.getAge());
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
        List<StudentHBaseDoublePrimitive> students;
        int count;
        em = emf.createEntityManager();
        query = "Select s From StudentHBaseDoublePrimitive s where s.age = " + getRandomValue(short.class);
        q = em.createQuery(query);
        students = q.getResultList();
        Assert.assertNotNull(students);
        Assert.assertEquals(1, students.size());
        count = 0;
        for (StudentHBaseDoublePrimitive student : students)
        {
            Assert.assertEquals(getRandomValue(Double.class), student.getId());
            Assert.assertEquals(getRandomValue(short.class), student.getAge());
            Assert.assertEquals(getRandomValue(String.class), student.getName());
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
        List<StudentHBaseDoublePrimitive> students;
        int count;
        em = emf.createEntityManager();
        query = "Select s From StudentHBaseDoublePrimitive s where s.name = Kuldeep";
        q = em.createQuery(query);
        students = q.getResultList();
        Assert.assertNotNull(students);
        Assert.assertEquals(2, students.size());
        count = 0;
        for (StudentHBaseDoublePrimitive student : students)
        {
            if (student.getId() == ((Double) getMaxValue(Double.class)).doubleValue())
            {
                Assert.assertEquals(getMaxValue(short.class), student.getAge());
                Assert.assertEquals("Kuldeep", student.getName());
                count++;
            }
            else
            {
                Assert.assertEquals(getMinValue(Double.class), student.getId());
                Assert.assertEquals(getPartialValue(short.class), student.getAge());
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
        String query = "Select s From StudentHBaseDoublePrimitive s ";
        Query q = em.createQuery(query);
        List<StudentHBaseDoublePrimitive> students = q.getResultList();
        Assert.assertNotNull(students);
        Assert.assertEquals(3, students.size());
        int count = 0;
        for (StudentHBaseDoublePrimitive student : students)
        {
            if (student.getId() == ((Double) getMaxValue(Double.class)).doubleValue())
            {
                Assert.assertEquals(getMaxValue(short.class), student.getAge());
                Assert.assertEquals("Kuldeep", student.getName());
                count++;
            }
            else if (student.getId() == ((Double) getMinValue(Double.class)).doubleValue())
            {
                Assert.assertEquals(getPartialValue(short.class), student.getAge());
                Assert.assertEquals(getMinValue(String.class), student.getName());
                count++;
            }
            else
            {
                Assert.assertEquals(getRandomValue(Double.class), student.getId());
                Assert.assertEquals(getRandomValue(short.class), student.getAge());
                Assert.assertEquals(getRandomValue(String.class), student.getName());
                count++;
            }
        }
        Assert.assertEquals(3, count);
        em.close();
    }

    public void startCluster()
    {
        cli = new HBaseCli();
        cli.startCluster();
    }

    public void stopCluster()
    {
        cli.stopCluster();
    }

    public void createSchema()
    {
        cli.createTable(keyspace, table);
    }

    public void dropSchema()
    {
        cli.dropTable(keyspace);
    }

}
