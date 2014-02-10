package com.impetus.client.crud.datatypes;

import java.sql.SQLException;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.crud.datatypes.entities.StudentBooleanWrapper;
import com.impetus.kundera.query.QueryHandlerException;

public class StudentRdbmsBooleanWrapperTest extends RdbmsBase
{

    

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

    public void testPersist(Boolean useSameEm)
    {
        EntityManager em = emf.createEntityManager();

        // Insert max value of Boolean
        StudentBooleanWrapper studentMax = new StudentBooleanWrapper();
        studentMax.setAge((Short) getMaxValue(short.class));
        studentMax.setId((Boolean) getMaxValue(Boolean.class));
        studentMax.setName((String) getMaxValue(String.class));
        em.persist(studentMax);

        // Insert min value of Boolean
        StudentBooleanWrapper studentMin = new StudentBooleanWrapper();
        studentMin.setAge((Short) getMinValue(short.class));
        studentMin.setId((Boolean) getMinValue(Boolean.class));
        studentMin.setName((String) getMinValue(String.class));
        em.persist(studentMin);

        em.close();
    }

    public void testFindById(Boolean useSameEm)
    {
        EntityManager em = emf.createEntityManager();

        StudentBooleanWrapper studentMax = em.find(StudentBooleanWrapper.class, getMaxValue(Boolean.class));
        Assert.assertNotNull(studentMax);
        Assert.assertEquals(getMaxValue(short.class), studentMax.getAge());
        Assert.assertEquals(getMaxValue(String.class), studentMax.getName());

        if (!useSameEm)
        {
            em.close();
            em = emf.createEntityManager();
        }
        StudentBooleanWrapper studentMin = em.find(StudentBooleanWrapper.class, getMinValue(Boolean.class));
        Assert.assertNotNull(studentMin);
        Assert.assertEquals(getMinValue(short.class), studentMin.getAge());
        Assert.assertEquals(getMinValue(String.class), studentMin.getName());

        em.close();
    }

    public void testMerge(Boolean useSameEm)
    {
        EntityManager em = emf.createEntityManager();
        StudentBooleanWrapper student = em.find(StudentBooleanWrapper.class, getMaxValue(Boolean.class));
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
        StudentBooleanWrapper newStudent = em.find(StudentBooleanWrapper.class, getMaxValue(Boolean.class));
        Assert.assertNotNull(newStudent);
        Assert.assertEquals(getMaxValue(short.class), newStudent.getAge());
        Assert.assertEquals("Kuldeep", newStudent.getName());
    }

    public void testFindByQuery(Boolean useSameEm)
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
        List<StudentBooleanWrapper> students;
        int count;
        em = emf.createEntityManager();
        query = "Select s From StudentBooleanWrapper s where s.age = " + getMinValue(short.class)
                + " and s.name > 'Amresh' and s.name <= '" + getMaxValue(String.class) +"'";
        q = em.createQuery(query);
        students = q.getResultList();
        Assert.assertNotNull(students);
        Assert.assertEquals(1, students.size());
        count = 0;
        for (StudentBooleanWrapper student : students)
        {
            Assert.assertEquals(getMinValue(Boolean.class), student.getId());
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
        List<StudentBooleanWrapper> students;
        em = emf.createEntityManager();
        query = "Select s From StudentBooleanWrapper s where s.id between ?1 and ?2";
        q = em.createQuery(query);
        q.setParameter(1, getMinValue(Boolean.class));
        q.setParameter(2, getMaxValue(Boolean.class));
        students = q.getResultList();
        Assert.assertNotNull(students);
        Assert.assertEquals(2, students.size());
        int count = 0;
        for (StudentBooleanWrapper student : students)
        {
            if (student.getId().equals(getMaxValue(Boolean.class)))
            {
                Assert.assertEquals(getMaxValue(short.class), student.getAge());
                Assert.assertEquals("Kuldeep", student.getName());
                count++;
            }
            else if (student.getId().equals(getMinValue(Boolean.class)))
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
        List<StudentBooleanWrapper> students;
        int count;
        em = emf.createEntityManager();
        query = "Select s From StudentBooleanWrapper s where s.name = 'Kuldeep' or s.age > " + getMinValue(short.class);
        try
        {
            q = em.createQuery(query);
            students = q.getResultList();
            Assert.assertNotNull(students);
            Assert.assertEquals(2, students.size());
            count = 0;
            for (StudentBooleanWrapper student : students)
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
                }}
            Assert.assertEquals(2, count);
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
        List<StudentBooleanWrapper> students;
        int count;
        em = emf.createEntityManager();
        query = "Select s From StudentBooleanWrapper s where s.name = 'Kuldeep' and s.age > "
                + getMinValue(short.class) + " and s.age <= " + getMaxValue(short.class);
        q = em.createQuery(query);
        students = q.getResultList();
        Assert.assertNotNull(students);
        Assert.assertEquals(1, students.size());
        count = 0;
        for (StudentBooleanWrapper student : students)
        {
            Assert.assertEquals(getMaxValue(Boolean.class), student.getId());
            Assert.assertEquals(getMaxValue(short.class), student.getAge());
            Assert.assertEquals("Kuldeep", student.getName());
            count++;
        }
        Assert.assertEquals(1, count);
        em.close();
    }

    public void testNamedQueryUseSameEm(Boolean useSameEm)
    {
        updateNamed(true);
        deleteNamed(true);
    }

    public void testNamedQuery(Boolean useSameEm)
    {
        updateNamed(false);
        deleteNamed(false);
    }

    public void testDelete(Boolean useSameEm)
    {
        EntityManager em = emf.createEntityManager();

        StudentBooleanWrapper studentMax = em.find(StudentBooleanWrapper.class, getMinValue(Boolean.class));
        Assert.assertNotNull(studentMax);
        Assert.assertEquals(getMinValue(short.class), studentMax.getAge());
        Assert.assertEquals("Kuldeep", studentMax.getName());
        em.remove(studentMax);
        if (!useSameEm)
        {
            em.close();
            em = emf.createEntityManager();
        }
        studentMax = em.find(StudentBooleanWrapper.class, getMinValue(Boolean.class));
        Assert.assertNull(studentMax);
        em.close();
    }

    /**
     * 
     */
    private void deleteNamed(Boolean useSameEm)
    {

        String deleteQuery = "Delete From StudentBooleanWrapper s where s.name='Vivek'";
        EntityManager em = emf.createEntityManager();
        Query q = em.createQuery(deleteQuery);
        q.executeUpdate();
        if (!useSameEm)
        {
            em.close();
            em = emf.createEntityManager();
        }
        StudentBooleanWrapper newStudent = em.find(StudentBooleanWrapper.class, getRandomValue(Boolean.class));
        Assert.assertNull(newStudent);
        em.close();
    }

    /**
     * @return
     */
    private void updateNamed(Boolean useSameEm)
    {
        EntityManager em = emf.createEntityManager();
        String updateQuery = "Update StudentBooleanWrapper s SET s.name='Vivek' where s.id=true";
        Query q = em.createQuery(updateQuery);
        q.executeUpdate();
        if (!useSameEm)
        {
            em.close();
            em = emf.createEntityManager();
        }
        StudentBooleanWrapper newStudent = em.find(StudentBooleanWrapper.class, getMaxValue(Boolean.class));
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
        List<StudentBooleanWrapper> students;
        em = emf.createEntityManager();
        query = "Select s From StudentBooleanWrapper s where s.name = 'Kuldeep' and s.age between "
                + getMinValue(short.class) + " and " + getMaxValue(short.class);
        q = em.createQuery(query);
        students = q.getResultList();
        Assert.assertNotNull(students);
        Assert.assertEquals(2, students.size());
        int count = 0;
        for (StudentBooleanWrapper student : students)
        {
            if (student.getId() == ((Boolean) getMaxValue(Boolean.class)).booleanValue())
            {
                Assert.assertEquals(getMaxValue(short.class), student.getAge());
                Assert.assertEquals("Kuldeep", student.getName());
                count++;
            }
            else if (student.getId() == ((Boolean) getMinValue(Boolean.class)).booleanValue())
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
        List<StudentBooleanWrapper> students;
        em = emf.createEntityManager();
        query = "Select s From StudentBooleanWrapper s where s.name = 'Amresh' and s.age > " + getMinValue(short.class)
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
        List<StudentBooleanWrapper> students;
        int count;
        em = emf.createEntityManager();
        query = "Select s From StudentBooleanWrapper s where s.name = 'Kuldeep' and s.age >= "
                + getMinValue(short.class) + " and s.age <= " + getMaxValue(short.class);
        q = em.createQuery(query);
        students = q.getResultList();
        Assert.assertNotNull(students);
        Assert.assertEquals(2, students.size());
        count = 0;
        for (StudentBooleanWrapper student : students)
        {
            if (student.getId().equals(getMaxValue(Boolean.class)))
            {
                Assert.assertEquals(getMaxValue(short.class), student.getAge());
                Assert.assertEquals("Kuldeep", student.getName());
                count++;
            }
            else
            {
                Assert.assertEquals(getMinValue(Boolean.class), student.getId());
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
        List<StudentBooleanWrapper> students;
        int count;
        em = emf.createEntityManager();
        query = "Select s From StudentBooleanWrapper s where s.age = " + getMinValue(short.class);
        q = em.createQuery(query);
        students = q.getResultList();
        Assert.assertNotNull(students);
        Assert.assertEquals(1, students.size());
        count = 0;
        for (StudentBooleanWrapper student : students)
        {
            Assert.assertEquals(getMinValue(Boolean.class), student.getId());
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
        List<StudentBooleanWrapper> students;
        int count;
        em = emf.createEntityManager();
        query = "Select s From StudentBooleanWrapper s where s.name = 'Kuldeep'";
        q = em.createQuery(query);
        students = q.getResultList();
        Assert.assertNotNull(students);
        Assert.assertEquals(2, students.size());
        count = 0;
        for (StudentBooleanWrapper student : students)
        {
            if (student.getId().equals(getMaxValue(Boolean.class)))
            {
                Assert.assertEquals(getMaxValue(short.class), student.getAge());
                Assert.assertEquals("Kuldeep", student.getName());
                count++;
            }
            else
            {
                Assert.assertEquals(getMinValue(Boolean.class), student.getId());
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
        String query = "Select s From StudentBooleanWrapper s ";
        Query q = em.createQuery(query);
        List<StudentBooleanWrapper> students = q.getResultList();
        Assert.assertNotNull(students);
        Assert.assertEquals(2, students.size());
        int count = 0;
        for (StudentBooleanWrapper student : students)
        {
            if (student.getId().equals(getMaxValue(Boolean.class)))
            {
                Assert.assertEquals(getMaxValue(short.class), student.getAge());
                Assert.assertEquals("Kuldeep", student.getName());
                count++;
            }
            else if (student.getId().equals(getMinValue(Boolean.class)))
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
    	
    }

    public void stopCluster()
    {
        // TODO Auto-generated method stub

    }

    public void createSchema() throws SQLException
    {
        try
        {
            cli.createSchema("testdb");
            cli.update("CREATE TABLE TESTDB.StudentBooleanWrapper (id BOOLEAN PRIMARY KEY, NAME VARCHAR(256), AGE SMALLINT)");
        }
        catch (Exception e)
        {
            cli.update("DELETE FROM TESTDB.StudentBooleanWrapper");
            cli.update("DROP TABLE TESTDB.StudentBooleanWrapper");
            cli.update("DROP SCHEMA TESTDB");
            cli.update("CREATE TABLE TESTDB.StudentBooleanWrapper (id BOOLEAN PRIMARY KEY, NAME VARCHAR(256), AGE SMALLINT)");
        }
    }

    public void dropSchema()
    {
        try
        {
            cli.update("DELETE FROM TESTDB.StudentBooleanWrapper");
            cli.update("DROP TABLE TESTDB.StudentBooleanWrapper");
            cli.update("DROP SCHEMA TESTDB");
            cli.closeConnection();
        }
        catch (Exception e)
        {
            // Nothing to do
        }
    }
}
