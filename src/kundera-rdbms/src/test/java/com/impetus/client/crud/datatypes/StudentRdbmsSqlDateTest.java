package com.impetus.client.crud.datatypes;

import java.sql.Date;
import java.sql.SQLException;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.crud.datatypes.entities.StudentSqlDate;

public class StudentRdbmsSqlDateTest extends RdbmsBase
{
    private static final Date DATE = new Date(System.currentTimeMillis());
    
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

        // Insert random value of Date
        StudentSqlDate student = new StudentSqlDate();
        student.setAge((Short) getRandomValue(short.class));
        student.setId((Date) getRandomValue());
        student.setName((String) getRandomValue(String.class));
        em.persist(student);

        // Insert max value of Date
        StudentSqlDate studentMax = new StudentSqlDate();
        studentMax.setAge((Short) getMaxValue(short.class));
        studentMax.setId((Date) getMaxValue());
        studentMax.setName((String) getMaxValue(String.class));
        em.persist(studentMax);

        // Insert min value of Date
        StudentSqlDate studentMin = new StudentSqlDate();
        studentMin.setAge((Short) getMinValue(short.class));
        studentMin.setId((Date) getMinValue());
        studentMin.setName((String) getMinValue(String.class));
        em.persist(studentMin);

        em.close();
    }

    private Object getMinValue()
    {
        return new Date(1970, 1, 1);
    }

    private Object getRandomValue()
    {
        return DATE;
    }

    private Object getMaxValue()
    {
        return new Date(2100, 1, 1);
    }

    public void testFindById(boolean useSameEm)
    {
        EntityManager em = emf.createEntityManager();

        StudentSqlDate studentMax = em.find(StudentSqlDate.class, getMaxValue());
        Assert.assertNotNull(studentMax);
        Assert.assertEquals(getMaxValue(short.class), studentMax.getAge());
        Assert.assertEquals(getMaxValue(String.class), studentMax.getName());

        if (!useSameEm)
        {
            em.close();
            em = emf.createEntityManager();
        }
        StudentSqlDate studentMin = em.find(StudentSqlDate.class, getMinValue());
        Assert.assertNotNull(studentMin);
        Assert.assertEquals(getMinValue(short.class), studentMin.getAge());
        Assert.assertEquals(getMinValue(String.class), studentMin.getName());

        if (!useSameEm)
        {
            em.close();
            em = emf.createEntityManager();
        }
        StudentSqlDate student = em.find(StudentSqlDate.class, getRandomValue());
        Assert.assertNotNull(student);
        Assert.assertEquals(getRandomValue(short.class), student.getAge());
        Assert.assertEquals(getRandomValue(String.class), student.getName());
        em.close();
    }

    public void testMerge(boolean useSameEm)
    {
        EntityManager em = emf.createEntityManager();
        StudentSqlDate student = em.find(StudentSqlDate.class, getMaxValue());
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
        StudentSqlDate newStudent = em.find(StudentSqlDate.class, getMaxValue());
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
        // findByRange();
    }

    private void findByAgeAndNameGTAndLT()
    {
        EntityManager em;
        String query;
        Query q;
        List<StudentSqlDate> students;
        int count;
        em = emf.createEntityManager();
        query = "Select s From StudentSqlDate s where s.age = " + getMinValue(short.class)
                + " and s.name > Amresh and s.name <= " + getMaxValue(String.class);
        q = em.createQuery(query);
        students = q.getResultList();
        Assert.assertNotNull(students);
        Assert.assertEquals(1, students.size());
        count = 0;
        for (StudentSqlDate student : students)
        {
            Assert.assertEquals(getMinValue(), student.getId());
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
        List<StudentSqlDate> students;
        em = emf.createEntityManager();
        query = "Select s From StudentSqlDate s where s.id between " + getMinValue() + " and " + getMaxValue();
        q = em.createQuery(query);
        students = q.getResultList();
        Assert.assertNotNull(students);
        Assert.assertEquals(3, students.size());
        int count = 0;
        for (StudentSqlDate student : students)
        {
            if (student.getId().equals(getMaxValue()))
            {
                Assert.assertEquals(getMaxValue(short.class), student.getAge());
                Assert.assertEquals("Kuldeep", student.getName());
                count++;
            }
            else if (student.getId().equals(getMinValue()))
            {
                Assert.assertEquals(getMinValue(short.class), student.getAge());
                Assert.assertEquals(getMinValue(String.class), student.getName());
                count++;
            }
            else
            {
                Assert.assertEquals(((Date) getRandomValue()).getDate(), student.getId().getDate());
                Assert.assertEquals(((Date) getRandomValue()).getYear(), student.getId().getYear());
                Assert.assertEquals(((Date) getRandomValue()).getMonth(), student.getId().getMonth());
                Assert.assertEquals(getRandomValue(short.class), student.getAge());
                Assert.assertEquals(getRandomValue(String.class), student.getName());
                count++;
            }
        }
        Assert.assertEquals(3, count);
        em.close();
    }

    private void findByNameAndAgeWithOrClause()
    {
        EntityManager em;
        String query;
        Query q;
        List<StudentSqlDate> students;
        int count;
        em = emf.createEntityManager();
        query = "Select s From StudentSqlDate s where s.name = Kuldeep and s.age > " + getMinValue(short.class);
        q = em.createQuery(query);
        students = q.getResultList();
        Assert.assertNotNull(students);
        Assert.assertEquals(1, students.size());
        count = 0;
        for (StudentSqlDate student : students)
        {
            Assert.assertEquals(((Date) getMaxValue()).getDate(), student.getId().getDate());
            Assert.assertEquals(((Date) getMaxValue()).getYear(), student.getId().getYear());
            Assert.assertEquals(((Date) getMaxValue()).getMonth(), student.getId().getMonth());
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
        List<StudentSqlDate> students;
        int count;
        em = emf.createEntityManager();
        query = "Select s From StudentSqlDate s where s.name = Kuldeep and s.age > " + getMinValue(short.class)
                + " and s.age <= " + getMaxValue(short.class);
        q = em.createQuery(query);
        students = q.getResultList();
        Assert.assertNotNull(students);
        Assert.assertEquals(1, students.size());
        count = 0;
        for (StudentSqlDate student : students)
        {
            Assert.assertEquals(((Date) getMaxValue()).getDate(), student.getId().getDate());
            Assert.assertEquals(((Date) getMaxValue()).getYear(), student.getId().getYear());
            Assert.assertEquals(((Date) getMaxValue()).getMonth(), student.getId().getMonth());
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

        StudentSqlDate studentMax = em.find(StudentSqlDate.class, getMaxValue());
        Assert.assertNotNull(studentMax);
        Assert.assertEquals(getMaxValue(short.class), studentMax.getAge());
        Assert.assertEquals("Kuldeep", studentMax.getName());
        em.remove(studentMax);
        if (!useSameEm)
        {
            em.close();
            em = emf.createEntityManager();
        }
        studentMax = em.find(StudentSqlDate.class, getMaxValue());
        Assert.assertNull(studentMax);
        em.close();
    }

    /**
     * 
     */
    private void deleteNamed(boolean useSameEm)
    {

        String deleteQuery = "Delete From StudentSqlDate s where s.name=Vivek";
        EntityManager em = emf.createEntityManager();
        Query q = em.createQuery(deleteQuery);
        q.executeUpdate();
        if (!useSameEm)
        {
            em.close();
            em = emf.createEntityManager();
        }
        StudentSqlDate newStudent = em.find(StudentSqlDate.class, getRandomValue());
        Assert.assertNull(newStudent);
        em.close();
    }

    /**
     * @return
     */
    private void updateNamed(boolean useSameEm)
    {
        EntityManager em = emf.createEntityManager();
        String updateQuery = "Update StudentSqlDate s SET s.name=Vivek where s.name=Amresh";
        Query q = em.createQuery(updateQuery);
        q.executeUpdate();
        if (!useSameEm)
        {
            em.close();
            em = emf.createEntityManager();
        }
        StudentSqlDate newStudent = em.find(StudentSqlDate.class, getRandomValue());
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
        List<StudentSqlDate> students;
        int count;
        em = emf.createEntityManager();
        query = "Select s From StudentSqlDate s where s.name = Amresh and s.age between " + getMinValue(short.class)
                + " and " + getMaxValue(short.class);
        q = em.createQuery(query);
        students = q.getResultList();
        Assert.assertNotNull(students);
        Assert.assertEquals(1, students.size());
        count = 0;
        for (StudentSqlDate student : students)
        {
            Assert.assertEquals(((Date) getRandomValue()).getDate(), student.getId().getDate());
            Assert.assertEquals(((Date) getRandomValue()).getYear(), student.getId().getYear());
            Assert.assertEquals(((Date) getRandomValue()).getMonth(), student.getId().getMonth());
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
        List<StudentSqlDate> students;
        int count;
        em = emf.createEntityManager();
        query = "Select s From StudentSqlDate s where s.name = Amresh and s.age > " + getMinValue(short.class)
                + " and s.age < " + getMaxValue(short.class);
        q = em.createQuery(query);
        students = q.getResultList();
        Assert.assertNotNull(students);
        Assert.assertEquals(1, students.size());
        count = 0;
        for (StudentSqlDate student : students)
        {
            Assert.assertEquals(((Date) getRandomValue()).getDate(), student.getId().getDate());
            Assert.assertEquals(((Date) getRandomValue()).getYear(), student.getId().getYear());
            Assert.assertEquals(((Date) getRandomValue()).getMonth(), student.getId().getMonth());
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
        List<StudentSqlDate> students;
        int count;
        em = emf.createEntityManager();
        query = "Select s From StudentSqlDate s where s.name = Kuldeep and s.age >= " + getMinValue(short.class)
                + " and s.age <= " + getMaxValue(short.class);
        q = em.createQuery(query);
        students = q.getResultList();
        Assert.assertNotNull(students);
        Assert.assertEquals(2, students.size());
        count = 0;
        for (StudentSqlDate student : students)
        {
            if (student.getId().equals(getMaxValue()))
            {
                Assert.assertEquals(getMaxValue(short.class), student.getAge());
                Assert.assertEquals("Kuldeep", student.getName());
                count++;
            }
            else
            {
                Assert.assertEquals(((Date) getMinValue()).getDate(), student.getId().getDate());
                Assert.assertEquals(((Date) getMinValue()).getYear(), student.getId().getYear());
                Assert.assertEquals(((Date) getMinValue()).getMonth(), student.getId().getMonth());
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
        List<StudentSqlDate> students;
        int count;
        em = emf.createEntityManager();
        query = "Select s From StudentSqlDate s where s.age = " + getRandomValue(short.class);
        q = em.createQuery(query);
        students = q.getResultList();
        Assert.assertNotNull(students);
        Assert.assertEquals(1, students.size());
        count = 0;
        for (StudentSqlDate student : students)
        {
            Assert.assertEquals(((Date) getRandomValue()).getDate(), student.getId().getDate());
            Assert.assertEquals(((Date) getRandomValue()).getYear(), student.getId().getYear());
            Assert.assertEquals(((Date) getRandomValue()).getMonth(), student.getId().getMonth());
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
        List<StudentSqlDate> students;
        int count;
        em = emf.createEntityManager();
        query = "Select s From StudentSqlDate s where s.name = Kuldeep";
        q = em.createQuery(query);
        students = q.getResultList();
        Assert.assertNotNull(students);
        Assert.assertEquals(2, students.size());
        count = 0;
        for (StudentSqlDate student : students)
        {
            if (student.getId().equals(getMaxValue()))
            {
                Assert.assertEquals(getMaxValue(short.class), student.getAge());
                Assert.assertEquals("Kuldeep", student.getName());
                count++;
            }
            else
            {
                Assert.assertEquals(((Date) getMinValue()).getDate(), student.getId().getDate());
                Assert.assertEquals(((Date) getMinValue()).getYear(), student.getId().getYear());
                Assert.assertEquals(((Date) getMinValue()).getMonth(), student.getId().getMonth());
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
        String query = "Select s From StudentSqlDate s ";
        Query q = em.createQuery(query);
        List<StudentSqlDate> students = q.getResultList();
        Assert.assertNotNull(students);
        Assert.assertEquals(3, students.size());
        int count = 0;
        for (StudentSqlDate student : students)
        {
            if (student.getId().equals(getMaxValue()))
            {
                Assert.assertEquals(getMaxValue(short.class), student.getAge());
                Assert.assertEquals("Kuldeep", student.getName());
                count++;
            }
            else if (student.getId().equals(getMinValue()))
            {
                Assert.assertEquals(getMinValue(short.class), student.getAge());
                Assert.assertEquals(getMinValue(String.class), student.getName());
                count++;
            }
            else
            {
                Assert.assertEquals(((Date) getRandomValue()).getDate(), student.getId().getDate());
                Assert.assertEquals(((Date) getRandomValue()).getYear(), student.getId().getYear());
                Assert.assertEquals(((Date) getRandomValue()).getMonth(), student.getId().getMonth());
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
            cli.update("CREATE TABLE TESTDB.StudentSqlDate (id DATE PRIMARY KEY, NAME VARCHAR(256), AGE SMALLINT)");
        }
        catch (Exception e)
        {
            cli.update("DELETE FROM TESTDB.StudentSqlDate");
            cli.update("DROP TABLE TESTDB.StudentSqlDate");
            cli.update("DROP SCHEMA TESTDB");
            cli.update("CREATE TABLE TESTDB.StudentSqlDate (id DATE PRIMARY KEY, NAME VARCHAR(256), AGE SMALLINT)");
        }
    }

    public void dropSchema()
    {
        try
        {
            cli.update("DELETE FROM TESTDB.StudentSqlDate");
            cli.update("DROP TABLE TESTDB.StudentSqlDate");
            cli.update("DROP SCHEMA TESTDB");
            cli.closeConnection();
        }
        catch (Exception e)
        {
            // Nothing to do
        }
    }
}
