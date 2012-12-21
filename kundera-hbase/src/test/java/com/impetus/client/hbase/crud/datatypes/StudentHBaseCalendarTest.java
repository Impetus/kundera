package com.impetus.client.hbase.crud.datatypes;

import java.util.Calendar;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.hbase.crud.datatypes.entities.StudentHBaseCalendar;
import com.impetus.client.hbase.junits.HBaseCli;
import com.impetus.kundera.query.QueryHandlerException;

public class StudentHBaseCalendarTest extends Base
{

    private static final String table = "StudentHBaseCalendar";

    private EntityManagerFactory emf;

    private HBaseCli cli;

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

        // Insert max value of Calendar
        StudentHBaseCalendar studentMax = new StudentHBaseCalendar();
        studentMax.setAge((Short) getMaxValue(short.class));
        studentMax.setId(((Calendar) getMaxValue(Calendar.class)));
        studentMax.setName((String) getMaxValue(String.class));
        em.persist(studentMax);

        em.close();
    }

    public void testFindById(boolean useSameEm)
    {
        EntityManager em = emf.createEntityManager();

        StudentHBaseCalendar studentMax = em.find(StudentHBaseCalendar.class, getMaxValue(Calendar.class));
        Assert.assertNotNull(studentMax);
        Assert.assertEquals(getMaxValue(short.class), studentMax.getAge());
        Assert.assertEquals(getMaxValue(String.class), studentMax.getName());

        em.close();
    }

    public void testMerge(boolean useSameEm)
    {
        EntityManager em = emf.createEntityManager();
        StudentHBaseCalendar student = em.find(StudentHBaseCalendar.class, getMaxValue(Calendar.class));
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
        StudentHBaseCalendar newStudent = em.find(StudentHBaseCalendar.class, getMaxValue(Calendar.class));
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
//        findByRange();
    }

    private void findByAgeAndNameGTAndLT()
    {
        EntityManager em;
        String query;
        Query q;
        List<StudentHBaseCalendar> students;
        int count;
        em = emf.createEntityManager();
        query = "Select s From StudentHBaseCalendar s where s.age = " + getMaxValue(short.class)
                + " and s.name > Amresh and s.name <= " + getMaxValue(String.class);
        q = em.createQuery(query);
        students = q.getResultList();
        Assert.assertNotNull(students);
        Assert.assertEquals(1, students.size());
        count = 0;
        for (StudentHBaseCalendar student : students)
        {
            Assert.assertEquals(getMaxValue(Calendar.class), student.getId());
            Assert.assertEquals(getMaxValue(short.class), student.getAge());
            Assert.assertEquals("Kuldeep", student.getName());
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
        List<StudentHBaseCalendar> students;
        em = emf.createEntityManager();
        query = "Select s From StudentHBaseCalendar s where s.id between " + getRandomValue(Calendar.class) + " and "
                + getMaxValue(Calendar.class);
        q = em.createQuery(query);
        students = q.getResultList();
        Assert.assertNotNull(students);
        Assert.assertEquals(2, students.size());
        int count = 0;
        for (StudentHBaseCalendar student : students)
        {
            if (student.getId().equals(getRandomValue(Calendar.class)))
            {
                Assert.assertEquals(getRandomValue(short.class), student.getAge());
                Assert.assertEquals(getRandomValue(String.class), student.getName());
                count++;
            }
            else if (student.getId().equals(getMinValue(Calendar.class)))
            {
                Assert.assertEquals(getPartialValue(short.class), student.getAge());
                Assert.assertEquals(getMinValue(String.class), student.getName());
                count++;
            }
        }
        Assert.assertEquals(2, count);
        em.close();
    }

    private void findByNameAndAgeWithOrClause()
    {
        EntityManager em = null;
        String query;
        Query q;
        try
        {
            List<StudentHBaseCalendar> students;
            int count;
            em = emf.createEntityManager();
            query = "Select s From StudentHBaseCalendar s where s.name = Kuldeep and s.age > "
                    + getPartialValue(short.class);
            q = em.createQuery(query);
            students = q.getResultList();
            Assert.assertNotNull(students);
            Assert.assertEquals(1, students.size());
            count = 0;
            for (StudentHBaseCalendar student : students)
            {
                Assert.assertEquals(getMaxValue(Calendar.class), student.getId());
                Assert.assertEquals(getMaxValue(short.class), student.getAge());
                Assert.assertEquals("Kuldeep", student.getName());
                count++;
            }
            Assert.assertEquals(1, count);
        }
        catch (QueryHandlerException qhe)
        {
            Assert.assertEquals("unsupported clause OR for Hbase", qhe.getMessage());
        }
        finally
        {
            if (em != null)
            {
                em.close();
            }
        }
    }

    private void findByNameAndAgeGTAndLTEQ()
    {

        EntityManager em;
        String query;
        Query q;
        List<StudentHBaseCalendar> students;
        int count;
        em = emf.createEntityManager();
        query = "Select s From StudentHBaseCalendar s where s.name = Kuldeep and s.age > "
                + getPartialValue(short.class) + " and s.age <= " + getMaxValue(short.class);
        q = em.createQuery(query);
        students = q.getResultList();
        Assert.assertNotNull(students);
        Assert.assertEquals(1, students.size());
        count = 0;
        for (StudentHBaseCalendar student : students)
        {
            Assert.assertEquals(getMaxValue(Calendar.class), student.getId());
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

        // Insert max value of Calendar
        StudentHBaseCalendar studentMax = new StudentHBaseCalendar();
        studentMax.setAge((Short) getMaxValue(short.class));
        studentMax.setId(((Calendar) getMaxValue(Calendar.class)));
        studentMax.setName((String) getMaxValue(String.class));
        em.persist(studentMax);

        em.close();
        
         em = emf.createEntityManager();

         studentMax = em.find(StudentHBaseCalendar.class, getMaxValue(Calendar.class));
        Assert.assertNotNull(studentMax);
        Assert.assertEquals(getMaxValue(short.class), studentMax.getAge());
        Assert.assertEquals(getMaxValue(String.class), studentMax.getName());
        em.remove(studentMax);
        if (!useSameEm)
        {
            em.close();
            em = emf.createEntityManager();
        }
        studentMax = em.find(StudentHBaseCalendar.class, getMaxValue(Calendar.class));
        Assert.assertNull(studentMax);
        em.close();
    }

    /**
     * 
     */
    private void deleteNamed(boolean useSameEm)
    {

        String deleteQuery = "Delete From StudentHBaseCalendar s where s.name=Vivek";
        EntityManager em = emf.createEntityManager();
        Query q = em.createQuery(deleteQuery);
        q.executeUpdate();
        if (!useSameEm)
        {
            em.close();
            em = emf.createEntityManager();
        }
        StudentHBaseCalendar newStudent = em.find(StudentHBaseCalendar.class, getRandomValue(Calendar.class));
        Assert.assertNull(newStudent);
        em.close();
    }

    /**
     * @return
     */
    private void updateNamed(boolean useSameEm)
    {
        EntityManager em = emf.createEntityManager();
        String updateQuery = "Update StudentHBaseCalendar s SET s.name=Vivek where s.name=Kuldeep";
        Query q = em.createQuery(updateQuery);
        q.executeUpdate();
        if (!useSameEm)
        {
            em.close();
            em = emf.createEntityManager();
        }
        StudentHBaseCalendar newStudent = em.find(StudentHBaseCalendar.class, getRandomValue(Calendar.class));
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
        List<StudentHBaseCalendar> students;        
        em = emf.createEntityManager();
        query = "Select s From StudentHBaseCalendar s where s.name = Amresh and s.age between "
                + getPartialValue(short.class) + " and " + getMaxValue(short.class);
        q = em.createQuery(query);
        students = q.getResultList();
        Assert.assertNull(students);
      
        em.close();
    }

    private void findByNameAndAgeGTAndLT()
    {
        EntityManager em;
        String query;
        Query q;
        List<StudentHBaseCalendar> students;
        em = emf.createEntityManager();
        query = "Select s From StudentHBaseCalendar s where s.name = Kuldeep and s.age > "
                + getPartialValue(short.class) + " and s.age < " + getMaxValue(short.class);
        q = em.createQuery(query);
        students = q.getResultList();
        Assert.assertNull(students);

        em.close();

    }

    private void findByNameAndAgeGTEQAndLTEQ()
    {
        EntityManager em;
        String query;
        Query q;
        List<StudentHBaseCalendar> students;
        int count;
        em = emf.createEntityManager();
        query = "Select s From StudentHBaseCalendar s where s.name = Kuldeep and s.age >= "
                + getPartialValue(short.class) + " and s.age <= " + getMaxValue(short.class);
        q = em.createQuery(query);
        students = q.getResultList();
        Assert.assertNotNull(students);
        Assert.assertEquals(1, students.size());
        count = 0;
        for (StudentHBaseCalendar student : students)
        {
            if (student.getId().equals(getMaxValue(Calendar.class)))
            {
                Assert.assertEquals(getMaxValue(short.class), student.getAge());
                Assert.assertEquals("Kuldeep", student.getName());
                count++;
            }
        }
        Assert.assertEquals(1, count);
        em.close();

    }

    private void findByAge()
    {
        EntityManager em;
        String query;
        Query q;
        List<StudentHBaseCalendar> students;
        int count;
        em = emf.createEntityManager();
        query = "Select s From StudentHBaseCalendar s where s.age = " + getMaxValue(short.class);
        q = em.createQuery(query);
        students = q.getResultList();
        Assert.assertNotNull(students);
        Assert.assertEquals(1, students.size());
        count = 0;
        for (StudentHBaseCalendar student : students)
        {
            Assert.assertEquals(getMaxValue(Calendar.class), student.getId());
            Assert.assertEquals(getMaxValue(short.class), student.getAge());
            Assert.assertEquals("Kuldeep", student.getName());
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
        List<StudentHBaseCalendar> students;
        int count;
        em = emf.createEntityManager();
        query = "Select s From StudentHBaseCalendar s where s.name = Kuldeep";
        q = em.createQuery(query);
        students = q.getResultList();
        Assert.assertNotNull(students);
        Assert.assertEquals(1, students.size());
        count = 0;
        for (StudentHBaseCalendar student : students)
        {
            if (student.getId().equals(getMaxValue(Calendar.class)))
            {
                Assert.assertEquals(getMaxValue(short.class), student.getAge());
                Assert.assertEquals("Kuldeep", student.getName());
                count++;
            }
        }
        Assert.assertEquals(1, count);
        em.close();
    }

    /**
     * 
     */
    private void findAllQuery()
    {
        EntityManager em = emf.createEntityManager();
        // Selet all query.
        String query = "Select s From StudentHBaseCalendar s ";
        Query q = em.createQuery(query);
        List<StudentHBaseCalendar> students = q.getResultList();
        Assert.assertNotNull(students);
        Assert.assertEquals(1, students.size());
        int count = 0;
        for (StudentHBaseCalendar student : students)
        {
            if (student.getId().equals(getMaxValue(Calendar.class)))
            {
                Assert.assertEquals(getMaxValue(short.class), student.getAge());
                Assert.assertEquals("Kuldeep", student.getName());
                count++;
            }
        }
        Assert.assertEquals(1, count);
        em.close();
    }

    public void startCluster()
    {
        cli = new HBaseCli();
        cli.startCluster();
    }

    public void stopCluster()
    {
    }

    public void createSchema()
    {
        cli.createTable(table);
        
    }

    public void dropSchema()
    {
        cli.dropTable(table);
    }

}
