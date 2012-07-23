/**
 * 
 */
package com.impetus.client.crud.datatypes;

import java.util.List;

import javax.persistence.Query;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * The Class StudentDaoTest.
 * 
 * @author Vivek Mishra
 */
public class StudentMongoTest extends StudentBase<StudentMongo>
{
    String persistenceUnit = "mongoTest";

    /**
     * Sets the up.
     * 
     * @throws Exception
     *             the exception
     */
    @Before
    public void setUp() throws Exception
    {
        setupInternal(persistenceUnit);
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
         teardownInternal(persistenceUnit);
    }

    @SuppressWarnings("deprecation")
    @Test
    public void executeTests()
    {
        onInsert();
        onMerge();
    }

    /**
     * Test method for.
     * 
     * @throws InstantiationException
     *             the instantiation exception
     * @throws IllegalAccessException
     *             the illegal access exception
     *             {@link com.impetus.kundera.examples.student.StudentDao#saveStudent(com.impetus.kundera.examples.crud.datatype.entities.StudentMongo)}
     *             .
     */

    public void onInsert()
    {
        try
        {
            onInsert(new StudentMongo());
        }
        catch (InstantiationException e)
        {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
        catch (IllegalAccessException e)
        {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }

        // find by id.
        StudentEntityDef s = em.find(StudentMongo.class, studentId1);
        assertOnDataTypes((StudentMongo) s);

        // // find by name.
        assertFindByName(em, "StudentMongo", StudentMongo.class, "Amresh", "studentName");

        // find by name and age.
        assertFindByNameAndAge(em, "StudentMongo", StudentMongo.class, "Amresh", "10", "studentName");

        // find by name, age clause
        assertFindByNameAndAgeGTAndLT(em, "StudentMongo", StudentMongo.class, "Amresh", "10", "20", "studentName");
        //
        // // find by between clause
        assertFindByNameAndAgeBetween(em, "StudentMongo", StudentMongo.class, "Amresh", "10", "15", "studentName");

        // find by Range.
        assertFindByRange(em, "StudentMongo", StudentMongo.class, "12345677", "12345678", "studentId");

        // find by without where clause.
        assertFindWithoutWhereClause(em, "StudentMongo", StudentMongo.class);
    }

    /**
     * On merge.
     */
    public void onMerge()
    {
        em.persist(prepareData((Long) studentId1, 78575785897L, "Amresh", true, 10, 'C', (byte) 5, (short) 8,
                (float) 69.6, 163.76765654, enrolmentDate, enrolmentTime, joiningDateAndTime, new Integer(3), new Long(
                        978423946455l), 135434.89, newSqlDate, sqlTime, sqlTimestamp, bigDecimal, bigInteger, calendar,
                new StudentMongo()));
        StudentMongo s = em.find(StudentMongo.class, studentId1);
        Assert.assertNotNull(s);
        Assert.assertEquals("Amresh", s.getStudentName());
        // modify record.
        s.setStudentName("NewAmresh");
        em.merge(s);
//        emf.close();
        Query q = em.createQuery("Select p from StudentMongo p where p.studentName = NewAmresh");
        try
        {
            List<StudentMongo> results = q.getResultList();
            Assert.assertNotNull(results);
            Assert.assertEquals(1, results.size());
        }
        catch (Exception e)
        {
            Assert.fail("Failure onMerge test");
        }
    }

    @Override
    void startServer()
    {
    }

    @Override
    void stopServer()
    {
    }

    @Override
    void createSchema()
    {
    }

    @Override
    void deleteSchema()
    {
         em.remove(em.find(StudentMongo.class, studentId1));
         em.remove(em.find(StudentMongo.class, studentId2));
         em.remove(em.find(StudentMongo.class, studentId3));
    }

}