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
package com.impetus.client.crud.datatypes;

import java.math.BigInteger;
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
        
        

        // Query on Date.
        String query = "Select s from StudentMongo s where s.enrolmentDate =:enrolmentDate";
        Query q = em.createQuery(query);
        q.setParameter("enrolmentDate", enrolmentDate);
        List<StudentMongo> results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(3, results.size());

        // Query on long.
        /* String */query = "Select s from StudentMongo s where s.uniqueId =?1";
        /* Query */q = em.createQuery(query);
        q.setParameter(1, 78575785897L);

        /* List<StudentMongo> */results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(78575785897L, results.get(0).getUniqueId());

        // Assert on boolean.
        query = "Select s from StudentMongo s where s.isExceptional =?1";
        q = em.createQuery(query);
        q.setParameter(1, true);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(2, results.size());
        Assert.assertEquals(true, results.get(0).isExceptional());

        // with false.
        query = "Select s from StudentMongo s where s.isExceptional =?1";
        q = em.createQuery(query);
        q.setParameter(1, false);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(false, results.get(0).isExceptional());

        // query on int.

        query = "Select s from StudentMongo s where s.age =?1";
        q = em.createQuery(query);
        q.setParameter(1, 10);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(false, results.get(0).isExceptional());
        Assert.assertEquals(10, results.get(0).getAge());

        // query on char (semester)

        query = "Select s from StudentMongo s where s.semester =?1";
        q = em.createQuery(query);
        q.setParameter(1, 'A');
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(false, results.get(0).isExceptional());
        Assert.assertEquals(10, results.get(0).getAge());
        Assert.assertEquals('A', results.get(0).getSemester());

        // // query on byte (digitalSignature)
        //
        // query =
        // "Select s from StudentMongo s where s.digitalSignature =?1";
        // q = em.createQuery(query);
        // q.setParameter(1, 50);
        // results = q.getResultList();
        // Assert.assertNotNull(results);
        // Assert.assertEquals(2,results.size());
        // Assert.assertEquals(true, results.get(0).isExceptional());
        // Assert.assertEquals(50, results.get(0).getDigitalSignature());
        // Assert.assertEquals(50, results.get(1).getDigitalSignature());
        //

        // query on float (percentage)
        query = "Select s from StudentMongo s where s.percentage =?1";
        q = em.createQuery(query);
        q.setParameter(1, 61.6f);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(false, results.get(0).isExceptional());
        Assert.assertEquals(61.6f, results.get(0).getPercentage());

        // query on double (height)

        query = "Select s from StudentMongo s where s.height =?1";
        q = em.createQuery(query);
        q.setParameter(1, 163.76765654);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(false, results.get(0).isExceptional());
        Assert.assertEquals(163.76765654, results.get(0).getHeight());

        // query on big integer.
        query = "Select s from StudentMongo s where s.bigInteger =?1";
        q = em.createQuery(query);
        q.setParameter(1, bigInteger);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(false, results.get(0).isExceptional());
        Assert.assertEquals(163.76765654, results.get(0).getHeight());

        // invalid.
        q.setParameter(1, new BigInteger("1234567823"));
        results = q.getResultList();
        Assert.assertNull(results);
        
        
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
        // emf.close();
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