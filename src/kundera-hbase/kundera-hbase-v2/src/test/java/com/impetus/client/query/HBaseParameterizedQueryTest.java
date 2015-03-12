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
package com.impetus.client.query;

import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.Persistence;
import javax.persistence.Query;

import junit.framework.Assert;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.impetus.client.hbase.testingutil.HBaseTestingUtils;

/**
 * The Class Student test case for HBase.
 * 
 * @author Kuldeep.mishra
 * 
 */
public class HBaseParameterizedQueryTest extends StudentBaseTest
{

    /**
     * Sets the up before class.
     * 
     * @throws Exception
     *             the exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception
    {
        emf = Persistence.createEntityManagerFactory(HBASE_PU);
    }

    /**
     * Sets the up.
     * 
     * @throws Exception
     *             the exception
     */
    @Before
    public void setUp() throws Exception
    {
        em = emf.createEntityManager();
        try
        {
            persistStudents();
        }
        catch (Exception e)
        {
            Assert.fail(e.getMessage());
        }
    }

    /**
     * Parameterized select query test.
     */
    @Test
    public void parameterizedSelectQueryTest()
    {

        namedParametersSelectQueryTest();
        ordinalParametersSelectQueryTest();
    }

    /**
     * Parameterized update query test.
     */
    @Test
    public void parameterizedUpdateQueryTest()
    {

        namedParametersUpdateQueryTest();
        ordinalParametersupdateQueryTest();
    }

    /**
     * Parameterized delete query test.
     */
    @Test
    public void parameterizedDeleteQueryTest()
    {

        namedParametersDeleteQueryTest();
        ordinalParametersDeleteQueryTest();
    }

    /**
     * Named parameters select query test.
     */
    private void namedParametersSelectQueryTest()
    {

        // Query on studentId(long).
        String query = "Select s from Student s where s.studentId = :studentId";
        Query q = em.createQuery(query);
        q.setParameter("studentId", 12345677L);
        List<Student> results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(12345677L, results.get(0).getStudentId());
        Assert.assertEquals("Amresh", results.get(0).getStudentName());

        // Query on studentName(String).
        query = "Select s from Student s where s.studentName = :studentName";
        q = em.createQuery(query);
        q.setParameter("studentName", "Devender");
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals("Devender", results.get(0).getStudentName());
        Assert.assertEquals(true, results.get(0).isExceptional());

        // Assert on isExceptional(boolean(true)).
        query = "Select s from Student s where s.isExceptional =:flag";
        q = em.createQuery(query);
        q.setParameter("flag", true);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(2, results.size());
        Assert.assertEquals(true, results.get(0).isExceptional());
        Assert.assertEquals(true, results.get(1).isExceptional());

        // Assert on isExceptional(boolean(false)).
        query = "Select s from Student s where s.isExceptional = :flag";
        q = em.createQuery(query);
        q.setParameter("flag", false);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(false, results.get(0).isExceptional());

        // query on age(int).
        query = "Select s from Student s where s.age = :age";
        q = em.createQuery(query);
        q.setParameter("age", 10);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(false, results.get(0).isExceptional());
        Assert.assertEquals(10, results.get(0).getAge());

        // query on semester(char)
        query = "Select s from Student s where s.semester = :semester";
        q = em.createQuery(query);
        q.setParameter("semester", 'A');
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals('A', results.get(0).getSemester());
        Assert.assertEquals(false, results.get(0).isExceptional());
        Assert.assertEquals(10, results.get(0).getAge());

        // query on digitalSignature (byte).
        query = "Select s from Student s where s.digitalSignature = :digitalSignature";
        q = em.createQuery(query);
        q.setParameter("digitalSignature", (byte) 50);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(2, results.size());
        Assert.assertEquals((byte) 50, results.get(0).getDigitalSignature());
        Assert.assertEquals(true, results.get(0).isExceptional());
        Assert.assertEquals((byte) 50, results.get(1).getDigitalSignature());
        Assert.assertEquals(true, results.get(1).isExceptional());

        // query on cgpa (short).
        query = "Select s from Student s where s.cgpa = :cgpa";
        q = em.createQuery(query);
        q.setParameter("cgpa", (short) 8);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(3, results.size());
        Assert.assertEquals(8, results.get(0).getCgpa());
        Assert.assertEquals(false, results.get(0).isExceptional());
        Assert.assertEquals(8, results.get(1).getCgpa());
        Assert.assertEquals(true, results.get(1).isExceptional());
        Assert.assertEquals(8, results.get(2).getCgpa());
        Assert.assertEquals(true, results.get(2).isExceptional());

        // query on percentage(float)
        query = "Select s from Student s where s.percentage =:percentage";
        q = em.createQuery(query);
        q.setParameter("percentage", 61.6);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(61.6f, results.get(0).getPercentage());
        Assert.assertEquals(false, results.get(0).isExceptional());

        // query on height (double)
        query = "Select s from Student s where s.height =:height";
        q = em.createQuery(query);
        q.setParameter("height", 163.76765654);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(163.76765654, results.get(0).getHeight());
        Assert.assertEquals(false, results.get(0).isExceptional());

        // Query on enrolmentDate
        query = "Select s from Student s where s.enrolmentDate = :date";
        q = em.createQuery(query);
        q.setParameter("date", enrolmentDate);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(3, results.size());
        Assert.assertEquals(enrolmentDate, results.get(0).getEnrolmentDate());
        Assert.assertEquals(false, results.get(0).isExceptional());
        Assert.assertEquals(enrolmentDate, results.get(1).getEnrolmentDate());
        Assert.assertEquals(true, results.get(1).isExceptional());
        Assert.assertEquals(enrolmentDate, results.get(2).getEnrolmentDate());
        Assert.assertEquals(true, results.get(2).isExceptional());

        // Query on enrolmentTime
        query = "Select s from Student s where s.enrolmentTime = :time";
        q = em.createQuery(query);
        q.setParameter("time", enrolmentTime);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(3, results.size());
        Assert.assertEquals(enrolmentTime, results.get(0).getEnrolmentTime());
        Assert.assertEquals(false, results.get(0).isExceptional());
        Assert.assertEquals(enrolmentTime, results.get(1).getEnrolmentTime());
        Assert.assertEquals(true, results.get(1).isExceptional());
        Assert.assertEquals(enrolmentTime, results.get(2).getEnrolmentTime());
        Assert.assertEquals(true, results.get(2).isExceptional());

        // Query on joiningDateAndTime
        query = "Select s from Student s where s.joiningDateAndTime = :time";
        q = em.createQuery(query);
        q.setParameter("time", joiningDateAndTime);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(3, results.size());
        Assert.assertEquals(joiningDateAndTime, results.get(0).getJoiningDateAndTime());
        Assert.assertEquals(false, results.get(0).isExceptional());
        Assert.assertEquals(joiningDateAndTime, results.get(1).getJoiningDateAndTime());
        Assert.assertEquals(true, results.get(1).isExceptional());
        Assert.assertEquals(joiningDateAndTime, results.get(2).getJoiningDateAndTime());
        Assert.assertEquals(true, results.get(2).isExceptional());

        // query on yearsSpent (Integer).
        query = "Select s from Student s where s.yearsSpent = :years";
        q = em.createQuery(query);
        q.setParameter("years", new Integer(3));
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(3, results.size());
        Assert.assertEquals(new Integer(3), results.get(0).getYearsSpent());
        Assert.assertEquals(false, results.get(0).isExceptional());
        Assert.assertEquals(new Integer(3), results.get(1).getYearsSpent());
        Assert.assertEquals(true, results.get(1).isExceptional());
        Assert.assertEquals(new Integer(3), results.get(2).getYearsSpent());
        Assert.assertEquals(true, results.get(2).isExceptional());

        // query on rollNumber (Long).
        query = "Select s from Student s where s.rollNumber= :rollNumber";
        q = em.createQuery(query);
        q.setParameter("rollNumber", new Long(978423946455l));
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(3, results.size());
        Assert.assertEquals(new Long(978423946455l), results.get(0).getRollNumber());
        Assert.assertEquals(false, results.get(0).isExceptional());
        Assert.assertEquals(new Long(978423946455l), results.get(1).getRollNumber());
        Assert.assertEquals(true, results.get(1).isExceptional());
        Assert.assertEquals(new Long(978423946455l), results.get(2).getRollNumber());
        Assert.assertEquals(true, results.get(2).isExceptional());

        // query on monthlyFee (Double).
        query = "Select s from Student s where s.monthlyFee= :monthlyFee";
        q = em.createQuery(query);
        q.setParameter("monthlyFee", 135434.89);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(3, results.size());
        Assert.assertEquals(135434.89, results.get(0).getMonthlyFee());
        Assert.assertEquals(false, results.get(0).isExceptional());
        Assert.assertEquals(135434.89, results.get(1).getMonthlyFee());
        Assert.assertEquals(true, results.get(1).isExceptional());
        Assert.assertEquals(135434.89, results.get(2).getMonthlyFee());
        Assert.assertEquals(true, results.get(2).isExceptional());

        // query on sqlDate.
        query = "Select s from Student s where s.sqlDate= :date";
        q = em.createQuery(query);
        q.setParameter("date", sqlDate);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(3, results.size());
        Assert.assertEquals(sqlDate, results.get(0).getSqlDate());
        Assert.assertEquals(false, results.get(0).isExceptional());
        Assert.assertEquals(sqlDate, results.get(1).getSqlDate());
        Assert.assertEquals(true, results.get(1).isExceptional());
        Assert.assertEquals(sqlDate, results.get(2).getSqlDate());
        Assert.assertEquals(true, results.get(2).isExceptional());

        // query on sqlTimestamp.
        query = "Select s from Student s where s.sqlTimestamp= :timeStamp";
        q = em.createQuery(query);
        q.setParameter("timeStamp", sqlTimestamp);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(3, results.size());
        Assert.assertEquals(sqlTimestamp, results.get(0).getSqlTimestamp());
        Assert.assertEquals(false, results.get(0).isExceptional());
        Assert.assertEquals(sqlTimestamp, results.get(1).getSqlTimestamp());
        Assert.assertEquals(true, results.get(1).isExceptional());
        Assert.assertEquals(sqlTimestamp, results.get(2).getSqlTimestamp());
        Assert.assertEquals(true, results.get(2).isExceptional());

        // query on sqlTime.
        query = "Select s from Student s where s.sqlTime=:time";
        q = em.createQuery(query);
        q.setParameter("time", sqlTime);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(3, results.size());
        Assert.assertEquals(sqlTime, results.get(0).getSqlTime());
        Assert.assertEquals(false, results.get(0).isExceptional());
        Assert.assertEquals(sqlTime, results.get(1).getSqlTime());
        Assert.assertEquals(true, results.get(1).isExceptional());
        Assert.assertEquals(sqlTime, results.get(2).getSqlTime());
        Assert.assertEquals(true, results.get(2).isExceptional());

        // query on big integer.
        query = "Select s from Student s where s.bigInteger = :bigInt";
        q = em.createQuery(query);
        q.setParameter("bigInt", bigInteger);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(3, results.size());
        Assert.assertEquals(bigInteger, results.get(0).getBigInteger());
        Assert.assertEquals(false, results.get(0).isExceptional());
        Assert.assertEquals(bigInteger, results.get(1).getBigInteger());
        Assert.assertEquals(true, results.get(1).isExceptional());
        Assert.assertEquals(bigInteger, results.get(2).getBigInteger());
        Assert.assertEquals(true, results.get(2).isExceptional());

        // query on big integer.
        query = "Select s from Student s where s.bigDecimal = :bigDeci";
        q = em.createQuery(query);
        q.setParameter("bigDeci", bigDecimal);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(3, results.size());
        Assert.assertEquals(bigDecimal, results.get(0).getBigDecimal());
        Assert.assertEquals(false, results.get(0).isExceptional());
        Assert.assertEquals(bigDecimal, results.get(1).getBigDecimal());
        Assert.assertEquals(true, results.get(1).isExceptional());
        Assert.assertEquals(bigDecimal, results.get(2).getBigDecimal());
        Assert.assertEquals(true, results.get(2).isExceptional());

        // query on calendar.
        query = "Select s from Student s where s.calendar = :cal";
        q = em.createQuery(query);
        q.setParameter("cal", calendar);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(3, results.size());
        Assert.assertEquals(calendar, results.get(0).getCalendar());
        Assert.assertEquals(false, results.get(0).isExceptional());
        Assert.assertEquals(calendar, results.get(1).getCalendar());
        Assert.assertEquals(true, results.get(1).isExceptional());
        Assert.assertEquals(calendar, results.get(2).getCalendar());
        Assert.assertEquals(true, results.get(2).isExceptional());

        // query on cpga and digitalSignature.
        query = "Select s from Student s where s.cgpa = :cgpa and s.digitalSignature >= :ds1 and s.digitalSignature <= :ds2";
        q = em.createQuery(query);
        q.setParameter("cgpa", (short) 8);
        q.setParameter("ds1", (byte) 5);
        q.setParameter("ds2", (byte) 50);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(3, results.size());
        Assert.assertEquals((short) 8, results.get(0).getCgpa());
        Assert.assertEquals((byte) 5, results.get(0).getDigitalSignature());
        Assert.assertEquals((short) 8, results.get(1).getCgpa());
        Assert.assertEquals((byte) 50, results.get(1).getDigitalSignature());
        Assert.assertEquals((short) 8, results.get(2).getCgpa());
        Assert.assertEquals((byte) 50, results.get(2).getDigitalSignature());

        // query on percentage and height.
        query = "Select s from Student s where s.percentage >= :percent1 and s.percentage <= :percent2 and s.height = :height";
        q = em.createQuery(query);
        q.setParameter("height", 163.76765654);
        q.setParameter("percent1", 61.6);
        q.setParameter("percent2", 69.3);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(163.76765654, results.get(0).getHeight());
        Assert.assertEquals(61.6f, results.get(0).getPercentage());

        // query on cpga and semester.
        query = "Select s from Student s where s.cgpa = :cgpa and s.semester >= :sem1 and s.semester < :sem2";
        q = em.createQuery(query);
        q.setParameter("cgpa", (short) 8);
        q.setParameter("sem1", 'A');
        q.setParameter("sem2", 'C');
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(2, results.size());
        Assert.assertEquals((short) 8, results.get(0).getCgpa());
        Assert.assertEquals('A', results.get(0).getSemester());
        Assert.assertEquals((short) 8, results.get(1).getCgpa());
        Assert.assertEquals('B', results.get(1).getSemester());

        // query on invalid cpga and studentId.
        query = "Select s from Student s where s.cgpa = :cgpa and s.studentId >= :id1 and s.studentId <= :id2";
        q = em.createQuery(query);
        q.setParameter("cgpa", (short) 2);
        q.setParameter("id1", 12345687L);
        q.setParameter("id2", 12345689L);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertTrue(results.isEmpty());

    }

    /**
     * Ordinal parameters select query test.
     */
    private void ordinalParametersSelectQueryTest()
    {

        // Query on studentId(long).
        String query = "Select s from Student s where s.studentId = ?1";
        Query q = em.createQuery(query);
        q.setParameter(1, 12345677L);
        List<Student> results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(12345677L, results.get(0).getStudentId());
        Assert.assertEquals("Amresh", results.get(0).getStudentName());

        // Query on studentName(String).
        query = "Select s from Student s where s.studentName = ?1";
        q = em.createQuery(query);
        q.setParameter(1, "Devender");
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals("Devender", results.get(0).getStudentName());
        Assert.assertEquals(true, results.get(0).isExceptional());

        // Assert on isExceptional(boolean(true)).
        query = "Select s from Student s where s.isExceptional =?1";
        q = em.createQuery(query);
        q.setParameter(1, true);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(2, results.size());
        Assert.assertEquals(true, results.get(0).isExceptional());
        Assert.assertEquals(true, results.get(1).isExceptional());

        // Assert on isExceptional(boolean(false)).
        query = "Select s from Student s where s.isExceptional =?1";
        q = em.createQuery(query);
        q.setParameter(1, false);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(false, results.get(0).isExceptional());

        // query on age(int).
        query = "Select s from Student s where s.age =?1";
        q = em.createQuery(query);
        q.setParameter(1, 10);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(false, results.get(0).isExceptional());
        Assert.assertEquals(10, results.get(0).getAge());

        // query on semester(char)
        query = "Select s from Student s where s.semester =?1";
        q = em.createQuery(query);
        q.setParameter(1, 'A');
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals('A', results.get(0).getSemester());
        Assert.assertEquals(false, results.get(0).isExceptional());
        Assert.assertEquals(10, results.get(0).getAge());

        // query on digitalSignature (byte).
        query = "Select s from Student s where s.digitalSignature =?1";
        q = em.createQuery(query);
        q.setParameter(1, (byte) 50);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(2, results.size());
        Assert.assertEquals((byte) 50, results.get(0).getDigitalSignature());
        Assert.assertEquals(true, results.get(0).isExceptional());
        Assert.assertEquals((byte) 50, results.get(1).getDigitalSignature());
        Assert.assertEquals(true, results.get(1).isExceptional());

        // query on cgpa (short).
        query = "Select s from Student s where s.cgpa =?1";
        q = em.createQuery(query);
        q.setParameter(1, (short) 8);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(3, results.size());
        Assert.assertEquals(8, results.get(0).getCgpa());
        Assert.assertEquals(false, results.get(0).isExceptional());
        Assert.assertEquals(8, results.get(1).getCgpa());
        Assert.assertEquals(true, results.get(1).isExceptional());
        Assert.assertEquals(8, results.get(2).getCgpa());
        Assert.assertEquals(true, results.get(2).isExceptional());

        // query on percentage(float)
        query = "Select s from Student s where s.percentage =?1";
        q = em.createQuery(query);
        q.setParameter(1, 61.6);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(61.6f, results.get(0).getPercentage());
        Assert.assertEquals(false, results.get(0).isExceptional());

        // query on height (double)
        query = "Select s from Student s where s.height =?1";
        q = em.createQuery(query);
        q.setParameter(1, 163.76765654);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(163.76765654, results.get(0).getHeight());
        Assert.assertEquals(false, results.get(0).isExceptional());

        // Query on enrolmentDate
        query = "Select s from Student s where s.enrolmentDate = ?1";
        q = em.createQuery(query);
        q.setParameter(1, enrolmentDate);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(3, results.size());
        Assert.assertEquals(enrolmentDate, results.get(0).getEnrolmentDate());
        Assert.assertEquals(false, results.get(0).isExceptional());
        Assert.assertEquals(enrolmentDate, results.get(1).getEnrolmentDate());
        Assert.assertEquals(true, results.get(1).isExceptional());
        Assert.assertEquals(enrolmentDate, results.get(2).getEnrolmentDate());
        Assert.assertEquals(true, results.get(2).isExceptional());

        // Query on enrolmentTime
        query = "Select s from Student s where s.enrolmentTime = ?1";
        q = em.createQuery(query);
        q.setParameter(1, enrolmentTime);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(3, results.size());
        Assert.assertEquals(enrolmentTime, results.get(0).getEnrolmentTime());
        Assert.assertEquals(false, results.get(0).isExceptional());
        Assert.assertEquals(enrolmentTime, results.get(1).getEnrolmentTime());
        Assert.assertEquals(true, results.get(1).isExceptional());
        Assert.assertEquals(enrolmentTime, results.get(2).getEnrolmentTime());
        Assert.assertEquals(true, results.get(2).isExceptional());

        // Query on joiningDateAndTime
        query = "Select s from Student s where s.joiningDateAndTime = ?1";
        q = em.createQuery(query);
        q.setParameter(1, joiningDateAndTime);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(3, results.size());
        Assert.assertEquals(joiningDateAndTime, results.get(0).getJoiningDateAndTime());
        Assert.assertEquals(false, results.get(0).isExceptional());
        Assert.assertEquals(joiningDateAndTime, results.get(1).getJoiningDateAndTime());
        Assert.assertEquals(true, results.get(1).isExceptional());
        Assert.assertEquals(joiningDateAndTime, results.get(2).getJoiningDateAndTime());
        Assert.assertEquals(true, results.get(2).isExceptional());

        // query on yearsSpent (Integer).
        query = "Select s from Student s where s.yearsSpent =?1";
        q = em.createQuery(query);
        q.setParameter(1, new Integer(3));
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(3, results.size());
        Assert.assertEquals(new Integer(3), results.get(0).getYearsSpent());
        Assert.assertEquals(false, results.get(0).isExceptional());
        Assert.assertEquals(new Integer(3), results.get(1).getYearsSpent());
        Assert.assertEquals(true, results.get(1).isExceptional());
        Assert.assertEquals(new Integer(3), results.get(2).getYearsSpent());
        Assert.assertEquals(true, results.get(2).isExceptional());

        // query on rollNumber (Long).
        query = "Select s from Student s where s.rollNumber=?1";
        q = em.createQuery(query);
        q.setParameter(1, new Long(978423946455l));
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(3, results.size());
        Assert.assertEquals(new Long(978423946455l), results.get(0).getRollNumber());
        Assert.assertEquals(false, results.get(0).isExceptional());
        Assert.assertEquals(new Long(978423946455l), results.get(1).getRollNumber());
        Assert.assertEquals(true, results.get(1).isExceptional());
        Assert.assertEquals(new Long(978423946455l), results.get(2).getRollNumber());
        Assert.assertEquals(true, results.get(2).isExceptional());

        // query on monthlyFee (Double).
        query = "Select s from Student s where s.monthlyFee=?1";
        q = em.createQuery(query);
        q.setParameter(1, 135434.89);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(3, results.size());
        Assert.assertEquals(135434.89, results.get(0).getMonthlyFee());
        Assert.assertEquals(false, results.get(0).isExceptional());
        Assert.assertEquals(135434.89, results.get(1).getMonthlyFee());
        Assert.assertEquals(true, results.get(1).isExceptional());
        Assert.assertEquals(135434.89, results.get(2).getMonthlyFee());
        Assert.assertEquals(true, results.get(2).isExceptional());

        // query on sqlDate.
        query = "Select s from Student s where s.sqlDate=?1";
        q = em.createQuery(query);
        q.setParameter(1, sqlDate);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(3, results.size());
        Assert.assertEquals(sqlDate, results.get(0).getSqlDate());
        Assert.assertEquals(false, results.get(0).isExceptional());
        Assert.assertEquals(sqlDate, results.get(1).getSqlDate());
        Assert.assertEquals(true, results.get(1).isExceptional());
        Assert.assertEquals(sqlDate, results.get(2).getSqlDate());
        Assert.assertEquals(true, results.get(2).isExceptional());

        // query on sqlTimestamp.
        query = "Select s from Student s where s.sqlTimestamp=?1";
        q = em.createQuery(query);
        q.setParameter(1, sqlTimestamp);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(3, results.size());
        Assert.assertEquals(sqlTimestamp, results.get(0).getSqlTimestamp());
        Assert.assertEquals(false, results.get(0).isExceptional());
        Assert.assertEquals(sqlTimestamp, results.get(1).getSqlTimestamp());
        Assert.assertEquals(true, results.get(1).isExceptional());
        Assert.assertEquals(sqlTimestamp, results.get(2).getSqlTimestamp());
        Assert.assertEquals(true, results.get(2).isExceptional());

        // query on sqlTime.
        query = "Select s from Student s where s.sqlTime=?1";
        q = em.createQuery(query);
        q.setParameter(1, sqlTime);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(3, results.size());
        Assert.assertEquals(sqlTime, results.get(0).getSqlTime());
        Assert.assertEquals(false, results.get(0).isExceptional());
        Assert.assertEquals(sqlTime, results.get(1).getSqlTime());
        Assert.assertEquals(true, results.get(1).isExceptional());
        Assert.assertEquals(sqlTime, results.get(2).getSqlTime());
        Assert.assertEquals(true, results.get(2).isExceptional());

        // query on big integer.
        query = "Select s from Student s where s.bigInteger =?1";
        q = em.createQuery(query);
        q.setParameter(1, bigInteger);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(3, results.size());
        Assert.assertEquals(bigInteger, results.get(0).getBigInteger());
        Assert.assertEquals(false, results.get(0).isExceptional());
        Assert.assertEquals(bigInteger, results.get(1).getBigInteger());
        Assert.assertEquals(true, results.get(1).isExceptional());
        Assert.assertEquals(bigInteger, results.get(2).getBigInteger());
        Assert.assertEquals(true, results.get(2).isExceptional());

        // query on big integer.
        query = "Select s from Student s where s.bigDecimal =?1";
        q = em.createQuery(query);
        q.setParameter(1, bigDecimal);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(3, results.size());
        Assert.assertEquals(bigDecimal, results.get(0).getBigDecimal());
        Assert.assertEquals(false, results.get(0).isExceptional());
        Assert.assertEquals(bigDecimal, results.get(1).getBigDecimal());
        Assert.assertEquals(true, results.get(1).isExceptional());
        Assert.assertEquals(bigDecimal, results.get(2).getBigDecimal());
        Assert.assertEquals(true, results.get(2).isExceptional());

        // query on calendar.
        query = "Select s from Student s where s.calendar =?1";
        q = em.createQuery(query);
        q.setParameter(1, calendar);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(3, results.size());
        Assert.assertEquals(calendar, results.get(0).getCalendar());
        Assert.assertEquals(false, results.get(0).isExceptional());
        Assert.assertEquals(calendar, results.get(1).getCalendar());
        Assert.assertEquals(true, results.get(1).isExceptional());
        Assert.assertEquals(calendar, results.get(2).getCalendar());
        Assert.assertEquals(true, results.get(2).isExceptional());

        // query on cpga and digitalSignature.
        query = "Select s from Student s where s.cgpa =?1 and s.digitalSignature >= ?2 and s.digitalSignature <= ?3";
        q = em.createQuery(query);
        q.setParameter(1, (short) 8);
        q.setParameter(2, (byte) 5);
        q.setParameter(3, (byte) 50);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(3, results.size());
        Assert.assertEquals((short) 8, results.get(0).getCgpa());
        Assert.assertEquals((byte) 5, results.get(0).getDigitalSignature());
        Assert.assertEquals((short) 8, results.get(1).getCgpa());
        Assert.assertEquals((byte) 50, results.get(1).getDigitalSignature());
        Assert.assertEquals((short) 8, results.get(2).getCgpa());
        Assert.assertEquals((byte) 50, results.get(2).getDigitalSignature());

        // query on cpga and digitalSignature.
        query = "Select s from Student s where s.digitalSignature >= ?2 and s.digitalSignature <= ?3 and s.cgpa =?1";
        q = em.createQuery(query);
        q.setParameter(1, (short) 8);
        q.setParameter(2, (byte) 5);
        q.setParameter(3, (byte) 50);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(3, results.size());
        Assert.assertEquals((short) 8, results.get(0).getCgpa());
        Assert.assertEquals((byte) 5, results.get(0).getDigitalSignature());
        Assert.assertEquals((short) 8, results.get(1).getCgpa());
        Assert.assertEquals((byte) 50, results.get(1).getDigitalSignature());
        Assert.assertEquals((short) 8, results.get(2).getCgpa());
        Assert.assertEquals((byte) 50, results.get(2).getDigitalSignature());

        // query on percentage and height.
        query = "Select s from Student s where s.percentage >= ?2 and s.percentage <= ?3 and s.height =?1";
        q = em.createQuery(query);
        q.setParameter(1, 163.76765654);
        q.setParameter(2, 61.6);
        q.setParameter(3, 69.3);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(163.76765654, results.get(0).getHeight());
        Assert.assertEquals(61.6f, results.get(0).getPercentage());

        // query on cpga and semester.
        query = "Select s from Student s where s.cgpa =?1 and s.semester >= ?2 and s.semester < ?3";
        q = em.createQuery(query);
        q.setParameter(1, (short) 8);
        q.setParameter(2, 'A');
        q.setParameter(3, 'C');
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(2, results.size());
        Assert.assertEquals((short) 8, results.get(0).getCgpa());
        Assert.assertEquals('A', results.get(0).getSemester());
        Assert.assertEquals((short) 8, results.get(1).getCgpa());
        Assert.assertEquals('B', results.get(1).getSemester());

        // query on invalid cpga and studentId.
        query = "Select s from Student s where s.cgpa =?1 and s.studentId >= ?2 and s.studentId <= ?3";
        q = em.createQuery(query);
        q.setParameter(1, (short) 2);
        q.setParameter(2, 12345677L);
        q.setParameter(3, 12345679L);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertTrue(results.isEmpty());
    }

    /**
     * Named parameters update query test.
     */
    private void namedParametersUpdateQueryTest()
    {
        persistStudents();
        Query q = em.createQuery("update Student s set s.studentName = :newName where s.studentName = :name");
        q.setParameter("newName", "NewAmresh");
        q.setParameter("name", "Amresh");
        int results = q.executeUpdate();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results);
        Student s1 = em.find(Student.class, 12345677L);
        Assert.assertEquals("NewAmresh", s1.getStudentName());

        persistStudents();
        q = em.createQuery("update Student s set s.studentName = :newName,s.semester=:newSemester where s.isExceptional = :flag and s.semester= :semester");
        q.setParameter("newName", "NewDevender");
        q.setParameter("newSemester", 'A');
        q.setParameter("flag", true);
        q.setParameter("semester", 'B');
        results = q.executeUpdate();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results);
        s1 = em.find(Student.class, 12345678L);
        Assert.assertEquals("NewDevender", s1.getStudentName());
        Assert.assertEquals('A', s1.getSemester());

        persistStudents();
        q = em.createQuery("update Student s set s.studentName = :newName,s.semester=:newSemester where s.isExceptional = :flag or s.semester= :semester");
        q.setParameter("newName", "NewDevender");
        q.setParameter("newSemester", 'A');
        q.setParameter("flag", true);
        q.setParameter("semester", 'B');
        results = q.executeUpdate();
        Assert.assertNotNull(results);
        Assert.assertEquals(2, results);
        s1 = em.find(Student.class, 12345678L);
        Assert.assertEquals("NewDevender", s1.getStudentName());
        Assert.assertEquals('A', s1.getSemester());

        Student s2 = em.find(Student.class, 12345679L);
        Assert.assertEquals("NewDevender", s2.getStudentName());
        Assert.assertEquals('A', s2.getSemester());

    }

    /**
     * Ordinal parametersupdate query test.
     */
    private void ordinalParametersupdateQueryTest()
    {
        persistStudents();
        Query q = em.createQuery("update Student s set s.studentName = ?1 where s.studentName = ?2");
        q.setParameter(1, "NewAmresh");
        q.setParameter(2, "Amresh");
        int results = q.executeUpdate();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results);
        Student s1 = em.find(Student.class, 12345677L);
        Assert.assertEquals("NewAmresh", s1.getStudentName());

        persistStudents();
        q = em.createQuery("update Student s set s.studentName = ?1,s.semester= ?2 where s.isExceptional = ?3 and s.semester= ?4");
        q.setParameter(1, "NewDevender");
        q.setParameter(2, 'A');
        q.setParameter(3, true);
        q.setParameter(4, 'B');
        results = q.executeUpdate();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results);
        s1 = em.find(Student.class, 12345678L);
        Assert.assertEquals("NewDevender", s1.getStudentName());
        Assert.assertEquals('A', s1.getSemester());

        persistStudents();
        q = em.createQuery("update Student s set s.studentName = ?1,s.semester= ?2 where s.isExceptional = ?3 or s.semester= ?4");
        q.setParameter(1, "NewDevender");
        q.setParameter(2, 'A');
        q.setParameter(3, true);
        q.setParameter(4, 'B');
        results = q.executeUpdate();
        Assert.assertNotNull(results);
        Assert.assertEquals(2, results);
        s1 = em.find(Student.class, 12345678L);
        Assert.assertEquals("NewDevender", s1.getStudentName());
        Assert.assertEquals('A', s1.getSemester());

        Student s2 = em.find(Student.class, 12345679L);
        Assert.assertEquals("NewDevender", s2.getStudentName());
        Assert.assertEquals('A', s2.getSemester());

    }

    /**
     * Named parameters delete query test.
     */
    private void namedParametersDeleteQueryTest()
    {
        persistStudents();
        Student s1 = em.find(Student.class, 12345677L);
        Assert.assertNotNull(s1);
        Query q = em.createQuery("delete from Student s where s.studentName = :name");
        q.setParameter("name", "Amresh");
        int results = q.executeUpdate();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results);
        em.clear();
        s1 = em.find(Student.class, 12345677L);
        Assert.assertNull(s1);

        persistStudents();
        s1 = em.find(Student.class, 12345678L);
        Assert.assertNotNull(s1);
        q = em.createQuery("delete from Student s where s.isExceptional = :flag and s.semester= :semester");
        q.setParameter("flag", true);
        q.setParameter("semester", 'B');
        results = q.executeUpdate();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results);
        em.clear();
        s1 = em.find(Student.class, 12345678L);
        Assert.assertNull(s1);

        persistStudents();
        s1 = em.find(Student.class, 12345678L);
        Assert.assertNotNull(s1);
        Student s2 = em.find(Student.class, 12345679L);
        Assert.assertNotNull(s2);
        q = em.createQuery("delete from Student s where s.isExceptional = :flag or s.semester= :semester");
        q.setParameter("flag", true);
        q.setParameter("semester", 'B');
        results = q.executeUpdate();
        Assert.assertNotNull(results);
        Assert.assertEquals(2, results);
        em.clear();
        s1 = em.find(Student.class, 12345678L);
        Assert.assertNull(s1);
        em.clear();
        s2 = em.find(Student.class, 12345679L);
        Assert.assertNull(s2);
    }

    /**
     * Ordinal parameters delete query test.
     */
    private void ordinalParametersDeleteQueryTest()
    {
        persistStudents();
        Student s1 = em.find(Student.class, 12345677L);
        Assert.assertNotNull(s1);
        Query q = em.createQuery("delete from Student s where s.studentName = ?1");
        q.setParameter(1, "Amresh");
        int results = q.executeUpdate();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results);
        em.clear();
        s1 = em.find(Student.class, 12345677L);
        Assert.assertNull(s1);

        persistStudents();
        s1 = em.find(Student.class, 12345678L);
        Assert.assertNotNull(s1);
        q = em.createQuery("delete from Student s where s.isExceptional = ?1 and s.semester= ?2");
        q.setParameter(1, true);
        q.setParameter(2, 'B');
        results = q.executeUpdate();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results);
        em.clear();
        s1 = em.find(Student.class, 12345678L);
        Assert.assertNull(s1);

        persistStudents();
        s1 = em.find(Student.class, 12345678L);
        Assert.assertNotNull(s1);
        Student s2 = em.find(Student.class, 12345679L);
        Assert.assertNotNull(s2);
        q = em.createQuery("delete from Student s where s.isExceptional = ?1 or s.semester= ?2");
        q.setParameter(1, true);
        q.setParameter(2, 'B');
        results = q.executeUpdate();
        Assert.assertNotNull(results);
        Assert.assertEquals(2, results);
        em.clear();
        s1 = em.find(Student.class, 12345678L);
        Assert.assertNull(s1);
        em.clear();
        s2 = em.find(Student.class, 12345679L);
        Assert.assertNull(s2);
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
        EntityManager em = emf.createEntityManager();
        deleteStudents();
        em.close();
    }

    /**
     * Tear down after class.
     * 
     * @throws Exception
     *             the exception
     */
    @AfterClass
    public static void tearDownAfterClass() throws Exception
    {
        emf.close();
        emf = null;
        HBaseTestingUtils.dropSchema(SCHEMA);
    }

}
