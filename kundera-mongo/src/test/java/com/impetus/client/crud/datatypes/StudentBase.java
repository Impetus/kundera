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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Calendar;
import java.util.Date;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import sun.security.util.BigInt;

import junit.framework.Assert;

import com.impetus.client.crud.BaseTest;

/**
 * The Class StudentBase.
 * 
 * @param <E>
 *            the element type
 */
public abstract class StudentBase<E extends StudentEntityDef> extends BaseTest
{
    public static final boolean RUN_IN_EMBEDDED_MODE = true;

    public static final boolean AUTO_MANAGE_SCHEMA = true;

    /** The emf. */
    protected EntityManagerFactory emf;

    /** The em. */
    protected EntityManager em;

    // protected String persistenceUnit =
    // /*"twissandra,twibase,twingo,picmysql"*/null;

    /** The student id1. */
    protected Object studentId1;

    /** The student id2. */
    protected Object studentId2;

    /** The student id3. */
    protected Object studentId3;

    /** The enrolment date. */
    protected Date enrolmentDate = new Date();

    /** The joining date and time. */
    protected Date joiningDateAndTime = new Date();

    /** The date. */
    protected long date = new Date().getTime();

    /** The new sql date. */
    protected java.sql.Date newSqlDate = new java.sql.Date(date);

    /** The enrolment time. */
    protected Date enrolmentTime = new Date();

    /** The sql time. */
    protected java.sql.Time sqlTime = new java.sql.Time(date);

    /** The sql timestamp. */
    protected java.sql.Timestamp sqlTimestamp = new java.sql.Timestamp(date);

    /** The big decimal. */
    protected BigDecimal bigDecimal = new BigDecimal(123456789);

    /** The big integer. */
    protected BigInteger bigInteger = new BigInteger("123456789");

    /** The number of students. */
    protected int numberOfStudents = 1000;

    /** The calendar. */
    protected Calendar calendar = Calendar.getInstance();

    /** The dao. */
    // StudentDao dao;

    /**
     * Sets the up internal.
     * 
     * @param persisntenceUnit
     *            the new up internal
     */
    protected void setupInternal(String persisntenceUnit)
    {
        // dao = new StudentDao(persistenceUnit);

        if (RUN_IN_EMBEDDED_MODE)
        {
            startServer();
        }

        if (AUTO_MANAGE_SCHEMA)
        {
            createSchema();
        }

        studentId1 = new Long(12345677);
        studentId2 = new Long(12345678);
        studentId3 = new Long(12345679);

        emf = Persistence.createEntityManagerFactory(persisntenceUnit);
        em = emf.createEntityManager();
    }

    /**
     * Sets the up internal.
     * 
     * @param persistenceUnit
     *            the new up internal
     */
    protected void teardownInternal(String persistenceUnit)
    {

        if (RUN_IN_EMBEDDED_MODE)
        {
            stopServer();
        }

        if (AUTO_MANAGE_SCHEMA)
        {
            deleteSchema();
        }

        if (emf != null)
        {
            emf.close();
        }
    }

    /**
     * on insert.
     * 
     * @param instance
     *            the instance
     * @throws InstantiationException
     *             the instantiation exception
     * @throws IllegalAccessException
     *             the illegal access exception
     */
    protected void onInsert(E instance) throws InstantiationException, IllegalAccessException
    {

        em.persist(prepareData((Long) studentId1, 78575785897L, "Amresh", false, 10, 'A', (byte) 5, (short) 8,
                (float) 61.6, 163.76765654, enrolmentDate, enrolmentTime, joiningDateAndTime, new Integer(3), new Long(
                        978423946455l), 135434.89, newSqlDate, sqlTime, sqlTimestamp, bigDecimal, bigInteger, calendar,
                ((E) instance.getClass().newInstance())));

        em.persist(prepareData((Long) studentId2, 78575785898L, "Amresh", true, 20, 'C', (byte) 5, (short) 8,
                (float) 69.6, 163.76765655, enrolmentDate, enrolmentTime, joiningDateAndTime, new Integer(3), new Long(
                        978423946455l), 135434.89, newSqlDate, sqlTime, sqlTimestamp, bigDecimal, new BigInteger("1234567810"), calendar,
                ((E) instance.getClass().newInstance())));

        em.persist(prepareData((Long) studentId3, 78575785899L, "Amresh", true, 15, 'C', (byte) 5, (short) 8,
                (float) 69.6, 163.76765656, enrolmentDate, enrolmentTime, joiningDateAndTime, new Integer(3), new Long(
                        978423946455l), 135434.89, newSqlDate, sqlTime, sqlTimestamp, bigDecimal, new BigInteger("1234567811"), calendar,
                ((E) instance.getClass().newInstance())));

    }

    /**
     * Prepare data.
     * 
     * @param studentId
     *            the student id
     * @param uniqueId
     *            the unique id
     * @param studentName
     *            the student name
     * @param isExceptional
     *            the is exceptional
     * @param age
     *            the age
     * @param semester
     *            the semester
     * @param digitalSignature
     *            the digital signature
     * @param cgpa
     *            the cgpa
     * @param percentage
     *            the percentage
     * @param height
     *            the height
     * @param enrolmentDate
     *            the enrolment date
     * @param enrolmentTime
     *            the enrolment time
     * @param joiningDateAndTime
     *            the joining date and time
     * @param yearsSpent
     *            the years spent
     * @param rollNumber
     *            the roll number
     * @param monthlyFee
     *            the monthly fee
     * @param newSqlDate
     *            the new sql date
     * @param sqlTime
     *            the sql time
     * @param sqlTimestamp
     *            the sql timestamp
     * @param bigDecimal
     *            the big decimal
     * @param bigInteger
     *            the big integer
     * @param calendar
     *            the calendar
     * @param o
     *            the o
     * @return the person
     */
    protected E prepareData(long studentId, long uniqueId, String studentName, boolean isExceptional, int age,
            char semester, byte digitalSignature, short cgpa, float percentage, double height,
            java.util.Date enrolmentDate, java.util.Date enrolmentTime, java.util.Date joiningDateAndTime,
            Integer yearsSpent, Long rollNumber, Double monthlyFee, java.sql.Date newSqlDate, java.sql.Time sqlTime,
            java.sql.Timestamp sqlTimestamp, BigDecimal bigDecimal, BigInteger bigInteger, Calendar calendar, E o)
    {
        o.setStudentId((Long) studentId);
        o.setUniqueId(uniqueId);
        o.setStudentName(studentName);
        o.setExceptional(isExceptional);
        o.setAge(age);
        o.setSemester(semester);
        o.setDigitalSignature(digitalSignature);
        o.setCgpa(cgpa);
        o.setPercentage(percentage);
        o.setHeight(height);

        o.setEnrolmentDate(enrolmentDate);
        o.setEnrolmentTime(enrolmentTime);
        o.setJoiningDateAndTime(joiningDateAndTime);

        o.setYearsSpent(yearsSpent);
        o.setRollNumber(rollNumber);
        o.setMonthlyFee(monthlyFee);
        o.setSqlDate(newSqlDate);
        o.setSqlTime(sqlTime);
        o.setSqlTimestamp(sqlTimestamp);
        o.setBigDecimal(bigDecimal);
        o.setBigInteger(bigInteger);
        o.setCalendar(calendar);
        return (E) o;
    }

    /**
     * Assert on data types.
     * 
     * @param s
     *            the s
     */
    protected void assertOnDataTypes(E s)
    {

        Assert.assertNotNull(s);
        Assert.assertEquals(((Long) studentId1).longValue(), s.getStudentId());
        Assert.assertEquals(78575785897L, s.getUniqueId());
        Assert.assertEquals("Amresh", s.getStudentName());
        Assert.assertEquals(false, s.isExceptional());
        Assert.assertEquals(10, s.getAge());
        Assert.assertEquals('A', s.getSemester());
        Assert.assertEquals((byte) 5, s.getDigitalSignature());
        Assert.assertEquals((short) 8, s.getCgpa());
        Assert.assertEquals((float) 61.6, s.getPercentage());
        Assert.assertEquals(163.76765654, s.getHeight());

        Assert.assertEquals(enrolmentDate.getDate(), s.getEnrolmentDate().getDate());
        Assert.assertEquals(enrolmentDate.getMonth(), s.getEnrolmentDate().getMonth());
        Assert.assertEquals(enrolmentDate.getYear(), s.getEnrolmentDate().getYear());

        Assert.assertEquals(enrolmentTime.getHours(), s.getEnrolmentTime().getHours());
        Assert.assertEquals(enrolmentTime.getMinutes(), s.getEnrolmentTime().getMinutes());
        Assert.assertEquals(enrolmentTime.getSeconds(), s.getEnrolmentTime().getSeconds());

        Assert.assertEquals(joiningDateAndTime.getDate(), s.getJoiningDateAndTime().getDate());
        Assert.assertEquals(joiningDateAndTime.getMonth(), s.getJoiningDateAndTime().getMonth());
        Assert.assertEquals(joiningDateAndTime.getYear(), s.getJoiningDateAndTime().getYear());
        Assert.assertEquals(joiningDateAndTime.getHours(), s.getJoiningDateAndTime().getHours());
        Assert.assertEquals(joiningDateAndTime.getMinutes(), s.getJoiningDateAndTime().getMinutes());
        Assert.assertEquals(joiningDateAndTime.getSeconds(), s.getJoiningDateAndTime().getSeconds());

        Assert.assertEquals(newSqlDate.getDate(), s.getSqlDate().getDate());
        Assert.assertEquals(newSqlDate.getMonth(), s.getSqlDate().getMonth());
        Assert.assertEquals(newSqlDate.getYear(), s.getSqlDate().getYear());

        Assert.assertEquals(sqlTime.getMinutes(), s.getSqlTime().getMinutes());
        Assert.assertEquals(sqlTime.getSeconds(), s.getSqlTime().getSeconds());
        Assert.assertEquals(sqlTime.getHours(), s.getSqlTime().getHours());

        Assert.assertEquals(sqlTimestamp.getDate(), s.getSqlTimestamp().getDate());
        Assert.assertEquals(sqlTimestamp.getMonth(), s.getSqlTimestamp().getMonth());
        Assert.assertEquals(sqlTimestamp.getYear(), s.getSqlTimestamp().getYear());
        Assert.assertEquals(sqlTimestamp.getHours(), s.getSqlTimestamp().getHours());
        Assert.assertEquals(sqlTimestamp.getMinutes(), s.getSqlTimestamp().getMinutes());
        Assert.assertEquals(sqlTimestamp.getSeconds(), s.getSqlTimestamp().getSeconds());

        Assert.assertEquals(Math.round(bigDecimal.doubleValue()), Math.round(s.getBigDecimal().doubleValue()));
        Assert.assertEquals(bigInteger, s.getBigInteger());

        Assert.assertEquals(calendar.get(Calendar.YEAR), s.getCalendar().get(Calendar.YEAR));
        Assert.assertEquals(calendar.get(Calendar.MONTH), s.getCalendar().get(Calendar.MONTH));
        Assert.assertEquals(calendar.get(Calendar.WEEK_OF_YEAR), s.getCalendar().get(Calendar.WEEK_OF_YEAR));
        Assert.assertEquals(calendar.get(Calendar.WEEK_OF_MONTH), s.getCalendar().get(Calendar.WEEK_OF_MONTH));
        Assert.assertEquals(calendar.get(Calendar.DAY_OF_MONTH), s.getCalendar().get(Calendar.DAY_OF_MONTH));
        Assert.assertEquals(calendar.get(Calendar.DAY_OF_WEEK), s.getCalendar().get(Calendar.DAY_OF_WEEK));
        Assert.assertEquals(calendar.get(Calendar.DAY_OF_WEEK_IN_MONTH),
                s.getCalendar().get(Calendar.DAY_OF_WEEK_IN_MONTH));
        Assert.assertEquals(calendar.get(Calendar.DAY_OF_YEAR), s.getCalendar().get(Calendar.DAY_OF_YEAR));
        Assert.assertEquals(calendar.get(Calendar.HOUR), s.getCalendar().get(Calendar.HOUR));
        Assert.assertEquals(calendar.get(Calendar.HOUR_OF_DAY), s.getCalendar().get(Calendar.HOUR_OF_DAY));
        Assert.assertEquals(calendar.get(Calendar.AM), s.getCalendar().get(Calendar.AM));
        Assert.assertEquals(calendar.get(Calendar.PM), s.getCalendar().get(Calendar.PM));
        Assert.assertEquals(calendar.get(Calendar.AM_PM), s.getCalendar().get(Calendar.AM_PM));

        Assert.assertEquals(new Integer(3), s.getYearsSpent());
        Assert.assertEquals(new Long(978423946455l), s.getRollNumber());
        Assert.assertEquals(new Double(135434.89), s.getMonthlyFee());

    }

    abstract void startServer();

    abstract void stopServer();

    abstract void createSchema();

    abstract void deleteSchema();

}
