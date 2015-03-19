/*******************************************************************************
 * * Copyright 2015 Impetus Infotech.
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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Calendar;
import java.util.Date;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;

/**
 * The Class StudentBase.
 * 
 * @author Devender Yadav
 * 
 */
public abstract class StudentBaseTest
{

    /** The Constant SCHEMA. */
    protected static final String SCHEMA = "HBaseNew";

    /** The Constant HBASE_PU. */
    protected static final String HBASE_PU = "queryTest";

    /** The emf. */
    protected static EntityManagerFactory emf;

    /** The em. */
    protected EntityManager em;

    /** The enrolment date. */
    protected Date enrolmentDate = new Date();

    /** The joining date and time. */
    protected Date joiningDateAndTime = new Date();

    /** The date. */
    protected long date = new Date().getTime();

    /** The new sql date. */
    protected java.sql.Date sqlDate = new java.sql.Date(date);

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

    /** The calendar. */
    protected Calendar calendar = Calendar.getInstance();

    /**
     * Persist students.
     */
    protected void persistStudents()
    {

        Student s1 = prepareData(12345677L, "Amresh", false, 10, 'A', (byte) 5, (short) 8, (float) 61.6, 163.76765654,
                enrolmentDate, enrolmentTime, joiningDateAndTime, new Integer(3), new Long(978423946455l), 135434.89,
                sqlDate, sqlTime, sqlTimestamp, bigDecimal, bigInteger, calendar);
        Student s2 = prepareData(12345678L, "Devender", true, 20, 'B', (byte) 50, (short) 8, (float) 63.6,
                163.76765655, enrolmentDate, enrolmentTime, joiningDateAndTime, new Integer(3),
                new Long(978423946455l), 135434.89, sqlDate, sqlTime, sqlTimestamp, bigDecimal, bigInteger, calendar);
        Student s3 = prepareData(12345679L, "Pragalbh", true, 15, 'C', (byte) 50, (short) 8, (float) 69.6,
                163.76765656, enrolmentDate, enrolmentTime, joiningDateAndTime, new Integer(3),
                new Long(978423946455l), 135434.89, sqlDate, sqlTime, sqlTimestamp, bigDecimal, bigInteger, calendar);

        em.persist(s1);
        em.persist(s2);
        em.persist(s3);
        em.clear();

    }

    /**
     * Delete students.
     */
    protected void deleteStudents()
    {
        em.createQuery("delete from Student s").executeUpdate();
        em.clear();
    }

    /**
     * Prepare data.
     * 
     * @param studentId
     *            the student id
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
     * @return the student
     */
    protected Student prepareData(long studentId, String studentName, boolean isExceptional, int age, char semester,
            byte digitalSignature, short cgpa, float percentage, double height, java.util.Date enrolmentDate,
            java.util.Date enrolmentTime, java.util.Date joiningDateAndTime, Integer yearsSpent, Long rollNumber,
            Double monthlyFee, java.sql.Date newSqlDate, java.sql.Time sqlTime, java.sql.Timestamp sqlTimestamp,
            BigDecimal bigDecimal, BigInteger bigInteger, Calendar calendar)
    {
        Student s = new Student();
        s.setStudentId(studentId);
        s.setStudentName(studentName);
        s.setExceptional(isExceptional);
        s.setAge(age);
        s.setSemester(semester);
        s.setDigitalSignature(digitalSignature);
        s.setCgpa(cgpa);
        s.setPercentage(percentage);
        s.setHeight(height);
        s.setEnrolmentDate(enrolmentDate);
        s.setEnrolmentTime(enrolmentTime);
        s.setJoiningDateAndTime(joiningDateAndTime);
        s.setYearsSpent(yearsSpent);
        s.setRollNumber(rollNumber);
        s.setMonthlyFee(monthlyFee);
        s.setSqlDate(newSqlDate);
        s.setSqlTime(sqlTime);
        s.setSqlTimestamp(sqlTimestamp);
        s.setBigDecimal(bigDecimal);
        s.setBigInteger(bigInteger);
        s.setCalendar(calendar);
        return s;
    }
}
