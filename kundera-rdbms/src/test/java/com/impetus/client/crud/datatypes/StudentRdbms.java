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

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

@Entity
@Table(name = "STUDENT", schema = "test")
public class StudentRdbms implements StudentEntityDef
{
    // Primitive Types
    @Id
    @Column(name = "STUDENT_ID")
    private long studentId;

    @Column(name = "UNIQUE_ID")
    private long uniqueId;

    @Column(name = "STUDENT_NAME")
    private String studentName;

    @Column(name = "IS_EXCEPTIONAL")
    private boolean isExceptional;

    @Column(name = "AGE")
    private int age;

    @Column(name = "SEMESTER")
    private char semester; // A,B,C,D,E,F for i to vi

    @Column(name = "DIGITAL_SIGNATURE")
    private byte digitalSignature;

    @Column(name = "CGPA")
    private short cgpa; // 1-10

    @Column(name = "PERCENTAGE")
    private float percentage;

    @Column(name = "HEIGHT")
    private double height;

    // Date-time types
    @Column(name = "ENROLMENT_DATE")
    @Temporal(TemporalType.DATE)
    private java.util.Date enrolmentDate;

    @Column(name = "ENROLMENT_TIME")
    @Temporal(TemporalType.TIME)
    private java.util.Date enrolmentTime;

    @Column(name = "JOINING_DATE_TIME")
    @Temporal(TemporalType.TIMESTAMP)
    private java.util.Date joiningDateAndTime;

    // Wrapper types

    @Column(name = "YEARS_SPENT")
    private Integer yearsSpent;

    @Column(name = "ROLL_NUMBER")
    private Long rollNumber;

    @Column(name = "MONTHLY_FEE")
    private Double monthlyFee;

    @Column(name = "SQL_DATE")
    private java.sql.Date sqlDate;

    @Column(name = "SQL_TIMESTAMP")
    private java.sql.Timestamp sqlTimestamp;

    @Column(name = "SQL_TIME")
    private java.sql.Time sqlTime;

    @Column(name = "BIG_INT")
    private BigInteger bigInteger;

    @Column(name = "BIG_DECIMAL")
    private BigDecimal bigDecimal;

    @Column(name = "CALENDAR")
    private Calendar calendar;

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.examples.crud.student.StudentEntityDef#getStudentId()
     */
    @Override
    public long getStudentId()
    {
        return studentId;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.examples.crud.student.StudentEntityDef#setStudentId
     * (long)
     */
    @Override
    public void setStudentId(long studentId)
    {
        this.studentId = studentId;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.examples.crud.student.StudentEntityDef#getUniqueId()
     */
    @Override
    public long getUniqueId()
    {
        return uniqueId;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.examples.crud.student.StudentEntityDef#setUniqueId
     * (long)
     */
    @Override
    public void setUniqueId(long uniqueId)
    {
        this.uniqueId = uniqueId;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.examples.crud.student.StudentEntityDef#getStudentName
     * ()
     */
    @Override
    public String getStudentName()
    {
        return studentName;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.examples.crud.student.StudentEntityDef#setStudentName
     * (java.lang.String)
     */
    @Override
    public void setStudentName(String studentName)
    {
        this.studentName = studentName;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.examples.crud.student.StudentEntityDef#isExceptional
     * ()
     */
    @Override
    public boolean isExceptional()
    {
        return isExceptional;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.examples.crud.student.StudentEntityDef#setExceptional
     * (boolean)
     */
    @Override
    public void setExceptional(boolean isExceptional)
    {
        this.isExceptional = isExceptional;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.examples.crud.student.StudentEntityDef#getAge()
     */
    @Override
    public int getAge()
    {
        return age;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.examples.crud.student.StudentEntityDef#setAge(int)
     */
    @Override
    public void setAge(int age)
    {
        this.age = age;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.examples.crud.student.StudentEntityDef#getSemester()
     */
    @Override
    public char getSemester()
    {
        return semester;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.examples.crud.student.StudentEntityDef#setSemester
     * (char)
     */
    @Override
    public void setSemester(char semester)
    {
        this.semester = semester;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.examples.crud.student.StudentEntityDef#
     * getDigitalSignature()
     */
    @Override
    public byte getDigitalSignature()
    {
        return digitalSignature;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.examples.crud.student.StudentEntityDef#
     * setDigitalSignature(byte)
     */
    @Override
    public void setDigitalSignature(byte digitalSignature)
    {
        this.digitalSignature = digitalSignature;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.examples.crud.student.StudentEntityDef#getCgpa()
     */
    @Override
    public short getCgpa()
    {
        return cgpa;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.examples.crud.student.StudentEntityDef#setCgpa(short)
     */
    @Override
    public void setCgpa(short cgpa)
    {
        this.cgpa = cgpa;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.examples.crud.student.StudentEntityDef#getPercentage
     * ()
     */
    @Override
    public float getPercentage()
    {
        return percentage;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.examples.crud.student.StudentEntityDef#setPercentage
     * (float)
     */
    @Override
    public void setPercentage(float percentage)
    {
        this.percentage = percentage;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.examples.crud.student.StudentEntityDef#getHeight()
     */
    @Override
    public double getHeight()
    {
        return height;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.examples.crud.student.StudentEntityDef#setHeight(
     * double)
     */
    @Override
    public void setHeight(double height)
    {
        this.height = height;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.examples.crud.student.StudentEntityDef#getEnrolmentDate
     * ()
     */
    @Override
    public java.util.Date getEnrolmentDate()
    {
        return enrolmentDate;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.examples.crud.student.StudentEntityDef#setEnrolmentDate
     * (java.util.Date)
     */
    @Override
    public void setEnrolmentDate(java.util.Date enrolmentDate)
    {
        this.enrolmentDate = enrolmentDate;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.examples.crud.student.StudentEntityDef#getEnrolmentTime
     * ()
     */
    @Override
    public java.util.Date getEnrolmentTime()
    {
        return enrolmentTime;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.examples.crud.student.StudentEntityDef#setEnrolmentTime
     * (java.util.Date)
     */
    @Override
    public void setEnrolmentTime(java.util.Date enrolmentTime)
    {
        this.enrolmentTime = enrolmentTime;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.examples.crud.student.StudentEntityDef#
     * getJoiningDateAndTime()
     */
    @Override
    public java.util.Date getJoiningDateAndTime()
    {
        return joiningDateAndTime;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.examples.crud.student.StudentEntityDef#
     * setJoiningDateAndTime(java.util.Date)
     */
    @Override
    public void setJoiningDateAndTime(java.util.Date joiningDateAndTime)
    {
        this.joiningDateAndTime = joiningDateAndTime;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.examples.crud.student.StudentEntityDef#getYearsSpent
     * ()
     */
    @Override
    public Integer getYearsSpent()
    {
        return yearsSpent;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.examples.crud.student.StudentEntityDef#setYearsSpent
     * (java.lang.Integer)
     */
    @Override
    public void setYearsSpent(Integer yearsSpent)
    {
        this.yearsSpent = yearsSpent;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.examples.crud.student.StudentEntityDef#getRollNumber
     * ()
     */
    @Override
    public Long getRollNumber()
    {
        return rollNumber;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.examples.crud.student.StudentEntityDef#setRollNumber
     * (java.lang.Long)
     */
    @Override
    public void setRollNumber(Long rollNumber)
    {
        this.rollNumber = rollNumber;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.examples.crud.student.StudentEntityDef#getMonthlyFee
     * ()
     */
    @Override
    public Double getMonthlyFee()
    {
        return monthlyFee;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.examples.crud.student.StudentEntityDef#setMonthlyFee
     * (java.lang.Double)
     */
    @Override
    public void setMonthlyFee(Double monthlyFee)
    {
        this.monthlyFee = monthlyFee;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.examples.crud.student.StudentEntityDef#getSqlDate()
     */
    @Override
    public java.sql.Date getSqlDate()
    {
        return sqlDate;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.examples.crud.student.StudentEntityDef#setSqlDate
     * (java.sql.Date)
     */
    @Override
    public void setSqlDate(java.sql.Date sqlDate)
    {
        this.sqlDate = sqlDate;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.examples.crud.student.StudentEntityDef#getSqlTimestamp
     * ()
     */
    @Override
    public java.sql.Timestamp getSqlTimestamp()
    {
        return sqlTimestamp;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.examples.crud.student.StudentEntityDef#setSqlTimestamp
     * (java.sql.Timestamp)
     */
    @Override
    public void setSqlTimestamp(java.sql.Timestamp sqlTimestamp)
    {
        this.sqlTimestamp = sqlTimestamp;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.examples.crud.student.StudentEntityDef#getSqlTime()
     */
    @Override
    public java.sql.Time getSqlTime()
    {
        return sqlTime;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.examples.crud.student.StudentEntityDef#setSqlTime
     * (java.sql.Time)
     */
    @Override
    public void setSqlTime(java.sql.Time sqlTime)
    {
        this.sqlTime = sqlTime;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.examples.crud.student.StudentEntityDef#getBigInteger
     * ()
     */
    @Override
    public BigInteger getBigInteger()
    {
        return bigInteger;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.examples.crud.student.StudentEntityDef#setBigInteger
     * (java.math.BigInteger)
     */
    @Override
    public void setBigInteger(BigInteger bigInteger)
    {
        this.bigInteger = bigInteger;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.examples.crud.student.StudentEntityDef#getBigDecimal
     * ()
     */
    @Override
    public BigDecimal getBigDecimal()
    {
        return bigDecimal;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.examples.crud.student.StudentEntityDef#setBigDecimal
     * (java.math.BigDecimal)
     */
    @Override
    public void setBigDecimal(BigDecimal bigDecimal)
    {
        this.bigDecimal = bigDecimal;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.examples.crud.student.StudentEntityDef#getCalendar()
     */
    @Override
    public Calendar getCalendar()
    {
        return calendar;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.examples.crud.student.StudentEntityDef#setCalendar
     * (java.util.Calendar)
     */
    @Override
    public void setCalendar(Calendar calendar)
    {
        this.calendar = calendar;
    }

}
