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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Calendar;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

/**
 * The Class Student.
 * 
 * @author Devender Yadav
 */
@Entity
@Table(name = "STUDENTS", schema = "HBaseNew@queryTest")
public class Student
{
    // Primitive Types
    /** The student id. */
    @Id
    @Column(name = "STUDENT_ID")
    private long studentId;

    /** The student name. */
    @Column(name = "STUDENT_NAME")
    private String studentName;

    /** The is exceptional. */
    @Column(name = "IS_EXCEPTIONAL")
    private boolean isExceptional;

    /** The age. */
    @Column(name = "AGE")
    private int age;

    /** The semester. */
    @Column(name = "SEMESTER")
    private char semester; // A,B,C,D,E,F for i to vi

    /** The digital signature. */
    @Column(name = "DIGITAL_SIGNATURE")
    private byte digitalSignature;

    /** The cgpa. */
    @Column(name = "CGPA")
    private short cgpa; // 1-10

    /** The percentage. */
    @Column(name = "PERCENTAGE")
    private float percentage;

    /** The height. */
    @Column(name = "HEIGHT")
    private double height;

    // Date-time types
    /** The enrolment date. */
    @Column(name = "ENROLMENT_DATE")
    @Temporal(TemporalType.DATE)
    private java.util.Date enrolmentDate;

    /** The enrolment time. */
    @Column(name = "ENROLMENT_TIME")
    @Temporal(TemporalType.TIME)
    private java.util.Date enrolmentTime;

    /** The joining date and time. */
    @Column(name = "JOINING_DATE_TIME")
    @Temporal(TemporalType.TIMESTAMP)
    private java.util.Date joiningDateAndTime;

    // Wrapper types

    /** The years spent. */
    @Column(name = "YEARS_SPENT")
    private Integer yearsSpent;

    /** The roll number. */
    @Column(name = "ROLL_NUMBER")
    private Long rollNumber;

    /** The monthly fee. */
    @Column(name = "MONTHLY_FEE")
    private Double monthlyFee;

    /** The sql date. */
    @Column(name = "SQL_DATE")
    private java.sql.Date sqlDate;

    /** The sql timestamp. */
    @Column(name = "SQL_TIMESTAMP")
    private java.sql.Timestamp sqlTimestamp;

    /** The sql time. */
    @Column(name = "SQL_TIME")
    private java.sql.Time sqlTime;

    /** The big integer. */
    @Column(name = "BIG_INT")
    private BigInteger bigInteger;

    /** The big decimal. */
    @Column(name = "BIG_DECIMAL")
    private BigDecimal bigDecimal;

    /** The calendar. */
    @Column(name = "CALENDAR")
    private Calendar calendar;

    /**
     * Gets the student id.
     *
     * @return the studentId
     */
    public long getStudentId()
    {
        return studentId;
    }

    /**
     * Sets the student id.
     *
     * @param studentId            the studentId to set
     */
    public void setStudentId(long studentId)
    {
        this.studentId = studentId;
    }

    /**
     * Gets the student name.
     *
     * @return the studentName
     */
    public String getStudentName()
    {
        return studentName;
    }

    /**
     * Sets the student name.
     *
     * @param studentName            the studentName to set
     */
    public void setStudentName(String studentName)
    {
        this.studentName = studentName;
    }

    /**
     * Checks if is exceptional.
     *
     * @return the isExceptional
     */
    public boolean isExceptional()
    {
        return isExceptional;
    }

    /**
     * Sets the exceptional.
     *
     * @param isExceptional            the isExceptional to set
     */
    public void setExceptional(boolean isExceptional)
    {
        this.isExceptional = isExceptional;
    }

    /**
     * Gets the age.
     *
     * @return the age
     */
    public int getAge()
    {
        return age;
    }

    /**
     * Sets the age.
     *
     * @param age            the age to set
     */
    public void setAge(int age)
    {
        this.age = age;
    }

    /**
     * Gets the semester.
     *
     * @return the semester
     */
    public char getSemester()
    {
        return semester;
    }

    /**
     * Sets the semester.
     *
     * @param semester            the semester to set
     */
    public void setSemester(char semester)
    {
        this.semester = semester;
    }

    /**
     * Gets the digital signature.
     *
     * @return the digitalSignature
     */
    public byte getDigitalSignature()
    {
        return digitalSignature;
    }

    /**
     * Sets the digital signature.
     *
     * @param digitalSignature            the digitalSignature to set
     */
    public void setDigitalSignature(byte digitalSignature)
    {
        this.digitalSignature = digitalSignature;
    }

    /**
     * Gets the cgpa.
     *
     * @return the cgpa
     */
    public short getCgpa()
    {
        return cgpa;
    }

    /**
     * Sets the cgpa.
     *
     * @param cgpa            the cgpa to set
     */
    public void setCgpa(short cgpa)
    {
        this.cgpa = cgpa;
    }

    /**
     * Gets the percentage.
     *
     * @return the percentage
     */
    public float getPercentage()
    {
        return percentage;
    }

    /**
     * Sets the percentage.
     *
     * @param percentage            the percentage to set
     */
    public void setPercentage(float percentage)
    {
        this.percentage = percentage;
    }

    /**
     * Gets the height.
     *
     * @return the height
     */
    public double getHeight()
    {
        return height;
    }

    /**
     * Sets the height.
     *
     * @param height            the height to set
     */
    public void setHeight(double height)
    {
        this.height = height;
    }

    /**
     * Gets the enrolment date.
     *
     * @return the enrolmentDate
     */
    public java.util.Date getEnrolmentDate()
    {
        return enrolmentDate;
    }

    /**
     * Sets the enrolment date.
     *
     * @param enrolmentDate            the enrolmentDate to set
     */
    public void setEnrolmentDate(java.util.Date enrolmentDate)
    {
        this.enrolmentDate = enrolmentDate;
    }

    /**
     * Gets the enrolment time.
     *
     * @return the enrolmentTime
     */
    public java.util.Date getEnrolmentTime()
    {
        return enrolmentTime;
    }

    /**
     * Sets the enrolment time.
     *
     * @param enrolmentTime            the enrolmentTime to set
     */
    public void setEnrolmentTime(java.util.Date enrolmentTime)
    {
        this.enrolmentTime = enrolmentTime;
    }

    /**
     * Gets the joining date and time.
     *
     * @return the joiningDateAndTime
     */
    public java.util.Date getJoiningDateAndTime()
    {
        return joiningDateAndTime;
    }

    /**
     * Sets the joining date and time.
     *
     * @param joiningDateAndTime            the joiningDateAndTime to set
     */
    public void setJoiningDateAndTime(java.util.Date joiningDateAndTime)
    {
        this.joiningDateAndTime = joiningDateAndTime;
    }

    /**
     * Gets the years spent.
     *
     * @return the yearsSpent
     */
    public Integer getYearsSpent()
    {
        return yearsSpent;
    }

    /**
     * Sets the years spent.
     *
     * @param yearsSpent            the yearsSpent to set
     */
    public void setYearsSpent(Integer yearsSpent)
    {
        this.yearsSpent = yearsSpent;
    }

    /**
     * Gets the roll number.
     *
     * @return the rollNumber
     */
    public Long getRollNumber()
    {
        return rollNumber;
    }

    /**
     * Sets the roll number.
     *
     * @param rollNumber            the rollNumber to set
     */
    public void setRollNumber(Long rollNumber)
    {
        this.rollNumber = rollNumber;
    }

    /**
     * Gets the monthly fee.
     *
     * @return the monthlyFee
     */
    public Double getMonthlyFee()
    {
        return monthlyFee;
    }

    /**
     * Sets the monthly fee.
     *
     * @param monthlyFee            the monthlyFee to set
     */
    public void setMonthlyFee(Double monthlyFee)
    {
        this.monthlyFee = monthlyFee;
    }

    /**
     * Gets the sql date.
     *
     * @return the sql date
     */
    public java.sql.Date getSqlDate()
    {
        return sqlDate;
    }

    /**
     * Sets the sql date.
     *
     * @param sqlDate the new sql date
     */
    public void setSqlDate(java.sql.Date sqlDate)
    {
        this.sqlDate = sqlDate;
    }

    /**
     * Gets the sql timestamp.
     *
     * @return the sqlTimestamp
     */
    public java.sql.Timestamp getSqlTimestamp()
    {
        return sqlTimestamp;
    }

    /**
     * Sets the sql timestamp.
     *
     * @param sqlTimestamp            the sqlTimestamp to set
     */
    public void setSqlTimestamp(java.sql.Timestamp sqlTimestamp)
    {
        this.sqlTimestamp = sqlTimestamp;
    }

    /**
     * Gets the sql time.
     *
     * @return the sqlTime
     */
    public java.sql.Time getSqlTime()
    {
        return sqlTime;
    }

    /**
     * Sets the sql time.
     *
     * @param sqlTime            the sqlTime to set
     */
    public void setSqlTime(java.sql.Time sqlTime)
    {
        this.sqlTime = sqlTime;
    }

    /**
     * Gets the big integer.
     *
     * @return the bigInteger
     */
    public BigInteger getBigInteger()
    {
        return bigInteger;
    }

    /**
     * Sets the big integer.
     *
     * @param bigInteger            the bigInteger to set
     */
    public void setBigInteger(BigInteger bigInteger)
    {
        this.bigInteger = bigInteger;
    }

    /**
     * Gets the big decimal.
     *
     * @return the bigDecimal
     */
    public BigDecimal getBigDecimal()
    {
        return bigDecimal;
    }

    /**
     * Sets the big decimal.
     *
     * @param bigDecimal            the bigDecimal to set
     */
    public void setBigDecimal(BigDecimal bigDecimal)
    {
        this.bigDecimal = bigDecimal;
    }

    /**
     * Gets the calendar.
     *
     * @return the calendar
     */
    public Calendar getCalendar()
    {
        return calendar;
    }

    /**
     * Sets the calendar.
     *
     * @param calendar            the calendar to set
     */
    public void setCalendar(Calendar calendar)
    {
        this.calendar = calendar;
    }

}
