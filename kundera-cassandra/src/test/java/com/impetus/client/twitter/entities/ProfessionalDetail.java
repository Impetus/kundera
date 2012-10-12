/**
 * Copyright 2012 Impetus Infotech.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.impetus.client.twitter.entities;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;

import javax.persistence.Column;
import javax.persistence.Embeddable;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

import com.impetus.kundera.annotations.Index;
import com.impetus.kundera.annotations.IndexedColumn;

/**
 * Holds professional details of any user
 * 
 * @author amresh.singh
 */

@Embeddable
@Index(index = true, indexedColumns = { @IndexedColumn(name = "professionId"), @IndexedColumn(name = "departmentName"),
        @IndexedColumn(name = "isExceptional"), @IndexedColumn(name = "age"), @IndexedColumn(name = "grade"),
        @IndexedColumn(name = "digitalSignature"), @IndexedColumn(name = "rating"),
        @IndexedColumn(name = "compliance"), @IndexedColumn(name = "height"), @IndexedColumn(name = "enrolmentDate"),
        @IndexedColumn(name = "enrolmentTime"), @IndexedColumn(name = "joiningDateAndTime"),
        @IndexedColumn(name = "yearsSpent"), @IndexedColumn(name = "uniqueId"), @IndexedColumn(name = "monthlySalary"),
        @IndexedColumn(name = "birthday"), @IndexedColumn(name = "birthtime"), @IndexedColumn(name = "anniversary"),
        @IndexedColumn(name = "jobAttempts"), @IndexedColumn(name = "accumulatedWealth"),
        @IndexedColumn(name = "graduationDay") })
public class ProfessionalDetail
{
    // Primitive Types
    @Column(name = "PROFESSION_ID")
    private long professionId;

    @Column(name = "DEPARTMENT_NAME")
    private String departmentName;

    @Column(name = "IS_EXCEPTIONAL")
    private boolean isExceptional;

    @Column(name = "AGE")
    private int age;

    @Column(name = "GRADE")
    private char grade; // A,B,C,D,E,F for i to vi

    @Column(name = "DIGITAL_SIGNATURE")
    private byte digitalSignature;

    @Column(name = "RATING")
    private short rating; // 1-10

    @Column(name = "COMPLIANCE")
    private float compliance;

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

    @Column(name = "UNIQUE_ID")
    private Long uniqueId;

    @Column(name = "MONTHLY_SALARY")
    private Double monthlySalary;

    @Column(name = "BIRTH_DAY")
    private java.sql.Date birthday;

    @Column(name = "BIRTH_TIME")
    private java.sql.Time birthtime;

    @Column(name = "ANNIVERSARY")
    private java.sql.Timestamp anniversary;

    @Column(name = "JOB_ATTEMPTS")
    private BigInteger jobAttempts;

    @Column(name = "ACCUMULATED_WEALTH")
    private BigDecimal accumulatedWealth;

    @Column(name = "GRADUATION_DAY")
    private Calendar graduationDay;

    public ProfessionalDetail(long professionId, String departmentName, boolean isExceptional, int age, char grade,
            byte digitalSignature, short rating, float compliance, double height, Date enrolmentDate,
            Date enrolmentTime, Date joiningDateAndTime, Integer yearsSpent, Long uniqueId, Double monthlySalary,
            java.sql.Date birthday, Time birthtime, Timestamp anniversary, BigInteger jobAttempts,
            BigDecimal accumulatedWealth, Calendar graduationDay)
    {
        super();
        this.professionId = professionId;
        this.departmentName = departmentName;
        this.isExceptional = isExceptional;
        this.age = age;
        this.grade = grade;
        this.digitalSignature = digitalSignature;
        this.rating = rating;
        this.compliance = compliance;
        this.height = height;
        this.enrolmentDate = enrolmentDate;
        this.enrolmentTime = enrolmentTime;
        this.joiningDateAndTime = joiningDateAndTime;
        this.yearsSpent = yearsSpent;
        this.uniqueId = uniqueId;
        this.monthlySalary = monthlySalary;
        this.birthday = birthday;
        this.birthtime = birthtime;
        this.anniversary = anniversary;
        this.jobAttempts = jobAttempts;
        this.accumulatedWealth = accumulatedWealth;
        this.graduationDay = graduationDay;
    }

    public ProfessionalDetail()
    {

    }

    /**
     * @return the professionId
     */
    public long getProfessionId()
    {
        return professionId;
    }

    /**
     * @param professionId
     *            the professionId to set
     */
    public void setProfessionId(long professionId)
    {
        this.professionId = professionId;
    }

    /**
     * @return the departmentName
     */
    public String getDepartmentName()
    {
        return departmentName;
    }

    /**
     * @param departmentName
     *            the departmentName to set
     */
    public void setDepartmentName(String departmentName)
    {
        this.departmentName = departmentName;
    }

    /**
     * @return the isExceptional
     */
    public boolean isExceptional()
    {
        return isExceptional;
    }

    /**
     * @param isExceptional
     *            the isExceptional to set
     */
    public void setExceptional(boolean isExceptional)
    {
        this.isExceptional = isExceptional;
    }

    /**
     * @return the age
     */
    public int getAge()
    {
        return age;
    }

    /**
     * @param age
     *            the age to set
     */
    public void setAge(int age)
    {
        this.age = age;
    }

    /**
     * @return the grade
     */
    public char getGrade()
    {
        return grade;
    }

    /**
     * @param grade
     *            the grade to set
     */
    public void setGrade(char grade)
    {
        this.grade = grade;
    }

    /**
     * @return the digitalSignature
     */
    public byte getDigitalSignature()
    {
        return digitalSignature;
    }

    /**
     * @param digitalSignature
     *            the digitalSignature to set
     */
    public void setDigitalSignature(byte digitalSignature)
    {
        this.digitalSignature = digitalSignature;
    }

    /**
     * @return the rating
     */
    public short getRating()
    {
        return rating;
    }

    /**
     * @param rating
     *            the rating to set
     */
    public void setRating(short rating)
    {
        this.rating = rating;
    }

    /**
     * @return the compliance
     */
    public float getCompliance()
    {
        return compliance;
    }

    /**
     * @param compliance
     *            the compliance to set
     */
    public void setCompliance(float compliance)
    {
        this.compliance = compliance;
    }

    /**
     * @return the height
     */
    public double getHeight()
    {
        return height;
    }

    /**
     * @param height
     *            the height to set
     */
    public void setHeight(double height)
    {
        this.height = height;
    }

    /**
     * @return the enrolmentDate
     */
    public java.util.Date getEnrolmentDate()
    {
        return enrolmentDate;
    }

    /**
     * @param enrolmentDate
     *            the enrolmentDate to set
     */
    public void setEnrolmentDate(java.util.Date enrolmentDate)
    {
        this.enrolmentDate = enrolmentDate;
    }

    /**
     * @return the enrolmentTime
     */
    public java.util.Date getEnrolmentTime()
    {
        return enrolmentTime;
    }

    /**
     * @param enrolmentTime
     *            the enrolmentTime to set
     */
    public void setEnrolmentTime(java.util.Date enrolmentTime)
    {
        this.enrolmentTime = enrolmentTime;
    }

    /**
     * @return the joiningDateAndTime
     */
    public java.util.Date getJoiningDateAndTime()
    {
        return joiningDateAndTime;
    }

    /**
     * @param joiningDateAndTime
     *            the joiningDateAndTime to set
     */
    public void setJoiningDateAndTime(java.util.Date joiningDateAndTime)
    {
        this.joiningDateAndTime = joiningDateAndTime;
    }

    /**
     * @return the yearsSpent
     */
    public Integer getYearsSpent()
    {
        return yearsSpent;
    }

    /**
     * @param yearsSpent
     *            the yearsSpent to set
     */
    public void setYearsSpent(Integer yearsSpent)
    {
        this.yearsSpent = yearsSpent;
    }

    /**
     * @return the uniqueId
     */
    public Long getUniqueId()
    {
        return uniqueId;
    }

    /**
     * @param uniqueId
     *            the uniqueId to set
     */
    public void setUniqueId(Long uniqueId)
    {
        this.uniqueId = uniqueId;
    }

    /**
     * @return the monthlySalary
     */
    public Double getMonthlySalary()
    {
        return monthlySalary;
    }

    /**
     * @param monthlySalary
     *            the monthlySalary to set
     */
    public void setMonthlySalary(Double monthlySalary)
    {
        this.monthlySalary = monthlySalary;
    }

    /**
     * @return the birthday
     */
    public java.sql.Date getBirthday()
    {
        return birthday;
    }

    /**
     * @param birthday
     *            the birthday to set
     */
    public void setBirthday(java.sql.Date birthday)
    {
        this.birthday = birthday;
    }

    /**
     * @return the birthtime
     */
    public java.sql.Time getBirthtime()
    {
        return birthtime;
    }

    /**
     * @param birthtime
     *            the birthtime to set
     */
    public void setBirthtime(java.sql.Time birthtime)
    {
        this.birthtime = birthtime;
    }

    /**
     * @return the anniversary
     */
    public java.sql.Timestamp getAnniversary()
    {
        return anniversary;
    }

    /**
     * @param anniversary
     *            the anniversary to set
     */
    public void setAnniversary(java.sql.Timestamp anniversary)
    {
        this.anniversary = anniversary;
    }

    /**
     * @return the jobAttempts
     */
    public BigInteger getJobAttempts()
    {
        return jobAttempts;
    }

    /**
     * @param jobAttempts
     *            the jobAttempts to set
     */
    public void setJobAttempts(BigInteger jobAttempts)
    {
        this.jobAttempts = jobAttempts;
    }

    /**
     * @return the accumulatedWealth
     */
    public BigDecimal getAccumulatedWealth()
    {
        return accumulatedWealth;
    }

    /**
     * @param accumulatedWealth
     *            the accumulatedWealth to set
     */
    public void setAccumulatedWealth(BigDecimal accumulatedWealth)
    {
        this.accumulatedWealth = accumulatedWealth;
    }

    /**
     * @return the graduationDay
     */
    public Calendar getGraduationDay()
    {
        return graduationDay;
    }

    /**
     * @param graduationDay
     *            the graduationDay to set
     */
    public void setGraduationDay(Calendar graduationDay)
    {
        this.graduationDay = graduationDay;
    }

}
