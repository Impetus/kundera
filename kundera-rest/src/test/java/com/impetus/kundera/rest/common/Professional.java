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
package com.impetus.kundera.rest.common;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Calendar;
import java.util.Date;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import javax.xml.bind.annotation.XmlRootElement;

import com.impetus.kundera.index.Index;
import com.impetus.kundera.index.IndexCollection;

@Entity
@Table(name = "PROFESSIONAL", schema = "KunderaExamples@twissandra")
@IndexCollection(columns = { @Index(name = "departmentId"), @Index(name = "isExceptional"), @Index(name = "age"),
        @Index(name = "grade"), @Index(name = "digitalSignature"), @Index(name = "rating"),
        @Index(name = "compliance"), @Index(name = "height"), @Index(name = "enrolmentDate"),
        @Index(name = "enrolmentTime"), @Index(name = "joiningDateAndTime"), @Index(name = "yearsSpent"),
        @Index(name = "uniqueId"), @Index(name = "monthlySalary"), @Index(name = "birthday"),
        @Index(name = "birthtime"), @Index(name = "anniversary"), @Index(name = "jobAttempts"),
        @Index(name = "accumulatedWealth"), @Index(name = "graduationDay") })
@NamedQueries(value = {
        @NamedQuery(name = "findByDepartment", query = "Select p from Professional p where p.departmentId = :departmentId"),
        @NamedQuery(name = "findByEnrolmentDate", query = "Select p from Professional p where p.enrolmentDate = ?1") })
@XmlRootElement
public class Professional
{
    // Primitive Types
    @Id
    @Column(name = "PROFESSION_ID")
    private String professionId;

    @Column(name = "DEPARTMENT_ID")
    private long departmentId;

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

    /*
     * @Column(name = "BIRTH_DAY") private java.sql.Date birthday;
     * 
     * @Column(name = "BIRTH_TIME") private java.sql.Time birthtime;
     * 
     * @Column(name = "ANNIVERSARY") private java.sql.Timestamp anniversary;
     */

    @Column(name = "JOB_ATTEMPTS")
    private BigInteger jobAttempts;

    @Column(name = "ACCUMULATED_WEALTH")
    private BigDecimal accumulatedWealth;

    @Column(name = "GRADUATION_DAY")
    private Calendar graduationDay;

    public Professional()
    {

    }

    public Professional(String professionId, long departmentId, boolean isExceptional, int age, char grade,
            byte digitalSignature, short rating, float compliance, double height, Date enrolmentDate,
            Date enrolmentTime, Date joiningDateAndTime, Integer yearsSpent, Long uniqueId, Double monthlySalary,
            /* java.sql.Date birthday, Time birthtime, Timestamp anniversary, */BigInteger jobAttempts,
            BigDecimal accumulatedWealth, Calendar graduationDay)
    {
        super();
        this.professionId = professionId;
        this.departmentId = departmentId;
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
        /*
         * this.birthday = birthday; this.birthtime = birthtime;
         * this.anniversary = anniversary;
         */
        this.jobAttempts = jobAttempts;
        this.accumulatedWealth = accumulatedWealth;
        this.graduationDay = graduationDay;
    }

    /**
     * @return the professionId
     */
    public String getProfessionId()
    {
        return professionId;
    }

    /**
     * @param professionId
     *            the professionId to set
     */
    public void setProfessionId(String professionId)
    {
        this.professionId = professionId;
    }

    /**
     * @return the departmentId
     */
    public long getDepartmentId()
    {
        return departmentId;
    }

    /**
     * @param departmentId
     *            the departmentId to set
     */
    public void setDepartmentId(long departmentId)
    {
        this.departmentId = departmentId;
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