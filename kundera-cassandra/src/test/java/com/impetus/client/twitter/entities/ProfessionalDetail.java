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

/**
 * Holds professional details of any user 
 * @author amresh.singh
 */

@Embeddable
@Index(index=true, columns={"professionId", "departmentName", "isExceptional"
        , "age", "grade", "digitalSignature", "rating", "compliance", "height",
        "enrolmentDate", "enrolmentTime", "joiningDateAndTime", "yearsSpent",
        "uniqueId", "monthlySalary", "sqlDate", "sqlTimestamp", "sqlTime",
        "bigInteger", "bigDecimal", "calendar"})
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
    
    public ProfessionalDetail()
    {
        
    }    

    public ProfessionalDetail(long professionId, String departmentName, boolean isExceptional, int age, char grade,
            byte digitalSignature, short rating, float compliance, double height, Date enrolmentDate,
            Date enrolmentTime, Date joiningDateAndTime, Integer yearsSpent, Long uniqueId, Double monthlySalary,
            java.sql.Date sqlDate, Timestamp sqlTimestamp, Time sqlTime, BigInteger bigInteger, BigDecimal bigDecimal,
            Calendar calendar)
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
        this.sqlDate = sqlDate;
        this.sqlTimestamp = sqlTimestamp;
        this.sqlTime = sqlTime;
        this.bigInteger = bigInteger;
        this.bigDecimal = bigDecimal;
        this.calendar = calendar;
    }

    /**
     * @return the professionId
     */
    public long getProfessionId()
    {
        return professionId;
    }

    /**
     * @param professionId the professionId to set
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
     * @param departmentName the departmentName to set
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
     * @param isExceptional the isExceptional to set
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
     * @param age the age to set
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
     * @param grade the grade to set
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
     * @param digitalSignature the digitalSignature to set
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
     * @param rating the rating to set
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
     * @param compliance the compliance to set
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
     * @param height the height to set
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
     * @param enrolmentDate the enrolmentDate to set
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
     * @param enrolmentTime the enrolmentTime to set
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
     * @param joiningDateAndTime the joiningDateAndTime to set
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
     * @param yearsSpent the yearsSpent to set
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
     * @param uniqueId the uniqueId to set
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
     * @param monthlySalary the monthlySalary to set
     */
    public void setMonthlySalary(Double monthlySalary)
    {
        this.monthlySalary = monthlySalary;
    }

    /**
     * @return the sqlDate
     */
    public java.sql.Date getSqlDate()
    {
        return sqlDate;
    }

    /**
     * @param sqlDate the sqlDate to set
     */
    public void setSqlDate(java.sql.Date sqlDate)
    {
        this.sqlDate = sqlDate;
    }

    /**
     * @return the sqlTimestamp
     */
    public java.sql.Timestamp getSqlTimestamp()
    {
        return sqlTimestamp;
    }

    /**
     * @param sqlTimestamp the sqlTimestamp to set
     */
    public void setSqlTimestamp(java.sql.Timestamp sqlTimestamp)
    {
        this.sqlTimestamp = sqlTimestamp;
    }

    /**
     * @return the sqlTime
     */
    public java.sql.Time getSqlTime()
    {
        return sqlTime;
    }

    /**
     * @param sqlTime the sqlTime to set
     */
    public void setSqlTime(java.sql.Time sqlTime)
    {
        this.sqlTime = sqlTime;
    }

    /**
     * @return the bigInteger
     */
    public BigInteger getBigInteger()
    {
        return bigInteger;
    }

    /**
     * @param bigInteger the bigInteger to set
     */
    public void setBigInteger(BigInteger bigInteger)
    {
        this.bigInteger = bigInteger;
    }

    /**
     * @return the bigDecimal
     */
    public BigDecimal getBigDecimal()
    {
        return bigDecimal;
    }

    /**
     * @param bigDecimal the bigDecimal to set
     */
    public void setBigDecimal(BigDecimal bigDecimal)
    {
        this.bigDecimal = bigDecimal;
    }

    /**
     * @return the calendar
     */
    public Calendar getCalendar()
    {
        return calendar;
    }

    /**
     * @param calendar the calendar to set
     */
    public void setCalendar(Calendar calendar)
    {
        this.calendar = calendar;
    }    

}
