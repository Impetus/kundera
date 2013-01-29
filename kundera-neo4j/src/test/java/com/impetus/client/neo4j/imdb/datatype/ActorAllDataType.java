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
package com.impetus.client.neo4j.imdb.datatype;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.ManyToMany;
import javax.persistence.MapKeyJoinColumn;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

import com.impetus.client.neo4j.imdb.Movie;
import com.impetus.client.neo4j.imdb.Role;
import com.impetus.kundera.index.Index;
import com.impetus.kundera.index.IndexCollection;

/**
 * Entity Class for Actor 
 * @author amresh.singh
 */
@Entity
@Table   //Ignored for Neo4J
@IndexCollection(columns={@Index(name = "name", type = "KEYS")})
public class ActorAllDataType
{
    @Id
    @Column(name="ACTOR_ID")   
    private int id;
    
    @Column(name="ACTOR_NAME")
    private String name;
    
    @Column(name = "DEPARTMENT_ID")
    private long departmentId;

    @Column(name = "IS_EXCEPTIONAL")
    private boolean isExceptional;

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

    @Column(name = "JOB_ATTEMPTS")
    private BigInteger jobAttempts;

    @Column(name = "ACCUMULATED_WEALTH")
    private BigDecimal accumulatedWealth;

    @Column(name = "GRADUATION_DAY")
    private Calendar graduationDay;
    
    
    public ActorAllDataType()
    {
    }
    

    public ActorAllDataType(int id, String name, long departmentId, boolean isExceptional, char grade,
            byte digitalSignature, short rating, float compliance, double height, Date enrolmentDate,
            Date enrolmentTime, Date joiningDateAndTime, Integer yearsSpent, Long uniqueId, Double monthlySalary,
            BigInteger jobAttempts,
            BigDecimal accumulatedWealth, Calendar graduationDay)
    {
        
        super();
        this.id = id;
        this.name = name;
        this.departmentId = departmentId;
        this.isExceptional = isExceptional;
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
        this.jobAttempts = jobAttempts;
        this.accumulatedWealth = accumulatedWealth;
        this.graduationDay = graduationDay;        
    }
    
    @ManyToMany(cascade = CascadeType.ALL, fetch = FetchType.LAZY) 
    @MapKeyJoinColumn(name="ACTS_IN")
    private Map<RoleAllDataType, MovieAllDataType> movies;
    
    public void addMovie(RoleAllDataType role, MovieAllDataType movie)
    {
        if(movies == null) movies = new HashMap<RoleAllDataType, MovieAllDataType>();
        movies.put(role, movie);
    }

    /**
     * @return the id
     */
    public int getId()
    {
        return id;
    }

    /**
     * @param id the id to set
     */
    public void setId(int id)
    {
        this.id = id;
    }

    /**
     * @return the name
     */
    public String getName()
    {
        return name;
    }

    /**
     * @param name the name to set
     */
    public void setName(String name)
    {
        this.name = name;
    }

    /**
     * @return the departmentId
     */
    public long getDepartmentId()
    {
        return departmentId;
    }

    /**
     * @param departmentId the departmentId to set
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
     * @param isExceptional the isExceptional to set
     */
    public void setExceptional(boolean isExceptional)
    {
        this.isExceptional = isExceptional;
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
     * @return the jobAttempts
     */
    public BigInteger getJobAttempts()
    {
        return jobAttempts;
    }

    /**
     * @param jobAttempts the jobAttempts to set
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
     * @param accumulatedWealth the accumulatedWealth to set
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
     * @param graduationDay the graduationDay to set
     */
    public void setGraduationDay(Calendar graduationDay)
    {
        this.graduationDay = graduationDay;
    }


    /**
     * @return the movies
     */
    public Map<RoleAllDataType, MovieAllDataType> getMovies()
    {
        return movies;
    }


    /**
     * @param movies the movies to set
     */
    public void setMovies(Map<RoleAllDataType, MovieAllDataType> movies)
    {
        this.movies = movies;
    }    
    

}
