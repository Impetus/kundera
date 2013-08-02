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

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.OneToOne;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

import com.impetus.kundera.index.Index;
import com.impetus.kundera.index.IndexCollection;

/**
 * Role Entity containing all data types
 * 
 * @author amresh.singh
 */
@Entity
@Table
@IndexCollection(columns = { @Index(name = "roleType", type = "KEYS") })
public class RoleAllDataType
{
    @Id
    @Column(name = "ROLE_NAME")
    private String roleName;

    @Column(name = "ROLE_TYPE")
    private String roleType;

    @Column(name = "ROLE_CODE")
    private int roleCode;

    @Column(name = "ROLE_LOCATION_ID")
    private long roleLocationId;

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

    @Column(name = "IMDB_RATING")
    private double imdbRating;

    // Date-time types
    @Column(name = "ROLE_START_DATE")
    @Temporal(TemporalType.DATE)
    private java.util.Date roleStartDate;

    @Column(name = "ROLE_START_TIME")
    @Temporal(TemporalType.TIME)
    private java.util.Date roleStartTime;

    @Column(name = "SCRIPT_READ_TIME")
    @Temporal(TemporalType.TIMESTAMP)
    private java.util.Date scriptReadTime;

    // Wrapper types
    @Column(name = "YEARS_SPENT")
    private Integer yearsSpent;

    @Column(name = "UNIQUE_ID")
    private Long uniqueId;

    @Column(name = "REMUNERATION")
    private Double remuneration;

    @Column(name = "REHEARSAL_ATTEMPTS")
    private BigInteger rehearsalAttempts;

    @Column(name = "TO_BE_PAID_FOR_ROLE")
    private BigDecimal toBePaidForRole;

    @Column(name = "GRADUATION_DAY")
    private Calendar graduationDay;

    @OneToOne
    private ActorAllDataType actor;

    @OneToOne
    private MovieAllDataType movie;

    public RoleAllDataType()
    {

    }

    public RoleAllDataType(String roleName, String roleType, int roleCode, long roleLocationId, boolean isExceptional,
            char grade, byte digitalSignature, short rating, float compliance, double imdbRating, Date roleStartDate,
            Date roleStartTime, Date scriptReadTime, Integer yearsSpent, Long uniqueId, Double remuneration,
            BigInteger rehearsalAttempts, BigDecimal toBePaidForRole, Calendar graduationDay)
    {
        super();
        this.roleName = roleName;
        this.roleType = roleType;
        this.roleCode = roleCode;
        this.roleLocationId = roleLocationId;
        this.isExceptional = isExceptional;
        this.grade = grade;
        this.digitalSignature = digitalSignature;
        this.rating = rating;
        this.compliance = compliance;
        this.imdbRating = imdbRating;
        this.roleStartDate = roleStartDate;
        this.roleStartTime = roleStartTime;
        this.scriptReadTime = scriptReadTime;
        this.yearsSpent = yearsSpent;
        this.uniqueId = uniqueId;
        this.remuneration = remuneration;
        this.rehearsalAttempts = rehearsalAttempts;
        this.toBePaidForRole = toBePaidForRole;
        this.graduationDay = graduationDay;
    }

    /**
     * @return the roleName
     */
    public String getRoleName()
    {
        return roleName;
    }

    /**
     * @param roleName
     *            the roleName to set
     */
    public void setRoleName(String roleName)
    {
        this.roleName = roleName;
    }

    /**
     * @return the roleType
     */
    public String getRoleType()
    {
        return roleType;
    }

    /**
     * @param roleType
     *            the roleType to set
     */
    public void setRoleType(String roleType)
    {
        this.roleType = roleType;
    }

    /**
     * @return the roleCode
     */
    public int getRoleCode()
    {
        return roleCode;
    }

    /**
     * @param roleCode
     *            the roleCode to set
     */
    public void setRoleCode(int roleCode)
    {
        this.roleCode = roleCode;
    }

    /**
     * @return the roleLocationId
     */
    public long getRoleLocationId()
    {
        return roleLocationId;
    }

    /**
     * @param roleLocationId
     *            the roleLocationId to set
     */
    public void setRoleLocationId(long roleLocationId)
    {
        this.roleLocationId = roleLocationId;
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
     * @return the imdbRating
     */
    public double getImdbRating()
    {
        return imdbRating;
    }

    /**
     * @param imdbRating
     *            the imdbRating to set
     */
    public void setImdbRating(double imdbRating)
    {
        this.imdbRating = imdbRating;
    }

    /**
     * @return the roleStartDate
     */
    public java.util.Date getRoleStartDate()
    {
        return roleStartDate;
    }

    /**
     * @param roleStartDate
     *            the roleStartDate to set
     */
    public void setRoleStartDate(java.util.Date roleStartDate)
    {
        this.roleStartDate = roleStartDate;
    }

    /**
     * @return the roleStartTime
     */
    public java.util.Date getRoleStartTime()
    {
        return roleStartTime;
    }

    /**
     * @param roleStartTime
     *            the roleStartTime to set
     */
    public void setRoleStartTime(java.util.Date roleStartTime)
    {
        this.roleStartTime = roleStartTime;
    }

    /**
     * @return the scriptReadTime
     */
    public java.util.Date getScriptReadTime()
    {
        return scriptReadTime;
    }

    /**
     * @param scriptReadTime
     *            the scriptReadTime to set
     */
    public void setScriptReadTime(java.util.Date scriptReadTime)
    {
        this.scriptReadTime = scriptReadTime;
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
     * @return the remuneration
     */
    public Double getRemuneration()
    {
        return remuneration;
    }

    /**
     * @param remuneration
     *            the remuneration to set
     */
    public void setRemuneration(Double remuneration)
    {
        this.remuneration = remuneration;
    }

    /**
     * @return the rehearsalAttempts
     */
    public BigInteger getRehearsalAttempts()
    {
        return rehearsalAttempts;
    }

    /**
     * @param rehearsalAttempts
     *            the rehearsalAttempts to set
     */
    public void setRehearsalAttempts(BigInteger rehearsalAttempts)
    {
        this.rehearsalAttempts = rehearsalAttempts;
    }

    /**
     * @return the toBePaidForRole
     */
    public BigDecimal getToBePaidForRole()
    {
        return toBePaidForRole;
    }

    /**
     * @param toBePaidForRole
     *            the toBePaidForRole to set
     */
    public void setToBePaidForRole(BigDecimal toBePaidForRole)
    {
        this.toBePaidForRole = toBePaidForRole;
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

    /**
     * @return the actor
     */
    public ActorAllDataType getActor()
    {
        return actor;
    }

    /**
     * @param actor
     *            the actor to set
     */
    public void setActor(ActorAllDataType actor)
    {
        this.actor = actor;
    }

    /**
     * @return the movie
     */
    public MovieAllDataType getMovie()
    {
        return movie;
    }

    /**
     * @param movie
     *            the movie to set
     */
    public void setMovie(MovieAllDataType movie)
    {
        this.movie = movie;
    }

}
