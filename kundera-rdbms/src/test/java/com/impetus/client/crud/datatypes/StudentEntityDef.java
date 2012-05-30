/*******************************************************************************
 * * Copyright 2011 Impetus Infotech.
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

/**
 * @author vivek.mishra
 *
 */
public interface StudentEntityDef
{

    /**
     * @return the studentId
     */
    long getStudentId();

    /**
     * @param studentId
     *            the studentId to set
     */
    void setStudentId(long studentId);

    /**
     * @return the uniqueId
     */
    long getUniqueId();

    /**
     * @param uniqueId
     *            the uniqueId to set
     */
    void setUniqueId(long uniqueId);

    /**
     * @return the studentName
     */
    String getStudentName();

    /**
     * @param studentName
     *            the studentName to set
     */
    void setStudentName(String studentName);

    /**
     * @return the isExceptional
     */
    boolean isExceptional();

    /**
     * @param isExceptional
     *            the isExceptional to set
     */
    void setExceptional(boolean isExceptional);

    /**
     * @return the age
     */
    int getAge();

    /**
     * @param age
     *            the age to set
     */
    void setAge(int age);

    /**
     * @return the semester
     */
    char getSemester();

    /**
     * @param semester
     *            the semester to set
     */
    void setSemester(char semester);

    /**
     * @return the digitalSignature
     */
    byte getDigitalSignature();

    /**
     * @param digitalSignature
     *            the digitalSignature to set
     */
    void setDigitalSignature(byte digitalSignature);

    /**
     * @return the cgpa
     */
    short getCgpa();

    /**
     * @param cgpa
     *            the cgpa to set
     */
    void setCgpa(short cgpa);

    /**
     * @return the percentage
     */
    float getPercentage();

    /**
     * @param percentage
     *            the percentage to set
     */
    void setPercentage(float percentage);

    /**
     * @return the height
     */
    double getHeight();

    /**
     * @param height
     *            the height to set
     */
    void setHeight(double height);

    /**
     * @return the enrolmentDate
     */
    java.util.Date getEnrolmentDate();

    /**
     * @param enrolmentDate
     *            the enrolmentDate to set
     */
    void setEnrolmentDate(java.util.Date enrolmentDate);

    /**
     * @return the enrolmentTime
     */
    java.util.Date getEnrolmentTime();

    /**
     * @param enrolmentTime
     *            the enrolmentTime to set
     */
    void setEnrolmentTime(java.util.Date enrolmentTime);

    /**
     * @return the joiningDateAndTime
     */
    java.util.Date getJoiningDateAndTime();

    /**
     * @param joiningDateAndTime
     *            the joiningDateAndTime to set
     */
    void setJoiningDateAndTime(java.util.Date joiningDateAndTime);

    /**
     * @return the yearsSpent
     */
    Integer getYearsSpent();

    /**
     * @param yearsSpent
     *            the yearsSpent to set
     */
    void setYearsSpent(Integer yearsSpent);

    /**
     * @return the rollNumber
     */
    Long getRollNumber();

    /**
     * @param rollNumber
     *            the rollNumber to set
     */
    void setRollNumber(Long rollNumber);

    /**
     * @return the monthlyFee
     */
    Double getMonthlyFee();

    /**
     * @param monthlyFee
     *            the monthlyFee to set
     */
    void setMonthlyFee(Double monthlyFee);

    java.sql.Date getSqlDate();

    void setSqlDate(java.sql.Date sqlDate);

    /**
     * @return the sqlTimestamp
     */
    java.sql.Timestamp getSqlTimestamp();

    /**
     * @param sqlTimestamp
     *            the sqlTimestamp to set
     */
    void setSqlTimestamp(java.sql.Timestamp sqlTimestamp);

    /**
     * @return the sqlTime
     */
    java.sql.Time getSqlTime();

    /**
     * @param sqlTime
     *            the sqlTime to set
     */
    void setSqlTime(java.sql.Time sqlTime);

    /**
     * @return the bigInteger
     */
    BigInteger getBigInteger();

    /**
     * @param bigInteger
     *            the bigInteger to set
     */
    void setBigInteger(BigInteger bigInteger);

    /**
     * @return the bigDecimal
     */
    BigDecimal getBigDecimal();

    /**
     * @param bigDecimal
     *            the bigDecimal to set
     */
    void setBigDecimal(BigDecimal bigDecimal);

    /**
     * @return the calendar
     */
    Calendar getCalendar();

    /**
     * @param calendar
     *            the calendar to set
     */
    void setCalendar(Calendar calendar);

}