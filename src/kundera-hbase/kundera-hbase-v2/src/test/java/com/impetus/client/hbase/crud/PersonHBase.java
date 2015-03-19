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
package com.impetus.client.hbase.crud;

/**
 * @author Pragalbh Garg
 *
 */
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * The Class PersonHBase.
 */
@Entity
@Table(name = "PERSON_HBASE", schema = "HBaseNew@crudTest")
public class PersonHBase
{

    /** The person id. */
    @Id
    @Column(name = "PERSON_ID")
    private String personId;

    /** The person name. */
    @Column(name = "PERSON_NAME")
    private String personName;

    /** The age. */
    @Column(name = "AGE")
    private Integer age;

    /** The day. */
    @Column(name = "ENUM")
    @Enumerated(EnumType.STRING)
    private Day day;

    /** The month. */
    @Column(name = "MONTH_ENUM")
    @Enumerated(EnumType.STRING)
    private Month month;

    /**
     * Gets the person id.
     * 
     * @return the person id
     */
    public String getPersonId()
    {
        return personId;
    }

    /**
     * Gets the person name.
     * 
     * @return the person name
     */
    public String getPersonName()
    {
        return personName;
    }

    /**
     * Sets the person name.
     * 
     * @param personName
     *            the new person name
     */
    public void setPersonName(String personName)
    {
        this.personName = personName;
    }

    /**
     * Sets the person id.
     * 
     * @param personId
     *            the new person id
     */
    public void setPersonId(String personId)
    {
        this.personId = personId;
    }

    /**
     * Gets the age.
     * 
     * @return the age
     */
    public Integer getAge()
    {
        return age;
    }

    /**
     * Sets the age.
     * 
     * @param age
     *            the age to set
     */
    public void setAge(int age)
    {
        this.age = age;
    }

    /**
     * Gets the day.
     * 
     * @return the day
     */
    public Day getDay()
    {
        return day;
    }

    /**
     * Sets the day.
     * 
     * @param day
     *            the day to set
     */
    public void setDay(Day day)
    {
        this.day = day;
    }

    /**
     * Gets the month.
     * 
     * @return the month
     */
    public Month getMonth()
    {
        return month;
    }

    /**
     * Sets the month.
     * 
     * @param month
     *            the new month
     */
    public void setMonth(Month month)
    {
        this.month = month;
    }

    /**
     * The Enum Day.
     */
    enum Day
    {

        /** The monday. */
        MONDAY,
        /** The tuesday. */
        TUESDAY,
        /** The wednesday. */
        WEDNESDAY,
        /** The thursday. */
        THURSDAY,
        /** The friday. */
        FRIDAY,
        /** The saturday. */
        SATURDAY,
        /** The sunday. */
        SUNDAY;
    }

    /**
     * The Enum Month.
     */
    enum Month
    {

        /** The jan. */
        JAN,
        /** The feb. */
        FEB,
        /** The march. */
        MARCH,
        /** The april. */
        APRIL,
        /** The may. */
        MAY,
        /** The june. */
        JUNE;
    }

}
