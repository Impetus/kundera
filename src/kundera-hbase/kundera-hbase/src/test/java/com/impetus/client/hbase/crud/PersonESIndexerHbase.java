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
package com.impetus.client.hbase.crud;

import java.sql.Timestamp;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.Id;
import javax.persistence.Table;

import com.impetus.kundera.index.IndexCollection;

/**
 * The Class PersonESIndexerHbase.
 */
@Entity
@Table(name = "PERSON_ESINDEXER", schema = "KunderaExamples@hbaseESindexerTest")
@IndexCollection(columns = { @com.impetus.kundera.index.Index(name = "personName"),
        @com.impetus.kundera.index.Index(name = "age"), @com.impetus.kundera.index.Index(name = "date"),
        @com.impetus.kundera.index.Index(name = "salary") })
public class PersonESIndexerHbase
{

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 6068131491098913126L;

    /** The Constant UID. */
    public static final String UID = "uid";

    /** The Constant EID. */
    public static final String EID = "eid";

    /** The Constant FIRST_NAME. */
    public static final String FIRST_NAME = "firstName";

    /** The Constant LAST_NAME. */
    public static final String LAST_NAME = "lastName";

    /** The Constant CITY. */
    public static final String CITY = "city";

    /** The Constant CREATED. */
    public static final String CREATED = "created";

    /** The Constant LAST_MODIFIED. */
    public static final String LAST_MODIFIED = "lastModified";

    /** The person id. */
    @Id
    @Column
    private String personId;

    /** The person name. */
    @Column(name = "PERSON_NAME")
    private String personName;

    /** The salary. */
    @Column
    private Double salary;

    /** The age. */
    @Column(name = "AGE")
    private Integer age;

    /** The date. */
    @Column(name = "DATE")
    private Timestamp date;

    /**
     * Gets the date.
     * 
     * @return the date
     */
    public Timestamp getDate()
    {
        return date;
    }

    /**
     * Sets the date.
     * 
     * @param date
     *            the new date
     */
    public void setDate(Timestamp date)
    {
        this.date = date;
    }

    /** The day. */
    @Column(name = "ENUM")
    @Enumerated(EnumType.STRING)
    private Day day;

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
    public void setAge(Integer age)
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
     * Gets the salary.
     * 
     * @return the salary
     */
    public Double getSalary()
    {
        return salary;
    }

    /**
     * Sets the salary.
     * 
     * @param salary
     *            the new salary
     */
    public void setSalary(Double salary)
    {
        this.salary = salary;
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

}
