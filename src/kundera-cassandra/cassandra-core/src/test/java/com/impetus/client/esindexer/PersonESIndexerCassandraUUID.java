/*******************************************************************************
 * * Copyright 2014 Impetus Infotech.
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
package com.impetus.client.esindexer;

import java.sql.Timestamp;
import java.util.UUID;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.Id;
import javax.persistence.Table;

import com.impetus.kundera.index.IndexCollection;

@Entity
@Table(name = "PERSON_ESINDEXERUUID", schema = "KunderaExamples@esIndexerTest")
@IndexCollection(columns = { @com.impetus.kundera.index.Index(name = "personName"),
    @com.impetus.kundera.index.Index(name = "age"), @com.impetus.kundera.index.Index(name = "date") })
public class PersonESIndexerCassandraUUID {

    private static final long serialVersionUID = 6068131491098913126L;

    public static final String UID = "uid";

    public static final String EID = "eid";

    public static final String FIRST_NAME = "firstName";

    public static final String LAST_NAME = "lastName";

    public static final String CITY = "city";

    public static final String CREATED = "created";

    public static final String LAST_MODIFIED = "lastModified";

    /** The person id. */
    @Id
    // @Column(name = "PERSON_ID")
    private UUID personId;

    /** The person name. */
    @Column(name = "PERSON_NAME")
    private String personName;

    /** The age. */
    @Column(name = "AGE")
    private String age;

    @Column(name = "DATE")
    private Timestamp date;

    public Timestamp getDate() {
        return date;
    }

    public void setDate(Timestamp date) {
        this.date = date;
    }

    @Column(name = "ENUM")
    @Enumerated(EnumType.STRING)
    private Day day;

    /**
     * Gets the person id.
     * 
     * @return the person id
     */
    public UUID getPersonId() {
        return personId;
    }

    /**
     * Gets the person name.
     * 
     * @return the person name
     */
    public String getPersonName() {
        return personName;
    }

    /**
     * Sets the person name.
     * 
     * @param personName
     *            the new person name
     */
    public void setPersonName(String personName) {
        this.personName = personName;
    }

    /**
     * Sets the person id.
     * 
     * @param personId
     *            the new person id
     */
    public void setPersonId(UUID personId) {
        this.personId = personId;
    }

    /**
     * @return the age
     */
    public String getAge() {
        return age;
    }

    /**
     * @param age
     *            the age to set
     */
    public void setAge(String age) {
        this.age = age;
    }

    /**
     * @return the day
     */
    public Day getDay() {
        return day;
    }

    /**
     * @param day
     *            the day to set
     */
    public void setDay(Day day) {
        this.day = day;
    }

    enum Day {
        MONDAY, TUESDAY, WEDNESDAY, THURSDAY, FRIDAY, SATURDAY, SUNDAY;
    }

}