/*******************************************************************************
 *  * Copyright 2015 Impetus Infotech.
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
package com.impetus.client.spark;

import java.io.Serializable;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * The Class Personnel.
 * 
 * @author amitkumar
 */
@Entity
@Table(name = "person")
public class Person implements Serializable
{
    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 1L;

    /** The id. */
    @Id
    private int id;

    /** The first name. */
    private String firstName;

    /** The state number. */
    private int salary;

    /**
     * Instantiates a new personnel.
     */
    public Person()
    {

    }

    /**
     * Gets the id.
     * 
     * @return the id
     */
    public int getId()
    {
        return id;
    }

    /**
     * Sets the id.
     * 
     * @param id
     *            the new id
     */
    public void setId(int id)
    {
        this.id = id;
    }

    /**
     * Gets the first name.
     * 
     * @return the first name
     */
    public String getFirstName()
    {
        return firstName;
    }

    /**
     * Sets the first name.
     * 
     * @param firstName
     *            the new first name
     */
    public void setFirstName(String firstName)
    {
        this.firstName = firstName;
    }

    /**
     * Gets the salary.
     * 
     * @return the salary
     */
    public int getSalary()
    {
        return salary;
    }

    /**
     * Sets the salary.
     * 
     * @param salary
     *            the new salary
     */
    public void setSalary(int salary)
    {
        this.salary = salary;
    }

    /**
     * Gets the serialversionuid.
     * 
     * @return the serialversionuid
     */
    public static long getSerialversionuid()
    {
        return serialVersionUID;
    }
}
