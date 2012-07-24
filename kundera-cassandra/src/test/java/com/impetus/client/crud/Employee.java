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
package com.impetus.client.crud;

import java.util.Set;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.Table;

/**
 * @author vivek.mishra
 * 
 */

@Entity
@Table(name = "PERSON", schema = "KunderaExamples@secIdxCassandraTest")
public class Employee
{
    @Id
    @Column(name = "PERSON_ID")
    private String personId;

    @Column(name = "PERSON_NAME")
    private String personName;

    @OneToMany(cascade = CascadeType.ALL, fetch = FetchType.LAZY, mappedBy = "manager")
    private Set<Employee> Employees;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "MANAGER_ID")
    private Employee manager;

    public Employee()
    {
    }

    /**
     * @return the personId
     */
    public String getPersonId()
    {
        return personId;
    }

    /**
     * @param personId
     *            the personId to set
     */
    public void setPersonId(String personId)
    {
        this.personId = personId;
    }

    /**
     * @return the personName
     */
    public String getPersonName()
    {
        return personName;
    }

    /**
     * @param personName
     *            the personName to set
     */
    public void setPersonName(String personName)
    {
        this.personName = personName;
    }

    /**
     * @return the employees
     */
    public Set<Employee> getEmployees()
    {
        return Employees;
    }

    /**
     * @param employees
     *            the employees to set
     */
    public void setEmployees(Set<Employee> employees)
    {
        Employees = employees;
    }

    /**
     * @return the manager
     */
    public Employee getManager()
    {
        return manager;
    }

    /**
     * @param manager
     *            the manager to set
     */
    public void setManager(Employee manager)
    {
        this.manager = manager;
    }

    // Constructors, Getters, setters here
}