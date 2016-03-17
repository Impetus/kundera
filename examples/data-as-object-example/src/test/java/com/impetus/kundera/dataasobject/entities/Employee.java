/*******************************************************************************
 *  * Copyright 2016 Impetus Infotech.
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
package com.impetus.kundera.dataasobject.entities;

import javax.persistence.Entity;
import javax.persistence.Id;

import com.impetus.core.DefaultKunderaEntity;

/**
 * The Class Employee.
 */
@Entity
public class Employee extends DefaultKunderaEntity<Employee, Long>
{

    /** The employee id. */
    @Id
    private Long employeeId;

    /** The salary. */
    private Double salary;

    /** The name. */
    private String name;

    /**
     * Gets the emplyoee id.
     *
     * @return the emplyoee id
     */
    public Long getEmplyoeeId()
    {
        return employeeId;
    }

    /**
     * Sets the emplyoee id.
     *
     * @param emplyoeeId
     *            the new emplyoee id
     */
    public void setEmplyoeeId(Long emplyoeeId)
    {
        this.employeeId = emplyoeeId;
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
     * Gets the name.
     *
     * @return the name
     */
    public String getName()
    {
        return name;
    }

    /**
     * Sets the name.
     *
     * @param name
     *            the new name
     */
    public void setName(String name)
    {
        this.name = name;
    }

}
