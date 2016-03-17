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
import com.impetus.kundera.index.Index;
import com.impetus.kundera.index.IndexCollection;

/**
 * The Class Department.
 */
@Entity
@IndexCollection(columns = { @Index(name = "employeeId") })
public class Department extends DefaultKunderaEntity<Department, Long>
{

    /** The dept id. */
    @Id
    private Long deptId;

    /** The employee id. */
    private Long employeeId;

    /** The department name. */
    private String departmentName;

    /**
     * Gets the dept id.
     *
     * @return the dept id
     */
    public Long getDeptId()
    {
        return deptId;
    }

    /**
     * Sets the dept id.
     *
     * @param deptId
     *            the new dept id
     */
    public void setDeptId(Long deptId)
    {
        this.deptId = deptId;
    }

    /**
     * Gets the employee id.
     *
     * @return the employee id
     */
    public Long getEmployeeId()
    {
        return employeeId;
    }

    /**
     * Sets the employee id.
     *
     * @param employeeId
     *            the new employee id
     */
    public void setEmployeeId(Long employeeId)
    {
        this.employeeId = employeeId;
    }

    /**
     * Gets the department name.
     *
     * @return the department name
     */
    public String getDepartmentName()
    {
        return departmentName;
    }

    /**
     * Sets the department name.
     *
     * @param departmentName
     *            the new department name
     */
    public void setDepartmentName(String departmentName)
    {
        this.departmentName = departmentName;
    }

}
