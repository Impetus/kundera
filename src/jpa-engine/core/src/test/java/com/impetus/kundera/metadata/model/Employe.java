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
package com.impetus.kundera.metadata.model;

import javax.persistence.Column;
import javax.persistence.Embedded;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

import com.impetus.kundera.index.Index;
import com.impetus.kundera.index.IndexCollection;

/**
 * @author Kuldeep Mishra
 * 
 */
@Entity
@Table(name = "EMPLOYE", schema = "KunderaMetaDataTest@metaDataTest")
@IndexCollection(columns = { @Index(name = "empName"), @Index(name = "age")})
public class Employe
{

    /** The person id. */
    @Id
    @Column(name = "EMP_ID")
    private String empId;

    /** The person name. */
    @Column(name = "EMP_NAME")
    private String empName;

    /** The age. */
    @Column(name = "AGE")
    private short age;

    /** The personal data. */
    @Embedded
    private Department departmentData;

    /**
     * Gets the person id.
     * 
     * @return the person id
     */
    public String getEmpId()
    {
        return empId;
    }

    /**
     * Gets the person name.
     * 
     * @return the person name
     */
    public String getEmpName()
    {
        return empName;
    }

    /**
     * Sets the person name.
     * 
     * @param personName
     *            the new person name
     */
    public void setEmpName(String empName)
    {
        this.empName = empName;
    }

    /**
     * Sets the person id.
     * 
     * @param personId
     *            the new person id
     */
    public void setEmpId(String empId)
    {
        this.empId = empId;
    }

    /**
     * Gets the age.
     * 
     * @return the age
     */
    public short getAge()
    {
        return age;
    }

    /**
     * Sets the age.
     * 
     * @param age
     *            the age to set
     */
    public void setAge(short age)
    {
        this.age = age;
    }

    /**
     * Gets the personal data.
     * 
     * @return the personalData
     */
    public Department getDepartmentData()
    {
        return departmentData;
    }

    /**
     * Sets the personal data.
     * 
     * @param personalData
     *            the personalData to set
     */
    public void setDepartmentData(Department departmentData)
    {
        this.departmentData = departmentData;
    }

}
