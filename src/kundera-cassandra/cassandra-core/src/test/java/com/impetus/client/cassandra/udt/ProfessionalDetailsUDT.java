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
/*
 * author: karthikp.manchala
 */
package com.impetus.client.cassandra.udt;

import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.Column;
import javax.persistence.Embeddable;

/**
 * The Class ProfessionalDetailsUDT.
 */
@Embeddable
public class ProfessionalDetailsUDT
{

    /** The company. */
    @Column
    private String company;

    /** The project. */
    @Column
    private List<Integer> extentions;

    /** The colleagues. */
    @Column
    private Set<String> colleagues;

    /** The projects. */
    @Column
    private Map<Integer, String> projects;

    /** The grade. */
    @Column
    private String grade;

    /** The monthly salary. */
    @Column
    private Double monthlySalary;

    /**
     * Gets the company.
     * 
     * @return the company
     */
    public String getCompany()
    {
        return company;
    }

    /**
     * Sets the company.
     * 
     * @param company
     *            the new company
     */
    public void setCompany(String company)
    {
        this.company = company;
    }

    /**
     * Gets the extentions.
     * 
     * @return the extentions
     */
    public List<Integer> getExtentions()
    {
        return extentions;
    }

    /**
     * Sets the extentions.
     * 
     * @param extentions
     *            the new extentions
     */
    public void setExtentions(List<Integer> extentions)
    {
        this.extentions = extentions;
    }

    /**
     * Gets the colleagues.
     * 
     * @return the colleagues
     */
    public Set<String> getColleagues()
    {
        return colleagues;
    }

    /**
     * Sets the colleagues.
     * 
     * @param colleagues
     *            the new colleagues
     */
    public void setColleagues(Set<String> colleagues)
    {
        this.colleagues = colleagues;
    }

    /**
     * Gets the projects.
     * 
     * @return the projects
     */
    public Map<Integer, String> getProjects()
    {
        return projects;
    }

    /**
     * Sets the projects.
     * 
     * @param projects
     *            the projects
     */
    public void setProjects(Map<Integer, String> projects)
    {
        this.projects = projects;
    }

    /**
     * Gets the grade.
     * 
     * @return the grade
     */
    public String getGrade()
    {
        return grade;
    }

    /**
     * Sets the grade.
     * 
     * @param grade
     *            the new grade
     */
    public void setGrade(String grade)
    {
        this.grade = grade;
    }

    /**
     * Gets the monthly salary.
     * 
     * @return the monthly salary
     */
    public Double getMonthlySalary()
    {
        return monthlySalary;
    }

    /**
     * Sets the monthly salary.
     * 
     * @param monthlySalary
     *            the new monthly salary
     */
    public void setMonthlySalary(Double monthlySalary)
    {
        this.monthlySalary = monthlySalary;
    }

}
