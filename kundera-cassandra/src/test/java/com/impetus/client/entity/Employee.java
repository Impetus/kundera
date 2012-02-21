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
package com.impetus.client.entity;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.ManyToMany;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.Table;

//@Entity
/**
 * The Class Employee.
 */
@Table(name = "Employee", schema = "Blog")
public class Employee
{

    /** The name. */
    @Id
    private String name;

    /** The role. */
    @Column
    private String role;

    /** The team. */
    @OneToMany(cascade = { CascadeType.ALL }, fetch = FetchType.LAZY)
    private List<Employee> team = new ArrayList<Employee>();

    /** The boss. */
    @ManyToOne(cascade = { CascadeType.ALL })
    private Employee boss;

    /** The deptt. */
    @ManyToMany(cascade = { CascadeType.ALL })
    private List<Department> deptt = new ArrayList<Department>();

    /**
     * Instantiates a new employee.
     *
     * @param name the name
     * @param role the role
     */
    public Employee(String name, String role)
    {
        this.name = name;
        this.role = role;
    }

    /**
     * Instantiates a new employee.
     */
    public Employee()
    {
        super();
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
     * @param name the name to set
     */
    public void setName(String name)
    {
        this.name = name;
    }

    /**
     * Gets the role.
     *
     * @return the role
     */
    public String getRole()
    {
        return role;
    }

    /**
     * Sets the role.
     *
     * @param role the role to set
     */
    public void setRole(String role)
    {
        this.role = role;
    }

    /**
     * Gets the team.
     *
     * @return the team
     */
    public List<Employee> getTeam()
    {
        return team;
    }

    /**
     * Sets the team.
     *
     * @param team the team to set
     */
    public void setTeam(List<Employee> team)
    {
        this.team = team;
    }

    /**
     * Addto team.
     *
     * @param e the e
     * @see java.util.List#add(java.lang.Object)
     */
    public void addtoTeam(Employee... e)
    {
        for (Employee e_ : e)
            team.add(e_);
    }

    /**
     * Gets the boss.
     *
     * @return the boss
     */
    public Employee getBoss()
    {
        return boss;
    }

    /**
     * Sets the boss.
     *
     * @param boss the boss to set
     */
    public void setBoss(Employee boss)
    {
        this.boss = boss;
    }

    /**
     * Gets the deptt.
     *
     * @return the deptt
     */
    public List<Department> getDeptt()
    {
        return deptt;
    }

    /**
     * Addto deptt.
     *
     * @param d the d
     */
    public void addtoDeptt(Department... d)
    {
        for (Department d_ : d)
            deptt.add(d_);
    }

    /* @see java.lang.Object#toString() */
    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append("Employee [name=");
        builder.append(name);
        builder.append(", role=");
        builder.append(role);
        if (null != boss)
        {
            builder.append(", boss=");
            builder.append(boss.getName());
        }
        builder.append(", team=(");
        for (Employee e : team)
        {
            builder.append(e.getName() + ",");
        }
        builder.append(")");

        builder.append(", deptt=(");
        for (Department d : deptt)
        {
            builder.append(d.getName() + ",");
        }
        builder.append(")");

        builder.append("]");
        return builder.toString();
    }

}
