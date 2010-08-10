/*
 * Copyright 2010 Impetus Infotech.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.impetus.kundera.entity;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.ManyToMany;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;

import com.impetus.kundera.api.ColumnFamily;

@Entity
@ColumnFamily(family = "Employee")
public class Employee {

	@Id
	private String name;

	@Column
	private String role;

	@OneToMany (cascade={CascadeType.ALL}, fetch=FetchType.LAZY)
	private List<Employee> team = new ArrayList<Employee>();

	@ManyToOne (cascade={CascadeType.ALL})
	private Employee boss;
	
	@ManyToMany (cascade={CascadeType.ALL})
	private List<Department> deptt = new ArrayList<Department>();
	
	/**
	 * @param name
	 * @param role
	 */
	public Employee(String name, String role) {
		this.name = name;
		this.role = role;
	}

	/**
	 * 
	 */
	public Employee() {
		super();
	}

	/**
	 * @return the name
	 */
	public String getName() {
		return name;
	}

	/**
	 * @param name
	 *            the name to set
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * @return the role
	 */
	public String getRole() {
		return role;
	}

	/**
	 * @param role
	 *            the role to set
	 */
	public void setRole(String role) {
		this.role = role;
	}

	/**
	 * @return the team
	 */
	public List<Employee> getTeam() {
		return team;
	}

	/**
	 * @param team
	 *            the team to set
	 */
	public void setTeam(List<Employee> team) {
		this.team = team;
	}

	/**
	 * @param e
	 * @return
	 * @see java.util.List#add(java.lang.Object)
	 */
	public void addtoTeam (Employee... e) {
		for (Employee e_ : e)
			team.add(e_);
	}

	/**
	 * @return the boss
	 */
	public Employee getBoss() {
		return boss;
	}

	/**
	 * @param boss the boss to set
	 */
	public void setBoss(Employee boss) {
		this.boss = boss;
	}

	
	/**
	 * @return the deptt
	 */
	public List<Department> getDeptt() {
		return deptt;
	}

	public void addtoDeptt (Department... d) {
		for (Department d_ : d)
			deptt.add(d_);
	}

	/* @see java.lang.Object#toString() */
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("Employee [name=");
		builder.append(name);
		builder.append(", role=");
		builder.append(role);
		if (null != boss) {
			builder.append(", boss=");
			builder.append(boss.getName());
		}
		builder.append(", team=(");
		for (Employee e : team) {
			builder.append(e.getName() + ",");
		}
		builder.append(")");

		builder.append(", deptt=(");
		for (Department d : deptt) {
			builder.append(d.getName() + ",");
		}
		builder.append(")");

		builder.append("]");
		return builder.toString();
	}

}
