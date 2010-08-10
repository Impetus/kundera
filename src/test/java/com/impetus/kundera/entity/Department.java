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

import com.impetus.kundera.api.Cacheable;
import com.impetus.kundera.api.ColumnFamily;

@Entity
@ColumnFamily (family="Department")
@Cacheable (true)
public class Department {

	@Id
	private String name;
	
	@Column
	private String address;
	
	@ManyToMany (cascade={CascadeType.ALL}, fetch=FetchType.LAZY)
	private List<Employee> employees = new ArrayList<Employee>();

	public Department() {
		
	}
	
	/**
	 * @param name
	 * @param address
	 */
	public Department(String name, String address) {
		super();
		this.name = name;
		this.address = address;
	}

	/**
	 * @return the name
	 */
	public String getName() {
		return name;
	}

	/**
	 * @param name the name to set
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * @return the address
	 */
	public String getAddress() {
		return address;
	}

	/**
	 * @param address the address to set
	 */
	public void setAddress(String address) {
		this.address = address;
	}

	/**
	 * @return the employees
	 */
	public List<Employee> getEmployees() {
		return employees;
	}

	/**
	 * @param employees the employees to set
	 */
	public void setEmployees(List<Employee> employees) {
		this.employees = employees;
	}
	
	public void addEmployee (Employee... e) {
		for (Employee e_ : e)
			employees.add(e_);
	}

	/* @see java.lang.Object#toString() */
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("Department [name=");
		builder.append(name);
		builder.append(", address=");
		builder.append(address);

		builder.append(", employees=(");
		for (Employee e : employees) {
			builder.append(e.getName() + ",");
		}
		builder.append(")");
		builder.append("]");
		return builder.toString();
	}
	
	

	
}
