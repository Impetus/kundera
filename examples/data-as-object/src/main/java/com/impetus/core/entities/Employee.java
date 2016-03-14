package com.impetus.core.entities;

import javax.persistence.Entity;
import javax.persistence.Id;

import com.impetus.core.DefaultKunderaEntity;

@Entity
public class Employee extends DefaultKunderaEntity<Employee, Long> {
	@Id
	private Long emplyoeeId;

	private Double salary;

	private String name;

	public Long getEmplyoeeId() {
		return emplyoeeId;
	}

	public void setEmplyoeeId(Long emplyoeeId) {
		this.emplyoeeId = emplyoeeId;
	}

	public Double getSalary() {
		return salary;
	}

	public void setSalary(Double salary) {
		this.salary = salary;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

}
