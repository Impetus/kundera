package com.impetus.dao.entities;

import javax.persistence.Entity;
import javax.persistence.Id;

import com.impetus.core.DefaultKunderaEntity;
import com.impetus.kundera.index.Index;
import com.impetus.kundera.index.IndexCollection;

@Entity
@IndexCollection(columns = { @Index(name = "employeeId")})
public class Department extends DefaultKunderaEntity<Department, Long> {
	@Id
	private Long deptId;

	private Long employeeId;

	private String departmentName;

	public Long getDeptId() {
		return deptId;
	}

	public void setDeptId(Long deptId) {
		this.deptId = deptId;
	}

	public Long getEmployeeId() {
		return employeeId;
	}

	public void setEmployeeId(Long employeeId) {
		this.employeeId = employeeId;
	}

	public String getDepartmentName() {
		return departmentName;
	}

	public void setDepartmentName(String departmentName) {
		this.departmentName = departmentName;
	}

}
