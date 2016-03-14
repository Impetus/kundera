package com.impetus.dao.crud;

import com.impetus.core.entities.Employee;

public class TestCRUD {
	public static void main(String args[]) {
		Employee emp = new Employee();
		emp.setEmplyoeeId(100l);
		emp.setName("kart");
		emp.setSalary(10000d);
		
		emp.save();
	}
}
